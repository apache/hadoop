/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.shortcircuit;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.net.DomainPeer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReleaseShortCircuitAccessResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.DomainSocketWatcher;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Waitable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The ShortCircuitCache tracks things which the client needs to access
 * HDFS block files via short-circuit.
 *
 * These things include: memory-mapped regions, file descriptors, and shared
 * memory areas for communicating with the DataNode.
 */
@InterfaceAudience.Private
public class ShortCircuitCache implements Closeable {
  public static final Log LOG = LogFactory.getLog(ShortCircuitCache.class);

  /**
   * Expiry thread which makes sure that the file descriptors get closed
   * after a while.
   */
  private class CacheCleaner implements Runnable, Closeable {
    private ScheduledFuture<?> future;

    /**
     * Run the CacheCleaner thread.
     *
     * Whenever a thread requests a ShortCircuitReplica object, we will make
     * sure it gets one.  That ShortCircuitReplica object can then be re-used
     * when another thread requests a ShortCircuitReplica object for the same
     * block.  So in that sense, there is no maximum size to the cache.
     *
     * However, when a ShortCircuitReplica object is unreferenced by the
     * thread(s) that are using it, it becomes evictable.  There are two
     * separate eviction lists-- one for mmaped objects, and another for
     * non-mmaped objects.  We do this in order to avoid having the regular
     * files kick the mmaped files out of the cache too quickly.  Reusing
     * an already-existing mmap gives a huge performance boost, since the
     * page table entries don't have to be re-populated.  Both the mmap
     * and non-mmap evictable lists have maximum sizes and maximum lifespans.
     */
    @Override
    public void run() {
      ShortCircuitCache.this.lock.lock();
      try {
        if (ShortCircuitCache.this.closed) return;
        long curMs = Time.monotonicNow();

        if (LOG.isDebugEnabled()) {
          LOG.debug(this + ": cache cleaner running at " + curMs);
        }

        int numDemoted = demoteOldEvictableMmaped(curMs);
        int numPurged = 0;
        Long evictionTimeNs = Long.valueOf(0);
        while (true) {
          Entry<Long, ShortCircuitReplica> entry = 
              evictable.ceilingEntry(evictionTimeNs);
          if (entry == null) break;
          evictionTimeNs = entry.getKey();
          long evictionTimeMs = 
              TimeUnit.MILLISECONDS.convert(evictionTimeNs, TimeUnit.NANOSECONDS);
          if (evictionTimeMs + maxNonMmappedEvictableLifespanMs >= curMs) break;
          ShortCircuitReplica replica = entry.getValue();
          if (LOG.isTraceEnabled()) {
            LOG.trace("CacheCleaner: purging " + replica + ": " + 
                  StringUtils.getStackTrace(Thread.currentThread()));
          }
          purge(replica);
          numPurged++;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(this + ": finishing cache cleaner run started at " +
            curMs + ".  Demoted " + numDemoted + " mmapped replicas; " +
            "purged " + numPurged + " replicas.");
        }
      } finally {
        ShortCircuitCache.this.lock.unlock();
      }
    }

    @Override
    public void close() throws IOException {
      if (future != null) {
        future.cancel(false);
      }
    }

    public void setFuture(ScheduledFuture<?> future) {
      this.future = future;
    }

    /**
     * Get the rate at which this cleaner thread should be scheduled.
     *
     * We do this by taking the minimum expiration time and dividing by 4.
     *
     * @return the rate in milliseconds at which this thread should be
     *         scheduled.
     */
    public long getRateInMs() {
      long minLifespanMs =
          Math.min(maxNonMmappedEvictableLifespanMs,
              maxEvictableMmapedLifespanMs);
      long sampleTimeMs = minLifespanMs / 4;
      return (sampleTimeMs < 1) ? 1 : sampleTimeMs;
    }
  }

  /**
   * A task which asks the DataNode to release a short-circuit shared memory
   * slot.  If successful, this will tell the DataNode to stop monitoring
   * changes to the mlock status of the replica associated with the slot.
   * It will also allow us (the client) to re-use this slot for another
   * replica.  If we can't communicate with the DataNode for some reason,
   * we tear down the shared memory segment to avoid being in an inconsistent
   * state.
   */
  private class SlotReleaser implements Runnable {
    /**
     * The slot that we need to release.
     */
    private final Slot slot;

    SlotReleaser(Slot slot) {
      this.slot = slot;
    }

    @Override
    public void run() {
      if (LOG.isTraceEnabled()) {
        LOG.trace(ShortCircuitCache.this + ": about to release " + slot);
      }
      final DfsClientShm shm = (DfsClientShm)slot.getShm();
      final DomainSocket shmSock = shm.getPeer().getDomainSocket();
      DomainSocket sock = null;
      DataOutputStream out = null;
      final String path = shmSock.getPath();
      boolean success = false;
      try {
        sock = DomainSocket.connect(path);
        out = new DataOutputStream(
            new BufferedOutputStream(sock.getOutputStream()));
        new Sender(out).releaseShortCircuitFds(slot.getSlotId());
        DataInputStream in = new DataInputStream(sock.getInputStream());
        ReleaseShortCircuitAccessResponseProto resp =
            ReleaseShortCircuitAccessResponseProto.parseFrom(
                PBHelper.vintPrefixed(in));
        if (resp.getStatus() != Status.SUCCESS) {
          String error = resp.hasError() ? resp.getError() : "(unknown)";
          throw new IOException(resp.getStatus().toString() + ": " + error);
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace(ShortCircuitCache.this + ": released " + slot);
        }
        success = true;
      } catch (IOException e) {
        LOG.error(ShortCircuitCache.this + ": failed to release " +
            "short-circuit shared memory slot " + slot + " by sending " +
            "ReleaseShortCircuitAccessRequestProto to " + path +
            ".  Closing shared memory segment.", e);
      } finally {
        if (success) {
          shmManager.freeSlot(slot);
        } else {
          shm.getEndpointShmManager().shutdown(shm);
        }
        IOUtils.cleanup(LOG, sock, out);
      }
    }
  }

  public interface ShortCircuitReplicaCreator {
    /**
     * Attempt to create a ShortCircuitReplica object.
     *
     * This callback will be made without holding any locks.
     *
     * @return a non-null ShortCircuitReplicaInfo object.
     */
    ShortCircuitReplicaInfo createShortCircuitReplicaInfo();
  }

  /**
   * Lock protecting the cache.
   */
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * The executor service that runs the cacheCleaner.
   */
  private final ScheduledThreadPoolExecutor cleanerExecutor
  = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().
          setDaemon(true).setNameFormat("ShortCircuitCache_Cleaner").
          build());

  /**
   * The executor service that runs the cacheCleaner.
   */
  private final ScheduledThreadPoolExecutor releaserExecutor
      = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().
          setDaemon(true).setNameFormat("ShortCircuitCache_SlotReleaser").
          build());

  /**
   * A map containing all ShortCircuitReplicaInfo objects, organized by Key.
   * ShortCircuitReplicaInfo objects may contain a replica, or an InvalidToken
   * exception.
   */
  private final HashMap<ExtendedBlockId, Waitable<ShortCircuitReplicaInfo>> 
      replicaInfoMap = new HashMap<ExtendedBlockId,
          Waitable<ShortCircuitReplicaInfo>>();

  /**
   * The CacheCleaner.  We don't create this and schedule it until it becomes
   * necessary.
   */
  private CacheCleaner cacheCleaner;

  /**
   * Tree of evictable elements.
   *
   * Maps (unique) insertion time in nanoseconds to the element.
   */
  private final TreeMap<Long, ShortCircuitReplica> evictable =
      new TreeMap<Long, ShortCircuitReplica>();

  /**
   * Maximum total size of the cache, including both mmapped and
   * no$-mmapped elements.
   */
  private final int maxTotalSize;

  /**
   * Non-mmaped elements older than this will be closed.
   */
  private long maxNonMmappedEvictableLifespanMs;

  /**
   * Tree of mmaped evictable elements.
   *
   * Maps (unique) insertion time in nanoseconds to the element.
   */
  private final TreeMap<Long, ShortCircuitReplica> evictableMmapped =
      new TreeMap<Long, ShortCircuitReplica>();

  /**
   * Maximum number of mmaped evictable elements.
   */
  private int maxEvictableMmapedSize;

  /**
   * Mmaped elements older than this will be closed.
   */
  private final long maxEvictableMmapedLifespanMs;

  /**
   * The minimum number of milliseconds we'll wait after an unsuccessful
   * mmap attempt before trying again.
   */
  private final long mmapRetryTimeoutMs;

  /**
   * How long we will keep replicas in the cache before declaring them
   * to be stale.
   */
  private final long staleThresholdMs;

  /**
   * True if the ShortCircuitCache is closed.
   */
  private boolean closed = false;

  /**
   * Number of existing mmaps associated with this cache.
   */
  private int outstandingMmapCount = 0;

  /**
   * Manages short-circuit shared memory segments for the client.
   */
  private final DfsClientShmManager shmManager;

  /**
   * Create a {@link ShortCircuitCache} object from a {@link Configuration}
   */
  public static ShortCircuitCache fromConf(Configuration conf) {
    return new ShortCircuitCache(
        conf.getInt(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_KEY,
            DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_DEFAULT),
        conf.getLong(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_KEY,
            DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_DEFAULT),
        conf.getInt(DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_SIZE,
            DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_SIZE_DEFAULT),
        conf.getLong(DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS,
            DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS_DEFAULT),
        conf.getLong(DFSConfigKeys.DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS,
            DFSConfigKeys.DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS_DEFAULT),
        conf.getLong(DFSConfigKeys.DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS,
            DFSConfigKeys.DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS_DEFAULT),
        conf.getInt(DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS,
            DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT));
  }

  public ShortCircuitCache(int maxTotalSize, long maxNonMmappedEvictableLifespanMs,
      int maxEvictableMmapedSize, long maxEvictableMmapedLifespanMs,
      long mmapRetryTimeoutMs, long staleThresholdMs, int shmInterruptCheckMs) {
    Preconditions.checkArgument(maxTotalSize >= 0);
    this.maxTotalSize = maxTotalSize;
    Preconditions.checkArgument(maxNonMmappedEvictableLifespanMs >= 0);
    this.maxNonMmappedEvictableLifespanMs = maxNonMmappedEvictableLifespanMs;
    Preconditions.checkArgument(maxEvictableMmapedSize >= 0);
    this.maxEvictableMmapedSize = maxEvictableMmapedSize;
    Preconditions.checkArgument(maxEvictableMmapedLifespanMs >= 0);
    this.maxEvictableMmapedLifespanMs = maxEvictableMmapedLifespanMs;
    this.mmapRetryTimeoutMs = mmapRetryTimeoutMs;
    this.staleThresholdMs = staleThresholdMs;
    DfsClientShmManager shmManager = null;
    if ((shmInterruptCheckMs > 0) &&
        (DomainSocketWatcher.getLoadingFailureReason() == null)) {
      try {
        shmManager = new DfsClientShmManager(shmInterruptCheckMs);
      } catch (IOException e) {
        LOG.error("failed to create ShortCircuitShmManager", e);
      }
    }
    this.shmManager = shmManager;
  }

  public long getStaleThresholdMs() {
    return staleThresholdMs;
  }

  /**
   * Increment the reference count of a replica, and remove it from any free
   * list it may be in.
   *
   * You must hold the cache lock while calling this function.
   *
   * @param replica      The replica we're removing.
   */
  private void ref(ShortCircuitReplica replica) {
    lock.lock();
    try {
      Preconditions.checkArgument(replica.refCount > 0,
          "can't ref " + replica + " because its refCount reached " +
          replica.refCount);
      Long evictableTimeNs = replica.getEvictableTimeNs();
      replica.refCount++;
      if (evictableTimeNs != null) {
        String removedFrom = removeEvictable(replica);
        if (LOG.isTraceEnabled()) {
          LOG.trace(this + ": " + removedFrom +
              " no longer contains " + replica + ".  refCount " +
              (replica.refCount - 1) + " -> " + replica.refCount +
              StringUtils.getStackTrace(Thread.currentThread()));

        }
      } else if (LOG.isTraceEnabled()) {
        LOG.trace(this + ": replica  refCount " +
            (replica.refCount - 1) + " -> " + replica.refCount +
            StringUtils.getStackTrace(Thread.currentThread()));
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Unreference a replica.
   *
   * You must hold the cache lock while calling this function.
   *
   * @param replica   The replica being unreferenced.
   */
  void unref(ShortCircuitReplica replica) {
    lock.lock();
    try {
      // If the replica is stale or unusable, but we haven't purged it yet,
      // let's do that.  It would be a shame to evict a non-stale replica so
      // that we could put a stale or unusable one into the cache.
      if (!replica.purged) {
        String purgeReason = null;
        if (!replica.getDataStream().getChannel().isOpen()) {
          purgeReason = "purging replica because its data channel is closed.";
        } else if (!replica.getMetaStream().getChannel().isOpen()) {
          purgeReason = "purging replica because its meta channel is closed.";
        } else if (replica.isStale()) {
          purgeReason = "purging replica because it is stale.";
        }
        if (purgeReason != null) {
          LOG.debug(this + ": " + purgeReason);
          purge(replica);
        }
      }
      String addedString = "";
      boolean shouldTrimEvictionMaps = false;
      int newRefCount = --replica.refCount;
      if (newRefCount == 0) {
        // Close replica, since there are no remaining references to it.
        Preconditions.checkArgument(replica.purged,
            "Replica " + replica + " reached a refCount of 0 without " +
            "being purged");
        replica.close();
      } else if (newRefCount == 1) {
        Preconditions.checkState(null == replica.getEvictableTimeNs(),
            "Replica " + replica + " had a refCount higher than 1, " +
              "but was still evictable (evictableTimeNs = " +
                replica.getEvictableTimeNs() + ")");
        if (!replica.purged) {
          // Add the replica to the end of an eviction list.
          // Eviction lists are sorted by time.
          if (replica.hasMmap()) {
            insertEvictable(System.nanoTime(), replica, evictableMmapped);
            addedString = "added to evictableMmapped, ";
          } else {
            insertEvictable(System.nanoTime(), replica, evictable);
            addedString = "added to evictable, ";
          }
          shouldTrimEvictionMaps = true;
        }
      } else {
        Preconditions.checkArgument(replica.refCount >= 0,
            "replica's refCount went negative (refCount = " +
            replica.refCount + " for " + replica + ")");
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace(this + ": unref replica " + replica +
            ": " + addedString + " refCount " +
            (newRefCount + 1) + " -> " + newRefCount +
            StringUtils.getStackTrace(Thread.currentThread()));
      }
      if (shouldTrimEvictionMaps) {
        trimEvictionMaps();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Demote old evictable mmaps into the regular eviction map.
   *
   * You must hold the cache lock while calling this function.
   *
   * @param now   Current time in monotonic milliseconds.
   * @return      Number of replicas demoted.
   */
  private int demoteOldEvictableMmaped(long now) {
    int numDemoted = 0;
    boolean needMoreSpace = false;
    Long evictionTimeNs = Long.valueOf(0);

    while (true) {
      Entry<Long, ShortCircuitReplica> entry = 
          evictableMmapped.ceilingEntry(evictionTimeNs);
      if (entry == null) break;
      evictionTimeNs = entry.getKey();
      long evictionTimeMs = 
          TimeUnit.MILLISECONDS.convert(evictionTimeNs, TimeUnit.NANOSECONDS);
      if (evictionTimeMs + maxEvictableMmapedLifespanMs >= now) {
        if (evictableMmapped.size() < maxEvictableMmapedSize) {
          break;
        }
        needMoreSpace = true;
      }
      ShortCircuitReplica replica = entry.getValue();
      if (LOG.isTraceEnabled()) {
        String rationale = needMoreSpace ? "because we need more space" : 
            "because it's too old";
        LOG.trace("demoteOldEvictable: demoting " + replica + ": " +
            rationale + ": " +
            StringUtils.getStackTrace(Thread.currentThread()));
      }
      removeEvictable(replica, evictableMmapped);
      munmap(replica);
      insertEvictable(evictionTimeNs, replica, evictable);
      numDemoted++;
    }
    return numDemoted;
  }

  /**
   * Trim the eviction lists.
   */
  private void trimEvictionMaps() {
    long now = Time.monotonicNow();
    demoteOldEvictableMmaped(now);

    while (true) {
      long evictableSize = evictable.size();
      long evictableMmappedSize = evictableMmapped.size();
      if (evictableSize + evictableMmappedSize <= maxTotalSize) {
        return;
      }
      ShortCircuitReplica replica;
      if (evictableSize == 0) {
       replica = evictableMmapped.firstEntry().getValue();
      } else {
       replica = evictable.firstEntry().getValue();
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace(this + ": trimEvictionMaps is purging " + replica +
          StringUtils.getStackTrace(Thread.currentThread()));
      }
      purge(replica);
    }
  }

  /**
   * Munmap a replica, updating outstandingMmapCount.
   *
   * @param replica  The replica to munmap.
   */
  private void munmap(ShortCircuitReplica replica) {
    replica.munmap();
    outstandingMmapCount--;
  }

  /**
   * Remove a replica from an evictable map.
   *
   * @param replica   The replica to remove.
   * @return          The map it was removed from.
   */
  private String removeEvictable(ShortCircuitReplica replica) {
    if (replica.hasMmap()) {
      removeEvictable(replica, evictableMmapped);
      return "evictableMmapped";
    } else {
      removeEvictable(replica, evictable);
      return "evictable";
    }
  }

  /**
   * Remove a replica from an evictable map.
   *
   * @param replica   The replica to remove.
   * @param map       The map to remove it from.
   */
  private void removeEvictable(ShortCircuitReplica replica,
      TreeMap<Long, ShortCircuitReplica> map) {
    Long evictableTimeNs = replica.getEvictableTimeNs();
    Preconditions.checkNotNull(evictableTimeNs);
    ShortCircuitReplica removed = map.remove(evictableTimeNs);
    Preconditions.checkState(removed == replica,
        "failed to make " + replica + " unevictable");
    replica.setEvictableTimeNs(null);
  }

  /**
   * Insert a replica into an evictable map.
   *
   * If an element already exists with this eviction time, we add a nanosecond
   * to it until we find an unused key.
   *
   * @param evictionTimeNs   The eviction time in absolute nanoseconds.
   * @param replica          The replica to insert.
   * @param map              The map to insert it into.
   */
  private void insertEvictable(Long evictionTimeNs,
      ShortCircuitReplica replica, TreeMap<Long, ShortCircuitReplica> map) {
    while (map.containsKey(evictionTimeNs)) {
      evictionTimeNs++;
    }
    Preconditions.checkState(null == replica.getEvictableTimeNs());
    replica.setEvictableTimeNs(evictionTimeNs);
    map.put(evictionTimeNs, replica);
  }

  /**
   * Purge a replica from the cache.
   *
   * This doesn't necessarily close the replica, since there may be
   * outstanding references to it.  However, it does mean the cache won't
   * hand it out to anyone after this.
   *
   * You must hold the cache lock while calling this function.
   *
   * @param replica   The replica being removed.
   */
  private void purge(ShortCircuitReplica replica) {
    boolean removedFromInfoMap = false;
    String evictionMapName = null;
    Preconditions.checkArgument(!replica.purged);
    replica.purged = true;
    Waitable<ShortCircuitReplicaInfo> val = replicaInfoMap.get(replica.key);
    if (val != null) {
      ShortCircuitReplicaInfo info = val.getVal();
      if ((info != null) && (info.getReplica() == replica)) {
        replicaInfoMap.remove(replica.key);
        removedFromInfoMap = true;
      }
    }
    Long evictableTimeNs = replica.getEvictableTimeNs();
    if (evictableTimeNs != null) {
      evictionMapName = removeEvictable(replica);
    }
    if (LOG.isTraceEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append(this).append(": ").append(": purged ").
          append(replica).append(" from the cache.");
      if (removedFromInfoMap) {
        builder.append("  Removed from the replicaInfoMap.");
      }
      if (evictionMapName != null) {
        builder.append("  Removed from ").append(evictionMapName);
      }
      LOG.trace(builder.toString());
    }
    unref(replica);
  }

  /**
   * Fetch or create a replica.
   *
   * You must hold the cache lock while calling this function.
   *
   * @param key          Key to use for lookup.
   * @param creator      Replica creator callback.  Will be called without
   *                     the cache lock being held.
   *
   * @return             Null if no replica could be found or created.
   *                     The replica, otherwise.
   */
  public ShortCircuitReplicaInfo fetchOrCreate(ExtendedBlockId key,
      ShortCircuitReplicaCreator creator) {
    Waitable<ShortCircuitReplicaInfo> newWaitable = null;
    lock.lock();
    try {
      ShortCircuitReplicaInfo info = null;
      do {
        if (closed) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(this + ": can't fetchOrCreate " + key +
                " because the cache is closed.");
          }
          return null;
        }
        Waitable<ShortCircuitReplicaInfo> waitable = replicaInfoMap.get(key);
        if (waitable != null) {
          try {
            info = fetch(key, waitable);
          } catch (RetriableException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(this + ": retrying " + e.getMessage());
            }
            continue;
          }
        }
      } while (false);
      if (info != null) return info;
      // We need to load the replica ourselves.
      newWaitable = new Waitable<ShortCircuitReplicaInfo>(lock.newCondition());
      replicaInfoMap.put(key, newWaitable);
    } finally {
      lock.unlock();
    }
    return create(key, creator, newWaitable);
  }

  /**
   * Fetch an existing ReplicaInfo object.
   *
   * @param key       The key that we're using.
   * @param waitable  The waitable object to wait on.
   * @return          The existing ReplicaInfo object, or null if there is
   *                  none.
   *
   * @throws RetriableException   If the caller needs to retry.
   */
  private ShortCircuitReplicaInfo fetch(ExtendedBlockId key,
      Waitable<ShortCircuitReplicaInfo> waitable) throws RetriableException {
    // Another thread is already in the process of loading this
    // ShortCircuitReplica.  So we simply wait for it to complete.
    ShortCircuitReplicaInfo info;
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace(this + ": found waitable for " + key);
      }
      info = waitable.await();
    } catch (InterruptedException e) {
      LOG.info(this + ": interrupted while waiting for " + key);
      Thread.currentThread().interrupt();
      throw new RetriableException("interrupted");
    }
    if (info.getInvalidTokenException() != null) {
      LOG.warn(this + ": could not get " + key + " due to InvalidToken " +
            "exception.", info.getInvalidTokenException());
      return info;
    }
    ShortCircuitReplica replica = info.getReplica();
    if (replica == null) {
      LOG.warn(this + ": failed to get " + key);
      return info;
    }
    if (replica.purged) {
      // Ignore replicas that have already been purged from the cache.
      throw new RetriableException("Ignoring purged replica " +
          replica + ".  Retrying.");
    }
    // Check if the replica is stale before using it.
    // If it is, purge it and retry.
    if (replica.isStale()) {
      LOG.info(this + ": got stale replica " + replica + ".  Removing " +
          "this replica from the replicaInfoMap and retrying.");
      // Remove the cache's reference to the replica.  This may or may not
      // trigger a close.
      purge(replica);
      throw new RetriableException("ignoring stale replica " + replica);
    }
    ref(replica);
    return info;
  }

  private ShortCircuitReplicaInfo create(ExtendedBlockId key,
      ShortCircuitReplicaCreator creator,
      Waitable<ShortCircuitReplicaInfo> newWaitable) {
    // Handle loading a new replica.
    ShortCircuitReplicaInfo info = null;
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace(this + ": loading " + key);
      }
      info = creator.createShortCircuitReplicaInfo();
    } catch (RuntimeException e) {
      LOG.warn(this + ": failed to load " + key, e);
    }
    if (info == null) info = new ShortCircuitReplicaInfo();
    lock.lock();
    try {
      if (info.getReplica() != null) {
        // On success, make sure the cache cleaner thread is running.
        if (LOG.isTraceEnabled()) {
          LOG.trace(this + ": successfully loaded " + info.getReplica());
        }
        startCacheCleanerThreadIfNeeded();
        // Note: new ShortCircuitReplicas start with a refCount of 2,
        // indicating that both this cache and whoever requested the 
        // creation of the replica hold a reference.  So we don't need
        // to increment the reference count here.
      } else {
        // On failure, remove the waitable from the replicaInfoMap.
        Waitable<ShortCircuitReplicaInfo> waitableInMap = replicaInfoMap.get(key);
        if (waitableInMap == newWaitable) replicaInfoMap.remove(key);
        if (info.getInvalidTokenException() != null) {
          LOG.warn(this + ": could not load " + key + " due to InvalidToken " +
              "exception.", info.getInvalidTokenException());
        } else {
          LOG.warn(this + ": failed to load " + key);
        }
      }
      newWaitable.provide(info);
    } finally {
      lock.unlock();
    }
    return info;
  }

  private void startCacheCleanerThreadIfNeeded() {
    if (cacheCleaner == null) {
      cacheCleaner = new CacheCleaner();
      long rateMs = cacheCleaner.getRateInMs();
      ScheduledFuture<?> future =
          cleanerExecutor.scheduleAtFixedRate(cacheCleaner, rateMs, rateMs,
              TimeUnit.MILLISECONDS);
      cacheCleaner.setFuture(future);
      if (LOG.isDebugEnabled()) {
        LOG.debug(this + ": starting cache cleaner thread which will run " +
          "every " + rateMs + " ms");
      }
    }
  }

  ClientMmap getOrCreateClientMmap(ShortCircuitReplica replica,
      boolean anchored) {
    Condition newCond;
    lock.lock();
    try {
      while (replica.mmapData != null) {
        if (replica.mmapData instanceof MappedByteBuffer) {
          ref(replica);
          MappedByteBuffer mmap = (MappedByteBuffer)replica.mmapData;
          return new ClientMmap(replica, mmap, anchored);
        } else if (replica.mmapData instanceof Long) {
          long lastAttemptTimeMs = (Long)replica.mmapData;
          long delta = Time.monotonicNow() - lastAttemptTimeMs;
          if (delta < mmapRetryTimeoutMs) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(this + ": can't create client mmap for " +
                  replica + " because we failed to " +
                  "create one just " + delta + "ms ago.");
            }
            return null;
          }
          if (LOG.isTraceEnabled()) {
            LOG.trace(this + ": retrying client mmap for " + replica +
                ", " + delta + " ms after the previous failure.");
          }
        } else if (replica.mmapData instanceof Condition) {
          Condition cond = (Condition)replica.mmapData;
          cond.awaitUninterruptibly();
        } else {
          Preconditions.checkState(false, "invalid mmapData type " +
              replica.mmapData.getClass().getName());
        }
      }
      newCond = lock.newCondition();
      replica.mmapData = newCond;
    } finally {
      lock.unlock();
    }
    MappedByteBuffer map = replica.loadMmapInternal();
    lock.lock();
    try {
      if (map == null) {
        replica.mmapData = Long.valueOf(Time.monotonicNow());
        newCond.signalAll();
        return null;
      } else {
        outstandingMmapCount++;
        replica.mmapData = map;
        ref(replica);
        newCond.signalAll();
        return new ClientMmap(replica, map, anchored);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Close the cache and free all associated resources.
   */
  @Override
  public void close() {
    try {
      lock.lock();
      if (closed) return;
      closed = true;
      LOG.info(this + ": closing");
      maxNonMmappedEvictableLifespanMs = 0;
      maxEvictableMmapedSize = 0;
      // Close and join cacheCleaner thread.
      IOUtils.cleanup(LOG, cacheCleaner);
      // Purge all replicas.
      while (true) {
        Entry<Long, ShortCircuitReplica> entry = evictable.firstEntry();
        if (entry == null) break;
        purge(entry.getValue());
      }
      while (true) {
        Entry<Long, ShortCircuitReplica> entry = evictableMmapped.firstEntry();
        if (entry == null) break;
        purge(entry.getValue());
      }
    } finally {
      lock.unlock();
    }
    IOUtils.cleanup(LOG, shmManager);
  }

  @VisibleForTesting // ONLY for testing
  public interface CacheVisitor {
    void visit(int numOutstandingMmaps,
        Map<ExtendedBlockId, ShortCircuitReplica> replicas,
        Map<ExtendedBlockId, InvalidToken> failedLoads,
        Map<Long, ShortCircuitReplica> evictable,
        Map<Long, ShortCircuitReplica> evictableMmapped);
  }

  @VisibleForTesting // ONLY for testing
  public void accept(CacheVisitor visitor) {
    lock.lock();
    try {
      Map<ExtendedBlockId, ShortCircuitReplica> replicas =
          new HashMap<ExtendedBlockId, ShortCircuitReplica>();
      Map<ExtendedBlockId, InvalidToken> failedLoads =
          new HashMap<ExtendedBlockId, InvalidToken>();
      for (Entry<ExtendedBlockId, Waitable<ShortCircuitReplicaInfo>> entry :
            replicaInfoMap.entrySet()) {
        Waitable<ShortCircuitReplicaInfo> waitable = entry.getValue();
        if (waitable.hasVal()) {
          if (waitable.getVal().getReplica() != null) {
            replicas.put(entry.getKey(), waitable.getVal().getReplica());
          } else {
            // The exception may be null here, indicating a failed load that
            // isn't the result of an invalid block token.
            failedLoads.put(entry.getKey(),
                waitable.getVal().getInvalidTokenException());
          }
        }
      }
      if (LOG.isDebugEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder.append("visiting ").append(visitor.getClass().getName()).
            append("with outstandingMmapCount=").append(outstandingMmapCount).
            append(", replicas=");
        String prefix = "";
        for (Entry<ExtendedBlockId, ShortCircuitReplica> entry : replicas.entrySet()) {
          builder.append(prefix).append(entry.getValue());
          prefix = ",";
        }
        prefix = "";
        builder.append(", failedLoads=");
        for (Entry<ExtendedBlockId, InvalidToken> entry : failedLoads.entrySet()) {
          builder.append(prefix).append(entry.getValue());
          prefix = ",";
        }
        prefix = "";
        builder.append(", evictable=");
        for (Entry<Long, ShortCircuitReplica> entry : evictable.entrySet()) {
          builder.append(prefix).append(entry.getKey()).
              append(":").append(entry.getValue());
          prefix = ",";
        }
        prefix = "";
        builder.append(", evictableMmapped=");
        for (Entry<Long, ShortCircuitReplica> entry : evictableMmapped.entrySet()) {
          builder.append(prefix).append(entry.getKey()).
              append(":").append(entry.getValue());
          prefix = ",";
        }
        LOG.debug(builder.toString());
      }
      visitor.visit(outstandingMmapCount, replicas, failedLoads,
            evictable, evictableMmapped);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    return "ShortCircuitCache(0x" +
        Integer.toHexString(System.identityHashCode(this)) + ")";
  }

  /**
   * Allocate a new shared memory slot.
   *
   * @param datanode       The datanode to allocate a shm slot with.
   * @param peer           A peer connected to the datanode.
   * @param usedPeer       Will be set to true if we use up the provided peer.
   * @param blockId        The block id and block pool id of the block we're 
   *                         allocating this slot for.
   * @param clientName     The name of the DFSClient allocating the shared
   *                         memory.
   * @return               Null if short-circuit shared memory is disabled;
   *                         a short-circuit memory slot otherwise.
   * @throws IOException   An exception if there was an error talking to 
   *                         the datanode.
   */
  public Slot allocShmSlot(DatanodeInfo datanode,
        DomainPeer peer, MutableBoolean usedPeer,
        ExtendedBlockId blockId, String clientName) throws IOException {
    if (shmManager != null) {
      return shmManager.allocSlot(datanode, peer, usedPeer,
          blockId, clientName);
    } else {
      return null;
    }
  }

  /**
   * Free a slot immediately.
   *
   * ONLY use this if the DataNode is not yet aware of the slot.
   * 
   * @param slot           The slot to free.
   */
  public void freeSlot(Slot slot) {
    Preconditions.checkState(shmManager != null);
    slot.makeInvalid();
    shmManager.freeSlot(slot);
  }
  
  /**
   * Schedule a shared memory slot to be released.
   *
   * @param slot           The slot to release.
   */
  public void scheduleSlotReleaser(Slot slot) {
    Preconditions.checkState(shmManager != null);
    releaserExecutor.execute(new SlotReleaser(slot));
  }

  @VisibleForTesting
  public DfsClientShmManager getDfsClientShmManager() {
    return shmManager;
  }
}
