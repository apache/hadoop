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
package org.apache.hadoop.hdfs.client;

import java.io.Closeable;

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_SIZE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_THREAD_RUNS_PER_TIMEOUT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_THREAD_RUNS_PER_TIMEOUT_DEFAULT;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.IOUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Tracks mmap instances used on an HDFS client.
 *
 * mmaps can be used concurrently by multiple threads at once.
 * mmaps cannot be closed while they are in use.
 *
 * The cache is important for performance, because the first time an mmap is
 * created, the page table entries (PTEs) are not yet set up.
 * Even when reading data that is entirely resident in memory, reading an
 * mmap the second time is faster.
 */
@InterfaceAudience.Private
public class ClientMmapManager implements Closeable {
  public static final Log LOG = LogFactory.getLog(ClientMmapManager.class);

  private boolean closed = false;

  private final int cacheSize;

  private final long timeoutNs;

  private final int runsPerTimeout;

  private final Lock lock = new ReentrantLock();
  
  /**
   * Maps block, datanode_id to the client mmap object.
   * If the ClientMmap is in the process of being loaded,
   * {@link Waitable<ClientMmap>#await()} will block.
   *
   * Protected by the ClientMmapManager lock.
   */
  private final TreeMap<Key, Waitable<ClientMmap>> mmaps =
      new TreeMap<Key, Waitable<ClientMmap>>();

  /**
   * Maps the last use time to the client mmap object.
   * We ensure that each last use time is unique by inserting a jitter of a
   * nanosecond or two if necessary.
   * 
   * Protected by the ClientMmapManager lock.
   * ClientMmap objects that are in use are never evictable.
   */
  private final TreeMap<Long, ClientMmap> evictable =
      new TreeMap<Long, ClientMmap>();

  private final ScheduledThreadPoolExecutor executor = 
      new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().
          setDaemon(true).setNameFormat("ClientMmapManager").
          build());
  
  /**
   * The CacheCleaner for this ClientMmapManager.  We don't create this
   * and schedule it until it becomes necessary.
   */
  private CacheCleaner cacheCleaner;

  /**
   * Factory method to create a ClientMmapManager from a Hadoop
   * configuration.
   */
  public static ClientMmapManager fromConf(Configuration conf) {
    return new ClientMmapManager(conf.getInt(DFS_CLIENT_MMAP_CACHE_SIZE,
      DFS_CLIENT_MMAP_CACHE_SIZE_DEFAULT),
      conf.getLong(DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS,
        DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS_DEFAULT),
      conf.getInt(DFS_CLIENT_MMAP_CACHE_THREAD_RUNS_PER_TIMEOUT,
        DFS_CLIENT_MMAP_CACHE_THREAD_RUNS_PER_TIMEOUT_DEFAULT));
  }

  public ClientMmapManager(int cacheSize, long timeoutMs, int runsPerTimeout) {
    this.cacheSize = cacheSize;
    this.timeoutNs = timeoutMs * 1000000;
    this.runsPerTimeout = runsPerTimeout;
  }
  
  long getTimeoutMs() {
    return this.timeoutNs / 1000000;
  }

  int getRunsPerTimeout() {
    return this.runsPerTimeout;
  }
  
  public String verifyConfigurationMatches(Configuration conf) {
    StringBuilder bld = new StringBuilder();
    int cacheSize = conf.getInt(DFS_CLIENT_MMAP_CACHE_SIZE,
                    DFS_CLIENT_MMAP_CACHE_SIZE_DEFAULT);
    if (this.cacheSize != cacheSize) {
      bld.append("You specified a cache size of ").append(cacheSize).
          append(", but the existing cache size is ").append(this.cacheSize).
          append(".  ");
    }
    long timeoutMs = conf.getLong(DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS,
        DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS_DEFAULT);
    if (getTimeoutMs() != timeoutMs) {
      bld.append("You specified a cache timeout of ").append(timeoutMs).
          append(" ms, but the existing cache timeout is ").
          append(getTimeoutMs()).append("ms").append(".  ");
    }
    int runsPerTimeout = conf.getInt(
        DFS_CLIENT_MMAP_CACHE_THREAD_RUNS_PER_TIMEOUT,
        DFS_CLIENT_MMAP_CACHE_THREAD_RUNS_PER_TIMEOUT_DEFAULT);
    if (getRunsPerTimeout() != runsPerTimeout) {
      bld.append("You specified ").append(runsPerTimeout).
          append(" runs per timeout, but the existing runs per timeout is ").
          append(getTimeoutMs()).append(".  ");
    }
    return bld.toString();
  }

  private static class Waitable<T> {
    private T val;
    private final Condition cond;

    public Waitable(Condition cond) {
      this.val = null;
      this.cond = cond;
    }

    public T await() throws InterruptedException {
      while (this.val == null) {
        this.cond.await();
      }
      return this.val;
    }

    public void provide(T val) {
      this.val = val;
      this.cond.signalAll();
    }
  }

  private static class Key implements Comparable<Key> {
    private final ExtendedBlock block;
    private final DatanodeID datanode;
    
    Key(ExtendedBlock block, DatanodeID datanode) {
      this.block = block;
      this.datanode = datanode;
    }

    /**
     * Compare two ClientMmap regions that we're storing.
     *
     * When we append to a block, we bump the genstamp.  It is important to 
     * compare the genStamp here.  That way, we will not return a shorter 
     * mmap than required.
     */
    @Override
    public int compareTo(Key o) {
      return ComparisonChain.start().
          compare(block.getBlockId(), o.block.getBlockId()).
          compare(block.getGenerationStamp(), o.block.getGenerationStamp()).
          compare(block.getBlockPoolId(), o.block.getBlockPoolId()).
          compare(datanode, o.datanode).
          result();
    }

    @Override
    public boolean equals(Object rhs) {
      if (rhs == null) {
        return false;
      }
      try {
        Key o = (Key)rhs;
        return (compareTo(o) == 0);
      } catch (ClassCastException e) {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return block.hashCode() ^ datanode.hashCode();
    }
  }

  /**
   * Thread which handles expiring mmaps from the cache.
   */
  private static class CacheCleaner implements Runnable, Closeable {
    private WeakReference<ClientMmapManager> managerRef;
    private ScheduledFuture<?> future;
    
    CacheCleaner(ClientMmapManager manager) {
      this.managerRef= new WeakReference<ClientMmapManager>(manager);
    }

    @Override
    public void run() {
      ClientMmapManager manager = managerRef.get();
      if (manager == null) return;
      long curTime = System.nanoTime();
      try {
        manager.lock.lock();
        manager.evictStaleEntries(curTime);
      } finally {
        manager.lock.unlock();
      }
    }
    
    void setFuture(ScheduledFuture<?> future) {
      this.future = future;
    }

    @Override
    public void close() throws IOException {
      future.cancel(false);
    }
  }

  /**
   * Evict entries which are older than curTime + timeoutNs from the cache.
   *
   * NOTE: you must call this function with the lock held.
   */
  private void evictStaleEntries(long curTime) {
    if (closed) {
      return;
    }
    Iterator<Entry<Long, ClientMmap>> iter =
        evictable.entrySet().iterator(); 
    while (iter.hasNext()) {
      Entry<Long, ClientMmap> entry = iter.next();
      if (entry.getKey() + timeoutNs >= curTime) {
        return;
      }
      ClientMmap mmap = entry.getValue();
      Key key = new Key(mmap.getBlock(), mmap.getDatanodeID());
      mmaps.remove(key);
      iter.remove();
      mmap.unmap();
    }
  }

  /**
   * Evict one mmap object from the cache.
   *
   * NOTE: you must call this function with the lock held.
   *
   * @return                  True if an object was evicted; false if none
   *                          could be evicted.
   */
  private boolean evictOne() {
    Entry<Long, ClientMmap> entry = evictable.pollFirstEntry();
    if (entry == null) {
      // We don't want to try creating another mmap region, because the
      // cache is full.
      return false;
    }
    ClientMmap evictedMmap = entry.getValue(); 
    Key evictedKey = new Key(evictedMmap.getBlock(), 
                             evictedMmap.getDatanodeID());
    mmaps.remove(evictedKey);
    evictedMmap.unmap();
    return true;
  }

  /**
   * Create a new mmap object.
   * 
   * NOTE: you must call this function with the lock held.
   *
   * @param key              The key which describes this mmap.
   * @param in               The input stream to use to create the mmap.
   * @return                 The new mmap object, or null if there were
   *                         insufficient resources.
   * @throws IOException     If there was an I/O error creating the mmap.
   */
  private ClientMmap create(Key key, FileInputStream in) throws IOException {
    if (mmaps.size() + 1 > cacheSize) {
      if (!evictOne()) {
        LOG.warn("mmap cache is full (with " + cacheSize + " elements) and " +
              "nothing is evictable.  Ignoring request for mmap with " +
              "datanodeID=" + key.datanode + ", " + "block=" + key.block);
        return null;
      }
    }
    // Create the condition variable that other threads may wait on.
    Waitable<ClientMmap> waitable =
        new Waitable<ClientMmap>(lock.newCondition());
    mmaps.put(key, waitable);
    // Load the entry
    boolean success = false;
    ClientMmap mmap = null;
    try {
      try {
        lock.unlock();
        mmap = ClientMmap.load(this, in, key.block, key.datanode);
      } finally {
        lock.lock();
      }
      if (cacheCleaner == null) {
        cacheCleaner = new CacheCleaner(this);
        ScheduledFuture<?> future = 
            executor.scheduleAtFixedRate(cacheCleaner,
                timeoutNs, timeoutNs / runsPerTimeout, TimeUnit.NANOSECONDS);
        cacheCleaner.setFuture(future);
      }
      success = true;
    } finally {
      if (!success) {
        LOG.warn("failed to create mmap for datanodeID=" + key.datanode +
                  ", " + "block=" + key.block);
        mmaps.remove(key);
      }
      waitable.provide(mmap);
    }
    if (LOG.isDebugEnabled()) {
      LOG.info("created a new ClientMmap for block " + key.block +
          " on datanode " + key.datanode);
    }
    return mmap;
  }

  /**
   * Get or create an mmap region.
   * 
   * @param node       The DataNode that owns the block for this mmap region.
   * @param block      The block ID, block pool ID, and generation stamp of 
   *                     the block we want to read.
   * @param in         An open file for this block.  This stream is only used
   *                     if we have to create a new mmap; if we use an
   *                     existing one, it is ignored.
   *
   * @return           The client mmap region.
   */
  public ClientMmap fetch(DatanodeID datanodeID, ExtendedBlock block,
      FileInputStream in) throws IOException, InterruptedException {
    LOG.debug("fetching mmap with datanodeID=" + datanodeID + ", " +
        "block=" + block);
    Key key = new Key(block, datanodeID);
    ClientMmap mmap = null;
    try {
      lock.lock();
      if (closed) {
        throw new IOException("ClientMmapManager is closed.");
      }
      while (mmap == null) {
        Waitable<ClientMmap> entry = mmaps.get(key);
        if (entry == null) {
          return create(key, in);
        }
        mmap = entry.await();
      }
      if (mmap.ref() == 1) {
        // When going from nobody using the mmap (ref = 0) to somebody
        // using the mmap (ref = 1), we must make the mmap un-evictable.
        evictable.remove(mmap.getLastEvictableTimeNs());
      }
    }
    finally {
      lock.unlock();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("reusing existing mmap with datanodeID=" + datanodeID +
              ", " + "block=" + block);
    }
    return mmap;
  }

  /**
   * Make an mmap evictable.
   * 
   * When an mmap is evictable, it may be removed from the cache if necessary.
   * mmaps can only be evictable if nobody is using them.
   *
   * @param mmap             The mmap to make evictable.
   */
  void makeEvictable(ClientMmap mmap) {
    try {
      lock.lock();
      if (closed) {
        // If this ClientMmapManager is closed, then don't bother with the
        // cache; just close the mmap.
        mmap.unmap();
        return;
      }
      long now = System.nanoTime();
      while (evictable.containsKey(now)) {
        now++;
      }
      mmap.setLastEvictableTimeNs(now);
      evictable.put(now, mmap);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      lock.lock();
      closed = true;
      IOUtils.cleanup(LOG, cacheCleaner);

      // Unmap all the mmaps that nobody is using.
      // The ones which are in use will be unmapped just as soon as people stop
      // using them.
      evictStaleEntries(Long.MAX_VALUE);

      executor.shutdown();
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  public interface ClientMmapVisitor {
    void accept(ClientMmap mmap);
  }

  @VisibleForTesting
  public synchronized void visitMmaps(ClientMmapVisitor visitor)
      throws InterruptedException {
    for (Waitable<ClientMmap> entry : mmaps.values()) {
      visitor.accept(entry.await());
    }
  }

  public void visitEvictable(ClientMmapVisitor visitor)
      throws InterruptedException {
    for (ClientMmap mmap : evictable.values()) {
      visitor.accept(mmap);
    }
  }
}
