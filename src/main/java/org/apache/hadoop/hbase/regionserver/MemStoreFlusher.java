/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ConcurrentModificationException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread that flushes cache on request
 *
 * NOTE: This class extends Thread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 *
 * @see FlushRequester
 */
class MemStoreFlusher extends Thread implements FlushRequester {
  static final Log LOG = LogFactory.getLog(MemStoreFlusher.class);
  // These two data members go together.  Any entry in the one must have
  // a corresponding entry in the other.
  private final BlockingQueue<FlushQueueEntry> flushQueue =
    new DelayQueue<FlushQueueEntry>();
  private final Map<HRegion, FlushRegionEntry> regionsInQueue =
    new HashMap<HRegion, FlushRegionEntry>();
  private AtomicBoolean wakeupPending = new AtomicBoolean();

  private final long threadWakeFrequency;
  private final HRegionServer server;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition flushOccurred = lock.newCondition();

  protected final long globalMemStoreLimit;
  protected final long globalMemStoreLimitLowMark;

  private static final float DEFAULT_UPPER = 0.4f;
  private static final float DEFAULT_LOWER = 0.35f;
  private static final String UPPER_KEY =
    "hbase.regionserver.global.memstore.upperLimit";
  private static final String LOWER_KEY =
    "hbase.regionserver.global.memstore.lowerLimit";
  private long blockingStoreFilesNumber;
  private long blockingWaitTime;

  /**
   * @param conf
   * @param server
   */
  public MemStoreFlusher(final Configuration conf,
      final HRegionServer server) {
    super();
    this.server = server;
    this.threadWakeFrequency =
      conf.getLong(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
    long max = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    this.globalMemStoreLimit = globalMemStoreLimit(max, DEFAULT_UPPER,
      UPPER_KEY, conf);
    long lower = globalMemStoreLimit(max, DEFAULT_LOWER, LOWER_KEY, conf);
    if (lower > this.globalMemStoreLimit) {
      lower = this.globalMemStoreLimit;
      LOG.info("Setting globalMemStoreLimitLowMark == globalMemStoreLimit " +
        "because supplied " + LOWER_KEY + " was > " + UPPER_KEY);
    }
    this.globalMemStoreLimitLowMark = lower;
    this.blockingStoreFilesNumber =
      conf.getInt("hbase.hstore.blockingStoreFiles", 7);
    if (this.blockingStoreFilesNumber == -1) {
      this.blockingStoreFilesNumber = 1 +
        conf.getInt("hbase.hstore.compactionThreshold", 3);
    }
    this.blockingWaitTime = conf.getInt("hbase.hstore.blockingWaitTime",
      90000);
    LOG.info("globalMemStoreLimit=" +
      StringUtils.humanReadableInt(this.globalMemStoreLimit) +
      ", globalMemStoreLimitLowMark=" +
      StringUtils.humanReadableInt(this.globalMemStoreLimitLowMark) +
      ", maxHeap=" + StringUtils.humanReadableInt(max));
  }

  /**
   * Calculate size using passed <code>key</code> for configured
   * percentage of <code>max</code>.
   * @param max
   * @param defaultLimit
   * @param key
   * @param c
   * @return Limit.
   */
  static long globalMemStoreLimit(final long max,
     final float defaultLimit, final String key, final Configuration c) {
    float limit = c.getFloat(key, defaultLimit);
    return getMemStoreLimit(max, limit, defaultLimit);
  }

  static long getMemStoreLimit(final long max, final float limit,
      final float defaultLimit) {
    if (limit >= 0.9f || limit < 0.1f) {
      LOG.warn("Setting global memstore limit to default of " + defaultLimit +
        " because supplied value outside allowed range of 0.1 -> 0.9");
    }
    return (long)(max * limit);
  }
  
  /**
   * The memstore across all regions has exceeded the low water mark. Pick
   * one region to flush and flush it synchronously (this is called from the
   * flush thread)
   * @return true if successful
   */
  private boolean flushOneForGlobalPressure() {
    SortedMap<Long, HRegion> regionsBySize =
        server.getCopyOfOnlineRegionsSortedBySize();
    // Find the biggest region that doesn't have too many storefiles
    HRegion bestFlushableRegion = getBiggestMemstoreRegion(regionsBySize, true);
    // Find the biggest region, total, even if it might have too many flushes.
    HRegion bestAnyRegion = getBiggestMemstoreRegion(regionsBySize, false);

    Preconditions.checkState(bestAnyRegion != null,
        "Above memory mark but there are no regions!");
    
    HRegion regionToFlush;
    if (bestAnyRegion.memstoreSize.get() > 2 * bestFlushableRegion.memstoreSize.get()) {
      // Even if it's not supposed to be flushed, pick a region if it's more than twice
      // as big as the best flushable one - otherwise when we're under pressure we make
      // lots of little flushes and cause lots of compactions, etc, which just makes
      // life worse!
      LOG.info("Under global heap pressure: " +
          "Region " + bestAnyRegion.getRegionNameAsString() + " has too many " +
          "store files, but is " +
          StringUtils.humanReadableInt(bestAnyRegion.memstoreSize.get()) +
          " vs best flushable region's " +
          StringUtils.humanReadableInt(bestFlushableRegion.memstoreSize.get()) +
          ". Choosing the bigger.");
      regionToFlush = bestAnyRegion;
    } else {
      regionToFlush = bestFlushableRegion;
    }
    
    Preconditions.checkState(regionToFlush.memstoreSize.get() > 0);
    
    LOG.info("Flush of region " + regionToFlush + " due to global heap pressure");
    return flushRegion(regionToFlush, true);
  }

  @Override
  public void run() {
    while (!this.server.isStopped()) {
      FlushQueueEntry fqe = null;
      try {
        wakeupPending.set(false); // allow someone to wake us up again
        fqe = flushQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
        if (fqe == null || fqe instanceof WakeupFlushThread) {
          if (isAboveLowWaterMark()) {
            LOG.info("Flush thread woke up with memory above low water.");
            flushOneForGlobalPressure();
            // Enqueue another one of these tokens so we'll wake up again
            wakeupFlushThread();            
          }
          continue;
        }
        FlushRegionEntry fre = (FlushRegionEntry)fqe;
        if (!flushRegion(fre)) {
          break;
        }
      } catch (InterruptedException ex) {
        continue;
      } catch (ConcurrentModificationException ex) {
        continue;
      } catch (Exception ex) {
        LOG.error("Cache flusher failed for entry " + fqe);
        if (!server.checkFileSystem()) {
          break;
        }
      }
    }
    this.regionsInQueue.clear();
    this.flushQueue.clear();
    LOG.info(getName() + " exiting");
  }

  private void wakeupFlushThread() {
    if (wakeupPending.compareAndSet(false, true)) {
      flushQueue.add(new WakeupFlushThread());
    }
  }

  private HRegion getBiggestMemstoreRegion(
      SortedMap<Long, HRegion> regionsBySize,
      boolean checkStoreFileCount) {
    synchronized (regionsInQueue) {
      for (HRegion region : regionsBySize.values()) {
        if (checkStoreFileCount && isTooManyStoreFiles(region)) {
          continue;
        }
        return region;
      }
    }
    return null;
  }
  
  /**
   * Return true if global memory usage is above the high watermark
   */
  private boolean isAboveHighWaterMark() {
    return server.getGlobalMemStoreSize() >= globalMemStoreLimit;
  }
  
  /**
   * Return true if we're above the high watermark
   */
  private boolean isAboveLowWaterMark() {
    return server.getGlobalMemStoreSize() >= globalMemStoreLimitLowMark;
  }

  public void requestFlush(HRegion r) {
    synchronized (regionsInQueue) {
      if (!regionsInQueue.containsKey(r)) {
        // This entry has no delay so it will be added at the top of the flush
        // queue.  It'll come out near immediately.
        FlushRegionEntry fqe = new FlushRegionEntry(r);
        this.regionsInQueue.put(r, fqe);
        this.flushQueue.add(fqe);
      }
    }
  }

  public int getFlushQueueSize() {
    return flushQueue.size();
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    lock.lock();
    try {
      this.interrupt();
    } finally {
      lock.unlock();
    }
  }

  /*
   * A flushRegion that checks store file count.  If too many, puts the flush
   * on delay queue to retry later.
   * @param fqe
   * @return true if the region was successfully flushed, false otherwise. If 
   * false, there will be accompanying log messages explaining why the log was
   * not flushed.
   */
  private boolean flushRegion(final FlushRegionEntry fqe) {
    HRegion region = fqe.region;
    if (!fqe.region.getRegionInfo().isMetaRegion() &&
        isTooManyStoreFiles(region)) {
      if (fqe.isMaximumWait(this.blockingWaitTime)) {
        LOG.info("Waited " + (System.currentTimeMillis() - fqe.createTime) +
          "ms on a compaction to clean up 'too many store files'; waited " +
          "long enough... proceeding with flush of " +
          region.getRegionNameAsString());
      } else {
        // If this is first time we've been put off, then emit a log message.
        if (fqe.getRequeueCount() <= 0) {
          // Note: We don't impose blockingStoreFiles constraint on meta regions
          LOG.warn("Region " + region.getRegionNameAsString() + " has too many " +
            "store files; delaying flush up to " + this.blockingWaitTime + "ms");
        }
        this.server.compactSplitThread.requestCompaction(region, getName());
        // Put back on the queue.  Have it come back out of the queue
        // after a delay of this.blockingWaitTime / 100 ms.
        this.flushQueue.add(fqe.requeue(this.blockingWaitTime / 100));
        // Tell a lie, it's not flushed but it's ok
        return true;
      }
    }
    return flushRegion(region, false);
  }

  /*
   * Flush a region.
   * @param region Region to flush.
   * @param emergencyFlush Set if we are being force flushed. If true the region
   * needs to be removed from the flush queue. If false, when we were called
   * from the main flusher run loop and we got the entry to flush by calling
   * poll on the flush queue (which removed it).
   *
   * @return true if the region was successfully flushed, false otherwise. If
   * false, there will be accompanying log messages explaining why the log was
   * not flushed.
   */
  private boolean flushRegion(final HRegion region, final boolean emergencyFlush) {
    synchronized (this.regionsInQueue) {
      FlushRegionEntry fqe = this.regionsInQueue.remove(region);
      if (fqe != null && emergencyFlush) {
        // Need to remove from region from delay queue.  When NOT an
        // emergencyFlush, then item was removed via a flushQueue.poll.
        flushQueue.remove(fqe);
     }
     lock.lock();
    }
    try {
      if (region.flushcache()) {
        server.compactSplitThread.requestCompaction(region, getName());
      }
      server.getMetrics().addFlush(region.getRecentFlushInfo());
    } catch (DroppedSnapshotException ex) {
      // Cache flush can fail in a few places. If it fails in a critical
      // section, we get a DroppedSnapshotException and a replay of hlog
      // is required. Currently the only way to do this is a restart of
      // the server. Abort because hdfs is probably bad (HBASE-644 is a case
      // where hdfs was bad but passed the hdfs check).
      server.abort("Replay of HLog required. Forcing server shutdown", ex);
      return false;
    } catch (IOException ex) {
      LOG.error("Cache flush failed" +
        (region != null ? (" for region " + Bytes.toString(region.getRegionName())) : ""),
        RemoteExceptionHandler.checkIOException(ex));
      if (!server.checkFileSystem()) {
        return false;
      }
    } finally {
      flushOccurred.signalAll();
      lock.unlock();
    }
    return true;
  }

  private boolean isTooManyStoreFiles(HRegion region) {
    for (Store hstore: region.stores.values()) {
      if (hstore.getStorefilesCount() > this.blockingStoreFilesNumber) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if the regionserver's memstore memory usage is greater than the
   * limit. If so, flush regions with the biggest memstores until we're down
   * to the lower limit. This method blocks callers until we're down to a safe
   * amount of memstore consumption.
   */
  public synchronized void reclaimMemStoreMemory() {
    if (isAboveHighWaterMark()) {
      lock.lock();
      try {
        while (isAboveHighWaterMark()) {
          wakeupFlushThread();
          flushOccurred.awaitUninterruptibly();
        }
      } finally {
        lock.unlock();
      }
    } else if (isAboveLowWaterMark()) {
      wakeupFlushThread();
    }
  }

  interface FlushQueueEntry extends Delayed {}
  
  /**
   * Token to insert into the flush queue that ensures that the flusher does not sleep
   */
  static class WakeupFlushThread implements FlushQueueEntry {
    @Override
    public long getDelay(TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(Delayed o) {
      return -1;
    }
  }
  
  /**
   * Datastructure used in the flush queue.  Holds region and retry count.
   * Keeps tabs on how old this object is.  Implements {@link Delayed}.  On
   * construction, the delay is zero. When added to a delay queue, we'll come
   * out near immediately.  Call {@link #requeue(long)} passing delay in
   * milliseconds before readding to delay queue if you want it to stay there
   * a while.
   */
  static class FlushRegionEntry implements FlushQueueEntry {
    private final HRegion region;
    
    private final long createTime;
    private long whenToExpire;
    private int requeueCount = 0;

    FlushRegionEntry(final HRegion r) {
      this.region = r;
      this.createTime = System.currentTimeMillis();
      this.whenToExpire = this.createTime;
    }

    /**
     * @param maximumWait
     * @return True if we have been delayed > <code>maximumWait</code> milliseconds.
     */
    public boolean isMaximumWait(final long maximumWait) {
      return (System.currentTimeMillis() - this.createTime) > maximumWait;
    }

    /**
     * @return Count of times {@link #resetDelay()} was called; i.e this is
     * number of times we've been requeued.
     */
    public int getRequeueCount() {
      return this.requeueCount;
    }
 
    /**
     * @param when When to expire, when to come up out of the queue.
     * Specify in milliseconds.  This method adds System.currentTimeMillis()
     * to whatever you pass.
     * @return This.
     */
    public FlushRegionEntry requeue(final long when) {
      this.whenToExpire = System.currentTimeMillis() + when;
      this.requeueCount++;
      return this;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(this.whenToExpire - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
      return Long.valueOf(getDelay(TimeUnit.MILLISECONDS) -
        other.getDelay(TimeUnit.MILLISECONDS)).intValue();
    }
    
    @Override
    public String toString() {
      return "[flush region " + Bytes.toString(region.getRegionName()) + "]";
    }
  }
}
