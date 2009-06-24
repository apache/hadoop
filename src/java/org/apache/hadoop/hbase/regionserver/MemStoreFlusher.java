/**
 * Copyright 2008 The Apache Software Foundation
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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

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
  private final BlockingQueue<HRegion> flushQueue =
    new LinkedBlockingQueue<HRegion>();
  
  private final HashSet<HRegion> regionsInQueue = new HashSet<HRegion>();

  private final long threadWakeFrequency;
  private final HRegionServer server;
  private final ReentrantLock lock = new ReentrantLock();

  protected final long globalMemStoreLimit;
  protected final long globalMemStoreLimitLowMark;
  
  private static final float DEFAULT_UPPER = 0.4f;
  private static final float DEFAULT_LOWER = 0.25f;
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
  public MemStoreFlusher(final HBaseConfiguration conf,
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
      conf.getInt("hbase.hstore.blockingStoreFiles", -1);
    if (this.blockingStoreFilesNumber == -1) {
      this.blockingStoreFilesNumber = 1 +
        conf.getInt("hbase.hstore.compactionThreshold", 3);
    }
    this.blockingWaitTime = conf.getInt("hbase.hstore.blockingWaitTime",
      90000); // default of 180 seconds
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
     final float defaultLimit, final String key, final HBaseConfiguration c) {
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
  
  @Override
  public void run() {
    while (!this.server.isStopRequested() && this.server.isInSafeMode()) {
      try {
        Thread.sleep(threadWakeFrequency);
      } catch (InterruptedException ex) {
        continue;
      }
    }
    while (!server.isStopRequested()) {
      HRegion r = null;
      try {
        r = flushQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
        if (r == null) {
          continue;
        }
        if (!flushRegion(r, false)) {
          break;
        }
      } catch (InterruptedException ex) {
        continue;
      } catch (ConcurrentModificationException ex) {
        continue;
      } catch (Exception ex) {
        LOG.error("Cache flush failed" +
          (r != null ? (" for region " + Bytes.toString(r.getRegionName())) : ""),
          ex);
        if (!server.checkFileSystem()) {
          break;
        }
      }
    }
    regionsInQueue.clear();
    flushQueue.clear();
    LOG.info(getName() + " exiting");
  }
  
  public void request(HRegion r) {
    synchronized (regionsInQueue) {
      if (!regionsInQueue.contains(r)) {
        regionsInQueue.add(r);
        flushQueue.add(r);
      }
    }
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
   * Flush a region.
   * 
   * @param region the region to be flushed
   * @param removeFromQueue True if the region needs to be removed from the
   * flush queue. False if called from the main flusher run loop and true if
   * called from flushSomeRegions to relieve memory pressure from the region
   * server.  If <code>true</code>, we are in a state of emergency; we are not
   * taking on updates regionserver-wide, not until memory is flushed.  In this
   * case, do not let a compaction run inline with blocked updates. Compactions
   * can take a long time. Stopping compactions, there is a danger that number
   * of flushes will overwhelm compaction on a busy server; we'll have to see.
   * That compactions do not run when called out of flushSomeRegions means that
   * compactions can be reported by the historian without danger of deadlock
   * (HBASE-670).
   * 
   * <p>In the main run loop, regions have already been removed from the flush
   * queue, and if this method is called for the relief of memory pressure,
   * this may not be necessarily true. We want to avoid trying to remove 
   * region from the queue because if it has already been removed, it requires a
   * sequential scan of the queue to determine that it is not in the queue.
   * 
   * <p>If called from flushSomeRegions, the region may be in the queue but
   * it may have been determined that the region had a significant amount of 
   * memory in use and needed to be flushed to relieve memory pressure. In this
   * case, its flush may preempt the pending request in the queue, and if so,
   * it needs to be removed from the queue to avoid flushing the region
   * multiple times.
   * 
   * @return true if the region was successfully flushed, false otherwise. If 
   * false, there will be accompanying log messages explaining why the log was
   * not flushed.
   */
  private boolean flushRegion(HRegion region, boolean removeFromQueue) {
    // Wait until it is safe to flush
    int count = 0;
    boolean triggered = false;
    while (count++ < (blockingWaitTime / 500)) {
      for (Store hstore: region.stores.values()) {
        if (hstore.getStorefilesCount() > this.blockingStoreFilesNumber) {
          // always request a compaction
          server.compactSplitThread.compactionRequested(region, getName());
          // only log once
          if (!triggered) {
            LOG.info("Too many store files for region " + region + ": " +
              hstore.getStorefilesCount() + ", waiting");
            triggered = true;
          }
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            // ignore
          }
          continue;
        }
      }
      if (triggered) {
        LOG.info("Compaction completed on region " + region +
          ", proceeding");
      }
      break;
    }
    synchronized (regionsInQueue) {
      // See comment above for removeFromQueue on why we do not
      // take the region out of the set. If removeFromQueue is true, remove it
      // from the queue too if it is there. This didn't used to be a
      // constraint, but now that HBASE-512 is in play, we need to try and
      // limit double-flushing of regions.
      if (regionsInQueue.remove(region) && removeFromQueue) {
        flushQueue.remove(region);
      }
      lock.lock();
    }
    try {
      // See comment above for removeFromQueue on why we do not
      // compact if removeFromQueue is true. Note that region.flushCache()
      // only returns true if a flush is done and if a compaction is needed.
      if (region.flushcache() && !removeFromQueue) {
        server.compactSplitThread.compactionRequested(region, getName());
      }
    } catch (DroppedSnapshotException ex) {
      // Cache flush can fail in a few places. If it fails in a critical
      // section, we get a DroppedSnapshotException and a replay of hlog
      // is required. Currently the only way to do this is a restart of
      // the server. Abort because hdfs is probably bad (HBASE-644 is a case
      // where hdfs was bad but passed the hdfs check).
      LOG.fatal("Replay of hlog required. Forcing server shutdown", ex);
      server.abort();
      return false;
    } catch (IOException ex) {
      LOG.error("Cache flush failed"
          + (region != null ? (" for region " + Bytes.toString(region.getRegionName())) : ""),
          RemoteExceptionHandler.checkIOException(ex));
      if (!server.checkFileSystem()) {
        return false;
      }
    } finally {
      lock.unlock();
    }

    return true;
  }
  
  /**
   * Check if the regionserver's memstore memory usage is greater than the 
   * limit. If so, flush regions with the biggest memstores until we're down
   * to the lower limit. This method blocks callers until we're down to a safe
   * amount of memstore consumption.
   */
  public synchronized void reclaimMemStoreMemory() {
    if (server.getGlobalMemStoreSize() >= globalMemStoreLimit) {
      flushSomeRegions();
    }
  }

  /*
   * Emergency!  Need to flush memory.
   */
  private synchronized void flushSomeRegions() {
    // keep flushing until we hit the low water mark
    long globalMemStoreSize = -1;
    ArrayList<HRegion> regionsToCompact = new ArrayList<HRegion>();
    for (SortedMap<Long, HRegion> m =
        this.server.getCopyOfOnlineRegionsSortedBySize();
      (globalMemStoreSize = server.getGlobalMemStoreSize()) >=
        this.globalMemStoreLimitLowMark;) {
      // flush the region with the biggest memstore
      if (m.size() <= 0) {
        LOG.info("No online regions to flush though we've been asked flush " +
          "some; globalMemStoreSize=" +
          StringUtils.humanReadableInt(globalMemStoreSize) +
          ", globalMemStoreLimitLowMark=" +
          StringUtils.humanReadableInt(this.globalMemStoreLimitLowMark));
        break;
      }
      HRegion biggestMemStoreRegion = m.remove(m.firstKey());
      LOG.info("Forced flushing of " +  biggestMemStoreRegion.toString() +
        " because global memstore limit of " +
        StringUtils.humanReadableInt(this.globalMemStoreLimit) +
        " exceeded; currently " +
        StringUtils.humanReadableInt(globalMemStoreSize) + " and flushing till " +
        StringUtils.humanReadableInt(this.globalMemStoreLimitLowMark));
      if (!flushRegion(biggestMemStoreRegion, true)) {
        LOG.warn("Flush failed");
        break;
      }
      regionsToCompact.add(biggestMemStoreRegion);
    }
    for (HRegion region : regionsToCompact) {
      server.compactSplitThread.compactionRequested(region, getName());
    }
  }
}
