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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.SortedMap;
import java.util.ConcurrentModificationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;

/**
 * Thread that flushes cache on request
 *
 * NOTE: This class extends Thread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 * 
 * @see FlushRequester
 */
class MemcacheFlusher extends Thread implements FlushRequester {
  static final Log LOG = LogFactory.getLog(MemcacheFlusher.class);
  private final BlockingQueue<HRegion> flushQueue =
    new LinkedBlockingQueue<HRegion>();
  
  private final HashSet<HRegion> regionsInQueue = new HashSet<HRegion>();

  private final long threadWakeFrequency;
  private final HRegionServer server;
  private final ReentrantLock lock = new ReentrantLock();

  protected final long globalMemcacheLimit;
  protected final long globalMemcacheLimitLowMark;
  
  /**
   * @param conf
   * @param server
   */
  public MemcacheFlusher(final HBaseConfiguration conf, final HRegionServer server) {
    super();
    this.server = server;
    threadWakeFrequency = conf.getLong(
        HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
        
    // default memcache limit of 512MB
    globalMemcacheLimit = 
      conf.getLong("hbase.regionserver.globalMemcacheLimit", 512 * 1024 * 1024);
    // default memcache low mark limit of 256MB, which is half the upper limit
    globalMemcacheLimitLowMark = 
      conf.getLong("hbase.regionserver.globalMemcacheLimitLowMark", 
        globalMemcacheLimit / 2);        
  }
  
  @Override
  public void run() {
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
          (r != null ? (" for region " + r.getRegionName()) : ""),
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
   * it needs to be removed from the queue to avoid flushing the region multiple
   * times.
   * 
   * @return true if the region was successfully flushed, false otherwise. If 
   * false, there will be accompanying log messages explaining why the log was
   * not flushed.
   */
  private boolean flushRegion(HRegion region, boolean removeFromQueue) {
    synchronized (regionsInQueue) {
      // take the region out of the set. If removeFromQueue is true, remove it
      // from the queue too if it is there. This didn't used to be a constraint,
      // but now that HBASE-512 is in play, we need to try and limit
      // double-flushing of regions.
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
          + (region != null ? (" for region " + region.getRegionName()) : ""),
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
   * Check if the regionserver's memcache memory usage is greater than the 
   * limit. If so, flush regions with the biggest memcaches until we're down
   * to the lower limit. This method blocks callers until we're down to a safe
   * amount of memcache consumption.
   */
  public void reclaimMemcacheMemory() {
    if (server.getGlobalMemcacheSize() >= globalMemcacheLimit) {
      flushSomeRegions();
    }
  }

  /*
   * Emergency!  Need to flush memory.  While running this method all updates
   * to this regionserver are blocked.
   */
  private synchronized void flushSomeRegions() {
    SortedMap<Long, HRegion> m =
      this.server.getCopyOfOnlineRegionsSortedBySize();
    if (m.size() <= 0) {
      LOG.info("No online regions to flush though we've been asked flush some.");
      return;
    }
    // keep flushing until we hit the low water mark
    while (server.getGlobalMemcacheSize() >= globalMemcacheLimitLowMark) {
      // flush the region with the biggest memcache
      HRegion biggestMemcacheRegion = m.remove(m.firstKey());
      if (!flushRegion(biggestMemcacheRegion, true)) {
        // Something bad happened - give up.
        break;
      }
    }
  }
}