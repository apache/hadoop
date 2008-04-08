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
import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Comparator;
import java.util.ConcurrentModificationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;

/** Flush cache upon request */
class Flusher extends Thread implements CacheFlushListener {
  static final Log LOG = LogFactory.getLog(Flusher.class);
  private final BlockingQueue<HRegion> flushQueue =
    new LinkedBlockingQueue<HRegion>();
  
  private final HashSet<HRegion> regionsInQueue = new HashSet<HRegion>();

  private final long threadWakeFrequency;
  private final long optionalFlushPeriod;
  private final HRegionServer server;
  private final Integer lock = new Integer(0);
  private final Integer memcacheSizeLock = new Integer(0);  
  private long lastOptionalCheck = System.currentTimeMillis();

  protected final long globalMemcacheLimit;
  protected final long globalMemcacheLimitLowMark;
  
  /**
   * @param conf
   * @param server
   */
  public Flusher(final HBaseConfiguration conf, final HRegionServer server) {
    super();
    this.server = server;
    optionalFlushPeriod = conf.getLong(
        "hbase.regionserver.optionalcacheflushinterval", 30 * 60 * 1000L);
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
  
  /** {@inheritDoc} */
  @Override
  public void run() {
    while (!server.isStopRequested()) {
      HRegion r = null;
      try {
        enqueueOptionalFlushRegions();
        r = flushQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
        if (!flushImmediately(r)) {
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
  
  /** {@inheritDoc} */
  public void flushRequested(HRegion r) {
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
  void interruptPolitely() {
    synchronized (lock) {
      interrupt();
    }
  }
  
  /**
   * Flush a region right away, while respecting concurrency with the async
   * flushing that is always going on.
   */
  private boolean flushImmediately(HRegion region) {
    try {
      if (region != null) {
        synchronized (regionsInQueue) {
          // take the region out of the set and the queue, if it happens to be 
          // in the queue. this didn't used to be a constraint, but now that
          // HBASE-512 is in play, we need to try and limit double-flushing
          // regions.
          regionsInQueue.remove(region);
          flushQueue.remove(region);
        }
        synchronized (lock) { // Don't interrupt while we're working
          if (region.flushcache()) {
            server.compactSplitThread.compactionRequested(region);
          }
        }
      }      
    } catch (DroppedSnapshotException ex) {
      // Cache flush can fail in a few places.  If it fails in a critical
      // section, we get a DroppedSnapshotException and a replay of hlog
      // is required. Currently the only way to do this is a restart of
      // the server.
      LOG.fatal("Replay of hlog required. Forcing server restart", ex);
      if (!server.checkFileSystem()) {
        return false;
      }
      server.stop();
    } catch (IOException ex) {
      LOG.error("Cache flush failed" +
        (region != null ? (" for region " + region.getRegionName()) : ""),
        RemoteExceptionHandler.checkIOException(ex));
      if (!server.checkFileSystem()) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Find the regions that should be optionally flushed and put them on the
   * flush queue.
   */
  private void enqueueOptionalFlushRegions() {
    long now = System.currentTimeMillis();
    if (now - threadWakeFrequency > lastOptionalCheck) {
      lastOptionalCheck = now;
      // Queue up regions for optional flush if they need it
      Set<HRegion> regions = server.getRegionsToCheck();
      for (HRegion region: regions) {
        synchronized (regionsInQueue) {
          if (!regionsInQueue.contains(region) &&
              (now - optionalFlushPeriod) > region.getLastFlushTime()) {
            regionsInQueue.add(region);
            flushQueue.add(region);
            region.setLastFlushTime(now);
          }
        }
      }
    }
  }
  
  /**
   * Check if the regionserver's memcache memory usage is greater than the 
   * limit. If so, flush regions with the biggest memcaches until we're down
   * to the lower limit. This method blocks callers until we're down to a safe
   * amount of memcache consumption.
   */
  public void reclaimMemcacheMemory() {
    synchronized (memcacheSizeLock) {
      if (server.getGlobalMemcacheSize() >= globalMemcacheLimit) {
        flushSomeRegions();
      }
    }
  }
  
  private void flushSomeRegions() {
    // we'll sort the regions in reverse
    SortedMap<Long, HRegion> sortedRegions = new TreeMap<Long, HRegion>(
      new Comparator<Long>() {
        public int compare(Long a, Long b) {
          return -1 * a.compareTo(b);
        }
      }
    );
    
    // copy over all the regions
    for (HRegion region : server.onlineRegions.values()) {
      sortedRegions.put(region.memcacheSize.get(), region);
    }
    
    // keep flushing until we hit the low water mark
    while (server.getGlobalMemcacheSize() >= globalMemcacheLimitLowMark) {
      // flush the region with the biggest memcache
      HRegion biggestMemcacheRegion = 
        sortedRegions.remove(sortedRegions.firstKey());
      flushImmediately(biggestMemcacheRegion);
    }
  }
  
}