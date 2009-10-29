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
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.StringUtils;

/** 
 * Compact region on request and then run split if appropriate
 */
class CompactSplitThread extends Thread implements HConstants {
  static final Log LOG = LogFactory.getLog(CompactSplitThread.class);
  
  private HTable root = null;
  private HTable meta = null;
  private final long frequency;
  private final ReentrantLock lock = new ReentrantLock();
  
  private final HRegionServer server;
  private final HBaseConfiguration conf;
  
  private final BlockingQueue<HRegion> compactionQueue =
    new LinkedBlockingQueue<HRegion>();
  
  private final HashSet<HRegion> regionsInQueue = new HashSet<HRegion>();

  private volatile int limit = 1;

  /** @param server */
  public CompactSplitThread(HRegionServer server) {
    super();
    this.server = server;
    this.conf = server.conf;
    this.frequency =
      conf.getLong("hbase.regionserver.thread.splitcompactcheckfrequency",
      20 * 1000);
  }
  
  @Override
  public void run() {
    int count = 0;
    while (!this.server.isStopRequested()) {
      HRegion r = null;
      try {
        if ((limit > 0) && (++count > limit)) {
          try {
            Thread.sleep(this.frequency);
          } catch (InterruptedException ex) {
            continue;
          }
          count = 0;
        }
        r = compactionQueue.poll(this.frequency, TimeUnit.MILLISECONDS);
        if (r != null && !this.server.isStopRequested()) {
          synchronized (regionsInQueue) {
            regionsInQueue.remove(r);
          }
          lock.lock();
          try {
            // Don't interrupt us while we are working
            byte [] midKey = r.compactStores();
            if (midKey != null && !this.server.isStopRequested()) {
              split(r, midKey);
            }
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException ex) {
        continue;
      } catch (IOException ex) {
        LOG.error("Compaction/Split failed for region " +
            r.getRegionNameAsString(),
          RemoteExceptionHandler.checkIOException(ex));
        if (!server.checkFileSystem()) {
          break;
        }
      } catch (Exception ex) {
        LOG.error("Compaction failed" +
            (r != null ? (" for region " + r.getRegionNameAsString()) : ""),
            ex);
        if (!server.checkFileSystem()) {
          break;
        }
      }
    }
    regionsInQueue.clear();
    compactionQueue.clear();
    LOG.info(getName() + " exiting");
  }

  /**
   * @param r HRegion store belongs to
   * @param why Why compaction requested -- used in debug messages
   */
  public synchronized void compactionRequested(final HRegion r,
      final String why) {
    compactionRequested(r, false, why);
  }

  /**
   * @param r HRegion store belongs to
   * @param force Whether next compaction should be major
   * @param why Why compaction requested -- used in debug messages
   */
  public synchronized void compactionRequested(final HRegion r,
      final boolean force, final String why) {
    if (this.server.stopRequested.get()) {
      return;
    }
    r.setForceMajorCompaction(force);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Compaction " + (force? "(major) ": "") +
        "requested for region " + r.getRegionNameAsString() +
        "/" + r.getRegionInfo().getEncodedName() +
        (why != null && !why.isEmpty()? " because: " + why: ""));
    }
    synchronized (regionsInQueue) {
      if (!regionsInQueue.contains(r)) {
        compactionQueue.add(r);
        regionsInQueue.add(r);
      }
    }
  }
  
  private void split(final HRegion region, final byte [] midKey)
  throws IOException {
    final HRegionInfo oldRegionInfo = region.getRegionInfo();
    final long startTime = System.currentTimeMillis();
    final HRegion[] newRegions = region.splitRegion(midKey);
    if (newRegions == null) {
      // Didn't need to be split
      return;
    }
    
    // When a region is split, the META table needs to updated if we're
    // splitting a 'normal' region, and the ROOT table needs to be
    // updated if we are splitting a META region.
    HTable t = null;
    if (region.getRegionInfo().isMetaTable()) {
      // We need to update the root region
      if (this.root == null) {
        this.root = new HTable(conf, ROOT_TABLE_NAME);
      }
      t = root;
    } else {
      // For normal regions we need to update the meta region
      if (meta == null) {
        meta = new HTable(conf, META_TABLE_NAME);
      }
      t = meta;
    }

    // Mark old region as offline and split in META.
    // NOTE: there is no need for retry logic here. HTable does it for us.
    oldRegionInfo.setOffline(true);
    oldRegionInfo.setSplit(true);
    // Inform the HRegionServer that the parent HRegion is no-longer online.
    this.server.removeFromOnlineRegions(oldRegionInfo);

    Put put = new Put(oldRegionInfo.getRegionName());
    put.add(CATALOG_FAMILY, REGIONINFO_QUALIFIER, 
      Writables.getBytes(oldRegionInfo));
    put.add(CATALOG_FAMILY, SPLITA_QUALIFIER,
      Writables.getBytes(newRegions[0].getRegionInfo()));
    put.add(CATALOG_FAMILY, SPLITB_QUALIFIER,
      Writables.getBytes(newRegions[1].getRegionInfo()));
    t.put(put);
    
    // Add new regions to META
    for (int i = 0; i < newRegions.length; i++) {
      put = new Put(newRegions[i].getRegionName());
      put.add(CATALOG_FAMILY, REGIONINFO_QUALIFIER, Writables.getBytes(
        newRegions[i].getRegionInfo()));
      t.put(put);
    }
        
    // Now tell the master about the new regions
    server.reportSplit(oldRegionInfo, newRegions[0].getRegionInfo(),
      newRegions[1].getRegionInfo());
    LOG.info("region split, META updated, and report to master all" +
      " successful. Old region=" + oldRegionInfo.toString() +
      ", new regions: " + newRegions[0].toString() + ", " +
      newRegions[1].toString() + ". Split took " +
      StringUtils.formatTimeDiff(System.currentTimeMillis(), startTime));
    
    // Do not serve the new regions. Let the Master assign them.
  }

  /**
   * Sets the number of compactions allowed per cycle.
   * @param limit the number of compactions allowed, or -1 to unlimit
   */
  void setLimit(int limit) {
    this.limit = limit;
  }

  int getLimit() {
    return this.limit;
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */ 
  void interruptIfNecessary() {
    if (lock.tryLock()) {
      this.interrupt();
    }
  }
}