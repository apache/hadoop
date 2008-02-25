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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Writables;

/** 
 * Compact region on request and then run split if appropriate
 */
class CompactSplitThread extends Thread 
implements RegionUnavailableListener, HConstants {
  static final Log LOG = LogFactory.getLog(CompactSplitThread.class);
    
  private HTable root = null;
  private HTable meta = null;
  private long startTime;
  private final long frequency;
  
  private HRegionServer server;
  private HBaseConfiguration conf;
  
  private final BlockingQueue<QueueEntry> compactionQueue =
    new LinkedBlockingQueue<QueueEntry>();

  /** constructor */
  public CompactSplitThread(HRegionServer server) {
    super();
    this.server = server;
    this.conf = server.conf;
    this.frequency =
      conf.getLong("hbase.regionserver.thread.splitcompactcheckfrequency",
      20 * 1000);
  }
  
  /** {@inheritDoc} */
  @Override
  public void run() {
    while (!server.isStopRequested()) {
      QueueEntry e = null;
      try {
        e = compactionQueue.poll(this.frequency, TimeUnit.MILLISECONDS);
        if (e == null) {
          continue;
        }
        e.getRegion().compactIfNeeded();
        split(e.getRegion());
      } catch (InterruptedException ex) {
        continue;
      } catch (IOException ex) {
        LOG.error("Compaction failed" +
            (e != null ? (" for region " + e.getRegion().getRegionName()) : ""),
            RemoteExceptionHandler.checkIOException(ex));
        if (!server.checkFileSystem()) {
          break;
        }

      } catch (Exception ex) {
        LOG.error("Compaction failed" +
            (e != null ? (" for region " + e.getRegion().getRegionName()) : ""),
            ex);
        if (!server.checkFileSystem()) {
          break;
        }
      }
    }
    LOG.info(getName() + " exiting");
  }
  
  /**
   * @param e QueueEntry for region to be compacted
   */
  public void compactionRequested(QueueEntry e) {
    compactionQueue.add(e);
  }
  
  void compactionRequested(final HRegion r) {
    compactionRequested(new QueueEntry(r, System.currentTimeMillis()));
  }
  
  private void split(final HRegion region) throws IOException {
    final HRegionInfo oldRegionInfo = region.getRegionInfo();
    final HRegion[] newRegions = region.splitRegion(this);
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
    LOG.info("Updating " + t.getTableName() + " with region split info");

    // Mark old region as offline and split in META.
    // NOTE: there is no need for retry logic here. HTable does it for us.
    oldRegionInfo.setOffline(true);
    oldRegionInfo.setSplit(true);
    BatchUpdate update = new BatchUpdate(oldRegionInfo.getRegionName());
    update.put(COL_REGIONINFO, Writables.getBytes(oldRegionInfo));
    update.put(COL_SPLITA, Writables.getBytes(newRegions[0].getRegionInfo()));
    update.put(COL_SPLITB, Writables.getBytes(newRegions[1].getRegionInfo()));
    t.commit(update);
    
    // Add new regions to META
    for (int i = 0; i < newRegions.length; i++) {
      update = new BatchUpdate(newRegions[i].getRegionName());
      update.put(COL_REGIONINFO, Writables.getBytes(
        newRegions[i].getRegionInfo()));
      t.commit(update);
    }
        
    // Now tell the master about the new regions
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reporting region split to master");
    }
    server.reportSplit(oldRegionInfo, newRegions[0].getRegionInfo(),
      newRegions[1].getRegionInfo());
    LOG.info("region split, META updated, and report to master all" +
      " successful. Old region=" + oldRegionInfo.toString() +
      ", new regions: " + newRegions[0].toString() + ", " +
      newRegions[1].toString() + ". Split took " +
      StringUtils.formatTimeDiff(System.currentTimeMillis(), startTime));
    
    // Do not serve the new regions. Let the Master assign them.
  }
  
  /** {@inheritDoc} */
  public void closing(final Text regionName) {
    startTime = System.currentTimeMillis();
    server.getWriteLock().lock();
    try {
      // Remove region from regions Map and add it to the Map of retiring
      // regions.
      server.setRegionClosing(regionName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(regionName.toString() + " closing (" +
          "Adding to retiringRegions)");
      }
    } finally {
      server.getWriteLock().unlock();
    }
  }
  
  /** {@inheritDoc} */
  public void closed(final Text regionName) {
    server.getWriteLock().lock();
    try {
      server.setRegionClosed(regionName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(regionName.toString() + " closed");
      }
    } finally {
      server.getWriteLock().unlock();
    }
  }
}
