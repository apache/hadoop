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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.RemoteExceptionHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * MetaScanner <code>META</code> table.
 * 
 * When a <code>META</code> server comes on line, a MetaRegion object is
 * queued up by regionServerReport() and this thread wakes up.
 *
 * It's important to do this work in a separate thread, or else the blocking 
 * action would prevent other work from getting done.
 */
class MetaScanner extends BaseScanner {
  /** Initial work for the meta scanner is queued up here */
  private volatile BlockingQueue<MetaRegion> metaRegionsToScan =
    new LinkedBlockingQueue<MetaRegion>();
    
  private final List<MetaRegion> metaRegionsToRescan =
    new ArrayList<MetaRegion>();
    
  /**
   * Constructor
   * 
   * @param master
   */
  public MetaScanner(HMaster master) {
    super(master, false, master.getShutdownRequested());
  }

  // Don't retry if we get an error while scanning. Errors are most often
  // caused by the server going away. Wait until next rescan interval when
  // things should be back to normal.
  private boolean scanOneMetaRegion(MetaRegion region) {
    while (!this.master.isClosed() &&
        !this.master.getRegionManager().isInitialRootScanComplete() &&
        this.master.getRegionManager().getRootRegionLocation() == null) {
      sleep();
    }
    if (this.master.isClosed()) {
      return false;
    }

    try {
      // Don't interrupt us while we're working
      synchronized (scannerLock) {
        scanRegion(region);
        this.master.getRegionManager().putMetaRegionOnline(region);
      }
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.warn("Scan one META region: " + region.toString(), e);
      // The region may have moved (TestRegionServerAbort, etc.).  If
      // so, either it won't be in the onlineMetaRegions list or its host
      // address has changed and the containsValue will fail. If not
      // found, best thing to do here is probably return.
      if (!this.master.getRegionManager().isMetaRegionOnline(region.getStartKey())) {
        LOG.debug("Scanned region is no longer in map of online " +
        "regions or its value has changed");
        return false;
      }
      // Make sure the file system is still available
      this.master.checkFileSystem();
    } catch (Exception e) {
      // If for some reason we get some other kind of exception, 
      // at least log it rather than go out silently.
      LOG.error("Unexpected exception", e);
    }
    return true;
  }

  @Override
  protected boolean initialScan() {
    MetaRegion region = null;
    while (!this.master.isClosed() &&
        (region == null && metaRegionsToScan.size() > 0) &&
          !metaRegionsScanned()) {
      try {
        region = metaRegionsToScan.poll(this.master.getThreadWakeFrequency(), 
          TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // continue
      }
      if (region == null && metaRegionsToRescan.size() != 0) {
        region = metaRegionsToRescan.remove(0);
      }
      if (region != null) {
        if (!scanOneMetaRegion(region)) {
          metaRegionsToRescan.add(region);
        }
      }
    }
    initialScanComplete = true;
    return true;
  }

  @Override
  protected void maintenanceScan() {
    List<MetaRegion> regions =
      this.master.getRegionManager().getListOfOnlineMetaRegions();
    int regionCount = 0;
    for (MetaRegion r: regions) {
      scanOneMetaRegion(r);
      regionCount++;
    }
    LOG.info("All " + regionCount + " .META. region(s) scanned");
    metaRegionsScanned();
  }

  /*
   * Called by the meta scanner when it has completed scanning all meta 
   * regions. This wakes up any threads that were waiting for this to happen.
   * @param totalRows Total rows scanned.
   * @param regionCount Count of regions in  .META. table.
   * @return False if number of meta regions matches count of online regions.
   */
  private synchronized boolean metaRegionsScanned() {
    if (!this.master.getRegionManager().isInitialRootScanComplete() ||
        this.master.getRegionManager().numMetaRegions() !=
          this.master.getRegionManager().numOnlineMetaRegions()) {
      return false;
    }
    notifyAll();
    return true;
  }

  /**
   * Other threads call this method to wait until all the meta regions have
   * been scanned.
   */
  synchronized boolean waitForMetaRegionsOrClose() {
    while (!this.master.isClosed()) {
      synchronized (master.getRegionManager()) {
        if (this.master.getRegionManager().isInitialRootScanComplete() &&
            this.master.getRegionManager().numMetaRegions() ==
              this.master.getRegionManager().numOnlineMetaRegions()) {
          break;
        }
      }
      try {
        wait(this.master.getThreadWakeFrequency());
      } catch (InterruptedException e) {
        // continue
      }
    }
    return this.master.isClosed();
  }
  
  /**
   * Add another meta region to scan to the queue.
   */ 
  void addMetaRegionToScan(MetaRegion m) {
    metaRegionsToScan.add(m);
  }
}