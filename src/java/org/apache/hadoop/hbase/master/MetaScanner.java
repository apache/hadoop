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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.RemoteExceptionHandler;

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
  private final List<MetaRegion> metaRegionsToRescan =
    new ArrayList<MetaRegion>();
  
  /** Constructor */
  public MetaScanner(HMaster master) {
    super(master, false, master.metaRescanInterval, master.closed);
  }

  private boolean scanOneMetaRegion(MetaRegion region) {
    // Don't retry if we get an error while scanning. Errors are most often
    // caused by the server going away. Wait until next rescan interval when
    // things should be back to normal
    boolean scanSuccessful = false;
    while (!master.closed.get() && !master.rootScanned &&
      master.rootRegionLocation.get() == null) {
      master.sleeper.sleep();
    }
    if (master.closed.get()) {
      return scanSuccessful;
    }

    try {
      // Don't interrupt us while we're working
      synchronized (master.metaScannerLock) {
        scanRegion(region);
        master.onlineMetaRegions.put(region.getStartKey(), region);
      }
      scanSuccessful = true;
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.warn("Scan one META region: " + region.toString(), e);
      // The region may have moved (TestRegionServerAbort, etc.).  If
      // so, either it won't be in the onlineMetaRegions list or its host
      // address has changed and the containsValue will fail. If not
      // found, best thing to do here is probably return.
      if (!master.onlineMetaRegions.containsValue(region.getStartKey())) {
        LOG.debug("Scanned region is no longer in map of online " +
        "regions or its value has changed");
        return scanSuccessful;
      }
      // Make sure the file system is still available
      master.checkFileSystem();
    } catch (Exception e) {
      // If for some reason we get some other kind of exception, 
      // at least log it rather than go out silently.
      LOG.error("Unexpected exception", e);
    }
    return scanSuccessful;
  }

  @Override
  protected boolean initialScan() {
    MetaRegion region = null;
    while (!master.closed.get() && region == null && !metaRegionsScanned()) {
      try {
        region = master.metaRegionsToScan.poll(master.threadWakeFrequency, 
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
    master.initialMetaScanComplete = true;
    return true;
  }

  @Override
  protected void maintenanceScan() {
    ArrayList<MetaRegion> regions = new ArrayList<MetaRegion>();
    synchronized (master.onlineMetaRegions) {
      regions.addAll(master.onlineMetaRegions.values());
    }
    for (MetaRegion r: regions) {
      scanOneMetaRegion(r);
    }
    metaRegionsScanned();
  }

  /**
   * Called by the meta scanner when it has completed scanning all meta 
   * regions. This wakes up any threads that were waiting for this to happen.
   */
  private synchronized boolean metaRegionsScanned() {
    if (!master.rootScanned ||
        master.numberOfMetaRegions.get() != master.onlineMetaRegions.size()) {
      return false;
    }
    LOG.info("all meta regions scanned");
    notifyAll();
    return true;
  }

  /**
   * Other threads call this method to wait until all the meta regions have
   * been scanned.
   */
  synchronized boolean waitForMetaRegionsOrClose() {
    while (!master.closed.get()) {
      if (master.rootScanned &&
          master.numberOfMetaRegions.get() == master.onlineMetaRegions.size()) {
        break;
      }

      try {
        wait(master.threadWakeFrequency);
      } catch (InterruptedException e) {
        // continue
      }
    }
    return master.closed.get();
  }
}