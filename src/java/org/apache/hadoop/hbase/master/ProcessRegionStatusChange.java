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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;

/**
 * Abstract class that performs common operations for 
 * @see #ProcessRegionClose and @see #ProcessRegionOpen
 */
abstract class ProcessRegionStatusChange extends RegionServerOperation {
  protected final boolean isMetaTable;
  protected final HRegionInfo regionInfo;
  private volatile MetaRegion metaRegion = null;
  protected volatile byte[] metaRegionName = null;
  
  /**
   * @param master
   * @param regionInfo
   */
  public ProcessRegionStatusChange(HMaster master, HRegionInfo regionInfo) {
    super(master);
    this.regionInfo = regionInfo;
    this.isMetaTable = regionInfo.isMetaTable();
  }
  
  protected boolean metaRegionAvailable() {
    boolean available = true;
    if (isMetaTable) {
      // This operation is for the meta table
      if (!rootAvailable()) {
        // But we can't proceed unless the root region is available
        available = false;
      }
    } else {
      if (!master.regionManager.isInitialRootScanComplete() ||
          !metaTableAvailable()) {
        // The root region has not been scanned or the meta table is not
        // available so we can't proceed.
        // Put the operation on the delayedToDoQueue
        requeue();
        available = false;
      }
    }
    return available;
  }

  protected MetaRegion getMetaRegion() {
    if (isMetaTable) {
      this.metaRegionName = HRegionInfo.ROOT_REGIONINFO.getRegionName();
      this.metaRegion = new MetaRegion(master.getRootRegionLocation(),
          this.metaRegionName, HConstants.EMPTY_START_ROW);
    } else {
      this.metaRegion =
        master.regionManager.getFirstMetaRegionForRegion(regionInfo);
      this.metaRegionName = this.metaRegion.getRegionName();
    }
    return this.metaRegion;
  }
}