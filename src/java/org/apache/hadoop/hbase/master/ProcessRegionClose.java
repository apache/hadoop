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

import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.HRegionInfo;

/**
 * ProcessRegionClose is the way we do post-processing on a closed region. We
 * only spawn one of these asynchronous tasks when the region needs to be 
 * either offlined or deleted. We used to create one of these tasks whenever
 * a region was closed, but since closing a region that isn't being offlined
 * or deleted doesn't actually require post processing, it's no longer 
 * necessary.
 */
class ProcessRegionClose extends ProcessRegionStatusChange {
  protected final  boolean offlineRegion;
  protected final boolean deleteRegion;

  /**
  * @param master
  * @param regionInfo Region to operate on
  * @param offlineRegion if true, set the region to offline in meta
  * @param deleteRegion if true, delete the region row from meta and then
  * delete the region files from disk.
  */
  public ProcessRegionClose(HMaster master, HRegionInfo regionInfo, 
   boolean offlineRegion, boolean deleteRegion) {

   super(master, regionInfo);
   this.offlineRegion = offlineRegion;
   this.deleteRegion = deleteRegion;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "ProcessRegionClose of " + this.regionInfo.getRegionName() +
      ", " + this.offlineRegion + ", " + this.deleteRegion;
  }

  @Override
  protected boolean process() throws IOException {
    Boolean result =
      new RetryableMetaOperation<Boolean>(this.metaRegion, this.master) {
        public Boolean call() throws IOException {
          LOG.info("region closed: " + regionInfo.getRegionName());

          // Mark the Region as unavailable in the appropriate meta table

          if (!metaRegionAvailable()) {
            // We can't proceed unless the meta region we are going to update
            // is online. metaRegionAvailable() has put this operation on the
            // delayedToDoQueue, so return true so the operation is not put 
            // back on the toDoQueue
            return true;
          }

          if (deleteRegion) {
            HRegion.removeRegionFromMETA(server, metaRegionName,
                regionInfo.getRegionName());
          } else if (offlineRegion) {
            // offline the region in meta and then note that we've offlined the
            // region. 
            HRegion.offlineRegionInMETA(server, metaRegionName,
                regionInfo);
            master.regionManager.regionOfflined(regionInfo.getRegionName());
          }
          return true;
        }
    }.doWithRetries();

    // now that meta is updated, if we need to delete the region's files, now's
    // the time.
    if (deleteRegion) {
      try {
        HRegion.deleteRegion(master.fs, master.rootdir, regionInfo);
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.error("failed delete region " + regionInfo.getRegionName(), e);
        throw e;
      }
    }
    return result == null ? true : result;
  }
}
