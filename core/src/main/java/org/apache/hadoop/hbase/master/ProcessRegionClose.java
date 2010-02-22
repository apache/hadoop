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
  protected final boolean offlineRegion;
  protected final boolean reassignRegion;

  /**
  * @param master
  * @param regionInfo Region to operate on
  * @param offlineRegion if true, set the region to offline in meta
  * @param reassignRegion if true, region is to be reassigned
  */
  public ProcessRegionClose(HMaster master, HRegionInfo regionInfo, 
      boolean offlineRegion, boolean reassignRegion) {

   super(master, regionInfo);
   this.offlineRegion = offlineRegion;
   this.reassignRegion = reassignRegion;
  }

  @Override
  public String toString() {
    return "ProcessRegionClose of " + this.regionInfo.getRegionNameAsString() +
      ", " + this.offlineRegion + ", reassign: " + this.reassignRegion;
  }

  @Override
  protected boolean process() throws IOException {
    Boolean result = null;
    if (offlineRegion || reassignRegion) {
      result =
        new RetryableMetaOperation<Boolean>(getMetaRegion(), this.master) {
          public Boolean call() throws IOException {


            // We can't proceed unless the meta region we are going to update
            // is online. metaRegionAvailable() will put this operation on the
            // delayedToDoQueue, so return true so the operation is not put 
            // back on the toDoQueue

            if (metaRegionAvailable()) {
              if(offlineRegion) {
                // offline the region in meta and then remove it from the
                // set of regions in transition
                HRegion.offlineRegionInMETA(server, metaRegionName,
                    regionInfo);
                master.getRegionManager().removeRegion(regionInfo);
                LOG.info("region closed: " + regionInfo.getRegionNameAsString());
              } else {
                // we are reassigning the region eventually, so set it unassigned
                // and remove the server info
                HRegion.cleanRegionInMETA(server, metaRegionName,
                    regionInfo);
                master.getRegionManager().setUnassigned(regionInfo, false);
                LOG.info("region set as unassigned: " + regionInfo.getRegionNameAsString());
              }
            }
            return true;
          }
        }.doWithRetries();
        result = result == null ? true : result;

    } else {
      LOG.info("Region was neither offlined, or asked to be reassigned, what gives: " +
      regionInfo.getRegionNameAsString());
    }

    return result == null ? true : result;
  }
}
