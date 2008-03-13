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
import org.apache.hadoop.hbase.RemoteExceptionHandler;

/**
* ProcessRegionClose is instantiated when a region server reports that it
* has closed a region.
*/
class ProcessRegionClose extends ProcessRegionStatusChange {
  private boolean reassignRegion;
  private boolean deleteRegion;

  /**
  * @param regionInfo
  * @param reassignRegion
  * @param deleteRegion
  */
  public ProcessRegionClose(HMaster master, HRegionInfo regionInfo, 
   boolean reassignRegion, boolean deleteRegion) {

   super(master, regionInfo);
   this.reassignRegion = reassignRegion;
   this.deleteRegion = deleteRegion;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "ProcessRegionClose of " + this.regionInfo.getRegionName() +
      ", " + this.reassignRegion + ", " + this.deleteRegion;
  }

  @Override
  protected boolean process() throws IOException {
    for (int tries = 0; tries < numRetries; tries++) {
      if (master.closed.get()) {
        return true;
      }
      LOG.info("region closed: " + regionInfo.getRegionName());

      // Mark the Region as unavailable in the appropriate meta table

      if (!metaRegionAvailable()) {
        // We can't proceed unless the meta region we are going to update
        // is online. metaRegionAvailable() has put this operation on the
        // delayedToDoQueue, so return true so the operation is not put 
        // back on the toDoQueue
        return true;
      }

      try {
        if (deleteRegion) {
          HRegion.removeRegionFromMETA(getMetaServer(), metaRegionName,
            regionInfo.getRegionName());
        } else if (!this.reassignRegion) {
          HRegion.offlineRegionInMETA(getMetaServer(), metaRegionName,
            regionInfo);
        }
        break;

      } catch (IOException e) {
        if (tries == numRetries - 1) {
          throw RemoteExceptionHandler.checkIOException(e);
        }
        continue;
      }
    }

    if (reassignRegion) {
      LOG.info("reassign region: " + regionInfo.getRegionName());
      master.regionManager.setUnassigned(regionInfo);
    } else if (deleteRegion) {
      try {
        HRegion.deleteRegion(master.fs, master.rootdir, regionInfo);
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.error("failed delete region " + regionInfo.getRegionName(), e);
        throw e;
      }
    }
    return true;
  }
}
