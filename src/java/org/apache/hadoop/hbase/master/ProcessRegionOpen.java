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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.RegionHistorian;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * ProcessRegionOpen is instantiated when a region server reports that it is
 * serving a region. This applies to all meta and user regions except the 
 * root region which is handled specially.
 */
class ProcessRegionOpen extends ProcessRegionStatusChange {
  protected final HServerAddress serverAddress;
  protected final byte [] startCode;

  /**
   * @param master
   * @param info
   * @param regionInfo
   * @throws IOException
   */
  @SuppressWarnings("unused")
  public ProcessRegionOpen(HMaster master, HServerInfo info, 
    HRegionInfo regionInfo)
  throws IOException {
    super(master, regionInfo);
    this.serverAddress = info.getServerAddress();
    if (this.serverAddress == null) {
      throw new NullPointerException("Server address cannot be null; " +
        "hbase-958 debugging");
    }
    this.startCode = Bytes.toBytes(info.getStartCode());
  }

  @Override
  public String toString() {
    return "PendingOpenOperation from " + serverAddress.toString();
  }

  @Override
  protected boolean process() throws IOException {
    Boolean result =
      new RetryableMetaOperation<Boolean>(getMetaRegion(), this.master) {
        private final RegionHistorian historian = RegionHistorian.getInstance();
      
        public Boolean call() throws IOException {
          LOG.info(regionInfo.getRegionNameAsString() + " open on " +
            serverAddress.toString());
          if (!metaRegionAvailable()) {
            // We can't proceed unless the meta region we are going to update
            // is online. metaRegionAvailable() has put this operation on the
            // delayedToDoQueue, so return true so the operation is not put 
            // back on the toDoQueue
            return true;
          }

          // Register the newly-available Region's location.
          LOG.info("updating row " + regionInfo.getRegionNameAsString() +
              " in region " + Bytes.toString(metaRegionName) +
              " with startcode " + Bytes.toLong(startCode) + " and server " +
              serverAddress.toString());
          BatchUpdate b = new BatchUpdate(regionInfo.getRegionName());
          b.put(COL_SERVER, Bytes.toBytes(serverAddress.toString()));
          b.put(COL_STARTCODE, startCode);
          server.batchUpdate(metaRegionName, b, -1L);
          if (!this.historian.isOnline()) {
            // This is safest place to do the onlining of the historian in
            // the master.  When we get to here, we know there is a .META.
            // for the historian to go against.
            this.historian.online(this.master.getConfiguration());
          }
          this.historian.addRegionOpen(regionInfo, serverAddress);
          synchronized (master.regionManager) {
            if (isMetaTable) {
              // It's a meta region.
              MetaRegion m = new MetaRegion(new HServerAddress(serverAddress),
                  regionInfo.getRegionName(), regionInfo.getStartKey());
              if (!master.regionManager.isInitialMetaScanComplete()) {
                // Put it on the queue to be scanned for the first time.
                LOG.debug("Adding " + m.toString() + " to regions to scan");
                master.regionManager.addMetaRegionToScan(m);
              } else {
                // Add it to the online meta regions
                LOG.debug("Adding to onlineMetaRegions: " + m.toString());
                master.regionManager.putMetaRegionOnline(m);
                // Interrupting the Meta Scanner sleep so that it can
                // process regions right away
                master.regionManager.metaScannerThread.interrupt();
              }
            }
            // If updated successfully, remove from pending list.
            master.regionManager.removeRegion(regionInfo);
            return true;
          }
        }
    }.doWithRetries();
    return result == null ? true : result;
  }
}