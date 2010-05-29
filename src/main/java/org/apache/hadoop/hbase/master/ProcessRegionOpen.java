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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * ProcessRegionOpen is instantiated when a region server reports that it is
 * serving a region. This applies to all meta and user regions except the
 * root region which is handled specially.
 */
class ProcessRegionOpen extends ProcessRegionStatusChange {
  protected final HServerInfo serverInfo;

  /**
   * @param master
   * @param info
   * @param regionInfo
   */
  public ProcessRegionOpen(HMaster master, HServerInfo info,
      HRegionInfo regionInfo) {
    super(master, regionInfo);
    if (info == null) {
      throw new NullPointerException("HServerInfo cannot be null; " +
        "hbase-958 debugging");
    }
    this.serverInfo = info;
  }

  @Override
  public String toString() {
    return "PendingOpenOperation from " + serverInfo.getServerName();
  }

  @Override
  protected boolean process() throws IOException {
    // TODO: The below check is way too convoluted!!!
    if (!metaRegionAvailable()) {
      // We can't proceed unless the meta region we are going to update
      // is online. metaRegionAvailable() has put this operation on the
      // delayedToDoQueue, so return true so the operation is not put
      // back on the toDoQueue
      return true;
    }
    HRegionInterface server =
        master.getServerConnection().getHRegionConnection(getMetaRegion().getServer());
    LOG.info(regionInfo.getRegionNameAsString() + " open on " +
      serverInfo.getServerName());

    // Register the newly-available Region's location.
    Put p = new Put(regionInfo.getRegionName());
    p.add(CATALOG_FAMILY, SERVER_QUALIFIER,
      Bytes.toBytes(serverInfo.getHostnamePort()));
    p.add(CATALOG_FAMILY, STARTCODE_QUALIFIER,
      Bytes.toBytes(serverInfo.getStartCode()));
    server.put(metaRegionName, p);
    LOG.info("Updated row " + regionInfo.getRegionNameAsString() +
      " in region " + Bytes.toString(metaRegionName) + " with startcode=" +
      serverInfo.getStartCode() + ", server=" + serverInfo.getHostnamePort());
    synchronized (master.getRegionManager()) {
      if (isMetaTable) {
        // It's a meta region.
        MetaRegion m =
            new MetaRegion(new HServerAddress(serverInfo.getServerAddress()),
                regionInfo);
        if (!master.getRegionManager().isInitialMetaScanComplete()) {
          // Put it on the queue to be scanned for the first time.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding " + m.toString() + " to regions to scan");
          }
          master.getRegionManager().addMetaRegionToScan(m);
        } else {
          // Add it to the online meta regions
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding to onlineMetaRegions: " + m.toString());
          }
          master.getRegionManager().putMetaRegionOnline(m);
          // Interrupting the Meta Scanner sleep so that it can
          // process regions right away
          master.getRegionManager().metaScannerThread.triggerNow();
        }
      }
      // If updated successfully, remove from pending list if the state
      // is consistent. For example, a disable could be called before the
      // synchronization.
      if(master.getRegionManager().
          isOfflined(regionInfo.getRegionNameAsString())) {
        LOG.warn("We opened a region while it was asked to be closed.");
      } else {
        master.getRegionManager().removeRegion(regionInfo);
      }
      return true;
    }
  }

  @Override
  protected int getPriority() {
    return 0; // highest priority
  }
}
