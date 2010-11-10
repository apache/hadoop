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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

public class HBaseFsckRepair {

  public static void fixDupeAssignment(Configuration conf, HRegionInfo region,
      List<HServerAddress> servers)
  throws IOException {

    HRegionInfo actualRegion = new HRegionInfo(region);

    // Clear status in master and zk
    clearInMaster(conf, actualRegion);
    clearInZK(conf, actualRegion);

    // Close region on the servers
    for(HServerAddress server : servers) {
      closeRegion(conf, server, actualRegion);
    }

    // It's unassigned so fix it as such
    fixUnassigned(conf, actualRegion);
  }

  public static void fixUnassigned(Configuration conf, HRegionInfo region)
  throws IOException {

    HRegionInfo actualRegion = new HRegionInfo(region);

    // Clear status in master and zk
    clearInMaster(conf, actualRegion);
    clearInZK(conf, actualRegion);

    // Clear assignment in META or ROOT
    clearAssignment(conf, actualRegion);
  }

  private static void clearInMaster(Configuration conf, HRegionInfo region)
  throws IOException {
    System.out.println("Region being cleared in master: " + region);
    HMasterInterface master = HConnectionManager.getConnection(conf).getMaster();
    long masterVersion =
      master.getProtocolVersion("org.apache.hadoop.hbase.ipc.HMasterInterface", 25);
    System.out.println("Master protocol version: " + masterVersion);
    try {
      // TODO: Do we want to do it this way?
      //       Better way is to tell master to fix the issue itself?
      //       That way it can use in-memory state to determine best plan
//      master.clearFromTransition(region);
    } catch (Exception e) {}
  }

  private static void clearInZK(Configuration conf, HRegionInfo region)
  throws IOException {
    ZooKeeperWatcher zkw =
      HConnectionManager.getConnection(conf).getZooKeeperWatcher();
    try {
      ZKAssign.deleteNodeFailSilent(zkw, region);
    } catch (KeeperException e) {
      throw new IOException("Unexpected ZK exception", e);
    }
  }

  private static void closeRegion(Configuration conf, HServerAddress server,
      HRegionInfo region)
  throws IOException {
    HRegionInterface rs =
      HConnectionManager.getConnection(conf).getHRegionConnection(server);
    rs.closeRegion(region, false);
  }

  private static void clearAssignment(Configuration conf,
      HRegionInfo region)
  throws IOException {
    HTable ht = null;
    if (region.isMetaTable()) {
      // Clear assignment in ROOT
      ht = new HTable(conf, HConstants.ROOT_TABLE_NAME);
    }
    else {
      // Clear assignment in META
      ht = new HTable(conf, HConstants.META_TABLE_NAME);
    }
    Delete del = new Delete(region.getRegionName());
    del.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
    del.deleteColumns(HConstants.CATALOG_FAMILY,
        HConstants.STARTCODE_QUALIFIER);
    ht.delete(del);
  }
}