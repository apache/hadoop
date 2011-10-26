/**
 * Copyright 2011 The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

public class TestMasterRestartAfterDisablingTable {

  private static final Log LOG = LogFactory.getLog(TestMasterRestartAfterDisablingTable.class);

  @Test
  public void testForCheckingIfEnableAndDisableWorksFineAfterSwitch()
      throws Exception {
    final int NUM_MASTERS = 2;
    final int NUM_RS = 1;
    final int NUM_REGIONS_TO_CREATE = 4;

    // Start the cluster
    log("Starting cluster");
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.master.assignment.timeoutmonitor.period", 2000);
    conf.setInt("hbase.master.assignment.timeoutmonitor.timeout", 5000);
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "testmasterRestart", null);
    HMaster master = cluster.getMaster();

    // Create a table with regions
    byte[] table = Bytes.toBytes("tableRestart");
    byte[] family = Bytes.toBytes("family");
    log("Creating table with " + NUM_REGIONS_TO_CREATE + " regions");
    HTable ht = TEST_UTIL.createTable(table, family);
    int numRegions = TEST_UTIL.createMultiRegions(conf, ht, family,
        NUM_REGIONS_TO_CREATE);
    numRegions += 2; // catalogs
    log("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    log("Disabling table\n");
    TEST_UTIL.getHBaseAdmin().disableTable(table);

    NavigableSet<String> regions = getAllOnlineRegions(cluster);
    assertEquals(
        "The number of regions for the table tableRestart should be 0 and only"
            + "the catalog tables should be present.", 2, regions.size());

    List<MasterThread> masterThreads = cluster.getMasterThreads();
    MasterThread activeMaster = null;
    if (masterThreads.get(0).getMaster().isActiveMaster()) {
      activeMaster = masterThreads.get(0);
    } else {
      activeMaster = masterThreads.get(1);
    }
    activeMaster.getMaster().stop(
        "stopping the active master so that the backup can become active");
    cluster.hbaseCluster.waitOnMaster(activeMaster);
    cluster.waitForActiveAndReadyMaster();

    log("Enabling table\n");
    TEST_UTIL.getHBaseAdmin().enableTable(table);
    log("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    log("Verifying there are " + numRegions + " assigned on cluster\n");
    regions = getAllOnlineRegions(cluster);
    assertEquals(
        "The assigned regions were not onlined after master switch except for the catalog tables.",
        6, regions.size());
    TEST_UTIL.shutdownMiniCluster();
  }

  private void log(String msg) {
    LOG.debug("\n\nTRR: " + msg + "\n");
  }

  private void blockUntilNoRIT(ZooKeeperWatcher zkw, HMaster master)
      throws KeeperException, InterruptedException {
    ZKAssign.blockUntilNoRIT(zkw);
    master.assignmentManager.waitUntilNoRegionsInTransition(60000);
  }

  private NavigableSet<String> getAllOnlineRegions(MiniHBaseCluster cluster)
      throws IOException {
    NavigableSet<String> online = new TreeSet<String>();
    for (RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
      for (HRegionInfo region : rst.getRegionServer().getOnlineRegions()) {
        online.add(region.getRegionNameAsString());
      }
    }
    return online;
  }
}
