/**
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
import org.apache.hadoop.hbase.master.handler.OpenedRegionHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MockServer;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestOpenedRegionHandler {

  private static final Log LOG = LogFactory
      .getLog(TestOpenedRegionHandler.class);

  private HBaseTestingUtility TEST_UTIL;
  private final int NUM_MASTERS = 1;
  private final int NUM_RS = 1;
  private Configuration conf;
  private Configuration resetConf;
  private ZooKeeperWatcher zkw;

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    TEST_UTIL = new HBaseTestingUtility(conf);
  }
  
  @After
  public void tearDown() throws Exception {
    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL = new HBaseTestingUtility(resetConf);
  }

  @Test
  public void testOpenedRegionHandlerOnMasterRestart() throws Exception {
    // Start the cluster
    log("Starting cluster");
    conf = HBaseConfiguration.create();
    resetConf = conf;
    conf.setInt("hbase.master.assignment.timeoutmonitor.period", 2000);
    conf.setInt("hbase.master.assignment.timeoutmonitor.timeout", 5000);
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    String tableName = "testOpenedRegionHandlerOnMasterRestart";
    MiniHBaseCluster cluster = createRegions(tableName);
    abortMaster(cluster);

    HRegionServer regionServer = cluster.getRegionServer(0);
    HRegion region = getRegionBeingServed(cluster, regionServer);

    // forcefully move a region to OPENED state in zk
    // Create a ZKW to use in the test
    zkw = HBaseTestingUtility.createAndForceNodeToOpenedState(TEST_UTIL,
        region, regionServer.getServerName());

    // Start up a new master
    log("Starting up a new master");
    cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    // Failover should be completed, now wait for no RIT
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);
  }
  @Test
  public void testShouldNotCompeleteOpenedRegionSuccessfullyIfVersionMismatches()
      throws Exception {
    try {
      int testIndex = 0;
      TEST_UTIL.startMiniZKCluster();
      final Server server = new MockServer(TEST_UTIL);
      HTableDescriptor htd = new HTableDescriptor(
          "testShouldNotCompeleteOpenedRegionSuccessfullyIfVersionMismatches");
      HRegionInfo hri = new HRegionInfo(htd.getName(),
          Bytes.toBytes(testIndex), Bytes.toBytes(testIndex + 1));
      HRegion region = HRegion.createHRegion(hri, HBaseTestingUtility
          .getTestDir(), TEST_UTIL.getConfiguration(), htd);
      assertNotNull(region);
      AssignmentManager am = Mockito.mock(AssignmentManager.class);
      when(am.isRegionInTransition(hri)).thenReturn(
          new RegionState(region.getRegionInfo(), RegionState.State.OPEN,
              System.currentTimeMillis(), server.getServerName()));
      // create a node with OPENED state
      zkw = HBaseTestingUtility.createAndForceNodeToOpenedState(TEST_UTIL,
          region, server.getServerName());
      when(am.getZKTable()).thenReturn(new ZKTable(zkw));
      Stat stat = new Stat();
      String nodeName = ZKAssign.getNodeName(zkw, region.getRegionInfo()
          .getEncodedName());
      ZKUtil.getDataAndWatch(zkw, nodeName, stat);

      // use the version for the OpenedRegionHandler
      OpenedRegionHandler handler = new OpenedRegionHandler(server, am, region
          .getRegionInfo(), server.getServerName(), stat.getVersion());
      // Once again overwrite the same znode so that the version changes.
      ZKAssign.transitionNode(zkw, region.getRegionInfo(), server
          .getServerName(), EventType.RS_ZK_REGION_OPENED,
          EventType.RS_ZK_REGION_OPENED, stat.getVersion());

      // Should not invoke assignmentmanager.regionOnline. If it is 
      // invoked as per current mocking it will throw null pointer exception.
      boolean expectedException = false;
      try {
        handler.process();
      } catch (Exception e) {
        expectedException = true;
      }
      assertFalse("The process method should not throw any exception.",
          expectedException);
      List<String> znodes = ZKUtil.listChildrenAndWatchForNewChildren(zkw,
          zkw.assignmentZNode);
      String regionName = znodes.get(0);
      assertEquals("The region should not be opened successfully.", regionName,
          region.getRegionInfo().getEncodedName());
    } finally {
      TEST_UTIL.shutdownMiniZKCluster();
    }
  }
  private MiniHBaseCluster createRegions(String tableName)
      throws InterruptedException, ZooKeeperConnectionException, IOException,
      KeeperException {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    zkw = new ZooKeeperWatcher(conf, "testOpenedRegionHandler", null);

    // Create a table with regions
    byte[] table = Bytes.toBytes(tableName);
    byte[] family = Bytes.toBytes("family");
    TEST_UTIL.createTable(table, family);

    //wait till the regions are online
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);

    return cluster;
  }
  private void abortMaster(MiniHBaseCluster cluster) {
    // Stop the master
    log("Aborting master");
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    log("Master has aborted");
  }
  private HRegion getRegionBeingServed(MiniHBaseCluster cluster,
      HRegionServer regionServer) {
    Collection<HRegion> onlineRegionsLocalContext = regionServer
        .getOnlineRegionsLocalContext();
    Iterator<HRegion> iterator = onlineRegionsLocalContext.iterator();
    HRegion region = null;
    while (iterator.hasNext()) {
      region = iterator.next();
      if (!region.getRegionInfo().isMetaRegion()
          && !region.getRegionInfo().isRootRegion()) {
        break;
      }
    }
    return region;
  }
  private void log(String msg) {
    LOG.debug("\n\nTRR: " + msg + "\n");
  }
}
