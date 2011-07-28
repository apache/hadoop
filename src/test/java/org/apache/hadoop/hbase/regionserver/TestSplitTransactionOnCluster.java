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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.master.handler.SplitRegionHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Like {@link TestSplitTransaction} in that we're testing {@link SplitTransaction}
 * only the below tests are against a running cluster where {@link TestSplitTransaction}
 * is tests against a bare {@link HRegion}.
 */
public class TestSplitTransactionOnCluster {
  private static final Log LOG =
    LogFactory.getLog(TestSplitTransactionOnCluster.class);
  private HBaseAdmin admin = null;
  private MiniHBaseCluster cluster = null;
  private static final int NB_SERVERS = 2;

  private static final HBaseTestingUtility TESTING_UTIL =
    new HBaseTestingUtility();

  @BeforeClass public static void before() throws Exception {
    TESTING_UTIL.getConfiguration().setInt("hbase.balancer.period", 60000);
    // Needed because some tests have splits happening on RS that are killed
    // We don't want to wait 3min for the master to figure it out
    TESTING_UTIL.getConfiguration().setInt(
        "hbase.master.assignment.timeoutmonitor.timeout", 4000);
    TESTING_UTIL.startMiniCluster(NB_SERVERS);
  }

  @AfterClass public static void after() throws Exception {
    TESTING_UTIL.shutdownMiniCluster();
  }

  @Before public void setup() throws IOException {
    TESTING_UTIL.ensureSomeRegionServersAvailable(NB_SERVERS);
    this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());
    this.cluster = TESTING_UTIL.getMiniHBaseCluster();
  }

  private HRegionInfo getAndCheckSingleTableRegion(final List<HRegion> regions) {
    assertEquals(1, regions.size());
    return regions.get(0).getRegionInfo();
  }

  /**
   * A test that intentionally has master fail the processing of the split message.
   * Tests that the regionserver split ephemeral node gets cleaned up if it
   * crashes and that after we process server shutdown, the daughters are up on
   * line.
   * @throws IOException
   * @throws InterruptedException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test (timeout = 300000) public void testRSSplitEphemeralsDisappearButDaughtersAreOnlinedAfterShutdownHandling()
  throws IOException, InterruptedException, NodeExistsException, KeeperException {
    final byte [] tableName =
      Bytes.toBytes("ephemeral");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.balanceSwitch(false);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = server.getOnlineRegions().size();
      // Now, before we split, set special flag in master, a flag that has
      // it FAIL the processing of split.
      SplitRegionHandler.TEST_SKIP = true;
      // Now try splitting and it should work.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertTrue(daughters.size() >= 2);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(t.getConnection().getZooKeeperWatcher(),
        hri.getEncodedName());
      Stat stats =
        t.getConnection().getZooKeeperWatcher().getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats=" + stats);
      RegionTransitionData rtd =
        ZKAssign.getData(t.getConnection().getZooKeeperWatcher(),
          hri.getEncodedName());
      // State could be SPLIT or SPLITTING.
      assertTrue(rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLIT) ||
        rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLITTING));
      // Now crash the server
      cluster.abortRegionServer(tableRegionIndex);
      waitUntilRegionServerDead();

      // Wait till regions are back on line again.
      while(cluster.getRegions(tableName).size() < daughters.size()) {
        LOG.info("Waiting for repair to happen");
        Thread.sleep(1000);
      }
      // Assert daughters are online.
      regions = cluster.getRegions(tableName);
      for (HRegion r: regions) {
        assertTrue(daughters.contains(r));
      }
      // Finally assert that the ephemeral SPLIT znode was cleaned up.
      stats = t.getConnection().getZooKeeperWatcher().getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE AFTER SERVER ABORT, path=" + path + ", stats=" + stats);
      assertTrue(stats == null);
    } finally {
      // Set this flag back.
      SplitRegionHandler.TEST_SKIP = false;
      admin.balanceSwitch(true);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  @Test (timeout = 300000) public void testExistingZnodeBlocksSplitAndWeRollback()
  throws IOException, InterruptedException, NodeExistsException, KeeperException {
    final byte [] tableName =
      Bytes.toBytes("testExistingZnodeBlocksSplitAndWeRollback");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.balanceSwitch(false);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = server.getOnlineRegions().size();
      // Insert into zk a blocking znode, a znode of same name as region
      // so it gets in way of our splitting.
      ZKAssign.createNodeClosing(t.getConnection().getZooKeeperWatcher(),
        hri, new ServerName("any.old.server", 1234, -1));
      // Now try splitting.... should fail.  And each should successfully
      // rollback.
      this.admin.split(hri.getRegionNameAsString());
      this.admin.split(hri.getRegionNameAsString());
      this.admin.split(hri.getRegionNameAsString());
      // Wait around a while and assert count of regions remains constant.
      for (int i = 0; i < 10; i++) {
        Thread.sleep(100);
        assertEquals(regionCount, server.getOnlineRegions().size());
      }
      // Now clear the zknode
      ZKAssign.deleteClosingNode(t.getConnection().getZooKeeperWatcher(), hri);
      // Now try splitting and it should work.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertTrue(daughters.size() >= 2);
      // OK, so split happened after we cleared the blocking node.
    } finally {
      admin.balanceSwitch(true);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  /**
   * Messy test that simulates case where SplitTransactions fails to add one
   * of the daughters up into the .META. table before crash.  We're testing
   * fact that the shutdown handler will fixup the missing daughter region
   * adding it back into .META.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout = 300000) public void testShutdownSimpleFixup()
  throws IOException, InterruptedException {
    final byte [] tableName = Bytes.toBytes("testShutdownSimpleFixup");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.balanceSwitch(false);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = server.getOnlineRegions().size();
      // Now split.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertTrue(daughters.size() >= 2);
      // Remove one of the daughters from .META. to simulate failed insert of
      // daughter region up into .META.
      removeDaughterFromMeta(daughters.get(0).getRegionName());
      // Now crash the server
      cluster.abortRegionServer(tableRegionIndex);
      waitUntilRegionServerDead();
      // Wait till regions are back on line again.
      while(cluster.getRegions(tableName).size() < daughters.size()) {
        LOG.info("Waiting for repair to happen");
        Thread.sleep(1000);
      }
      // Assert daughters are online.
      regions = cluster.getRegions(tableName);
      for (HRegion r: regions) {
        assertTrue(daughters.contains(r));
      }
    } finally {
      admin.balanceSwitch(true);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  /**
   * Test that if daughter split on us, we won't do the shutdown handler fixup
   * just because we can't find the immediate daughter of an offlined parent.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000) public void testShutdownFixupWhenDaughterHasSplit()
  throws IOException, InterruptedException {
    final byte [] tableName =
      Bytes.toBytes("testShutdownFixupWhenDaughterHasSplit");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.balanceSwitch(false);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = server.getOnlineRegions().size();
      // Now split.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertTrue(daughters.size() >= 2);
      // Now split one of the daughters.
      regionCount = server.getOnlineRegions().size();
      HRegionInfo daughter = daughters.get(0).getRegionInfo();
      // Compact first to ensure we have cleaned up references -- else the split
      // will fail.
      this.admin.compact(daughter.getRegionName());
      daughters = cluster.getRegions(tableName);
      HRegion daughterRegion = null;
      for (HRegion r: daughters) {
        if (r.getRegionInfo().equals(daughter)) daughterRegion = r;
      }
      assertTrue(daughterRegion != null);
      while (true) {
        if (!daughterRegion.hasReferences()) break;
        Threads.sleep(100);
      }
      split(daughter, server, regionCount);
      // Get list of daughters
      daughters = cluster.getRegions(tableName);
      // Now crash the server
      cluster.abortRegionServer(tableRegionIndex);
      waitUntilRegionServerDead();
      // Wait till regions are back on line again.
      while(cluster.getRegions(tableName).size() < daughters.size()) {
        LOG.info("Waiting for repair to happen");
        Thread.sleep(1000);
      }
      // Assert daughters are online and ONLY the original daughters -- that
      // fixup didn't insert one during server shutdown recover.
      regions = cluster.getRegions(tableName);
      assertEquals(daughters.size(), regions.size());
      for (HRegion r: regions) {
        assertTrue(daughters.contains(r));
      }
    } finally {
      admin.balanceSwitch(true);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  private void split(final HRegionInfo hri, final HRegionServer server,
      final int regionCount)
  throws IOException, InterruptedException {
    this.admin.split(hri.getRegionNameAsString());
    while (server.getOnlineRegions().size() <= regionCount) {
      LOG.debug("Waiting on region to split");
      Thread.sleep(100);
    }
  }

  private void removeDaughterFromMeta(final byte [] regionName) throws IOException {
    HTable metaTable =
      new HTable(TESTING_UTIL.getConfiguration(), HConstants.META_TABLE_NAME);
    Delete d = new Delete(regionName);
    LOG.info("Deleted " + Bytes.toString(regionName));
    metaTable.delete(d);
  }

  /**
   * Ensure single table region is not on same server as the single .META. table
   * region.
   * @param admin
   * @param hri
   * @return Index of the server hosting the single table region
   * @throws UnknownRegionException
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws InterruptedException
   */
  private int ensureTableRegionNotOnSameServerAsMeta(final HBaseAdmin admin,
      final HRegionInfo hri)
  throws UnknownRegionException, MasterNotRunningException,
  ZooKeeperConnectionException, InterruptedException {
    MiniHBaseCluster cluster = TESTING_UTIL.getMiniHBaseCluster();
    // Now make sure that the table region is not on same server as that hosting
    // .META.  We don't want .META. replay polluting our test when we later crash
    // the table region serving server.
    int metaServerIndex = cluster.getServerWithMeta();
    assertTrue(metaServerIndex != -1);
    HRegionServer metaRegionServer = cluster.getRegionServer(metaServerIndex);
    int tableRegionIndex = cluster.getServerWith(hri.getRegionName());
    assertTrue(tableRegionIndex != -1);
    HRegionServer tableRegionServer = cluster.getRegionServer(tableRegionIndex);
    if (metaRegionServer.getServerName().equals(tableRegionServer.getServerName())) {
      HRegionServer hrs = getOtherRegionServer(cluster, metaRegionServer);
      LOG.info("Moving " + hri.getRegionNameAsString() + " to " +
        hrs.getServerName() + "; metaServerIndex=" + metaServerIndex);
      admin.move(hri.getEncodedNameAsBytes(), hrs.getServerName().getBytes());
    }
    // Wait till table region is up on the server that is NOT carrying .META..
    while (true) {
      tableRegionIndex = cluster.getServerWith(hri.getRegionName());
      if (tableRegionIndex != -1 && tableRegionIndex != metaServerIndex) break;
      LOG.debug("Waiting on region move off the .META. server; current index " +
        tableRegionIndex + " and metaServerIndex=" + metaServerIndex);
      Thread.sleep(100);
    }
    // Verify for sure table region is not on same server as .META.
    tableRegionIndex = cluster.getServerWith(hri.getRegionName());
    assertTrue(tableRegionIndex != -1);
    assertNotSame(metaServerIndex, tableRegionIndex);
    return tableRegionIndex;
  }

  /**
   * Find regionserver other than the one passed.
   * Can't rely on indexes into list of regionservers since crashed servers
   * occupy an index.
   * @param cluster
   * @param notThisOne
   * @return A regionserver that is not <code>notThisOne</code> or null if none
   * found
   */
  private HRegionServer getOtherRegionServer(final MiniHBaseCluster cluster,
      final HRegionServer notThisOne) {
    for (RegionServerThread rst: cluster.getRegionServerThreads()) {
      HRegionServer hrs = rst.getRegionServer();
      if (hrs.getServerName().equals(notThisOne.getServerName())) continue;
      if (hrs.isStopping() || hrs.isStopped()) continue;
      return hrs;
    }
    return null;
  }

  private void printOutRegions(final HRegionServer hrs, final String prefix)
      throws IOException {
    List<HRegionInfo> regions = hrs.getOnlineRegions();
    for (HRegionInfo region: regions) {
      LOG.info(prefix + region.getRegionNameAsString());
    }
  }

  private void waitUntilRegionServerDead() throws InterruptedException {
    // Wait until the master processes the RS shutdown
    while (cluster.getMaster().getClusterStatus().
        getServers().size() == NB_SERVERS) {
      LOG.info("Waiting on server to go down");
      Thread.sleep(100);
    }
  }
}
