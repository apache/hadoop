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
package org.apache.hadoop.hbase;


import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the draining servers feature.
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-4298">HBASE-4298</a>
 */
public class TestDrainingServer {
  private static final Log LOG = LogFactory.getLog(TestDrainingServer.class);
  private static final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private static final byte [] TABLENAME = Bytes.toBytes("t");
  private static final byte [] FAMILY = Bytes.toBytes("f");
  private static final int COUNT_OF_REGIONS = HBaseTestingUtility.KEYS.length;

  /**
   * Spin up a cluster with a bunch of regions on it.
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(5);
    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    htd.addFamily(new HColumnDescriptor(FAMILY));
    TEST_UTIL.createMultiRegionsInMeta(TEST_UTIL.getConfiguration(), htd,
        HBaseTestingUtility.KEYS);
    // Make a mark for the table in the filesystem.
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    FSTableDescriptors.
      createTableDescriptor(fs, FSUtils.getRootDir(TEST_UTIL.getConfiguration()), htd);
    // Assign out the regions we just created.
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.disableTable(TABLENAME);
    admin.enableTable(TABLENAME);
    // Assert that every regionserver has some regions on it.
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = cluster.getRegionServer(i);
      Assert.assertFalse(hrs.getOnlineRegions().isEmpty());
    }
  }

  private static HRegionServer setDrainingServer(final HRegionServer hrs)
  throws KeeperException {
    LOG.info("Making " + hrs.getServerName() + " the draining server; " +
      "it has " + hrs.getNumberOfOnlineRegions() + " online regions");
    ZooKeeperWatcher zkw = hrs.getZooKeeper();
    String hrsDrainingZnode =
      ZKUtil.joinZNode(zkw.drainingZNode, hrs.getServerName().toString());
    ZKUtil.createWithParents(zkw, hrsDrainingZnode);
    return hrs;
  }

  private static HRegionServer unsetDrainingServer(final HRegionServer hrs)
  throws KeeperException {
    ZooKeeperWatcher zkw = hrs.getZooKeeper();
    String hrsDrainingZnode =
      ZKUtil.joinZNode(zkw.drainingZNode, hrs.getServerName().toString());
    ZKUtil.deleteNode(zkw, hrsDrainingZnode);
    return hrs;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test adding server to draining servers and then move regions off it.
   * Make sure that no regions are moved back to the draining server.
   * @throws IOException 
   * @throws KeeperException 
   */
  @Test  // (timeout=30000)
  public void testDrainingServerOffloading()
  throws IOException, KeeperException {
    // I need master in the below.
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    HRegionInfo hriToMoveBack = null;
    // Set first server as draining server.
    HRegionServer drainingServer =
      setDrainingServer(TEST_UTIL.getMiniHBaseCluster().getRegionServer(0));
    try {
      final int regionsOnDrainingServer =
        drainingServer.getNumberOfOnlineRegions();
      Assert.assertTrue(regionsOnDrainingServer > 0);
      List<HRegionInfo> hris = drainingServer.getOnlineRegions();
      for (HRegionInfo hri : hris) {
        // Pass null and AssignmentManager will chose a random server BUT it
        // should exclude draining servers.
        master.move(hri.getEncodedNameAsBytes(), null);
        // Save off region to move back.
        hriToMoveBack = hri;
      }
      // Wait for regions to come back on line again.
      waitForAllRegionsOnline();
      Assert.assertEquals(0, drainingServer.getNumberOfOnlineRegions());
    } finally {
      unsetDrainingServer(drainingServer);
    }
    // Now we've unset the draining server, we should be able to move a region
    // to what was the draining server.
    master.move(hriToMoveBack.getEncodedNameAsBytes(),
      Bytes.toBytes(drainingServer.getServerName().toString()));
    // Wait for regions to come back on line again.
    waitForAllRegionsOnline();
    Assert.assertEquals(1, drainingServer.getNumberOfOnlineRegions());
  }

  /**
   * Test that draining servers are ignored even after killing regionserver(s).
   * Verify that the draining server is not given any of the dead servers regions.
   * @throws KeeperException
   * @throws IOException
   */
  @Test  (timeout=30000)
  public void testDrainingServerWithAbort() throws KeeperException, IOException {
    // Add first server to draining servers up in zk.
    HRegionServer drainingServer =
      setDrainingServer(TEST_UTIL.getMiniHBaseCluster().getRegionServer(0));
    try {
      final int regionsOnDrainingServer =
        drainingServer.getNumberOfOnlineRegions();
      Assert.assertTrue(regionsOnDrainingServer > 0);
      // Kill a few regionservers.
      int aborted = 0;
      final int numberToAbort = 2;
      for (int i = 1; i < TEST_UTIL.getMiniHBaseCluster().countServedRegions(); i++) {
        HRegionServer hrs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(i);
        if (hrs.getServerName().equals(drainingServer.getServerName())) continue;
        hrs.abort("Aborting");
        aborted++;
        if (aborted >= numberToAbort) break;
      }
      // Wait for regions to come back on line again.
      waitForAllRegionsOnline();
      // Assert the draining server still has the same number of regions.
      Assert.assertEquals(regionsOnDrainingServer,
        drainingServer.getNumberOfOnlineRegions());
    } finally {
      unsetDrainingServer(drainingServer);
    }
  }

  private void waitForAllRegionsOnline() {
    while (TEST_UTIL.getMiniHBaseCluster().getMaster().
        getAssignmentManager().isRegionsInTransition()) {
      Threads.sleep(10);
    }
    // Wait for regions to come back on line again.
    while (!isAllRegionsOnline()) {
      Threads.sleep(10);
    }
  }

  private boolean isAllRegionsOnline() {
    return TEST_UTIL.getMiniHBaseCluster().countServedRegions() ==
      (COUNT_OF_REGIONS + 2 /*catalog regions*/);
  }
}
