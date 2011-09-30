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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test whether region rebalancing works. (HBASE-71)
 */
public class TestRegionRebalancing {
  final Log LOG = LogFactory.getLog(this.getClass().getName());
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  HTable table;
  HTableDescriptor desc;
  private static final byte [] FAMILY_NAME = Bytes.toBytes("col");

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() {
    this.desc = new HTableDescriptor("test");
    this.desc.addFamily(new HColumnDescriptor(FAMILY_NAME));
  }

  /**
   * For HBASE-71. Try a few different configurations of starting and stopping
   * region servers to see if the assignment or regions is pretty balanced.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRebalanceOnRegionServerNumberChange()
  throws IOException, InterruptedException {
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    admin.createTable(this.desc, HBaseTestingUtility.KEYS);
    this.table = new HTable(UTIL.getConfiguration(), this.desc.getName());
    CatalogTracker ct = new CatalogTracker(UTIL.getConfiguration());
    ct.start();
    Map<HRegionInfo, ServerName> regions = null;
    try {
      regions = MetaReader.fullScan(ct);
    } finally {
      ct.stop();
    }
    for (Map.Entry<HRegionInfo, ServerName> e: regions.entrySet()) {
      LOG.info(e);
    }
    assertEquals("Test table should have right number of regions",
      HBaseTestingUtility.KEYS.length + 1/*One extra to account for start/end keys*/,
      this.table.getStartKeys().length);

    // verify that the region assignments are balanced to start out
    assertRegionsAreBalanced();

    // add a region server - total of 2
    LOG.info("Started second server=" +
      UTIL.getHbaseCluster().startRegionServer().getRegionServer().getServerName());
    UTIL.getHbaseCluster().getMaster().balance();
    assertRegionsAreBalanced();

    // add a region server - total of 3
    LOG.info("Started third server=" +
        UTIL.getHbaseCluster().startRegionServer().getRegionServer().getServerName());
    UTIL.getHbaseCluster().getMaster().balance();
    assertRegionsAreBalanced();

    // kill a region server - total of 2
    LOG.info("Stopped third server=" + UTIL.getHbaseCluster().stopRegionServer(2, false));
    UTIL.getHbaseCluster().waitOnRegionServer(2);
    UTIL.getHbaseCluster().getMaster().balance();
    assertRegionsAreBalanced();

    // start two more region servers - total of 4
    LOG.info("Readding third server=" +
        UTIL.getHbaseCluster().startRegionServer().getRegionServer().getServerName());
    LOG.info("Added fourth server=" +
        UTIL.getHbaseCluster().startRegionServer().getRegionServer().getServerName());
    UTIL.getHbaseCluster().getMaster().balance();
    assertRegionsAreBalanced();

    for (int i = 0; i < 6; i++){
      LOG.info("Adding " + (i + 5) + "th region server");
      UTIL.getHbaseCluster().startRegionServer();
    }
    UTIL.getHbaseCluster().getMaster().balance();
    assertRegionsAreBalanced();
  }

  /** figure out how many regions are currently being served. */
  private int getRegionCount() throws IOException {
    int total = 0;
    for (HRegionServer server : getOnlineRegionServers()) {
      total += server.getOnlineRegions().size();
    }
    return total;
  }

  /**
   * Determine if regions are balanced. Figure out the total, divide by the
   * number of online servers, then test if each server is +/- 1 of average
   * rounded up.
   */
  private void assertRegionsAreBalanced() throws IOException {
    // TODO: Fix this test.  Old balancer used to run with 'slop'.  New
    // balancer does not.
    boolean success = false;
    float slop = (float)UTIL.getConfiguration().getFloat("hbase.regions.slop", 0.1f);
    if (slop <= 0) slop = 1;

    for (int i = 0; i < 5; i++) {
      success = true;
      // make sure all the regions are reassigned before we test balance
      waitForAllRegionsAssigned();

      int regionCount = getRegionCount();
      List<HRegionServer> servers = getOnlineRegionServers();
      double avg = UTIL.getHbaseCluster().getMaster().getAverageLoad();
      int avgLoadPlusSlop = (int)Math.ceil(avg * (1 + slop));
      int avgLoadMinusSlop = (int)Math.floor(avg * (1 - slop)) - 1;
      LOG.debug("There are " + servers.size() + " servers and " + regionCount
        + " regions. Load Average: " + avg + " low border: " + avgLoadMinusSlop
        + ", up border: " + avgLoadPlusSlop + "; attempt: " + i);

      for (HRegionServer server : servers) {
        int serverLoad = server.getOnlineRegions().size();
        LOG.debug(server.getServerName() + " Avg: " + avg + " actual: " + serverLoad);
        if (!(avg > 2.0 && serverLoad <= avgLoadPlusSlop
            && serverLoad >= avgLoadMinusSlop)) {
          LOG.debug(server.getServerName() + " Isn't balanced!!! Avg: " + avg +
              " actual: " + serverLoad + " slop: " + slop);
          success = false;
        }
      }

      if (!success) {
        // one or more servers are not balanced. sleep a little to give it a
        // chance to catch up. then, go back to the retry loop.
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {}

        UTIL.getHbaseCluster().getMaster().balance();
        continue;
      }

      // if we get here, all servers were balanced, so we should just return.
      return;
    }
    // if we get here, we tried 5 times and never got to short circuit out of
    // the retry loop, so this is a failure.
    fail("After 5 attempts, region assignments were not balanced.");
  }

  private List<HRegionServer> getOnlineRegionServers() {
    List<HRegionServer> list = new ArrayList<HRegionServer>();
    for (JVMClusterUtil.RegionServerThread rst :
        UTIL.getHbaseCluster().getRegionServerThreads()) {
      if (rst.getRegionServer().isOnline()) {
        list.add(rst.getRegionServer());
      }
    }
    return list;
  }

  /**
   * Wait until all the regions are assigned.
   */
  private void waitForAllRegionsAssigned() throws IOException {
    while (getRegionCount() < 22) {
    // while (!cluster.getMaster().allRegionsAssigned()) {
      LOG.debug("Waiting for there to be 22 regions, but there are " + getRegionCount() + " right now.");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
    }
  }
}