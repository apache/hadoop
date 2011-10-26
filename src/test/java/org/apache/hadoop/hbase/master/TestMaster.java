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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventHandler.EventHandlerListener;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Joiner;

import static org.junit.Assert.*;

public class TestMaster {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestMaster.class);
  private static final byte[] TABLENAME = Bytes.toBytes("TestMaster");
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMasterOpsWhileSplitting() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();

    TEST_UTIL.createTable(TABLENAME, FAMILYNAME);
    TEST_UTIL.loadTable(new HTable(TEST_UTIL.getConfiguration(), TABLENAME),
      FAMILYNAME);

    List<Pair<HRegionInfo, ServerName>> tableRegions =
      MetaReader.getTableRegionsAndLocations(m.getCatalogTracker(),
          Bytes.toString(TABLENAME));
    LOG.info("Regions after load: " + Joiner.on(',').join(tableRegions));
    assertEquals(1, tableRegions.size());
    assertArrayEquals(HConstants.EMPTY_START_ROW,
        tableRegions.get(0).getFirst().getStartKey());
    assertArrayEquals(HConstants.EMPTY_END_ROW,
        tableRegions.get(0).getFirst().getEndKey());

    // Now trigger a split and stop when the split is in progress
    CountDownLatch split = new CountDownLatch(1);
    CountDownLatch proceed = new CountDownLatch(1);
    RegionSplitListener list = new RegionSplitListener(split, proceed);
    cluster.getMaster().executorService.
      registerListener(EventType.RS_ZK_REGION_SPLIT, list);

    LOG.info("Splitting table");
    admin.split(TABLENAME);
    LOG.info("Waiting for split result to be about to open");
    split.await(60, TimeUnit.SECONDS);
    try {
      LOG.info("Making sure we can call getTableRegions while opening");
      tableRegions = MetaReader.getTableRegionsAndLocations(m.getCatalogTracker(),
        TABLENAME, false);

      LOG.info("Regions: " + Joiner.on(',').join(tableRegions));
      // We have three regions because one is split-in-progress
      assertEquals(3, tableRegions.size());
      LOG.info("Making sure we can call getTableRegionClosest while opening");
      Pair<HRegionInfo, ServerName> pair =
        m.getTableRegionForRow(TABLENAME, Bytes.toBytes("cde"));
      LOG.info("Result is: " + pair);
      Pair<HRegionInfo, ServerName> tableRegionFromName =
        MetaReader.getRegion(m.getCatalogTracker(),
            pair.getFirst().getRegionName());
      assertEquals(tableRegionFromName.getFirst(), pair.getFirst());
    } finally {
      proceed.countDown();
    }
  }

  static class RegionSplitListener implements EventHandlerListener {
    CountDownLatch split, proceed;

    public RegionSplitListener(CountDownLatch split, CountDownLatch proceed) {
      this.split = split;
      this.proceed = proceed;
    }

    @Override
    public void afterProcess(EventHandler event) {
      if (event.getEventType() != EventType.RS_ZK_REGION_SPLIT) {
        return;
      }
      try {
        split.countDown();
        proceed.await(60, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
      return;
    }

    @Override
    public void beforeProcess(EventHandler event) {
    }
  }
}