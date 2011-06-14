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
package org.apache.hadoop.hbase.client;

import junit.framework.AssertionFailedError;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.migration.HRegionInfo090x;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;

import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hadoop.hbase.util.Writables;

import java.util.List;

public class TestMetaMigration {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster miniHBaseCluster = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    miniHBaseCluster = TEST_UTIL.startMiniCluster(1);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testHRegionInfoForMigration() throws Exception {
    LOG.info("Starting testHRegionInfoForMigration");
    HTableDescriptor htd = new HTableDescriptor("testMetaMigration");
    htd.addFamily(new HColumnDescriptor("family"));
    HRegionInfo090x hrim = new HRegionInfo090x(htd, HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_END_ROW);
    LOG.info("INFO 1 = " + hrim);
    byte[] bytes = Writables.getBytes(hrim);
    LOG.info(" BYtes.toString = " + Bytes.toString(bytes));
    LOG.info(" HTD bytes = " + Bytes.toString(Writables.getBytes(hrim.getTableDesc())));
    HRegionInfo090x info = Writables.getHRegionInfoForMigration(bytes);
    LOG.info("info = " + info);
    LOG.info("END testHRegionInfoForMigration");

  }

  @Test
  public void testMetaUpdatedFlagInROOT() throws Exception {
    LOG.info("Starting testMetaUpdatedFlagInROOT");
    boolean metaUpdated = miniHBaseCluster.getMaster().isMetaHRIUpdated();
    assertEquals(true, metaUpdated);
    LOG.info("END testMetaUpdatedFlagInROOT");
  }

  @Test
  public void testUpdatesOnMetaWithLegacyHRI() throws Exception {
    LOG.info("Starting testMetaWithLegacyHRI");
    final byte[] FAMILY = Bytes.toBytes("family");
    HTableDescriptor htd = new HTableDescriptor("testMetaMigration");
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
      htd.addFamily(hcd);
    Configuration conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.createMultiRegionsWithLegacyHRI(conf, htd, FAMILY,
        new byte[][]{
            HConstants.EMPTY_START_ROW,
            Bytes.toBytes("region_a"),
            Bytes.toBytes("region_b")});
    CatalogTracker ct = miniHBaseCluster.getMaster().getCatalogTracker();
    // just for this test set it to false.
    MetaEditor.updateRootWithMetaMigrationStatus(ct, false);
    MetaReader.fullScanMetaAndPrint(ct);
    LOG.info("MEta Print completed.testUpdatesOnMetaWithLegacyHRI");

    List<HTableDescriptor> htds = MetaEditor.updateMetaWithNewRegionInfo(
          TEST_UTIL.getHBaseCluster().getMaster());
    assertEquals(3, htds.size());
    // Assert that the flag in ROOT is updated to reflect the correct status
    boolean metaUpdated = miniHBaseCluster.getMaster().isMetaHRIUpdated();
    assertEquals(true, metaUpdated);
    LOG.info("END testMetaWithLegacyHRI");

  }

  //@Test
  public void dtestUpdatesOnMetaWithNewHRI() throws Exception {
    LOG.info("Starting testMetaWithLegacyHRI");
    final byte[] FAMILY = Bytes.toBytes("family");
    HTableDescriptor htd = new HTableDescriptor("testMetaMigration");
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
      htd.addFamily(hcd);
    Configuration conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.createMultiRegionsWithNewHRI(conf, htd, FAMILY,
        new byte[][]{
            HConstants.EMPTY_START_ROW,
            Bytes.toBytes("region_a"),
            Bytes.toBytes("region_b")});
    List<HTableDescriptor> htds = MetaEditor.updateMetaWithNewRegionInfo(
          TEST_UTIL.getHBaseCluster().getMaster());
    assertEquals(3, htds.size());
  }




  public static void assertEquals(int expected,
                               int actual) {
    if (expected != actual) {
      throw new AssertionFailedError("expected:<" +
      expected + "> but was:<" +
      actual + ">");
    }
  }

  public static void assertEquals(boolean expected,
                               boolean actual) {
    if (expected != actual) {
      throw new AssertionFailedError("expected:<" +
      expected + "> but was:<" +
      actual + ">");
    }
  }



}
