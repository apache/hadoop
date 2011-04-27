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
package org.apache.hadoop.hbase.catalog;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test {@link MetaReader}, {@link MetaEditor}, and {@link RootLocationEditor}.
 */
public class TestMetaReaderEditor {
  private static final Log LOG = LogFactory.getLog(TestMetaReaderEditor.class);
  private static final  HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private ZooKeeperWatcher zkw;
  private CatalogTracker ct;
  private final static Abortable ABORTABLE = new Abortable() {
    private final AtomicBoolean abort = new AtomicBoolean(false);

    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
      abort.set(true);
    }
  };

  @BeforeClass public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @Before public void setup() throws IOException, InterruptedException {
    Configuration c = new Configuration(UTIL.getConfiguration());
    zkw = new ZooKeeperWatcher(c, "TestMetaReaderEditor", ABORTABLE);
    HConnection connection = HConnectionManager.getConnection(c);
    ct = new CatalogTracker(zkw, connection, ABORTABLE);
    ct.start();
  }

  @AfterClass public static void afterClass() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test public void testGetRegionsCatalogTables()
  throws IOException, InterruptedException {
    List<HRegionInfo> regions =
      MetaReader.getTableRegions(ct, HConstants.META_TABLE_NAME);
    assertTrue(regions.size() >= 1);
    assertTrue(MetaReader.getTableRegionsAndLocations(ct,
      Bytes.toString(HConstants.META_TABLE_NAME)).size() >= 1);
    assertTrue(MetaReader.getTableRegionsAndLocations(ct,
      Bytes.toString(HConstants.ROOT_TABLE_NAME)).size() == 1);
  }

  @Test public void testTableExists() throws IOException {
    final String name = "testTableExists";
    final byte [] nameBytes = Bytes.toBytes(name);
    assertFalse(MetaReader.tableExists(ct, name));
    UTIL.createTable(nameBytes, HConstants.CATALOG_FAMILY);
    assertTrue(MetaReader.tableExists(ct, name));
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.disableTable(name);
    admin.deleteTable(name);
    assertFalse(MetaReader.tableExists(ct, name));
    assertTrue(MetaReader.tableExists(ct,
      Bytes.toString(HConstants.META_TABLE_NAME)));
    assertTrue(MetaReader.tableExists(ct,
      Bytes.toString(HConstants.ROOT_TABLE_NAME)));
  }

  @Test public void testGetRegion() throws IOException, InterruptedException {
    final String name = "testGetRegion";
    LOG.info("Started " + name);
    final byte [] nameBytes = Bytes.toBytes(name);
    HTable t = UTIL.createTable(nameBytes, HConstants.CATALOG_FAMILY);
    int regionCount = UTIL.createMultiRegions(t, HConstants.CATALOG_FAMILY);

    // Test it works getting a region from user table.
    List<HRegionInfo> regions = MetaReader.getTableRegions(ct, nameBytes);
    assertEquals(regionCount, regions.size());
    Pair<HRegionInfo, ServerName> pair =
      MetaReader.getRegion(ct, regions.get(0).getRegionName());
    assertEquals(regions.get(0).getEncodedName(),
      pair.getFirst().getEncodedName());
    // Test get on non-existent region.
    pair = MetaReader.getRegion(ct, Bytes.toBytes("nonexistent-region"));
    assertNull(pair);
    // Test it works getting a region from meta/root.
    pair =
      MetaReader.getRegion(ct, HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
    assertEquals(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName(),
      pair.getFirst().getEncodedName());
    LOG.info("Finished " + name);
  }

  // Test for the optimization made in HBASE-3650
  @Test public void testScanMetaForTable() throws IOException {
    final String name = "testScanMetaForTable";
    LOG.info("Started " + name);

    /** Create 5 tables
     - testScanMetaForTable
     - testScanMetaForTable0
     - testScanMetaForTable1
     - testScanMetaForTable2
     - testScanMetaForTablf
    **/

    UTIL.createTable(Bytes.toBytes(name), HConstants.CATALOG_FAMILY);
    for (int i = 3; i < 3; i ++) {
      UTIL.createTable(Bytes.toBytes(name+i), HConstants.CATALOG_FAMILY);
    }
    // name that is +1 greater than the first one (e+1=f)
    byte[] greaterName = Bytes.toBytes("testScanMetaForTablf");
    UTIL.createTable(greaterName, HConstants.CATALOG_FAMILY);

    // Now make sure we only get the regions from 1 of the tables at a time

    assertEquals(1, MetaReader.getTableRegions(ct, Bytes.toBytes(name)).size());
    for (int i = 3; i < 3; i ++) {
      assertEquals(1, MetaReader.getTableRegions(ct, Bytes.toBytes(name+i)).size());
    }
    assertEquals(1, MetaReader.getTableRegions(ct, greaterName).size());
  }
}