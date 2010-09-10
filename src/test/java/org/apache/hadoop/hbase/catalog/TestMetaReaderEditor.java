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
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test {@link MetaReader}, {@link MetaEditor}, and {@link RootLocationEditor}.
 */
public class TestMetaReaderEditor {
  private static final Log LOG = LogFactory.getLog(TestMetaReaderEditor.class);
  private static final  HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static ZooKeeperWatcher ZKW;
  private static CatalogTracker CT;
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
    ZKW = new ZooKeeperWatcher(UTIL.getConfiguration(),
      "TestMetaReaderEditor", ABORTABLE);
    HConnection connection =
      HConnectionManager.getConnection(UTIL.getConfiguration());
    CT = new CatalogTracker(ZKW, connection, ABORTABLE);
    CT.start();
  }

  @AfterClass public static void afterClass() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test public void testGetRegionsCatalogTables()
  throws IOException, InterruptedException {
    List<HRegionInfo> regions =
      MetaReader.getTableRegions(CT, HConstants.META_TABLE_NAME);
    assertTrue(regions.size() >= 1);
    assertTrue(MetaReader.getTableRegionsAndLocations(CT,
      Bytes.toString(HConstants.META_TABLE_NAME)).size() >= 1);
    assertTrue(MetaReader.getTableRegionsAndLocations(CT,
      Bytes.toString(HConstants.ROOT_TABLE_NAME)).size() == 1);
  }

  @Test public void testTableExists() throws IOException {
    final String name = "testTableExists";
    final byte [] nameBytes = Bytes.toBytes(name);
    assertFalse(MetaReader.tableExists(CT, name));
    UTIL.createTable(nameBytes, HConstants.CATALOG_FAMILY);
    assertTrue(MetaReader.tableExists(CT, name));
    UTIL.getHBaseAdmin().disableTable(name);
    UTIL.getHBaseAdmin().deleteTable(name);
    assertFalse(MetaReader.tableExists(CT, name));
    assertTrue(MetaReader.tableExists(CT,
      Bytes.toString(HConstants.META_TABLE_NAME)));
    assertTrue(MetaReader.tableExists(CT,
      Bytes.toString(HConstants.ROOT_TABLE_NAME)));
  }

  @Test public void testGetRegion() throws IOException, InterruptedException {
    final String name = "testGetRegion";
    final byte [] nameBytes = Bytes.toBytes(name);
    HTable t = UTIL.createTable(nameBytes, HConstants.CATALOG_FAMILY);
    int regionCount = UTIL.createMultiRegions(t, HConstants.CATALOG_FAMILY);

    // Test it works getting a region from user table.
    List<HRegionInfo> regions = MetaReader.getTableRegions(CT, nameBytes);
    assertEquals(regionCount, regions.size());
    Pair<HRegionInfo, HServerAddress> pair =
      MetaReader.getRegion(CT, regions.get(0).getRegionName());
    assertEquals(regions.get(0).getEncodedName(),
      pair.getFirst().getEncodedName());
    // Test get on non-existent region.
    pair = MetaReader.getRegion(CT, Bytes.toBytes("nonexistent-region"));
    assertNull(pair);
    // Test it works getting a region from meta/root.
    pair =
      MetaReader.getRegion(CT, HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
    assertEquals(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName(),
      pair.getFirst().getEncodedName());
  }
}
