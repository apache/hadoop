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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestCase.FlushCache;
import org.apache.hadoop.hbase.HBaseTestCase.HTableIncommon;
import org.apache.hadoop.hbase.HBaseTestCase.Incommon;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Port of old TestScanMultipleVersions, TestTimestamp and TestGetRowVersions
 * from old testing framework to {@link HBaseTestingUtility}.
 */
public class TestMultiVersions {
  private static final Log LOG = LogFactory.getLog(TestMultiVersions.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private HBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    this.admin = new HBaseAdmin(UTIL.getConfiguration());
  }

  @After
  public void after() throws IOException {
    this.admin.close();
  }

  /**
  * Tests user specifiable time stamps putting, getting and scanning.  Also
   * tests same in presence of deletes.  Test cores are written so can be
   * run against an HRegion and against an HTable: i.e. both local and remote.
   * 
   * <p>Port of old TestTimestamp test to here so can better utilize the spun
   * up cluster running more than a single test per spin up.  Keep old tests'
   * crazyness.
   */
  @Test
  public void testTimestamps() throws Exception {
    HTableDescriptor desc = new HTableDescriptor("testTimestamps");
    desc.addFamily(new HColumnDescriptor(TimestampTestBase.FAMILY_NAME));
    this.admin.createTable(desc);
    HTable table = new HTable(UTIL.getConfiguration(), desc.getName());
    // TODO: Remove these deprecated classes or pull them in here if this is
    // only test using them.
    Incommon incommon = new HTableIncommon(table);
    TimestampTestBase.doTestDelete(incommon, new FlushCache() {
      public void flushcache() throws IOException {
        UTIL.getHbaseCluster().flushcache();
      }
     });

    // Perhaps drop and readd the table between tests so the former does
    // not pollute this latter?  Or put into separate tests.
    TimestampTestBase.doTestTimestampScanning(incommon, new FlushCache() {
      public void flushcache() throws IOException {
        UTIL.getMiniHBaseCluster().flushcache();
      }
    });
  }

  /**
   * Verifies versions across a cluster restart.
   * Port of old TestGetRowVersions test to here so can better utilize the spun
   * up cluster running more than a single test per spin up.  Keep old tests'
   * crazyness.
   */
  @Test
  public void testGetRowVersions() throws Exception {
    final String tableName = "testGetRowVersions";
    final byte [] contents = Bytes.toBytes("contents");
    final byte [] row = Bytes.toBytes("row");
    final byte [] value1 = Bytes.toBytes("value1");
    final byte [] value2 = Bytes.toBytes("value2");
    final long timestamp1 = 100L;
    final long timestamp2 = 200L;
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(contents));
    this.admin.createTable(desc);
    Put put = new Put(row, timestamp1, null);
    put.add(contents, contents, value1);
    HTable table = new HTable(UTIL.getConfiguration(), tableName);
    table.put(put);
    // Shut down and restart the HBase cluster
    UTIL.shutdownMiniHBaseCluster();
    LOG.debug("HBase cluster shut down -- restarting");
    UTIL.startMiniHBaseCluster(1, 1);
    // Make a new connection.  Use new Configuration instance because old one
    // is tied to an HConnection that has since gone stale.
    table = new HTable(new Configuration(UTIL.getConfiguration()), tableName);
    // Overwrite previous value
    put = new Put(row, timestamp2, null);
    put.add(contents, contents, value2);
    table.put(put);
    // Now verify that getRow(row, column, latest) works
    Get get = new Get(row);
    // Should get one version by default
    Result r = table.get(get);
    assertNotNull(r);
    assertFalse(r.isEmpty());
    assertTrue(r.size() == 1);
    byte [] value = r.getValue(contents, contents);
    assertTrue(value.length != 0);
    assertTrue(Bytes.equals(value, value2));
    // Now check getRow with multiple versions
    get = new Get(row);
    get.setMaxVersions();
    r = table.get(get);
    assertTrue(r.size() == 2);
    value = r.getValue(contents, contents);
    assertTrue(value.length != 0);
    assertTrue(Bytes.equals(value, value2));
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map =
      r.getMap();
    NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap =
      map.get(contents);
    NavigableMap<Long, byte[]> versionMap = familyMap.get(contents);
    assertTrue(versionMap.size() == 2);
    assertTrue(Bytes.equals(value1, versionMap.get(timestamp1)));
    assertTrue(Bytes.equals(value2, versionMap.get(timestamp2)));
  }

  /**
   * Port of old TestScanMultipleVersions test here so can better utilize the
   * spun up cluster running more than just a single test.  Keep old tests
   * crazyness.
   * 
   * <p>Tests five cases of scans and timestamps.
   * @throws Exception
   */
  @Test
  public void testScanMultipleVersions() throws Exception {
    final byte [] tableName = Bytes.toBytes("testScanMultipleVersions");
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    final byte [][] rows = new byte[][] {
      Bytes.toBytes("row_0200"),
      Bytes.toBytes("row_0800")
    };
    final byte [][] splitRows = new byte[][] {Bytes.toBytes("row_0500")};
    final long [] timestamp = new long[] {100L, 1000L};
    this.admin.createTable(desc, splitRows);
    HTable table = new HTable(UTIL.getConfiguration(), tableName);
    // Assert we got the region layout wanted.
    NavigableMap<HRegionInfo, ServerName> locations = table.getRegionLocations();
    assertEquals(2, locations.size());
    int index = 0;
    for (Map.Entry<HRegionInfo, ServerName> e: locations.entrySet()) {
      HRegionInfo hri = e.getKey();
      if (index == 0) {
        assertTrue(Bytes.equals(HConstants.EMPTY_START_ROW, hri.getStartKey()));
        assertTrue(Bytes.equals(hri.getEndKey(), splitRows[0]));
      } else if (index == 1) {
        assertTrue(Bytes.equals(splitRows[0], hri.getStartKey()));
        assertTrue(Bytes.equals(hri.getEndKey(), HConstants.EMPTY_END_ROW));
      }
      index++;
    }
    // Insert data
    for (int i = 0; i < locations.size(); i++) {
      for (int j = 0; j < timestamp.length; j++) {
        Put put = new Put(rows[i], timestamp[j], null);
        put.add(HConstants.CATALOG_FAMILY, null, timestamp[j],
            Bytes.toBytes(timestamp[j]));
        table.put(put);
      }
    }
    // There are 5 cases we have to test. Each is described below.
    for (int i = 0; i < rows.length; i++) {
      for (int j = 0; j < timestamp.length; j++) {
        Get get = new Get(rows[i]);
        get.addFamily(HConstants.CATALOG_FAMILY);
        get.setTimeStamp(timestamp[j]);
        Result result = table.get(get);
        int cellCount = 0;
        for(@SuppressWarnings("unused")KeyValue kv : result.list()) {
          cellCount++;
        }
        assertTrue(cellCount == 1);
      }
    }

    // Case 1: scan with LATEST_TIMESTAMP. Should get two rows
    int count = 0;
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner s = table.getScanner(scan);
    try {
      for (Result rr = null; (rr = s.next()) != null;) {
        System.out.println(rr.toString());
        count += 1;
      }
      assertEquals("Number of rows should be 2", 2, count);
    } finally {
      s.close();
    }

    // Case 2: Scan with a timestamp greater than most recent timestamp
    // (in this case > 1000 and < LATEST_TIMESTAMP. Should get 2 rows.

    count = 0;
    scan = new Scan();
    scan.setTimeRange(1000L, Long.MAX_VALUE);
    scan.addFamily(HConstants.CATALOG_FAMILY);

    s = table.getScanner(scan);
    try {
      while (s.next() != null) {
        count += 1;
      }
      assertEquals("Number of rows should be 2", 2, count);
    } finally {
      s.close();
    }

    // Case 3: scan with timestamp equal to most recent timestamp
    // (in this case == 1000. Should get 2 rows.

    count = 0;
    scan = new Scan();
    scan.setTimeStamp(1000L);
    scan.addFamily(HConstants.CATALOG_FAMILY);

    s = table.getScanner(scan);
    try {
      while (s.next() != null) {
        count += 1;
      }
      assertEquals("Number of rows should be 2", 2, count);
    } finally {
      s.close();
    }

    // Case 4: scan with timestamp greater than first timestamp but less than
    // second timestamp (100 < timestamp < 1000). Should get 2 rows.

    count = 0;
    scan = new Scan();
    scan.setTimeRange(100L, 1000L);
    scan.addFamily(HConstants.CATALOG_FAMILY);

    s = table.getScanner(scan);
    try {
      while (s.next() != null) {
        count += 1;
      }
      assertEquals("Number of rows should be 2", 2, count);
    } finally {
      s.close();
    }

    // Case 5: scan with timestamp equal to first timestamp (100)
    // Should get 2 rows.

    count = 0;
    scan = new Scan();
    scan.setTimeStamp(100L);
    scan.addFamily(HConstants.CATALOG_FAMILY);

    s = table.getScanner(scan);
    try {
      while (s.next() != null) {
        count += 1;
      }
      assertEquals("Number of rows should be 2", 2, count);
    } finally {
      s.close();
    }
  }
}