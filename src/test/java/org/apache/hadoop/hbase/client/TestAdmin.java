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
package org.apache.hadoop.hbase.client;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.TestHLogUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Class to test HBaseAdmin.
 * Spins up the minicluster once at test start and then takes it down afterward.
 * Add any testing of HBaseAdmin functionality here.
 */
public class TestAdmin {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private HBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    this.admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
  }

  @Test
  public void testDisableAndEnableTable() throws IOException {
    final byte [] row = Bytes.toBytes("row");
    final byte [] qualifier = Bytes.toBytes("qualifier");
    final byte [] value = Bytes.toBytes("value");
    final byte [] table = Bytes.toBytes("testDisableAndEnableTable");
    HTable ht = TEST_UTIL.createTable(table, HConstants.CATALOG_FAMILY);
    Put put = new Put(row);
    put.add(HConstants.CATALOG_FAMILY, qualifier, value);
    ht.put(put);
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    ht.get(get);

    this.admin.disableTable(table);

    // Test that table is disabled
    get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    boolean ok = false;
    try {
      ht.get(get);
    } catch (NotServingRegionException e) {
      ok = true;
    } catch (RetriesExhaustedException e) {
      ok = true;
    }
    assertTrue(ok);
    this.admin.enableTable(table);

    // Test that table is enabled
    try {
      ht.get(get);
    } catch (RetriesExhaustedException e) {
      ok = false;
    }
    assertTrue(ok);
  }

  @Test
  public void testDisableAndEnableTables() throws IOException {
    final byte [] row = Bytes.toBytes("row");
    final byte [] qualifier = Bytes.toBytes("qualifier");
    final byte [] value = Bytes.toBytes("value");
    final byte [] table1 = Bytes.toBytes("testDisableAndEnableTable1");
    final byte [] table2 = Bytes.toBytes("testDisableAndEnableTable2");
    HTable ht1 = TEST_UTIL.createTable(table1, HConstants.CATALOG_FAMILY);
    HTable ht2 = TEST_UTIL.createTable(table2, HConstants.CATALOG_FAMILY);
    Put put = new Put(row);
    put.add(HConstants.CATALOG_FAMILY, qualifier, value);
    ht1.put(put);
    ht2.put(put);
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    ht1.get(get);
    ht2.get(get);

    this.admin.disableTables("testDisableAndEnableTable.*");

    // Test that tables are disabled
    get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    boolean ok = false;
    try {
      ht1.get(get);
      ht2.get(get);
    } catch (NotServingRegionException e) {
      ok = true;
    } catch (RetriesExhaustedException e) {
      ok = true;
    }
    assertTrue(ok);
    this.admin.enableTables("testDisableAndEnableTable.*");

    // Test that tables are enabled
    try {
      ht1.get(get);
    } catch (RetriesExhaustedException e) {
      ok = false;
    }
    try {
      ht2.get(get);
    } catch (RetriesExhaustedException e) {
      ok = false;
    }
    assertTrue(ok);
  }

  @Test
  public void testCreateTable() throws IOException {
    HTableDescriptor [] tables = admin.listTables();
    int numTables = tables.length;
    TEST_UTIL.createTable(Bytes.toBytes("testCreateTable"),
      HConstants.CATALOG_FAMILY);
    tables = this.admin.listTables();
    assertEquals(numTables + 1, tables.length);
  }

  @Test
  public void testGetTableDescriptor() throws IOException {
    HColumnDescriptor fam1 = new HColumnDescriptor("fam1");
    HColumnDescriptor fam2 = new HColumnDescriptor("fam2");
    HColumnDescriptor fam3 = new HColumnDescriptor("fam3");
    HTableDescriptor htd = new HTableDescriptor("myTestTable");
    htd.addFamily(fam1);
    htd.addFamily(fam2);
    htd.addFamily(fam3);
    this.admin.createTable(htd);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), "myTestTable");
    HTableDescriptor confirmedHtd = table.getTableDescriptor();
    assertEquals(htd.compareTo(confirmedHtd), 0);
  }

  @Test
  public void testHColumnValidName() {
       boolean exceptionThrown = false;
       try {
       HColumnDescriptor fam1 = new HColumnDescriptor("\\test\\abc");
       } catch(IllegalArgumentException iae) {
           exceptionThrown = true;
           assertTrue(exceptionThrown);
       }
   }
  /**
   * Verify schema modification takes.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test 
  public void testOnlineChangeTableSchema() throws IOException, InterruptedException {
    final byte [] tableName = Bytes.toBytes("changeTableSchemaOnline");
    HTableDescriptor [] tables = admin.listTables();
    MasterServices masterServices = TEST_UTIL.getMiniHBaseCluster().getMaster();
    int numTables = tables.length;
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    tables = this.admin.listTables();
    assertEquals(numTables + 1, tables.length);

    // FIRST, do htabledescriptor changes.
    HTableDescriptor htd = this.admin.getTableDescriptor(tableName);
    // Make a copy and assert copy is good.
    HTableDescriptor copy = new HTableDescriptor(htd);
    assertTrue(htd.equals(copy));
    // Now amend the copy. Introduce differences.
    long newFlushSize = htd.getMemStoreFlushSize() / 2;
    copy.setMemStoreFlushSize(newFlushSize);
    final String key = "anyoldkey";
    assertTrue(htd.getValue(key) == null);
    copy.setValue(key, key);
    boolean expectedException = false;
    try {
      modifyTable(tableName, copy);
    } catch (TableNotDisabledException re) {
      expectedException = true;
    }
    assertFalse(expectedException);
    HTableDescriptor modifiedHtd = this.admin.getTableDescriptor(tableName);
    // Assert returned modifiedhcd is same as the copy.
    assertFalse(htd.equals(modifiedHtd));
    assertTrue(copy.equals(modifiedHtd));
    assertEquals(newFlushSize, modifiedHtd.getMemStoreFlushSize());
    assertEquals(key, modifiedHtd.getValue(key));

    // Now work on column family changes.
    htd = this.admin.getTableDescriptor(tableName);
    int countOfFamilies = modifiedHtd.getFamilies().size();
    assertTrue(countOfFamilies > 0);
    HColumnDescriptor hcd = modifiedHtd.getFamilies().iterator().next();
    int maxversions = hcd.getMaxVersions();
    final int newMaxVersions = maxversions + 1;
    hcd.setMaxVersions(newMaxVersions);
    final byte [] hcdName = hcd.getName();
    expectedException = false;
    try {
      this.admin.modifyColumn(tableName, hcd);
    } catch (TableNotDisabledException re) {
      expectedException = true;
    }
    assertFalse(expectedException);
    modifiedHtd = this.admin.getTableDescriptor(tableName);
    HColumnDescriptor modifiedHcd = modifiedHtd.getFamily(hcdName);
    assertEquals(newMaxVersions, modifiedHcd.getMaxVersions());

    // Try adding a column
    assertFalse(this.admin.isTableDisabled(tableName));
    final String xtracolName = "xtracol";
    htd = this.admin.getTableDescriptor(tableName);
    HColumnDescriptor xtracol = new HColumnDescriptor(xtracolName);
    xtracol.setValue(xtracolName, xtracolName);
    expectedException = false;
    try {
      this.admin.addColumn(tableName, xtracol);
    } catch (TableNotDisabledException re) {
      expectedException = true;
    }
    // Add column should work even if the table is enabled
    assertFalse(expectedException);
    modifiedHtd = this.admin.getTableDescriptor(tableName);
    hcd = modifiedHtd.getFamily(xtracol.getName());
    assertTrue(hcd != null);
    assertTrue(hcd.getValue(xtracolName).equals(xtracolName));

    // Delete the just-added column.
    this.admin.deleteColumn(tableName, xtracol.getName());
    modifiedHtd = this.admin.getTableDescriptor(tableName);
    hcd = modifiedHtd.getFamily(xtracol.getName());
    assertTrue(hcd == null);

    // Delete the table
    this.admin.disableTable(tableName);
    this.admin.deleteTable(tableName);
    this.admin.listTables();
    assertFalse(this.admin.tableExists(tableName));
  }

  /**
   * Modify table is async so wait on completion of the table operation in master.
   * @param tableName
   * @param htd
   * @throws IOException
   */
  private void modifyTable(final byte [] tableName, final HTableDescriptor htd)
  throws IOException {
    MasterServices services = TEST_UTIL.getMiniHBaseCluster().getMaster();
    ExecutorService executor = services.getExecutorService();
    AtomicBoolean done = new AtomicBoolean(false);
    executor.registerListener(EventType.C_M_MODIFY_TABLE, new DoneListener(done));
    this.admin.modifyTable(tableName, htd);
    while (!done.get()) {
      synchronized (done) {
        try {
          done.wait(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    executor.unregisterListener(EventType.C_M_MODIFY_TABLE);
  }

  /**
   * Listens for when an event is done in Master.
   */
  static class DoneListener implements EventHandler.EventHandlerListener {
    private final AtomicBoolean done;

    DoneListener(final AtomicBoolean done) {
      super();
      this.done = done;
    }

    @Override
    public void afterProcess(EventHandler event) {
      this.done.set(true);
      synchronized (this.done) {
        // Wake anyone waiting on this value to change.
        this.done.notifyAll();
      }
    }

    @Override
    public void beforeProcess(EventHandler event) {
      // continue
    }
  }

  protected void verifyRoundRobinDistribution(HTable ht, int expectedRegions) throws IOException {
    int numRS = ht.getConnection().getCurrentNrHRS();
    Map<HRegionInfo,HServerAddress> regions = ht.getRegionsInfo();
    Map<HServerAddress, List<HRegionInfo>> server2Regions = new HashMap<HServerAddress, List<HRegionInfo>>();
    for (Map.Entry<HRegionInfo,HServerAddress> entry : regions.entrySet()) {
      HServerAddress server = entry.getValue();
      List<HRegionInfo> regs = server2Regions.get(server);
      if (regs == null) {
        regs = new ArrayList<HRegionInfo>();
        server2Regions.put(server, regs);
      }
      regs.add(entry.getKey());
    }
    float average = (float) expectedRegions/numRS;
    int min = (int)Math.floor(average);
    int max = (int)Math.ceil(average);
    for (List<HRegionInfo> regionList : server2Regions.values()) {
      assertTrue(regionList.size() == min || regionList.size() == max);
    }
  }

  @Test
  public void testCreateTableWithRegions() throws IOException, InterruptedException {

    byte[] tableName = Bytes.toBytes("testCreateTableWithRegions");

    byte [][] splitKeys = {
        new byte [] { 1, 1, 1 },
        new byte [] { 2, 2, 2 },
        new byte [] { 3, 3, 3 },
        new byte [] { 4, 4, 4 },
        new byte [] { 5, 5, 5 },
        new byte [] { 6, 6, 6 },
        new byte [] { 7, 7, 7 },
        new byte [] { 8, 8, 8 },
        new byte [] { 9, 9, 9 },
    };
    int expectedRegions = splitKeys.length + 1;

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, splitKeys);

    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Map<HRegionInfo,HServerAddress> regions = ht.getRegionsInfo();
    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + regions.size(),
        expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");

    Iterator<HRegionInfo> hris = regions.keySet().iterator();
    HRegionInfo hri = hris.next();
    assertTrue(hri.getStartKey() == null || hri.getStartKey().length == 0);
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[0]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[0]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[1]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[1]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[2]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[2]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[3]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[3]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[4]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[4]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[5]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[5]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[6]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[6]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[7]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[7]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[8]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[8]));
    assertTrue(hri.getEndKey() == null || hri.getEndKey().length == 0);

    verifyRoundRobinDistribution(ht, expectedRegions);

    // Now test using start/end with a number of regions

    // Use 80 bit numbers to make sure we aren't limited
    byte [] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    byte [] endKey =   { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };

    // Splitting into 10 regions, we expect (null,1) ... (9, null)
    // with (1,2) (2,3) (3,4) (4,5) (5,6) (6,7) (7,8) (8,9) in the middle

    expectedRegions = 10;

    byte [] TABLE_2 = Bytes.add(tableName, Bytes.toBytes("_2"));

    desc = new HTableDescriptor(TABLE_2);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.createTable(desc, startKey, endKey, expectedRegions);

    ht = new HTable(TEST_UTIL.getConfiguration(), TABLE_2);
    regions = ht.getRegionsInfo();
    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + regions.size(),
        expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");

    hris = regions.keySet().iterator();
    hri = hris.next();
    assertTrue(hri.getStartKey() == null || hri.getStartKey().length == 0);
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {1,1,1,1,1,1,1,1,1,1}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {1,1,1,1,1,1,1,1,1,1}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {2,2,2,2,2,2,2,2,2,2}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {2,2,2,2,2,2,2,2,2,2}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {3,3,3,3,3,3,3,3,3,3}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {3,3,3,3,3,3,3,3,3,3}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {4,4,4,4,4,4,4,4,4,4}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {4,4,4,4,4,4,4,4,4,4}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {5,5,5,5,5,5,5,5,5,5}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {5,5,5,5,5,5,5,5,5,5}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {6,6,6,6,6,6,6,6,6,6}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {6,6,6,6,6,6,6,6,6,6}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {7,7,7,7,7,7,7,7,7,7}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {7,7,7,7,7,7,7,7,7,7}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {8,8,8,8,8,8,8,8,8,8}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {8,8,8,8,8,8,8,8,8,8}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {9,9,9,9,9,9,9,9,9,9}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {9,9,9,9,9,9,9,9,9,9}));
    assertTrue(hri.getEndKey() == null || hri.getEndKey().length == 0);

    verifyRoundRobinDistribution(ht, expectedRegions);

    // Try once more with something that divides into something infinite

    startKey = new byte [] { 0, 0, 0, 0, 0, 0 };
    endKey = new byte [] { 1, 0, 0, 0, 0, 0 };

    expectedRegions = 5;

    byte [] TABLE_3 = Bytes.add(tableName, Bytes.toBytes("_3"));

    desc = new HTableDescriptor(TABLE_3);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.createTable(desc, startKey, endKey, expectedRegions);

    ht = new HTable(TEST_UTIL.getConfiguration(), TABLE_3);
    regions = ht.getRegionsInfo();
    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + regions.size(),
        expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");

    verifyRoundRobinDistribution(ht, expectedRegions);

    // Try an invalid case where there are duplicate split keys
    splitKeys = new byte [][] {
        new byte [] { 1, 1, 1 },
        new byte [] { 2, 2, 2 },
        new byte [] { 3, 3, 3 },
        new byte [] { 2, 2, 2 }
    };

    byte [] TABLE_4 = Bytes.add(tableName, Bytes.toBytes("_4"));
    desc = new HTableDescriptor(TABLE_4);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    try {
      admin.createTable(desc, splitKeys);
      assertTrue("Should not be able to create this table because of " +
          "duplicate split keys", false);
    } catch(IllegalArgumentException iae) {
      // Expected
    }
  }

  @Test
  public void testTableExist() throws IOException {
    final byte [] table = Bytes.toBytes("testTableExist");
    boolean exist = false;
    exist = this.admin.tableExists(table);
    assertEquals(false, exist);
    TEST_UTIL.createTable(table, HConstants.CATALOG_FAMILY);
    exist = this.admin.tableExists(table);
    assertEquals(true, exist);
  }

  /**
   * Tests forcing split from client and having scanners successfully ride over split.
   * @throws Exception
   * @throws IOException
   */
  @Test
  public void testForceSplit() throws Exception {
    byte[][] familyNames = new byte[][] { Bytes.toBytes("cf") };
    int[] rowCounts = new int[] { 6000 };
    int numVersions = HColumnDescriptor.DEFAULT_VERSIONS;
    int blockSize = 256;
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    byte[] splitKey = Bytes.toBytes(3500);
    splitTest(splitKey, familyNames, rowCounts, numVersions, blockSize);
  }

  /**
   * Multi-family scenario. Tests forcing split from client and
   * having scanners successfully ride over split.
   * @throws Exception
   * @throws IOException
   */
  @Test
  public void testForceSplitMultiFamily() throws Exception {
    int numVersions = HColumnDescriptor.DEFAULT_VERSIONS;

    // use small HFile block size so that we can have lots of blocks in HFile
    // Otherwise, if there is only one block,
    // HFileBlockIndex.midKey()'s value == startKey
    int blockSize = 256;
    byte[][] familyNames = new byte[][] { Bytes.toBytes("cf1"),
      Bytes.toBytes("cf2") };

    // one of the column families isn't splittable
    int[] rowCounts = new int[] { 6000, 1 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    rowCounts = new int[] { 1, 6000 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    // one column family has much smaller data than the other
    // the split key should be based on the largest column family
    rowCounts = new int[] { 6000, 300 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    rowCounts = new int[] { 300, 6000 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

  }

  void splitTest(byte[] splitPoint, byte[][] familyNames, int[] rowCounts,
    int numVersions, int blockSize) throws Exception {
    byte [] tableName = Bytes.toBytes("testForceSplit");
    assertFalse(admin.tableExists(tableName));
    final HTable table = TEST_UTIL.createTable(tableName, familyNames,
      numVersions, blockSize);
    try {
      int rowCount = 0;

      // insert rows into column families. The number of rows that have values
      // in a specific column family is decided by rowCounts[familyIndex]
      for (int index = 0; index < familyNames.length; index++) {
        for (int i = 0; i < rowCounts[index]; i++) {
          byte[] k = Bytes.toBytes(i);
          Put put = new Put(k);
          put.add(familyNames[index], new byte[0], k);
          table.put(put);
        }

        if ( rowCount < rowCounts[index] ) {
          rowCount = rowCounts[index];
        }
      }

      // get the initial layout (should just be one region)
      Map<HRegionInfo,HServerAddress> m = table.getRegionsInfo();
      System.out.println("Initial regions (" + m.size() + "): " + m);
      assertTrue(m.size() == 1);

      // Verify row count
      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);
      int rows = 0;
      for(@SuppressWarnings("unused") Result result : scanner) {
        rows++;
      }
      scanner.close();
      assertEquals(rowCount, rows);

      // Have an outstanding scan going on to make sure we can scan over splits.
      scan = new Scan();
      scanner = table.getScanner(scan);
      // Scan first row so we are into first region before split happens.
      scanner.next();

      final AtomicInteger count = new AtomicInteger(0);
      Thread t = new Thread("CheckForSplit") {
        public void run() {
          for (int i = 0; i < 20; i++) {
            try {
              sleep(1000);
            } catch (InterruptedException e) {
              continue;
            }
            // check again    table = new HTable(conf, tableName);
            Map<HRegionInfo, HServerAddress> regions = null;
            try {
              regions = table.getRegionsInfo();
            } catch (IOException e) {
              e.printStackTrace();
            }
            if (regions == null) continue;
            count.set(regions.size());
            if (count.get() >= 2) break;
            LOG.debug("Cycle waiting on split");
          }
        }
      };
      t.start();
      // Split the table
      this.admin.split(tableName, splitPoint);
      t.join();

      // Verify row count
      rows = 1; // We counted one row above.
      for (@SuppressWarnings("unused") Result result : scanner) {
        rows++;
        if (rows > rowCount) {
          scanner.close();
          assertTrue("Scanned more than expected (" + rowCount + ")", false);
        }
      }
      scanner.close();
      assertEquals(rowCount, rows);

      Map<HRegionInfo, HServerAddress> regions = null;
      try {
        regions = table.getRegionsInfo();
      } catch (IOException e) {
        e.printStackTrace();
      }
      assertEquals(2, regions.size());
      HRegionInfo[] r = regions.keySet().toArray(new HRegionInfo[0]);
      if (splitPoint != null) {
        // make sure the split point matches our explicit configuration
        assertEquals(Bytes.toString(splitPoint),
            Bytes.toString(r[0].getEndKey()));
        assertEquals(Bytes.toString(splitPoint),
            Bytes.toString(r[1].getStartKey()));
        LOG.debug("Properly split on " + Bytes.toString(splitPoint));
      } else {
        if (familyNames.length > 1) {
          int splitKey = Bytes.toInt(r[0].getEndKey());
          // check if splitKey is based on the largest column family
          // in terms of it store size
          int deltaForLargestFamily = Math.abs(rowCount/2 - splitKey);
          for (int index = 0; index < familyNames.length; index++) {
            int delta = Math.abs(rowCounts[index]/2 - splitKey);
            assertTrue(delta >= deltaForLargestFamily);
          }
        }
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  /**
   * HADOOP-2156
   * @throws IOException
   */
  @Test (expected=IllegalArgumentException.class)
  public void testEmptyHHTableDescriptor() throws IOException {
    this.admin.createTable(new HTableDescriptor());
  }

  @Test (expected=IllegalArgumentException.class)
  public void testInvalidHColumnDescriptor() throws IOException {
     HColumnDescriptor hcd = new HColumnDescriptor("/cfamily/name");
  }

  @Test
  public void testEnableDisableAddColumnDeleteColumn() throws Exception {
    byte [] tableName = Bytes.toBytes("testMasterAdmin");
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    this.admin.disableTable(tableName);
    try {
      new HTable(TEST_UTIL.getConfiguration(), tableName);
    } catch (org.apache.hadoop.hbase.client.RegionOfflineException e) {
      // Expected
    }
    this.admin.addColumn(tableName, new HColumnDescriptor("col2"));
    this.admin.enableTable(tableName);
    try {
      this.admin.deleteColumn(tableName, Bytes.toBytes("col2"));
    } catch(TableNotDisabledException e) {
      // Expected
    }
    this.admin.disableTable(tableName);
    this.admin.deleteColumn(tableName, Bytes.toBytes("col2"));
    this.admin.deleteTable(tableName);
  }

  @Test
  public void testCreateBadTables() throws IOException {
    String msg = null;
    try {
      this.admin.createTable(HTableDescriptor.ROOT_TABLEDESC);
    } catch (IllegalArgumentException e) {
      msg = e.toString();
    }
    assertTrue("Unexcepted exception message " + msg, msg != null &&
      msg.startsWith(IllegalArgumentException.class.getName()) &&
      msg.contains(HTableDescriptor.ROOT_TABLEDESC.getNameAsString()));
    msg = null;
    try {
      this.admin.createTable(HTableDescriptor.META_TABLEDESC);
    } catch(IllegalArgumentException e) {
      msg = e.toString();
    }
    assertTrue("Unexcepted exception message " + msg, msg != null &&
      msg.startsWith(IllegalArgumentException.class.getName()) &&
      msg.contains(HTableDescriptor.META_TABLEDESC.getNameAsString()));

    // Now try and do concurrent creation with a bunch of threads.
    final HTableDescriptor threadDesc =
      new HTableDescriptor("threaded_testCreateBadTables");
    threadDesc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    int count = 10;
    Thread [] threads = new Thread [count];
    final AtomicInteger successes = new AtomicInteger(0);
    final AtomicInteger failures = new AtomicInteger(0);
    final HBaseAdmin localAdmin = this.admin;
    for (int i = 0; i < count; i++) {
      threads[i] = new Thread(Integer.toString(i)) {
        @Override
        public void run() {
          try {
            localAdmin.createTable(threadDesc);
            successes.incrementAndGet();
          } catch (TableExistsException e) {
            failures.incrementAndGet();
          } catch (IOException e) {
            throw new RuntimeException("Failed threaded create" + getName(), e);
          }
        }
      };
    }
    for (int i = 0; i < count; i++) {
      threads[i].start();
    }
    for (int i = 0; i < count; i++) {
      while(threads[i].isAlive()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
    // All threads are now dead.  Count up how many tables were created and
    // how many failed w/ appropriate exception.
    assertEquals(1, successes.get());
    assertEquals(count - 1, failures.get());
  }

  /**
   * Test for hadoop-1581 'HBASE: Unopenable tablename bug'.
   * @throws Exception
   */
  @Test
  public void testTableNameClash() throws Exception {
    String name = "testTableNameClash";
    admin.createTable(new HTableDescriptor(name + "SOMEUPPERCASE"));
    admin.createTable(new HTableDescriptor(name));
    // Before fix, below would fail throwing a NoServerForRegionException.
    new HTable(TEST_UTIL.getConfiguration(), name);
  }

  /***
   * HMaster.createTable used to be kind of synchronous call
   * Thus creating of table with lots of regions can cause RPC timeout
   * After the fix to make createTable truly async, RPC timeout shouldn't be an
   * issue anymore
   * @throws Exception
   */
  @Test
  public void testCreateTableRPCTimeOut() throws Exception {
    String name = "testCreateTableRPCTimeOut";
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 1500);

    int expectedRegions = 100;
    // Use 80 bit numbers to make sure we aren't limited
    byte [] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    byte [] endKey =   { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };
    HBaseAdmin hbaseadmin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    hbaseadmin.createTable(new HTableDescriptor(name), startKey, endKey,
      expectedRegions);
  }

  /**
   * Test read only tables
   * @throws Exception
   */
  @Test
  public void testReadOnlyTable() throws Exception {
    byte [] name = Bytes.toBytes("testReadOnlyTable");
    HTable table = TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    byte[] value = Bytes.toBytes("somedata");
    // This used to use an empty row... That must have been a bug
    Put put = new Put(value);
    put.add(HConstants.CATALOG_FAMILY, HConstants.CATALOG_FAMILY, value);
    table.put(put);
  }

  /**
   * Test that user table names can contain '-' and '.' so long as they do not
   * start with same. HBASE-771
   * @throws IOException
   */
  @Test
  public void testTableNames() throws IOException {
    byte[][] illegalNames = new byte[][] {
        Bytes.toBytes("-bad"),
        Bytes.toBytes(".bad"),
        HConstants.ROOT_TABLE_NAME,
        HConstants.META_TABLE_NAME
    };
    for (int i = 0; i < illegalNames.length; i++) {
      try {
        new HTableDescriptor(illegalNames[i]);
        throw new IOException("Did not detect '" +
          Bytes.toString(illegalNames[i]) + "' as an illegal user table name");
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
    byte[] legalName = Bytes.toBytes("g-oo.d");
    try {
      new HTableDescriptor(legalName);
    } catch (IllegalArgumentException e) {
      throw new IOException("Legal user table name: '" +
        Bytes.toString(legalName) + "' caused IllegalArgumentException: " +
        e.getMessage());
    }
  }

  /**
   * For HADOOP-2579
   * @throws IOException
   */
  @Test (expected=TableExistsException.class)
  public void testTableExistsExceptionWithATable() throws IOException {
    final byte [] name = Bytes.toBytes("testTableExistsExceptionWithATable");
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
  }

  /**
   * Can't disable a table if the table isn't in enabled state
   * @throws IOException
   */
  @Test (expected=TableNotEnabledException.class)
  public void testTableNotEnabledExceptionWithATable() throws IOException {
    final byte [] name = Bytes.toBytes(
      "testTableNotEnabledExceptionWithATable");
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    this.admin.disableTable(name);
    this.admin.disableTable(name);
  }

  /**
   * Can't enable a table if the table isn't in disabled state
   * @throws IOException
   */
  @Test (expected=TableNotDisabledException.class)
  public void testTableNotDisabledExceptionWithATable() throws IOException {
    final byte [] name = Bytes.toBytes(
      "testTableNotDisabledExceptionWithATable");
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    this.admin.enableTable(name);
  }

  /**
   * For HADOOP-2579
   * @throws IOException
   */
  @Test (expected=TableNotFoundException.class)
  public void testTableNotFoundExceptionWithoutAnyTables() throws IOException {
    new HTable(TEST_UTIL.getConfiguration(),
        "testTableNotFoundExceptionWithoutAnyTables");
  }
  @Test
  public void testShouldCloseTheRegionBasedOnTheEncodedRegionName()
      throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion");
    HBaseAdmin admin = createTable(TABLENAME);

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLENAME);
    List<HRegionInfo> onlineRegions = rs.getOnlineRegions();
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaRegion() && !regionInfo.isRootRegion()) {
        info = regionInfo;
        admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(), rs
            .getServerName().getServerName());
      }
    }
    Thread.sleep(1000);
    onlineRegions = rs.getOnlineRegions();
    assertFalse("The region should not be present in online regions list.",
        onlineRegions.contains(info));
  }

  @Test
  public void testCloseRegionIfInvalidRegionNameIsPassed() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion1");
    HBaseAdmin admin = createTable(TABLENAME);

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLENAME);
    List<HRegionInfo> onlineRegions = rs.getOnlineRegions();
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaRegion() && !regionInfo.isRootRegion()) {
        if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion1")) {
          info = regionInfo;
          admin.closeRegionWithEncodedRegionName("sample", rs.getServerName()
              .getServerName());
        }
      }
    }
    onlineRegions = rs.getOnlineRegions();
    assertTrue("The region should be present in online regions list.",
        onlineRegions.contains(info));
  }

  @Test
  public void testCloseRegionThatFetchesTheHRIFromMeta() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion2");
    HBaseAdmin admin = createTable(TABLENAME);

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLENAME);
    List<HRegionInfo> onlineRegions = rs.getOnlineRegions();
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaRegion() && !regionInfo.isRootRegion()) {

        if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion2")) {
          info = regionInfo;
          admin.closeRegion(regionInfo.getRegionNameAsString(), rs
              .getServerName().getServerName());
        }
      }
    }
    Thread.sleep(1000);
    onlineRegions = rs.getOnlineRegions();
    assertFalse("The region should not be present in online regions list.",
        onlineRegions.contains(info));
  }

  @Test
  public void testCloseRegionWhenServerNameIsNull() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion3");
    HBaseAdmin admin = createTable(TABLENAME);

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLENAME);

    try {
      List<HRegionInfo> onlineRegions = rs.getOnlineRegions();
      for (HRegionInfo regionInfo : onlineRegions) {
        if (!regionInfo.isMetaRegion() && !regionInfo.isRootRegion()) {
          if (regionInfo.getRegionNameAsString()
              .contains("TestHBACloseRegion3")) {
            admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(),
                null);
          }
        }
      }
      fail("The test should throw exception if the servername passed is null.");
    } catch (IllegalArgumentException e) {
    }
  }
  

  @Test
  public void testCloseRegionWhenServerNameIsEmpty() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegionWhenServerNameIsEmpty");
    HBaseAdmin admin = createTable(TABLENAME);

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLENAME);

    try {
      List<HRegionInfo> onlineRegions = rs.getOnlineRegions();
      for (HRegionInfo regionInfo : onlineRegions) {
        if (!regionInfo.isMetaRegion() && !regionInfo.isRootRegion()) {
          if (regionInfo.getRegionNameAsString()
              .contains("TestHBACloseRegionWhenServerNameIsEmpty")) {
            admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(),
                " ");
          }
        }
      }
      fail("The test should throw exception if the servername passed is empty.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testCloseRegionWhenEncodedRegionNameIsNotGiven() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion4");
    HBaseAdmin admin = createTable(TABLENAME);

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLENAME);

    List<HRegionInfo> onlineRegions = rs.getOnlineRegions();
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaRegion() && !regionInfo.isRootRegion()) {
        if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion4")) {
          info = regionInfo;
          admin.closeRegionWithEncodedRegionName(regionInfo
              .getRegionNameAsString(), rs.getServerName().getServerName());
        }
      }
    }
    onlineRegions = rs.getOnlineRegions();
    assertTrue("The region should be present in online regions list.",
        onlineRegions.contains(info));
  }

  private HBaseAdmin createTable(byte[] TABLENAME) throws IOException {

    Configuration config = TEST_UTIL.getConfiguration();
    HBaseAdmin admin = new HBaseAdmin(config);

    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    HColumnDescriptor hcd = new HColumnDescriptor("value");

    htd.addFamily(hcd);
    admin.createTable(htd, null);
    return admin;
  }


  @Test
  public void testHundredsOfTable() throws IOException{
    final int times = 100;
    HColumnDescriptor fam1 = new HColumnDescriptor("fam1");
    HColumnDescriptor fam2 = new HColumnDescriptor("fam2");
    HColumnDescriptor fam3 = new HColumnDescriptor("fam3");

    for(int i = 0; i < times; i++) {
      HTableDescriptor htd = new HTableDescriptor("table"+i);
      htd.addFamily(fam1);
      htd.addFamily(fam2);
      htd.addFamily(fam3);
      this.admin.createTable(htd);
    }

    for(int i = 0; i < times; i++) {
      String tableName = "table"+i;
      this.admin.disableTable(tableName);
      byte [] tableNameBytes = Bytes.toBytes(tableName);
      assertTrue(this.admin.isTableDisabled(tableNameBytes));
      this.admin.enableTable(tableName);
      assertFalse(this.admin.isTableDisabled(tableNameBytes));
      this.admin.disableTable(tableName);
      assertTrue(this.admin.isTableDisabled(tableNameBytes));
      this.admin.deleteTable(tableName);
    }
  }
  

  /**
   * For HBASE-2556
   * @throws IOException
   */  
  @Test
  public void testGetTableRegions() throws IOException {

    byte[] tableName = Bytes.toBytes("testGetTableRegions");

    int expectedRegions = 10;

    // Use 80 bit numbers to make sure we aren't limited
    byte [] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    byte [] endKey =   { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };


    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.createTable(desc, startKey, endKey, expectedRegions);

    List<HRegionInfo> RegionInfos = admin.getTableRegions(tableName);
    
    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + RegionInfos.size(),
        expectedRegions, RegionInfos.size());
    
 }

  @Test
  public void testHLogRollWriting() throws Exception {
    setUpforLogRolling();
    String className = this.getClass().getName();
    StringBuilder v = new StringBuilder(className);
    while (v.length() < 1000) {
      v.append(className);
    }
    byte[] value = Bytes.toBytes(v.toString());
    HRegionServer regionServer = startAndWriteData("TestLogRolling", value);
    LOG.info("after writing there are "
        + TestHLogUtils.getNumLogFiles(regionServer.getWAL()) + " log files");

    // flush all regions

    List<HRegion> regions = new ArrayList<HRegion>(regionServer
        .getOnlineRegionsLocalContext());
    for (HRegion r : regions) {
      r.flushcache();
    }
    admin.rollHLogWriter(regionServer.getServerName().getServerName());
    int count = TestHLogUtils.getNumLogFiles(regionServer.getWAL());
    LOG.info("after flushing all regions and rolling logs there are " +
        count + " log files");
    assertTrue(("actual count: " + count), count <= 2);
  }

  private void setUpforLogRolling() {
    // Force a region split after every 768KB
    TEST_UTIL.getConfiguration().setLong("hbase.hregion.max.filesize",
        768L * 1024L);

    // We roll the log after every 32 writes
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.maxlogentries", 32);

    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.logroll.errors.tolerated", 2);
    TEST_UTIL.getConfiguration().setInt("ipc.ping.interval", 10 * 1000);
    TEST_UTIL.getConfiguration().setInt("ipc.socket.timeout", 10 * 1000);
    TEST_UTIL.getConfiguration().setInt("hbase.rpc.timeout", 10 * 1000);

    // For less frequently updated regions flush after every 2 flushes
    TEST_UTIL.getConfiguration().setInt(
        "hbase.hregion.memstore.optionalflushcount", 2);

    // We flush the cache after every 8192 bytes
    TEST_UTIL.getConfiguration().setInt("hbase.hregion.memstore.flush.size",
        8192);

    // Increase the amount of time between client retries
    TEST_UTIL.getConfiguration().setLong("hbase.client.pause", 10 * 1000);

    // Reduce thread wake frequency so that other threads can get
    // a chance to run.
    TEST_UTIL.getConfiguration().setInt(HConstants.THREAD_WAKE_FREQUENCY,
        2 * 1000);

    /**** configuration for testLogRollOnDatanodeDeath ****/
    // make sure log.hflush() calls syncFs() to open a pipeline
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // lower the namenode & datanode heartbeat so the namenode
    // quickly detects datanode failures
    TEST_UTIL.getConfiguration().setInt("heartbeat.recheck.interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    // the namenode might still try to choose the recently-dead datanode
    // for a pipeline, so try to a new pipeline multiple times
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.write.retries", 30);
    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.hlog.tolerable.lowreplication", 2);
    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.hlog.lowreplication.rolllimit", 3);
  }
  
  private HRegionServer startAndWriteData(String tableName, byte[] value)
      throws IOException {
    // When the META table can be opened, the region servers are running
    new HTable(TEST_UTIL.getConfiguration(), HConstants.META_TABLE_NAME);
    HRegionServer regionServer = TEST_UTIL.getHbaseCluster()
        .getRegionServerThreads().get(0).getRegionServer();

    // Create the test table and open it
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);

    regionServer = TEST_UTIL.getRSForFirstRegionInTable(Bytes
        .toBytes(tableName));
    for (int i = 1; i <= 256; i++) { // 256 writes should cause 8 log rolls
      Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", i)));
      put.add(HConstants.CATALOG_FAMILY, null, value);
      table.put(put);
      if (i % 32 == 0) {
        // After every 32 writes sleep to let the log roller run
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
    return regionServer;
  }
  
  /**
   * HBASE-4417 checkHBaseAvailable() doesn't close zk connections
   */
  @Test
  public void testCheckHBaseAvailableClosesConnection() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    for(int i=0; i<1000;i++) {
      HBaseAdmin.checkHBaseAvailable(conf);
    }
  }
  
}
