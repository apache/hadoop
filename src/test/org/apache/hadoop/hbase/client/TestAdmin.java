/**
 * Copyright 2009 The Apache Software Foundation
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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;
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
  public void testCreateTable() throws IOException {
    HTableDescriptor [] tables = admin.listTables();
    int numTables = tables.length;
    TEST_UTIL.createTable(Bytes.toBytes("testCreateTable"),
      HConstants.CATALOG_FAMILY);
    tables = this.admin.listTables();
    assertEquals(numTables + 1, tables.length);
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

    this.admin.disableTable(table);

    // Test that table is disabled
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    boolean ok = false;
    try {
      ht.get(get);
    } catch (RetriesExhaustedException e) {
      ok = true;
    }
    assertEquals(true, ok);
    this.admin.enableTable(table);
    
    //Test that table is enabled
    try {
      ht.get(get);
    } catch (RetriesExhaustedException e) {
      ok = false;
    }
    assertEquals(true, ok);
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
    byte [] familyName = HConstants.CATALOG_FAMILY;
    byte [] tableName = Bytes.toBytes("testForceSplit");
    final HTable table = TEST_UTIL.createTable(tableName, familyName);
    byte[] k = new byte[3];
    int rowCount = 0;
    for (byte b1 = 'a'; b1 < 'z'; b1++) {
      for (byte b2 = 'a'; b2 < 'z'; b2++) {
        for (byte b3 = 'a'; b3 < 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.add(familyName, new byte[0], k);
          table.put(put);
          rowCount++;
        }
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
    // tell the master to split the table
    admin.split(Bytes.toString(tableName));
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
  }

  /**
   * HADOOP-2156
   * @throws IOException
   */
  @Test (expected=IllegalArgumentException.class)
  public void testEmptyHHTableDescriptor() throws IOException {
    this.admin.createTable(new HTableDescriptor());
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
  public void testTableNotFoundExceptionWithATable() throws IOException {
    final byte [] name = Bytes.toBytes("testTableNotFoundExceptionWithATable");
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
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
}