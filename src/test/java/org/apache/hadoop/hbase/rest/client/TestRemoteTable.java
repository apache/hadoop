/*
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

package org.apache.hadoop.hbase.rest.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.HBaseRESTTestingUtility;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;

import static org.junit.Assert.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRemoteTable {
  private static final Log LOG = LogFactory.getLog(TestRemoteTable.class);
  private static final String TABLE = "TestRemoteTable";
  private static final byte[] ROW_1 = Bytes.toBytes("testrow1");
  private static final byte[] ROW_2 = Bytes.toBytes("testrow2");
  private static final byte[] ROW_3 = Bytes.toBytes("testrow3");
  private static final byte[] ROW_4 = Bytes.toBytes("testrow4");
  private static final byte[] COLUMN_1 = Bytes.toBytes("a");
  private static final byte[] COLUMN_2 = Bytes.toBytes("b");
  private static final byte[] COLUMN_3 = Bytes.toBytes("c");
  private static final byte[] QUALIFIER_1 = Bytes.toBytes("1");
  private static final byte[] QUALIFIER_2 = Bytes.toBytes("2");
  private static final byte[] VALUE_1 = Bytes.toBytes("testvalue1");
  private static final byte[] VALUE_2 = Bytes.toBytes("testvalue2");

  private static final long ONE_HOUR = 60 * 60 * 1000;
  private static final long TS_2 = System.currentTimeMillis();
  private static final long TS_1 = TS_2 - ONE_HOUR;

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL = 
    new HBaseRESTTestingUtility();
  private static RemoteHTable remoteTable;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    LOG.info("Admin Connection=" + admin.getConnection() + ", " + 
      admin.getConnection().getZooKeeperWatcher());
    if (!admin.tableExists(TABLE)) {
      HTableDescriptor htd = new HTableDescriptor(TABLE);
      htd.addFamily(new HColumnDescriptor(COLUMN_1));
      htd.addFamily(new HColumnDescriptor(COLUMN_2));
      htd.addFamily(new HColumnDescriptor(COLUMN_3));
      admin.createTable(htd);
      HTable table = new HTable(TEST_UTIL.getConfiguration(), TABLE);
      LOG.info("Table connection=" + table.getConnection() + ", " +
        admin.getConnection().getZooKeeperWatcher());
      Put put = new Put(ROW_1);
      put.add(COLUMN_1, QUALIFIER_1, TS_2, VALUE_1);
      table.put(put);
      put = new Put(ROW_2);
      put.add(COLUMN_1, QUALIFIER_1, TS_1, VALUE_1);
      put.add(COLUMN_1, QUALIFIER_1, TS_2, VALUE_2);
      put.add(COLUMN_2, QUALIFIER_2, TS_2, VALUE_2);
      table.put(put);
      table.flushCommits();
    }
    remoteTable = new RemoteHTable(
      new Client(new Cluster().add("localhost", 
          REST_TEST_UTIL.getServletPort())),
        TEST_UTIL.getConfiguration(), TABLE, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    remoteTable.close();
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetTableDescriptor() throws IOException {
    HTableDescriptor local = new HTable(TEST_UTIL.getConfiguration(),
      TABLE).getTableDescriptor();
    assertEquals(remoteTable.getTableDescriptor(), local);
  }

  @Test
  public void testGet() throws IOException {
    Get get = new Get(ROW_1);
    Result result = remoteTable.get(get);
    byte[] value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    byte[] value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1));
    assertNull(value2);

    get = new Get(ROW_1);
    get.addFamily(COLUMN_3);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNull(value1);
    assertNull(value2);

    get = new Get(ROW_1);
    get.addColumn(COLUMN_1, QUALIFIER_1);
    get.addColumn(COLUMN_2, QUALIFIER_2);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1));
    assertNull(value2);

    get = new Get(ROW_2);
    result = remoteTable.get(get);    
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_2, value1)); // @TS_2
    assertNotNull(value2);
    assertTrue(Bytes.equals(VALUE_2, value2));

    get = new Get(ROW_2);
    get.addFamily(COLUMN_1);
    result = remoteTable.get(get);    
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_2, value1)); // @TS_2
    assertNull(value2);

    get = new Get(ROW_2);
    get.addColumn(COLUMN_1, QUALIFIER_1);
    get.addColumn(COLUMN_2, QUALIFIER_2);
    result = remoteTable.get(get);    
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_2, value1)); // @TS_2
    assertNotNull(value2);
    assertTrue(Bytes.equals(VALUE_2, value2));

    // test timestamp

    get = new Get(ROW_2);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    get.setTimeStamp(TS_1);
    result = remoteTable.get(get);    
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1)); // @TS_1
    assertNull(value2);

    // test timerange

    get = new Get(ROW_2);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    get.setTimeRange(0, TS_1 + 1);
    result = remoteTable.get(get);    
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1)); // @TS_1
    assertNull(value2);

    // test maxVersions

    get = new Get(ROW_2);
    get.addFamily(COLUMN_1);
    get.setMaxVersions(2);
    result = remoteTable.get(get);
    int count = 0;
    for (KeyValue kv: result.list()) {
      if (Bytes.equals(COLUMN_1, kv.getFamily()) && TS_1 == kv.getTimestamp()) {
        assertTrue(Bytes.equals(VALUE_1, kv.getValue())); // @TS_1
        count++;
      }
      if (Bytes.equals(COLUMN_1, kv.getFamily()) && TS_2 == kv.getTimestamp()) {
        assertTrue(Bytes.equals(VALUE_2, kv.getValue())); // @TS_2
        count++;
      }
    }
    assertEquals(2, count);
  }

  @Test
  public void testPut() throws IOException {
    Put put = new Put(ROW_3);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    remoteTable.put(put);

    Get get = new Get(ROW_3);
    get.addFamily(COLUMN_1);
    Result result = remoteTable.get(get);
    byte[] value = result.getValue(COLUMN_1, QUALIFIER_1);
    assertNotNull(value);
    assertTrue(Bytes.equals(VALUE_1, value));

    // multiput

    List<Put> puts = new ArrayList<Put>();
    put = new Put(ROW_3);
    put.add(COLUMN_2, QUALIFIER_2, VALUE_2);
    puts.add(put);
    put = new Put(ROW_4);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_4);
    put.add(COLUMN_2, QUALIFIER_2, VALUE_2);
    puts.add(put);
    remoteTable.put(puts);

    get = new Get(ROW_3);
    get.addFamily(COLUMN_2);
    result = remoteTable.get(get);
    value = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value);
    assertTrue(Bytes.equals(VALUE_2, value));
    get = new Get(ROW_4);
    result = remoteTable.get(get);
    value = result.getValue(COLUMN_1, QUALIFIER_1);
    assertNotNull(value);
    assertTrue(Bytes.equals(VALUE_1, value));
    value = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value);
    assertTrue(Bytes.equals(VALUE_2, value));
  }

  public void testDelete() throws IOException {
    Put put = new Put(ROW_3);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    put.add(COLUMN_2, QUALIFIER_2, VALUE_2);
    remoteTable.put(put);

    Get get = new Get(ROW_3);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    Result result = remoteTable.get(get);
    byte[] value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    byte[] value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1));
    assertNotNull(value2);
    assertTrue(Bytes.equals(VALUE_2, value2));

    Delete delete = new Delete(ROW_3);
    delete.deleteColumn(COLUMN_2, QUALIFIER_2);
    remoteTable.delete(delete);
    
    get = new Get(ROW_3);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1));
    assertNull(value2);

    delete = new Delete(ROW_3);
    remoteTable.delete(delete);

    get = new Get(ROW_3);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNull(value1);
    assertNull(value2);
  }

  public void testScanner() throws IOException {
    List<Put> puts = new ArrayList<Put>();
    Put put = new Put(ROW_1);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_2);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_3);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_4);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    remoteTable.put(puts);

    ResultScanner scanner = remoteTable.getScanner(new Scan());

    Result[] results = scanner.next(1);
    assertNotNull(results);
    assertEquals(1, results.length);
    assertTrue(Bytes.equals(ROW_1, results[0].getRow()));

    results = scanner.next(3);
    assertNotNull(results);
    assertEquals(3, results.length);
    assertTrue(Bytes.equals(ROW_2, results[0].getRow()));
    assertTrue(Bytes.equals(ROW_3, results[1].getRow()));
    assertTrue(Bytes.equals(ROW_4, results[2].getRow()));

    results = scanner.next(1);
    assertNull(results);

    scanner.close();
  }
}
