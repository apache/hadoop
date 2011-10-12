/*
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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the block cache summary functionality in StoreFile, 
 * which contains the BlockCache
 *
 */
public class TestStoreFileBlockCacheSummary {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();  
  private static final String TEST_TABLE = "testTable";
  private static final String TEST_TABLE2 = "testTable2";
  private static final String TEST_CF = "testFamily";
  private static byte [] FAMILY = Bytes.toBytes(TEST_CF);
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");

  private final int TOTAL_ROWS = 4;
  
  /**
   * @throws java.lang.Exception exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws java.lang.Exception exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  

  private Put createPut(byte[] family, String row) {
    Put put = new Put( Bytes.toBytes(row));
    put.add(family, QUALIFIER, VALUE);
    return put;
  }
  
  /**
  * This test inserts data into multiple tables and then reads both tables to ensure
  * they are in the block cache.
  *
  * @throws Exception exception
  */
 @Test
 public void testBlockCacheSummary() throws Exception {
   HTable ht = TEST_UTIL.createTable(Bytes.toBytes(TEST_TABLE), FAMILY);
   addRows(ht, FAMILY);

   HTable ht2 = TEST_UTIL.createTable(Bytes.toBytes(TEST_TABLE2), FAMILY);
   addRows(ht2, FAMILY);

   TEST_UTIL.flush();
   
   scan(ht, FAMILY);
   scan(ht2, FAMILY);
      
   BlockCache bc =
     new CacheConfig(TEST_UTIL.getConfiguration()).getBlockCache();
   List<BlockCacheColumnFamilySummary> bcs = 
     bc.getBlockCacheColumnFamilySummaries(TEST_UTIL.getConfiguration());
   LOG.info("blockCacheSummary: " + bcs);

   assertEquals("blockCache summary has entries", 3, bcs.size());
   
   BlockCacheColumnFamilySummary e = bcs.get(0);
   assertEquals("table", "-ROOT-", e.getTable());
   assertEquals("cf", "info", e.getColumnFamily());

   e = bcs.get(1);
   assertEquals("table", TEST_TABLE, e.getTable());
   assertEquals("cf", TEST_CF, e.getColumnFamily());

   e = bcs.get(2);
   assertEquals("table", TEST_TABLE2, e.getTable());
   assertEquals("cf", TEST_CF, e.getColumnFamily());

 }

 private void addRows(HTable ht, byte[] family) throws IOException {
 
   List<Row> rows = new ArrayList<Row>();
   for (int i = 0; i < TOTAL_ROWS;i++) {
     rows.add(createPut(family, "row" + i));
   }
   
   HTableUtil.bucketRsBatch( ht, rows);
 }

 private void scan(HTable ht, byte[] family) throws IOException {
   Scan scan = new Scan();
   scan.addColumn(family, QUALIFIER);
   
   int count = 0;
   for(@SuppressWarnings("unused") Result result : ht.getScanner(scan)) {
     count++;
   }
   if (TOTAL_ROWS != count) {
     throw new IOException("Incorrect number of rows!");
   }
 }
}
