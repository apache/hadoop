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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.BeforeClass;

/**
 * Class to test HBaseAdmin.
 * Spins up the minicluster once at test start and then takes it down afterward.
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
}