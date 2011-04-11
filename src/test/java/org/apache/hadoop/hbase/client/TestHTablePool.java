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

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests HTablePool.
 */
public class TestHTablePool  {
  private static HBaseTestingUtility TEST_UTIL   =  new HBaseTestingUtility();
  private final static byte [] TABLENAME = Bytes.toBytes("TestHTablePool");

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLENAME, HConstants.CATALOG_FAMILY);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testTableWithStringName() throws Exception {
    HTablePool pool =
      new HTablePool(TEST_UTIL.getConfiguration(), Integer.MAX_VALUE);
    String tableName = Bytes.toString(TABLENAME);

    // Request a table from an empty pool
    HTableInterface table = pool.getTable(tableName);
    Assert.assertNotNull(table);

    // Return the table to the pool
    pool.putTable(table);

    // Request a table of the same name
    HTableInterface sameTable = pool.getTable(tableName);
    Assert.assertSame(table, sameTable);
  }

  @Test
  public void testTableWithByteArrayName() throws IOException {
    HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(), Integer.MAX_VALUE);

    // Request a table from an empty pool
    HTableInterface table = pool.getTable(TABLENAME);
    Assert.assertNotNull(table);

    // Return the table to the pool
    pool.putTable(table);

    // Request a table of the same name
    HTableInterface sameTable = pool.getTable(TABLENAME);
    Assert.assertSame(table, sameTable);
  }

  @Test
  public void testTableWithMaxSize() throws Exception {
    HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(), 2);

    // Request tables from an empty pool
    HTableInterface table1 = pool.getTable(TABLENAME);
    HTableInterface table2 = pool.getTable(TABLENAME);
    HTableInterface table3 = pool.getTable(TABLENAME);

    // Return the tables to the pool
    pool.putTable(table1);
    pool.putTable(table2);
    // The pool should reject this one since it is already full
    pool.putTable(table3);

    // Request tables of the same name
    HTableInterface sameTable1 = pool.getTable(TABLENAME);
    HTableInterface sameTable2 = pool.getTable(TABLENAME);
    HTableInterface sameTable3 = pool.getTable(TABLENAME);
    Assert.assertSame(table1, sameTable1);
    Assert.assertSame(table2, sameTable2);
    Assert.assertNotSame(table3, sameTable3);
  }

  @Test
  public void testTablesWithDifferentNames() throws IOException {
    HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(), Integer.MAX_VALUE);
    byte [] otherTable = Bytes.toBytes("OtherTable");
    TEST_UTIL.createTable(otherTable, HConstants.CATALOG_FAMILY);

    // Request a table from an empty pool
    HTableInterface table1 = pool.getTable(TABLENAME);
    HTableInterface table2 = pool.getTable(otherTable);
    Assert.assertNotNull(table2);

    // Return the tables to the pool
    pool.putTable(table1);
    pool.putTable(table2);

    // Request tables of the same names
    HTableInterface sameTable1 = pool.getTable(TABLENAME);
    HTableInterface sameTable2 = pool.getTable(otherTable);
    Assert.assertSame(table1, sameTable1);
    Assert.assertSame(table2, sameTable2);
  }


  @Test
  public void testCloseTablePool() throws IOException {
    HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(), 4);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    if (admin.tableExists(TABLENAME)) {
      admin.disableTable(TABLENAME);
      admin.deleteTable(TABLENAME);
    }

    HTableDescriptor tableDescriptor = new HTableDescriptor(TABLENAME);
    tableDescriptor.addFamily(new HColumnDescriptor("randomFamily"));
    admin.createTable(tableDescriptor);


    // Request tables from an empty pool
    HTableInterface[] tables = new HTableInterface[4];
    for (int i = 0; i < 4; ++i ) {
      tables[i] = pool.getTable(TABLENAME);
    }

    pool.closeTablePool(TABLENAME);

    for (int i = 0; i < 4; ++i ) {
      pool.putTable(tables[i]);
    }

    Assert.assertEquals(4, pool.getCurrentPoolSize(Bytes.toString(TABLENAME)));

    pool.closeTablePool(TABLENAME);

    Assert.assertEquals(0, pool.getCurrentPoolSize(Bytes.toString(TABLENAME)));
  }
}