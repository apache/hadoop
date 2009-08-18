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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests HTablePool
 */
public class TestHTablePool extends HBaseTestCase {

  public void testTableWithStringName() {
    HTablePool pool = new HTablePool((HBaseConfiguration)null, Integer.MAX_VALUE);
    String tableName = "testTable";

    // Request a table from an empty pool
    HTableInterface table = pool.getTable(tableName);
    assertNotNull(table);

    // Return the table to the pool
    pool.putTable(table);

    // Request a table of the same name
    HTableInterface sameTable = pool.getTable(tableName);
    assertSame(table, sameTable);
  }

  public void testTableWithByteArrayName() {
    HTablePool pool = new HTablePool((HBaseConfiguration)null, Integer.MAX_VALUE);
    byte[] tableName = Bytes.toBytes("testTable");

    // Request a table from an empty pool
    HTableInterface table = pool.getTable(tableName);
    assertNotNull(table);

    // Return the table to the pool
    pool.putTable(table);

    // Request a table of the same name
    HTableInterface sameTable = pool.getTable(tableName);
    assertSame(table, sameTable);
  }

  public void testTableWithMaxSize() {
    HTablePool pool = new HTablePool((HBaseConfiguration)null, 2);
    String tableName = "testTable";

    // Request tables from an empty pool
    HTableInterface table1 = pool.getTable(tableName);
    HTableInterface table2 = pool.getTable(tableName);
    HTableInterface table3 = pool.getTable(tableName);

    // Return the tables to the pool
    pool.putTable(table1);
    pool.putTable(table2);
    // The pool should reject this one since it is already full
    pool.putTable(table3);

    // Request tables of the same name
    HTableInterface sameTable1 = pool.getTable(tableName);
    HTableInterface sameTable2 = pool.getTable(tableName);
    HTableInterface sameTable3 = pool.getTable(tableName);
    assertSame(table1, sameTable1);
    assertSame(table2, sameTable2);
    assertNotSame(table3, sameTable3);
  }

  public void testTablesWithDifferentNames() {
    HTablePool pool = new HTablePool((HBaseConfiguration)null, Integer.MAX_VALUE);
    String tableName1 = "testTable1";
    String tableName2 = "testTable2";

    // Request a table from an empty pool
    HTableInterface table1 = pool.getTable(tableName1);
    HTableInterface table2 = pool.getTable(tableName2);
    assertNotNull(table2);

    // Return the tables to the pool
    pool.putTable(table1);
    pool.putTable(table2);

    // Request tables of the same names
    HTableInterface sameTable1 = pool.getTable(tableName1);
    HTableInterface sameTable2 = pool.getTable(tableName2);
    assertSame(table1, sameTable1);
    assertSame(table2, sameTable2);
  }

}
