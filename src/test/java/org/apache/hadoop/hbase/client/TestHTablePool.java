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
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Tests HTablePool.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({TestHTablePool.TestHTableReusablePool.class, TestHTablePool.TestHTableThreadLocalPool.class})
public class TestHTablePool {
	private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
	private final static byte[] TABLENAME = Bytes.toBytes("TestHTablePool");

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
			TEST_UTIL.startMiniCluster(1);
			TEST_UTIL.createTable(TABLENAME, HConstants.CATALOG_FAMILY);
		}

    @AfterClass
		public static void tearDownAfterClass() throws IOException {
			TEST_UTIL.shutdownMiniCluster();
		}

	public abstract static class TestHTablePoolType extends TestCase {
		protected abstract PoolType getPoolType();

		@Test
		public void testTableWithStringName() throws Exception {
			HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(),
					Integer.MAX_VALUE, getPoolType());
			String tableName = Bytes.toString(TABLENAME);

			// Request a table from an empty pool
			HTableInterface table = pool.getTable(tableName);
			Assert.assertNotNull(table);

			// Close table (returns table to the pool)
			table.close();

			// Request a table of the same name
			HTableInterface sameTable = pool.getTable(tableName);
			Assert.assertSame(
					((HTablePool.PooledHTable) table).getWrappedTable(),
					((HTablePool.PooledHTable) sameTable).getWrappedTable());
		}

		@Test
		public void testTableWithByteArrayName() throws IOException {
			HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(),
					Integer.MAX_VALUE, getPoolType());

			// Request a table from an empty pool
			HTableInterface table = pool.getTable(TABLENAME);
			Assert.assertNotNull(table);

			// Close table (returns table to the pool)
			table.close();

			// Request a table of the same name
			HTableInterface sameTable = pool.getTable(TABLENAME);
			Assert.assertSame(
					((HTablePool.PooledHTable) table).getWrappedTable(),
					((HTablePool.PooledHTable) sameTable).getWrappedTable());
		}

		@Test
		public void testTablesWithDifferentNames() throws IOException {
			HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(),
					Integer.MAX_VALUE, getPoolType());
      // We add the class to the table name as the HBase cluster is reused
      //  during the tests: this gives naming unicity.
			byte[] otherTable = Bytes.toBytes(
        "OtherTable_" + getClass().getSimpleName()
      );
			TEST_UTIL.createTable(otherTable, HConstants.CATALOG_FAMILY);

			// Request a table from an empty pool
			HTableInterface table1 = pool.getTable(TABLENAME);
			HTableInterface table2 = pool.getTable(otherTable);
			Assert.assertNotNull(table2);

			// Close tables (returns tables to the pool)
			table1.close();
			table2.close();

			// Request tables of the same names
			HTableInterface sameTable1 = pool.getTable(TABLENAME);
			HTableInterface sameTable2 = pool.getTable(otherTable);
			Assert.assertSame(
					((HTablePool.PooledHTable) table1).getWrappedTable(),
					((HTablePool.PooledHTable) sameTable1).getWrappedTable());
			Assert.assertSame(
					((HTablePool.PooledHTable) table2).getWrappedTable(),
					((HTablePool.PooledHTable) sameTable2).getWrappedTable());
		}
    @Test
    public void testProxyImplementationReturned() {
      HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(),
          Integer.MAX_VALUE);
      String tableName = Bytes.toString(TABLENAME);// Request a table from
                              // an
                              // empty pool
      HTableInterface table = pool.getTable(tableName);

      // Test if proxy implementation is returned
      Assert.assertTrue(table instanceof HTablePool.PooledHTable);
    }

    @Test
    public void testDeprecatedUsagePattern() throws IOException {
      HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(),
          Integer.MAX_VALUE);
      String tableName = Bytes.toString(TABLENAME);// Request a table from
                              // an
                              // empty pool

      // get table will return proxy implementation
      HTableInterface table = pool.getTable(tableName);

      // put back the proxy implementation instead of closing it
      pool.putTable(table);

      // Request a table of the same name
      HTableInterface sameTable = pool.getTable(tableName);

      // test no proxy over proxy created
      Assert.assertSame(((HTablePool.PooledHTable) table).getWrappedTable(),
          ((HTablePool.PooledHTable) sameTable).getWrappedTable());
    }

    @Test
    public void testReturnDifferentTable() throws IOException {
      HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(),
          Integer.MAX_VALUE);
      String tableName = Bytes.toString(TABLENAME);// Request a table from
                              // an
                              // empty pool

      // get table will return proxy implementation
      final HTableInterface table = pool.getTable(tableName);
      HTableInterface alienTable = new HTable(TEST_UTIL.getConfiguration(),
          TABLENAME) {
        // implementation doesn't matter as long the table is not from
        // pool
      };
      try {
        // put the wrong table in pool
        pool.putTable(alienTable);
        Assert.fail("alien table accepted in pool");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue("alien table rejected", true);
      }
    }

    @Test
    public void testClassCastException() {
      //this test makes sure that client code that
      //casts the table it got from pool to HTable won't break
      HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(),
        Integer.MAX_VALUE);
      String tableName = Bytes.toString(TABLENAME);
      try {
        // get table and check if type is HTable
        HTable table = (HTable) pool.getTable(tableName);
        Assert.assertTrue("return type is HTable as expected", true);
      } catch (ClassCastException e) {
        Assert.fail("return type is not HTable");
      }
    }
  }

	public static class TestHTableReusablePool extends TestHTablePoolType {
		@Override
		protected PoolType getPoolType() {
			return PoolType.Reusable;
		}

		@Test
		public void testTableWithMaxSize() throws Exception {
			HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(), 2,
					getPoolType());

			// Request tables from an empty pool
			HTableInterface table1 = pool.getTable(TABLENAME);
			HTableInterface table2 = pool.getTable(TABLENAME);
			HTableInterface table3 = pool.getTable(TABLENAME);

			// Close tables (returns tables to the pool)
			table1.close();
			table2.close();
			// The pool should reject this one since it is already full
			table3.close();

			// Request tables of the same name
			HTableInterface sameTable1 = pool.getTable(TABLENAME);
			HTableInterface sameTable2 = pool.getTable(TABLENAME);
			HTableInterface sameTable3 = pool.getTable(TABLENAME);
			Assert.assertSame(
					((HTablePool.PooledHTable) table1).getWrappedTable(),
					((HTablePool.PooledHTable) sameTable1).getWrappedTable());
			Assert.assertSame(
					((HTablePool.PooledHTable) table2).getWrappedTable(),
					((HTablePool.PooledHTable) sameTable2).getWrappedTable());
			Assert.assertNotSame(
					((HTablePool.PooledHTable) table3).getWrappedTable(),
					((HTablePool.PooledHTable) sameTable3).getWrappedTable());
		}

		@Test
		public void testCloseTablePool() throws IOException {
			HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(), 4,
					getPoolType());
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
			for (int i = 0; i < 4; ++i) {
				tables[i] = pool.getTable(TABLENAME);
			}

			pool.closeTablePool(TABLENAME);

			for (int i = 0; i < 4; ++i) {
				tables[i].close();
			}

			Assert.assertEquals(4,
					pool.getCurrentPoolSize(Bytes.toString(TABLENAME)));

			pool.closeTablePool(TABLENAME);

			Assert.assertEquals(0,
					pool.getCurrentPoolSize(Bytes.toString(TABLENAME)));
		}
	}

	public static class TestHTableThreadLocalPool extends TestHTablePoolType {
		@Override
		protected PoolType getPoolType() {
			return PoolType.ThreadLocal;
		}

		@Test
		public void testTableWithMaxSize() throws Exception {
			HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(), 2,
					getPoolType());

			// Request tables from an empty pool
			HTableInterface table1 = pool.getTable(TABLENAME);
			HTableInterface table2 = pool.getTable(TABLENAME);
			HTableInterface table3 = pool.getTable(TABLENAME);

			// Close tables (returns tables to the pool)
			table1.close();
			table2.close();
			// The pool should not reject this one since the number of threads
			// <= 2
			table3.close();

			// Request tables of the same name
			HTableInterface sameTable1 = pool.getTable(TABLENAME);
			HTableInterface sameTable2 = pool.getTable(TABLENAME);
			HTableInterface sameTable3 = pool.getTable(TABLENAME);
			Assert.assertSame(
					((HTablePool.PooledHTable) table3).getWrappedTable(),
					((HTablePool.PooledHTable) sameTable1).getWrappedTable());
			Assert.assertSame(
					((HTablePool.PooledHTable) table3).getWrappedTable(),
					((HTablePool.PooledHTable) sameTable2).getWrappedTable());
			Assert.assertSame(
					((HTablePool.PooledHTable) table3).getWrappedTable(),
					((HTablePool.PooledHTable) sameTable3).getWrappedTable());
		}

		@Test
		public void testCloseTablePool() throws IOException {
			HTablePool pool = new HTablePool(TEST_UTIL.getConfiguration(), 4,
					getPoolType());
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
			for (int i = 0; i < 4; ++i) {
				tables[i] = pool.getTable(TABLENAME);
			}

			pool.closeTablePool(TABLENAME);

			for (int i = 0; i < 4; ++i) {
				tables[i].close();
			}

			Assert.assertEquals(1,
					pool.getCurrentPoolSize(Bytes.toString(TABLENAME)));

			pool.closeTablePool(TABLENAME);

			Assert.assertEquals(0,
					pool.getCurrentPoolSize(Bytes.toString(TABLENAME)));
		}
	}
}
