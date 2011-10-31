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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.TestHRegionServerBulkLoad;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Multimap;

/**
 * Test cases for the atomic load error handling of the bulk load functionality.
 */
public class TestLoadIncrementalHFilesSplitRecovery {
  final static Log LOG = LogFactory.getLog(TestHRegionServerBulkLoad.class);

  private static HBaseTestingUtility util;

  final static int NUM_CFS = 10;
  final static byte[] QUAL = Bytes.toBytes("qual");
  final static int ROWCOUNT = 100;

  private final static byte[][] families = new byte[NUM_CFS][];
  static {
    for (int i = 0; i < NUM_CFS; i++) {
      families[i] = Bytes.toBytes(family(i));
    }
  }

  static byte[] rowkey(int i) {
    return Bytes.toBytes(String.format("row_%08d", i));
  }

  static String family(int i) {
    return String.format("family_%04d", i);
  }

  static byte[] value(int i) {
    return Bytes.toBytes(String.format("%010d", i));
  }

  public static void buildHFiles(FileSystem fs, Path dir, int value)
      throws IOException {
    byte[] val = value(value);
    for (int i = 0; i < NUM_CFS; i++) {
      Path testIn = new Path(dir, family(i));

      TestHRegionServerBulkLoad.createHFile(fs, new Path(testIn, "hfile_" + i),
          Bytes.toBytes(family(i)), QUAL, val, ROWCOUNT);
    }
  }

  /**
   * Creates a table with given table name and specified number of column
   * families if the table does not already exist.
   */
  private void setupTable(String table, int cfs) throws IOException {
    try {
      LOG.info("Creating table " + table);
      HTableDescriptor htd = new HTableDescriptor(table);
      for (int i = 0; i < 10; i++) {
        htd.addFamily(new HColumnDescriptor(family(i)));
      }

      HBaseAdmin admin = util.getHBaseAdmin();
      admin.createTable(htd);
    } catch (TableExistsException tee) {
      LOG.info("Table " + table + " already exists");
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    util = new HBaseTestingUtility();
    util.startMiniCluster(1);
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    util.shutdownMiniCluster();
  }

  void assertExpectedTable(String table, int count, int value) {
    try {
      assertEquals(util.getHBaseAdmin().listTables(table).length, 1);

      HTable t = new HTable(util.getConfiguration(), table);
      Scan s = new Scan();
      ResultScanner sr = t.getScanner(s);
      int i = 0;
      for (Result r : sr) {
        i++;
        for (NavigableMap<byte[], byte[]> nm : r.getNoVersionMap().values()) {
          for (byte[] val : nm.values()) {
            assertTrue(Bytes.equals(val, value(value)));
          }
        }

      }
      assertEquals(count, i);

    } catch (IOException e) {
      fail("Failed due to exception");
    }
  }

  @Test
  public void testBulkLoadPhaseRecovery() throws Exception {
    String table = "bulkPhaseRetry";
    setupTable(table, 10);

    final AtomicInteger attmptedCalls = new AtomicInteger();
    final AtomicInteger failedCalls = new AtomicInteger();
    LoadIncrementalHFiles lih = new LoadIncrementalHFiles(
        util.getConfiguration()) {

      protected List<LoadQueueItem> tryAtomicRegionLoad(final HConnection conn,
          byte[] tableName, final byte[] first, Collection<LoadQueueItem> lqis) {
        int i = attmptedCalls.incrementAndGet();
        if (i == 1) {
          HConnection errConn = mock(HConnection.class);
          try {
            doThrow(new IOException("injecting bulk load error")).when(errConn)
                .getRegionServerWithRetries((ServerCallable) anyObject());
          } catch (Exception e) {
            LOG.fatal("mocking cruft, should never happen", e);
            throw new RuntimeException("mocking cruft, should never happen");
          }
          failedCalls.incrementAndGet();
          return super.tryAtomicRegionLoad(errConn, tableName, first, lqis);
        }

        return super.tryAtomicRegionLoad(conn, tableName, first, lqis);
      }
    };

    // create HFiles for different column families
    FileSystem fs = util.getTestFileSystem();
    Path dir = util.getDataTestDir(table);
    buildHFiles(fs, dir, 1);
    HTable t = new HTable(util.getConfiguration(), Bytes.toBytes(table));
    lih.doBulkLoad(dir, t);

    // check that data was loaded
    assertEquals(attmptedCalls.get(), 2);
    assertEquals(failedCalls.get(), 1);
    assertExpectedTable(table, ROWCOUNT, 1);
  }

  /**
   * This test exercises the path where there is a split after initial
   * validation but before the atomic bulk load call. We cannot use presplitting
   * to test this path, so we actually inject a split just before the atomic
   * region load.
   */
  @Test
  public void testSplitWhileBulkLoadPhase() throws Exception {
    final String table = "bulkPhaseSplit";
    setupTable(table, 10);
    LoadIncrementalHFiles lih = new LoadIncrementalHFiles(
        util.getConfiguration());


    // create HFiles for different column families
    Path dir = util.getDataTestDir(table);
    Path bulk1 = new Path(dir, "normalBulkload");
    FileSystem fs = util.getTestFileSystem();
    buildHFiles(fs, bulk1, 1);
    HTable t = new HTable(util.getConfiguration(), Bytes.toBytes(table));
    lih.doBulkLoad(bulk1, t);
    assertExpectedTable(table, ROWCOUNT, 1);

    // Now let's cause trouble
    final AtomicInteger attmptedCalls = new AtomicInteger();
    LoadIncrementalHFiles lih2 = new LoadIncrementalHFiles(
        util.getConfiguration()) {

      protected List<LoadQueueItem> tryAtomicRegionLoad(final HConnection conn,
          byte[] tableName, final byte[] first, Collection<LoadQueueItem> lqis) {
        int i = attmptedCalls.incrementAndGet();
        if (i == 1) {
          // On first attempt force a split.
          try {
            // need to call regions server to by synchronous but isn't visible.

            HRegionServer hrs = util.getRSForFirstRegionInTable(Bytes
                .toBytes(table));

            HRegionInfo region = null;
            for (HRegionInfo hri : hrs.getOnlineRegions()) {
              if (Bytes.equals(hri.getTableName(), Bytes.toBytes(table))) {
                // splitRegion doesn't work if startkey/endkey are null
                hrs.splitRegion(hri, rowkey(ROWCOUNT / 2)); // hard code split
              }
            }

            int regions;
            do {
              regions = 0;
              for (HRegionInfo hri : hrs.getOnlineRegions()) {
                if (Bytes.equals(hri.getTableName(), Bytes.toBytes(table))) {
                  regions++;
                }
              }
              if (regions != 2) {
                LOG.info("Taking some time to complete split...");
                Thread.sleep(250);
              }
            } while (regions != 2);
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }

        return super.tryAtomicRegionLoad(conn, tableName, first, lqis);
      }
    };

    // create HFiles for different column families
    Path bulk2 = new Path(dir, "bulkload2");
    buildHFiles(fs, bulk2, 2); // all values are '2'
    lih2.doBulkLoad(bulk2, t);

    // check that data was loaded

    // The three expected attempts are 1) failure because need to split, 2)
    // load of split top 3) load of split bottom
    assertEquals(attmptedCalls.get(), 3);
    assertExpectedTable(table, ROWCOUNT, 2);
  }

  @Test(expected = IOException.class)
  public void testGroupOrSplitFailure() throws Exception {
    String table = "groupOrSplitStoreFail";
    setupTable(table, 10);

    try {
      LoadIncrementalHFiles lih = new LoadIncrementalHFiles(
          util.getConfiguration()) {
        int i = 0;

        protected List<LoadQueueItem> groupOrSplit(
            Multimap<ByteBuffer, LoadQueueItem> regionGroups,
            final LoadQueueItem item, final HTable table,
            final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
          i++;

          if (i == 5) {
            throw new IOException("failure");
          }
          return super.groupOrSplit(regionGroups, item, table, startEndKeys);
        }
      };

      Path dir = util.getDataTestDir(table);

      // create HFiles for different column families
      FileSystem fs = util.getTestFileSystem();
      buildHFiles(fs, dir, 1);
      HTable t = new HTable(util.getConfiguration(), Bytes.toBytes(table));
      lih.doBulkLoad(dir, t);

      // check that data was loaded
      assertExpectedTable(table, ROWCOUNT, 1);

    } finally {
      util.shutdownMiniCluster();
    }
  }
}
