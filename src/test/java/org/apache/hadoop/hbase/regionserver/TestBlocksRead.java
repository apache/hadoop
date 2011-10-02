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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;

import org.junit.Test;

public class TestBlocksRead extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestBlocksRead.class);

  private static BlockCache blockCache;

  private HBaseConfiguration getConf() {
    HBaseConfiguration conf = new HBaseConfiguration();

    // disable compactions in this test.
    conf.setInt("hbase.hstore.compactionThreshold", 10000);
    return conf;
  }

  HRegion region = null;
  private final String DIR = HBaseTestingUtility.getTestDir() +
    "/TestBlocksRead/";

  /**
   * @see org.apache.hadoop.hbase.HBaseTestCase#setUp()
   */
  @SuppressWarnings("deprecation")
  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    EnvironmentEdgeManagerTestHelper.reset();
  }

  private void initHRegion (byte [] tableName, String callingMethod,
      HBaseConfiguration conf, byte [] ... families)
    throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for(byte [] family : families) {
        HColumnDescriptor familyDesc =  new HColumnDescriptor(
          family,
          HColumnDescriptor.DEFAULT_VERSIONS,
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          HColumnDescriptor.DEFAULT_BLOCKCACHE,
          1, // small block size deliberate; each kv on its own block
          HColumnDescriptor.DEFAULT_TTL,
          HColumnDescriptor.DEFAULT_BLOOMFILTER,
          HColumnDescriptor.DEFAULT_REPLICATION_SCOPE);
      htd.addFamily(familyDesc);
    }
    HRegionInfo info = new HRegionInfo(htd.getName(), null, null, false);
    Path path = new Path(DIR + callingMethod);
    region = HRegion.createHRegion(info, path, conf, htd);
    blockCache = StoreFile.getBlockCache(conf);
  }

  private void putData(byte[] cf, String row, String col, long version)
  throws IOException {
    putData(cf, row, col, version, version);
  }

  // generates a value to put for a row/col/version.
  private static byte[] genValue(String row, String col, long version) {
    return Bytes.toBytes("Value:" + row + "#" + col + "#" + version);
  }

  private void putData(byte[] cf, String row, String col,
                       long versionStart, long versionEnd)
  throws IOException {
    byte columnBytes[] = Bytes.toBytes(col);
    Put put = new Put(Bytes.toBytes(row));

    for (long version = versionStart; version <= versionEnd; version++) {
      put.add(cf, columnBytes, version, genValue(row, col, version));
    }
    region.put(put);
  }

  private KeyValue[] getData(byte[] cf, String row, List<String> columns,
                             int expBlocks)
    throws IOException {

    long blocksStart = getBlkAccessCount(cf);
    Get get = new Get(Bytes.toBytes(row));

    for (String column : columns) {
      get.addColumn(cf, Bytes.toBytes(column));
    }

    KeyValue[] kvs = region.get(get, null).raw();
    long blocksEnd = getBlkAccessCount(cf);
    if (expBlocks != -1) {
      assertEquals("Blocks Read Check", expBlocks, blocksEnd - blocksStart);
    }
    System.out.println("Blocks Read = " + (blocksEnd - blocksStart) +
                       "Expected = " + expBlocks);
    return kvs;
  }

  private KeyValue[] getData(byte[] cf, String row, String column,
                             int expBlocks)
    throws IOException {
    return getData(cf, row, Arrays.asList(column), expBlocks);
  }

  private void deleteFamily(byte[] cf, String row, long version)
    throws IOException {
    Delete del = new Delete(Bytes.toBytes(row));
    del.deleteFamily(cf, version);
    region.delete(del, null, true);
  }

  private void deleteFamily(byte[] cf, String row, String column, long version)
    throws IOException {
    Delete del = new Delete(Bytes.toBytes(row));
    del.deleteColumns(cf, Bytes.toBytes(column), version);
    region.delete(del, null, true);
  }

  private static void verifyData(KeyValue kv, String expectedRow,
                                 String expectedCol, long expectedVersion) {
    assertEquals("RowCheck", expectedRow, Bytes.toString(kv.getRow()));
    assertEquals("ColumnCheck", expectedCol, Bytes.toString(kv.getQualifier()));
    assertEquals("TSCheck", expectedVersion, kv.getTimestamp());
    assertEquals("ValueCheck",
                 Bytes.toString(genValue(expectedRow, expectedCol, expectedVersion)),
                 Bytes.toString(kv.getValue()));
  }

  private static long getBlkAccessCount(byte[] cf) {
      return blockCache.getStats().getRequestCount();
  }

  /**
   * Test # of blocks read for some simple seek cases.
   * @throws Exception
   */
  @Test
  public void testBlocksRead() throws Exception {
    byte [] TABLE = Bytes.toBytes("testBlocksRead");
    byte [] FAMILY = Bytes.toBytes("cf1");
    byte [][] FAMILIES = new byte[][] { FAMILY };
    KeyValue kvs[];

    HBaseConfiguration conf = getConf();
    initHRegion(TABLE, getName(), conf, FAMILIES);

    putData(FAMILY, "row", "col1", 1);
    putData(FAMILY, "row", "col2", 2);
    putData(FAMILY, "row", "col3", 3);
    putData(FAMILY, "row", "col4", 4);
    putData(FAMILY, "row", "col5", 5);
    putData(FAMILY, "row", "col6", 6);
    putData(FAMILY, "row", "col7", 7);
    region.flushcache();

    // Expected block reads: 1
    kvs = getData(FAMILY, "row", "col1", 1);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col1", 1);

    // Expected block reads: 2
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2"), 2);
    assertEquals(2, kvs.length);
    verifyData(kvs[0], "row", "col1", 1);
    verifyData(kvs[1], "row", "col2", 2);

    // Expected block reads: 3
    kvs = getData(FAMILY, "row", Arrays.asList("col2", "col3"), 3);
    assertEquals(2, kvs.length);
    verifyData(kvs[0], "row", "col2", 2);
    verifyData(kvs[1], "row", "col3", 3);

    // Expected block reads: 3
    kvs = getData(FAMILY, "row", Arrays.asList("col5"), 3);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col5", 5);
  }

  /**
   * Test # of blocks read (targetted at some of the cases Lazy Seek optimizes).
   * @throws Exception
   */
  @Test
  public void testLazySeekBlocksRead() throws Exception {
    byte [] TABLE = Bytes.toBytes("testLazySeekBlocksRead");
    byte [] FAMILY = Bytes.toBytes("cf1");
    byte [][] FAMILIES = new byte[][] { FAMILY };
    KeyValue kvs[];

    HBaseConfiguration conf = getConf();
    initHRegion(TABLE, getName(), conf, FAMILIES);

    // File 1
    putData(FAMILY, "row", "col1", 1);
    putData(FAMILY, "row", "col2", 2);
    region.flushcache();

    // File 2
    putData(FAMILY, "row", "col1", 3);
    putData(FAMILY, "row", "col2", 4);
    region.flushcache();

    // Baseline expected blocks read: 2
    kvs = getData(FAMILY, "row", Arrays.asList("col1"), 2);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col1", 3);

    // Baseline expected blocks read: 4
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2"), 4);
    assertEquals(2, kvs.length);
    verifyData(kvs[0], "row", "col1", 3);
    verifyData(kvs[1], "row", "col2", 4);

    // File 3: Add another column
    putData(FAMILY, "row", "col3", 5);
    region.flushcache();

    // Baseline expected blocks read: 5
    kvs = getData(FAMILY, "row", "col3", 5);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col3", 5);

    // Get a column from older file.
    // Baseline expected blocks read: 3
    kvs = getData(FAMILY, "row", Arrays.asList("col1"), 3);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col1", 3);

    // File 4: Delete the entire row.
    deleteFamily(FAMILY, "row", 6);
    region.flushcache();

    // Baseline expected blocks read: 6.
    kvs = getData(FAMILY, "row", "col1", 6);
    assertEquals(0, kvs.length);
    kvs = getData(FAMILY, "row", "col2", 6);
    assertEquals(0, kvs.length);
    kvs = getData(FAMILY, "row", "col3", 6);
    assertEquals(0, kvs.length);
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2", "col3"), 6);
    assertEquals(0, kvs.length);

    // File 5: Delete
    deleteFamily(FAMILY, "row", 10);
    region.flushcache();

    // File 6: some more puts, but with timestamps older than the
    // previous delete.
    putData(FAMILY, "row", "col1", 7);
    putData(FAMILY, "row", "col2", 8);
    putData(FAMILY, "row", "col3", 9);
    region.flushcache();

    // Baseline expected blocks read: 10
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2", "col3"), 10);
    assertEquals(0, kvs.length);

    // File 7: Put back new data
    putData(FAMILY, "row", "col1", 11);
    putData(FAMILY, "row", "col2", 12);
    putData(FAMILY, "row", "col3", 13);
    region.flushcache();

    // Baseline expected blocks read: 13
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2", "col3"), 13);
    assertEquals(3, kvs.length);
    verifyData(kvs[0], "row", "col1", 11);
    verifyData(kvs[1], "row", "col2", 12);
    verifyData(kvs[2], "row", "col3", 13);
  }
}
