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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Test cases for the "load" half of the HFileOutputFormat bulk load
 * functionality. These tests run faster than the full MR cluster
 * tests in TestHFileOutputFormat
 */
public class TestLoadIncrementalHFiles {

  private static final byte[] TABLE = Bytes.toBytes("mytable");
  private static final byte[] QUALIFIER = Bytes.toBytes("myqual");
  private static final byte[] FAMILY = Bytes.toBytes("myfam");

  private static final byte[][] SPLIT_KEYS = new byte[][] {
    Bytes.toBytes("ddd"),
    Bytes.toBytes("ppp")
  };

  public static int BLOCKSIZE = 64*1024;
  public static String COMPRESSION =
    Compression.Algorithm.NONE.getName();

  private HBaseTestingUtility util = new HBaseTestingUtility();

  /**
   * Test case that creates some regions and loads
   * HFiles that fit snugly inside those regions
   */
  @Test
  public void testSimpleLoad() throws Exception {
    runTest("testSimpleLoad", BloomType.NONE,
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("cccc") },
          new byte[][]{ Bytes.toBytes("ddd"), Bytes.toBytes("ooo") },
    });
  }

  /**
   * Test case that creates some regions and loads
   * HFiles that cross the boundaries of those regions
   */
  @Test
  public void testRegionCrossingLoad() throws Exception {
    runTest("testRegionCrossingLoad", BloomType.NONE,
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
          new byte[][]{ Bytes.toBytes("fff"), Bytes.toBytes("zzz") },
    });
  }

  /**
   * Test loading into a column family that has a ROW bloom filter.
   */
  @Test
  public void testRegionCrossingRowBloom() throws Exception {
    runTest("testRegionCrossingLoadRowBloom", BloomType.ROW,
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
          new byte[][]{ Bytes.toBytes("fff"), Bytes.toBytes("zzz") },
    });
  }
  
  /**
   * Test loading into a column family that has a ROWCOL bloom filter.
   */
  @Test
  public void testRegionCrossingRowColBloom() throws Exception {
    runTest("testRegionCrossingLoadRowColBloom", BloomType.ROWCOL,
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
          new byte[][]{ Bytes.toBytes("fff"), Bytes.toBytes("zzz") },
    });
  }

  private void runTest(String testName, BloomType bloomType, 
          byte[][][] hfileRanges) throws Exception {
    Path dir = HBaseTestingUtility.getTestDir(testName);
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(FAMILY));

    int hfileIdx = 0;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      createHFile(util.getConfiguration(), fs, new Path(familyDir, "hfile_"
          + hfileIdx++), FAMILY, QUALIFIER, from, to, 1000);
    }
    int expectedRows = hfileIdx * 1000;


    util.startMiniCluster();
    try {
      HBaseAdmin admin = new HBaseAdmin(util.getConfiguration());
      HTableDescriptor htd = new HTableDescriptor(TABLE);
      HColumnDescriptor familyDesc = new HColumnDescriptor(FAMILY);
      familyDesc.setBloomFilterType(bloomType);
      htd.addFamily(familyDesc);
      admin.createTable(htd, SPLIT_KEYS);

      HTable table = new HTable(util.getConfiguration(), TABLE);
      util.waitTableAvailable(TABLE, 30000);
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
          util.getConfiguration());
      loader.doBulkLoad(dir, table);

      assertEquals(expectedRows, util.countRows(table));
    } finally {
      util.shutdownMiniCluster();
    }
  }

  @Test
  public void testSplitStoreFile() throws IOException {
    Path dir = HBaseTestingUtility.getTestDir("testSplitHFile");
    FileSystem fs = util.getTestFileSystem();
    Path testIn = new Path(dir, "testhfile");
    HColumnDescriptor familyDesc = new HColumnDescriptor(FAMILY);
    createHFile(util.getConfiguration(), fs, testIn, FAMILY, QUALIFIER,
        Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), 1000);

    Path bottomOut = new Path(dir, "bottom.out");
    Path topOut = new Path(dir, "top.out");

    LoadIncrementalHFiles.splitStoreFile(
        util.getConfiguration(), testIn,
        familyDesc, Bytes.toBytes("ggg"),
        bottomOut,
        topOut);

    int rowCount = verifyHFile(bottomOut);
    rowCount += verifyHFile(topOut);
    assertEquals(1000, rowCount);
  }

  private int verifyHFile(Path p) throws IOException {
    Configuration conf = util.getConfiguration();
    HFile.Reader reader = HFile.createReader(
        p.getFileSystem(conf), p, new CacheConfig(conf));
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner(false, false);
    scanner.seekTo();
    int count = 0;
    do {
      count++;
    } while (scanner.next());
    assertTrue(count > 0);
    return count;
  }


  /**
   * Create an HFile with the given number of rows between a given
   * start key and end key.
   * TODO put me in an HFileTestUtil or something?
   */
  static void createHFile(
      Configuration conf,
      FileSystem fs, Path path,
      byte[] family, byte[] qualifier,
      byte[] startKey, byte[] endKey, int numRows) throws IOException
  {
    HFile.Writer writer =
      HFile.getWriterFactory(conf, new CacheConfig(conf)).createWriter(fs, path,
        BLOCKSIZE, COMPRESSION,
        KeyValue.KEY_COMPARATOR);
    long now = System.currentTimeMillis();
    try {
      // subtract 2 since iterateOnSplits doesn't include boundary keys
      for (byte[] key : Bytes.iterateOnSplits(startKey, endKey, numRows-2)) {
        KeyValue kv = new KeyValue(key, family, qualifier, now, key);
        writer.append(kv);
      }
    } finally {
      writer.close();
    }
  }

  private void addStartEndKeysForTest(TreeMap<byte[], Integer> map, byte[] first, byte[] last) {
    Integer value = map.containsKey(first)?(Integer)map.get(first):0;
    map.put(first, value+1);

    value = map.containsKey(last)?(Integer)map.get(last):0;
    map.put(last, value-1);
  }

  @Test 
  public void testInferBoundaries() {
    TreeMap<byte[], Integer> map = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);

    /* Toy example
     *     c---------i            o------p          s---------t     v------x
     * a------e    g-----k   m-------------q   r----s            u----w
     *
     * Should be inferred as:
     * a-----------------k   m-------------q   r--------------t  u---------x
     * 
     * The output should be (m,r,u) 
     */

    String first;
    String last;

    first = "a"; last = "e";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());
    
    first = "r"; last = "s";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "o"; last = "p";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "g"; last = "k";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "v"; last = "x";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "c"; last = "i";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "m"; last = "q";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "s"; last = "t";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());
    
    first = "u"; last = "w";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    byte[][] keysArray = LoadIncrementalHFiles.inferBoundaries(map);
    byte[][] compare = new byte[3][];
    compare[0] = "m".getBytes();
    compare[1] = "r".getBytes(); 
    compare[2] = "u".getBytes();

    assertEquals(keysArray.length, 3);

    for (int row = 0; row<keysArray.length; row++){
      assertArrayEquals(keysArray[row], compare[row]);
    }
  }

}
