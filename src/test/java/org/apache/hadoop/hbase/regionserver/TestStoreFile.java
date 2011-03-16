/**
 * Copyright 2007 The Apache Software Foundation
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache.CacheStats;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.mockito.Mockito;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Test HStoreFile
 */
public class TestStoreFile extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestStoreFile.class);
  private MiniDFSCluster cluster;

  @Override
  public void setUp() throws Exception {
    try {
      this.cluster = new MiniDFSCluster(this.conf, 2, true, (String[])null);
      // Set the hbase.rootdir to be the home directory in mini dfs.
      this.conf.set(HConstants.HBASE_DIR,
        this.cluster.getFileSystem().getHomeDirectory().toString());
    } catch (IOException e) {
      shutdownDfs(cluster);
    }
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    shutdownDfs(cluster);
    // ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
    //  "Temporary end-of-test thread dump debugging HADOOP-2040: " + getName());
  }

  /**
   * Write a file and then assert that we can read from top and bottom halves
   * using two HalfMapFiles.
   * @throws Exception
   */
  public void testBasicHalfMapFile() throws Exception {
    // Make up a directory hierarchy that has a regiondir and familyname.
    StoreFile.Writer writer = StoreFile.createWriter(this.fs,
      new Path(new Path(this.testDir, "regionname"), "familyname"), 2 * 1024);
    writeStoreFile(writer);
    checkHalfHFile(new StoreFile(this.fs, writer.getPath(), true, conf,
        StoreFile.BloomType.NONE, false));
  }

  private void writeStoreFile(final StoreFile.Writer writer) throws IOException {
    writeStoreFile(writer, Bytes.toBytes(getName()), Bytes.toBytes(getName()));
  }
  /*
   * Writes HStoreKey and ImmutableBytes data to passed writer and
   * then closes it.
   * @param writer
   * @throws IOException
   */
  public static void writeStoreFile(final StoreFile.Writer writer, byte[] fam, byte[] qualifier)
  throws IOException {
    long now = System.currentTimeMillis();
    try {
      for (char d = FIRST_CHAR; d <= LAST_CHAR; d++) {
        for (char e = FIRST_CHAR; e <= LAST_CHAR; e++) {
          byte[] b = new byte[] { (byte) d, (byte) e };
          writer.append(new KeyValue(b, fam, qualifier, now, b));
        }
      }
    } finally {
      writer.close();
    }
  }

  /**
   * Test that our mechanism of writing store files in one region to reference
   * store files in other regions works.
   * @throws IOException
   */
  public void testReference()
  throws IOException {
    Path storedir = new Path(new Path(this.testDir, "regionname"), "familyname");
    Path dir = new Path(storedir, "1234567890");
    // Make a store file and write data to it.
    StoreFile.Writer writer = StoreFile.createWriter(this.fs, dir, 8 * 1024);
    writeStoreFile(writer);
    StoreFile hsf = new StoreFile(this.fs, writer.getPath(), true, conf,
        StoreFile.BloomType.NONE, false);
    StoreFile.Reader reader = hsf.createReader();
    // Split on a row, not in middle of row.  Midkey returned by reader
    // may be in middle of row.  Create new one with empty column and
    // timestamp.
    KeyValue kv = KeyValue.createKeyValueFromKey(reader.midkey());
    byte [] midRow = kv.getRow();
    kv = KeyValue.createKeyValueFromKey(reader.getLastKey());
    byte [] finalRow = kv.getRow();
    // Make a reference
    Path refPath = StoreFile.split(fs, dir, hsf, midRow, Range.top);
    StoreFile refHsf = new StoreFile(this.fs, refPath, true, conf,
        StoreFile.BloomType.NONE, false);
    // Now confirm that I can read from the reference and that it only gets
    // keys from top half of the file.
    HFileScanner s = refHsf.createReader().getScanner(false, false);
    for(boolean first = true; (!s.isSeeked() && s.seekTo()) || s.next();) {
      ByteBuffer bb = s.getKey();
      kv = KeyValue.createKeyValueFromKey(bb);
      if (first) {
        assertTrue(Bytes.equals(kv.getRow(), midRow));
        first = false;
      }
    }
    assertTrue(Bytes.equals(kv.getRow(), finalRow));
  }

  private void checkHalfHFile(final StoreFile f)
  throws IOException {
    byte [] midkey = f.createReader().midkey();
    KeyValue midKV = KeyValue.createKeyValueFromKey(midkey);
    byte [] midRow = midKV.getRow();
    // Create top split.
    Path topDir = Store.getStoreHomedir(this.testDir, "1",
      Bytes.toBytes(f.getPath().getParent().getName()));
    if (this.fs.exists(topDir)) {
      this.fs.delete(topDir, true);
    }
    Path topPath = StoreFile.split(this.fs, topDir, f, midRow, Range.top);
    // Create bottom split.
    Path bottomDir = Store.getStoreHomedir(this.testDir, "2",
      Bytes.toBytes(f.getPath().getParent().getName()));
    if (this.fs.exists(bottomDir)) {
      this.fs.delete(bottomDir, true);
    }
    Path bottomPath = StoreFile.split(this.fs, bottomDir,
      f, midRow, Range.bottom);
    // Make readers on top and bottom.
    StoreFile.Reader top = new StoreFile(this.fs, topPath, true, conf,
        StoreFile.BloomType.NONE, false).createReader();
    StoreFile.Reader bottom = new StoreFile(this.fs, bottomPath, true, conf,
        StoreFile.BloomType.NONE, false).createReader();
    ByteBuffer previous = null;
    LOG.info("Midkey: " + midKV.toString());
    ByteBuffer bbMidkeyBytes = ByteBuffer.wrap(midkey);
    try {
      // Now make two HalfMapFiles and assert they can read the full backing
      // file, one from the top and the other from the bottom.
      // Test bottom half first.
      // Now test reading from the top.
      boolean first = true;
      ByteBuffer key = null;
      HFileScanner topScanner = top.getScanner(false, false);
      while ((!topScanner.isSeeked() && topScanner.seekTo()) ||
          (topScanner.isSeeked() && topScanner.next())) {
        key = topScanner.getKey();

        assertTrue(topScanner.getReader().getComparator().compare(key.array(),
          key.arrayOffset(), key.limit(), midkey, 0, midkey.length) >= 0);
        if (first) {
          first = false;
          LOG.info("First in top: " + Bytes.toString(Bytes.toBytes(key)));
        }
      }
      LOG.info("Last in top: " + Bytes.toString(Bytes.toBytes(key)));

      first = true;
      HFileScanner bottomScanner = bottom.getScanner(false, false);
      while ((!bottomScanner.isSeeked() && bottomScanner.seekTo()) ||
          bottomScanner.next()) {
        previous = bottomScanner.getKey();
        key = bottomScanner.getKey();
        if (first) {
          first = false;
          LOG.info("First in bottom: " +
            Bytes.toString(Bytes.toBytes(previous)));
        }
        assertTrue(key.compareTo(bbMidkeyBytes) < 0);
      }
      if (previous != null) {
        LOG.info("Last in bottom: " + Bytes.toString(Bytes.toBytes(previous)));
      }
      // Remove references.
      this.fs.delete(topPath, false);
      this.fs.delete(bottomPath, false);

      // Next test using a midkey that does not exist in the file.
      // First, do a key that is < than first key. Ensure splits behave
      // properly.
      byte [] badmidkey = Bytes.toBytes("  .");
      topPath = StoreFile.split(this.fs, topDir, f, badmidkey, Range.top);
      bottomPath = StoreFile.split(this.fs, bottomDir, f, badmidkey,
        Range.bottom);
      top = new StoreFile(this.fs, topPath, true, conf,
          StoreFile.BloomType.NONE, false).createReader();
      bottom = new StoreFile(this.fs, bottomPath, true, conf,
          StoreFile.BloomType.NONE, false).createReader();
      bottomScanner = bottom.getScanner(false, false);
      int count = 0;
      while ((!bottomScanner.isSeeked() && bottomScanner.seekTo()) ||
          bottomScanner.next()) {
        count++;
      }
      // When badkey is < than the bottom, should return no values.
      assertTrue(count == 0);
      // Now read from the top.
      first = true;
      topScanner = top.getScanner(false, false);
      while ((!topScanner.isSeeked() && topScanner.seekTo()) ||
          topScanner.next()) {
        key = topScanner.getKey();
        assertTrue(topScanner.getReader().getComparator().compare(key.array(),
          key.arrayOffset(), key.limit(), badmidkey, 0, badmidkey.length) >= 0);
        if (first) {
          first = false;
          KeyValue keyKV = KeyValue.createKeyValueFromKey(key);
          LOG.info("First top when key < bottom: " + keyKV);
          String tmp = Bytes.toString(keyKV.getRow());
          for (int i = 0; i < tmp.length(); i++) {
            assertTrue(tmp.charAt(i) == 'a');
          }
        }
      }
      KeyValue keyKV = KeyValue.createKeyValueFromKey(key);
      LOG.info("Last top when key < bottom: " + keyKV);
      String tmp = Bytes.toString(keyKV.getRow());
      for (int i = 0; i < tmp.length(); i++) {
        assertTrue(tmp.charAt(i) == 'z');
      }
      // Remove references.
      this.fs.delete(topPath, false);
      this.fs.delete(bottomPath, false);

      // Test when badkey is > than last key in file ('||' > 'zz').
      badmidkey = Bytes.toBytes("|||");
      topPath = StoreFile.split(this.fs, topDir, f, badmidkey, Range.top);
      bottomPath = StoreFile.split(this.fs, bottomDir, f, badmidkey,
        Range.bottom);
      top = new StoreFile(this.fs, topPath, true, conf,
          StoreFile.BloomType.NONE, false).createReader();
      bottom = new StoreFile(this.fs, bottomPath, true, conf,
          StoreFile.BloomType.NONE, false).createReader();
      first = true;
      bottomScanner = bottom.getScanner(false, false);
      while ((!bottomScanner.isSeeked() && bottomScanner.seekTo()) ||
          bottomScanner.next()) {
        key = bottomScanner.getKey();
        if (first) {
          first = false;
          keyKV = KeyValue.createKeyValueFromKey(key);
          LOG.info("First bottom when key > top: " + keyKV);
          tmp = Bytes.toString(keyKV.getRow());
          for (int i = 0; i < tmp.length(); i++) {
            assertTrue(tmp.charAt(i) == 'a');
          }
        }
      }
      keyKV = KeyValue.createKeyValueFromKey(key);
      LOG.info("Last bottom when key > top: " + keyKV);
      for (int i = 0; i < tmp.length(); i++) {
        assertTrue(Bytes.toString(keyKV.getRow()).charAt(i) == 'z');
      }
      count = 0;
      topScanner = top.getScanner(false, false);
      while ((!topScanner.isSeeked() && topScanner.seekTo()) ||
          (topScanner.isSeeked() && topScanner.next())) {
        count++;
      }
      // When badkey is < than the bottom, should return no values.
      assertTrue(count == 0);
    } finally {
      if (top != null) {
        top.close();
      }
      if (bottom != null) {
        bottom.close();
      }
      fs.delete(f.getPath(), true);
    }
  }

  private static String ROOT_DIR =
    HBaseTestingUtility.getTestDir("TestStoreFile").toString();
  private static String localFormatter = "%010d";

  private void bloomWriteRead(StoreFile.Writer writer, FileSystem fs)
  throws Exception {
    float err = conf.getFloat(StoreFile.IO_STOREFILE_BLOOM_ERROR_RATE, 0);
    Path f = writer.getPath();
    long now = System.currentTimeMillis();
    for (int i = 0; i < 2000; i += 2) {
      String row = String.format(localFormatter, i);
      KeyValue kv = new KeyValue(row.getBytes(), "family".getBytes(),
        "col".getBytes(), now, "value".getBytes());
      writer.append(kv);
    }
    writer.close();

    StoreFile.Reader reader = new StoreFile.Reader(fs, f, null, false, false);
    reader.loadFileInfo();
    reader.loadBloomfilter();
    StoreFileScanner scanner = reader.getStoreFileScanner(false, false);

    // check false positives rate
    int falsePos = 0;
    int falseNeg = 0;
    for (int i = 0; i < 2000; i++) {
      String row = String.format(localFormatter, i);
      TreeSet<byte[]> columns = new TreeSet<byte[]>();
      columns.add("family:col".getBytes());

      Scan scan = new Scan(row.getBytes(),row.getBytes());
      scan.addColumn("family".getBytes(), "family:col".getBytes());
      boolean exists = scanner.shouldSeek(scan, columns);
      if (i % 2 == 0) {
        if (!exists) falseNeg++;
      } else {
        if (exists) falsePos++;
      }
    }
    reader.close();
    fs.delete(f, true);
    System.out.println("False negatives: " + falseNeg);
    assertEquals(0, falseNeg);
    System.out.println("False positives: " + falsePos);
    if (!(falsePos <= 2* 2000 * err)) {
      System.out.println("WTFBBQ! " + falsePos + ", " + (2* 2000 * err) );
    }
    assertTrue(falsePos <= 2* 2000 * err);
  }

  public void testBloomFilter() throws Exception {
    FileSystem fs = FileSystem.getLocal(conf);
    conf.setFloat(StoreFile.IO_STOREFILE_BLOOM_ERROR_RATE, (float)0.01);
    conf.setBoolean(StoreFile.IO_STOREFILE_BLOOM_ENABLED, true);

    // write the file
    Path f = new Path(ROOT_DIR, getName());
    StoreFile.Writer writer = new StoreFile.Writer(fs, f,
        StoreFile.DEFAULT_BLOCKSIZE_SMALL, HFile.DEFAULT_COMPRESSION_ALGORITHM,
        conf, KeyValue.COMPARATOR, StoreFile.BloomType.ROW, 2000, false);

    bloomWriteRead(writer, fs);
  }

  public void testBloomTypes() throws Exception {
    float err = (float) 0.01;
    FileSystem fs = FileSystem.getLocal(conf);
    conf.setFloat(StoreFile.IO_STOREFILE_BLOOM_ERROR_RATE, err);
    conf.setBoolean(StoreFile.IO_STOREFILE_BLOOM_ENABLED, true);

    int rowCount = 50;
    int colCount = 10;
    int versions = 2;

    // run once using columns and once using rows
    StoreFile.BloomType[] bt =
      {StoreFile.BloomType.ROWCOL, StoreFile.BloomType.ROW};
    int[] expKeys    = {rowCount*colCount, rowCount};
    // below line deserves commentary.  it is expected bloom false positives
    //  column = rowCount*2*colCount inserts
    //  row-level = only rowCount*2 inserts, but failures will be magnified by
    //              2nd for loop for every column (2*colCount)
    float[] expErr   = {2*rowCount*colCount*err, 2*rowCount*2*colCount*err};

    for (int x : new int[]{0,1}) {
      // write the file
      Path f = new Path(ROOT_DIR, getName());
      StoreFile.Writer writer = new StoreFile.Writer(fs, f,
          StoreFile.DEFAULT_BLOCKSIZE_SMALL,
          HFile.DEFAULT_COMPRESSION_ALGORITHM,
          conf, KeyValue.COMPARATOR, bt[x], expKeys[x], false);

      long now = System.currentTimeMillis();
      for (int i = 0; i < rowCount*2; i += 2) { // rows
        for (int j = 0; j < colCount*2; j += 2) {   // column qualifiers
          String row = String.format(localFormatter, i);
          String col = String.format(localFormatter, j);
          for (int k= 0; k < versions; ++k) { // versions
            KeyValue kv = new KeyValue(row.getBytes(),
              "family".getBytes(), ("col" + col).getBytes(),
                now-k, Bytes.toBytes((long)-1));
            writer.append(kv);
          }
        }
      }
      writer.close();

      StoreFile.Reader reader = new StoreFile.Reader(fs, f, null, false, false);
      reader.loadFileInfo();
      reader.loadBloomfilter();
      StoreFileScanner scanner = reader.getStoreFileScanner(false, false);
      assertEquals(expKeys[x], reader.bloomFilter.getKeyCount());

      // check false positives rate
      int falsePos = 0;
      int falseNeg = 0;
      for (int i = 0; i < rowCount*2; ++i) { // rows
        for (int j = 0; j < colCount*2; ++j) {   // column qualifiers
          String row = String.format(localFormatter, i);
          String col = String.format(localFormatter, j);
          TreeSet<byte[]> columns = new TreeSet<byte[]>();
          columns.add(("col" + col).getBytes());

          Scan scan = new Scan(row.getBytes(),row.getBytes());
          scan.addColumn("family".getBytes(), ("col"+col).getBytes());
          boolean exists = scanner.shouldSeek(scan, columns);
          boolean shouldRowExist = i % 2 == 0;
          boolean shouldColExist = j % 2 == 0;
          shouldColExist = shouldColExist || bt[x] == StoreFile.BloomType.ROW;
          if (shouldRowExist && shouldColExist) {
            if (!exists) falseNeg++;
          } else {
            if (exists) falsePos++;
          }
        }
      }
      reader.close();
      fs.delete(f, true);
      System.out.println(bt[x].toString());
      System.out.println("  False negatives: " + falseNeg);
      System.out.println("  False positives: " + falsePos);
      assertEquals(0, falseNeg);
      assertTrue(falsePos < 2*expErr[x]);
    }
  }

  public void testBloomEdgeCases() throws Exception {
    float err = (float)0.005;
    FileSystem fs = FileSystem.getLocal(conf);
    Path f = new Path(ROOT_DIR, getName());
    conf.setFloat(StoreFile.IO_STOREFILE_BLOOM_ERROR_RATE, err);
    conf.setBoolean(StoreFile.IO_STOREFILE_BLOOM_ENABLED, true);
    conf.setInt(StoreFile.IO_STOREFILE_BLOOM_MAX_KEYS, 1000);

    // this should not create a bloom because the max keys is too small
    StoreFile.Writer writer = new StoreFile.Writer(fs, f,
        StoreFile.DEFAULT_BLOCKSIZE_SMALL, HFile.DEFAULT_COMPRESSION_ALGORITHM,
        conf, KeyValue.COMPARATOR, StoreFile.BloomType.ROW, 2000, false);
    assertFalse(writer.hasBloom());
    writer.close();
    fs.delete(f, true);

    conf.setInt(StoreFile.IO_STOREFILE_BLOOM_MAX_KEYS, Integer.MAX_VALUE);

    // TODO: commented out because we run out of java heap space on trunk
    /*
    // the below config caused IllegalArgumentException in our production cluster
    // however, the resulting byteSize is < MAX_INT, so this should work properly
    writer = new StoreFile.Writer(fs, f,
        StoreFile.DEFAULT_BLOCKSIZE_SMALL, HFile.DEFAULT_COMPRESSION_ALGORITHM,
        conf, KeyValue.COMPARATOR, StoreFile.BloomType.ROW, 272446963);
    assertTrue(writer.hasBloom());
    bloomWriteRead(writer, fs);
    */

    // this, however, is too large and should not create a bloom
    // because Java can't create a contiguous array > MAX_INT
    writer = new StoreFile.Writer(fs, f,
        StoreFile.DEFAULT_BLOCKSIZE_SMALL, HFile.DEFAULT_COMPRESSION_ALGORITHM,
        conf, KeyValue.COMPARATOR, StoreFile.BloomType.ROW, Integer.MAX_VALUE,
        false);
    assertFalse(writer.hasBloom());
    writer.close();
    fs.delete(f, true);
  }

  public void testFlushTimeComparator() {
    assertOrdering(StoreFile.Comparators.FLUSH_TIME,
        mockStoreFile(true, 1000, -1, "/foo/123"),
        mockStoreFile(true, 1000, -1, "/foo/126"),
        mockStoreFile(true, 2000, -1, "/foo/126"),
        mockStoreFile(false, -1, 1, "/foo/1"),
        mockStoreFile(false, -1, 3, "/foo/2"),
        mockStoreFile(false, -1, 5, "/foo/2"),
        mockStoreFile(false, -1, 5, "/foo/3"));
  }

  /**
   * Assert that the given comparator orders the given storefiles in the
   * same way that they're passed.
   */
  private void assertOrdering(Comparator<StoreFile> comparator, StoreFile ... sfs) {
    ArrayList<StoreFile> sorted = Lists.newArrayList(sfs);
    Collections.shuffle(sorted);
    Collections.sort(sorted, comparator);
    LOG.debug("sfs: " + Joiner.on(",").join(sfs));
    LOG.debug("sorted: " + Joiner.on(",").join(sorted));
    assertTrue(Iterables.elementsEqual(Arrays.asList(sfs), sorted));
  }

  /**
   * Create a mock StoreFile with the given attributes.
   */
  private StoreFile mockStoreFile(boolean bulkLoad, long bulkTimestamp,
      long seqId, String path) {
    StoreFile mock = Mockito.mock(StoreFile.class);
    Mockito.doReturn(bulkLoad).when(mock).isBulkLoadResult();
    Mockito.doReturn(bulkTimestamp).when(mock).getBulkLoadTimestamp();
    if (bulkLoad) {
      // Bulk load files will throw if you ask for their sequence ID
      Mockito.doThrow(new IllegalAccessError("bulk load"))
        .when(mock).getMaxSequenceId();
    } else {
      Mockito.doReturn(seqId).when(mock).getMaxSequenceId();
    }
    Mockito.doReturn(new Path(path)).when(mock).getPath();
    String name = "mock storefile, bulkLoad=" + bulkLoad +
      " bulkTimestamp=" + bulkTimestamp +
      " seqId=" + seqId +
      " path=" + path;
    Mockito.doReturn(name).when(mock).toString();
    return mock;
  }

  /**
   *Generate a list of KeyValues for testing based on given parameters
   * @param timestamps
   * @param numRows
   * @param qualifier
   * @param family
   * @return
   */
  List<KeyValue> getKeyValueSet(long[] timestamps, int numRows,
      byte[] qualifier, byte[] family) {
    List<KeyValue> kvList = new ArrayList<KeyValue>();
    for (int i=1;i<=numRows;i++) {
      byte[] b = Bytes.toBytes(i) ;
      LOG.info(Bytes.toString(b));
      LOG.info(Bytes.toString(b));
      for (long timestamp: timestamps)
      {
        kvList.add(new KeyValue(b, family, qualifier, timestamp, b));
      }
    }
    return kvList;
  }

  /**
   * Test to ensure correctness when using StoreFile with multiple timestamps
   * @throws IOException
   */
  public void testMultipleTimestamps() throws IOException {
    byte[] family = Bytes.toBytes("familyname");
    byte[] qualifier = Bytes.toBytes("qualifier");
    int numRows = 10;
    long[] timestamps = new long[] {20,10,5,1};
    Scan scan = new Scan();

    Path storedir = new Path(new Path(this.testDir, "regionname"),
    "familyname");
    Path dir = new Path(storedir, "1234567890");
    StoreFile.Writer writer = StoreFile.createWriter(this.fs, dir, 8 * 1024);

    List<KeyValue> kvList = getKeyValueSet(timestamps,numRows,
        family, qualifier);

    for (KeyValue kv : kvList) {
      writer.append(kv);
    }
    writer.appendMetadata(0, false);
    writer.close();

    StoreFile hsf = new StoreFile(this.fs, writer.getPath(), true, conf,
        StoreFile.BloomType.NONE, false);
    StoreFile.Reader reader = hsf.createReader();
    StoreFileScanner scanner = reader.getStoreFileScanner(false, false);
    TreeSet<byte[]> columns = new TreeSet<byte[]>();
    columns.add(qualifier);

    scan.setTimeRange(20, 100);
    assertTrue(scanner.shouldSeek(scan, columns));

    scan.setTimeRange(1, 2);
    assertTrue(scanner.shouldSeek(scan, columns));

    scan.setTimeRange(8, 10);
    assertTrue(scanner.shouldSeek(scan, columns));

    scan.setTimeRange(7, 50);
    assertTrue(scanner.shouldSeek(scan, columns));

    /*This test is not required for correctness but it should pass when
     * timestamp range optimization is on*/
    //scan.setTimeRange(27, 50);
    //assertTrue(!scanner.shouldSeek(scan, columns));
  }

  public void testCacheOnWriteEvictOnClose() throws Exception {
    Configuration conf = this.conf;
    conf.setBoolean("hbase.rs.evictblocksonclose", false);

    // Find a home for our files
    Path baseDir = new Path(new Path(this.testDir, "regionname"),
    "twoCOWEOC");

    // Grab the block cache and get the initial hit/miss counts
    BlockCache bc = StoreFile.getBlockCache(conf);
    assertNotNull(bc);
    CacheStats cs = bc.getStats();
    long startHit = cs.getHitCount();
    long startMiss = cs.getMissCount();
    long startEvicted = cs.getEvictedCount();

    // Let's write a StoreFile with three blocks, with cache on write off
    conf.setBoolean("hbase.rs.cacheblocksonwrite", false);
    Path pathCowOff = new Path(baseDir, "123456789");
    StoreFile.Writer writer = writeStoreFile(conf, pathCowOff, 3);
    StoreFile hsf = new StoreFile(this.fs, writer.getPath(), true, conf,
        StoreFile.BloomType.NONE, false);
    LOG.debug(hsf.getPath().toString());

    // Read this file, we should see 3 misses
    StoreFile.Reader reader = hsf.createReader();
    reader.loadFileInfo();
    StoreFileScanner scanner = reader.getStoreFileScanner(true, true);
    scanner.seek(KeyValue.LOWESTKEY);
    while (scanner.next() != null);
    assertEquals(startHit, cs.getHitCount());
    assertEquals(startMiss + 3, cs.getMissCount());
    assertEquals(startEvicted, cs.getEvictedCount());
    startMiss += 3;
    scanner.close();
    reader.close();

    // Now write a StoreFile with three blocks, with cache on write on
    conf.setBoolean("hbase.rs.cacheblocksonwrite", true);
    Path pathCowOn = new Path(baseDir, "123456788");
    writer = writeStoreFile(conf, pathCowOn, 3);
    hsf = new StoreFile(this.fs, writer.getPath(), true, conf,
        StoreFile.BloomType.NONE, false);

    // Read this file, we should see 3 hits
    reader = hsf.createReader();
    scanner = reader.getStoreFileScanner(true, true);
    scanner.seek(KeyValue.LOWESTKEY);
    while (scanner.next() != null);
    assertEquals(startHit + 3, cs.getHitCount());
    assertEquals(startMiss, cs.getMissCount());
    assertEquals(startEvicted, cs.getEvictedCount());
    startHit += 3;
    scanner.close();
    reader.close();

    // Let's read back the two files to ensure the blocks exactly match
    hsf = new StoreFile(this.fs, pathCowOff, true, conf,
        StoreFile.BloomType.NONE, false);
    StoreFile.Reader readerOne = hsf.createReader();
    readerOne.loadFileInfo();
    StoreFileScanner scannerOne = readerOne.getStoreFileScanner(true, true);
    scannerOne.seek(KeyValue.LOWESTKEY);
    hsf = new StoreFile(this.fs, pathCowOn, true, conf,
        StoreFile.BloomType.NONE, false);
    StoreFile.Reader readerTwo = hsf.createReader();
    readerTwo.loadFileInfo();
    StoreFileScanner scannerTwo = readerTwo.getStoreFileScanner(true, true);
    scannerTwo.seek(KeyValue.LOWESTKEY);
    KeyValue kv1 = null;
    KeyValue kv2 = null;
    while ((kv1 = scannerOne.next()) != null) {
      kv2 = scannerTwo.next();
      assertTrue(kv1.equals(kv2));
    }
    assertNull(scannerTwo.next());
    assertEquals(startHit + 6, cs.getHitCount());
    assertEquals(startMiss, cs.getMissCount());
    assertEquals(startEvicted, cs.getEvictedCount());
    startHit += 6;
    scannerOne.close();
    readerOne.close();
    scannerTwo.close();
    readerTwo.close();

    // Let's close the first file with evict on close turned on
    conf.setBoolean("hbase.rs.evictblocksonclose", true);
    hsf = new StoreFile(this.fs, pathCowOff, true, conf,
        StoreFile.BloomType.NONE, false);
    reader = hsf.createReader();
    reader.close();

    // We should have 3 new evictions
    assertEquals(startHit, cs.getHitCount());
    assertEquals(startMiss, cs.getMissCount());
    assertEquals(startEvicted + 3, cs.getEvictedCount());
    startEvicted += 3;

    // Let's close the second file with evict on close turned off
    conf.setBoolean("hbase.rs.evictblocksonclose", false);
    hsf = new StoreFile(this.fs, pathCowOn, true, conf,
        StoreFile.BloomType.NONE, false);
    reader = hsf.createReader();
    reader.close();

    // We expect no changes
    assertEquals(startHit, cs.getHitCount());
    assertEquals(startMiss, cs.getMissCount());
    assertEquals(startEvicted, cs.getEvictedCount());
  }

  private StoreFile.Writer writeStoreFile(Configuration conf, Path path,
      int numBlocks)
  throws IOException {
    // Let's put ~5 small KVs in each block, so let's make 5*numBlocks KVs
    int numKVs = 5 * numBlocks;
    List<KeyValue> kvs = new ArrayList<KeyValue>(numKVs);
    byte [] b = Bytes.toBytes("x");
    int totalSize = 0;
    for (int i=numKVs;i>0;i--) {
      KeyValue kv = new KeyValue(b, b, b, i, b);
      kvs.add(kv);
      totalSize += kv.getLength();
    }
    int blockSize = totalSize / numBlocks;
    StoreFile.Writer writer = new StoreFile.Writer(fs, path, blockSize,
        HFile.DEFAULT_COMPRESSION_ALGORITHM,
        conf, KeyValue.COMPARATOR, StoreFile.BloomType.NONE, 2000,
        conf.getBoolean("hbase.rs.cacheblocksonwrite", false));
    // We'll write N-1 KVs to ensure we don't write an extra block
    kvs.remove(kvs.size()-1);
    for (KeyValue kv : kvs) {
      writer.append(kv);
    }
    writer.appendMetadata(0, false);
    writer.close();
    return writer;
  }
}
