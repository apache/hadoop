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
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;

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
    HFile.Writer writer = StoreFile.createWriter(this.fs,
      new Path(new Path(this.testDir, "regionname"), "familyname"), 2 * 1024);
    writeStoreFile(writer);
    checkHalfHFile(new StoreFile(this.fs, writer.getPath(), true, conf, 
        StoreFile.BloomType.NONE, false));
  }

  private void writeStoreFile(final HFile.Writer writer) throws IOException {
    writeStoreFile(writer, Bytes.toBytes(getName()), Bytes.toBytes(getName()));
  }
  /*
   * Writes HStoreKey and ImmutableBytes data to passed writer and
   * then closes it.
   * @param writer
   * @throws IOException
   */
  public static void writeStoreFile(final HFile.Writer writer, byte[] fam, byte[] qualifier)
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
    HFile.Writer writer = StoreFile.createWriter(this.fs, dir, 8 * 1024);
    writeStoreFile(writer);
    StoreFile hsf = new StoreFile(this.fs, writer.getPath(), true, conf, 
        StoreFile.BloomType.NONE, false);
    HFile.Reader reader = hsf.createReader();
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
    Path topDir = Store.getStoreHomedir(this.testDir, 1,
      Bytes.toBytes(f.getPath().getParent().getName()));
    if (this.fs.exists(topDir)) {
      this.fs.delete(topDir, true);
    }
    Path topPath = StoreFile.split(this.fs, topDir, f, midRow, Range.top);
    // Create bottom split.
    Path bottomDir = Store.getStoreHomedir(this.testDir, 2,
      Bytes.toBytes(f.getPath().getParent().getName()));
    if (this.fs.exists(bottomDir)) {
      this.fs.delete(bottomDir, true);
    }
    Path bottomPath = StoreFile.split(this.fs, bottomDir,
      f, midRow, Range.bottom);
    // Make readers on top and bottom.
    HFile.Reader top = new StoreFile(this.fs, topPath, true, conf, 
        StoreFile.BloomType.NONE, false).createReader();
    HFile.Reader bottom = new StoreFile(this.fs, bottomPath, true, conf, 
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
  
  public void testBloomFilter() throws Exception {
    FileSystem fs = FileSystem.getLocal(conf);
    conf.setFloat("io.hfile.bloom.error.rate", (float)0.01);
    conf.setBoolean("io.hfile.bloom.enabled", true);
    
    // write the file
    Path f = new Path(ROOT_DIR, getName());
    StoreFile.Writer writer = new StoreFile.Writer(fs, f, 
        StoreFile.DEFAULT_BLOCKSIZE_SMALL, HFile.DEFAULT_COMPRESSION_ALGORITHM, 
        conf, KeyValue.COMPARATOR, StoreFile.BloomType.ROW, 2000);

    long now = System.currentTimeMillis();
    for (int i = 0; i < 2000; i += 2) {
      String row = String.format(localFormatter, Integer.valueOf(i));
      KeyValue kv = new KeyValue(row.getBytes(), "family".getBytes(),
        "col".getBytes(), now, "value".getBytes());
      writer.append(kv);
    }
    writer.close();
    
    StoreFile.Reader reader = new StoreFile.Reader(fs, f, null, false);
    reader.loadFileInfo();
    reader.loadBloomfilter();
    HFileScanner scanner = reader.getScanner(false, false);

    // check false positives rate
    int falsePos = 0;
    int falseNeg = 0;
    for (int i = 0; i < 2000; i++) {
      String row = String.format(localFormatter, Integer.valueOf(i));
      TreeSet<byte[]> columns = new TreeSet<byte[]>();
      columns.add("family:col".getBytes());
      
      boolean exists = scanner.shouldSeek(row.getBytes(), columns);
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
    assertTrue(falsePos < 2);
  }
  
  public void testBloomTypes() throws Exception {
    float err = (float) 0.01;
    FileSystem fs = FileSystem.getLocal(conf);
    conf.setFloat("io.hfile.bloom.error.rate", err);
    conf.setBoolean("io.hfile.bloom.enabled", true);
    
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
          conf, KeyValue.COMPARATOR, bt[x], expKeys[x]);
  
      long now = System.currentTimeMillis();
      for (int i = 0; i < rowCount*2; i += 2) { // rows
        for (int j = 0; j < colCount*2; j += 2) {   // column qualifiers
          String row = String.format(localFormatter, Integer.valueOf(i));
          String col = String.format(localFormatter, Integer.valueOf(j)); 
          for (int k= 0; k < versions; ++k) { // versions 
            KeyValue kv = new KeyValue(row.getBytes(), 
              "family".getBytes(), ("col" + col).getBytes(), 
                now-k, Bytes.toBytes((long)-1));
            writer.append(kv);
          }
        }
      }
      writer.close();

      StoreFile.Reader reader = new StoreFile.Reader(fs, f, null, false);
      reader.loadFileInfo();
      reader.loadBloomfilter();
      HFileScanner scanner = reader.getScanner(false, false);
      assertEquals(expKeys[x], reader.getBloomFilter().getKeyCount());
      
      // check false positives rate
      int falsePos = 0;
      int falseNeg = 0;
      for (int i = 0; i < rowCount*2; ++i) { // rows
        for (int j = 0; j < colCount*2; ++j) {   // column qualifiers
          String row = String.format(localFormatter, Integer.valueOf(i));
          String col = String.format(localFormatter, Integer.valueOf(j)); 
          TreeSet<byte[]> columns = new TreeSet<byte[]>();
          columns.add(("col" + col).getBytes());

          boolean exists = scanner.shouldSeek(row.getBytes(), columns);
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
  
}
