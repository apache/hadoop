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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
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
    HFile.Writer writer = StoreFile.getWriter(this.fs,
      new Path(new Path(this.testDir, "regionname"), "familyname"),
      2 * 1024, null, null, false);
    writeStoreFile(writer);
    checkHalfHFile(new StoreFile(this.fs, writer.getPath()));
  }

  /*
   * Writes HStoreKey and ImmutableBytes data to passed writer and
   * then closes it.
   * @param writer
   * @throws IOException
   */
  private void writeStoreFile(final HFile.Writer writer)
  throws IOException {
    try {
      for (char d = FIRST_CHAR; d <= LAST_CHAR; d++) {
        for (char e = FIRST_CHAR; e <= LAST_CHAR; e++) {
          byte[] b = new byte[] { (byte) d, (byte) e };
          byte [] t = Bytes.toBytes(new String(b, HConstants.UTF8_ENCODING));
          HStoreKey hsk = new HStoreKey(t, t, System.currentTimeMillis());
          writer.append(hsk.getBytes(), t);
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
    HFile.Writer writer = StoreFile.getWriter(this.fs, dir, 8 * 1024, null,
      null, false);
    writeStoreFile(writer);
    StoreFile hsf = new StoreFile(this.fs, writer.getPath());
    HFile.Reader reader = hsf.getReader();
    // Split on a row, not in middle of row.  Midkey returned by reader
    // may be in middle of row.  Create new one with empty column and
    // timestamp.
    HStoreKey hsk = HStoreKey.create(reader.midkey());
    byte [] midkey = hsk.getRow();
    hsk = HStoreKey.create(reader.getLastKey());
    byte [] finalKey = hsk.getRow();
    // Make a reference
    Path refPath = StoreFile.split(fs, dir, hsf, reader.midkey(), Range.top);
    StoreFile refHsf = new StoreFile(this.fs, refPath);
    // Now confirm that I can read from the reference and that it only gets
    // keys from top half of the file.
    HFileScanner s = refHsf.getReader().getScanner();
    for(boolean first = true; (!s.isSeeked() && s.seekTo()) || s.next();) {
      ByteBuffer bb = s.getKey();
      hsk = HStoreKey.create(bb.array(), bb.arrayOffset(), bb.limit());
      if (first) {
        assertTrue(Bytes.equals(hsk.getRow(), midkey));
        first = false;
      }
    }
    assertTrue(Bytes.equals(hsk.getRow(), finalKey));
  }

  private void checkHalfHFile(final StoreFile f)
  throws IOException {
    byte [] midkey = f.getReader().midkey();
    // Create top split.
    Path topDir = Store.getStoreHomedir(this.testDir, 1,
      Bytes.toBytes(f.getPath().getParent().getName()));
    if (this.fs.exists(topDir)) {
      this.fs.delete(topDir, true);
    }
    Path topPath = StoreFile.split(this.fs, topDir, f, midkey, Range.top);
    // Create bottom split.
    Path bottomDir = Store.getStoreHomedir(this.testDir, 2,
      Bytes.toBytes(f.getPath().getParent().getName()));
    if (this.fs.exists(bottomDir)) {
      this.fs.delete(bottomDir, true);
    }
    Path bottomPath = StoreFile.split(this.fs, bottomDir,
      f, midkey, Range.bottom);
    // Make readers on top and bottom.
    HFile.Reader top = new StoreFile(this.fs, topPath).getReader();
    HFile.Reader bottom = new StoreFile(this.fs, bottomPath).getReader();
    ByteBuffer previous = null;
    LOG.info("Midkey: " + Bytes.toString(midkey));
    byte [] midkeyBytes = new HStoreKey(midkey).getBytes();
    ByteBuffer bbMidkeyBytes = ByteBuffer.wrap(midkeyBytes);
    try {
      // Now make two HalfMapFiles and assert they can read the full backing
      // file, one from the top and the other from the bottom.
      // Test bottom half first.
      // Now test reading from the top.
      boolean first = true;
      ByteBuffer key = null;
      HFileScanner topScanner = top.getScanner();
      while ((!topScanner.isSeeked() && topScanner.seekTo()) ||
          (topScanner.isSeeked() && topScanner.next())) {
        key = topScanner.getKey();
        
        assertTrue(topScanner.getReader().getComparator().compare(key.array(),
          key.arrayOffset(), key.limit(), midkeyBytes, 0, midkeyBytes.length) >= 0);
        if (first) {
          first = false;
          LOG.info("First in top: " + Bytes.toString(Bytes.toBytes(key)));
        }
      }
      LOG.info("Last in top: " + Bytes.toString(Bytes.toBytes(key)));
      
      first = true;
      HFileScanner bottomScanner = bottom.getScanner();
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
      top = new StoreFile(this.fs, topPath).getReader();
      bottom = new StoreFile(this.fs, bottomPath).getReader();
      bottomScanner = bottom.getScanner();
      int count = 0;
      while ((!bottomScanner.isSeeked() && bottomScanner.seekTo()) ||
          bottomScanner.next()) {
        count++;
      }
      // When badkey is < than the bottom, should return no values.
      assertTrue(count == 0);
      // Now read from the top.
      first = true;
      topScanner = top.getScanner();
      while ((!topScanner.isSeeked() && topScanner.seekTo()) ||
          topScanner.next()) {
        key = topScanner.getKey();
        assertTrue(topScanner.getReader().getComparator().compare(key.array(),
          key.arrayOffset(), key.limit(), badmidkey, 0, badmidkey.length) >= 0);
        if (first) {
          first = false;
          first = false;
          HStoreKey keyhsk = HStoreKey.create(key);
          LOG.info("First top when key < bottom: " + keyhsk);
          String tmp = Bytes.toString(keyhsk.getRow());
          for (int i = 0; i < tmp.length(); i++) {
            assertTrue(tmp.charAt(i) == 'a');
          }
        }
      }
      HStoreKey keyhsk = HStoreKey.create(key);
      LOG.info("Last top when key < bottom: " + keyhsk);
      String tmp = Bytes.toString(keyhsk.getRow());
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
      top = new StoreFile(this.fs, topPath).getReader();
      bottom = new StoreFile(this.fs, bottomPath).getReader();
      first = true;
      bottomScanner = bottom.getScanner();
      while ((!bottomScanner.isSeeked() && bottomScanner.seekTo()) ||
          bottomScanner.next()) {
        key = bottomScanner.getKey();
        if (first) {
          first = false;
          keyhsk = HStoreKey.create(key);
          LOG.info("First bottom when key > top: " + keyhsk);
          tmp = Bytes.toString(keyhsk.getRow());
          for (int i = 0; i < tmp.length(); i++) {
            assertTrue(tmp.charAt(i) == 'a');
          }
        }
      }
      keyhsk = HStoreKey.create(key);
      LOG.info("Last bottom when key > top: " + keyhsk);
      for (int i = 0; i < tmp.length(); i++) {
        assertTrue(Bytes.toString(keyhsk.getRow()).charAt(i) == 'z');
      }
      count = 0;
      topScanner = top.getScanner();
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
}