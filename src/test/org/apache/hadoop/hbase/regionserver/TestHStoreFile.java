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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.HalfMapFileReader;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.MapFile;
import org.apache.hadoop.hbase.io.SequenceFile;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.WritableComparable;
/**
 * Test HStoreFile
 */
public class TestHStoreFile extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestHStoreFile.class);
  private static String DIR = "/";
  private MiniDFSCluster cluster;
  private Path dir = null;
  
  @Override
  public void setUp() throws Exception {
    try {
      this.cluster = new MiniDFSCluster(this.conf, 2, true, (String[])null);
      // Set the hbase.rootdir to be the home directory in mini dfs.
      this.conf.set(HConstants.HBASE_DIR,
        this.cluster.getFileSystem().getHomeDirectory().toString());
      this.dir = new Path(DIR, getName());
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
  
  private Path writeMapFile(final String name)
  throws IOException {
    Path path = new Path(DIR, name);
    MapFile.Writer writer = new MapFile.Writer(this.conf, fs, path.toString(),
      HStoreKey.class, ImmutableBytesWritable.class);
    writeStoreFile(writer);
    return path;
  }
  
  private Path writeSmallMapFile(final String name)
  throws IOException {
    Path path = new Path(DIR, name);
    MapFile.Writer writer = new MapFile.Writer(this.conf, fs, path.toString(),
      HStoreKey.class, ImmutableBytesWritable.class);
    try {
      for (char d = FIRST_CHAR; d <= LAST_CHAR; d++) {
        byte[] b = new byte[] {(byte)d};
        byte [] t = Bytes.toBytes(new String(b, HConstants.UTF8_ENCODING));
        writer.append(new HStoreKey(t, t, System.currentTimeMillis()),
            new ImmutableBytesWritable(t));
      }
    } finally {
      writer.close();
    }
    return path;
  }
  
  /*
   * Writes HStoreKey and ImmutableBytes data to passed writer and
   * then closes it.
   * @param writer
   * @throws IOException
   */
  private void writeStoreFile(final MapFile.Writer writer)
  throws IOException {
    try {
      for (char d = FIRST_CHAR; d <= LAST_CHAR; d++) {
        for (char e = FIRST_CHAR; e <= LAST_CHAR; e++) {
          byte[] b = new byte[] { (byte) d, (byte) e };
          byte [] t = Bytes.toBytes(new String(b, HConstants.UTF8_ENCODING));
          writer.append(new HStoreKey(t, t, System.currentTimeMillis()),
            new ImmutableBytesWritable(t));
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
    // Make a store file and write data to it.
    HStoreFile hsf = new HStoreFile(this.conf, this.fs, this.dir,
      HRegionInfo.FIRST_META_REGIONINFO,
      Bytes.toBytes("colfamily"), 1234567890L, null);
    MapFile.Writer writer =
      hsf.getWriter(this.fs, SequenceFile.CompressionType.NONE, false, 0);
    writeStoreFile(writer);
    MapFile.Reader reader = hsf.getReader(this.fs, false, false);
    // Split on a row, not in middle of row.  Midkey returned by reader
    // may be in middle of row.  Create new one with empty column and
    // timestamp.
    HStoreKey midkey = new HStoreKey(((HStoreKey)reader.midKey()).getRow());
    HStoreKey hsk = new HStoreKey();
    reader.finalKey(hsk);
    byte [] finalKey = hsk.getRow();
    // Make a reference for the bottom half of the just written file.
    Reference reference =
      new Reference(hsf.getEncodedRegionName(), hsf.getFileId(),
          midkey, Reference.Range.top);
    HStoreFile refHsf = new HStoreFile(this.conf, this.fs, 
        new Path(DIR, getName()),
        HRegionInfo.FIRST_META_REGIONINFO,
        hsf.getColFamily(), 456, reference);
    // Assert that reference files are written and that we can write and
    // read the info reference file at least.
    refHsf.writeReferenceFiles(this.fs);
    assertTrue(this.fs.exists(refHsf.getMapFilePath()));
    assertTrue(this.fs.exists(refHsf.getInfoFilePath()));
    Reference otherReference =
      HStoreFile.readSplitInfo(refHsf.getInfoFilePath(), this.fs);
    assertEquals(reference.getEncodedRegionName(),
        otherReference.getEncodedRegionName());
    assertEquals(reference.getFileId(),
        otherReference.getFileId());
    assertEquals(reference.getMidkey().toString(),
        otherReference.getMidkey().toString());
    // Now confirm that I can read from the reference and that it only gets
    // keys from top half of the file.
    MapFile.Reader halfReader = refHsf.getReader(this.fs, false, false);
    HStoreKey key = new HStoreKey();
    ImmutableBytesWritable value = new ImmutableBytesWritable();
    boolean first = true;
    while(halfReader.next(key, value)) {
      if (first) {
        assertTrue(Bytes.equals(key.getRow(), midkey.getRow()));
        first = false;
      }
    }
    assertTrue(Bytes.equals(key.getRow(), finalKey));
  }

  /**
   * Write a file and then assert that we can read from top and bottom halves
   * using two HalfMapFiles.
   * @throws Exception
   */
  public void testBasicHalfMapFile() throws Exception {
    Path p = writeMapFile(getName());
    WritableComparable midkey = getMidkey(p);
    checkHalfMapFile(p, midkey);
  }
  
  /**
   * Check HalfMapFile works even if file we're to go against is smaller than
   * the default MapFile interval of 128: i.e. index gets entry every 128 
   * keys.
   * @throws Exception
   */
  public void testSmallHalfMapFile() throws Exception {
    Path p = writeSmallMapFile(getName());
    // I know keys are a-z.  Let the midkey we want to use be 'd'.  See if
    // HalfMapFiles work even if size of file is < than default MapFile
    // interval.
    checkHalfMapFile(p, new HStoreKey("d"));
  }
  
  private WritableComparable getMidkey(final Path p) throws IOException {
    MapFile.Reader reader =
      new MapFile.Reader(this.fs, p.toString(), this.conf);
    HStoreKey key = new HStoreKey();
    ImmutableBytesWritable value = new ImmutableBytesWritable();
    reader.next(key, value);
    String firstKey = key.toString();
    WritableComparable midkey = reader.midKey();
    reader.finalKey(key);
    LOG.info("First key " + firstKey + ", midkey " + midkey.toString()
        + ", last key " + key.toString());
    reader.close();
    return midkey;
  }
  
  private void checkHalfMapFile(final Path p, WritableComparable midkey)
  throws IOException {
    MapFile.Reader top = null;
    MapFile.Reader bottom = null;
    HStoreKey key = new HStoreKey();
    ImmutableBytesWritable value = new ImmutableBytesWritable();
    String previous = null;
    try {
      // Now make two HalfMapFiles and assert they can read the full backing
      // file, one from the top and the other from the bottom.
      // Test bottom half first.
      bottom = new HalfMapFileReader(this.fs, p.toString(),
          this.conf, Reference.Range.bottom, midkey, null);
      boolean first = true;
      while (bottom.next(key, value)) {
        previous = key.toString();
        if (first) {
          first = false;
          LOG.info("First in bottom: " + previous);
        }
        assertTrue(key.compareTo((HStoreKey)midkey) < 0);
      }
      if (previous != null) {
        LOG.info("Last in bottom: " + previous.toString());
      }
      // Now test reading from the top.
      top = new HalfMapFileReader(this.fs, p.toString(), this.conf,
          Reference.Range.top, midkey, null);
      first = true;
      while (top.next(key, value)) {
        assertTrue(key.compareTo((HStoreKey)midkey) >= 0);
        if (first) {
          first = false;
          LOG.info("First in top: " + key.toString());
        }
      }
      LOG.info("Last in top: " + key.toString());

      // Next test using a midkey that does not exist in the file.
      // First, do a key that is < than first key. Ensure splits behave
      // properly.
      WritableComparable badkey = new HStoreKey("   ");
      bottom = new HalfMapFileReader(this.fs, p.toString(),
          this.conf, Reference.Range.bottom, badkey, null);
      // When badkey is < than the bottom, should return no values.
      assertFalse(bottom.next(key, value));
      // Now read from the top.
      top = new HalfMapFileReader(this.fs, p.toString(), this.conf,
          Reference.Range.top, badkey, null);
      first = true;
      while (top.next(key, value)) {
        assertTrue(key.compareTo((HStoreKey)badkey) >= 0);
        if (first) {
          first = false;
          LOG.info("First top when key < bottom: " + key.toString());
          String tmp = Bytes.toString(key.getRow());
          for (int i = 0; i < tmp.length(); i++) {
            assertTrue(tmp.charAt(i) == 'a');
          }
        }
      }
      LOG.info("Last top when key < bottom: " + key.toString());
      String tmp = Bytes.toString(key.getRow());
      for (int i = 0; i < tmp.length(); i++) {
        assertTrue(tmp.charAt(i) == 'z');
      }

      // Test when badkey is > than last key in file ('||' > 'zz').
      badkey = new HStoreKey("|||");
      bottom = new HalfMapFileReader(this.fs, p.toString(),
          this.conf, Reference.Range.bottom, badkey, null);
      first = true;
      while (bottom.next(key, value)) {
        if (first) {
          first = false;
          LOG.info("First bottom when key > top: " + key.toString());
          tmp = Bytes.toString(key.getRow());
          for (int i = 0; i < tmp.length(); i++) {
            assertTrue(tmp.charAt(i) == 'a');
          }
        }
      }
      LOG.info("Last bottom when key > top: " + key.toString());
      tmp = Bytes.toString(key.getRow());
      for (int i = 0; i < tmp.length(); i++) {
        assertTrue(tmp.charAt(i) == 'z');
      }
      // Now look at top. Should not return any values.
      top = new HalfMapFileReader(this.fs, p.toString(), this.conf,
          Reference.Range.top, badkey, null);
      assertFalse(top.next(key, value));
      
    } finally {
      if (top != null) {
        top.close();
      }
      if (bottom != null) {
        bottom.close();
      }
      fs.delete(p, true);
    }
  }
  
  /**
   * Assert HalFMapFile does right thing when midkey does not exist in the
   * backing file (its larger or smaller than any of the backing mapfiles keys).
   * 
   * @throws Exception
   */
  public void testOutOfRangeMidkeyHalfMapFile() throws Exception {
    MapFile.Reader top = null;
    MapFile.Reader bottom = null;
    HStoreKey key = new HStoreKey();
    ImmutableBytesWritable value = new ImmutableBytesWritable();
    Path p = writeMapFile(getName());
    try {
      try {
        // Test using a midkey that does not exist in the file.
        // First, do a key that is < than first key.  Ensure splits behave
        // properly.
        HStoreKey midkey = new HStoreKey("   ");
        bottom = new HalfMapFileReader(this.fs, p.toString(),
          this.conf, Reference.Range.bottom, midkey, null);
        // When midkey is < than the bottom, should return no values.
        assertFalse(bottom.next(key, value));
        // Now read from the top.
        top = new HalfMapFileReader(this.fs, p.toString(),
          this.conf, Reference.Range.top, midkey, null);
        boolean first = true;
        while (top.next(key, value)) {
          assertTrue(key.compareTo(midkey) >= 0);
          if (first) {
            first = false;
            LOG.info("First top when key < bottom: " + key.toString());
            assertEquals("aa", Bytes.toString(key.getRow()));
          }
        }
        LOG.info("Last top when key < bottom: " + key.toString());
        assertEquals("zz", Bytes.toString(key.getRow()));
        
        // Test when midkey is > than last key in file ('||' > 'zz').
        midkey = new HStoreKey("|||");
        bottom = new HalfMapFileReader(this.fs, p.toString(),
          this.conf, Reference.Range.bottom, midkey, null);
        first = true;
        while (bottom.next(key, value)) {
          if (first) {
            first = false;
            LOG.info("First bottom when key > top: " + key.toString());
            assertEquals("aa", Bytes.toString(key.getRow()));
          }
        }
        LOG.info("Last bottom when key > top: " + key.toString());
        assertEquals("zz", Bytes.toString(key.getRow()));
        // Now look at top.  Should not return any values.
        top = new HalfMapFileReader(this.fs, p.toString(),
          this.conf, Reference.Range.top, midkey, null);
        assertFalse(top.next(key, value));
      } finally {
        if (top != null) {
          top.close();
        }
        if (bottom != null) {
          bottom.close();
        }
        fs.delete(p, true);
      }
    } finally {
      this.fs.delete(p, true);
    }
  }
}