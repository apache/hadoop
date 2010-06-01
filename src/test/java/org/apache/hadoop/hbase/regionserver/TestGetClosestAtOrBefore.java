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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * {@link TestGet} is a medley of tests of get all done up as a single test.
 * This class
 */
public class TestGetClosestAtOrBefore extends HBaseTestCase implements HConstants {
  static final Log LOG = LogFactory.getLog(TestGetClosestAtOrBefore.class);
  private MiniDFSCluster miniHdfs;

  private static final byte [] T00 = Bytes.toBytes("000");
  private static final byte [] T10 = Bytes.toBytes("010");
  private static final byte [] T11 = Bytes.toBytes("011");
  private static final byte [] T12 = Bytes.toBytes("012");
  private static final byte [] T20 = Bytes.toBytes("020");
  private static final byte [] T30 = Bytes.toBytes("030");
  private static final byte [] T31 = Bytes.toBytes("031");
  private static final byte [] T35 = Bytes.toBytes("035");
  private static final byte [] T40 = Bytes.toBytes("040");

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    this.miniHdfs = new MiniDFSCluster(this.conf, 1, true, null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.miniHdfs.getFileSystem().getHomeDirectory().toString());
  }

  public void testUsingMetaAndBinary() throws IOException {
    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = filesystem.makeQualified(new Path(conf.get(HConstants.HBASE_DIR)));
    filesystem.mkdirs(rootdir);
    // Up flush size else we bind up when we use default catalog flush of 16k.
    HRegionInfo.FIRST_META_REGIONINFO.getTableDesc().
      setMemStoreFlushSize(64 * 1024 * 1024);
    HRegion mr = HRegion.createHRegion(HRegionInfo.FIRST_META_REGIONINFO,
      rootdir, this.conf);
    // Write rows for three tables 'A', 'B', and 'C'.
    for (char c = 'A'; c < 'D'; c++) {
      HTableDescriptor htd = new HTableDescriptor("" + c);
      final int last = 128;
      final int interval = 2;
      for (int i = 0; i <= last; i += interval) {
        HRegionInfo hri = new HRegionInfo(htd,
          i == 0? HConstants.EMPTY_BYTE_ARRAY: Bytes.toBytes((byte)i),
          i == last? HConstants.EMPTY_BYTE_ARRAY: Bytes.toBytes((byte)i + interval));
        Put put = new Put(hri.getRegionName());
        put.add(CATALOG_FAMILY, REGIONINFO_QUALIFIER, Writables.getBytes(hri));
        mr.put(put, false);
      }
    }
    InternalScanner s = mr.getScanner(new Scan());
    try {
      List<KeyValue> keys = new ArrayList<KeyValue>();
      while(s.next(keys)) {
        LOG.info(keys);
        keys.clear();
      }
    } finally {
      s.close();
    }
    findRow(mr, 'C', 44, 44);
    findRow(mr, 'C', 45, 44);
    findRow(mr, 'C', 46, 46);
    findRow(mr, 'C', 43, 42);
    mr.flushcache();
    findRow(mr, 'C', 44, 44);
    findRow(mr, 'C', 45, 44);
    findRow(mr, 'C', 46, 46);
    findRow(mr, 'C', 43, 42);
    // Now delete 'C' and make sure I don't get entries from 'B'.
    byte [] firstRowInC = HRegionInfo.createRegionName(Bytes.toBytes("" + 'C'),
      HConstants.EMPTY_BYTE_ARRAY, HConstants.ZEROES, false);
    Scan scan = new Scan(firstRowInC);
    s = mr.getScanner(scan);
    try {
      List<KeyValue> keys = new ArrayList<KeyValue>();
      while (s.next(keys)) {
        mr.delete(new Delete(keys.get(0).getRow()), null, false);
        keys.clear();
      }
    } finally {
      s.close();
    }
    // Assert we get null back (pass -1).
    findRow(mr, 'C', 44, -1);
    findRow(mr, 'C', 45, -1);
    findRow(mr, 'C', 46, -1);
    findRow(mr, 'C', 43, -1);
    mr.flushcache();
    findRow(mr, 'C', 44, -1);
    findRow(mr, 'C', 45, -1);
    findRow(mr, 'C', 46, -1);
    findRow(mr, 'C', 43, -1);
  }

  /*
   * @param mr
   * @param table
   * @param rowToFind
   * @param answer Pass -1 if we're not to find anything.
   * @return Row found.
   * @throws IOException
   */
  private byte [] findRow(final HRegion mr, final char table,
    final int rowToFind, final int answer)
  throws IOException {
    byte [] tableb = Bytes.toBytes("" + table);
    // Find the row.
    byte [] tofindBytes = Bytes.toBytes((short)rowToFind);
    byte [] metaKey = HRegionInfo.createRegionName(tableb, tofindBytes,
      HConstants.NINES, false);
    LOG.info("find=" + new String(metaKey));
    Result r = mr.getClosestRowBefore(metaKey);
    if (answer == -1) {
      assertNull(r);
      return null;
    }
    assertTrue(Bytes.compareTo(Bytes.toBytes((short)answer),
      extractRowFromMetaRow(r.getRow())) == 0);
    return r.getRow();
  }

  private byte [] extractRowFromMetaRow(final byte [] b) {
    int firstDelimiter = KeyValue.getDelimiter(b, 0, b.length,
      HRegionInfo.DELIMITER);
    int lastDelimiter = KeyValue.getDelimiterInReverse(b, 0, b.length,
      HRegionInfo.DELIMITER);
    int length = lastDelimiter - firstDelimiter - 1;
    byte [] row = new byte[length];
    System.arraycopy(b, firstDelimiter + 1, row, 0, length);
    return row;
  }

  /**
   * Test file of multiple deletes and with deletes as final key.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-751">HBASE-751</a>
   */
  public void testGetClosestRowBefore3() throws IOException{
    HRegion region = null;
    byte [] c0 = COLUMNS[0];
    byte [] c1 = COLUMNS[1];
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);

      Put p = new Put(T00);
      p.add(c0, c0, T00);
      region.put(p);

      p = new Put(T10);
      p.add(c0, c0, T10);
      region.put(p);

      p = new Put(T20);
      p.add(c0, c0, T20);
      region.put(p);

      Result r = region.getClosestRowBefore(T20, c0);
      assertTrue(Bytes.equals(T20, r.getRow()));

      Delete d = new Delete(T20);
      d.deleteColumn(c0, c0);
      region.delete(d, null, false);

      r = region.getClosestRowBefore(T20, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      p = new Put(T30);
      p.add(c0, c0, T30);
      region.put(p);

      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      d = new Delete(T30);
      d.deleteColumn(c0, c0);
      region.delete(d, null, false);

      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      region.flushcache();

      // try finding "010" after flush
      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Put into a different column family.  Should make it so I still get t10
      p = new Put(T20);
      p.add(c1, c1, T20);
      region.put(p);

      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      region.flushcache();

      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Now try combo of memcache and mapfiles.  Delete the t20 COLUMS[1]
      // in memory; make sure we get back t10 again.
      d = new Delete(T20);
      d.deleteColumn(c1, c1);
      region.delete(d, null, false);
      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Ask for a value off the end of the file.  Should return t10.
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      region.flushcache();
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Ok.  Let the candidate come out of hfile but have delete of
      // the candidate be in memory.
      p = new Put(T11);
      p.add(c0, c0, T11);
      region.put(p);
      d = new Delete(T10);
      d.deleteColumn(c1, c1);
      r = region.getClosestRowBefore(T12, c0);
      assertTrue(Bytes.equals(T11, r.getRow()));
    } finally {
      if (region != null) {
        try {
          region.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
        region.getLog().closeAndDelete();
      }
    }
  }

  /** For HBASE-694 */
  public void testGetClosestRowBefore2() throws IOException{
    HRegion region = null;
    byte [] c0 = COLUMNS[0];
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);

      Put p = new Put(T10);
      p.add(c0, c0, T10);
      region.put(p);

      p = new Put(T30);
      p.add(c0, c0, T30);
      region.put(p);

      p = new Put(T40);
      p.add(c0, c0, T40);
      region.put(p);

      // try finding "035"
      Result r = region.getClosestRowBefore(T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      region.flushcache();

      // try finding "035"
      r = region.getClosestRowBefore(T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      p = new Put(T20);
      p.add(c0, c0, T20);
      region.put(p);

      // try finding "035"
      r = region.getClosestRowBefore(T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      region.flushcache();

      // try finding "035"
      r = region.getClosestRowBefore(T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));
    } finally {
      if (region != null) {
        try {
          region.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
        region.getLog().closeAndDelete();
      }
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (this.miniHdfs != null) {
      this.miniHdfs.shutdown();
    }
    super.tearDown();
  }
}