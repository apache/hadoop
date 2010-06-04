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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/** JUnit test case for HLog */
public class TestHLog extends HBaseTestCase implements HConstants {
  static final Log LOG = LogFactory.getLog(TestHLog.class);
  private Path dir;
  private Path oldLogDir;
  private MiniDFSCluster cluster;

  @Override
  public void setUp() throws Exception {
    // Make block sizes small.
    this.conf.setInt("dfs.blocksize", 1024 * 1024);
    this.conf.setInt("hbase.regionserver.flushlogentries", 1);
    cluster = new MiniDFSCluster(conf, 3, true, (String[])null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.cluster.getFileSystem().getHomeDirectory().toString());
    super.setUp();
    this.dir = new Path("/hbase", getName());
    if (fs.exists(dir)) {
      fs.delete(dir, true);
    }
    this.oldLogDir = new Path(this.dir, HConstants.HREGION_OLDLOGDIR_NAME);

  }

  @Override
  public void tearDown() throws Exception {
    if (this.fs.exists(this.dir)) {
      this.fs.delete(this.dir, true);
    }
    shutdownDfs(cluster);
    super.tearDown();
  }

  /**
   * Just write multiple logs then split.  Before fix for HADOOP-2283, this
   * would fail.
   * @throws IOException
   */
  public void testSplit() throws IOException {

    final byte [] tableName = Bytes.toBytes(getName());
    final byte [] rowName = tableName;
    Path logdir = new Path(this.dir, HConstants.HREGION_LOGDIR_NAME);
    HLog log = new HLog(this.fs, logdir, this.oldLogDir, this.conf, null);
    final int howmany = 3;
    HRegionInfo[] infos = new HRegionInfo[3];
    for(int i = 0; i < howmany; i++) {
      infos[i] = new HRegionInfo(new HTableDescriptor(tableName),
                Bytes.toBytes("" + i), Bytes.toBytes("" + (i+1)), false);
    }
    // Add edits for three regions.
    try {
      for (int ii = 0; ii < howmany; ii++) {
        for (int i = 0; i < howmany; i++) {

          for (int j = 0; j < howmany; j++) {
            WALEdit edit = new WALEdit();
            byte [] family = Bytes.toBytes("column");
            byte [] qualifier = Bytes.toBytes(Integer.toString(j));
            byte [] column = Bytes.toBytes("column:" + Integer.toString(j));
            edit.add(new KeyValue(rowName, family, qualifier,
                System.currentTimeMillis(), column));
            LOG.info("Region " + i + ": " + edit);
            log.append(infos[i], tableName, edit,
              System.currentTimeMillis());
          }
        }
        log.hflush();
        log.rollWriter();
      }
      Path splitsdir = new Path(this.dir, "splits");
      List<Path> splits =
        HLog.splitLog(splitsdir, logdir, this.oldLogDir, this.fs, this.conf);
      verifySplits(splits, howmany);
      log = null;
    } finally {
      if (log != null) {
        log.closeAndDelete();
      }
    }
  }

  /**
   * Test new HDFS-265 sync.
   * @throws Exception
   */
  public void Broken_testSync() throws Exception {
    byte [] bytes = Bytes.toBytes(getName());
    // First verify that using streams all works.
    Path p = new Path(this.dir, getName() + ".fsdos");
    FSDataOutputStream out = fs.create(p);
    out.write(bytes);
    out.sync();
    FSDataInputStream in = fs.open(p);
    assertTrue(in.available() > 0);
    byte [] buffer = new byte [1024];
    int read = in.read(buffer);
    assertEquals(bytes.length, read);
    out.close();
    in.close();
    Path subdir = new Path(this.dir, "hlogdir");
    HLog wal = new HLog(this.fs, subdir, this.oldLogDir, this.conf, null);
    final int total = 20;

    HRegionInfo info = new HRegionInfo(new HTableDescriptor(bytes),
                null,null, false);

    for (int i = 0; i < total; i++) {
      WALEdit kvs = new WALEdit();
      kvs.add(new KeyValue(Bytes.toBytes(i), bytes, bytes));
      wal.append(info, bytes, kvs, System.currentTimeMillis());
    }
    // Now call sync and try reading.  Opening a Reader before you sync just
    // gives you EOFE.
    wal.sync();
    // Open a Reader.
    Path walPath = wal.computeFilename();
    HLog.Reader reader = HLog.getReader(fs, walPath, conf);
    int count = 0;
    HLog.Entry entry = new HLog.Entry();
    while ((entry = reader.next(entry)) != null) count++;
    assertEquals(total, count);
    reader.close();
    // Add test that checks to see that an open of a Reader works on a file
    // that has had a sync done on it.
    for (int i = 0; i < total; i++) {
      WALEdit kvs = new WALEdit();
      kvs.add(new KeyValue(Bytes.toBytes(i), bytes, bytes));
      wal.append(info, bytes, kvs, System.currentTimeMillis());
    }
    reader = HLog.getReader(fs, walPath, conf);
    count = 0;
    while((entry = reader.next(entry)) != null) count++;
    assertTrue(count >= total);
    reader.close();
    // If I sync, should see double the edits.
    wal.sync();
    reader = HLog.getReader(fs, walPath, conf);
    count = 0;
    while((entry = reader.next(entry)) != null) count++;
    assertEquals(total * 2, count);
    // Now do a test that ensures stuff works when we go over block boundary,
    // especially that we return good length on file.
    final byte [] value = new byte[1025 * 1024];  // Make a 1M value.
    for (int i = 0; i < total; i++) {
      WALEdit kvs = new WALEdit();
      kvs.add(new KeyValue(Bytes.toBytes(i), bytes, value));
      wal.append(info, bytes, kvs, System.currentTimeMillis());
    }
    // Now I should have written out lots of blocks.  Sync then read.
    wal.sync();
    reader = HLog.getReader(fs, walPath, conf);
    count = 0;
    while((entry = reader.next(entry)) != null) count++;
    assertEquals(total * 3, count);
    reader.close();
    // Close it and ensure that closed, Reader gets right length also.
    wal.close();
    reader = HLog.getReader(fs, walPath, conf);
    count = 0;
    while((entry = reader.next(entry)) != null) count++;
    assertEquals(total * 3, count);
    reader.close();
  }

  /**
   * Test the findMemstoresWithEditsOlderThan method.
   * @throws IOException
   */
  public void testFindMemstoresWithEditsOlderThan() throws IOException {
    Map<byte [], Long> regionsToSeqids = new HashMap<byte [], Long>();
    for (int i = 0; i < 10; i++) {
      Long l = Long.valueOf(i);
      regionsToSeqids.put(l.toString().getBytes(), l);
    }
    byte [][] regions =
      HLog.findMemstoresWithEditsOlderThan(1, regionsToSeqids);
    assertEquals(1, regions.length);
    assertTrue(Bytes.equals(regions[0], "0".getBytes()));
    regions = HLog.findMemstoresWithEditsOlderThan(3, regionsToSeqids);
    int count = 3;
    assertEquals(count, regions.length);
    // Regions returned are not ordered.
    for (int i = 0; i < count; i++) {
      assertTrue(Bytes.equals(regions[i], "0".getBytes()) ||
        Bytes.equals(regions[i], "1".getBytes()) ||
        Bytes.equals(regions[i], "2".getBytes()));
    }
  }

  private void verifySplits(List<Path> splits, final int howmany)
  throws IOException {
    assertEquals(howmany, splits.size());
    for (int i = 0; i < splits.size(); i++) {
      LOG.info("Verifying=" + splits.get(i));
      HLog.Reader reader = HLog.getReader(this.fs, splits.get(i), conf);
      try {
        int count = 0;
        String previousRegion = null;
        long seqno = -1;
        HLog.Entry entry = new HLog.Entry();
        while((entry = reader.next(entry)) != null) {
          HLogKey key = entry.getKey();
          WALEdit kv = entry.getEdit();
          String region = Bytes.toString(key.getRegionName());
          // Assert that all edits are for same region.
          if (previousRegion != null) {
            assertEquals(previousRegion, region);
          }
          LOG.info("oldseqno=" + seqno + ", newseqno=" + key.getLogSeqNum());
          assertTrue(seqno < key.getLogSeqNum());
          seqno = key.getLogSeqNum();
          previousRegion = region;
          count++;
        }
        assertEquals(howmany * howmany, count);
      } finally {
        reader.close();
      }
    }
  }

  /**
   * Tests that we can write out an edit, close, and then read it back in again.
   * @throws IOException
   */
  public void testEditAdd() throws IOException {
    final int COL_COUNT = 10;
    final byte [] tableName = Bytes.toBytes("tablename");
    final byte [] row = Bytes.toBytes("row");
    HLog.Reader reader = null;
    HLog log = new HLog(fs, dir, this.oldLogDir, this.conf, null);
    try {
      // Write columns named 1, 2, 3, etc. and then values of single byte
      // 1, 2, 3...
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      for (int i = 0; i < COL_COUNT; i++) {
        cols.add(new KeyValue(row, Bytes.toBytes("column"),
            Bytes.toBytes(Integer.toString(i)),
          timestamp, new byte[] { (byte)(i + '0') }));
      }
      HRegionInfo info = new HRegionInfo(new HTableDescriptor(tableName),
        row,Bytes.toBytes(Bytes.toString(row) + "1"), false);
      final byte [] regionName = info.getRegionName();
      log.append(info, tableName, cols, System.currentTimeMillis());
      long logSeqId = log.startCacheFlush();
      log.completeCacheFlush(regionName, tableName, logSeqId, info.isMetaRegion());
      log.close();
      Path filename = log.computeFilename();
      log = null;
      // Now open a reader on the log and assert append worked.
      reader = HLog.getReader(fs, filename, conf);
      // Above we added all columns on a single row so we only read one
      // entry in the below... thats why we have '1'.
      for (int i = 0; i < 1; i++) {
        HLog.Entry entry = reader.next(null);
        if (entry == null) break;
        HLogKey key = entry.getKey();
        WALEdit val = entry.getEdit();
        assertTrue(Bytes.equals(regionName, key.getRegionName()));
        assertTrue(Bytes.equals(tableName, key.getTablename()));
        KeyValue kv = val.getKeyValues().get(0);
        assertTrue(Bytes.equals(row, kv.getRow()));
        assertEquals((byte)(i + '0'), kv.getValue()[0]);
        System.out.println(key + " " + val);
      }
      HLog.Entry entry = null;
      while ((entry = reader.next(null)) != null) {
        HLogKey key = entry.getKey();
        WALEdit val = entry.getEdit();
        // Assert only one more row... the meta flushed row.
        assertTrue(Bytes.equals(regionName, key.getRegionName()));
        assertTrue(Bytes.equals(tableName, key.getTablename()));
        KeyValue kv = val.getKeyValues().get(0);
        assertTrue(Bytes.equals(HLog.METAROW, kv.getRow()));
        assertTrue(Bytes.equals(HLog.METAFAMILY, kv.getFamily()));
        assertEquals(0, Bytes.compareTo(HLog.COMPLETE_CACHE_FLUSH,
          val.getKeyValues().get(0).getValue()));
        System.out.println(key + " " + val);
      }
    } finally {
      if (log != null) {
        log.closeAndDelete();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * @throws IOException
   */
  public void testAppend() throws IOException {
    final int COL_COUNT = 10;
    final byte [] tableName = Bytes.toBytes("tablename");
    final byte [] row = Bytes.toBytes("row");
    this.conf.setBoolean("dfs.support.append", true);
    Reader reader = null;
    HLog log = new HLog(this.fs, dir, this.oldLogDir, this.conf, null);
    try {
      // Write columns named 1, 2, 3, etc. and then values of single byte
      // 1, 2, 3...
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      for (int i = 0; i < COL_COUNT; i++) {
        cols.add(new KeyValue(row, Bytes.toBytes("column"),
          Bytes.toBytes(Integer.toString(i)),
          timestamp, new byte[] { (byte)(i + '0') }));
      }
      HRegionInfo hri = new HRegionInfo(new HTableDescriptor(tableName),
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      log.append(hri, tableName, cols, System.currentTimeMillis());
      long logSeqId = log.startCacheFlush();
      log.completeCacheFlush(hri.getRegionName(), tableName, logSeqId, false);
      log.close();
      Path filename = log.computeFilename();
      log = null;
      // Now open a reader on the log and assert append worked.
      reader = HLog.getReader(fs, filename, conf);
      HLog.Entry entry = reader.next();
      assertEquals(COL_COUNT, entry.getEdit().size());
      int idx = 0;
      for (KeyValue val : entry.getEdit().getKeyValues()) {
        assertTrue(Bytes.equals(hri.getRegionName(),
          entry.getKey().getRegionName()));
        assertTrue(Bytes.equals(tableName, entry.getKey().getTablename()));
        assertTrue(Bytes.equals(row, val.getRow()));
        assertEquals((byte)(idx + '0'), val.getValue()[0]);
        System.out.println(entry.getKey() + " " + val);
        idx++;
      }

      // Get next row... the meta flushed row.
      entry = reader.next();
      assertEquals(1, entry.getEdit().size());
      for (KeyValue val : entry.getEdit().getKeyValues()) {
        assertTrue(Bytes.equals(hri.getRegionName(),
          entry.getKey().getRegionName()));
        assertTrue(Bytes.equals(tableName, entry.getKey().getTablename()));
        assertTrue(Bytes.equals(HLog.METAROW, val.getRow()));
        assertTrue(Bytes.equals(HLog.METAFAMILY, val.getFamily()));
        assertEquals(0, Bytes.compareTo(HLog.COMPLETE_CACHE_FLUSH,
          val.getValue()));
        System.out.println(entry.getKey() + " " + val);
      }
    } finally {
      if (log != null) {
        log.closeAndDelete();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }
}