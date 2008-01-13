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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;

/** JUnit test case for HLog */
public class TestHLog extends HBaseTestCase implements HConstants {
  private Path dir;
  private MiniDFSCluster cluster;

  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    super.setUp();
    this.dir = new Path("/hbase", getName());
    if (fs.exists(dir)) {
      fs.delete(dir);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void tearDown() throws Exception {
    if (this.fs.exists(this.dir)) {
      this.fs.delete(this.dir);
    }
    StaticTestEnvironment.shutdownDfs(cluster);
    super.tearDown();
  }
 
  /**
   * Just write multiple logs then split.  Before fix for HADOOP-2283, this
   * would fail.
   * @throws IOException
   */
  public void testSplit() throws IOException {
    final Text tableName = new Text(getName());
    final Text rowName = tableName;
    HLog log = new HLog(this.fs, this.dir, this.conf, null);
    // Add edits for three regions.
    try {
      for (int ii = 0; ii < 3; ii++) {
        for (int i = 0; i < 3; i++) {
          for (int j = 0; j < 3; j++) {
            TreeMap<HStoreKey, byte[]> edit = new TreeMap<HStoreKey, byte[]>();
            Text column = new Text(Integer.toString(j));
            edit.put(
                new HStoreKey(rowName, column, System.currentTimeMillis()),
                column.getBytes());
            log.append(new Text(Integer.toString(i)), tableName, edit);
          }
        }
        log.rollWriter();
      }
      HLog.splitLog(this.testDir, this.dir, this.fs, this.conf);
      log = null;
    } finally {
      if (log != null) {
        log.closeAndDelete();
      }
    }
  }

  /**
   * @throws IOException
   */
  public void testAppend() throws IOException {
    final int COL_COUNT = 10;
    final Text regionName = new Text("regionname");
    final Text tableName = new Text("tablename");
    final Text row = new Text("row");
    Reader reader = null;
    HLog log = new HLog(fs, dir, this.conf, null);
    try {
      // Write columns named 1, 2, 3, etc. and then values of single byte
      // 1, 2, 3...
      long timestamp = System.currentTimeMillis();
      TreeMap<HStoreKey, byte []> cols = new TreeMap<HStoreKey, byte []>();
      for (int i = 0; i < COL_COUNT; i++) {
        cols.put(new HStoreKey(row, new Text(Integer.toString(i)), timestamp),
            new byte[] { (byte)(i + '0') });
      }
      log.append(regionName, tableName, cols);
      long logSeqId = log.startCacheFlush();
      log.completeCacheFlush(regionName, tableName, logSeqId);
      log.close();
      Path filename = log.computeFilename(log.filenum - 1);
      log = null;
      // Now open a reader on the log and assert append worked.
      reader = new SequenceFile.Reader(fs, filename, conf);
      HLogKey key = new HLogKey();
      HLogEdit val = new HLogEdit();
      for (int i = 0; i < COL_COUNT; i++) {
        reader.next(key, val);
        assertEquals(regionName, key.getRegionName());
        assertEquals(tableName, key.getTablename());
        assertEquals(row, key.getRow());
        assertEquals((byte)(i + '0'), val.getVal()[0]);
        System.out.println(key + " " + val);
      }
      while (reader.next(key, val)) {
        // Assert only one more row... the meta flushed row.
        assertEquals(regionName, key.getRegionName());
        assertEquals(tableName, key.getTablename());
        assertEquals(HLog.METAROW, key.getRow());
        assertEquals(HLog.METACOLUMN, val.getColumn());
        assertEquals(0, HLogEdit.completeCacheFlush.compareTo(val.getVal()));
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

}