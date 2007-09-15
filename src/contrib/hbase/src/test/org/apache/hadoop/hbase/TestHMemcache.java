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
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HMemcache.Snapshot;
import org.apache.hadoop.io.Text;

/** memcache test case */
public class TestHMemcache extends TestCase {
  
  private HMemcache hmemcache;

  private Configuration conf;

  private static final int ROW_COUNT = 3;

  private static final int COLUMNS_COUNT = 3;
  
  private static final String COLUMN_FAMILY = "column";

  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.hmemcache = new HMemcache();
    // Set up a configuration that has configuration for a file
    // filesystem implementation.
    this.conf = new HBaseConfiguration();
  }

  private Text getRowName(final int index) {
    return new Text("row" + Integer.toString(index));
  }

  private Text getColumnName(final int rowIndex, final int colIndex) {
    return new Text(COLUMN_FAMILY + ":" + Integer.toString(rowIndex) + ";" +
        Integer.toString(colIndex));
  }

  /**
   * Adds {@link #ROW_COUNT} rows and {@link #COLUMNS_COUNT}
   * @param hmc Instance to add rows to.
   */
  private void addRows(final HMemcache hmc) throws UnsupportedEncodingException {
    for (int i = 0; i < ROW_COUNT; i++) {
      TreeMap<Text, byte []> columns = new TreeMap<Text, byte []>();
      for (int ii = 0; ii < COLUMNS_COUNT; ii++) {
        Text k = getColumnName(i, ii);
        columns.put(k, k.toString().getBytes(HConstants.UTF8_ENCODING));
      }
      hmc.add(getRowName(i), columns, System.currentTimeMillis());
    }
  }

  private HLog getLogfile() throws IOException {
    // Create a log file.
    Path testDir = new Path(conf.get("hadoop.tmp.dir", 
        System.getProperty("java.tmp.dir")), "hbase");
    Path logFile = new Path(testDir, this.getName());
    FileSystem fs = testDir.getFileSystem(conf);
    // Cleanup any old log file.
    if (fs.exists(logFile)) {
      fs.delete(logFile);
    }
    return new HLog(fs, logFile, this.conf);
  }

  private Snapshot runSnapshot(final HMemcache hmc, final HLog log)
    throws IOException {
    
    // Save off old state.
    int oldHistorySize = hmc.history.size();
    SortedMap<HStoreKey, byte []> oldMemcache = hmc.memcache;
    // Run snapshot.
    Snapshot s = hmc.snapshotMemcacheForLog(log);
    // Make some assertions about what just happened.
    assertEquals("Snapshot equals old memcache", hmc.snapshot,
        oldMemcache);
    assertEquals("Returned snapshot holds old memcache",
        s.memcacheSnapshot, oldMemcache);
    assertEquals("History has been incremented",
        oldHistorySize + 1, hmc.history.size());
    assertEquals("History holds old snapshot",
        hmc.history.get(oldHistorySize), oldMemcache);
    return s;
  }

  /** 
   * Test memcache snapshots
   * @throws IOException
   */
  public void testSnapshotting() throws IOException {
    final int snapshotCount = 5;
    final Text tableName = new Text(getName());
    HLog log = getLogfile();
    // Add some rows, run a snapshot. Do it a few times.
    for (int i = 0; i < snapshotCount; i++) {
      addRows(this.hmemcache);
      int historyInitialSize = this.hmemcache.history.size();
      Snapshot s = runSnapshot(this.hmemcache, log);
      log.completeCacheFlush(new Text(Integer.toString(i)),
          tableName, s.sequenceId);
      // Clean up snapshot now we are done with it.
      this.hmemcache.deleteSnapshot();
      assertTrue("History not being cleared",
        historyInitialSize == this.hmemcache.history.size());
    }
    log.closeAndDelete();
  }
  
  private void isExpectedRow(final int rowIndex, TreeMap<Text, byte []> row)
    throws UnsupportedEncodingException {
    
    int i = 0;
    for (Text colname: row.keySet()) {
      String expectedColname = getColumnName(rowIndex, i++).toString();
      String colnameStr = colname.toString();
      assertEquals("Column name", colnameStr, expectedColname);
      // Value is column name as bytes.  Usually result is
      // 100 bytes in size at least. This is the default size
      // for BytesWriteable.  For comparison, comvert bytes to
      // String and trim to remove trailing null bytes.
      byte [] value = row.get(colname);
      String colvalueStr = new String(value, HConstants.UTF8_ENCODING).trim();
      assertEquals("Content", colnameStr, colvalueStr);
    }
  }

  /** Test getFull from memcache
   * @throws UnsupportedEncodingException
   */
  public void testGetFull() throws UnsupportedEncodingException {
    addRows(this.hmemcache);
    for (int i = 0; i < ROW_COUNT; i++) {
      HStoreKey hsk = new HStoreKey(getRowName(i));
      TreeMap<Text, byte []> all = this.hmemcache.getFull(hsk);
      isExpectedRow(i, all);
    }
  }
  
  /**
   * Test memcache scanner
   * @throws IOException
   */
  public void testScanner() throws IOException {
    addRows(this.hmemcache);
    long timestamp = System.currentTimeMillis();
    Text [] cols = new Text[COLUMNS_COUNT * ROW_COUNT];
    for (int i = 0; i < ROW_COUNT; i++) {
      for (int ii = 0; ii < COLUMNS_COUNT; ii++) {
        cols[(ii + (i * COLUMNS_COUNT))] = getColumnName(i, ii);
      }
    }
    HInternalScannerInterface scanner =
      this.hmemcache.getScanner(timestamp, cols, new Text());
    HStoreKey key = new HStoreKey();
    TreeMap<Text, byte []> results = new TreeMap<Text, byte []>();
    for (int i = 0; scanner.next(key, results); i++) {
      assertTrue("Row name",
          key.toString().startsWith(getRowName(i).toString()));
      assertEquals("Count of columns", COLUMNS_COUNT,
          results.size());
      TreeMap<Text, byte []> row = new TreeMap<Text, byte []>();
      for(Map.Entry<Text, byte []> e: results.entrySet() ) {
        row.put(e.getKey(), e.getValue());
      }
      isExpectedRow(i, row);
      // Clear out set.  Otherwise row results accumulate.
      results.clear();
    }
  }
}
