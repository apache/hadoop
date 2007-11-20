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
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;

/** memcache test case */
public class TestHMemcache extends TestCase {
  
  private HStore.Memcache hmemcache;

  private static final int ROW_COUNT = 3;

  private static final int COLUMNS_COUNT = 3;
  
  private static final String COLUMN_FAMILY = "column";

  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.hmemcache = new HStore.Memcache();
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
  private void addRows(final HStore.Memcache hmc)
    throws UnsupportedEncodingException {
    
    for (int i = 0; i < ROW_COUNT; i++) {
      long timestamp = System.currentTimeMillis();
      for (int ii = 0; ii < COLUMNS_COUNT; ii++) {
        Text k = getColumnName(i, ii);
        hmc.add(new HStoreKey(getRowName(i), k, timestamp),
            k.toString().getBytes(HConstants.UTF8_ENCODING));
      }
    }
  }

  private void runSnapshot(final HStore.Memcache hmc) {
    // Save off old state.
    int oldHistorySize = hmc.snapshot.size();
    hmc.snapshot();
    // Make some assertions about what just happened.
    assertTrue("History size has not increased",
        oldHistorySize < hmc.snapshot.size());
  }

  /** 
   * Test memcache snapshots
   * @throws IOException
   */
  public void testSnapshotting() throws IOException {
    final int snapshotCount = 5;
    // Add some rows, run a snapshot. Do it a few times.
    for (int i = 0; i < snapshotCount; i++) {
      addRows(this.hmemcache);
      runSnapshot(this.hmemcache);
      this.hmemcache.getSnapshot();
      assertEquals("History not being cleared", 0,
          this.hmemcache.snapshot.size());
    }
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
      TreeMap<Text, byte []> all = new TreeMap<Text, byte[]>();
      this.hmemcache.getFull(hsk, all);
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
