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
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.Cell;

/** memcache test case */
public class TestHMemcache extends TestCase {
  
  private Memcache hmemcache;

  private static final int ROW_COUNT = 3;

  private static final int COLUMNS_COUNT = 3;
  
  private static final String COLUMN_FAMILY = "column";

  private static final int FIRST_ROW = 1;
  private static final int NUM_VALS = 1000;
  private static final Text CONTENTS_BASIC = new Text("contents:basic");
  private static final String CONTENTSTR = "contentstr";
  private static final String ANCHORNUM = "anchor:anchornum-";
  private static final String ANCHORSTR = "anchorstr";

  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.hmemcache = new Memcache();
  }

  /**
   * @throws UnsupportedEncodingException
   */
  public void testMemcache() throws UnsupportedEncodingException {
    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      Text row = new Text("row_" + k);
      HStoreKey key =
        new HStoreKey(row, CONTENTS_BASIC, System.currentTimeMillis());
      hmemcache.add(key, (CONTENTSTR + k).getBytes(HConstants.UTF8_ENCODING));
      
      key =
        new HStoreKey(row, new Text(ANCHORNUM + k), System.currentTimeMillis());
      hmemcache.add(key, (ANCHORSTR + k).getBytes(HConstants.UTF8_ENCODING));
    }

    // Read them back

    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      List<Cell> results;
      Text row = new Text("row_" + k);
      HStoreKey key = new HStoreKey(row, CONTENTS_BASIC, Long.MAX_VALUE);
      results = hmemcache.get(key, 1);
      assertNotNull("no data for " + key.toString(), results);
      assertEquals(1, results.size());
      String bodystr = new String(results.get(0).getValue(),
          HConstants.UTF8_ENCODING);
      String teststr = CONTENTSTR + k;
      assertTrue("Incorrect value for key: (" + key.toString() +
          "), expected: '" + teststr + "' got: '" +
          bodystr + "'", teststr.compareTo(bodystr) == 0);
      
      key = new HStoreKey(row, new Text(ANCHORNUM + k), Long.MAX_VALUE);
      results = hmemcache.get(key, 1);
      assertNotNull("no data for " + key.toString(), results);
      assertEquals(1, results.size());
      bodystr = new String(results.get(0).getValue(),
          HConstants.UTF8_ENCODING);
      teststr = ANCHORSTR + k;
      assertTrue("Incorrect value for key: (" + key.toString() +
          "), expected: '" + teststr + "' got: '" + bodystr + "'",
          teststr.compareTo(bodystr) == 0);
    }
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
  private void addRows(final Memcache hmc)
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

  private void runSnapshot(final Memcache hmc) {
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
  
  private void isExpectedRowWithoutTimestamps(final int rowIndex, TreeMap<Text, byte[]> row)
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

  private void isExpectedRow(final int rowIndex, TreeMap<Text, Cell> row)
  throws UnsupportedEncodingException {
    TreeMap<Text, byte[]> converted = new TreeMap<Text, byte[]>();
    for (Map.Entry<Text, Cell> entry : row.entrySet()) {
      converted.put(entry.getKey(), 
        entry.getValue() == null ? null : entry.getValue().getValue());
    }
    isExpectedRowWithoutTimestamps(rowIndex, converted);
  }
  
  /** Test getFull from memcache
   * @throws UnsupportedEncodingException
   */
  public void testGetFull() throws UnsupportedEncodingException {
    addRows(this.hmemcache);
    for (int i = 0; i < ROW_COUNT; i++) {
      HStoreKey hsk = new HStoreKey(getRowName(i));
      TreeMap<Text, Cell> all = new TreeMap<Text, Cell>();
      TreeMap<Text, Long> deletes = new TreeMap<Text, Long>();
      this.hmemcache.getFull(hsk, null, deletes, all);
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
      isExpectedRowWithoutTimestamps(i, row);
      // Clear out set.  Otherwise row results accumulate.
      results.clear();
    }
  }
  
  /** For HBASE-528 */
  public void testGetRowKeyAtOrBefore() {
    // set up some test data
    Text t10 = new Text("010");
    Text t20 = new Text("020");
    Text t30 = new Text("030");
    Text t35 = new Text("035");
    Text t40 = new Text("040");
    
    hmemcache.add(getHSKForRow(t10), "t10 bytes".getBytes());
    hmemcache.add(getHSKForRow(t20), "t20 bytes".getBytes());
    hmemcache.add(getHSKForRow(t30), "t30 bytes".getBytes());
    // write a delete in there to see if things still work ok
    hmemcache.add(getHSKForRow(t35), HLogEdit.deleteBytes.get());
    hmemcache.add(getHSKForRow(t40), "t40 bytes".getBytes());
    
    SortedMap<HStoreKey, Long> results = null;
    
    // try finding "015"
    results = new TreeMap<HStoreKey, Long>();
    Text t15 = new Text("015");
    hmemcache.getRowKeyAtOrBefore(t15, results);
    assertEquals(t10, results.lastKey().getRow());
    
    // try "020", we should get that row exactly
    results = new TreeMap<HStoreKey, Long>();
    hmemcache.getRowKeyAtOrBefore(t20, results);
    assertEquals(t20, results.lastKey().getRow());
  
    // try "038", should skip the deleted "035" and give "030"
    results = new TreeMap<HStoreKey, Long>();
    Text t38 = new Text("038");
    hmemcache.getRowKeyAtOrBefore(t38, results);
    assertEquals(t30, results.lastKey().getRow());
  
    // try "050", should get stuff from "040"
    results = new TreeMap<HStoreKey, Long>();
    Text t50 = new Text("050");
    hmemcache.getRowKeyAtOrBefore(t50, results);
    assertEquals(t40, results.lastKey().getRow());
  }
  
  private HStoreKey getHSKForRow(Text row) {
    return new HStoreKey(row, new Text("test_col:"), HConstants.LATEST_TIMESTAMP);
  }
  
  
  
}
