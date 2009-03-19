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
import java.rmi.UnexpectedException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/** memcache test case */
public class TestHMemcache extends TestCase {
  
  private Memcache hmemcache;

  private static final int ROW_COUNT = 3;

  private static final int COLUMNS_COUNT = 3;
  
  private static final String COLUMN_FAMILY = "column";

  private static final int FIRST_ROW = 1;
  private static final int NUM_VALS = 1000;
  private static final byte [] CONTENTS_BASIC = Bytes.toBytes("contents:basic");
  private static final String CONTENTSTR = "contentstr";
  private static final String ANCHORNUM = "anchor:anchornum-";
  private static final String ANCHORSTR = "anchorstr";

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
      byte [] row = Bytes.toBytes("row_" + k);
      HStoreKey key =
        new HStoreKey(row, CONTENTS_BASIC, System.currentTimeMillis());
      hmemcache.add(key, (CONTENTSTR + k).getBytes(HConstants.UTF8_ENCODING));
      
      key =
        new HStoreKey(row, Bytes.toBytes(ANCHORNUM + k), System.currentTimeMillis());
      hmemcache.add(key, (ANCHORSTR + k).getBytes(HConstants.UTF8_ENCODING));
    }

    // Read them back

    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      List<Cell> results;
      byte [] row = Bytes.toBytes("row_" + k);
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
      
      key = new HStoreKey(row, Bytes.toBytes(ANCHORNUM + k), Long.MAX_VALUE);
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

  private byte [] getRowName(final int index) {
    return Bytes.toBytes("row" + Integer.toString(index));
  }

  private byte [] getColumnName(final int rowIndex, final int colIndex) {
    return Bytes.toBytes(COLUMN_FAMILY + ":" + Integer.toString(rowIndex) + ";" +
        Integer.toString(colIndex));
  }

  /**
   * Adds {@link #ROW_COUNT} rows and {@link #COLUMNS_COUNT}
   * @param hmc Instance to add rows to.
   */
  private void addRows(final Memcache hmc) {
    for (int i = 0; i < ROW_COUNT; i++) {
      long timestamp = System.currentTimeMillis();
      for (int ii = 0; ii < COLUMNS_COUNT; ii++) {
        byte [] k = getColumnName(i, ii);
        hmc.add(new HStoreKey(getRowName(i), k, timestamp), k);
      }
    }
  }

  private void runSnapshot(final Memcache hmc) throws UnexpectedException {
    // Save off old state.
    int oldHistorySize = hmc.getSnapshot().size();
    hmc.snapshot();
    SortedMap<HStoreKey, byte[]> ss = hmc.getSnapshot();
    // Make some assertions about what just happened.
    assertTrue("History size has not increased", oldHistorySize < ss.size());
    hmc.clearSnapshot(ss);
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
      SortedMap<HStoreKey, byte[]> ss = this.hmemcache.getSnapshot();
      assertEquals("History not being cleared", 0, ss.size());
    }
  }
  
  private void isExpectedRowWithoutTimestamps(final int rowIndex,
      TreeMap<byte [], Cell> row) {
    int i = 0;
    for (Map.Entry<byte[], Cell> entry : row.entrySet()) {
      byte[] colname = entry.getKey();
      Cell cell = entry.getValue();
      String expectedColname = Bytes.toString(getColumnName(rowIndex, i++));
      String colnameStr = Bytes.toString(colname);
      assertEquals("Column name", colnameStr, expectedColname);
      // Value is column name as bytes.  Usually result is
      // 100 bytes in size at least. This is the default size
      // for BytesWriteable.  For comparison, convert bytes to
      // String and trim to remove trailing null bytes.
      byte [] value = cell.getValue();
      String colvalueStr = Bytes.toString(value).trim();
      assertEquals("Content", colnameStr, colvalueStr);
    }
  }

  private void isExpectedRow(final int rowIndex, TreeMap<byte [], Cell> row) {
    TreeMap<byte [], Cell> converted =
      new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte [], Cell> entry : row.entrySet()) {
      converted.put(entry.getKey(), 
        new Cell(entry.getValue() == null ? null : entry.getValue().getValue(),
            HConstants.LATEST_TIMESTAMP));
    }
    isExpectedRowWithoutTimestamps(rowIndex, converted);
  }
  
  /** Test getFull from memcache
   */
  public void testGetFull() {
    addRows(this.hmemcache);
    for (int i = 0; i < ROW_COUNT; i++) {
      HStoreKey hsk = new HStoreKey(getRowName(i));
      TreeMap<byte [], Cell> all =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      TreeMap<byte [], Long> deletes =
        new TreeMap<byte [], Long>(Bytes.BYTES_COMPARATOR);
      this.hmemcache.getFull(hsk, null, 1, deletes, all);
      isExpectedRow(i, all);
    }
  }

  /** Test getNextRow from memcache
   */
  public void testGetNextRow() {
    addRows(this.hmemcache);
    byte [] closestToEmpty =
      this.hmemcache.getNextRow(HConstants.EMPTY_BYTE_ARRAY);
    assertTrue(Bytes.equals(closestToEmpty, getRowName(0)));
    for (int i = 0; i < ROW_COUNT; i++) {
      byte [] nr = this.hmemcache.getNextRow(getRowName(i));
      if (i + 1 == ROW_COUNT) {
        assertEquals(nr, null);
      } else {
        assertTrue(Bytes.equals(nr, getRowName(i + 1)));
      }
    }
  }

  /** Test getClosest from memcache
   */
  public void testGetClosest() {
    addRows(this.hmemcache);
    byte [] closestToEmpty = this.hmemcache.getNextRow(HConstants.EMPTY_BYTE_ARRAY);
    assertTrue(Bytes.equals(closestToEmpty, getRowName(0)));
    for (int i = 0; i < ROW_COUNT; i++) {
      byte [] nr = this.hmemcache.getNextRow(getRowName(i));
      if (i + 1 == ROW_COUNT) {
        assertEquals(nr, null);
      } else {
        assertTrue(Bytes.equals(nr, getRowName(i + 1)));
      }
    }
  }

  /**
   * Test memcache scanner
   * @throws IOException
   */
  public void testScanner() throws IOException {
    addRows(this.hmemcache);
    long timestamp = System.currentTimeMillis();
    byte [][] cols = new byte[COLUMNS_COUNT * ROW_COUNT][];
    for (int i = 0; i < ROW_COUNT; i++) {
      for (int ii = 0; ii < COLUMNS_COUNT; ii++) {
        cols[(ii + (i * COLUMNS_COUNT))] = getColumnName(i, ii);
      }
    }
    InternalScanner scanner =
      this.hmemcache.getScanner(timestamp, cols, HConstants.EMPTY_START_ROW);
    HStoreKey key = new HStoreKey();
    TreeMap<byte [], Cell> results =
      new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; scanner.next(key, results); i++) {
      assertTrue("Row name",
          key.toString().startsWith(Bytes.toString(getRowName(i))));
      assertEquals("Count of columns", COLUMNS_COUNT,
          results.size());
      TreeMap<byte [], Cell> row =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      for(Map.Entry<byte [], Cell> e: results.entrySet() ) {
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
    byte [] t10 = Bytes.toBytes("010");
    byte [] t20 = Bytes.toBytes("020");
    byte [] t30 = Bytes.toBytes("030");
    byte [] t35 = Bytes.toBytes("035");
    byte [] t40 = Bytes.toBytes("040");
    
    hmemcache.add(getHSKForRow(t10), "t10 bytes".getBytes());
    hmemcache.add(getHSKForRow(t20), "t20 bytes".getBytes());
    hmemcache.add(getHSKForRow(t30), "t30 bytes".getBytes());
    // write a delete in there to see if things still work ok
    hmemcache.add(getHSKForRow(t35), HLogEdit.DELETED_BYTES);
    hmemcache.add(getHSKForRow(t40), "t40 bytes".getBytes());
    
    SortedMap<HStoreKey, Long> results = null;
    
    // try finding "015"
    results = new TreeMap<HStoreKey, Long>();
    byte [] t15 = Bytes.toBytes("015");
    hmemcache.getRowKeyAtOrBefore(t15, results);
    assertEquals(t10, results.lastKey().getRow());
    
    // try "020", we should get that row exactly
    results = new TreeMap<HStoreKey, Long>();
    hmemcache.getRowKeyAtOrBefore(t20, results);
    assertEquals(t20, results.lastKey().getRow());
  
    // try "038", should skip the deleted "035" and give "030"
    results = new TreeMap<HStoreKey, Long>();
    byte [] t38 = Bytes.toBytes("038");
    hmemcache.getRowKeyAtOrBefore(t38, results);
    assertEquals(t30, results.lastKey().getRow());
  
    // try "050", should get stuff from "040"
    results = new TreeMap<HStoreKey, Long>();
    byte [] t50 = Bytes.toBytes("050");
    hmemcache.getRowKeyAtOrBefore(t50, results);
    assertEquals(t40, results.lastKey().getRow());
  }
  
  private HStoreKey getHSKForRow(byte [] row) {
    return new HStoreKey(row, Bytes.toBytes("test_col:"), HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Test memcache scanner scanning cached rows, HBASE-686
   * @throws IOException
   */
  public void testScanner_686() throws IOException {
    addRows(this.hmemcache);
    long timestamp = System.currentTimeMillis();
    byte[][] cols = new byte[COLUMNS_COUNT * ROW_COUNT][];
    for (int i = 0; i < ROW_COUNT; i++) {
      for (int ii = 0; ii < COLUMNS_COUNT; ii++) {
        cols[(ii + (i * COLUMNS_COUNT))] = getColumnName(i, ii);
      }
    }
    //starting from each row, validate results should contain the starting row
    for (int startRowId = 0; startRowId < ROW_COUNT; startRowId++) {
      InternalScanner scanner = this.hmemcache.getScanner(timestamp,
          cols, getRowName(startRowId));
      HStoreKey key = new HStoreKey();
      TreeMap<byte[], Cell> results =
        new TreeMap<byte[], Cell>(Bytes.BYTES_COMPARATOR);
      for (int i = 0; scanner.next(key, results); i++) {
        int rowId = startRowId + i;
        assertTrue("Row name",
            key.toString().startsWith(Bytes.toString(getRowName(rowId))));
        assertEquals("Count of columns", COLUMNS_COUNT, results.size());
        TreeMap<byte[], Cell> row =
          new TreeMap<byte[], Cell>(Bytes.BYTES_COMPARATOR);
        for (Map.Entry<byte[], Cell> e : results.entrySet()) {
          row.put(e.getKey(),e.getValue());
        }
        isExpectedRow(rowId, row);
        // Clear out set.  Otherwise row results accumulate.
        results.clear();
      }
    }
  }
}