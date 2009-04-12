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
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion.Counter;
import org.apache.hadoop.hbase.util.Bytes;

/** memcache test case */
public class TestHMemcache extends TestCase {
  private Memcache hmemcache;

  private static final int ROW_COUNT = 10;

  private static final int COLUMNS_COUNT = 10;
  
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

  public void testGetWithDeletes() throws IOException {
    Memcache mc = new Memcache(HConstants.FOREVER, KeyValue.ROOT_COMPARATOR);
    final int start = 0;
    final int end = 5;
    long now = System.currentTimeMillis();
    for (int k = start; k <= end; k++) {
      byte [] row = Bytes.toBytes(k);
      KeyValue key = new KeyValue(row, CONTENTS_BASIC, now,
        (CONTENTSTR + k).getBytes(HConstants.UTF8_ENCODING));
      mc.add(key);
      System.out.println(key);
      key = new KeyValue(row, Bytes.toBytes(ANCHORNUM + k), now,
        (ANCHORSTR + k).getBytes(HConstants.UTF8_ENCODING));
      mc.add(key);
      System.out.println(key);
    }
    KeyValue key = new KeyValue(Bytes.toBytes(start), CONTENTS_BASIC, now);
    List<KeyValue> keys = mc.get(key, 1);
    assertTrue(keys.size() == 1);
    KeyValue delete = key.cloneDelete();
    mc.add(delete);
    keys = mc.get(delete, 1);
    assertTrue(keys.size() == 0);
  }

  public void testBinary() throws IOException {
    Memcache mc = new Memcache(HConstants.FOREVER, KeyValue.ROOT_COMPARATOR);
    final int start = 43;
    final int end = 46;
    for (int k = start; k <= end; k++) {
      byte [] kk = Bytes.toBytes(k);
      byte [] row =
        Bytes.toBytes(".META.,table," + Bytes.toString(kk) + ",1," + k);
      KeyValue key = new KeyValue(row, CONTENTS_BASIC,
        System.currentTimeMillis(),
        (CONTENTSTR + k).getBytes(HConstants.UTF8_ENCODING));
      mc.add(key);
      System.out.println(key);
//      key = new KeyValue(row, Bytes.toBytes(ANCHORNUM + k),
//        System.currentTimeMillis(),
//        (ANCHORSTR + k).getBytes(HConstants.UTF8_ENCODING));
//      mc.add(key);
//      System.out.println(key);
    }
    int index = start;
    for (KeyValue kv: mc.memcache) {
      System.out.println(kv);
      byte [] b = kv.getRow();
      // Hardcoded offsets into String
      String str = Bytes.toString(b, 13, 4);
      byte [] bb = Bytes.toBytes(index);
      String bbStr = Bytes.toString(bb);
      assertEquals(str, bbStr);
      index++;
    }
  }

  /**
   * @throws IOException 
   */
  public void testMemcache() throws IOException {
    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      byte [] row = Bytes.toBytes("row_" + k);
      KeyValue key = new KeyValue(row, CONTENTS_BASIC,
        System.currentTimeMillis(),
        (CONTENTSTR + k).getBytes(HConstants.UTF8_ENCODING));
      hmemcache.add(key);
      key = new KeyValue(row, Bytes.toBytes(ANCHORNUM + k),
        System.currentTimeMillis(),
        (ANCHORSTR + k).getBytes(HConstants.UTF8_ENCODING));
      hmemcache.add(key);
    }
    // this.hmemcache.dump();

    // Read them back

    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      List<KeyValue> results;
      byte [] row = Bytes.toBytes("row_" + k);
      KeyValue key = new KeyValue(row, CONTENTS_BASIC, Long.MAX_VALUE);
      results = hmemcache.get(key, 1);
      assertNotNull("no data for " + key.toString(), results);
      assertEquals(1, results.size());
      KeyValue kv = results.get(0);
      String bodystr = Bytes.toString(kv.getBuffer(), kv.getValueOffset(),
        kv.getValueLength());
      String teststr = CONTENTSTR + k;
      assertTrue("Incorrect value for key: (" + key.toString() +
          "), expected: '" + teststr + "' got: '" +
          bodystr + "'", teststr.compareTo(bodystr) == 0);
      
      key = new KeyValue(row, Bytes.toBytes(ANCHORNUM + k), Long.MAX_VALUE);
      results = hmemcache.get(key, 1);
      assertNotNull("no data for " + key.toString(), results);
      assertEquals(1, results.size());
      kv = results.get(0);
      bodystr = Bytes.toString(kv.getBuffer(), kv.getValueOffset(),
        kv.getValueLength());
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
   * @throws IOException 
   */
  private void addRows(final Memcache hmc) {
    for (int i = 0; i < ROW_COUNT; i++) {
      long timestamp = System.currentTimeMillis();
      for (int ii = 0; ii < COLUMNS_COUNT; ii++) {
        byte [] k = getColumnName(i, ii);
        hmc.add(new KeyValue(getRowName(i), k, timestamp, k));
      }
    }
  }

  private void runSnapshot(final Memcache hmc) throws UnexpectedException {
    // Save off old state.
    int oldHistorySize = hmc.getSnapshot().size();
    hmc.snapshot();
    Set<KeyValue> ss = hmc.getSnapshot();
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
      Set<KeyValue> ss = this.hmemcache.getSnapshot();
      assertEquals("History not being cleared", 0, ss.size());
    }
  }

  private void isExpectedRowWithoutTimestamps(final int rowIndex,
      List<KeyValue> kvs) {
    int i = 0;
    for (KeyValue kv: kvs) {
      String expectedColname = Bytes.toString(getColumnName(rowIndex, i++));
      String colnameStr = kv.getColumnString();
      assertEquals("Column name", colnameStr, expectedColname);
      // Value is column name as bytes.  Usually result is
      // 100 bytes in size at least. This is the default size
      // for BytesWriteable.  For comparison, convert bytes to
      // String and trim to remove trailing null bytes.
      String colvalueStr = Bytes.toString(kv.getBuffer(), kv.getValueOffset(),
        kv.getValueLength());
      assertEquals("Content", colnameStr, colvalueStr);
    }
  }

  /** Test getFull from memcache
   * @throws InterruptedException 
   */
  public void testGetFull() throws InterruptedException {
    addRows(this.hmemcache);
    Thread.sleep(1);
    addRows(this.hmemcache);
    Thread.sleep(1);
    addRows(this.hmemcache);
    Thread.sleep(1);
    addRows(this.hmemcache);
    long now = System.currentTimeMillis();
    Map<KeyValue, Counter> versionCounter =
      new TreeMap<KeyValue, Counter>(this.hmemcache.comparatorIgnoreTimestamp);
    for (int i = 0; i < ROW_COUNT; i++) {
      KeyValue kv = new KeyValue(getRowName(i), now);
      List<KeyValue> all = new ArrayList<KeyValue>();
      NavigableSet<KeyValue> deletes =
        new TreeSet<KeyValue>(KeyValue.COMPARATOR);
      this.hmemcache.getFull(kv, null, null, 1, versionCounter, deletes, all,
        System.currentTimeMillis());
      isExpectedRowWithoutTimestamps(i, all);
    }
    // Test getting two versions.
    versionCounter =
      new TreeMap<KeyValue, Counter>(this.hmemcache.comparatorIgnoreTimestamp);
    for (int i = 0; i < ROW_COUNT; i++) {
      KeyValue kv = new KeyValue(getRowName(i), now);
      List<KeyValue> all = new ArrayList<KeyValue>();
      NavigableSet<KeyValue> deletes =
        new TreeSet<KeyValue>(KeyValue.COMPARATOR);
      this.hmemcache.getFull(kv, null, null, 2, versionCounter, deletes, all,
        System.currentTimeMillis());
      byte [] previousRow = null;
      int count = 0;
      for (KeyValue k: all) {
        if (previousRow != null) {
          assertTrue(this.hmemcache.comparator.compareRows(k, previousRow) == 0);
        }
        previousRow = k.getRow();
        count++;
      }
      assertEquals(ROW_COUNT * 2, count);
    }
  }

  /** Test getNextRow from memcache
   * @throws InterruptedException 
   */
  public void testGetNextRow() throws InterruptedException {
    addRows(this.hmemcache);
    // Add more versions to make it a little more interesting.
    Thread.sleep(1);
    addRows(this.hmemcache);
    KeyValue closestToEmpty = this.hmemcache.getNextRow(KeyValue.LOWESTKEY);
    assertTrue(KeyValue.COMPARATOR.compareRows(closestToEmpty,
      new KeyValue(getRowName(0), System.currentTimeMillis())) == 0);
    for (int i = 0; i < ROW_COUNT; i++) {
      KeyValue nr = this.hmemcache.getNextRow(new KeyValue(getRowName(i),
        System.currentTimeMillis()));
      if (i + 1 == ROW_COUNT) {
        assertEquals(nr, null);
      } else {
        assertTrue(KeyValue.COMPARATOR.compareRows(nr,
          new KeyValue(getRowName(i + 1), System.currentTimeMillis())) == 0);
      }
    }
  }

  /** Test getClosest from memcache
   * @throws InterruptedException 
   */
  public void testGetClosest() throws InterruptedException {
    addRows(this.hmemcache);
    // Add more versions to make it a little more interesting.
    Thread.sleep(1);
    addRows(this.hmemcache);
    KeyValue kv = this.hmemcache.getNextRow(KeyValue.LOWESTKEY);
    assertTrue(KeyValue.COMPARATOR.compareRows(new KeyValue(getRowName(0),
      System.currentTimeMillis()), kv) == 0);
    for (int i = 0; i < ROW_COUNT; i++) {
      KeyValue nr = this.hmemcache.getNextRow(new KeyValue(getRowName(i),
        System.currentTimeMillis()));
      if (i + 1 == ROW_COUNT) {
        assertEquals(nr, null);
      } else {
        assertTrue(KeyValue.COMPARATOR.compareRows(nr,
          new KeyValue(getRowName(i + 1), System.currentTimeMillis())) == 0);
      }
    }
  }

  /**
   * Test memcache scanner
   * @throws IOException
   * @throws InterruptedException 
   */
  public void testScanner() throws IOException, InterruptedException {
    addRows(this.hmemcache);
    Thread.sleep(1);
    addRows(this.hmemcache);
    Thread.sleep(1);
    addRows(this.hmemcache);
    long timestamp = System.currentTimeMillis();
    NavigableSet<byte []> columns = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < ROW_COUNT; i++) {
      for (int ii = 0; ii < COLUMNS_COUNT; ii++) {
        columns.add(getColumnName(i, ii));
      }
    }
    InternalScanner scanner =
      this.hmemcache.getScanner(timestamp, columns, HConstants.EMPTY_START_ROW);
    List<KeyValue> results = new ArrayList<KeyValue>();
    for (int i = 0; scanner.next(results); i++) {
      KeyValue.COMPARATOR.compareRows(results.get(0), getRowName(i));
      assertEquals("Count of columns", COLUMNS_COUNT, results.size());
      isExpectedRowWithoutTimestamps(i, results);
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
    
    hmemcache.add(getKV(t10, "t10 bytes".getBytes()));
    hmemcache.add(getKV(t20, "t20 bytes".getBytes()));
    hmemcache.add(getKV(t30, "t30 bytes".getBytes()));
    hmemcache.add(getKV(t35, "t35 bytes".getBytes()));
    // write a delete in there to see if things still work ok
    hmemcache.add(getDeleteKV(t35));
    hmemcache.add(getKV(t40, "t40 bytes".getBytes()));
    
    NavigableSet<KeyValue> results = null;
    
    // try finding "015"
    results =
      new TreeSet<KeyValue>(this.hmemcache.comparator.getComparatorIgnoringType());
    KeyValue t15 = new KeyValue(Bytes.toBytes("015"),
      System.currentTimeMillis());
    hmemcache.getRowKeyAtOrBefore(t15, results);
    KeyValue kv = results.last();
    assertTrue(KeyValue.COMPARATOR.compareRows(kv, t10) == 0);

    // try "020", we should get that row exactly
    results =
      new TreeSet<KeyValue>(this.hmemcache.comparator.getComparatorIgnoringType());
    hmemcache.getRowKeyAtOrBefore(new KeyValue(t20, System.currentTimeMillis()),
      results);
    assertTrue(KeyValue.COMPARATOR.compareRows(results.last(), t20) == 0);

    // try "030", we should get that row exactly
    results =
      new TreeSet<KeyValue>(this.hmemcache.comparator.getComparatorIgnoringType());
    hmemcache.getRowKeyAtOrBefore(new KeyValue(t30, System.currentTimeMillis()),
      results);
    assertTrue(KeyValue.COMPARATOR.compareRows(results.last(), t30) == 0);
  
    // try "038", should skip the deleted "035" and give "030"
    results =
      new TreeSet<KeyValue>(this.hmemcache.comparator.getComparatorIgnoringType());
    byte [] t38 = Bytes.toBytes("038");
    hmemcache.getRowKeyAtOrBefore(new KeyValue(t38, System.currentTimeMillis()),
      results);
    assertTrue(KeyValue.COMPARATOR.compareRows(results.last(), t30) == 0);
  
    // try "050", should get stuff from "040"
    results =
      new TreeSet<KeyValue>(this.hmemcache.comparator.getComparatorIgnoringType());
    byte [] t50 = Bytes.toBytes("050");
    hmemcache.getRowKeyAtOrBefore(new KeyValue(t50, System.currentTimeMillis()),
      results);
    assertTrue(KeyValue.COMPARATOR.compareRows(results.last(), t40) == 0);
  }

  private KeyValue getDeleteKV(byte [] row) {
    return new KeyValue(row, Bytes.toBytes("test_col:"),
      HConstants.LATEST_TIMESTAMP, KeyValue.Type.Delete, null);
  }

  private KeyValue getKV(byte [] row, byte [] value) {
    return new KeyValue(row, Bytes.toBytes("test_col:"),
      HConstants.LATEST_TIMESTAMP, value);
  }

  /**
   * Test memcache scanner scanning cached rows, HBASE-686
   * @throws IOException
   */
  public void testScanner_686() throws IOException {
    addRows(this.hmemcache);
    long timestamp = System.currentTimeMillis();
    NavigableSet<byte []> cols = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < ROW_COUNT; i++) {
      for (int ii = 0; ii < COLUMNS_COUNT; ii++) {
        cols.add(getColumnName(i, ii));
      }
    }
    //starting from each row, validate results should contain the starting row
    for (int startRowId = 0; startRowId < ROW_COUNT; startRowId++) {
      InternalScanner scanner = this.hmemcache.getScanner(timestamp,
          cols, getRowName(startRowId));
      List<KeyValue> results = new ArrayList<KeyValue>();
      for (int i = 0; scanner.next(results); i++) {
        int rowId = startRowId + i;
        assertTrue("Row name",
          KeyValue.COMPARATOR.compareRows(results.get(0),
          getRowName(rowId)) == 0);
        assertEquals("Count of columns", COLUMNS_COUNT, results.size());
        List<KeyValue> row = new ArrayList<KeyValue>();
        for (KeyValue kv : results) {
          row.add(kv);
        }
        isExpectedRowWithoutTimestamps(rowId, row);
        // Clear out set.  Otherwise row results accumulate.
        results.clear();
      }
    }
  }
}