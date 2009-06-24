/*
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
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/** memstore test case */
public class TestMemStore extends TestCase {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private MemStore memstore;
  private static final int ROW_COUNT = 10;
  private static final int QUALIFIER_COUNT = 10;
  private static final byte [] FAMILY = Bytes.toBytes("column");
  private static final byte [] CONTENTS_BASIC = Bytes.toBytes("contents:basic");
  private static final String CONTENTSTR = "contentstr";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.memstore = new MemStore();
  }

  /** 
   * Test memstore snapshot happening while scanning.
   * @throws IOException
   */
  public void testScanAcrossSnapshot() throws IOException {
    int rowCount = addRows(this.memstore);
    KeyValueScanner [] memstorescanners = this.memstore.getScanners();
    Scan scan = new Scan();
    List<KeyValue> result = new ArrayList<KeyValue>();
    StoreScanner s = new StoreScanner(scan, null, HConstants.LATEST_TIMESTAMP,
      this.memstore.comparator, null, memstorescanners);
    int count = 0;
    try {
      while (s.next(result)) {
        LOG.info(result);
        count++;
        result.clear();
      }
    } finally {
      s.close();
    }
    assertEquals(rowCount, count);
    // Now assert can count same number even if a snapshot mid-scan.
    s = new StoreScanner(scan, null, HConstants.LATEST_TIMESTAMP,
      this.memstore.comparator, null, memstorescanners);
    count = 0;
    try {
      while (s.next(result)) {
        LOG.info(result);
        // Assert the stuff is coming out in right order.
        assertTrue(Bytes.compareTo(Bytes.toBytes(count), result.get(0).getRow()) == 0);
        count++;
        if (count == 2) {
          this.memstore.snapshot();
          LOG.info("Snapshotted");
        }
        result.clear();
      }
    } finally {
      s.close();
    }
    assertEquals(rowCount, count);
  }

  /** 
   * Test memstore snapshots
   * @throws IOException
   */
  public void testSnapshotting() throws IOException {
    final int snapshotCount = 5;
    // Add some rows, run a snapshot. Do it a few times.
    for (int i = 0; i < snapshotCount; i++) {
      addRows(this.memstore);
      runSnapshot(this.memstore);
      Map<KeyValue, ?> ss = this.memstore.getSnapshot();
      assertEquals("History not being cleared", 0, ss.size());
    }
  }

  public void testMultipleVersionsSimple() throws Exception {
    MemStore m = new MemStore(HConstants.FOREVER, KeyValue.COMPARATOR);
    byte [] row = Bytes.toBytes("testRow");
    byte [] family = Bytes.toBytes("testFamily");
    byte [] qf = Bytes.toBytes("testQualifier");
    long [] stamps = {1,2,3};
    byte [][] values = {Bytes.toBytes("value0"), Bytes.toBytes("value1"),
        Bytes.toBytes("value2")};
    KeyValue key0 = new KeyValue(row, family, qf, stamps[0], values[0]);
    KeyValue key1 = new KeyValue(row, family, qf, stamps[1], values[1]);
    KeyValue key2 = new KeyValue(row, family, qf, stamps[2], values[2]);
    
    m.add(key0);
    m.add(key1);
    m.add(key2);
    
    assertTrue("Expected memstore to hold 3 values, actually has " + 
        m.memstore.size(), m.memstore.size() == 3);
  }

  public void testBinary() throws IOException {
    MemStore mc = new MemStore(HConstants.FOREVER, KeyValue.ROOT_COMPARATOR);
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
    for (Map.Entry<KeyValue, ?> entry: mc.memstore.entrySet()) {
      System.out.println(entry);
      byte [] b = entry.getKey().getRow();
      // Hardcoded offsets into String
      String str = Bytes.toString(b, 13, 4);
      byte [] bb = Bytes.toBytes(index);
      String bbStr = Bytes.toString(bb);
      assertEquals(str, bbStr);
      index++;
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Get tests
  //////////////////////////////////////////////////////////////////////////////
  /** For HBASE-528 */
  public void testGetRowKeyAtOrBefore() {
    // set up some test data
    byte [] t10 = Bytes.toBytes("010");
    byte [] t20 = Bytes.toBytes("020");
    byte [] t30 = Bytes.toBytes("030");
    byte [] t35 = Bytes.toBytes("035");
    byte [] t40 = Bytes.toBytes("040");
    
    memstore.add(getKV(t10, "t10 bytes".getBytes()));
    memstore.add(getKV(t20, "t20 bytes".getBytes()));
    memstore.add(getKV(t30, "t30 bytes".getBytes()));
    memstore.add(getKV(t35, "t35 bytes".getBytes()));
    // write a delete in there to see if things still work ok
    memstore.add(getDeleteKV(t35));
    memstore.add(getKV(t40, "t40 bytes".getBytes()));
    
    NavigableSet<KeyValue> results = null;
    
    // try finding "015"
    results =
      new TreeSet<KeyValue>(this.memstore.comparator.getComparatorIgnoringType());
    KeyValue t15 = new KeyValue(Bytes.toBytes("015"),
      System.currentTimeMillis());
    memstore.getRowKeyAtOrBefore(t15, results);
    KeyValue kv = results.last();
    assertTrue(KeyValue.COMPARATOR.compareRows(kv, t10) == 0);

    // try "020", we should get that row exactly
    results =
      new TreeSet<KeyValue>(this.memstore.comparator.getComparatorIgnoringType());
    memstore.getRowKeyAtOrBefore(new KeyValue(t20, System.currentTimeMillis()),
      results);
    assertTrue(KeyValue.COMPARATOR.compareRows(results.last(), t20) == 0);

    // try "030", we should get that row exactly
    results =
      new TreeSet<KeyValue>(this.memstore.comparator.getComparatorIgnoringType());
    memstore.getRowKeyAtOrBefore(new KeyValue(t30, System.currentTimeMillis()),
      results);
    assertTrue(KeyValue.COMPARATOR.compareRows(results.last(), t30) == 0);
  
    // try "038", should skip the deleted "035" and give "030"
    results =
      new TreeSet<KeyValue>(this.memstore.comparator.getComparatorIgnoringType());
    byte [] t38 = Bytes.toBytes("038");
    memstore.getRowKeyAtOrBefore(new KeyValue(t38, System.currentTimeMillis()),
      results);
    assertTrue(KeyValue.COMPARATOR.compareRows(results.last(), t30) == 0);
  
    // try "050", should get stuff from "040"
    results =
      new TreeSet<KeyValue>(this.memstore.comparator.getComparatorIgnoringType());
    byte [] t50 = Bytes.toBytes("050");
    memstore.getRowKeyAtOrBefore(new KeyValue(t50, System.currentTimeMillis()),
      results);
    assertTrue(KeyValue.COMPARATOR.compareRows(results.last(), t40) == 0);
  }
  
  
  /** Test getNextRow from memstore
   * @throws InterruptedException 
   */
  public void testGetNextRow() throws Exception {
    addRows(this.memstore);
    // Add more versions to make it a little more interesting.
    Thread.sleep(1);
    addRows(this.memstore);
    KeyValue closestToEmpty = this.memstore.getNextRow(KeyValue.LOWESTKEY);
    assertTrue(KeyValue.COMPARATOR.compareRows(closestToEmpty,
      new KeyValue(Bytes.toBytes(0), System.currentTimeMillis())) == 0);
    for (int i = 0; i < ROW_COUNT; i++) {
      KeyValue nr = this.memstore.getNextRow(new KeyValue(Bytes.toBytes(i),
        System.currentTimeMillis()));
      if (i + 1 == ROW_COUNT) {
        assertEquals(nr, null);
      } else {
        assertTrue(KeyValue.COMPARATOR.compareRows(nr,
          new KeyValue(Bytes.toBytes(i + 1), System.currentTimeMillis())) == 0);
      }
    }
    //starting from each row, validate results should contain the starting row
    for (int startRowId = 0; startRowId < ROW_COUNT; startRowId++) {
      InternalScanner scanner =
          new StoreScanner(new Scan(Bytes.toBytes(startRowId)), FAMILY,
              Integer.MAX_VALUE, this.memstore.comparator, null,
              new KeyValueScanner[]{memstore.getScanners()[0]});
      List<KeyValue> results = new ArrayList<KeyValue>();
      for (int i = 0; scanner.next(results); i++) {
        int rowId = startRowId + i;
        assertTrue("Row name",
          KeyValue.COMPARATOR.compareRows(results.get(0),
          Bytes.toBytes(rowId)) == 0);
        assertEquals("Count of columns", QUALIFIER_COUNT, results.size());
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
  
  public void testGet_Basic_Found() throws IOException {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf1 = Bytes.toBytes("testqualifier1");
    byte [] qf2 = Bytes.toBytes("testqualifier2");
    byte [] qf3 = Bytes.toBytes("testqualifier3");
    byte [] val = Bytes.toBytes("testval");
    
    //Setting up memstore
    KeyValue add1 = new KeyValue(row, fam ,qf1, val);
    KeyValue add2 = new KeyValue(row, fam ,qf2, val);
    KeyValue add3 = new KeyValue(row, fam ,qf3, val);
    memstore.add(add1);
    memstore.add(add2);
    memstore.add(add3);
    
    //test
    Get get = new Get(row);
    NavigableSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    columns.add(qf2);
    long ttl = Long.MAX_VALUE;

    QueryMatcher matcher =
      new QueryMatcher(get, fam, columns, ttl, KeyValue.KEY_COMPARATOR, 1);
    
    List<KeyValue> result = new ArrayList<KeyValue>();
    boolean res = memstore.get(matcher, result);
    assertEquals(true, res);
  }
  
  public void testGet_Basic_NotFound() throws IOException {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf1 = Bytes.toBytes("testqualifier1");
    byte [] qf2 = Bytes.toBytes("testqualifier2");
    byte [] qf3 = Bytes.toBytes("testqualifier3");
    byte [] val = Bytes.toBytes("testval");
    
    //Setting up memstore
    KeyValue add1 = new KeyValue(row, fam ,qf1, val);
    KeyValue add3 = new KeyValue(row, fam ,qf3, val);
    memstore.add(add1);
    memstore.add(add3);
    
    //test
    Get get = new Get(row);
    NavigableSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    columns.add(qf2);
    long ttl = Long.MAX_VALUE;

    QueryMatcher matcher =
      new QueryMatcher(get, fam, columns, ttl, KeyValue.KEY_COMPARATOR, 1);
    
    List<KeyValue> result = new ArrayList<KeyValue>();
    boolean res = memstore.get(matcher, result);
    assertEquals(false, res);
  }

  public void testGet_memstoreAndSnapShot() throws IOException {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf1 = Bytes.toBytes("testqualifier1");
    byte [] qf2 = Bytes.toBytes("testqualifier2");
    byte [] qf3 = Bytes.toBytes("testqualifier3");
    byte [] qf4 = Bytes.toBytes("testqualifier4");
    byte [] qf5 = Bytes.toBytes("testqualifier5");
    byte [] val = Bytes.toBytes("testval");
    
    //Creating get
    Get get = new Get(row);
    NavigableSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    columns.add(qf2);
    columns.add(qf4);
    long ttl = Long.MAX_VALUE;

    QueryMatcher matcher =
      new QueryMatcher(get, fam, columns, ttl, KeyValue.KEY_COMPARATOR, 1);
    
    //Setting up memstore
    memstore.add(new KeyValue(row, fam ,qf1, val));
    memstore.add(new KeyValue(row, fam ,qf2, val));
    memstore.add(new KeyValue(row, fam ,qf3, val));
    //Creating a snapshot
    memstore.snapshot();
    assertEquals(3, memstore.snapshot.size());
    //Adding value to "new" memstore
    assertEquals(0, memstore.memstore.size());
    memstore.add(new KeyValue(row, fam ,qf4, val));
    memstore.add(new KeyValue(row, fam ,qf5, val));
    assertEquals(2, memstore.memstore.size());
    
    List<KeyValue> result = new ArrayList<KeyValue>();
    boolean res = memstore.get(matcher, result);
    assertEquals(true, res);
  }
  
  public void testGet_SpecificTimeStamp() throws IOException {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf1 = Bytes.toBytes("testqualifier1");
    byte [] qf2 = Bytes.toBytes("testqualifier2");
    byte [] qf3 = Bytes.toBytes("testqualifier3");
    byte [] val = Bytes.toBytes("testval");
    
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1++;
    long ts3 = ts2++;
    
    //Creating get
    Get get = new Get(row);
    get.setTimeStamp(ts2);
    NavigableSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    columns.add(qf1);
    columns.add(qf2);
    columns.add(qf3);
    long ttl = Long.MAX_VALUE;

    QueryMatcher matcher = new QueryMatcher(get, fam, columns, ttl,
      KeyValue.KEY_COMPARATOR, 1);
    
    //Setting up expected
    List<KeyValue> expected = new ArrayList<KeyValue>();
    KeyValue kv1 = new KeyValue(row, fam ,qf1, ts2, val);
    KeyValue kv2 = new KeyValue(row, fam ,qf2, ts2, val);
    KeyValue kv3 = new KeyValue(row, fam ,qf3, ts2, val);
    expected.add(kv1);
    expected.add(kv2);
    expected.add(kv3);
    
    //Setting up memstore
    memstore.add(new KeyValue(row, fam ,qf1, ts1, val));
    memstore.add(new KeyValue(row, fam ,qf2, ts1, val));
    memstore.add(new KeyValue(row, fam ,qf3, ts1, val));
    memstore.add(kv1);
    memstore.add(kv2);
    memstore.add(kv3);
    memstore.add(new KeyValue(row, fam ,qf1, ts3, val));
    memstore.add(new KeyValue(row, fam ,qf2, ts3, val));
    memstore.add(new KeyValue(row, fam ,qf3, ts3, val));
    
    //Get
    List<KeyValue> result = new ArrayList<KeyValue>();
    memstore.get(matcher, result);
    
    assertEquals(expected.size(), result.size());
    for(int i=0; i<expected.size(); i++){
      assertEquals(expected.get(i), result.get(i));
    }
  }
  
  
  //////////////////////////////////////////////////////////////////////////////
  // Delete tests
  //////////////////////////////////////////////////////////////////////////////
  public void testGetWithDelete() throws IOException {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf1 = Bytes.toBytes("testqualifier");
    byte [] val = Bytes.toBytes("testval");
    
    long ts1 = System.nanoTime();
    KeyValue put1 = new KeyValue(row, fam, qf1, ts1, val);
    long ts2 = ts1 + 1;
    KeyValue put2 = new KeyValue(row, fam, qf1, ts2, val);
    long ts3 = ts2 +1;
    KeyValue put3 = new KeyValue(row, fam, qf1, ts3, val);
    memstore.add(put1);
    memstore.add(put2);
    memstore.add(put3);
    
    assertEquals(3, memstore.memstore.size());
    
    KeyValue del2 = new KeyValue(row, fam, qf1, ts2, KeyValue.Type.Delete, val);
    memstore.delete(del2);

    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(put3);
    expected.add(del2);
    expected.add(put1);
    
    assertEquals(3, memstore.memstore.size());
    int i = 0;
    for(Map.Entry<KeyValue, ?> entry : memstore.memstore.entrySet()) {
      assertEquals(expected.get(i++), entry.getKey());
    }
  }
  
  public void testGetWithDeleteColumn() throws IOException {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf1 = Bytes.toBytes("testqualifier");
    byte [] val = Bytes.toBytes("testval");
    
    long ts1 = System.nanoTime();
    KeyValue put1 = new KeyValue(row, fam, qf1, ts1, val);
    long ts2 = ts1 + 1;
    KeyValue put2 = new KeyValue(row, fam, qf1, ts2, val);
    long ts3 = ts2 +1;
    KeyValue put3 = new KeyValue(row, fam, qf1, ts3, val);
    memstore.add(put1);
    memstore.add(put2);
    memstore.add(put3);
    
    assertEquals(3, memstore.memstore.size());
    
    KeyValue del2 = 
      new KeyValue(row, fam, qf1, ts2, KeyValue.Type.DeleteColumn, val);
    memstore.delete(del2);

    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(put3);
    expected.add(del2);
    
    assertEquals(2, memstore.memstore.size());
    int i = 0;
    for(Map.Entry<KeyValue, ?> entry : memstore.memstore.entrySet()) {
      assertEquals(expected.get(i++), entry.getKey());
    }
  }
  
  
  public void testGetWithDeleteFamily() throws IOException {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf1 = Bytes.toBytes("testqualifier1");
    byte [] qf2 = Bytes.toBytes("testqualifier2");
    byte [] qf3 = Bytes.toBytes("testqualifier3");
    byte [] val = Bytes.toBytes("testval");
    long ts = System.nanoTime();
    
    KeyValue put1 = new KeyValue(row, fam, qf1, ts, val);
    KeyValue put2 = new KeyValue(row, fam, qf2, ts, val);
    KeyValue put3 = new KeyValue(row, fam, qf3, ts, val);
    KeyValue put4 = new KeyValue(row, fam, qf3, ts+1, val);

    memstore.add(put1);
    memstore.add(put2);
    memstore.add(put3);
    memstore.add(put4);
    
    KeyValue del = 
      new KeyValue(row, fam, null, ts, KeyValue.Type.DeleteFamily, val);
    memstore.delete(del);

    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(del);
    expected.add(put4);
    
    assertEquals(2, memstore.memstore.size());
    int i = 0;
    for(Map.Entry<KeyValue, ?> entry : memstore.memstore.entrySet()) {
      assertEquals(expected.get(i++), entry.getKey());
    }
  }
  
  public void testKeepDeleteInmemstore() {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf = Bytes.toBytes("testqualifier");
    byte [] val = Bytes.toBytes("testval");
    long ts = System.nanoTime();
    memstore.add(new KeyValue(row, fam, qf, ts, val));
    KeyValue delete = new KeyValue(row, fam, qf, ts, KeyValue.Type.Delete, val);
    memstore.delete(delete);
    assertEquals(1, memstore.memstore.size());
    assertEquals(delete, memstore.memstore.firstKey());
  }

  public void testRetainsDeleteVersion() throws IOException {
    // add a put to memstore
    memstore.add(KeyValueTestUtil.create("row1", "fam", "a", 100, "dont-care"));

    // now process a specific delete:
    KeyValue delete = KeyValueTestUtil.create(
        "row1", "fam", "a", 100, KeyValue.Type.Delete, "dont-care");
    memstore.delete(delete);

    assertEquals(1, memstore.memstore.size());
    assertEquals(delete, memstore.memstore.firstKey());
  }
  public void testRetainsDeleteColumn() throws IOException {
    // add a put to memstore
    memstore.add(KeyValueTestUtil.create("row1", "fam", "a", 100, "dont-care"));

    // now process a specific delete:
    KeyValue delete = KeyValueTestUtil.create("row1", "fam", "a", 100,
        KeyValue.Type.DeleteColumn, "dont-care");
    memstore.delete(delete);

    assertEquals(1, memstore.memstore.size());
    assertEquals(delete, memstore.memstore.firstKey());
  }
  public void testRetainsDeleteFamily() throws IOException {
    // add a put to memstore
    memstore.add(KeyValueTestUtil.create("row1", "fam", "a", 100, "dont-care"));

    // now process a specific delete:
    KeyValue delete = KeyValueTestUtil.create("row1", "fam", "a", 100,
        KeyValue.Type.DeleteFamily, "dont-care");
    memstore.delete(delete);

    assertEquals(1, memstore.memstore.size());
    assertEquals(delete, memstore.memstore.firstKey());
  }

  
  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////  
  private byte [] makeQualifier(final int i1, final int i2){
    return Bytes.toBytes(Integer.toString(i1) + ";" +
        Integer.toString(i2));
  }
  
  /**
   * Adds {@link #ROW_COUNT} rows and {@link #COLUMNS_COUNT}
   * @param hmc Instance to add rows to.
   * @return How many rows we added.
   * @throws IOException 
   */
  private int addRows(final MemStore hmc) {
    for (int i = 0; i < ROW_COUNT; i++) {
      long timestamp = System.currentTimeMillis();
      for (int ii = 0; ii < QUALIFIER_COUNT; ii++) {
        byte [] row = Bytes.toBytes(i);
        byte [] qf = makeQualifier(i, ii);
        hmc.add(new KeyValue(row, FAMILY, qf, timestamp, qf));
      }
    }
    return ROW_COUNT;
  }

  private void runSnapshot(final MemStore hmc) throws UnexpectedException {
    // Save off old state.
    int oldHistorySize = hmc.getSnapshot().size();
    hmc.snapshot();
    Map<KeyValue, ?> ss = hmc.getSnapshot();
    // Make some assertions about what just happened.
    assertTrue("History size has not increased", oldHistorySize < ss.size());
    hmc.clearSnapshot(ss);
  }

  private void isExpectedRowWithoutTimestamps(final int rowIndex,
      List<KeyValue> kvs) {
    int i = 0;
    for (KeyValue kv: kvs) {
      String expectedColname = Bytes.toString(makeQualifier(rowIndex, i++));
      String colnameStr = Bytes.toString(kv.getQualifier());
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

  private KeyValue getDeleteKV(byte [] row) {
    return new KeyValue(row, Bytes.toBytes("test_col:"),
      HConstants.LATEST_TIMESTAMP, KeyValue.Type.Delete, null);
  }

  private KeyValue getKV(byte [] row, byte [] value) {
    return new KeyValue(row, Bytes.toBytes("test_col:"),
      HConstants.LATEST_TIMESTAMP, value);
  }
}