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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Run tests related to {@link TimestampsFilter} using HBase client APIs.
 * Sets up the HBase mini cluster once at start. Each creates a table
 * named for the method and does its stuff against that.
 */
public class TestMultipleTimestamps {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  @Test
  public void testReseeksWithOneColumnMiltipleTimestamp() throws IOException {
    byte [] TABLE = Bytes.toBytes("testReseeksWithOne" +
    "ColumnMiltipleTimestamps");
    byte [] FAMILY = Bytes.toBytes("event_log");
    byte [][] FAMILIES = new byte[][] { FAMILY };

    // create table; set versions to max...
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES, Integer.MAX_VALUE);

    Integer[] putRows = new Integer[] {1, 3, 5, 7};
    Integer[] putColumns = new Integer[] { 1, 3, 5};
    Long[] putTimestamps = new Long[] {1L, 2L, 3L, 4L, 5L};

    Integer[] scanRows = new Integer[] {3, 5};
    Integer[] scanColumns = new Integer[] {3};
    Long[] scanTimestamps = new Long[] {3L, 4L};
    int scanMaxVersions = 2;

    put(ht, FAMILY, putRows, putColumns, putTimestamps);

    flush(TABLE);

    ResultScanner scanner = scan(ht, FAMILY, scanRows, scanColumns,
        scanTimestamps, scanMaxVersions);

    KeyValue[] kvs;

    kvs = scanner.next().raw();
    assertEquals(2, kvs.length);
    checkOneCell(kvs[0], FAMILY, 3, 3, 4);
    checkOneCell(kvs[1], FAMILY, 3, 3, 3);
    kvs = scanner.next().raw();
    assertEquals(2, kvs.length);
    checkOneCell(kvs[0], FAMILY, 5, 3, 4);
    checkOneCell(kvs[1], FAMILY, 5, 3, 3);
  }

  @Test
  public void testReseeksWithMultipleColumnOneTimestamp() throws IOException {
    byte [] TABLE = Bytes.toBytes("testReseeksWithMultiple" +
    "ColumnOneTimestamps");
    byte [] FAMILY = Bytes.toBytes("event_log");
    byte [][] FAMILIES = new byte[][] { FAMILY };

    // create table; set versions to max...
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES, Integer.MAX_VALUE);

    Integer[] putRows = new Integer[] {1, 3, 5, 7};
    Integer[] putColumns = new Integer[] { 1, 3, 5};
    Long[] putTimestamps = new Long[] {1L, 2L, 3L, 4L, 5L};

    Integer[] scanRows = new Integer[] {3, 5};
    Integer[] scanColumns = new Integer[] {3,4};
    Long[] scanTimestamps = new Long[] {3L};
    int scanMaxVersions = 2;

    put(ht, FAMILY, putRows, putColumns, putTimestamps);

    flush(TABLE);

    ResultScanner scanner = scan(ht, FAMILY, scanRows, scanColumns,
        scanTimestamps, scanMaxVersions);

    KeyValue[] kvs;

    kvs = scanner.next().raw();
    assertEquals(1, kvs.length);
    checkOneCell(kvs[0], FAMILY, 3, 3, 3);
    kvs = scanner.next().raw();
    assertEquals(1, kvs.length);
    checkOneCell(kvs[0], FAMILY, 5, 3, 3);
  }

  @Test
  public void testReseeksWithMultipleColumnMultipleTimestamp() throws
  IOException {
    byte [] TABLE = Bytes.toBytes("testReseeksWithMultiple" +
    "ColumnMiltipleTimestamps");
    byte [] FAMILY = Bytes.toBytes("event_log");
    byte [][] FAMILIES = new byte[][] { FAMILY };

    // create table; set versions to max...
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES, Integer.MAX_VALUE);

    Integer[] putRows = new Integer[] {1, 3, 5, 7};
    Integer[] putColumns = new Integer[] { 1, 3, 5};
    Long[] putTimestamps = new Long[] {1L, 2L, 3L, 4L, 5L};

    Integer[] scanRows = new Integer[] {5, 7};
    Integer[] scanColumns = new Integer[] {3, 4, 5};
    Long[] scanTimestamps = new Long[] {2l, 3L};
    int scanMaxVersions = 2;

    put(ht, FAMILY, putRows, putColumns, putTimestamps);

    flush(TABLE);

    ResultScanner scanner = scan(ht, FAMILY, scanRows, scanColumns,
        scanTimestamps, scanMaxVersions);

    KeyValue[] kvs;

    kvs = scanner.next().raw();
    assertEquals(4, kvs.length);
    checkOneCell(kvs[0], FAMILY, 5, 3, 3);
    checkOneCell(kvs[1], FAMILY, 5, 3, 2);
    checkOneCell(kvs[2], FAMILY, 5, 5, 3);
    checkOneCell(kvs[3], FAMILY, 5, 5, 2);
    kvs = scanner.next().raw();
    assertEquals(4, kvs.length);
    checkOneCell(kvs[0], FAMILY, 7, 3, 3);
    checkOneCell(kvs[1], FAMILY, 7, 3, 2);
    checkOneCell(kvs[2], FAMILY, 7, 5, 3);
    checkOneCell(kvs[3], FAMILY, 7, 5, 2);
  }

  @Test
  public void testReseeksWithMultipleFiles() throws IOException {
    byte [] TABLE = Bytes.toBytes("testReseeksWithMultipleFiles");
    byte [] FAMILY = Bytes.toBytes("event_log");
    byte [][] FAMILIES = new byte[][] { FAMILY };

    // create table; set versions to max...
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES, Integer.MAX_VALUE);

    Integer[] putRows1 = new Integer[] {1, 2, 3};
    Integer[] putColumns1 = new Integer[] { 2, 5, 6};
    Long[] putTimestamps1 = new Long[] {1L, 2L, 5L};

    Integer[] putRows2 = new Integer[] {6, 7};
    Integer[] putColumns2 = new Integer[] {3, 6};
    Long[] putTimestamps2 = new Long[] {4L, 5L};

    Integer[] putRows3 = new Integer[] {2, 3, 5};
    Integer[] putColumns3 = new Integer[] {1, 2, 3};
    Long[] putTimestamps3 = new Long[] {4L,8L};


    Integer[] scanRows = new Integer[] {3, 5, 7};
    Integer[] scanColumns = new Integer[] {3, 4, 5};
    Long[] scanTimestamps = new Long[] {2l, 4L};
    int scanMaxVersions = 5;

    put(ht, FAMILY, putRows1, putColumns1, putTimestamps1);
    flush(TABLE);
    put(ht, FAMILY, putRows2, putColumns2, putTimestamps2);
    flush(TABLE);
    put(ht, FAMILY, putRows3, putColumns3, putTimestamps3);

    ResultScanner scanner = scan(ht, FAMILY, scanRows, scanColumns,
        scanTimestamps, scanMaxVersions);

    KeyValue[] kvs;

    kvs = scanner.next().raw();
    assertEquals(2, kvs.length);
    checkOneCell(kvs[0], FAMILY, 3, 3, 4);
    checkOneCell(kvs[1], FAMILY, 3, 5, 2);

    kvs = scanner.next().raw();
    assertEquals(1, kvs.length);
    checkOneCell(kvs[0], FAMILY, 5, 3, 4);

    kvs = scanner.next().raw();
    assertEquals(1, kvs.length);
    checkOneCell(kvs[0], FAMILY, 6, 3, 4);

    kvs = scanner.next().raw();
    assertEquals(1, kvs.length);
    checkOneCell(kvs[0], FAMILY, 7, 3, 4);
  }

  @Test
  public void testWithVersionDeletes() throws Exception {

    // first test from memstore (without flushing).
    testWithVersionDeletes(false);

    // run same test against HFiles (by forcing a flush).
    testWithVersionDeletes(true);
  }

  public void testWithVersionDeletes(boolean flushTables) throws IOException {
    byte [] TABLE = Bytes.toBytes("testWithVersionDeletes_" +
        (flushTables ? "flush" : "noflush"));
    byte [] FAMILY = Bytes.toBytes("event_log");
    byte [][] FAMILIES = new byte[][] { FAMILY };

    // create table; set versions to max...
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES, Integer.MAX_VALUE);

    // For row:0, col:0: insert versions 1 through 5.
    putNVersions(ht, FAMILY, 0, 0, 1, 5);

    if (flushTables) {
      flush(TABLE);
    }

    // delete version 4.
    deleteOneVersion(ht, FAMILY, 0, 0, 4);

    // request a bunch of versions including the deleted version. We should
    // only get back entries for the versions that exist.
    KeyValue kvs[] = getNVersions(ht, FAMILY, 0, 0,
        Arrays.asList(2L, 3L, 4L, 5L));
    assertEquals(3, kvs.length);
    checkOneCell(kvs[0], FAMILY, 0, 0, 5);
    checkOneCell(kvs[1], FAMILY, 0, 0, 3);
    checkOneCell(kvs[2], FAMILY, 0, 0, 2);
  }

  @Test
  public void testWithMultipleVersionDeletes() throws IOException {
    byte [] TABLE = Bytes.toBytes("testWithMultipleVersionDeletes");
    byte [] FAMILY = Bytes.toBytes("event_log");
    byte [][] FAMILIES = new byte[][] { FAMILY };

    // create table; set versions to max...
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES, Integer.MAX_VALUE);

    // For row:0, col:0: insert versions 1 through 5.
    putNVersions(ht, FAMILY, 0, 0, 1, 5);

    flush(TABLE);

    // delete all versions before 4.
    deleteAllVersionsBefore(ht, FAMILY, 0, 0, 4);

    // request a bunch of versions including the deleted version. We should
    // only get back entries for the versions that exist.
    KeyValue kvs[] = getNVersions(ht, FAMILY, 0, 0, Arrays.asList(2L, 3L));
    assertEquals(0, kvs.length);
  }

  @Test
  public void testWithColumnDeletes() throws IOException {
    byte [] TABLE = Bytes.toBytes("testWithColumnDeletes");
    byte [] FAMILY = Bytes.toBytes("event_log");
    byte [][] FAMILIES = new byte[][] { FAMILY };

    // create table; set versions to max...
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES, Integer.MAX_VALUE);

    // For row:0, col:0: insert versions 1 through 5.
    putNVersions(ht, FAMILY, 0, 0, 1, 5);

    flush(TABLE);

    // delete all versions before 4.
    deleteColumn(ht, FAMILY, 0, 0);

    // request a bunch of versions including the deleted version. We should
    // only get back entries for the versions that exist.
    KeyValue kvs[] = getNVersions(ht, FAMILY, 0, 0, Arrays.asList(2L, 3L));
    assertEquals(0, kvs.length);
  }

  @Test
  public void testWithFamilyDeletes() throws IOException {
    byte [] TABLE = Bytes.toBytes("testWithFamilyDeletes");
    byte [] FAMILY = Bytes.toBytes("event_log");
    byte [][] FAMILIES = new byte[][] { FAMILY };

    // create table; set versions to max...
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES, Integer.MAX_VALUE);

    // For row:0, col:0: insert versions 1 through 5.
    putNVersions(ht, FAMILY, 0, 0, 1, 5);

    flush(TABLE);

    // delete all versions before 4.
    deleteFamily(ht, FAMILY, 0);

    // request a bunch of versions including the deleted version. We should
    // only get back entries for the versions that exist.
    KeyValue kvs[] = getNVersions(ht, FAMILY, 0, 0, Arrays.asList(2L, 3L));
    assertEquals(0, kvs.length);
  }

  // Flush tables. Since flushing is asynchronous, sleep for a bit.
  private void flush(byte [] tableName) throws IOException {
    TEST_UTIL.flush(tableName);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException i) {
      // ignore
    }
  }

  /**
   * Assert that the passed in KeyValue has expected contents for the
   * specified row, column & timestamp.
   */
  private void checkOneCell(KeyValue kv, byte[] cf,
      int rowIdx, int colIdx, long ts) {

    String ctx = "rowIdx=" + rowIdx + "; colIdx=" + colIdx + "; ts=" + ts;

    assertEquals("Row mismatch which checking: " + ctx,
        "row:"+ rowIdx, Bytes.toString(kv.getRow()));

    assertEquals("ColumnFamily mismatch while checking: " + ctx,
        Bytes.toString(cf), Bytes.toString(kv.getFamily()));

    assertEquals("Column qualifier mismatch while checking: " + ctx,
        "column:" + colIdx,
        Bytes.toString(kv.getQualifier()));

    assertEquals("Timestamp mismatch while checking: " + ctx,
        ts, kv.getTimestamp());

    assertEquals("Value mismatch while checking: " + ctx,
        "value-version-" + ts, Bytes.toString(kv.getValue()));
  }

  /**
   * Uses the TimestampFilter on a Get to request a specified list of
   * versions for the row/column specified by rowIdx & colIdx.
   *
   */
  private  KeyValue[] getNVersions(HTable ht, byte[] cf, int rowIdx,
      int colIdx, List<Long> versions)
  throws IOException {
    byte row[] = Bytes.toBytes("row:" + rowIdx);
    byte column[] = Bytes.toBytes("column:" + colIdx);
    Get get = new Get(row);
    get.addColumn(cf, column);
    get.setMaxVersions();
    get.setTimeRange(Collections.min(versions), Collections.max(versions)+1);
    Result result = ht.get(get);

    return result.raw();
  }

  private  ResultScanner scan(HTable ht, byte[] cf,
      Integer[] rowIndexes, Integer[] columnIndexes,
      Long[] versions, int maxVersions)
  throws IOException {
    Arrays.asList(rowIndexes);
    byte startRow[] = Bytes.toBytes("row:" +
        Collections.min( Arrays.asList(rowIndexes)));
    byte endRow[] = Bytes.toBytes("row:" +
        Collections.max( Arrays.asList(rowIndexes))+1);
    Scan scan = new Scan(startRow, endRow);
    for (Integer colIdx: columnIndexes) {
      byte column[] = Bytes.toBytes("column:" + colIdx);
      scan.addColumn(cf, column);
    }
    scan.setMaxVersions(maxVersions);
    scan.setTimeRange(Collections.min(Arrays.asList(versions)),
        Collections.max(Arrays.asList(versions))+1);
    ResultScanner scanner = ht.getScanner(scan);
    return scanner;
  }

  private void put(HTable ht, byte[] cf, Integer[] rowIndexes,
      Integer[] columnIndexes, Long[] versions)
  throws IOException {
    for (int rowIdx: rowIndexes) {
      byte row[] = Bytes.toBytes("row:" + rowIdx);
      Put put = new Put(row);
      for(int colIdx: columnIndexes) {
        byte column[] = Bytes.toBytes("column:" + colIdx);
        for (long version: versions) {
          put.add(cf, column, version, Bytes.toBytes("value-version-" +
              version));
        }
      }
      ht.put(put);
    }
  }

  /**
   * Insert in specific row/column versions with timestamps
   * versionStart..versionEnd.
   */
  private void putNVersions(HTable ht, byte[] cf, int rowIdx, int colIdx,
      long versionStart, long versionEnd)
  throws IOException {
    byte row[] = Bytes.toBytes("row:" + rowIdx);
    byte column[] = Bytes.toBytes("column:" + colIdx);
    Put put = new Put(row);

    for (long idx = versionStart; idx <= versionEnd; idx++) {
      put.add(cf, column, idx, Bytes.toBytes("value-version-" + idx));
    }

    ht.put(put);
  }

  /**
   * For row/column specified by rowIdx/colIdx, delete the cell
   * corresponding to the specified version.
   */
  private void deleteOneVersion(HTable ht, byte[] cf, int rowIdx,
      int colIdx, long version)
  throws IOException {
    byte row[] = Bytes.toBytes("row:" + rowIdx);
    byte column[] = Bytes.toBytes("column:" + colIdx);
    Delete del = new Delete(row);
    del.deleteColumn(cf, column, version);
    ht.delete(del);
  }

  /**
   * For row/column specified by rowIdx/colIdx, delete all cells
   * preceeding the specified version.
   */
  private void deleteAllVersionsBefore(HTable ht, byte[] cf, int rowIdx,
      int colIdx, long version)
  throws IOException {
    byte row[] = Bytes.toBytes("row:" + rowIdx);
    byte column[] = Bytes.toBytes("column:" + colIdx);
    Delete del = new Delete(row);
    del.deleteColumns(cf, column, version);
    ht.delete(del);
  }

  private void deleteColumn(HTable ht, byte[] cf, int rowIdx, int colIdx) throws IOException {
    byte row[] = Bytes.toBytes("row:" + rowIdx);
    byte column[] = Bytes.toBytes("column:" + colIdx);
    Delete del = new Delete(row);
    del.deleteColumns(cf, column);
    ht.delete(del);
  }

  private void deleteFamily(HTable ht, byte[] cf, int rowIdx) throws IOException {
    byte row[] = Bytes.toBytes("row:" + rowIdx);
    Delete del = new Delete(row);
    del.deleteFamily(cf);
    ht.delete(del);
  }
}

