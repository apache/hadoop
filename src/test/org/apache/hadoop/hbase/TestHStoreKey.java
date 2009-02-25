/**
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
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.HStoreKey.StoreKeyByteComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Tests for the HStoreKey Plain and Meta RawComparators.
 */
public class TestHStoreKey extends TestCase {
  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testByteBuffer() throws Exception {
    final long ts = 123;
    final byte [] row = Bytes.toBytes("row");
    final byte [] column = Bytes.toBytes("column");
    HStoreKey hsk = new HStoreKey(row, column, ts);
    ByteBuffer bb = ByteBuffer.wrap(hsk.getBytes());
    assertTrue(Bytes.equals(row, HStoreKey.getRow(bb)));
    assertTrue(Bytes.equals(column, HStoreKey.getColumn(bb)));
    assertEquals(ts, HStoreKey.getTimestamp(bb));
  }

  /**
   * Test the byte comparator works same as the object comparator.
   */
  public void testRawComparator() throws IOException {
    long timestamp = System.currentTimeMillis();
    byte [] a = Bytes.toBytes("a");
    HStoreKey past = new HStoreKey(a, a, timestamp - 10);
    byte [] pastBytes = Writables.getBytes(past);
    HStoreKey now = new HStoreKey(a, a, timestamp);
    byte [] nowBytes = Writables.getBytes(now);
    HStoreKey future = new HStoreKey(a, a, timestamp + 10);
    byte [] futureBytes = Writables.getBytes(future);
    StoreKeyByteComparator comparator = new HStoreKey.StoreKeyByteComparator();
    assertTrue(past.compareTo(now) > 0);
    assertTrue(comparator.compare(pastBytes, nowBytes) > 0);
    assertTrue(now.compareTo(now) == 0);
    assertTrue(comparator.compare(nowBytes, nowBytes) == 0);
    assertTrue(future.compareTo(now) < 0);
    assertTrue(comparator.compare(futureBytes, nowBytes) < 0);
    // Check that empty column comes before one with a column
    HStoreKey nocolumn = new HStoreKey(a, timestamp);
    byte [] nocolumnBytes = Writables.getBytes(nocolumn);
    HStoreKey withcolumn = new HStoreKey(a, a, timestamp);
    byte [] withcolumnBytes = Writables.getBytes(withcolumn);
    assertTrue(nocolumn.compareTo(withcolumn) < 0);
    assertTrue(comparator.compare(nocolumnBytes, withcolumnBytes) < 0);
    // Check that empty column comes and LATEST comes before one with a column
    // and old timestamp.
    nocolumn = new HStoreKey(a, HConstants.LATEST_TIMESTAMP);
    nocolumnBytes = Writables.getBytes(nocolumn);
    withcolumn = new HStoreKey(a, a, timestamp);
    withcolumnBytes = Writables.getBytes(withcolumn);
    assertTrue(nocolumn.compareTo(withcolumn) < 0);
    assertTrue(comparator.compare(nocolumnBytes, withcolumnBytes) < 0);
  }
  
//  /**
//   * Tests cases where rows keys have characters below the ','.
//   * See HBASE-832
//   * @throws IOException 
//   */
//  public void testHStoreKeyBorderCases() throws IOException {
//    HRegionInfo info = new HRegionInfo(new HTableDescriptor("testtable"),
//        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY);
//
//    HStoreKey rowA = new HStoreKey("testtable,www.hbase.org/,1234",
//      "", Long.MAX_VALUE, info);
//    byte [] rowABytes = Writables.getBytes(rowA);
//    HStoreKey rowB = new HStoreKey("testtable,www.hbase.org/%20,99999",
//      "", Long.MAX_VALUE, info);
//    byte [] rowBBytes = Writables.getBytes(rowB);
//    assertTrue(rowA.compareTo(rowB) > 0);
//    HStoreKey.Comparator comparator = new HStoreKey.PlainStoreKeyComparator();
//    assertTrue(comparator.compare(rowABytes, rowBBytes) > 0);
//
//    rowA = new HStoreKey("testtable,www.hbase.org/,1234",
//        "", Long.MAX_VALUE, HRegionInfo.FIRST_META_REGIONINFO);
//    rowB = new HStoreKey("testtable,www.hbase.org/%20,99999",
//        "", Long.MAX_VALUE, HRegionInfo.FIRST_META_REGIONINFO);
//    assertTrue(rowA.compareTo(rowB) < 0);
//    assertTrue(comparator.compare(rowABytes, rowBBytes) < 0);
//
//    rowA = new HStoreKey("testtable,,1234",
//        "", Long.MAX_VALUE, HRegionInfo.FIRST_META_REGIONINFO);
//    rowB = new HStoreKey("testtable,$www.hbase.org/,99999",
//        "", Long.MAX_VALUE, HRegionInfo.FIRST_META_REGIONINFO);
//    assertTrue(rowA.compareTo(rowB) < 0);
//    assertTrue(comparator.compare(rowABytes, rowBBytes) < 0);
//
//    rowA = new HStoreKey(".META.,testtable,www.hbase.org/,1234,4321",
//        "", Long.MAX_VALUE, HRegionInfo.ROOT_REGIONINFO);
//    rowB = new HStoreKey(".META.,testtable,www.hbase.org/%20,99999,99999",
//        "", Long.MAX_VALUE, HRegionInfo.ROOT_REGIONINFO);
//    assertTrue(rowA.compareTo(rowB) > 0);
//    assertTrue(comparator.compare(rowABytes, rowBBytes) > 0);
//  }
}