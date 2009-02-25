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

import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

/**
 * Test comparing HBase objects.
 */
public class TestCompare extends TestCase {
  
  /**
   * HStoreKey sorts as you would expect in the row and column portions but
   * for the timestamps, it sorts in reverse with the newest sorting before
   * the oldest (This is intentional so we trip over the latest first when
   * iterating or looking in store files).
   */
  public void testHStoreKey() {
    long timestamp = System.currentTimeMillis();
    byte [] a = Bytes.toBytes("a");
    HStoreKey past = new HStoreKey(a, a, timestamp - 10);
    HStoreKey now = new HStoreKey(a, a, timestamp);
    HStoreKey future = new HStoreKey(a, a, timestamp + 10);
    assertTrue(past.compareTo(now) > 0);
    assertTrue(now.compareTo(now) == 0);
    assertTrue(future.compareTo(now) < 0);
    // Check that empty column comes before one with a column
    HStoreKey nocolumn = new HStoreKey(a, timestamp);
    HStoreKey withcolumn = new HStoreKey(a, a, timestamp);
    assertTrue(nocolumn.compareTo(withcolumn) < 0);
    // Check that empty column comes and LATEST comes before one with a column
    // and old timestamp.
    nocolumn = new HStoreKey(a, HConstants.LATEST_TIMESTAMP);
    withcolumn = new HStoreKey(a, a, timestamp);
    assertTrue(nocolumn.compareTo(withcolumn) < 0);
    // Test null keys.
    HStoreKey normal = new HStoreKey("a", "b");
    assertTrue(normal.compareTo(null) > 0);
    assertTrue(HStoreKey.compareTo(null, null) == 0);
    assertTrue(HStoreKey.compareTo(null, normal) < 0);
  }
  
  /**
   * Tests cases where rows keys have characters below the ','.
   * See HBASE-832
   */
  public void testHStoreKeyBorderCases() {
    /** TODO!!!!
    HRegionInfo info = new HRegionInfo(new HTableDescriptor("testtable"),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY);
    HStoreKey rowA = new HStoreKey("testtable,www.hbase.org/,1234",
        "", Long.MAX_VALUE, info);
    HStoreKey rowB = new HStoreKey("testtable,www.hbase.org/%20,99999",
        "", Long.MAX_VALUE, info);

    assertTrue(rowA.compareTo(rowB) > 0);

    rowA = new HStoreKey("testtable,www.hbase.org/,1234",
        "", Long.MAX_VALUE, HRegionInfo.FIRST_META_REGIONINFO);
    rowB = new HStoreKey("testtable,www.hbase.org/%20,99999",
        "", Long.MAX_VALUE, HRegionInfo.FIRST_META_REGIONINFO);

    assertTrue(rowA.compareTo(rowB) < 0);

    rowA = new HStoreKey("testtable,,1234",
        "", Long.MAX_VALUE, HRegionInfo.FIRST_META_REGIONINFO);
    rowB = new HStoreKey("testtable,$www.hbase.org/,99999",
        "", Long.MAX_VALUE, HRegionInfo.FIRST_META_REGIONINFO);

    assertTrue(rowA.compareTo(rowB) < 0);

    rowA = new HStoreKey(".META.,testtable,www.hbase.org/,1234,4321",
        "", Long.MAX_VALUE, HRegionInfo.ROOT_REGIONINFO);
    rowB = new HStoreKey(".META.,testtable,www.hbase.org/%20,99999,99999",
        "", Long.MAX_VALUE, HRegionInfo.ROOT_REGIONINFO);

    assertTrue(rowA.compareTo(rowB) > 0);
    */
  }

  
  /**
   * Sort of HRegionInfo.
   */
  public void testHRegionInfo() {
    HRegionInfo a = new HRegionInfo(new HTableDescriptor("a"), null, null);
    HRegionInfo b = new HRegionInfo(new HTableDescriptor("b"), null, null);
    assertTrue(a.compareTo(b) != 0);
    HTableDescriptor t = new HTableDescriptor("t");
    byte [] midway = Bytes.toBytes("midway");
    a = new HRegionInfo(t, null, midway);
    b = new HRegionInfo(t, midway, null);
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
    assertEquals(a, a);
    assertTrue(a.compareTo(a) == 0);
    a = new HRegionInfo(t, Bytes.toBytes("a"), Bytes.toBytes("d"));
    b = new HRegionInfo(t, Bytes.toBytes("e"), Bytes.toBytes("g"));
    assertTrue(a.compareTo(b) < 0);
    a = new HRegionInfo(t, Bytes.toBytes("aaaa"), Bytes.toBytes("dddd"));
    b = new HRegionInfo(t, Bytes.toBytes("e"), Bytes.toBytes("g"));
    assertTrue(a.compareTo(b) < 0);
    a = new HRegionInfo(t, Bytes.toBytes("aaaa"), Bytes.toBytes("dddd"));
    b = new HRegionInfo(t, Bytes.toBytes("aaaa"), Bytes.toBytes("eeee"));
    assertTrue(a.compareTo(b) < 0);
  }
}