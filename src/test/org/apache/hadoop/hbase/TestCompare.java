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

import org.apache.hadoop.io.Text;

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
    Text a = new Text("a");
    HStoreKey past = new HStoreKey(a, a, timestamp - 10);
    HStoreKey now = new HStoreKey(a, a, timestamp);
    HStoreKey future = new HStoreKey(a, a, timestamp + 10);
    assertTrue(past.compareTo(now) > 0);
    assertTrue(now.compareTo(now) == 0);
    assertTrue(future.compareTo(now) < 0);
  }
  
  /**
   * Sort of HRegionInfo.
   */
  public void testHRegionInfo() {
    HRegionInfo a = new HRegionInfo(new HTableDescriptor("a"), null, null);
    HRegionInfo b = new HRegionInfo(new HTableDescriptor("b"), null, null);
    assertTrue(a.compareTo(b) != 0);
    HTableDescriptor t = new HTableDescriptor("t");
    Text midway = new Text("midway");
    a = new HRegionInfo(t, null, midway);
    b = new HRegionInfo(t, midway, null);
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
    assertEquals(a, a);
    assertTrue(a.compareTo(a) == 0);
    a = new HRegionInfo(t, new Text("a"), new Text("d"));
    b = new HRegionInfo(t, new Text("e"), new Text("g"));
    assertTrue(a.compareTo(b) < 0);
    a = new HRegionInfo(t, new Text("aaaa"), new Text("dddd"));
    b = new HRegionInfo(t, new Text("e"), new Text("g"));
    assertTrue(a.compareTo(b) < 0);
    a = new HRegionInfo(t, new Text("aaaa"), new Text("dddd"));
    b = new HRegionInfo(t, new Text("aaaa"), new Text("eeee"));
    assertTrue(a.compareTo(b) < 0);
  }
}
