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

import junit.framework.TestCase;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test comparing HBase objects.
 */
public class TestCompare extends TestCase {

  /**
   * Sort of HRegionInfo.
   */
  public void testHRegionInfo() {
    HRegionInfo a = new HRegionInfo(Bytes.toBytes("a"), null, null);
    HRegionInfo b = new HRegionInfo(Bytes.toBytes("b"), null, null);
    assertTrue(a.compareTo(b) != 0);
    HTableDescriptor t = new HTableDescriptor("t");
    byte [] midway = Bytes.toBytes("midway");
    a = new HRegionInfo(t.getName(), null, midway);
    b = new HRegionInfo(t.getName(), midway, null);
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
    assertEquals(a, a);
    assertTrue(a.compareTo(a) == 0);
    a = new HRegionInfo(t.getName(), Bytes.toBytes("a"), Bytes.toBytes("d"));
    b = new HRegionInfo(t.getName(), Bytes.toBytes("e"), Bytes.toBytes("g"));
    assertTrue(a.compareTo(b) < 0);
    a = new HRegionInfo(t.getName(), Bytes.toBytes("aaaa"), Bytes.toBytes("dddd"));
    b = new HRegionInfo(t.getName(), Bytes.toBytes("e"), Bytes.toBytes("g"));
    assertTrue(a.compareTo(b) < 0);
    a = new HRegionInfo(t.getName(), Bytes.toBytes("aaaa"), Bytes.toBytes("dddd"));
    b = new HRegionInfo(t.getName(), Bytes.toBytes("aaaa"), Bytes.toBytes("eeee"));
    assertTrue(a.compareTo(b) < 0);
  }
}