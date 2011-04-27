/**
 * Copyright 2011 The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestHRegionLocation {
  @Test
  public void testHashAndEqualsCode() {
    ServerName hsa1 = new ServerName("localhost", 1234, -1L);
    HRegionLocation hrl1 = new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO,
      hsa1.getHostname(), hsa1.getPort());
    HRegionLocation hrl2 = new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO,
      hsa1.getHostname(), hsa1.getPort());
    assertEquals(hrl1.hashCode(), hrl2.hashCode());
    assertTrue(hrl1.equals(hrl2));
    HRegionLocation hrl3 = new HRegionLocation(HRegionInfo.ROOT_REGIONINFO,
      hsa1.getHostname(), hsa1.getPort());
    assertNotSame(hrl1, hrl3);
    assertFalse(hrl1.equals(hrl3));
  }

  @Test
  public void testToString() {
    ServerName hsa1 = new ServerName("localhost", 1234, -1L);
    HRegionLocation hrl1 = new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO,
      hsa1.getHostname(), hsa1.getPort());
    System.out.println(hrl1.toString());
  }

  @Test
  public void testCompareTo() {
    ServerName hsa1 = new ServerName("localhost", 1234, -1L);
    HRegionLocation hsl1 =
      new HRegionLocation(HRegionInfo.ROOT_REGIONINFO, hsa1.getHostname(), hsa1.getPort());
    ServerName hsa2 = new ServerName("localhost", 1235, -1L);
    HRegionLocation hsl2 =
      new HRegionLocation(HRegionInfo.ROOT_REGIONINFO, hsa2.getHostname(), hsa2.getPort());
    assertTrue(hsl1.compareTo(hsl1) == 0);
    assertTrue(hsl2.compareTo(hsl2) == 0);
    int compare1 = hsl1.compareTo(hsl2);
    int compare2 = hsl2.compareTo(hsl1);
    assertTrue((compare1 > 0)? compare2 < 0: compare2 > 0);
  }
}