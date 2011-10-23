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

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Writables;
import org.junit.Test;

public class TestHServerInfo {

  @Test
  public void testHashCodeAndEquals() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L, 5678);
    HServerInfo hsi2 = new HServerInfo(hsa1, 1L, 5678);
    HServerInfo hsi3 = new HServerInfo(hsa1, 2L, 5678);
    HServerInfo hsi4 = new HServerInfo(hsa1, 1L, 5677);
    HServerAddress hsa2 = new HServerAddress("localhost", 1235);
    HServerInfo hsi5 = new HServerInfo(hsa2, 1L, 5678);
    assertEquals(hsi1.hashCode(), hsi2.hashCode());
    assertTrue(hsi1.equals(hsi2));
    assertNotSame(hsi1.hashCode(), hsi3.hashCode());
    assertFalse(hsi1.equals(hsi3));
    assertNotSame(hsi1.hashCode(), hsi4.hashCode());
    assertFalse(hsi1.equals(hsi4));
    assertNotSame(hsi1.hashCode(), hsi5.hashCode());
    assertFalse(hsi1.equals(hsi5));
  }

  @Test
  public void testHServerInfoHServerInfo() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L, 5678);
    HServerInfo hsi2 = new HServerInfo(hsi1);
    assertEquals(hsi1, hsi2);
  }

  @Test
  public void testGetServerAddress() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L, 5678);
    assertEquals(hsi1.getServerAddress(), hsa1);
  }

  @Test
  public void testToString() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L, 5678);
    System.out.println(hsi1.toString());
  }

  @Test
  public void testReadFields() throws IOException {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L, 5678);
    HServerAddress hsa2 = new HServerAddress("localhost", 1235);
    HServerInfo hsi2 = new HServerInfo(hsa2, 1L, 5678);
    byte [] bytes = Writables.getBytes(hsi1);
    HServerInfo deserialized =
      (HServerInfo)Writables.getWritable(bytes, new HServerInfo());
    assertEquals(hsi1, deserialized);
    bytes = Writables.getBytes(hsi2);
    deserialized = (HServerInfo)Writables.getWritable(bytes, new HServerInfo());
    assertNotSame(hsa1, deserialized);
  }

  @Test
  public void testCompareTo() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L, 5678);
    HServerAddress hsa2 = new HServerAddress("localhost", 1235);
    HServerInfo hsi2 = new HServerInfo(hsa2, 1L, 5678);
    assertTrue(hsi1.compareTo(hsi1) == 0);
    assertTrue(hsi2.compareTo(hsi2) == 0);
    int compare1 = hsi1.compareTo(hsi2);
    int compare2 = hsi2.compareTo(hsi1);
    assertTrue((compare1 > 0)? compare2 < 0: compare2 > 0);
  }
}
