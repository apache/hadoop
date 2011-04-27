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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.hbase.util.Writables;
import org.junit.Test;

/**
 * Tests for {@link HServerAddress}
 */
public class TestHServerAddress {
  @Test
  public void testHashCode() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerAddress hsa2 = new HServerAddress("localhost", 1234);
    assertEquals(hsa1.hashCode(), hsa2.hashCode());
    HServerAddress hsa3 = new HServerAddress("localhost", 1235);
    assertNotSame(hsa1.hashCode(), hsa3.hashCode());
  }

  @Test
  public void testHServerAddress() {
    new HServerAddress();
  }

  @Test
  public void testHServerAddressInetSocketAddress() {
    HServerAddress hsa1 =
      new HServerAddress(new InetSocketAddress("localhost", 1234));
    System.out.println(hsa1.toString());
  }

  @Test
  public void testHServerAddressString() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerAddress hsa2 =
      new HServerAddress(new InetSocketAddress("localhost", 1234));
    assertTrue(hsa1.equals(hsa2));
  }

  @Test
  public void testHServerAddressHServerAddress() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerAddress hsa2 = new HServerAddress(hsa1);
    assertEquals(hsa1, hsa2);
  }

  @Test
  public void testReadFields() throws IOException {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerAddress hsa2 = new HServerAddress("localhost", 1235);
    byte [] bytes = Writables.getBytes(hsa1);
    HServerAddress deserialized =
      (HServerAddress)Writables.getWritable(bytes, new HServerAddress());
    assertEquals(hsa1, deserialized);
    bytes = Writables.getBytes(hsa2);
    deserialized =
      (HServerAddress)Writables.getWritable(bytes, new HServerAddress());
    assertNotSame(hsa1, deserialized);
  }
}