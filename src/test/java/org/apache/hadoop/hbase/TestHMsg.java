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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

public class TestHMsg extends TestCase {
  public void testList() {
    List<HMsg> msgs = new ArrayList<HMsg>();
    HMsg hmsg = null;
    final int size = 10;
    for (int i = 0; i < size; i++) {
      byte [] b = Bytes.toBytes(i);
      hmsg = new HMsg(HMsg.Type.STOP_REGIONSERVER,
        new HRegionInfo(new HTableDescriptor(Bytes.toBytes("test")), b, b));
      msgs.add(hmsg);
    }
    assertEquals(size, msgs.size());
    int index = msgs.indexOf(hmsg);
    assertNotSame(-1, index);
    msgs.remove(index);
    assertEquals(size - 1, msgs.size());
    byte [] other = Bytes.toBytes("other");
    hmsg = new HMsg(HMsg.Type.STOP_REGIONSERVER,
      new HRegionInfo(new HTableDescriptor(Bytes.toBytes("test")), other, other));
    assertEquals(-1, msgs.indexOf(hmsg));
    // Assert that two HMsgs are same if same content.
    byte [] b = Bytes.toBytes(1);
    hmsg = new HMsg(HMsg.Type.STOP_REGIONSERVER,
     new HRegionInfo(new HTableDescriptor(Bytes.toBytes("test")), b, b));
    assertNotSame(-1, msgs.indexOf(hmsg));
  }

  public void testSerialization() throws IOException {
    byte [] parentbytes = Bytes.toBytes("parent");
    HRegionInfo parent =
      new HRegionInfo(new HTableDescriptor(Bytes.toBytes("parent")),
      parentbytes, parentbytes);
    // Assert simple HMsg serializes
    HMsg hmsg = new HMsg(HMsg.Type.STOP_REGIONSERVER, parent);
    byte [] bytes = Writables.getBytes(hmsg);
    HMsg close = (HMsg)Writables.getWritable(bytes, new HMsg());
    assertTrue(close.equals(hmsg));
  }
}