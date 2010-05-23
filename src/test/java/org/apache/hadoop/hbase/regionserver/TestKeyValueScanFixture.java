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

import junit.framework.TestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class TestKeyValueScanFixture extends TestCase {


  public void testKeyValueScanFixture() throws IOException {
    KeyValue kvs[] = new KeyValue[]{
        KeyValueTestUtil.create("RowA", "family", "qf1",
            1, KeyValue.Type.Put, "value-1"),
        KeyValueTestUtil.create("RowA", "family", "qf2",
            1, KeyValue.Type.Put, "value-2"),
        KeyValueTestUtil.create("RowB", "family", "qf1",
            10, KeyValue.Type.Put, "value-10")
    };
    KeyValueScanner scan = new KeyValueScanFixture(
        KeyValue.COMPARATOR, kvs);

    // test simple things.
    assertNull(scan.peek());
    KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes("RowA"));
    // should seek to this:
    assertTrue(scan.seek(kv));
    KeyValue res = scan.peek();
    assertEquals(kvs[0], res);

    kv = KeyValue.createFirstOnRow(Bytes.toBytes("RowB"));
    assertTrue(scan.seek(kv));
    res = scan.peek();
    assertEquals(kvs[2], res);

    // ensure we pull things out properly:
    kv = KeyValue.createFirstOnRow(Bytes.toBytes("RowA"));
    assertTrue(scan.seek(kv));
    assertEquals(kvs[0], scan.peek());
    assertEquals(kvs[0], scan.next());
    assertEquals(kvs[1], scan.peek());
    assertEquals(kvs[1], scan.next());
    assertEquals(kvs[2], scan.peek());
    assertEquals(kvs[2], scan.next());
    assertEquals(null, scan.peek());
    assertEquals(null, scan.next());
  }
}
