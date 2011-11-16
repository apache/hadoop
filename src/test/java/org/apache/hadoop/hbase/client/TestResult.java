/*
 * Copyright 2010 The Apache Software Foundation
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

import junit.framework.TestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.hbase.HBaseTestCase.assertByteEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class TestResult extends TestCase {

  static KeyValue[] genKVs(final byte[] row, final byte[] family,
                           final byte[] value,
                    final long timestamp,
                    final int cols) {
    KeyValue [] kvs = new KeyValue[cols];

    for (int i = 0; i < cols ; i++) {
      kvs[i] = new KeyValue(
          row, family, Bytes.toBytes(i),
          timestamp,
          Bytes.add(value, Bytes.toBytes(i)));
    }
    return kvs;
  }

  static final byte [] row = Bytes.toBytes("row");
  static final byte [] family = Bytes.toBytes("family");
  static final byte [] value = Bytes.toBytes("value");

  public void testBasic() throws Exception {
    KeyValue [] kvs = genKVs(row, family, value, 1, 100);

    Arrays.sort(kvs, KeyValue.COMPARATOR);

    Result r = new Result(kvs);

    for (int i = 0; i < 100; ++i) {
      final byte[] qf = Bytes.toBytes(i);

      List<KeyValue> ks = r.getColumn(family, qf);
      assertEquals(1, ks.size());
      assertByteEquals(qf, ks.get(0).getQualifier());

      assertEquals(ks.get(0), r.getColumnLatest(family, qf));
      assertByteEquals(Bytes.add(value, Bytes.toBytes(i)), r.getValue(family, qf));
      assertTrue(r.containsColumn(family, qf));
    }
  }
  public void testMultiVersion() throws Exception {
    KeyValue [] kvs1 = genKVs(row, family, value, 1, 100);
    KeyValue [] kvs2 = genKVs(row, family, value, 200, 100);

    KeyValue [] kvs = new KeyValue[kvs1.length+kvs2.length];
    System.arraycopy(kvs1, 0, kvs, 0, kvs1.length);
    System.arraycopy(kvs2, 0, kvs, kvs1.length, kvs2.length);

    Arrays.sort(kvs, KeyValue.COMPARATOR);

    Result r = new Result(kvs);
    for (int i = 0; i < 100; ++i) {
      final byte[] qf = Bytes.toBytes(i);

      List<KeyValue> ks = r.getColumn(family, qf);
      assertEquals(2, ks.size());
      assertByteEquals(qf, ks.get(0).getQualifier());
      assertEquals(200, ks.get(0).getTimestamp());

      assertEquals(ks.get(0), r.getColumnLatest(family, qf));
      assertByteEquals(Bytes.add(value, Bytes.toBytes(i)), r.getValue(family, qf));
      assertTrue(r.containsColumn(family, qf));
    }
  }

  /**
   * Verify that Result.compareResults(...) behaves correctly.
   */
  public void testCompareResults() throws Exception {
    byte [] value1 = Bytes.toBytes("value1");
    byte [] qual = Bytes.toBytes("qual");

    KeyValue kv1 = new KeyValue(row, family, qual, value);
    KeyValue kv2 = new KeyValue(row, family, qual, value1);

    Result r1 = new Result(new KeyValue[] {kv1});
    Result r2 = new Result(new KeyValue[] {kv2});
    // no exception thrown
    Result.compareResults(r1, r1);
    try {
      // these are different (HBASE-4800)
      Result.compareResults(r1, r2);
      fail();
    } catch (Exception x) {
      assertTrue(x.getMessage().startsWith("This result was different:"));
    }
  }
}
