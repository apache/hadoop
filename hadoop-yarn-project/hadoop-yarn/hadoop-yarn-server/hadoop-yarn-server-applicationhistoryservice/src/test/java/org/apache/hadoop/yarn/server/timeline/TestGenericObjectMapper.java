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
package org.apache.hadoop.yarn.server.timeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparator;

import static org.junit.jupiter.api.Assertions.assertEquals;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TestGenericObjectMapper {

  @Test
  void testEncoding() {
    testEncoding(Long.MAX_VALUE);
    testEncoding(Long.MIN_VALUE);
    testEncoding(0L);
    testEncoding(128L);
    testEncoding(256L);
    testEncoding(512L);
    testEncoding(-256L);
  }

  private static void testEncoding(long l) {
    byte[] b = GenericObjectMapper.writeReverseOrderedLong(l);
    assertEquals(l,
        GenericObjectMapper.readReverseOrderedLong(b, 0),
        "error decoding");
    byte[] buf = new byte[16];
    System.arraycopy(b, 0, buf, 5, 8);
    assertEquals(l,
        GenericObjectMapper.readReverseOrderedLong(buf, 5),
        "error decoding at offset");
    if (l > Long.MIN_VALUE) {
      byte[] a = GenericObjectMapper.writeReverseOrderedLong(l-1);
      assertEquals(1,
          WritableComparator.compareBytes(a, 0, a.length, b, 0, b.length),
          "error preserving ordering");
    }
    if (l < Long.MAX_VALUE) {
      byte[] c = GenericObjectMapper.writeReverseOrderedLong(l+1);
      assertEquals(1,
          WritableComparator.compareBytes(b, 0, b.length, c, 0, c.length),
          "error preserving ordering");
    }
  }

  private static void verify(Object o) throws IOException {
    assertEquals(o, GenericObjectMapper.read(GenericObjectMapper.write(o)));
  }

  @Test
  void testValueTypes() throws IOException {
    verify(Integer.MAX_VALUE);
    verify(Integer.MIN_VALUE);
    assertEquals(Integer.MAX_VALUE, GenericObjectMapper.read(
        GenericObjectMapper.write((long) Integer.MAX_VALUE)));
    assertEquals(Integer.MIN_VALUE, GenericObjectMapper.read(
        GenericObjectMapper.write((long) Integer.MIN_VALUE)));
    verify((long) Integer.MAX_VALUE + 1L);
    verify((long) Integer.MIN_VALUE - 1L);

    verify(Long.MAX_VALUE);
    verify(Long.MIN_VALUE);

    assertEquals(42, GenericObjectMapper.read(GenericObjectMapper.write(42L)));
    verify(42);
    verify(1.23);
    verify("abc");
    verify(true);
    List<String> list = new ArrayList<String>();
    list.add("123");
    list.add("abc");
    verify(list);
    Map<String, String> map = new HashMap<String, String>();
    map.put("k1", "v1");
    map.put("k2", "v2");
    verify(map);
  }

}
