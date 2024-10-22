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
package org.apache.hadoop.mapred.nativetask.utils;

import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Longs;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.mapred.nativetask.util.BytesUtil;

public class TestBytesUtil {

  @Test
  void testBytesIntConversion() {
    final int a = 1000;
    final byte[] intBytes = Ints.toByteArray(a);

    assertEquals(a, BytesUtil.toInt(intBytes, 0));
  }

  @Test
  void testBytesLongConversion() {
    final long l = 1000000L;
    final byte[] longBytes = Longs.toByteArray(l);

    assertEquals(l, BytesUtil.toLong(longBytes, 0));
  }

  @Test
  void testBytesFloatConversion() {
    final float f = 3.14f;
    final byte[] floatBytes = BytesUtil.toBytes(f);

    assertEquals(f, BytesUtil.toFloat(floatBytes), 0.0f);
  }

  @Test
  void testBytesDoubleConversion() {
    final double d = 3.14;
    final byte[] doubleBytes = BytesUtil.toBytes(d);

    assertEquals(d, BytesUtil.toDouble(doubleBytes), 0.0);
  }

  @Test
  void testToStringBinary() {
    assertEquals("\\x01\\x02ABC",
        BytesUtil.toStringBinary(new byte[]{1, 2, 65, 66, 67}));
    assertEquals("\\x10\\x11",
        BytesUtil.toStringBinary(new byte[]{16, 17}));
  }
}
