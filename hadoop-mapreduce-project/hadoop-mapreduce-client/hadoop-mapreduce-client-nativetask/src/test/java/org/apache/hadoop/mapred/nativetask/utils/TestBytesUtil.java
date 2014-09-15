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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.mapred.nativetask.util.BytesUtil;

public class TestBytesUtil {

  @Test
  public void testBytesIntConversion() {
    final int a = 1000;
    final byte[] intBytes = Ints.toByteArray(a);

    Assert.assertEquals(a, BytesUtil.toInt(intBytes, 0));
  }

  @Test
  public void testBytesLongConversion() {
    final long l = 1000000L;
    final byte[] longBytes = Longs.toByteArray(l);

    Assert.assertEquals(l, BytesUtil.toLong(longBytes, 0));
  }

  @Test
  public void testBytesFloatConversion() {
    final float f = 3.14f;
    final byte[] floatBytes = BytesUtil.toBytes(f);

    Assert.assertEquals(f, BytesUtil.toFloat(floatBytes), 0.0f);
  }

  @Test
  public void testBytesDoubleConversion() {
    final double d = 3.14;
    final byte[] doubleBytes = BytesUtil.toBytes(d);

    Assert.assertEquals(d, BytesUtil.toDouble(doubleBytes), 0.0);
  }

  @Test
  public void testToStringBinary() {
    Assert.assertEquals("\\x01\\x02ABC",
        BytesUtil.toStringBinary(new byte[] { 1, 2, 65, 66, 67 }));
    Assert.assertEquals("\\x10\\x11",
        BytesUtil.toStringBinary(new byte[] { 16, 17 }));
  }
}
