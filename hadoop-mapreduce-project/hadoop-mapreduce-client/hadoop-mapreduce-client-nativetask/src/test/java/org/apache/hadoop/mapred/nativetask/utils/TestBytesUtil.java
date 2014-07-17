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

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.mapred.nativetask.util.BytesUtil;

@SuppressWarnings({ "deprecation" })
public class TestBytesUtil extends TestCase {

  public void testBytesStringConversion() {

    final String str = "I am good!";
    final byte[] bytes = BytesUtil.toBytes(str);

    Assert.assertEquals(str, BytesUtil.fromBytes(bytes));
 }

  public void testBytesIntConversion() {
    final int a = 1000;
    final byte[] intBytes = BytesUtil.toBytes(a);

    Assert.assertEquals(a, BytesUtil.toInt(intBytes));
  }

  public void testBytesLongConversion() {
    final long l = 1000000L;
    final byte[] longBytes = BytesUtil.toBytes(l);

    Assert.assertEquals(l, BytesUtil.toLong(longBytes));
  }

  public void testBytesFloatConversion() {
    final float f = 3.14f;
    final byte[] floatBytes = BytesUtil.toBytes(f);

    Assert.assertEquals(f, BytesUtil.toFloat(floatBytes));
  }

  public void testBytesDoubleConversion() {
    final double d = 3.14;
    final byte[] doubleBytes = BytesUtil.toBytes(d);

    Assert.assertEquals(d, BytesUtil.toDouble(doubleBytes));
  }
}
