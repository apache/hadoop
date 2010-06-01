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

package org.apache.hadoop.hbase.io;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.util.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TestImmutableBytesWritable extends TestCase {
  public void testHash() throws Exception {
    assertEquals(
      new ImmutableBytesWritable(Bytes.toBytes("xxabc"), 2, 3).hashCode(),
      new ImmutableBytesWritable(Bytes.toBytes("abc")).hashCode());
    assertEquals(
      new ImmutableBytesWritable(Bytes.toBytes("xxabcd"), 2, 3).hashCode(),
      new ImmutableBytesWritable(Bytes.toBytes("abc")).hashCode());
    assertNotSame(
      new ImmutableBytesWritable(Bytes.toBytes("xxabc"), 2, 3).hashCode(),
      new ImmutableBytesWritable(Bytes.toBytes("xxabc"), 2, 2).hashCode());
  }

  public void testComparison() throws Exception {
    runTests("aa", "b", -1);
    runTests("aa", "aa", 0);
    runTests("aa", "ab", -1);
    runTests("aa", "aaa", -1);
    runTests("", "", 0);
    runTests("", "a", -1);
  }

  private void runTests(String aStr, String bStr, int signum)
    throws Exception {
    ImmutableBytesWritable a = new ImmutableBytesWritable(
      Bytes.toBytes(aStr));
    ImmutableBytesWritable b = new ImmutableBytesWritable(
      Bytes.toBytes(bStr));

    doComparisonsOnObjects(a, b, signum);
    doComparisonsOnRaw(a, b, signum);

    // Tests for when the offset is non-zero
    a = new ImmutableBytesWritable(Bytes.toBytes("xxx" + aStr),
                                   3, aStr.length());
    b = new ImmutableBytesWritable(Bytes.toBytes("yy" + bStr),
                                   2, bStr.length());
    doComparisonsOnObjects(a, b, signum);
    doComparisonsOnRaw(a, b, signum);

    // Tests for when offset is nonzero and length doesn't extend to end
    a = new ImmutableBytesWritable(Bytes.toBytes("xxx" + aStr + "zzz"),
                                   3, aStr.length());
    b = new ImmutableBytesWritable(Bytes.toBytes("yy" + bStr + "aaa"),
                                   2, bStr.length());
    doComparisonsOnObjects(a, b, signum);
    doComparisonsOnRaw(a, b, signum);
  }


  private int signum(int i) {
    if (i > 0) return 1;
    if (i == 0) return 0;
    return -1;
  }

  private void doComparisonsOnRaw(ImmutableBytesWritable a,
                                  ImmutableBytesWritable b,
                                  int expectedSignum)
    throws IOException {
    ImmutableBytesWritable.Comparator comparator =
      new ImmutableBytesWritable.Comparator();

    ByteArrayOutputStream baosA = new ByteArrayOutputStream();
    ByteArrayOutputStream baosB = new ByteArrayOutputStream();

    a.write(new DataOutputStream(baosA));
    b.write(new DataOutputStream(baosB));

    assertEquals(
      "Comparing " + a + " and " + b + " as raw",
      signum(comparator.compare(baosA.toByteArray(), 0, baosA.size(),
                                baosB.toByteArray(), 0, baosB.size())),
      expectedSignum);

    assertEquals(
      "Comparing " + a + " and " + b + " as raw (inverse)",
      -signum(comparator.compare(baosB.toByteArray(), 0, baosB.size(),
                                 baosA.toByteArray(), 0, baosA.size())),
      expectedSignum);
  }

  private void doComparisonsOnObjects(ImmutableBytesWritable a,
                                      ImmutableBytesWritable b,
                                      int expectedSignum) {
    ImmutableBytesWritable.Comparator comparator =
      new ImmutableBytesWritable.Comparator();
    assertEquals(
      "Comparing " + a + " and " + b + " as objects",
      signum(comparator.compare(a, b)), expectedSignum);
    assertEquals(
      "Comparing " + a + " and " + b + " as objects (inverse)",
      -signum(comparator.compare(b, a)), expectedSignum);
  }
}
