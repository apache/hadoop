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
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Test native raw Reed-solomon encoding and decoding.
 */
public class TestNativeRSRawCoder extends TestRSRawCoderBase {

  @Before
  public void setup() {
    Assume.assumeTrue(ErasureCodeNative.isNativeCodeLoaded());
    this.encoderClass = NativeRSRawEncoder.class;
    this.decoderClass = NativeRSRawDecoder.class;
    setAllowDump(true);
  }

  @Test
  public void testCoding_6x3_erasing_all_d() {
    prepare(null, 6, 3, new int[]{0, 1, 2}, new int[0], true);
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding_6x3_erasing_d0_d2() {
    prepare(null, 6, 3, new int[] {0, 2}, new int[]{});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding_6x3_erasing_d0() {
    prepare(null, 6, 3, new int[]{0}, new int[0]);
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding_6x3_erasing_d2() {
    prepare(null, 6, 3, new int[]{2}, new int[]{});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding_6x3_erasing_d0_p0() {
    prepare(null, 6, 3, new int[]{0}, new int[]{0});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding_6x3_erasing_all_p() {
    prepare(null, 6, 3, new int[0], new int[]{0, 1, 2});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding_6x3_erasing_p0() {
    prepare(null, 6, 3, new int[0], new int[]{0});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding_6x3_erasing_p2() {
    prepare(null, 6, 3, new int[0], new int[]{2});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding_6x3_erasure_p0_p2() {
    prepare(null, 6, 3, new int[0], new int[]{0, 2});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding_6x3_erasing_d0_p0_p1() {
    prepare(null, 6, 3, new int[]{0}, new int[]{0, 1});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding_6x3_erasing_d0_d2_p2() {
    prepare(null, 6, 3, new int[]{0, 2}, new int[]{2});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCodingNegative_6x3_erasing_d2_d4() {
    prepare(null, 6, 3, new int[]{2, 4}, new int[0]);
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCodingNegative_6x3_erasing_too_many() {
    prepare(null, 6, 3, new int[]{2, 4}, new int[]{0, 1});
    testCodingWithErasingTooMany();
  }

  @Test
  public void testCoding_10x4_erasing_d0_p0() {
    prepare(null, 10, 4, new int[] {0}, new int[] {0});
    testCodingDoMixAndTwice();
  }
}
