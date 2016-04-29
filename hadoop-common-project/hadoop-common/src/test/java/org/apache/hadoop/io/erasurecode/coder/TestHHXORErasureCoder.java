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
package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.junit.Before;
import org.junit.Test;

public class TestHHXORErasureCoder extends TestHHErasureCoderBase {

  @Before
  public void setup() {
    this.encoderClass = HHXORErasureEncoder.class;
    this.decoderClass = HHXORErasureDecoder.class;
    this.numChunksInBlock = 10;
    this.subPacketSize = 2;
  }

  @Test
  public void testCodingNoDirectBuffer_10x4_erasing_d0() {
    prepare(null, 10, 4, new int[]{0}, new int[0]);
    /**
     * Doing twice to test if the coders can be repeatedly reused. This matters
     * as the underlying coding buffers are shared, which may have bugs.
     */
    testCoding(false);
    testCoding(false);
  }

  @Test
  public void testCodingDirectBufferWithConf_10x4_erasing_d0() {
    /**
     * This tests if the configuration items work or not.
     */
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.IO_ERASURECODE_CODEC_RS_DEFAULT_RAWCODER_KEY,
        RSRawErasureCoderFactory.class.getCanonicalName());
    prepare(conf, 10, 4, new int[]{0}, new int[0]);

    testCoding(true);
    testCoding(true);
  }

  @Test
  public void testCodingDirectBuffer_10x4_erasing_p1() {
    prepare(null, 10, 4, new int[]{}, new int[]{1});
    testCoding(true);
    testCoding(true);
  }

  @Test
  public void testCodingDirectBuffer_10x4_erasing_d4() {
    prepare(null, 10, 4, new int[] {4}, new int[] {});
    testCoding(true);
    testCoding(true);
  }

  @Test
  public void testCodingDirectBuffer_10x4_erasing_d0_p0() {
    prepare(null, 10, 4, new int[] {0}, new int[] {0});
    testCoding(true);
    testCoding(true);
  }

  @Test
  public void testCodingBothBuffers_10x4_erasing_d0_p0() {
    prepare(null, 10, 4, new int[] {0}, new int[] {0});

    /**
     * Doing in mixed buffer usage model to test if the coders can be repeatedly
     * reused with different buffer usage model. This matters as the underlying
     * coding buffers are shared, which may have bugs.
     */
    testCoding(true);
    testCoding(false);
    testCoding(true);
    testCoding(false);
  }

  @Test
  public void testCodingDirectBuffer_10x4_erasure_of_d2_d4_p0() {
    prepare(null, 10, 4, new int[] {2, 4}, new int[] {0});
    testCoding(true);
  }

  @Test
  public void testCodingDirectBuffer_10x4_erasing_d0_d1_p0_p1() {
    prepare(null, 10, 4, new int[] {0, 1}, new int[] {0, 1});
    testCoding(true);
  }

//  @Test
//  public void testCodingNoDirectBuffer_3x3_erasing_d0_p0() {
//    prepare(null, 3, 3, new int[] {0}, new int[] {0});
//    testCoding(false);
//  }

  @Test
  public void testCodingDirectBuffer_6x3_erasing_d0_p0() {
    prepare(null, 6, 3, new int[] {0}, new int[] {0});
    testCoding(true);
  }
}
