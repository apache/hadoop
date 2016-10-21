/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawDecoderLegacy;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoderLegacy;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the codec to raw coder mapping.
 */
public class TestCodecRawCoderMapping {

  private static Configuration conf;
  private static final int numDataUnit = 6;
  private static final int numParityUnit = 3;

  @Before
  public void setup() {
    conf = new Configuration();
  }

  @Test
  public void testRSDefaultRawCoder() {
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        numDataUnit, numParityUnit);
    // should return default raw coder of rs-default codec
    RawErasureEncoder encoder = CodecUtil.createRawEncoder(
        conf, ErasureCodeConstants.RS_DEFAULT_CODEC_NAME, coderOptions);
    Assert.assertTrue(encoder instanceof RSRawEncoder);
    RawErasureDecoder decoder = CodecUtil.createRawDecoder(
        conf, ErasureCodeConstants.RS_DEFAULT_CODEC_NAME, coderOptions);
    Assert.assertTrue(decoder instanceof RSRawDecoder);

    // should return default raw coder of rs-legacy codec
    encoder = CodecUtil.createRawEncoder(conf,
        ErasureCodeConstants.RS_LEGACY_CODEC_NAME, coderOptions);
    Assert.assertTrue(encoder instanceof RSRawEncoderLegacy);
    decoder = CodecUtil.createRawDecoder(conf,
        ErasureCodeConstants.RS_LEGACY_CODEC_NAME, coderOptions);
    Assert.assertTrue(decoder instanceof RSRawDecoderLegacy);
  }

  @Test
  public void testDedicatedRawCoderKey() {
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        numDataUnit, numParityUnit);

    String dummyFactName = "DummyNoneExistingFactory";
    // set the dummy factory to rs-legacy and create a raw coder
    // with rs-default, which is OK as the raw coder key is not used
    conf.set(CodecUtil.
        IO_ERASURECODE_CODEC_RS_LEGACY_RAWCODER_KEY, dummyFactName);
    RawErasureEncoder encoder = CodecUtil.createRawEncoder(conf,
        ErasureCodeConstants.RS_DEFAULT_CODEC_NAME, coderOptions);
    Assert.assertTrue(encoder instanceof RSRawEncoder);
    // now create the raw coder with rs-legacy, which should throw exception
    try {
      CodecUtil.createRawEncoder(conf,
          ErasureCodeConstants.RS_LEGACY_CODEC_NAME, coderOptions);
      Assert.fail();
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains("Failed to create raw coder", e);
    }
  }
}
