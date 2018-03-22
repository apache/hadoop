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
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RSLegacyRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RSLegacyRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.XORRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.XORRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.XORRawErasureCoderFactory;
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
    // should return default raw coder of rs codec
    RawErasureEncoder encoder = CodecUtil.createRawEncoder(
        conf, ErasureCodeConstants.RS_CODEC_NAME, coderOptions);
    RawErasureDecoder decoder = CodecUtil.createRawDecoder(
        conf, ErasureCodeConstants.RS_CODEC_NAME, coderOptions);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      Assert.assertTrue(encoder instanceof NativeRSRawEncoder);
      Assert.assertTrue(decoder instanceof NativeRSRawDecoder);
    } else {
      Assert.assertTrue(encoder instanceof RSRawEncoder);
      Assert.assertTrue(decoder instanceof RSRawDecoder);
    }

    // should return default raw coder of rs-legacy codec
    encoder = CodecUtil.createRawEncoder(conf,
        ErasureCodeConstants.RS_LEGACY_CODEC_NAME, coderOptions);
    Assert.assertTrue(encoder instanceof RSLegacyRawEncoder);
    decoder = CodecUtil.createRawDecoder(conf,
        ErasureCodeConstants.RS_LEGACY_CODEC_NAME, coderOptions);
    Assert.assertTrue(decoder instanceof RSLegacyRawDecoder);
  }

  @Test
  public void testDedicatedRawCoderKey() {
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        numDataUnit, numParityUnit);

    String dummyFactName = "DummyNoneExistingFactory";
    // set the dummy factory to raw coders then fail to create any rs raw coder.
    conf.set(CodecUtil.
        IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY, dummyFactName);
    try {
      CodecUtil.createRawEncoder(conf,
          ErasureCodeConstants.RS_CODEC_NAME, coderOptions);
      Assert.fail();
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(
          "Fail to create raw erasure encoder with given codec: rs", e);
    }

    // now create the raw coder with rs-legacy, which should throw exception
    conf.set(CodecUtil.
        IO_ERASURECODE_CODEC_RS_LEGACY_RAWCODERS_KEY, dummyFactName);
    try {
      CodecUtil.createRawEncoder(conf,
          ErasureCodeConstants.RS_LEGACY_CODEC_NAME, coderOptions);
      Assert.fail();
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(
          "Fail to create raw erasure encoder with given codec: rs", e);
    }
  }

  @Test
  public void testFallbackCoders() {
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
            numDataUnit, numParityUnit);
    conf.set(CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
        RSRawErasureCoderFactory.CODER_NAME +
        "," + NativeRSRawErasureCoderFactory.CODER_NAME);
    // should return default raw coder of rs codec
    RawErasureEncoder encoder = CodecUtil.createRawEncoder(
            conf, ErasureCodeConstants.RS_CODEC_NAME, coderOptions);
    Assert.assertTrue(encoder instanceof RSRawEncoder);
    RawErasureDecoder decoder = CodecUtil.createRawDecoder(
            conf, ErasureCodeConstants.RS_CODEC_NAME, coderOptions);
    Assert.assertTrue(decoder instanceof RSRawDecoder);
  }

  @Test
  public void testLegacyCodecFallback() {
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
            numDataUnit, numParityUnit);
    // should return default raw coder of rs-legacy codec
    RawErasureEncoder encoder = CodecUtil.createRawEncoder(
            conf, ErasureCodeConstants.RS_LEGACY_CODEC_NAME, coderOptions);
    Assert.assertTrue(encoder instanceof RSLegacyRawEncoder);
    RawErasureDecoder decoder = CodecUtil.createRawDecoder(
            conf, ErasureCodeConstants.RS_LEGACY_CODEC_NAME, coderOptions);
    Assert.assertTrue(decoder instanceof RSLegacyRawDecoder);
  }

  @Test
  public void testIgnoreInvalidCodec() {
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
            numDataUnit, numParityUnit);
    conf.set(CodecUtil.IO_ERASURECODE_CODEC_XOR_RAWCODERS_KEY,
        "invalid-codec," + XORRawErasureCoderFactory.CODER_NAME);
    // should return second coder specified by IO_ERASURECODE_CODEC_CODERS
    RawErasureEncoder encoder = CodecUtil.createRawEncoder(
            conf, ErasureCodeConstants.XOR_CODEC_NAME, coderOptions);
    Assert.assertTrue(encoder instanceof XORRawEncoder);
    RawErasureDecoder decoder = CodecUtil.createRawDecoder(
            conf, ErasureCodeConstants.XOR_CODEC_NAME, coderOptions);
    Assert.assertTrue(decoder instanceof XORRawDecoder);
  }
}
