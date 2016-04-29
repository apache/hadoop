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
package org.apache.hadoop.io.erasurecode;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureCoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

/**
 * A codec & coder utility to help create raw coders conveniently.
 */
@InterfaceAudience.Private
public final class CodecUtil {

  private CodecUtil() { }

  /**
   * Create RS raw encoder according to configuration.
   * @param conf configuration possibly with some items to configure the coder
   * @param numDataUnits number of data units in a coding group
   * @param numParityUnits number of parity units in a coding group
   * @param codec the codec to use. If null, will use the default codec
   * @return raw encoder
   */
  public static RawErasureEncoder createRSRawEncoder(
      Configuration conf, int numDataUnits, int numParityUnits, String codec) {
    Preconditions.checkNotNull(conf);
    if (codec == null) {
      codec = ErasureCodeConstants.RS_DEFAULT_CODEC_NAME;
    }
    RawErasureCoder rawCoder = createRawCoder(conf,
        getFactNameFromCodec(conf, codec), true, numDataUnits, numParityUnits);
    return (RawErasureEncoder) rawCoder;
  }

  /**
   * Create RS raw encoder using the default codec.
   */
  public static RawErasureEncoder createRSRawEncoder(
      Configuration conf, int numDataUnits, int numParityUnits) {
    return createRSRawEncoder(conf, numDataUnits, numParityUnits, null);
  }

  /**
   * Create RS raw decoder according to configuration.
   * @param conf configuration possibly with some items to configure the coder
   * @param numDataUnits number of data units in a coding group
   * @param numParityUnits number of parity units in a coding group
   * @param codec the codec to use. If null, will use the default codec
   * @return raw decoder
   */
  public static RawErasureDecoder createRSRawDecoder(
      Configuration conf, int numDataUnits, int numParityUnits, String codec) {
    Preconditions.checkNotNull(conf);
    if (codec == null) {
      codec = ErasureCodeConstants.RS_DEFAULT_CODEC_NAME;
    }
    RawErasureCoder rawCoder = createRawCoder(conf,
        getFactNameFromCodec(conf, codec), false, numDataUnits, numParityUnits);
    return (RawErasureDecoder) rawCoder;
  }

  /**
   * Create RS raw decoder using the default codec.
   */
  public static RawErasureDecoder createRSRawDecoder(
      Configuration conf, int numDataUnits, int numParityUnits) {
    return createRSRawDecoder(conf, numDataUnits, numParityUnits, null);
  }

  /**
   * Create XOR raw encoder according to configuration.
   * @param conf configuration possibly with some items to configure the coder
   * @param numDataUnits number of data units in a coding group
   * @param numParityUnits number of parity units in a coding group
   * @return raw encoder
   */
  public static RawErasureEncoder createXORRawEncoder(
      Configuration conf, int numDataUnits, int numParityUnits) {
    Preconditions.checkNotNull(conf);
    RawErasureCoder rawCoder = createRawCoder(conf,
        getFactNameFromCodec(conf, ErasureCodeConstants.XOR_CODEC_NAME),
        true, numDataUnits, numParityUnits);
    return (RawErasureEncoder) rawCoder;
  }

  /**
   * Create XOR raw decoder according to configuration.
   * @param conf configuration possibly with some items to configure the coder
   * @param numDataUnits number of data units in a coding group
   * @param numParityUnits number of parity units in a coding group
   * @return raw decoder
   */
  public static RawErasureDecoder createXORRawDecoder(
      Configuration conf, int numDataUnits, int numParityUnits) {
    Preconditions.checkNotNull(conf);
    RawErasureCoder rawCoder = createRawCoder(conf,
        getFactNameFromCodec(conf, ErasureCodeConstants.XOR_CODEC_NAME),
        false, numDataUnits, numParityUnits);
    return (RawErasureDecoder) rawCoder;
  }

  /**
   * Create raw coder using specified conf and raw coder factory key.
   * @param conf configuration possibly with some items to configure the coder
   * @param rawCoderFactory name of the raw coder factory
   * @param isEncoder is encoder or not we're going to create
   * @param numDataUnits number of data units in a coding group
   * @param numParityUnits number of parity units in a coding group
   * @return raw coder
   */
  public static RawErasureCoder createRawCoder(Configuration conf,
      String rawCoderFactory, boolean isEncoder, int numDataUnits,
                                               int numParityUnits) {

    RawErasureCoderFactory fact;
    try {
      Class<? extends RawErasureCoderFactory> factClass = conf.getClassByName(
          rawCoderFactory).asSubclass(RawErasureCoderFactory.class);
      fact = factClass.newInstance();
    } catch (ClassNotFoundException | InstantiationException |
        IllegalAccessException e) {
      throw new RuntimeException("Failed to create raw coder", e);
    }

    return isEncoder ? fact.createEncoder(numDataUnits, numParityUnits) :
            fact.createDecoder(numDataUnits, numParityUnits);
  }

  private static String getFactNameFromCodec(Configuration conf, String codec) {
    switch (codec) {
    case ErasureCodeConstants.RS_DEFAULT_CODEC_NAME:
      return conf.get(
          CommonConfigurationKeys.IO_ERASURECODE_CODEC_RS_DEFAULT_RAWCODER_KEY,
          CommonConfigurationKeys.
              IO_ERASURECODE_CODEC_RS_DEFAULT_RAWCODER_DEFAULT);
    case ErasureCodeConstants.RS_LEGACY_CODEC_NAME:
      return conf.get(
          CommonConfigurationKeys.IO_ERASURECODE_CODEC_RS_LEGACY_RAWCODER_KEY,
          CommonConfigurationKeys.
              IO_ERASURECODE_CODEC_RS_LEGACY_RAWCODER_DEFAULT);
    case ErasureCodeConstants.XOR_CODEC_NAME:
      return conf.get(
          CommonConfigurationKeys.IO_ERASURECODE_CODEC_XOR_RAWCODER_KEY,
          CommonConfigurationKeys.IO_ERASURECODE_CODEC_XOR_RAWCODER_DEFAULT);
    default:
      // For custom codec, we throw exception if the factory is not configured
      String rawCoderKey = "io.erasurecode.codec." + codec + ".rawcoder";
      String factName = conf.get(rawCoderKey);
      if (factName == null) {
        throw new IllegalArgumentException("Raw coder factory not configured " +
            "for custom codec " + codec);
      }
      return factName;
    }
  }
}
