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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.codec.ErasureCodec;
import org.apache.hadoop.io.erasurecode.codec.HHXORErasureCodec;
import org.apache.hadoop.io.erasurecode.codec.RSErasureCodec;
import org.apache.hadoop.io.erasurecode.codec.XORErasureCodec;
import org.apache.hadoop.io.erasurecode.coder.ErasureDecoder;
import org.apache.hadoop.io.erasurecode.coder.ErasureEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * A codec &amp; coder utility to help create coders conveniently.
 *
 * {@link CodecUtil} includes erasure coder configurations key and default
 * values such as coder class name and erasure codec option values included
 * by {@link ErasureCodecOptions}. {@link ErasureEncoder} and
 * {@link ErasureDecoder} are created by createEncoder and createDecoder
 * respectively.{@link RawErasureEncoder} and {@link RawErasureDecoder} are
 * are created by createRawEncoder and createRawDecoder.
 */
@InterfaceAudience.Private
public final class CodecUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CodecUtil.class);

  public static final String IO_ERASURECODE_CODEC = "io.erasurecode.codec.";

  /** Erasure coder XOR codec. */
  public static final String IO_ERASURECODE_CODEC_XOR_KEY =
      IO_ERASURECODE_CODEC + "xor";
  public static final String IO_ERASURECODE_CODEC_XOR =
      XORErasureCodec.class.getCanonicalName();
  /** Erasure coder Reed-Solomon codec. */
  public static final String IO_ERASURECODE_CODEC_RS_KEY =
      IO_ERASURECODE_CODEC + "rs";
  public static final String IO_ERASURECODE_CODEC_RS =
      RSErasureCodec.class.getCanonicalName();
  /** Erasure coder hitch hiker XOR codec. */
  public static final String IO_ERASURECODE_CODEC_HHXOR_KEY =
      IO_ERASURECODE_CODEC + "hhxor";
  public static final String IO_ERASURECODE_CODEC_HHXOR =
      HHXORErasureCodec.class.getCanonicalName();

  /** Comma separated raw codec name. The first coder is prior to the latter. */
  public static final String IO_ERASURECODE_CODEC_RS_LEGACY_RAWCODERS_KEY =
      IO_ERASURECODE_CODEC + "rs-legacy.rawcoders";
  public static final String IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY =
      IO_ERASURECODE_CODEC + "rs.rawcoders";

  /** Raw coder factory for the XOR codec. */
  public static final String IO_ERASURECODE_CODEC_XOR_RAWCODERS_KEY =
      IO_ERASURECODE_CODEC + "xor.rawcoders";

  private CodecUtil() { }

  /**
   * Create encoder corresponding to given codec.
   * @param options Erasure codec options
   * @return erasure encoder
   */
  public static ErasureEncoder createEncoder(Configuration conf,
      ErasureCodecOptions options) {
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(options);

    String codecKey = getCodecClassName(conf,
        options.getSchema().getCodecName());

    ErasureCodec codec = createCodec(conf, codecKey, options);
    return codec.createEncoder();
  }

  /**
   * Create decoder corresponding to given codec.
   * @param options Erasure codec options
   * @return erasure decoder
   */
  public static ErasureDecoder createDecoder(Configuration conf,
      ErasureCodecOptions options) {
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(options);

    String codecKey = getCodecClassName(conf,
        options.getSchema().getCodecName());

    ErasureCodec codec = createCodec(conf, codecKey, options);
    return codec.createDecoder();
  }

  /**
   * Create RS raw encoder according to configuration.
   * @param conf configuration
   * @param coderOptions coder options that's used to create the coder
   * @param codec the codec to use. If null, will use the default codec
   * @return raw encoder
   */
  public static RawErasureEncoder createRawEncoder(
      Configuration conf, String codec, ErasureCoderOptions coderOptions) {
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(codec);

    return createRawEncoderWithFallback(conf, codec, coderOptions);
  }

  /**
   * Create RS raw decoder according to configuration.
   * @param conf configuration
   * @param coderOptions coder options that's used to create the coder
   * @param codec the codec to use. If null, will use the default codec
   * @return raw decoder
   */
  public static RawErasureDecoder createRawDecoder(
      Configuration conf, String codec, ErasureCoderOptions coderOptions) {
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(codec);

    return createRawDecoderWithFallback(conf, codec, coderOptions);
  }

  private static RawErasureCoderFactory createRawCoderFactory(
      String coderName, String codecName) {
    RawErasureCoderFactory fact;
    fact = CodecRegistry.getInstance().
            getCoderByName(codecName, coderName);

    return fact;
  }

  public static boolean hasCodec(String codecName) {
    return (CodecRegistry.getInstance().getCoderNames(codecName) != null);
  }

  // Return a list of coder names
  private static String[] getRawCoderNames(
      Configuration conf, String codecName) {
    return conf.getStrings(
      IO_ERASURECODE_CODEC + codecName + ".rawcoders",
      CodecRegistry.getInstance().getCoderNames(codecName)
    );
  }

  private static RawErasureEncoder createRawEncoderWithFallback(
      Configuration conf, String codecName, ErasureCoderOptions coderOptions) {
    String[] rawCoderNames = getRawCoderNames(conf, codecName);
    for (String rawCoderName : rawCoderNames) {
      try {
        if (rawCoderName != null) {
          RawErasureCoderFactory fact = createRawCoderFactory(
              rawCoderName, codecName);
          return fact.createEncoder(coderOptions);
        }
      } catch (LinkageError | Exception e) {
        // Fallback to next coder if possible
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to create raw erasure encoder " + rawCoderName +
              ", fallback to next codec if possible", e);
        }
      }
    }
    throw new IllegalArgumentException("Fail to create raw erasure " +
       "encoder with given codec: " + codecName);
  }

  private static RawErasureDecoder createRawDecoderWithFallback(
      Configuration conf, String codecName, ErasureCoderOptions coderOptions) {
    String[] coders = getRawCoderNames(conf, codecName);
    for (String rawCoderName : coders) {
      try {
        if (rawCoderName != null) {
          RawErasureCoderFactory fact = createRawCoderFactory(
              rawCoderName, codecName);
          return fact.createDecoder(coderOptions);
        }
      } catch (LinkageError | Exception e) {
        // Fallback to next coder if possible
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to create raw erasure decoder " + rawCoderName +
                  ", fallback to next codec if possible", e);
        }
      }
    }
    throw new IllegalArgumentException("Fail to create raw erasure " +
        "decoder with given codec: " + codecName);
  }

  private static ErasureCodec createCodec(Configuration conf,
      String codecClassName, ErasureCodecOptions options) {
    ErasureCodec codec = null;
    try {
      Class<? extends ErasureCodec> codecClass =
              conf.getClassByName(codecClassName)
              .asSubclass(ErasureCodec.class);
      Constructor<? extends ErasureCodec> constructor
          = codecClass.getConstructor(Configuration.class,
          ErasureCodecOptions.class);
      codec = constructor.newInstance(conf, options);
    } catch (ClassNotFoundException | InstantiationException |
            IllegalAccessException | NoSuchMethodException |
            InvocationTargetException e) {
      throw new RuntimeException("Failed to create erasure codec", e);
    }

    if (codec == null) {
      throw new RuntimeException("Failed to create erasure codec");
    }

    return codec;
  }

  private static String getCodecClassName(Configuration conf, String codec) {
    switch (codec) {
    case ErasureCodeConstants.RS_CODEC_NAME:
      return conf.get(
          CodecUtil.IO_ERASURECODE_CODEC_RS_KEY,
          CodecUtil.IO_ERASURECODE_CODEC_RS);
    case ErasureCodeConstants.RS_LEGACY_CODEC_NAME:
      //TODO:rs-legacy should be handled differently.
      return conf.get(
          CodecUtil.IO_ERASURECODE_CODEC_RS_KEY,
          CodecUtil.IO_ERASURECODE_CODEC_RS);
    case ErasureCodeConstants.XOR_CODEC_NAME:
      return conf.get(
          CodecUtil.IO_ERASURECODE_CODEC_XOR_KEY,
          CodecUtil.IO_ERASURECODE_CODEC_XOR);
    case ErasureCodeConstants.HHXOR_CODEC_NAME:
      return conf.get(
          CodecUtil.IO_ERASURECODE_CODEC_HHXOR_KEY,
          CodecUtil.IO_ERASURECODE_CODEC_HHXOR);
    default:
      // For custom codec, we throw exception if the factory is not configured
      String codecKey = "io.erasurecode.codec." + codec + ".coder";
      String codecClass = conf.get(codecKey);
      if (codecClass == null) {
        throw new IllegalArgumentException("Codec not configured " +
                "for custom codec " + codec);
      }
      return codecClass;
    }
  }
}
