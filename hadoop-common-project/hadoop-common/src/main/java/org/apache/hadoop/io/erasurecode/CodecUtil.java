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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.codec.ErasureCodec;
import org.apache.hadoop.io.erasurecode.codec.HHXORErasureCodec;
import org.apache.hadoop.io.erasurecode.codec.RSErasureCodec;
import org.apache.hadoop.io.erasurecode.codec.XORErasureCodec;
import org.apache.hadoop.io.erasurecode.coder.ErasureDecoder;
import org.apache.hadoop.io.erasurecode.coder.ErasureEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeXORRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawErasureCoderFactoryLegacy;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.XORRawErasureCoderFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * A codec & coder utility to help create coders conveniently.
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

  private static final Log LOG = LogFactory.getLog(CodecUtil.class);

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
  public static final String IO_ERASURECODE_CODEC_RS_LEGACY_RAWCODERS_DEFAULT =
      RSRawErasureCoderFactoryLegacy.class.getCanonicalName();
  public static final String IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY =
      IO_ERASURECODE_CODEC + "rs.rawcoders";
  public static final String IO_ERASURECODE_CODEC_RS_RAWCODERS_DEFAULT =
      NativeRSRawErasureCoderFactory.class.getCanonicalName() +
      "," + RSRawErasureCoderFactory.class.getCanonicalName();

  /** Raw coder factory for the XOR codec. */
  public static final String IO_ERASURECODE_CODEC_XOR_RAWCODERS_KEY =
      IO_ERASURECODE_CODEC + "xor.rawcoders";
  public static final String IO_ERASURECODE_CODEC_XOR_RAWCODERS_DEFAULT =
      NativeXORRawErasureCoderFactory.class.getCanonicalName() +
      "," + XORRawErasureCoderFactory.class.getCanonicalName();

  // Default coders for each codec names.
  public static final Map<String, String> DEFAULT_CODERS_MAP = ImmutableMap.of(
      "rs",         IO_ERASURECODE_CODEC_RS_RAWCODERS_DEFAULT,
      "rs-legacy",  IO_ERASURECODE_CODEC_RS_LEGACY_RAWCODERS_DEFAULT,
      "xor",        IO_ERASURECODE_CODEC_XOR_RAWCODERS_DEFAULT
  );

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
      Configuration conf, String rawCoderFactoryKey) {
    RawErasureCoderFactory fact;
    try {
      Class<? extends RawErasureCoderFactory> factClass = conf.getClassByName(
          rawCoderFactoryKey).asSubclass(RawErasureCoderFactory.class);
      fact = factClass.newInstance();
    } catch (ClassNotFoundException | InstantiationException |
        IllegalAccessException e) {
      throw new RuntimeException("Failed to create raw coder factory", e);
    }

    if (fact == null) {
      throw new RuntimeException("Failed to create raw coder factory");
    }

    return fact;
  }

  // Return comma separated coder names
  private static String getRawCoders(Configuration conf, String codec) {
    return conf.get(
      IO_ERASURECODE_CODEC + codec + ".rawcoders",
      DEFAULT_CODERS_MAP.getOrDefault(codec, codec)
    );
  }

  private static RawErasureEncoder createRawEncoderWithFallback(
      Configuration conf, String codec, ErasureCoderOptions coderOptions) {
    String coders = getRawCoders(conf, codec);
    for (String factName : Splitter.on(",").split(coders)) {
      try {
        if (factName != null) {
          RawErasureCoderFactory fact = createRawCoderFactory(conf,
              factName);
          return fact.createEncoder(coderOptions);
        }
      } catch (LinkageError | Exception e) {
        // Fallback to next coder if possible
        LOG.warn("Failed to create raw erasure encoder " + factName +
            ", fallback to next codec if possible", e);
      }
    }
    throw new IllegalArgumentException("Fail to create raw erasure " +
       "encoder with given codec: " + codec);
  }

  private static RawErasureDecoder createRawDecoderWithFallback(
          Configuration conf, String codec, ErasureCoderOptions coderOptions) {
    String coders = getRawCoders(conf, codec);
    for (String factName : Splitter.on(",").split(coders)) {
      try {
        if (factName != null) {
          RawErasureCoderFactory fact = createRawCoderFactory(conf,
              factName);
          return fact.createDecoder(coderOptions);
        }
      } catch (LinkageError | Exception e) {
        // Fallback to next coder if possible
        LOG.warn("Failed to create raw erasure decoder " + factName +
            ", fallback to next codec if possible", e);
      }
    }
    throw new IllegalArgumentException("Fail to create raw erasure " +
            "encoder with given codec: " + codec);
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
