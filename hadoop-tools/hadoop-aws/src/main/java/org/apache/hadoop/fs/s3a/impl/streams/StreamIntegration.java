/*
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

package org.apache.hadoop.fs.s3a.impl.streams;

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ConfigurationHelper;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.VectoredIOContext;
import org.apache.hadoop.fs.store.LogExactlyOnce;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_VECTOR_ACTIVE_RANGE_READS;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_VECTOR_READS_MIN_SEEK_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_AWS_S3_VECTOR_ACTIVE_RANGE_READS;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_AWS_S3_VECTOR_READS_MIN_SEEK_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_CUSTOM_FACTORY;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_TYPE;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.fs.s3a.S3AUtils.intOption;
import static org.apache.hadoop.fs.s3a.S3AUtils.longBytesOption;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Stream integration, including factory construction.
 */
public final class StreamIntegration {

  /**
   * Enum/config name of a classic S3AInputStream: {@value}.
   */
  public static final String CLASSIC = "classic";

  /**
   * Enum/config name of of the Pinterest S3APrefetchingInputStream: {@value}.
   */
  public static final String PREFETCH = "prefetch";

  /**
   * Enum/config name of the analytics input stream: {@value}.
   */
  public static final String ANALYTICS = "analytics";

  /**
   * Reads in a classname : {@value}.
   */
  public static final String CUSTOM = "custom";

  /**
   * Special string for configuration only; is
   * mapped to the default stream type: {@value}.
   */
  public static final String DEFAULT = "default";

  /**
   * What is the default type?
   */
  public static final InputStreamType DEFAULT_STREAM_TYPE = InputStreamType.Classic;

  /**
   * Configuration deprecation log for warning about use of the
   * now deprecated {@code "fs.s3a.prefetch.enabled"} option..
   */
  private static final Logger LOG_DEPRECATION =
      LoggerFactory.getLogger(
          "org.apache.hadoop.conf.Configuration.deprecation");

  /**
   * Warn once on use of prefetch configuration option.
   */
  private static final LogExactlyOnce WARN_PREFETCH_KEY = new LogExactlyOnce(LOG_DEPRECATION);

  public static final String E_EMPTY_CUSTOM_CLASSNAME =
      "Configuration option " + INPUT_STREAM_CUSTOM_FACTORY
          + " is required when the input stream type is \"custom\"";

  public static final String E_INVALID_STREAM_TYPE = "Invalid stream type:";

  private StreamIntegration() {
  }

  /**
   * Create the input stream factory the configuration asks for.
   * <p>
   * This does not initialize the factory.
   * <p>
   * See {@link #determineInputStreamType(Configuration)} for the
   * resolution algorithm.
   * @param conf configuration
   * @return a stream factory.
   * @throws RuntimeException any binding/loading/instantiation problem
   */
  public static ObjectInputStreamFactory factoryFromConfig(final Configuration conf) {

    // Construct the factory.
    return determineInputStreamType(conf)
        .factory()
        .apply(conf);
  }

  /**
   * Determine the input stream type for the supplied configuration.
   * <p>
   * This does not perform any instantiation.
   * <p>
   * If the option {@code "fs.s3a.prefetch.enabled"} is set, the
   * prefetch stream is selected, after printing a
   * warning the first time this happens.
   * <p>
   * If the input stream type is declared as "default", then whatever
   * the current default stream type is returned, as defined by
   * {@link #DEFAULT_STREAM_TYPE}.
   * @param conf configuration
   * @return a stream factory.
   */
  static InputStreamType determineInputStreamType(final Configuration conf) {
    // work out the default stream; this includes looking at the
    // deprecated prefetch enabled key to see if it is set.
    if (conf.getBoolean(PREFETCH_ENABLED_KEY, false)) {
      // prefetch enabled, warn (once) then change it to be the default.
      WARN_PREFETCH_KEY.info("Using {} is deprecated: choose the appropriate stream in {}",
          PREFETCH_ENABLED_KEY, INPUT_STREAM_TYPE);
      return InputStreamType.Prefetch;
    }

    // retrieve the enum value, returning the configured value or
    // the (calculated) default
    return ConfigurationHelper.resolveEnum(conf,
        INPUT_STREAM_TYPE,
        InputStreamType.class,
        s -> {
          if (isEmpty(s) || DEFAULT.equalsIgnoreCase(s)) {
            // return default type.
            return DEFAULT_STREAM_TYPE;
          } else {
            // any other value
            throw new IllegalArgumentException(E_INVALID_STREAM_TYPE
                + " \"" + s + "\"");
          }
        });
  }

  /**
   * Load the input stream factory defined in the option
   * {@link Constants#INPUT_STREAM_CUSTOM_FACTORY}.
   * @param conf configuration to use
   * @return the custom factory
   * @throws RuntimeException any binding/loading/instantiation problem
   */
  static ObjectInputStreamFactory loadCustomFactory(Configuration conf) {

    // make sure the classname option is actually set
    final String name = conf.getTrimmed(INPUT_STREAM_CUSTOM_FACTORY, "");
    checkArgument(!isEmpty(name), E_EMPTY_CUSTOM_CLASSNAME);

    final Class<? extends ObjectInputStreamFactory> factoryClass =
        conf.getClass(INPUT_STREAM_CUSTOM_FACTORY,
            null,
            ObjectInputStreamFactory.class);

    try {
      final Constructor<? extends ObjectInputStreamFactory> ctor =
          factoryClass.getConstructor();
      return ctor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate custom class "
          + name + " " + e, e);
    }
  }

  /**
   * Populates the configurations related to vectored IO operations.
   * The context is still mutable at this point.
   * @param conf configuration object.
   * @return VectoredIOContext.
   */
  public static VectoredIOContext populateVectoredIOContext(Configuration conf) {
    final int minSeekVectored = (int) longBytesOption(conf, AWS_S3_VECTOR_READS_MIN_SEEK_SIZE,
        DEFAULT_AWS_S3_VECTOR_READS_MIN_SEEK_SIZE, 0);
    final int maxReadSizeVectored =
        (int) longBytesOption(conf, AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE,
            DEFAULT_AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE, 0);
    final int vectoredActiveRangeReads = intOption(conf,
        AWS_S3_VECTOR_ACTIVE_RANGE_READS, DEFAULT_AWS_S3_VECTOR_ACTIVE_RANGE_READS, 1);
    return new VectoredIOContext()
        .setMinSeekForVectoredReads(minSeekVectored)
        .setMaxReadSizeForVectoredReads(maxReadSizeVectored)
        .setVectoredActiveRangeReads(vectoredActiveRangeReads);
  }

}
