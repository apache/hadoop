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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.store.LogExactlyOnce;

import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_TYPE;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;

/**
 * Stream integration, including factory construction.
 */
public final class StreamIntegration {

  private static final Logger LOG_DEPRECATION =
      LoggerFactory.getLogger(
          "org.apache.hadoop.conf.Configuration.deprecation");

  /**
   * Warn once on use of prefetch boolean flag rather than enum.
   */
  private static final LogExactlyOnce WARN_PREFETCH_KEY = new LogExactlyOnce(LOG_DEPRECATION);

  /**
   * Create the input stream factory the configuration asks for.
   * This does not initialize the factory.
   * @param conf configuration
   * @return a stream factory.
   */
  public static ObjectInputStreamFactory createStreamFactory(final Configuration conf) {
    // choose the default input stream type

    // work out the default stream; this includes looking at the
    // deprecated prefetch enabled key to see if it is set.
    InputStreamType defaultStream = InputStreamType.DEFAULT_STREAM_TYPE;
    if (conf.getBoolean(PREFETCH_ENABLED_KEY, false)) {

      // prefetch enabled, warn (once) then change it to be the default.
      WARN_PREFETCH_KEY.info("Using {} is deprecated: choose the appropriate stream in {}",
          PREFETCH_ENABLED_KEY, INPUT_STREAM_TYPE);
      defaultStream = InputStreamType.Prefetch;
    }

    // retrieve the enum value, returning the configured value or
    // the default...then instantiate it.
    return conf.getEnum(INPUT_STREAM_TYPE, defaultStream)
        .factory()
        .apply(conf);
  }
}
