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

package org.apache.hadoop.fs.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.store.LogExactlyOnce;

/**
 * Class to help with use of FSBuilder.
 */
public class FSBuilderSupport {

  private static final Logger LOG =
      LoggerFactory.getLogger(FSBuilderSupport.class);

  public static final LogExactlyOnce LOG_PARSE_ERROR = new LogExactlyOnce(LOG);

  /**
   * Options which are parsed.
   */
  private final Configuration options;

  /**
   * Constructor.
   * @param options the configuration options from the builder.
   */
  public FSBuilderSupport(final Configuration options) {
    this.options = options;
  }

  public Configuration getOptions() {
    return options;
  }

  /**
   * Get a long value with resilience to unparseable values.
   * Negative values are replaced with the default.
   * @param key key to log
   * @param defVal default value
   * @return long value
   */
  public long getPositiveLong(String key, long defVal) {
    long l = getLong(key, defVal);
    if (l < 0) {
      LOG.debug("The option {} has a negative value {}, replacing with the default {}",
          key, l, defVal);
      l = defVal;
    }
    return l;
  }

  /**
   * Get a long value with resilience to unparseable values.
   * @param key key to log
   * @param defVal default value
   * @return long value
   */
  public long getLong(String key, long defVal) {
    final String v = options.getTrimmed(key, "");
    if (v.isEmpty()) {
      return defVal;
    }
    try {
      return options.getLong(key, defVal);
    } catch (NumberFormatException e) {
      final String msg = String.format(
          "The option %s value \"%s\" is not a long integer; using the default value %s",
          key, v, defVal);
      // not a long,
      LOG_PARSE_ERROR.warn(msg);
      LOG.debug("{}", msg, e);
      return defVal;
    }
  }

}
