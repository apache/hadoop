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

package org.apache.hadoop.fs.gs;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Hadoop configuration property.
 */
class HadoopConfigurationProperty<T> {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopConfigurationProperty.class);

  private final String key;
  private final List<String> deprecatedKeys;
  private final T defaultValue;

  private List<String> keyPrefixes = ImmutableList.of("");

  HadoopConfigurationProperty(String key) {
    this(key, null);
  }

  HadoopConfigurationProperty(String key, T defaultValue, String... deprecatedKeys) {
    this.key = key;
    this.deprecatedKeys =
        deprecatedKeys == null ? ImmutableList.of() : ImmutableList.copyOf(deprecatedKeys);
    this.defaultValue = defaultValue;
  }

  String getKey() {
    return key;
  }

  T getDefault() {
    return defaultValue;
  }

  T get(Configuration config, BiFunction<String, T, T> getterFn) {
    String lookupKey = getLookupKey(config, key, (c, k) -> c.get(k) != null);
    return logProperty(lookupKey, getterFn.apply(lookupKey, defaultValue));
  }

  Duration getTimeDuration(Configuration config) {
    String lookupKey = getLookupKey(config, key, (c, k) -> c.get(k) != null);
    String defValStr = defaultValue == null ? null : String.valueOf(defaultValue);
    return logProperty(
        lookupKey, Duration.ofMillis(config.getTimeDuration(lookupKey, defValStr, MILLISECONDS)));
  }

  HadoopConfigurationProperty<T> withPrefixes(List<String> prefixes) {
    this.keyPrefixes = ImmutableList.copyOf(prefixes);
    return this;
  }

  RedactedString getPassword(Configuration config) {
    checkState(defaultValue == null || defaultValue instanceof String, "Not a string property");
    String lookupKey = getLookupKey(config, key, (c, k) -> c.get(k) != null);
    char[] value;
    try {
      value = config.getPassword(lookupKey);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return logProperty(
            lookupKey,
            RedactedString.create(value == null ? (String) defaultValue : String.valueOf(value)));
  }

  private String getLookupKey(Configuration config, String lookupKey,
      BiFunction<Configuration, String, Boolean> checkFn) {
    for (String prefix : keyPrefixes) {
      String prefixedKey = prefix + lookupKey;
      if (checkFn.apply(config, prefixedKey)) {
        return prefixedKey;
      }
      for (String deprecatedKey : deprecatedKeys) {
        String prefixedDeprecatedKey = prefix + deprecatedKey;
        if (checkFn.apply(config, prefixedDeprecatedKey)) {
          LOG.warn("Using deprecated key '{}', use '{}' key instead.", prefixedDeprecatedKey,
              prefixedKey);
          return prefixedDeprecatedKey;
        }
      }
    }
    return keyPrefixes.get(0) + lookupKey;
  }

  private static <S> S logProperty(String key, S value) {
    LOG.trace("{} = {}", key, value);
    return value;
  }
}
