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

package org.apache.hadoop.conf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs access to {@link Configuration}.
 * Sensitive data will be redacted.
 */
@InterfaceAudience.Private
public class ConfigurationWithLogging extends Configuration {
  private static final Logger LOG =
      LoggerFactory.getLogger(ConfigurationWithLogging.class);

  private final Logger log;
  private final ConfigRedactor redactor;

  public ConfigurationWithLogging(Configuration conf) {
    super(conf);
    log = LOG;
    redactor = new ConfigRedactor(conf);
  }

  /**
   * @see Configuration#get(String).
   */
  @Override
  public String get(String name) {
    String value = super.get(name);
    log.info("Got {} = '{}'", name, redactor.redact(name, value));
    return value;
  }

  /**
   * @see Configuration#get(String, String).
   */
  @Override
  public String get(String name, String defaultValue) {
    String value = super.get(name, defaultValue);
    log.info("Got {} = '{}' (default '{}')", name,
        redactor.redact(name, value), redactor.redact(name, defaultValue));
    return value;
  }

  /**
   * @see Configuration#getBoolean(String, boolean).
   */
  @Override
  public boolean getBoolean(String name, boolean defaultValue) {
    boolean value = super.getBoolean(name, defaultValue);
    log.info("Got {} = '{}' (default '{}')", name, value, defaultValue);
    return value;
  }

  /**
   * @see Configuration#getFloat(String, float).
   */
  @Override
  public float getFloat(String name, float defaultValue) {
    float value = super.getFloat(name, defaultValue);
    log.info("Got {} = '{}' (default '{}')", name, value, defaultValue);
    return value;
  }

  /**
   * @see Configuration#getInt(String, int).
   */
  @Override
  public int getInt(String name, int defaultValue) {
    int value = super.getInt(name, defaultValue);
    log.info("Got {} = '{}' (default '{}')", name, value, defaultValue);
    return value;
  }

  /**
   * @see Configuration#getLong(String, long).
   */
  @Override
  public long getLong(String name, long defaultValue) {
    long value = super.getLong(name, defaultValue);
    log.info("Got {} = '{}' (default '{}')", name, value, defaultValue);
    return value;
  }

  /**
   * @see Configuration#set(String, String, String).
   */
  @Override
  public void set(String name, String value, String source) {
    log.info("Set {} to '{}'{}", name, redactor.redact(name, value),
        source == null ? "" : " from " + source);
    super.set(name, value, source);
  }
}
