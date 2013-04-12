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

package org.apache.hadoop.metrics2.impl;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;

/**
 * Helper class for building configs, mostly used in tests
 */
public class ConfigBuilder {
  /** The built config */
  public final PropertiesConfiguration config;

  /**
   * Default constructor
   */
  public ConfigBuilder() {
    config = new PropertiesConfiguration();
  }

  /**
   * Add a property to the config
   * @param key of the property
   * @param value of the property
   * @return self
   */
  public ConfigBuilder add(String key, Object value) {
    config.addProperty(key, value);
    return this;
  }

  /**
   * Save the config to a file
   * @param filename  to save
   * @return self
   * @throws RuntimeException
   */
  public ConfigBuilder save(String filename) {
    try {
      config.save(filename);
    }
    catch (Exception e) {
      throw new RuntimeException("Error saving config", e);
    }
    return this;
  }

  /**
   * Return a subset configuration (so getParent() can be used.)
   * @param prefix  of the subset
   * @return the subset config
   */
  public SubsetConfiguration subset(String prefix) {
    return new SubsetConfiguration(config, prefix, ".");
  }
}

