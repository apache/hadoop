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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Map;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;

/**
 * Prefix lookups over the raw (unexpanded) property values of a
 * {@link ConfigSnapshot}. The prefix index itself lives in the snapshot; this
 * class keeps the historical API and its raw-value semantics for the readers
 * that use it.
 */
public class ConfigurationProperties {
  private final ConfigSnapshot snapshot;

  /**
   * A constructor defined in order to conform to the type used by
   * {@code Configuration}. It must only be called by String keys and values.
   * @param props properties to store
   */
  public ConfigurationProperties(Map<String, String> props) {
    this(ConfigSnapshot.of(props));
  }

  ConfigurationProperties(ConfigSnapshot snapshot) {
    this.snapshot = snapshot;
  }

  /**
   * Filters all properties by a prefix. The property keys are trimmed by the
   * given prefix.
   * @param prefix prefix to filter property keys
   * @return properties matching given prefix
   */
  public Map<String, String> getPropertiesWithPrefix(String prefix) {
    return getPropertiesWithPrefix(prefix, false);
  }

  /**
   * Filters all properties by a prefix.
   * @param prefix prefix to filter property keys
   * @param fullyQualifiedKey whether collected property keys are to be trimmed
   *                          by the prefix, or must be kept as it is
   * @return properties matching given prefix
   */
  public Map<String, String> getPropertiesWithPrefix(
      String prefix, boolean fullyQualifiedKey) {
    return snapshot.getRawPropertiesWithPrefix(prefix, fullyQualifiedKey);
  }
}
