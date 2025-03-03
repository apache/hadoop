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

package org.apache.hadoop.fs.s3a.impl.store;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.DOWNGRADE_SYNCABLE_EXCEPTIONS;
import static org.apache.hadoop.fs.s3a.Constants.DOWNGRADE_SYNCABLE_EXCEPTIONS_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CONDITIONAL_CREATE_FILES;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CONDITIONAL_CREATE_ENABLED;

/**
 * Store configuration flags.
 */
public enum StoreConfigurationFlags {

  /* When adding new flags, insert in alphabetical order */

  /**
   * Is Conditional Create available?
   */
  ConditionalCreateAvailable(FS_S3A_CONDITIONAL_CREATE_ENABLED,
      true),

  /**
   * Should Conditional Create be used
   * as the file overwrite check?
   */
  ConditionalCreateForFiles(FS_S3A_CONDITIONAL_CREATE_FILES,
      false),

  /**
   * Downgrade exception raising on syncable API use when writing a file.
   */
  DowngradeSyncableExceptions(
      DOWNGRADE_SYNCABLE_EXCEPTIONS,
      DOWNGRADE_SYNCABLE_EXCEPTIONS_DEFAULT);
  /**
   * Key name; read from the configuration, and
   * for the capability probe unless the arity 3
   * constructor is used.
   */
  private final String key;

  /**
   * Capability to probe for in {@link #hasCapability(String)}.
   */
  private final String capability;

  /**
   * Default value when reading from the configuration.
   */
  private final boolean defaultValue;

  StoreConfigurationFlags(String key, boolean defaultValue) {
    this(key, "", defaultValue);
  }

  StoreConfigurationFlags(String key,
      String capability,
      boolean defaultValue) {
    this.key = key;
    this.capability = capability;
    this.defaultValue = defaultValue;
  }

  public String getKey() {
    return key;
  }

  public String getCapability() {
    return capability;
  }

  /**
   * Read from the the configuration, falling
   * back to the default value.
   * @param conf configuration.
   * @return the evaluated value.
   */
  public boolean evaluate(Configuration conf) {
    return conf.getBoolean(key, defaultValue);
  }

  /**
   * Does this enum's key match the supplied key.
   * @param k key to probe for
   * @return true if there is a match.
   */
  public boolean keyMatches(String k) {
    return key.equals(k);
  }

  /**
   * Does this enum's capability match the supplied key?
   * @param k key to probe for
   * @return true if there is a match.
   */
  public boolean hasCapability(String k) {
    return !capability.isEmpty() &&
        capability.equals(k);
  }


}
