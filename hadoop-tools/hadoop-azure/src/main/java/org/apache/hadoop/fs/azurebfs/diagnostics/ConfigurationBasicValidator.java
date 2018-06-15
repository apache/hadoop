/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.diagnostics;

import org.apache.hadoop.fs.azurebfs.contracts.diagnostics.ConfigurationValidator;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;

/**
 * ConfigurationBasicValidator covers the base case of missing user defined configuration value
 * @param <T> the type of the validated value
 */
abstract class ConfigurationBasicValidator<T> implements ConfigurationValidator {
  private final T defaultVal;
  private final String configKey;
  private final boolean throwIfInvalid;

  ConfigurationBasicValidator(final String configKey, final T defaultVal, final boolean throwIfInvalid) {
    this.configKey = configKey;
    this.defaultVal = defaultVal;
    this.throwIfInvalid = throwIfInvalid;
  }

  /**
   * This method handles the base case where the configValue is null, based on the throwIfInvalid it either throws or returns the defaultVal,
   * otherwise it returns null indicating that the configValue needs to be validated further
   * @param configValue the configuration value set by the user
   * @return the defaultVal in case the configValue is null and not required to be set, null in case the configValue not null
   * @throws InvalidConfigurationValueException in case the configValue is null and required to be set
   */
  public T validate(final String configValue) throws InvalidConfigurationValueException {
    if (configValue == null) {
      if (this.throwIfInvalid) {
        throw new InvalidConfigurationValueException(this.configKey);
      }
      return this.defaultVal;
    }
    return null;
  }

  public T getDefaultVal() {
    return this.defaultVal;
  }

  public String getConfigKey() {
    return this.configKey;
  }

  public boolean getThrowIfInvalid() {
    return this.throwIfInvalid;
  }
}
