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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.diagnostics.ConfigurationValidator;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;

/**
 * Integer configuration value Validator.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IntegerConfigurationBasicValidator extends ConfigurationBasicValidator<Integer> implements ConfigurationValidator {
  private final int min;
  private final int max;
  private final int outlier;

  public IntegerConfigurationBasicValidator(final int min, final int max, final int defaultVal, final String configKey, final boolean throwIfInvalid) {
    this(min, min, max, defaultVal, configKey, throwIfInvalid);
  }

  public IntegerConfigurationBasicValidator(final int outlier, final int min, final int max,
      final int defaultVal, final String configKey, final boolean throwIfInvalid) {
    super(configKey, defaultVal, throwIfInvalid);
    this.min = min;
    this.max = max;
    this.outlier = outlier;
  }

  public Integer validate(final String configValue) throws InvalidConfigurationValueException {
    Integer result = super.validate(configValue);
    if (result != null) {
      return result;
    }

    try {
      result = Integer.parseInt(configValue);
      // throw an exception if a 'within bounds' value is missing
      if (getThrowIfInvalid() && (result != outlier) && (result < this.min || result > this.max)) {
        throw new InvalidConfigurationValueException(getConfigKey());
      }

      if (result == outlier) {
        return result;
      }

      // set the value to the nearest bound if it's out of bounds
      if (result < this.min) {
        return this.min;
      }

      if (result > this.max) {
        return this.max;
      }
    } catch (NumberFormatException ex) {
      throw new InvalidConfigurationValueException(getConfigKey(), ex);
    }

    return result;
  }
}
