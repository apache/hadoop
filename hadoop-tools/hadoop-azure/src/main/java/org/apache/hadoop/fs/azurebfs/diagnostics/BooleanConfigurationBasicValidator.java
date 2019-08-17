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
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;

/**
 * Boolean configuration value validator.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BooleanConfigurationBasicValidator extends ConfigurationBasicValidator<Boolean> {
  private static final String TRUE = "true";
  private static final String FALSE = "false";

  public BooleanConfigurationBasicValidator(final String configKey, final boolean defaultVal, final boolean throwIfInvalid) {
    super(configKey, defaultVal, throwIfInvalid);
  }

  public Boolean validate(final String configValue) throws InvalidConfigurationValueException {
    Boolean result = super.validate(configValue);
    if (result != null) {
      return result;
    }

    if (configValue.equalsIgnoreCase(TRUE) || configValue.equalsIgnoreCase(FALSE)) {
      return Boolean.valueOf(configValue);
    }

    throw new InvalidConfigurationValueException(getConfigKey());
  }
}
