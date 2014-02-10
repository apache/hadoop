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

package org.apache.hadoop.yarn.conf;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

@Private
@Unstable
/**
 * Factory for {@link ConfigurationProvider} implementations.
 */
public class ConfigurationProviderFactory {
  /**
   * Creates an instance of {@link ConfigurationProvider} using given
   * configuration.
   * @param bootstrapConf
   * @return configurationProvider
   */
  @SuppressWarnings("unchecked")
  public static ConfigurationProvider
      getConfigurationProvider(Configuration bootstrapConf) {
    Class<? extends ConfigurationProvider> defaultProviderClass;
    try {
      defaultProviderClass = (Class<? extends ConfigurationProvider>)
          Class.forName(
              YarnConfiguration.DEFAULT_RM_CONFIGURATION_PROVIDER_CLASS);
    } catch (Exception e) {
      throw new YarnRuntimeException(
          "Invalid default configuration provider class"
              + YarnConfiguration.DEFAULT_RM_CONFIGURATION_PROVIDER_CLASS, e);
    }
    ConfigurationProvider configurationProvider =
        ReflectionUtils.newInstance(bootstrapConf.getClass(
            YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
            defaultProviderClass, ConfigurationProvider.class),
            bootstrapConf);
    return configurationProvider;
  }
}
