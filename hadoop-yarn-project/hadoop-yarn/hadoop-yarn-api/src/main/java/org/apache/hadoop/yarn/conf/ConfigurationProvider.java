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

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;

@Private
@Unstable
/**
 * Base class to implement ConfigurationProvider.
 * Real ConfigurationProvider implementations need to derive from it and
 * implement load methods to actually load the configuration.
 */
public abstract class ConfigurationProvider {

  public void init(Configuration bootstrapConf) throws Exception {
    initInternal(bootstrapConf);
  }

  public void close() throws Exception {
    closeInternal();
  }

  /**
   * Opens an InputStream at the indicated file
   * @param bootstrapConf Configuration
   * @param name The configuration file name
   * @return configuration
   * @throws YarnException
   * @throws IOException
   */
  public abstract InputStream getConfigurationInputStream(
      Configuration bootstrapConf, String name) throws YarnException,
      IOException;

  /**
   * Derived classes initialize themselves using this method.
   */
  public abstract void initInternal(Configuration bootstrapConf)
      throws Exception;

  /**
   * Derived classes close themselves using this method.
   */
  public abstract void closeInternal() throws Exception;
}
