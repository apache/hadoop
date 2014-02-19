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

package org.apache.hadoop.yarn;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

@Private
@Unstable
public class LocalConfigurationProvider extends ConfigurationProvider {

  @Override
  public InputStream getConfigurationInputStream(Configuration bootstrapConf,
      String name) throws IOException, YarnException {
    if (name == null || name.isEmpty()) {
      throw new YarnException(
          "Illegal argument! The parameter should not be null or empty");
    } else if (YarnConfiguration.RM_CONFIGURATION_FILES.contains(name)) {
      return bootstrapConf.getConfResourceAsInputStream(name);
    }
    return new FileInputStream(name);
  }

  @Override
  public void initInternal(Configuration bootstrapConf) throws Exception {
    // Do nothing
  }

  @Override
  public void closeInternal() throws Exception {
    // Do nothing
  }
}
