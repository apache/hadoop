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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link CapacityScheduler} configuration provider based on local
 * {@code capacity-scheduler.xml} file.
 */
public class FileBasedCSConfigurationProvider implements
    CSConfigurationProvider {

  private RMContext rmContext;

  /**
   * Construct file based CS configuration provider with given context.
   * @param rmContext the RM context
   */
  public FileBasedCSConfigurationProvider(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public void init(Configuration conf) {}

  @Override
  public CapacitySchedulerConfiguration loadConfiguration(Configuration conf)
      throws IOException {
    try {
      InputStream csInputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.CS_CONFIGURATION_FILE);
      if (csInputStream != null) {
        conf.addResource(csInputStream);
        return new CapacitySchedulerConfiguration(conf, false);
      }
      return new CapacitySchedulerConfiguration(conf, true);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
