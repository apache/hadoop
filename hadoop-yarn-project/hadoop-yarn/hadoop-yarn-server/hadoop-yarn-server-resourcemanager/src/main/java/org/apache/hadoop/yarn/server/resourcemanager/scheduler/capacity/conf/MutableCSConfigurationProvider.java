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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;

import java.io.IOException;
import java.util.Map;

/**
 * CS configuration provider which implements
 * {@link MutableConfigurationProvider} for modifying capacity scheduler
 * configuration.
 */
public class MutableCSConfigurationProvider implements CSConfigurationProvider,
    MutableConfigurationProvider {

  private Configuration schedConf;
  private YarnConfigurationStore confStore;
  private RMContext rmContext;
  private Configuration conf;

  public MutableCSConfigurationProvider(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public void init(Configuration config) throws IOException {
    String store = config.get(
        YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.DEFAULT_CONFIGURATION_STORE);
    switch (store) {
    case YarnConfiguration.MEMORY_CONFIGURATION_STORE:
      this.confStore = new InMemoryConfigurationStore();
      break;
    default:
      this.confStore = YarnConfigurationStoreFactory.getStore(config);
      break;
    }
    Configuration initialSchedConf = new Configuration(false);
    initialSchedConf.addResource(YarnConfiguration.CS_CONFIGURATION_FILE);
    this.schedConf = initialSchedConf;
    confStore.initialize(config, initialSchedConf);
    this.conf = config;
  }

  @Override
  public CapacitySchedulerConfiguration loadConfiguration(Configuration
      configuration) throws IOException {
    Configuration loadedConf = new Configuration(configuration);
    loadedConf.addResource(schedConf);
    return new CapacitySchedulerConfiguration(loadedConf, false);
  }

  @Override
  public void mutateConfiguration(String user,
      Map<String, String> confUpdate) {
    Configuration oldConf = new Configuration(schedConf);
    LogMutation log = new LogMutation(confUpdate, user);
    long id = confStore.logMutation(log);
    for (Map.Entry<String, String> kv : confUpdate.entrySet()) {
      schedConf.set(kv.getKey(), kv.getValue());
    }
    try {
      rmContext.getScheduler().reinitialize(conf, rmContext);
    } catch (IOException e) {
      schedConf = oldConf;
      confStore.confirmMutation(id, false);
      return;
    }
    confStore.confirmMutation(id, true);
  }
}
