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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Factory class for creating instances of {@link YarnConfigurationStore}.
 */
public final class YarnConfigurationStoreFactory {

  private static final Logger LOG = LoggerFactory.getLogger(
      YarnConfigurationStoreFactory.class);

  private YarnConfigurationStoreFactory() {
    // Unused.
  }

  public static YarnConfigurationStore getStore(Configuration conf) {
    String store = conf.get(
        YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    switch (store) {
      case YarnConfiguration.MEMORY_CONFIGURATION_STORE:
        return new InMemoryConfigurationStore();
      case YarnConfiguration.LEVELDB_CONFIGURATION_STORE:
        return new LeveldbConfigurationStore();
      case YarnConfiguration.ZK_CONFIGURATION_STORE:
        return new ZKConfigurationStore();
      case YarnConfiguration.FS_CONFIGURATION_STORE:
        return new FSSchedulerConfigurationStore();
      default:
        Class<? extends YarnConfigurationStore> storeClass =
            conf.getClass(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
            InMemoryConfigurationStore.class, YarnConfigurationStore.class);
        LOG.info("Using YarnConfigurationStore implementation - " + storeClass);
        return ReflectionUtils.newInstance(storeClass, conf);
    }
  }
}