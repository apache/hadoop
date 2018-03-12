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
package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.TimerTask;
import java.util.Set;

/**
 * Configuration based node attributes provider.
 */
public class ConfigurationNodeAttributesProvider
    extends NodeAttributesProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConfigurationNodeAttributesProvider.class);

  public ConfigurationNodeAttributesProvider() {
    super("Configuration Based Node Attributes Provider");
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    long taskInterval = conf.getLong(YarnConfiguration
            .NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS,
        YarnConfiguration
            .DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS);
    this.setIntervalTime(taskInterval);
    super.serviceInit(conf);
  }

  private void updateNodeAttributesFromConfig(Configuration conf)
      throws IOException {
    String configuredNodeAttributes = conf.get(
        YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES, null);
    setDescriptors(parseAttributes(configuredNodeAttributes));
  }

  // TODO parse attributes from configuration
  @VisibleForTesting
  public Set<NodeAttribute> parseAttributes(String config)
      throws IOException {
    return new HashSet<>();
  }

  private class ConfigurationMonitorTimerTask extends TimerTask {
    @Override
    public void run() {
      try {
        updateNodeAttributesFromConfig(new YarnConfiguration());
      } catch (Exception e) {
        LOG.error("Failed to update node attributes from "
            + YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES, e);
      }
    }
  }

  @Override
  protected void cleanUp() throws Exception {
    // Nothing to cleanup
  }

  @Override
  public TimerTask createTimerTask() {
    return new ConfigurationMonitorTimerTask();
  }
}
