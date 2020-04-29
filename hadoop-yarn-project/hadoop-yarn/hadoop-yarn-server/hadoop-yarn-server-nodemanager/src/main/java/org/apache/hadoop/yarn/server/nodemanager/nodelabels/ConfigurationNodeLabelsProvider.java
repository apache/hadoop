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

import java.io.IOException;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Provides Node's Labels by constantly monitoring the configuration.
 */
public class ConfigurationNodeLabelsProvider extends NodeLabelsProvider {

  private static final Logger LOG =
       LoggerFactory.getLogger(ConfigurationNodeLabelsProvider.class);

  public ConfigurationNodeLabelsProvider() {
    super("Configuration Based NodeLabels Provider");
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    long taskInterval = conf.getLong(
        YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS);
    this.setIntervalTime(taskInterval);
    super.serviceInit(conf);
  }

  private void updateNodeLabelsFromConfig(Configuration conf)
      throws IOException {
    String configuredNodePartition =
        conf.get(YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_PARTITION, null);
    setDescriptors(convertToNodeLabelSet(configuredNodePartition));
  }

  private class ConfigurationMonitorTimerTask extends TimerTask {
    @Override
    public void run() {
      try {
        updateNodeLabelsFromConfig(new YarnConfiguration());
      } catch (Exception e) {
        LOG.error("Failed to update node Labels from configuration.xml ", e);
      }
    }
  }

  @Override
  public TimerTask createTimerTask() {
    return new ConfigurationMonitorTimerTask();
  }

  @Override
  protected void cleanUp() throws Exception {
    //No cleanup Req!
  }
}
