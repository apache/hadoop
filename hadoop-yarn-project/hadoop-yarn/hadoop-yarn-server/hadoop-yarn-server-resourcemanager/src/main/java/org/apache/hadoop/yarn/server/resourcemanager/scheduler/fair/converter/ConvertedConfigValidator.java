/*
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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates the converted capacity-scheduler.xml by starting
 * a Capacity Scheduler instance.
 *
 */
public class ConvertedConfigValidator {
  private static final Logger LOG =
      LoggerFactory.getLogger(ConvertedConfigValidator.class);

  public void validateConvertedConfig(String outputDir)
      throws Exception {
    QueueMetrics.clearQueueMetrics();
    Path configPath = new Path(outputDir, "capacity-scheduler.xml");

    CapacitySchedulerConfiguration csConfig =
        new CapacitySchedulerConfiguration(
            new Configuration(false), false);
    csConfig.addResource(configPath);

    Path convertedSiteConfigPath = new Path(outputDir, "yarn-site.xml");
    Configuration siteConf = new YarnConfiguration(
        new Configuration(false));
    siteConf.addResource(convertedSiteConfigPath);

    RMContextImpl rmContext = new RMContextImpl();
    siteConf.set(YarnConfiguration.FS_BASED_RM_CONF_STORE, outputDir);
    ConfigurationProvider provider = new FileSystemBasedConfigurationProvider();
    provider.init(siteConf);
    rmContext.setConfigurationProvider(provider);
    RMNodeLabelsManager mgr = new RMNodeLabelsManager();
    mgr.init(siteConf);
    rmContext.setNodeLabelManager(mgr);

    try (CapacityScheduler cs = new CapacityScheduler()) {
      cs.setConf(siteConf);
      cs.setRMContext(rmContext);
      cs.serviceInit(csConfig);
      cs.serviceStart();
      LOG.info("Capacity scheduler was successfully started");
      cs.serviceStop();
    } catch (Exception e) {
      LOG.error("Could not start Capacity Scheduler", e);
      throw new VerificationException(
          "Verification of converted configuration failed", e);
    }
  }
}
