/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueDeletionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueConfigurationAutoRefreshPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;

/**
 * Converts a Fair Scheduler site configuration to Capacity Scheduler
 * site configuration.
 *
 */
public class FSYarnSiteConverter {
  private boolean preemptionEnabled;
  private boolean sizeBasedWeight;

  @SuppressWarnings({"deprecation", "checkstyle:linelength"})
  public void convertSiteProperties(Configuration conf,
      Configuration yarnSiteConfig, boolean drfUsed,
      boolean enableAsyncScheduler, boolean userPercentage,
      FSConfigToCSConfigConverterParams.PreemptionMode preemptionMode) {
    yarnSiteConfig.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getCanonicalName());

    if (conf.getBoolean(
        FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_ENABLED,
        FairSchedulerConfiguration.DEFAULT_CONTINUOUS_SCHEDULING_ENABLED)) {
      yarnSiteConfig.setBoolean(
          CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, true);
      int interval = conf.getInt(
          FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_SLEEP_MS,
          FairSchedulerConfiguration.DEFAULT_CONTINUOUS_SCHEDULING_SLEEP_MS);
      yarnSiteConfig.setInt(PREFIX +
          "schedule-asynchronously.scheduling-interval-ms", interval);
    }

    // This should be always true to trigger cs auto
    // refresh queue.
    yarnSiteConfig.setBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);

    if (conf.getBoolean(FairSchedulerConfiguration.PREEMPTION,
        FairSchedulerConfiguration.DEFAULT_PREEMPTION)) {
      preemptionEnabled = true;

      String policies = addMonitorPolicy(ProportionalCapacityPreemptionPolicy.
          class.getCanonicalName(), yarnSiteConfig);
      yarnSiteConfig.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
          policies);

      int waitTimeBeforeKill = conf.getInt(
          FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL,
          FairSchedulerConfiguration.DEFAULT_WAIT_TIME_BEFORE_KILL);
      yarnSiteConfig.setInt(
          CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL,
          waitTimeBeforeKill);

      long waitBeforeNextStarvationCheck = conf.getLong(
          FairSchedulerConfiguration.WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS,
          FairSchedulerConfiguration.DEFAULT_WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS);
      yarnSiteConfig.setLong(
          CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
          waitBeforeNextStarvationCheck);
    } else {
      if (preemptionMode ==
          FSConfigToCSConfigConverterParams.PreemptionMode.NO_POLICY) {
        yarnSiteConfig.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES, "");
      }
    }

    // For auto created queue's auto deletion.
    if (!userPercentage) {
      String policies = addMonitorPolicy(AutoCreatedQueueDeletionPolicy.
          class.getCanonicalName(), yarnSiteConfig);
      yarnSiteConfig.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
          policies);

      // Set the expired for deletion interval to 10s, consistent with fs.
      yarnSiteConfig.setInt(CapacitySchedulerConfiguration.
          AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME, 10);
    }

    if (conf.getBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE,
        FairSchedulerConfiguration.DEFAULT_ASSIGN_MULTIPLE)) {
      yarnSiteConfig.setBoolean(
          CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED, true);
    } else {
      yarnSiteConfig.setBoolean(
          CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED, false);
    }

    // Make auto cs conf refresh enabled.
    yarnSiteConfig.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        addMonitorPolicy(QueueConfigurationAutoRefreshPolicy
            .class.getCanonicalName(), yarnSiteConfig));

    int maxAssign = conf.getInt(FairSchedulerConfiguration.MAX_ASSIGN,
        FairSchedulerConfiguration.DEFAULT_MAX_ASSIGN);
    if (maxAssign != FairSchedulerConfiguration.DEFAULT_MAX_ASSIGN) {
      yarnSiteConfig.setInt(
          CapacitySchedulerConfiguration.MAX_ASSIGN_PER_HEARTBEAT,
          maxAssign);
    }

    float localityThresholdNode = conf.getFloat(
        FairSchedulerConfiguration.LOCALITY_THRESHOLD_NODE,
        FairSchedulerConfiguration.DEFAULT_LOCALITY_THRESHOLD_NODE);
    if (localityThresholdNode !=
        FairSchedulerConfiguration.DEFAULT_LOCALITY_THRESHOLD_NODE) {
      yarnSiteConfig.setFloat(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY,
          localityThresholdNode);
    }

    float localityThresholdRack = conf.getFloat(
        FairSchedulerConfiguration.LOCALITY_THRESHOLD_RACK,
        FairSchedulerConfiguration.DEFAULT_LOCALITY_THRESHOLD_RACK);
    if (localityThresholdRack !=
        FairSchedulerConfiguration.DEFAULT_LOCALITY_THRESHOLD_RACK) {
      yarnSiteConfig.setFloat(
          CapacitySchedulerConfiguration.RACK_LOCALITY_ADDITIONAL_DELAY,
          localityThresholdRack);
    }

    if (conf.getBoolean(FairSchedulerConfiguration.SIZE_BASED_WEIGHT,
        FairSchedulerConfiguration.DEFAULT_SIZE_BASED_WEIGHT)) {
      sizeBasedWeight = true;
    }

    if (drfUsed) {
      yarnSiteConfig.set(
          CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
          DominantResourceCalculator.class.getCanonicalName());
    }

    if (enableAsyncScheduler) {
      yarnSiteConfig.setBoolean(CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, true);
    }
  }

  public boolean isPreemptionEnabled() {
    return preemptionEnabled;
  }

  public boolean isSizeBasedWeight() {
    return sizeBasedWeight;
  }

  private String addMonitorPolicy(String policyName,
      Configuration yarnSiteConfig) {
    String policies =
        yarnSiteConfig.get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES);
    if (policies == null || policies.isEmpty()) {
      policies = policyName;
    } else {
      policies += "," + policyName;
    }
    return policies;
  }

}