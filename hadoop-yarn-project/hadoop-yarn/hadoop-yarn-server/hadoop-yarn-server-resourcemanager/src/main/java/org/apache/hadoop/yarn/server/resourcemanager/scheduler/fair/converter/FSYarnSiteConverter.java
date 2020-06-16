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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;

/**
 * Converts a Fair Scheduler site configuration to Capacity Scheduler
 * site configuration.
 *
 */
public class FSYarnSiteConverter {
  private boolean preemptionEnabled;
  private boolean autoCreateChildQueues;
  private boolean sizeBasedWeight;
  private boolean userAsDefaultQueue;

  @SuppressWarnings({"deprecation", "checkstyle:linelength"})
  public void convertSiteProperties(Configuration conf,
      Configuration yarnSiteConfig, boolean drfUsed, boolean enableAsyncScheduler) {
    yarnSiteConfig.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getCanonicalName());

    // TODO: deprecated property, check if necessary
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

    String mbIncrementAllocation =
        conf.get("yarn.resource-types.memory-mb.increment-allocation");
    if (mbIncrementAllocation != null) {
      yarnSiteConfig.set("yarn.scheduler.minimum-allocation-mb",
          mbIncrementAllocation);
    }

    String vcoreIncrementAllocation =
        conf.get("yarn.resource-types.vcores.increment-allocation");
    if (vcoreIncrementAllocation != null) {
      yarnSiteConfig.set("yarn.scheduler.minimum-allocation-vcores",
          vcoreIncrementAllocation);
    }

    if (conf.getBoolean(FairSchedulerConfiguration.PREEMPTION,
        FairSchedulerConfiguration.DEFAULT_PREEMPTION)) {
      yarnSiteConfig.setBoolean(
          YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
      preemptionEnabled = true;

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
    }

    if (conf.getBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE,
        FairSchedulerConfiguration.DEFAULT_ASSIGN_MULTIPLE)) {
      yarnSiteConfig.setBoolean(
          CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED, true);
    } else {
      yarnSiteConfig.setBoolean(
          CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED, false);
    }

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

    if (conf.getBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS,
        FairSchedulerConfiguration.DEFAULT_ALLOW_UNDECLARED_POOLS)) {
      autoCreateChildQueues = true;
    }

    if (conf.getBoolean(FairSchedulerConfiguration.SIZE_BASED_WEIGHT,
        FairSchedulerConfiguration.DEFAULT_SIZE_BASED_WEIGHT)) {
      sizeBasedWeight = true;
    }

    if (conf.getBoolean(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE,
        FairSchedulerConfiguration.DEFAULT_USER_AS_DEFAULT_QUEUE)) {
      userAsDefaultQueue = true;
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

  public boolean isAutoCreateChildQueues() {
    return autoCreateChildQueues;
  }

  public boolean isSizeBasedWeight() {
    return sizeBasedWeight;
  }

  public boolean isUserAsDefaultQueue() {
    return userAsDefaultQueue;
  }
}