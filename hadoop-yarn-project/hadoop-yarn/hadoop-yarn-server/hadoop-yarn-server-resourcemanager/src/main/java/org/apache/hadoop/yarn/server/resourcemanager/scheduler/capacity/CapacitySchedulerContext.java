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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * Read-only interface to {@link CapacityScheduler} context.
 */
public interface CapacitySchedulerContext {
  CapacitySchedulerConfiguration getConfiguration();
  
  Resource getMinimumResourceCapability();

  Resource getMaximumResourceCapability();

  Resource getMaximumResourceCapability(String queueName);

  RMContainerTokenSecretManager getContainerTokenSecretManager();
  
  int getNumClusterNodes();

  RMContext getRMContext();
  
  Resource getClusterResource();

  /**
   * Get the yarn configuration.
   */
  Configuration getConf();

  ResourceCalculator getResourceCalculator();
  
  FiCaSchedulerNode getNode(NodeId nodeId);

  FiCaSchedulerApp getApplicationAttempt(ApplicationAttemptId attemptId);

  PreemptionManager getPreemptionManager();

  SchedulerHealth getSchedulerHealth();

  long getLastNodeUpdateTime();

  /**
   * @return QueueCapacities root queue of the Capacity Scheduler Queue, root
   *         queue used capacities for different labels are same as that of the
   *         cluster.
   */
  ResourceUsage getClusterResourceUsage();

  ActivitiesManager getActivitiesManager();

  CapacitySchedulerQueueManager getCapacitySchedulerQueueManager();

  /**
   *
   * @return Max Cluster level App priority.
   */
  Priority getMaxClusterLevelAppPriority();

  /**
   * Returns if configuration is mutable.
   * @return if configuration is mutable
   */
  boolean isConfigurationMutable();

  /**
   * Get clock from scheduler
   * @return Clock
   */
  Clock getClock();

}
