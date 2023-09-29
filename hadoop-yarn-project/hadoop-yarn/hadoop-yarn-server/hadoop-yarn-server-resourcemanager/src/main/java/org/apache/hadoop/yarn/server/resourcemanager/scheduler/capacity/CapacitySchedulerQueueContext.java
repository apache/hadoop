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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * Class to store common queue related information, like instances
 * to necessary manager classes or the global CapacityScheduler
 * configuration.
 */
public class CapacitySchedulerQueueContext {

  // Manager classes
  private final CapacitySchedulerContext csContext;
  private final CapacitySchedulerQueueManager queueManager;
  private final RMNodeLabelsManager labelManager;
  private final PreemptionManager preemptionManager;
  private final ActivitiesManager activitiesManager;
  private final ResourceCalculator resourceCalculator;

  // CapacityScheduler configuration
  private CapacitySchedulerConfiguration configuration;

  private Resource minimumAllocation;

  public CapacitySchedulerQueueContext(CapacitySchedulerContext csContext) {
    this.csContext = csContext;
    this.queueManager = csContext.getCapacitySchedulerQueueManager();
    this.labelManager = csContext.getRMContext().getNodeLabelManager();
    this.preemptionManager = csContext.getPreemptionManager();
    this.activitiesManager = csContext.getActivitiesManager();
    this.resourceCalculator = csContext.getResourceCalculator();

    this.configuration = new CapacitySchedulerConfiguration(csContext.getConfiguration());
    this.minimumAllocation = csContext.getMinimumResourceCapability();
  }

  public void reinitialize() {
    // When csConfProvider.loadConfiguration is called, the useLocalConfigurationProvider is
    // correctly set to load the config entries from the capacity-scheduler.xml.
    // For this reason there is no need to reload from it again.
    this.configuration = new CapacitySchedulerConfiguration(csContext.getConfiguration(), false);
    this.minimumAllocation = csContext.getMinimumResourceCapability();
  }

  public CapacitySchedulerQueueManager getQueueManager() {
    return queueManager;
  }

  public RMNodeLabelsManager getLabelManager() {
    return labelManager;
  }

  public PreemptionManager getPreemptionManager() {
    return preemptionManager;
  }

  public ActivitiesManager getActivitiesManager() {
    return activitiesManager;
  }

  public ResourceCalculator getResourceCalculator() {
    return resourceCalculator;
  }

  public CapacitySchedulerConfiguration getConfiguration() {
    return configuration;
  }

  public void setConfigurationEntry(String name, String value) {
    this.configuration.set(name, value);
  }

  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  public Resource getClusterResource() {
    return csContext.getClusterResource();
  }

  public ResourceUsage getClusterResourceUsage() {
    return queueManager.getRootQueue().getQueueResourceUsage();
  }

  public SchedulerHealth getSchedulerHealth() {
    return csContext.getSchedulerHealth();
  }

  public long getLastNodeUpdateTime() {
    return csContext.getLastNodeUpdateTime();
  }

  public FiCaSchedulerNode getNode(NodeId nodeId) {
    return csContext.getNode(nodeId);
  }

  public FiCaSchedulerApp getApplicationAttempt(
      ApplicationAttemptId applicationAttemptId) {
    return csContext.getApplicationAttempt(applicationAttemptId);
  }

  public CapacityScheduler.PendingApplicationComparator getApplicationComparator() {
    return csContext.getPendingApplicationComparator();
  }
}
