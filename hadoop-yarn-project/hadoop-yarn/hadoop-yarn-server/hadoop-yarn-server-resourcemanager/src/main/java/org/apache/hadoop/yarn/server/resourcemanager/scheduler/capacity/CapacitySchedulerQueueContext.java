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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueConfigResolver;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolutionInputs;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolutionInputs.DynamicQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueTree;
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

  // The resolved configuration of the queues, resolved from the snapshot of
  // the installed configuration; guarded by this
  private ResolvedQueueTree resolvedQueueTree;
  private Set<QueuePath> existingDynamicParents;

  public CapacitySchedulerQueueContext(CapacitySchedulerContext csContext) {
    this.csContext = csContext;
    this.queueManager = csContext.getCapacitySchedulerQueueManager();
    this.labelManager = csContext.getRMContext().getNodeLabelManager();
    this.preemptionManager = csContext.getPreemptionManager();
    this.activitiesManager = csContext.getActivitiesManager();
    this.resourceCalculator = csContext.getResourceCalculator();

    installConfiguration(new CapacitySchedulerConfiguration(csContext.getConfiguration()));
    this.minimumAllocation = csContext.getMinimumResourceCapability();
  }

  public void reinitialize() {
    // When csConfProvider.loadConfiguration is called, the useLocalConfigurationProvider is
    // correctly set to load the config entries from the capacity-scheduler.xml.
    // For this reason there is no need to reload from it again.
    installConfiguration(new CapacitySchedulerConfiguration(csContext.getConfiguration(), false));
    this.minimumAllocation = csContext.getMinimumResourceCapability();
  }

  private void installConfiguration(CapacitySchedulerConfiguration conf) {
    this.configuration = conf;
    // Take the snapshot when the configuration is installed. Queue setup is
    // its first reader and nothing writes into the configuration before that,
    // so dynamic queue template writes during setup stay outside of it.
    ConfigSnapshot snapshot = conf.getConfigSnapshot();
    // Resolve the queues once, before the queues are parsed and set up from
    // it. The dynamic queues of the live hierarchy are resolved too, because
    // a refresh sets them up again.
    List<DynamicQueue> dynamicQueues = getExistingDynamicQueues();
    Set<QueuePath> dynamicParents = new HashSet<>();
    for (DynamicQueue queue : dynamicQueues) {
      if (!queue.isLeaf()) {
        dynamicParents.add(queue.getPath());
      }
    }
    ResolvedQueueTree tree = QueueConfigResolver.resolve(snapshot,
        ResolutionInputs.from(conf, dynamicQueues));
    synchronized (this) {
      this.resolvedQueueTree = tree;
      this.existingDynamicParents = Collections.unmodifiableSet(dynamicParents);
    }
  }

  private List<DynamicQueue> getExistingDynamicQueues() {
    List<DynamicQueue> dynamicQueues = new ArrayList<>();
    if (queueManager == null) {
      return dynamicQueues;
    }
    for (CSQueue queue : queueManager.getQueues().values()) {
      DynamicQueue dynamicQueue = DynamicQueue.of(queue.getQueuePathObject(),
          queue instanceof AutoCreatedLeafQueue, queue.isDynamicQueue(),
          queue instanceof AbstractLeafQueue);
      if (dynamicQueue != null) {
        dynamicQueues.add(dynamicQueue);
      }
    }
    return dynamicQueues;
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

  /**
   * Get the snapshot of the queue configuration installed by the last
   * (re)initialization. Entries written later through
   * {@link #setConfigurationEntry(String, String)} or into
   * {@link #getConfiguration()} are not part of it.
   * @return the configuration snapshot
   */
  public ConfigSnapshot getConfigSnapshot() {
    return configuration.getConfigSnapshot();
  }

  /**
   * Get the resolved configuration of the queues, resolved from
   * {@link #getConfigSnapshot()} by the last (re)initialization. Dynamic
   * queues created since then are added when they are set up.
   * @return the resolved queue tree
   */
  public synchronized ResolvedQueueTree getResolvedQueueTree() {
    return resolvedQueueTree;
  }

  /**
   * Get the resolved configuration a queue is set up from. A queue the tree
   * does not hold with the same kind and dynamic nature, for example a
   * dynamic queue created since the last (re)initialization, is resolved
   * against its parent's resolved configuration and added to the tree.
   * @param queuePath the queue path
   * @param kind the kind of the queue
   * @param dynamic whether the queue receives template entries
   * @return the resolved configuration of the queue
   */
  synchronized ResolvedQueueConfig getResolvedQueueConfig(QueuePath queuePath,
      QueueProperty.Kind kind, boolean dynamic) {
    ResolvedQueueConfig resolved = resolvedQueueTree.get(queuePath);
    if (resolved == null || resolved.getKind() != kind
        || resolved.isDynamic() != dynamic) {
      resolved = QueueConfigResolver.resolveQueue(resolvedQueueTree, queuePath,
          kind, dynamic);
    }
    return resolved;
  }

  /**
   * Get the resolved configuration of a queue that is already set up, for
   * example the parent of a queue being set up.
   * @param queuePath the queue path
   * @return the resolved configuration, or null if the queue is not resolved
   */
  synchronized ResolvedQueueConfig getResolvedQueueConfig(QueuePath queuePath) {
    return resolvedQueueTree.get(queuePath);
  }

  /**
   * Whether a queue was a dynamic parent queue in the queue hierarchy when
   * the configuration was installed; such a queue stays a parent queue even
   * if it is configured without children.
   * @param queuePath the queue path
   * @return true for an existing dynamic parent queue
   */
  synchronized boolean isExistingDynamicParent(QueuePath queuePath) {
    return existingDynamicParents.contains(queuePath);
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
