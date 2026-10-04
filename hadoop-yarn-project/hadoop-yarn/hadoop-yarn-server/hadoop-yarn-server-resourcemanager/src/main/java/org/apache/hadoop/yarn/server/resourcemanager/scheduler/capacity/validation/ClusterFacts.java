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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractAutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueKind;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Immutable copy of the live scheduler state that validation rules need:
 * the cluster resource, the node labels and every live queue.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ClusterFacts {

  private static final ClusterFacts EMPTY = new Builder().build();

  /** A live queue. */
  public static final class QueueFacts {
    private final String queuePath;
    private final QueueKind kind;
    private final QueueState state;
    private final boolean dynamic;
    private final boolean autoCreatedLeaf;
    private final int runningApplications;
    private final int pendingApplications;
    private final Resource maximumAllocation;

    /**
     * @param queuePath full path of the queue
     * @param kind kind of the queue
     * @param state current state of the queue
     * @param dynamic whether the queue is an AQC v2 dynamic queue
     * @param autoCreatedLeaf whether the queue is an AQC v1 auto created leaf
     *                        or a reservation queue
     * @param maximumAllocation the maximum allocation of the queue, or null
     */
    public QueueFacts(String queuePath, QueueKind kind, QueueState state,
        boolean dynamic, boolean autoCreatedLeaf, Resource maximumAllocation) {
      this(queuePath, kind, state, dynamic, autoCreatedLeaf,
          maximumAllocation, 0, 0);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    private QueueFacts(String queuePath, QueueKind kind, QueueState state,
        boolean dynamic, boolean autoCreatedLeaf, Resource maximumAllocation,
        int runningApplications, int pendingApplications) {
      this.queuePath = queuePath;
      this.kind = kind;
      this.state = state;
      this.dynamic = dynamic;
      this.autoCreatedLeaf = autoCreatedLeaf;
      this.runningApplications = runningApplications;
      this.pendingApplications = pendingApplications;
      this.maximumAllocation = maximumAllocation == null ? null
          : Resources.clone(maximumAllocation);
    }

    /**
     * @param running number of running applications
     * @param pending number of pending applications
     * @return a copy of these facts with the given application counts
     */
    public QueueFacts withApplications(int running, int pending) {
      return new QueueFacts(queuePath, kind, state, dynamic, autoCreatedLeaf,
          maximumAllocation, running, pending);
    }

    public String getQueuePath() {
      return queuePath;
    }

    public QueueKind getKind() {
      return kind;
    }

    public QueueState getState() {
      return state;
    }

    public boolean isDynamic() {
      return dynamic;
    }

    public boolean isAutoCreatedLeaf() {
      return autoCreatedLeaf;
    }

    public int getRunningApplications() {
      return runningApplications;
    }

    public int getPendingApplications() {
      return pendingApplications;
    }

    /** @return the maximum allocation, null when unknown */
    public Resource getMaximumAllocation() {
      return maximumAllocation;
    }
  }

  private final Resource clusterResource;
  private final ResourceCalculator resourceCalculator;
  private final Set<String> nodeLabels;
  private final Map<String, Resource> resourceByLabel;
  private final Map<String, QueueFacts> queues;
  private final boolean hierarchyChecksSkipped;

  private ClusterFacts(Builder builder) {
    this.clusterResource = Resources.clone(builder.clusterResource);
    this.resourceCalculator = builder.resourceCalculator;
    this.nodeLabels = Collections.unmodifiableSet(
        new LinkedHashSet<>(builder.nodeLabels));
    this.resourceByLabel = Collections.unmodifiableMap(
        new LinkedHashMap<>(builder.resourceByLabel));
    this.queues = Collections.unmodifiableMap(
        new LinkedHashMap<>(builder.queues));
    this.hierarchyChecksSkipped = builder.hierarchyChecksSkipped;
  }

  /** @return facts describing an empty cluster without queues */
  public static ClusterFacts empty() {
    return EMPTY;
  }

  /**
   * Copies the facts from the live scheduler. Takes the scheduler read lock
   * only, never the write lock.
   *
   * @param scheduler live scheduler
   * @return immutable facts
   */
  public static ClusterFacts capture(final CapacityScheduler scheduler) {
    return scheduler.callUnderReadLock(() -> captureLocked(scheduler));
  }

  private static ClusterFacts captureLocked(CapacityScheduler scheduler) {
    Builder builder = new Builder();
    Resource cluster = scheduler.getClusterResource();
    builder.clusterResource(cluster);
    builder.resourceCalculator(scheduler.getResourceCalculator());
    RMContext rmContext = scheduler.getRMContext();
    builder.clusterNodeLabels(rmContext, cluster);
    builder.hierarchyChecksSkipped(isHierarchyChecksSkipped(
        scheduler.isConfigurationMutable(), rmContext));

    CSQueue root = scheduler.getRootQueue();
    Deque<CSQueue> pending = new ArrayDeque<>();
    if (root != null) {
      pending.push(root);
    }
    while (!pending.isEmpty()) {
      CSQueue node = pending.pop();
      // The facts come from the queue store, which the refresh checks the
      // hierarchy transition against; the tree only gives the order
      CSQueue queue = scheduler.getCapacitySchedulerQueueManager() == null
          ? null : scheduler.getCapacitySchedulerQueueManager()
              .getQueueByFullName(node.getQueuePath());
      if (queue == null) {
        queue = node;
      }
      QueueMetrics metrics = queue.getMetrics();
      builder.queue(new QueueFacts(queue.getQueuePath(), QueueKind.of(queue),
          queue.getState(), queue.isDynamicQueue(),
          queue instanceof AbstractAutoCreatedLeafQueue,
          queue.getMaximumAllocation()).withApplications(
              metrics == null ? 0 : metrics.getAppsRunning(),
              metrics == null ? 0 : metrics.getAppsPending()));
      List<CSQueue> children = node.getChildQueues();
      if (children != null) {
        // Pushed in reverse, so that children are visited in order
        List<CSQueue> reversed = new ArrayList<>(children);
        Collections.reverse(reversed);
        for (CSQueue child : reversed) {
          pending.push(child);
        }
      }
    }
    return builder.build();
  }

  /** @return the cluster resource */
  public Resource getClusterResource() {
    return clusterResource;
  }

  /** @return the resource calculator of the scheduler */
  public ResourceCalculator getResourceCalculator() {
    return resourceCalculator;
  }

  /** @return the cluster node label names */
  public Set<String> getNodeLabels() {
    return nodeLabels;
  }

  /**
   * @param label a node label, the empty string for the default partition
   * @return the resource of the label, none for an unknown label
   */
  public Resource getResourceByLabel(String label) {
    Resource resource = resourceByLabel.get(
        label == null ? RMNodeLabelsManager.NO_LABEL : label);
    return resource == null ? Resources.none() : resource;
  }

  /** @return every live queue by full path, parents before children */
  public Map<String, QueueFacts> getQueues() {
    return queues;
  }

  /**
   * @param queuePath a full queue path
   * @return the live queue, or null
   */
  public QueueFacts getQueue(String queuePath) {
    return queues.get(queuePath);
  }

  /**
   * Whether a refresh would skip the queue hierarchy transition checks: the
   * configuration is mutable and the ResourceManager is in standby.
   * @return true if the hierarchy transition checks are skipped
   */
  public boolean isHierarchyChecksSkipped() {
    return hierarchyChecksSkipped;
  }

  /**
   * Whether a refresh skips the queue hierarchy transition checks: the
   * scheduler does so on a standby RM with a mutable configuration.
   * @param configurationMutable whether the scheduler configuration is mutable
   * @param rmContext the RM context, or null
   * @return true if the hierarchy transition checks are skipped
   */
  public static boolean isHierarchyChecksSkipped(boolean configurationMutable,
      RMContext rmContext) {
    return configurationMutable && rmContext != null
        && rmContext.getHAServiceState() == HAServiceState.STANDBY;
  }

  /** Builds facts, for a live scheduler or from a configured baseline. */
  public static final class Builder {
    private Resource clusterResource = Resources.none();
    private ResourceCalculator resourceCalculator =
        new DefaultResourceCalculator();
    private final Set<String> nodeLabels = new LinkedHashSet<>();
    private final Map<String, Resource> resourceByLabel =
        new LinkedHashMap<>();
    private final Map<String, QueueFacts> queues = new LinkedHashMap<>();
    private boolean hierarchyChecksSkipped;

    public Builder clusterResource(Resource resource) {
      this.clusterResource = resource == null ? Resources.none() : resource;
      return this;
    }

    public Builder resourceCalculator(ResourceCalculator calculator) {
      if (calculator != null) {
        this.resourceCalculator = calculator;
      }
      return this;
    }

    public Builder nodeLabel(String label) {
      nodeLabels.add(label);
      return this;
    }

    /**
     * Adds the cluster node labels and the resource of every label, the
     * default partition included, from the node labels manager.
     * @param rmContext the RM context, or null for none
     * @param cluster the cluster resource
     * @return this builder
     */
    public Builder clusterNodeLabels(RMContext rmContext, Resource cluster) {
      RMNodeLabelsManager labelManager =
          rmContext == null ? null : rmContext.getNodeLabelManager();
      if (labelManager == null) {
        return this;
      }
      resourceByLabel(RMNodeLabelsManager.NO_LABEL,
          labelManager.getResourceByLabel(RMNodeLabelsManager.NO_LABEL,
              cluster));
      for (NodeLabel label : labelManager.getClusterNodeLabels()) {
        nodeLabel(label.getName());
        resourceByLabel(label.getName(),
            labelManager.getResourceByLabel(label.getName(), cluster));
      }
      return this;
    }

    public Builder resourceByLabel(String label, Resource resource) {
      if (resource != null) {
        resourceByLabel.put(label, Resources.clone(resource));
      }
      return this;
    }

    public Builder queue(QueueFacts queue) {
      queues.put(queue.getQueuePath(), queue);
      return this;
    }

    public Builder hierarchyChecksSkipped(boolean skipped) {
      this.hierarchyChecksSkipped = skipped;
      return this;
    }

    public ClusterFacts build() {
      return new ClusterFacts(this);
    }
  }
}
