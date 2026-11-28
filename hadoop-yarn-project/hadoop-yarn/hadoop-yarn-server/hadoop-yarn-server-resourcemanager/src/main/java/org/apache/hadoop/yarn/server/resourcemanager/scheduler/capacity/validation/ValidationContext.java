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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueConfigResolver;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolutionInputs;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolutionInputs.DynamicQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueTree;

/**
 * Inputs shared by all rules of one validation run: the proposed
 * configuration, its resolved queue tree and the cluster facts.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ValidationContext {
  private final ConfigSnapshot proposed;
  private final ClusterFacts facts;
  private final CapacitySchedulerConfiguration configuration;
  private final boolean legacyQueueMode;
  private final Resource clusterMaximumAllocation;
  private final RuntimeException clusterMaximumAllocationFailure;
  private final RuntimeException clusterMaximumPriorityFailure;
  private final ResolvedQueueTree tree;
  private final Map<QueuePath, Integer> positions = new HashMap<>();
  private final Map<QueuePath, Integer> subtreeEnds = new HashMap<>();

  public ValidationContext(ConfigSnapshot proposed, ClusterFacts facts) {
    this.proposed = Objects.requireNonNull(proposed, "proposed");
    this.facts = Objects.requireNonNull(facts, "facts");

    Configuration conf = new Configuration(false);
    for (Map.Entry<String, String> entry
        : proposed.getRawProperties().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    this.configuration = new CapacitySchedulerConfiguration(conf, false);
    this.legacyQueueMode = configuration.isLegacyQueueMode();

    ResolutionInputs inputs =
        ResolutionInputs.from(configuration, dynamicQueues(facts));
    this.clusterMaximumAllocationFailure =
        inputs.getClusterMaximumAllocationFailure();
    this.clusterMaximumAllocation = clusterMaximumAllocationFailure == null
        ? inputs.getClusterMaximumAllocation() : null;
    this.clusterMaximumPriorityFailure =
        inputs.getClusterMaximumApplicationPriorityFailure();
    this.tree = QueueConfigResolver.resolve(proposed, inputs);

    int position = 0;
    for (ResolvedQueueConfig queue : tree.getQueues()) {
      QueuePath path = queue.getQueuePath();
      if (queue.getKind() == Kind.RESERVATION) {
        // Created right after its plan queue when the plan is parsed
        positions.put(path, getPosition(path.getParentObject()));
        continue;
      }
      positions.put(path, position);
      if (!queue.isDynamic()) {
        for (QueuePath p = path; p != null;
            p = p.isRoot() ? null : p.getParentObject()) {
          subtreeEnds.put(p, position);
        }
      }
      position++;
    }
  }

  /**
   * The existing dynamic queues the resolver takes into account, classified
   * like the scheduler classifies its live queues. An auto created leaf
   * under a managed parent is an AQC v1 {@code AutoCreatedLeafQueue}; the
   * other auto created leaves are ReservationQueues.
   * @param facts the cluster facts
   * @return the dynamic queues
   */
  static List<DynamicQueue> dynamicQueues(ClusterFacts facts) {
    List<DynamicQueue> dynamicQueues = new ArrayList<>();
    for (ClusterFacts.QueueFacts queue : facts.getQueues().values()) {
      QueuePath path = new QueuePath(queue.getQueuePath());
      if (path.isRoot()) {
        continue;
      }
      ClusterFacts.QueueFacts parent =
          facts.getQueue(path.getParentObject().getFullPath());
      DynamicQueue dynamicQueue = DynamicQueue.of(path,
          queue.isAutoCreatedLeaf() && parent != null
              && parent.getKind() == QueueKind.MANAGED_PARENT,
          queue.isDynamic(), queue.getKind().isLeaf());
      if (dynamicQueue != null) {
        dynamicQueues.add(dynamicQueue);
      }
    }
    return dynamicQueues;
  }

  public ConfigSnapshot getProposed() {
    return proposed;
  }

  public ClusterFacts getFacts() {
    return facts;
  }

  /**
   * @return the proposed configuration as a scheduler configuration, for the
   *         getters that rules call
   */
  public CapacitySchedulerConfiguration getConfiguration() {
    return configuration;
  }

  public boolean isLegacyQueueMode() {
    return legacyQueueMode;
  }

  /** @return the resolved queue tree of the proposed configuration */
  public ResolvedQueueTree getTree() {
    return tree;
  }

  /**
   * @return the cluster maximum allocation of the proposed configuration, or
   *         null when it cannot be read
   */
  public Resource getClusterMaximumAllocation() {
    return clusterMaximumAllocation;
  }

  /** @return why the cluster maximum allocation cannot be read, or null */
  public RuntimeException getClusterMaximumAllocationFailure() {
    return clusterMaximumAllocationFailure;
  }

  /** @return why the cluster maximum priority cannot be read, or null */
  public RuntimeException getClusterMaximumPriorityFailure() {
    return clusterMaximumPriorityFailure;
  }

  /**
   * @param queue a queue of the tree
   * @return its parent, or null for root
   */
  public ResolvedQueueConfig getParent(ResolvedQueueConfig queue) {
    QueuePath path = queue.getQueuePath();
    return path.isRoot() ? null : tree.get(path.getParentObject());
  }

  /**
   * Returns the children a parent is constructed with when the configuration
   * is parsed: its configured queues, without dynamic queues.
   * @param parent a queue of the tree
   * @return the configured children in configuration order
   */
  public List<ResolvedQueueConfig> getConfiguredChildren(
      ResolvedQueueConfig parent) {
    if (parent.getKind() != Kind.ROOT && parent.getKind() != Kind.PARENT) {
      return Collections.emptyList();
    }
    List<ResolvedQueueConfig> children = new ArrayList<>();
    for (ResolvedQueueConfig child : tree.getChildren(parent.getQueuePath())) {
      if (!child.isDynamic()) {
        children.add(child);
      }
    }
    return children;
  }

  /**
   * @param queue a queue of the tree
   * @return true if the queue is an AQC v1 managed parent
   */
  public boolean isManagedParent(ResolvedQueueConfig queue) {
    if (queue.getKind() != Kind.ROOT && queue.getKind() != Kind.PARENT) {
      return false;
    }
    Resolved<Boolean> enabled =
        queue.get(QueueProperties.AUTO_CREATE_CHILD_QUEUE_ENABLED);
    return enabled != null && !enabled.isFailed() && enabled.getValue();
  }

  /**
   * @param queue a queue of the tree
   * @return the kind of queue object the scheduler builds for it
   */
  public QueueKind getQueueKind(ResolvedQueueConfig queue) {
    switch (queue.getKind()) {
    case ROOT:
    case PARENT:
      return isManagedParent(queue) ? QueueKind.MANAGED_PARENT
          : QueueKind.PARENT;
    case PLAN:
      return QueueKind.OTHER_PARENT;
    default:
      return QueueKind.LEAF;
    }
  }

  /**
   * @param path a queue path
   * @return the position of the queue in parse order, -1 if unknown
   */
  int getPosition(QueuePath path) {
    Integer position = positions.get(path);
    return position == null ? -1 : position;
  }

  /**
   * @param path a queue path
   * @return the parse position after which the children of the queue are
   *         set, -1 if unknown
   */
  int getSubtreeEnd(QueuePath path) {
    Integer end = subtreeEnds.get(path);
    return end == null ? -1 : end;
  }
}
