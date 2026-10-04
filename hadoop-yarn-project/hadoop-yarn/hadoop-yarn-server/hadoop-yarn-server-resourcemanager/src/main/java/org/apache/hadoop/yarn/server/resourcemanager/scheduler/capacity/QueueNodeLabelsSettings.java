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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;

import java.io.IOException;
import java.util.Set;

/**
 * This class holds the accessible node labels, configured node labels and the default node
 * label expression of a queue, taken from its resolved configuration, in which the queue
 * inherits the accessible labels and the default label expression of its parent.
 */
public class QueueNodeLabelsSettings {
  private final QueuePath queuePath;
  private Set<String> accessibleLabels;
  private Set<String> configuredNodeLabels;
  private String defaultLabelExpression;

  /**
   * @param resolved the resolved configuration of the queue
   * @param parentResolved the resolved configuration of the parent, null for root
   * @param configuration the queue configuration, read for the configured node labels
   *                      when no index of them is given
   * @param queuePath the queue path
   * @param configuredNodeLabels the configured node labels of all queues
   * @throws IOException if the accessible labels are not a subset of the parent's
   */
  public QueueNodeLabelsSettings(ResolvedQueueConfig resolved,
      ResolvedQueueConfig parentResolved,
      CapacitySchedulerConfiguration configuration,
      QueuePath queuePath,
      ConfiguredNodeLabels configuredNodeLabels) throws IOException {
    this.queuePath = queuePath;
    this.accessibleLabels =
        resolved.get(QueueProperties.ACCESSIBLE_NODE_LABELS).getValue();
    this.defaultLabelExpression =
        resolved.get(QueueProperties.DEFAULT_NODE_LABEL_EXPRESSION).getValue();
    initializeConfiguredNodeLabels(configuration, configuredNodeLabels);
    validateNodeLabels(parentResolved);
  }

  private void initializeConfiguredNodeLabels(CapacitySchedulerConfiguration configuration,
      ConfiguredNodeLabels configuredNodeLabelsParam) {
    if (configuredNodeLabelsParam != null) {
      if (queuePath.isRoot()) {
        this.configuredNodeLabels = configuredNodeLabelsParam.getAllConfiguredLabels();
      } else {
        this.configuredNodeLabels = configuredNodeLabelsParam.getLabelsByQueue(
            queuePath.getFullPath());
      }
    } else {
      // Fallback to suboptimal but correct logic
      this.configuredNodeLabels = configuration.getConfiguredNodeLabels(queuePath);
    }
  }

  private void validateNodeLabels(ResolvedQueueConfig parentResolved) throws IOException {
    // Check if labels of this queue is a subset of parent queue, only do this
    // when the queue in question is not root
    if (!queuePath.isRoot()) {
      String error = QueueLabelChecks.checkAccessibleLabelsSubset(false,
          this.getAccessibleNodeLabels(),
          parentResolved.get(QueueProperties.ACCESSIBLE_NODE_LABELS).getValue());
      if (error != null) {
        throw new IOException(error);
      }
    }
  }

  public boolean isAccessibleToPartition(String nodePartition) {
    // if queue's label is *, it can access any node
    if (accessibleLabels != null && accessibleLabels.contains(RMNodeLabelsManager.ANY)) {
      return true;
    }
    // any queue can access to a node without label
    if (nodePartition == null || nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      return true;
    }
    // a queue can access to a node only if it contains any label of the node
    if (accessibleLabels != null && accessibleLabels.contains(nodePartition)) {
      return true;
    }
    // The partition cannot be accessed
    return false;
  }

  public Set<String> getAccessibleNodeLabels() {
    return accessibleLabels;
  }

  public Set<String> getConfiguredNodeLabels() {
    return configuredNodeLabels;
  }

  public String getDefaultLabelExpression() {
    return defaultLabelExpression;
  }
}
