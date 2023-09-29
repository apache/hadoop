/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains node labels for all queues extracted from configuration properties.
 * A queue has a configured node label if it has a property set with an
 * accessible-node-labels prefix.
 * Example:
 * yarn.scheduler.capacity.root.accessible-node-labels.test-label.capacity
 */
public class ConfiguredNodeLabels {
  private final Map<String, Set<String>> configuredNodeLabelsByQueue;
  private static final Set<String> NO_LABEL =
      ImmutableSet.of(RMNodeLabelsManager.NO_LABEL);

  public ConfiguredNodeLabels() {
    configuredNodeLabelsByQueue = new HashMap<>();
  }

  public ConfiguredNodeLabels(
      CapacitySchedulerConfiguration conf) {
    this.configuredNodeLabelsByQueue = conf.getConfiguredNodeLabelsByQueue();
  }

  /**
   * Returns a set of configured node labels for a queue. If no labels are set
   * for a queue, it defaults to a one element immutable collection containing
   * empty label.
   * @param queuePath path of the queue
   * @return configured node labels or an immutable set containing the empty
   * label
   */
  public Set<String> getLabelsByQueue(String queuePath) {
    Set<String> labels = configuredNodeLabelsByQueue.get(queuePath);

    if (labels == null) {
      return NO_LABEL;
    }

    return ImmutableSet.copyOf(labels);
  }

  /**
   * Set node labels for a specific queue.
   * @param queuePath path of the queue
   * @param nodeLabels configured node labels to set
   */
  public void setLabelsByQueue(
      String queuePath, Collection<String> nodeLabels) {
    configuredNodeLabelsByQueue.put(queuePath, new HashSet<>(nodeLabels));
  }

  /**
   * Get all configured node labels aggregated from each queue.
   * @return all node labels
   */
  public Set<String> getAllConfiguredLabels() {
    Set<String> nodeLabels = configuredNodeLabelsByQueue.values().stream()
        .flatMap(Set::stream).collect(Collectors.toSet());

    if (nodeLabels.size() == 0) {
      nodeLabels = NO_LABEL;
    }

    return nodeLabels;
  }
}
