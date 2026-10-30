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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;

/**
 * The resolved configuration of every queue of a hierarchy: the configured
 * queues and the existing dynamic queues. Created by
 * {@link QueueConfigResolver}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ResolvedQueueTree {
  private final ConfigSnapshot snapshot;
  private final ResolutionInputs inputs;
  /** Queue path (or template pseudo-queue path) to its labels. */
  private final Map<String, Set<String>> configuredNodeLabels;
  private final Map<QueuePath, ResolvedQueueConfig> queues =
      new LinkedHashMap<>();
  private final Map<QueuePath, List<ResolvedQueueConfig>> children =
      new LinkedHashMap<>();

  ResolvedQueueTree(ConfigSnapshot snapshot, ResolutionInputs inputs,
      Map<String, Set<String>> configuredNodeLabels) {
    this.snapshot = snapshot;
    this.inputs = inputs;
    this.configuredNodeLabels = configuredNodeLabels;
  }

  public ResolvedQueueConfig getRoot() {
    return queues.get(QueueConfigResolver.ROOT);
  }

  /**
   * Returns a queue's resolved configuration.
   * @param path the queue path
   * @return the resolved configuration, or {@code null} for an unknown queue
   */
  public ResolvedQueueConfig get(QueuePath path) {
    return queues.get(path);
  }

  /**
   * Returns the children of a queue, configured ones first in configuration
   * order, then dynamic ones.
   * @param path the queue path
   * @return the children, empty for a leaf or an unknown queue
   */
  public List<ResolvedQueueConfig> getChildren(QueuePath path) {
    List<ResolvedQueueConfig> list = children.get(path);
    return list == null ? Collections.<ResolvedQueueConfig>emptyList()
        : Collections.unmodifiableList(list);
  }

  /**
   * Returns every queue, parents before their children.
   * @return the resolved configurations
   */
  public Collection<ResolvedQueueConfig> getQueues() {
    return Collections.unmodifiableCollection(queues.values());
  }

  ConfigSnapshot getSnapshot() {
    return snapshot;
  }

  /**
   * Returns the inputs the tree was resolved with.
   * @return the resolution inputs
   */
  public ResolutionInputs getInputs() {
    return inputs;
  }

  Map<String, Set<String>> getConfiguredNodeLabels() {
    return configuredNodeLabels;
  }

  void add(ResolvedQueueConfig queue) {
    queues.put(queue.getQueuePath(), queue);
    if (!queue.getQueuePath().isRoot()) {
      QueuePath parent = queue.getQueuePath().getParentObject();
      List<ResolvedQueueConfig> list = children.get(parent);
      if (list == null) {
        list = new ArrayList<>();
        children.put(parent, list);
      }
      list.add(queue);
    }
  }
}
