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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * Represents a node in the cluster from the NodeQueueLoadMonitor's perspective
 */
public class ClusterNode {
  private final AtomicInteger queueLength = new AtomicInteger(0);
  private final AtomicInteger queueWaitTime = new AtomicInteger(-1);
  private long timestamp;
  final NodeId nodeId;
  private int queueCapacity = 0;
  private final HashSet<String> labels;

  public ClusterNode(NodeId nodeId) {
    this.nodeId = nodeId;
    this.labels = new HashSet<>();
    updateTimestamp();
  }

  public ClusterNode setQueueLength(int qLength) {
    this.queueLength.set(qLength);
    return this;
  }

  public ClusterNode setQueueWaitTime(int wTime) {
    this.queueWaitTime.set(wTime);
    return this;
  }

  public ClusterNode updateTimestamp() {
    this.timestamp = System.currentTimeMillis();
    return this;
  }

  public ClusterNode setQueueCapacity(int capacity) {
    this.queueCapacity = capacity;
    return this;
  }

  public ClusterNode setNodeLabels(Collection<String> labelsToAdd) {
    labels.clear();
    labels.addAll(labelsToAdd);
    return this;
  }

  public boolean hasLabel(String label) {
    return this.labels.contains(label);
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public AtomicInteger getQueueLength() {
    return this.queueLength;
  }

  public AtomicInteger getQueueWaitTime() {
    return this.queueWaitTime;
  }

  public int getQueueCapacity() {
    return this.queueCapacity;
  }

  public boolean isQueueFull() {
    return this.queueCapacity > 0 &&
        this.queueLength.get() >= this.queueCapacity;
  }
}
