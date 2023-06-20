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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;

public class CSQueueUsageTracker {
  private final CSQueueMetrics metrics;
  private int numContainers;

  /**
   * The timestamp of the last submitted application to this queue.
   * Only applies to dynamic queues.
   */
  private long lastSubmittedTimestamp;

  /**
   * Tracks resource usage by label like used-resource / pending-resource.
   */
  private final ResourceUsage queueUsage;

  private final QueueResourceQuotas queueResourceQuotas;

  public CSQueueUsageTracker(CSQueueMetrics metrics) {
    this.metrics = metrics;
    this.queueUsage = new ResourceUsage();
    this.queueResourceQuotas = new QueueResourceQuotas();
  }

  public int getNumContainers() {
    return numContainers;
  }

  public synchronized void increaseNumContainers() {
    numContainers++;
  }

  public synchronized void decreaseNumContainers() {
    numContainers--;
  }

  public CSQueueMetrics getMetrics() {
    return metrics;
  }

  public long getLastSubmittedTimestamp() {
    return lastSubmittedTimestamp;
  }

  public void setLastSubmittedTimestamp(long lastSubmittedTimestamp) {
    this.lastSubmittedTimestamp = lastSubmittedTimestamp;
  }

  public ResourceUsage getQueueUsage() {
    return queueUsage;
  }

  public QueueResourceQuotas getQueueResourceQuotas() {
    return queueResourceQuotas;
  }

}
