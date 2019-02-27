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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Map;

/**
 * This class is a main entry-point for any kind of metrics for
 * custom resources.
 * It provides increase and decrease methods for all types of metrics.
 */
public class QueueMetricsForCustomResources {
  private final QueueMetricsCustomResource aggregatePreemptedSeconds =
      new QueueMetricsCustomResource();
  private final QueueMetricsCustomResource allocated =
      new QueueMetricsCustomResource();
  private final QueueMetricsCustomResource available =
      new QueueMetricsCustomResource();
  private final QueueMetricsCustomResource pending =
      new QueueMetricsCustomResource();

  private final QueueMetricsCustomResource reserved =
      new QueueMetricsCustomResource();

  public void increaseReserved(Resource res) {
    reserved.increase(res);
  }

  public void decreaseReserved(Resource res) {
    reserved.decrease(res);
  }

  public void setAvailable(Resource res) {
    available.set(res);
  }

  public void increasePending(Resource res, int containers) {
    pending.increaseWithMultiplier(res, containers);
  }

  public void decreasePending(Resource res) {
    pending.decrease(res);
  }

  public void decreasePending(Resource res, int containers) {
    pending.decreaseWithMultiplier(res, containers);
  }

  public void increaseAllocated(Resource res) {
    allocated.increase(res);
  }

  public void increaseAllocated(Resource res, int containers) {
    allocated.increaseWithMultiplier(res, containers);
  }

  public void decreaseAllocated(Resource res) {
    allocated.decrease(res);
  }

  public void decreaseAllocated(Resource res, int containers) {
    allocated.decreaseWithMultiplier(res, containers);
  }

  public void increaseAggregatedPreemptedSeconds(Resource res, long seconds) {
    aggregatePreemptedSeconds.increaseWithMultiplier(res, seconds);
  }

  Map<String, Long> getAllocatedValues() {
    return allocated.getValues();
  }

  Map<String, Long> getAvailableValues() {
    return available.getValues();
  }

  Map<String, Long> getPendingValues() {
    return pending.getValues();
  }

  Map<String, Long> getReservedValues() {
    return reserved.getValues();
  }

  QueueMetricsCustomResource getAggregatePreemptedSeconds() {
    return aggregatePreemptedSeconds;
  }
}
