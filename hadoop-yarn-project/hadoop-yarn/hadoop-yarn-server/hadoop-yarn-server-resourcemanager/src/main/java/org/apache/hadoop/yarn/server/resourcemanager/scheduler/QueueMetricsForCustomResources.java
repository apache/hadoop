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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.metrics.CustomResourceMetrics;
import org.apache.hadoop.yarn.metrics.CustomResourceMetricValue;

import java.util.Map;

public class QueueMetricsForCustomResources extends CustomResourceMetrics {
  private final CustomResourceMetricValue aggregatePreemptedSeconds =
      new CustomResourceMetricValue();
  private final CustomResourceMetricValue aggregatePreempted =
      new CustomResourceMetricValue();
  private final CustomResourceMetricValue pending =
      new CustomResourceMetricValue();
  private final CustomResourceMetricValue reserved =
      new CustomResourceMetricValue();

  public void increaseReserved(Resource res) {
    reserved.increase(res);
  }

  public void decreaseReserved(Resource res) {
    reserved.decrease(res);
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

  public Map<String, Long> getPendingValues() {
    return pending.getValues();
  }

  public Map<String, Long> getReservedValues() {
    return reserved.getValues();
  }

  public void increaseAggregatedPreemptedSeconds(Resource res, long seconds) {
    aggregatePreemptedSeconds.increaseWithMultiplier(res, seconds);
  }

  public void increaseAggregatedPreempted(Resource res) {
    aggregatePreempted.increase(res);
  }

  CustomResourceMetricValue getAggregatePreemptedSeconds() {
    return aggregatePreemptedSeconds;
  }
}
