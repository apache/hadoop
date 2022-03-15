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
import org.apache.hadoop.yarn.metrics.CustomResourceMetricValue;

import java.util.Map;

/**
 * This class is a main entry-point for any kind of CSQueueMetrics for
 * custom resources.
 * It provides increase and decrease methods for all types of metrics.
 */
public class CSQueueMetricsForCustomResources
    extends QueueMetricsForCustomResources {
  private final CustomResourceMetricValue guaranteedCapacity =
      new CustomResourceMetricValue();
  private final CustomResourceMetricValue maxCapacity =
      new CustomResourceMetricValue();

  public void setGuaranteedCapacity(Resource res) {
    guaranteedCapacity.set(res);
  }

  public void setMaxCapacity(Resource res) {
    maxCapacity.set(res);
  }

  public Map<String, Long> getGuaranteedCapacity() {
    return guaranteedCapacity.getValues();
  }

  public Map<String, Long> getMaxCapacity() {
    return maxCapacity.getValues();
  }
}