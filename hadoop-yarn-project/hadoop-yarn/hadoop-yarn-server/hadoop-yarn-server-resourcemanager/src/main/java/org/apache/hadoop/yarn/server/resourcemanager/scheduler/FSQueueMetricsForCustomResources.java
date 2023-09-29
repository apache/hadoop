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
 * This class is a main entry-point for any kind of metrics for
 * custom resources.
 * It provides increase and decrease methods for all types of metrics.
 */
public class FSQueueMetricsForCustomResources {
  private final CustomResourceMetricValue
      fairShare = new CustomResourceMetricValue();
  private final CustomResourceMetricValue steadyFairShare =
      new CustomResourceMetricValue();
  private final CustomResourceMetricValue
      minShare = new CustomResourceMetricValue();
  private final CustomResourceMetricValue
      maxShare = new CustomResourceMetricValue();
  private final CustomResourceMetricValue
      maxAMShare = new CustomResourceMetricValue();
  private final CustomResourceMetricValue amResourceUsage =
      new CustomResourceMetricValue();

  public CustomResourceMetricValue getFairShare() {
    return fairShare;
  }

  public void setFairShare(Resource res) {
    fairShare.set(res);
  }

  public Map<String, Long> getFairShareValues() {
    return fairShare.getValues();
  }

  public CustomResourceMetricValue getSteadyFairShare() {
    return steadyFairShare;
  }

  public void setSteadyFairShare(Resource res) {
    steadyFairShare.set(res);
  }

  public Map<String, Long> getSteadyFairShareValues() {
    return steadyFairShare.getValues();
  }

  public CustomResourceMetricValue getMinShare() {
    return minShare;
  }

  public void setMinShare(Resource res) {
    minShare.set(res);
  }

  public Map<String, Long> getMinShareValues() {
    return minShare.getValues();
  }

  public CustomResourceMetricValue getMaxShare() {
    return maxShare;
  }

  public void setMaxShare(Resource res) {
    maxShare.set(res);
  }

  public Map<String, Long> getMaxShareValues() {
    return maxShare.getValues();
  }

  public CustomResourceMetricValue getMaxAMShare() {
    return maxAMShare;
  }

  public void setMaxAMShare(Resource res) {
    maxAMShare.set(res);
  }

  public Map<String, Long> getMaxAMShareValues() {
    return maxAMShare.getValues();
  }

  public CustomResourceMetricValue getAMResourceUsage() {
    return amResourceUsage;
  }

  public void setAMResourceUsage(Resource res) {
    amResourceUsage.set(res);
  }

  public Map<String, Long> getAMResourceUsageValues() {
    return amResourceUsage.getValues();
  }
}
