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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

@Metrics(context="yarn")
public class FSQueueMetrics extends QueueMetrics {

  @Metric("Fair share of memory in MB") MutableGaugeInt fairShareMB;
  @Metric("Fair share of CPU in vcores") MutableGaugeInt fairShareVCores;
  @Metric("Steady fair share of memory in MB") MutableGaugeInt steadyFairShareMB;
  @Metric("Steady fair share of CPU in vcores") MutableGaugeInt steadyFairShareVCores;
  @Metric("Minimum share of memory in MB") MutableGaugeInt minShareMB;
  @Metric("Minimum share of CPU in vcores") MutableGaugeInt minShareVCores;
  @Metric("Maximum share of memory in MB") MutableGaugeInt maxShareMB;
  @Metric("Maximum share of CPU in vcores") MutableGaugeInt maxShareVCores;
  
  FSQueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    super(ms, queueName, parent, enableUserMetrics, conf);
  }
  
  public void setFairShare(Resource resource) {
    fairShareMB.set(resource.getMemory());
    fairShareVCores.set(resource.getVirtualCores());
  }
  
  public int getFairShareMB() {
    return fairShareMB.value();
  }
  
  public int getFairShareVirtualCores() {
    return fairShareVCores.value();
  }

  public void setSteadyFairShare(Resource resource) {
    steadyFairShareMB.set(resource.getMemory());
    steadyFairShareVCores.set(resource.getVirtualCores());
  }

  public int getSteadyFairShareMB() {
    return steadyFairShareMB.value();
  }

  public int getSteadyFairShareVCores() {
    return steadyFairShareVCores.value();
  }

  public void setMinShare(Resource resource) {
    minShareMB.set(resource.getMemory());
    minShareVCores.set(resource.getVirtualCores());
  }
  
  public int getMinShareMB() {
    return minShareMB.value();
  }
  
  public int getMinShareVirtualCores() {
    return minShareVCores.value();
  }
  
  public void setMaxShare(Resource resource) {
    maxShareMB.set(resource.getMemory());
    maxShareVCores.set(resource.getVirtualCores());
  }
  
  public int getMaxShareMB() {
    return maxShareMB.value();
  }
  
  public int getMaxShareVirtualCores() {
    return maxShareVCores.value();
  }
  
  public synchronized 
  static FSQueueMetrics forQueue(String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    QueueMetrics metrics = queueMetrics.get(queueName);
    if (metrics == null) {
      metrics = new FSQueueMetrics(ms, queueName, parent, enableUserMetrics, conf)
          .tag(QUEUE_INFO, queueName);
      
      // Register with the MetricsSystems
      if (ms != null) {
        metrics = ms.register(
                sourceName(queueName).toString(), 
                "Metrics for queue: " + queueName, metrics);
      }
      queueMetrics.put(queueName, metrics);
    }

    return (FSQueueMetrics)metrics;
  }

}
