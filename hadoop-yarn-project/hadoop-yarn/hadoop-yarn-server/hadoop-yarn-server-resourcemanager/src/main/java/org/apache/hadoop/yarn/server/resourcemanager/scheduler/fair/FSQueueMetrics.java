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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

@Metrics(context="yarn")
public class FSQueueMetrics extends QueueMetrics {

  @Metric("Fair share of memory in MB") MutableGaugeLong fairShareMB;
  @Metric("Fair share of CPU in vcores") MutableGaugeLong fairShareVCores;
  @Metric("Steady fair share of memory in MB") MutableGaugeLong steadyFairShareMB;
  @Metric("Steady fair share of CPU in vcores") MutableGaugeLong steadyFairShareVCores;
  @Metric("Minimum share of memory in MB") MutableGaugeLong minShareMB;
  @Metric("Minimum share of CPU in vcores") MutableGaugeLong minShareVCores;
  @Metric("Maximum share of memory in MB") MutableGaugeLong maxShareMB;
  @Metric("Maximum share of CPU in vcores") MutableGaugeLong maxShareVCores;
  @Metric("Maximum number of applications") MutableGaugeInt maxApps;
  @Metric("Maximum AM share of memory in MB") MutableGaugeLong maxAMShareMB;
  @Metric("Maximum AM share of CPU in vcores") MutableGaugeInt maxAMShareVCores;
  @Metric("AM resource usage of memory in MB") MutableGaugeLong amResourceUsageMB;
  @Metric("AM resource usage of CPU in vcores") MutableGaugeInt amResourceUsageVCores;

  private String schedulingPolicy;

  FSQueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    super(ms, queueName, parent, enableUserMetrics, conf);
  }
  
  public void setFairShare(Resource resource) {
    fairShareMB.set(resource.getMemorySize());
    fairShareVCores.set(resource.getVirtualCores());
  }
  
  public long getFairShareMB() {
    return fairShareMB.value();
  }
  
  public long getFairShareVirtualCores() {
    return fairShareVCores.value();
  }

  public void setSteadyFairShare(Resource resource) {
    steadyFairShareMB.set(resource.getMemorySize());
    steadyFairShareVCores.set(resource.getVirtualCores());
  }

  public long getSteadyFairShareMB() {
    return steadyFairShareMB.value();
  }

  public long getSteadyFairShareVCores() {
    return steadyFairShareVCores.value();
  }

  public void setMinShare(Resource resource) {
    minShareMB.set(resource.getMemorySize());
    minShareVCores.set(resource.getVirtualCores());
  }
  
  public long getMinShareMB() {
    return minShareMB.value();
  }
  
  public long getMinShareVirtualCores() {
    return minShareVCores.value();
  }
  
  public void setMaxShare(Resource resource) {
    maxShareMB.set(resource.getMemorySize());
    maxShareVCores.set(resource.getVirtualCores());
  }
  
  public long getMaxShareMB() {
    return maxShareMB.value();
  }
  
  public long getMaxShareVirtualCores() {
    return maxShareVCores.value();
  }

  public int getMaxApps() {
    return maxApps.value();
  }

  public void setMaxApps(int max) {
    maxApps.set(max);
  }

  /**
   * Get the maximum memory size AM can use in MB.
   *
   * @return the maximum memory size AM can use
   */
  public long getMaxAMShareMB() {
    return maxAMShareMB.value();
  }

  /**
   * Get the maximum number of VCores AM can use.
   *
   * @return the maximum number of VCores AM can use
   */
  public int getMaxAMShareVCores() {
    return maxAMShareVCores.value();
  }

  /**
   * Set the maximum resource AM can use.
   *
   * @param resource the maximum resource AM can use
   */
  public void setMaxAMShare(Resource resource) {
    maxAMShareMB.set(resource.getMemorySize());
    maxAMShareVCores.set(resource.getVirtualCores());
  }

  /**
   * Get the AM memory usage in MB.
   *
   * @return the AM memory usage
   */
  public long getAMResourceUsageMB() {
    return amResourceUsageMB.value();
  }

  /**
   * Get the AM VCore usage.
   *
   * @return the AM VCore usage
   */
  public int getAMResourceUsageVCores() {
    return amResourceUsageVCores.value();
  }

  /**
   * Set the AM resource usage.
   *
   * @param resource the AM resource usage
   */
  public void setAMResourceUsage(Resource resource) {
    amResourceUsageMB.set(resource.getMemorySize());
    amResourceUsageVCores.set(resource.getVirtualCores());
  }

  /**
   * Get the scheduling policy.
   *
   * @return the scheduling policy
   */
  @Metric("Scheduling policy")
  public String getSchedulingPolicy() {
    return schedulingPolicy;
  }

  public void setSchedulingPolicy(String policy) {
    schedulingPolicy = policy;
  }

  public synchronized
  static FSQueueMetrics forQueue(String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return forQueue(ms, queueName, parent, enableUserMetrics, conf);
  }

  /**
   * Get the FS queue metric for the given queue. Create one and register it to
   * metrics system if there isn't one for the queue.
   *
   * @param ms the metric system
   * @param queueName queue name
   * @param parent parent queue
   * @param enableUserMetrics  if user metrics is needed
   * @param conf configuration
   * @return an FSQueueMetrics object
   */
  @VisibleForTesting
  public synchronized
  static FSQueueMetrics forQueue(MetricsSystem ms, String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf) {
    QueueMetrics metrics = QueueMetrics.getQueueMetrics().get(queueName);
    if (metrics == null) {
      metrics = new FSQueueMetrics(ms, queueName, parent, enableUserMetrics, conf)
          .tag(QUEUE_INFO, queueName);

      // Register with the MetricsSystems
      if (ms != null) {
        metrics = ms.register(
            sourceName(queueName).toString(),
            "Metrics for queue: " + queueName, metrics);
      }
      QueueMetrics.getQueueMetrics().put(queueName, metrics);
    }

    return (FSQueueMetrics)metrics;
  }
}
