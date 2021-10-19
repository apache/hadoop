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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeFloat;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.CSQueueMetricsForCustomResources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

@Metrics(context = "yarn")
public class CSQueueMetrics extends QueueMetrics {

  //Metrics updated only for "default" partition
  @Metric("AM memory limit in MB")
  MutableGaugeLong AMResourceLimitMB;
  @Metric("AM CPU limit in virtual cores")
  MutableGaugeLong AMResourceLimitVCores;
  @Metric("Used AM memory limit in MB")
  MutableGaugeLong usedAMResourceMB;
  @Metric("Used AM CPU limit in virtual cores")
  MutableGaugeLong usedAMResourceVCores;
  @Metric("Percent of Capacity Used")
  MutableGaugeFloat usedCapacity;
  @Metric("Percent of Absolute Capacity Used")
  MutableGaugeFloat absoluteUsedCapacity;
  @Metric("Guaranteed memory in MB")
  MutableGaugeLong guaranteedMB;
  @Metric("Guaranteed CPU in virtual cores")
  MutableGaugeInt guaranteedVCores;
  @Metric("Maximum memory in MB")
  MutableGaugeLong maxCapacityMB;
  @Metric("Maximum CPU in virtual cores")
  MutableGaugeInt maxCapacityVCores;
  @Metric("Guaranteed capacity in percentage relative to parent")
  private MutableGaugeFloat guaranteedCapacity;
  @Metric("Guaranteed capacity in percentage relative to total partition")
  private MutableGaugeFloat guaranteedAbsoluteCapacity;
  @Metric("Maximum capacity in percentage relative to parent")
  private MutableGaugeFloat maxCapacity;
  @Metric("Maximum capacity in percentage relative to total partition")
  private MutableGaugeFloat maxAbsoluteCapacity;

  private static final String GUARANTEED_CAPACITY_METRIC_PREFIX =
      "GuaranteedCapacity.";
  private static final String GUARANTEED_CAPACITY_METRIC_DESC =
      "GuaranteedCapacity of NAME";

  private static final String MAX_CAPACITY_METRIC_PREFIX =
      "MaxCapacity.";
  private static final String MAX_CAPACITY_METRIC_DESC =
      "MaxCapacity of NAME";

  private CSQueueMetricsForCustomResources csQueueMetricsForCustomResources;

  CSQueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    super(ms, queueName, parent, enableUserMetrics, conf);
  }

  /**
   * Register all custom resources metrics as part of initialization. As and
   * when this metric object construction happens for any queue, all custom
   * resource metrics value would be initialized with '0' like any other
   * mandatory resources metrics
   */
  protected void registerCustomResources() {
    Map<String, Long> customResources =
        csQueueMetricsForCustomResources.initAndGetCustomResources();
    csQueueMetricsForCustomResources
        .registerCustomResources(customResources, this.registry,
            GUARANTEED_CAPACITY_METRIC_PREFIX, GUARANTEED_CAPACITY_METRIC_DESC);
    csQueueMetricsForCustomResources
        .registerCustomResources(customResources, this.registry,
            MAX_CAPACITY_METRIC_PREFIX, MAX_CAPACITY_METRIC_DESC);
    super.registerCustomResources();
  }

  public long getAMResourceLimitMB() {
    return AMResourceLimitMB.value();
  }

  public long getAMResourceLimitVCores() {
    return AMResourceLimitVCores.value();
  }

  public long getUsedAMResourceMB() {
    return usedAMResourceMB.value();
  }

  public long getUsedAMResourceVCores() {
    return usedAMResourceVCores.value();
  }

  public void setAMResouceLimit(String partition, Resource res) {
    if(partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      AMResourceLimitMB.set(res.getMemorySize());
      AMResourceLimitVCores.set(res.getVirtualCores());
    }
  }

  public void setAMResouceLimitForUser(String partition,
      String user, Resource res) {
    CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.setAMResouceLimit(partition, res);
    }
  }

  public void incAMUsed(String partition, String user, Resource res) {
    if(partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      usedAMResourceMB.incr(res.getMemorySize());
      usedAMResourceVCores.incr(res.getVirtualCores());
      CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
      if (userMetrics != null) {
        userMetrics.incAMUsed(partition, user, res);
      }
    }
  }

  public void decAMUsed(String partition, String user, Resource res) {
    if(partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      usedAMResourceMB.decr(res.getMemorySize());
      usedAMResourceVCores.decr(res.getVirtualCores());
      CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
      if (userMetrics != null) {
        userMetrics.decAMUsed(partition, user, res);
      }
    }
  }

  public float getUsedCapacity() {
    return usedCapacity.value();
  }

  public void setUsedCapacity(String partition, float usedCap) {
    if(partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      this.usedCapacity.set(usedCap);
    }
  }

  public float getAbsoluteUsedCapacity() {
    return absoluteUsedCapacity.value();
  }

  public void setAbsoluteUsedCapacity(String partition,
      Float absoluteUsedCap) {
    if(partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      this.absoluteUsedCapacity.set(absoluteUsedCap);
    }
  }

  public long getGuaranteedMB() {
    return guaranteedMB.value();
  }

  public int getGuaranteedVCores() {
    return guaranteedVCores.value();
  }

  public void setGuaranteedResources(String partition, Resource res) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      guaranteedMB.set(res.getMemorySize());
      guaranteedVCores.set(res.getVirtualCores());
      if (csQueueMetricsForCustomResources != null) {
        csQueueMetricsForCustomResources.setGuaranteedCapacity(res);
        csQueueMetricsForCustomResources.registerCustomResources(
            csQueueMetricsForCustomResources.getGuaranteedCapacity(), registry,
            GUARANTEED_CAPACITY_METRIC_PREFIX, GUARANTEED_CAPACITY_METRIC_DESC);
      }
    }
  }

  public long getMaxCapacityMB() {
    return maxCapacityMB.value();
  }

  public int getMaxCapacityVCores() {
    return maxCapacityVCores.value();
  }

  public void setMaxCapacityResources(String partition, Resource res) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      maxCapacityMB.set(res.getMemorySize());
      maxCapacityVCores.set(res.getVirtualCores());
      if (csQueueMetricsForCustomResources != null) {
        csQueueMetricsForCustomResources.setMaxCapacity(res);
        csQueueMetricsForCustomResources.registerCustomResources(
            csQueueMetricsForCustomResources.getMaxCapacity(), registry,
            MAX_CAPACITY_METRIC_PREFIX, MAX_CAPACITY_METRIC_DESC);
      }
    }
  }

  @Override
  protected void createQueueMetricsForCustomResources() {
    if (ResourceUtils.getNumberOfKnownResourceTypes() > 2) {
      this.csQueueMetricsForCustomResources =
          new CSQueueMetricsForCustomResources();
      setQueueMetricsForCustomResources(csQueueMetricsForCustomResources);
      registerCustomResources();
    }
  }

  public synchronized static CSQueueMetrics forQueue(String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    QueueMetrics metrics = getQueueMetrics().get(queueName);
    if (metrics == null) {
      metrics =
          new CSQueueMetrics(ms, queueName, parent, enableUserMetrics, conf)
              .tag(QUEUE_INFO, queueName);

      // Register with the MetricsSystems
      if (ms != null) {
        metrics =
            ms.register(sourceName(queueName).toString(), "Metrics for queue: "
                + queueName, metrics);
      }
      getQueueMetrics().put(queueName, metrics);
    }

    return (CSQueueMetrics) metrics;
  }

  @Override
  public synchronized QueueMetrics getUserMetrics(String userName) {
    if (users == null) {
      return null;
    }
    CSQueueMetrics metrics = (CSQueueMetrics) users.get(userName);
    if (metrics == null) {
      metrics =
        new CSQueueMetrics(metricsSystem, queueName, null, false, conf);
      users.put(userName, metrics);
      metricsSystem.register(
          sourceName(queueName).append(",user=").append(userName).toString(),
          "Metrics for user '" + userName + "' in queue '" + queueName + "'",
          ((CSQueueMetrics) metrics.tag(QUEUE_INFO, queueName)).tag(USER_INFO,
              userName));
    }
    return metrics;
  }

  public float getGuaranteedCapacity() {
    return guaranteedCapacity.value();
  }

  public float getGuaranteedAbsoluteCapacity() {
    return guaranteedAbsoluteCapacity.value();
  }

  public void setGuaranteedCapacities(String partition, float capacity,
      float absoluteCapacity) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      guaranteedCapacity.set(capacity);
      guaranteedAbsoluteCapacity.set(absoluteCapacity);
    }
  }

  public float getMaxCapacity() {
    return maxCapacity.value();
  }

  public float getMaxAbsoluteCapacity() {
    return maxAbsoluteCapacity.value();
  }

  public void setMaxCapacities(String partition, float capacity,
      float absoluteCapacity) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      maxCapacity.set(capacity);
      maxAbsoluteCapacity.set(absoluteCapacity);
    }
  }
}
