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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

@Metrics(context = "yarn")
public class CSQueueMetrics extends QueueMetrics {

  @Metric("AM memory limit in MB")
  MutableGaugeInt AMResourceLimitMB;
  @Metric("AM CPU limit in virtual cores")
  MutableGaugeInt AMResourceLimitVCores;
  @Metric("Used AM memory limit in MB")
  MutableGaugeInt usedAMResourceMB;
  @Metric("Used AM CPU limit in virtual cores")
  MutableGaugeInt usedAMResourceVCores;

  CSQueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    super(ms, queueName, parent, enableUserMetrics, conf);
  }

  public int getAMResourceLimitMB() {
    return AMResourceLimitMB.value();
  }

  public int getAMResourceLimitVCores() {
    return AMResourceLimitVCores.value();
  }

  public int getUsedAMResourceMB() {
    return usedAMResourceMB.value();
  }

  public int getUsedAMResourceVCores() {
    return usedAMResourceVCores.value();
  }

  public void setAMResouceLimit(Resource res) {
    AMResourceLimitMB.set(res.getMemory());
    AMResourceLimitVCores.set(res.getVirtualCores());
  }

  public void setAMResouceLimitForUser(String user, Resource res) {
    CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.setAMResouceLimit(res);
    }
  }

  public void incAMUsed(String user, Resource res) {
    usedAMResourceMB.incr(res.getMemory());
    usedAMResourceVCores.incr(res.getVirtualCores());
    CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.incAMUsed(user, res);
    }
  }

  public void decAMUsed(String user, Resource res) {
    usedAMResourceMB.decr(res.getMemory());
    usedAMResourceVCores.decr(res.getVirtualCores());
    CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.decAMUsed(user, res);
    }
  }

  public synchronized static CSQueueMetrics forQueue(String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    QueueMetrics metrics = queueMetrics.get(queueName);
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
      queueMetrics.put(queueName, metrics);
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
      metrics = new CSQueueMetrics(metricsSystem, queueName, null, false, conf);
      users.put(userName, metrics);
      metricsSystem.register(
          sourceName(queueName).append(",user=").append(userName).toString(),
          "Metrics for user '" + userName + "' in queue '" + queueName + "'",
          ((CSQueueMetrics) metrics.tag(QUEUE_INFO, queueName)).tag(USER_INFO,
              userName));
    }
    return metrics;
  }

}
