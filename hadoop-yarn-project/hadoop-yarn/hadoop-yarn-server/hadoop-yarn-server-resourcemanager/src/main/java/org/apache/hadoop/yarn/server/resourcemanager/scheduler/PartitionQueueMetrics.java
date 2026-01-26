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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;

@Metrics(context = "yarn")
public class PartitionQueueMetrics extends QueueMetrics {

  private String partition;

  protected PartitionQueueMetrics(MetricsSystem ms, String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf,
      String partition) {
    super(ms, queueName, parent, enableUserMetrics, conf);
    this.partition = partition;
    if (getParentQueue() != null) {
      String newQueueName = (getParentQueue() instanceof CSQueue)
          ? ((CSQueue) getParentQueue()).getQueuePath()
          : getParentQueue().getQueueName();
      String parentMetricName =
          partition + METRIC_NAME_DELIMITER + newQueueName;
      setParent(getQueueMetrics().get(parentMetricName));
      storedPartitionMetrics = null;
    }
  }

  /**
   * Partition * Queue * User Metrics
   *
   * Computes Metrics at Partition (Node Label) * Queue * User Level.
   *
   * Sample JMX O/P Structure:
   *
   * PartitionQueueMetrics (labelX)
   *  QueueMetrics (A)
   *    usermetrics
   *  QueueMetrics (A1)
   *    usermetrics
   *    QueueMetrics (A2)
   *      usermetrics
   *    QueueMetrics (B)
   *      usermetrics
   *
   * @return QueueMetrics
   */
  @Override
  public synchronized QueueMetrics getUserMetrics(String userName) {
    if (users == null) {
      return null;
    }

    String partitionJMXStr =
        (partition.equals(DEFAULT_PARTITION)) ? DEFAULT_PARTITION_JMX_STR
            : partition;

    QueueMetrics metrics = (PartitionQueueMetrics) users.get(userName);
    if (metrics == null) {
      metrics = new PartitionQueueMetrics(this.metricsSystem, this.queueName,
          null, false, this.conf, this.partition);
      users.put(userName, metrics);
      metricsSystem.register(
          pSourceName(partitionJMXStr).append(qSourceName(queueName))
              .append(",user=").append(userName).toString(),
          "Metrics for user '" + userName + "' in queue '" + queueName + "'",
          ((PartitionQueueMetrics) metrics.tag(PARTITION_INFO, partitionJMXStr)
              .tag(QUEUE_INFO, queueName)).tag(USER_INFO, userName));
    }
    return metrics;
  }
}
