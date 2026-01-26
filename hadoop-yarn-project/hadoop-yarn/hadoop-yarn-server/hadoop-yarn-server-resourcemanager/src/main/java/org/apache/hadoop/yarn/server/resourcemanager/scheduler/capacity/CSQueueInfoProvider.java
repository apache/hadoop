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

import org.apache.hadoop.yarn.api.records.QueueConfigurations;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class CSQueueInfoProvider {

  private static final RecordFactory RECORD_FACTORY =
          RecordFactoryProvider.getRecordFactory(null);

  private CSQueueInfoProvider() {
  }

  public static QueueInfo getQueueInfo(AbstractCSQueue csQueue) {
    QueueInfo queueInfo = RECORD_FACTORY.newRecordInstance(QueueInfo.class);
    queueInfo.setSchedulerType("CapacityScheduler");
    queueInfo.setQueueName(csQueue.getQueuePathObject().getLeafName());
    queueInfo.setQueuePath(csQueue.getQueuePathObject().getFullPath());
    queueInfo.setAccessibleNodeLabels(csQueue.getAccessibleNodeLabels());
    queueInfo.setCapacity(csQueue.getCapacity());
    queueInfo.setMaximumCapacity(csQueue.getMaximumCapacity());
    queueInfo.setQueueState(csQueue.getState());
    queueInfo.setDefaultNodeLabelExpression(csQueue.getDefaultNodeLabelExpression());
    queueInfo.setCurrentCapacity(csQueue.getUsedCapacity());
    queueInfo.setQueueStatistics(getQueueStatistics(csQueue));
    queueInfo.setPreemptionDisabled(csQueue.getPreemptionDisabled());
    queueInfo.setIntraQueuePreemptionDisabled(
            csQueue.getIntraQueuePreemptionDisabled());
    queueInfo.setQueueConfigurations(getQueueConfigurations(csQueue));
    queueInfo.setWeight(csQueue.getQueueCapacities().getWeight());
    queueInfo.setMaxParallelApps(csQueue.getMaxParallelApps());
    return queueInfo;
  }

  private static QueueStatistics getQueueStatistics(AbstractCSQueue csQueue) {
    QueueStatistics stats = RECORD_FACTORY.newRecordInstance(
            QueueStatistics.class);
    CSQueueMetrics queueMetrics = csQueue.getMetrics();
    stats.setNumAppsSubmitted(queueMetrics.getAppsSubmitted());
    stats.setNumAppsRunning(queueMetrics.getAppsRunning());
    stats.setNumAppsPending(queueMetrics.getAppsPending());
    stats.setNumAppsCompleted(queueMetrics.getAppsCompleted());
    stats.setNumAppsKilled(queueMetrics.getAppsKilled());
    stats.setNumAppsFailed(queueMetrics.getAppsFailed());
    stats.setNumActiveUsers(queueMetrics.getActiveUsers());
    stats.setAvailableMemoryMB(queueMetrics.getAvailableMB());
    stats.setAllocatedMemoryMB(queueMetrics.getAllocatedMB());
    stats.setPendingMemoryMB(queueMetrics.getPendingMB());
    stats.setReservedMemoryMB(queueMetrics.getReservedMB());
    stats.setAvailableVCores(queueMetrics.getAvailableVirtualCores());
    stats.setAllocatedVCores(queueMetrics.getAllocatedVirtualCores());
    stats.setPendingVCores(queueMetrics.getPendingVirtualCores());
    stats.setReservedVCores(queueMetrics.getReservedVirtualCores());
    stats.setPendingContainers(queueMetrics.getPendingContainers());
    stats.setAllocatedContainers(queueMetrics.getAllocatedContainers());
    stats.setReservedContainers(queueMetrics.getReservedContainers());
    return stats;
  }

  private static Map<String, QueueConfigurations> getQueueConfigurations(AbstractCSQueue csQueue) {
    Map<String, QueueConfigurations> queueConfigurations = new HashMap<>();
    Set<String> nodeLabels = csQueue.getNodeLabelsForQueue();
    QueueResourceQuotas queueResourceQuotas = csQueue.getQueueResourceQuotas();
    for (String nodeLabel : nodeLabels) {
      QueueConfigurations queueConfiguration =
              RECORD_FACTORY.newRecordInstance(QueueConfigurations.class);
      QueueCapacities queueCapacities = csQueue.getQueueCapacities();
      float capacity = queueCapacities.getCapacity(nodeLabel);
      float absoluteCapacity = queueCapacities.getAbsoluteCapacity(nodeLabel);
      float maxCapacity = queueCapacities.getMaximumCapacity(nodeLabel);
      float absMaxCapacity =
              queueCapacities.getAbsoluteMaximumCapacity(nodeLabel);
      float maxAMPercentage =
              queueCapacities.getMaxAMResourcePercentage(nodeLabel);
      queueConfiguration.setCapacity(capacity);
      queueConfiguration.setAbsoluteCapacity(absoluteCapacity);
      queueConfiguration.setMaxCapacity(maxCapacity);
      queueConfiguration.setAbsoluteMaxCapacity(absMaxCapacity);
      queueConfiguration.setMaxAMPercentage(maxAMPercentage);
      queueConfiguration.setConfiguredMinCapacity(
              queueResourceQuotas.getConfiguredMinResource(nodeLabel));
      queueConfiguration.setConfiguredMaxCapacity(
              queueResourceQuotas.getConfiguredMaxResource(nodeLabel));
      queueConfiguration.setEffectiveMinCapacity(
              queueResourceQuotas.getEffectiveMinResource(nodeLabel));
      queueConfiguration.setEffectiveMaxCapacity(
              queueResourceQuotas.getEffectiveMaxResource(nodeLabel));
      queueConfigurations.put(nodeLabel, queueConfiguration);
    }
    return queueConfigurations;
  }
}
