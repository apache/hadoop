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

  private static final RecordFactory recordFactory =
          RecordFactoryProvider.getRecordFactory(null);

  public CSQueueInfoProvider() {
  }

  public static QueueInfo getQueueInfo(AbstractCSQueue csQueue) {
    // Deliberately doesn't use lock here, because this method will be invoked
    // from schedulerApplicationAttempt, to avoid deadlock, sacrifice
    // consistency here.
    // TODO, improve this
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
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
    queueInfo.setWeight(csQueue.getWeight());
    queueInfo.setMaxParallelApps(csQueue.getMaxParallelApps());
    return queueInfo;
  }

  private static QueueStatistics getQueueStatistics(AbstractCSQueue csQueue) {
    // Deliberately doesn't use lock here, because this method will be invoked
    // from schedulerApplicationAttempt, to avoid deadlock, sacrifice
    // consistency here.
    // TODO, improve this
    QueueStatistics stats = recordFactory.newRecordInstance(
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
              recordFactory.newRecordInstance(QueueConfigurations.class);
      float capacity = csQueue.getCapacityByNodeLabel(nodeLabel);
      float absoluteCapacity = csQueue.getAbsoluteCapacityByNodeLabel(nodeLabel);
      float maxCapacity = csQueue.getMaximumCapacityByNodeLabel(nodeLabel);
      float absMaxCapacity =
              csQueue.getAbsoluteMaximumCapacityByNodeLabel(nodeLabel);
      float maxAMPercentage =
              csQueue.getMaxAMResourcePercentageByNodeLabel(nodeLabel);
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
