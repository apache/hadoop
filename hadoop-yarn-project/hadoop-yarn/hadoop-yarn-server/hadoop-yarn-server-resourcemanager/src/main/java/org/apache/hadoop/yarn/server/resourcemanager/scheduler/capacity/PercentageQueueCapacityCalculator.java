package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;

import java.util.Set;

public class PercentageQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public void setup(CSQueue queue, CapacitySchedulerConfiguration conf, String label) {
    float sumCapacity = 0f;
    QueueCapacityVector capacityVector =
        queue.getConfiguredCapacityVector(label);
    for (String resourceName : getResourceNames(queue, label)) {
      sumCapacity += capacityVector.getResource(resourceName).getResourceValue();
    }
  }

  @Override
  public void calculateChildQueueResources(
      QueueHierarchyUpdateContext updateContext, CSQueue parentQueue, String label) {
    ResourceVector aggregatedUsedResource = new ResourceVector();
    for (CSQueue childQueue : parentQueue.getChildQueues()) {
      for (String resourceName : getResourceNames(childQueue, label)) {
        QueueCapacityVectorEntry configuredCapacityResource = childQueue
            .getConfiguredCapacityVector(label).getResource(resourceName);

        float parentAbsoluteCapacity = updateContext.getRelativeResourceRatio(
            parentQueue.getQueuePath(), label).getValue(resourceName);
        float queueAbsoluteCapacity = parentAbsoluteCapacity
            * configuredCapacityResource.getResourceValue() / 100;
        float remainingPerEffectiveResourceRatio = updateContext.getQueueBranchContext(
                parentQueue.getQueuePath()).getRemainingResource(label)
            .getValue(resourceName) / parentQueue.getEffectiveCapacity(label)
            .getResourceValue(resourceName);
        queueAbsoluteCapacity *= remainingPerEffectiveResourceRatio;

        long resource = Math.round(updateContext.getUpdatedClusterResource()
            .getResourceValue(configuredCapacityResource.getResourceName())
            * queueAbsoluteCapacity);
        childQueue.getQueueResourceQuotas().getEffectiveMinResource(label)
            .setResourceValue(configuredCapacityResource.getResourceName(),
                resource);

        updateContext.getRelativeResourceRatio(childQueue.getQueuePath(), label)
            .setValue(configuredCapacityResource.getResourceName(),
                queueAbsoluteCapacity);
        aggregatedUsedResource.setValue(
            configuredCapacityResource.getResourceName(), resource);
      }
    }

    updateContext.getQueueBranchContext(parentQueue.getQueuePath())
        .getRemainingResource(label).subtract(aggregatedUsedResource);
  }

  @Override
  public void setMetrics(
      QueueHierarchyUpdateContext updateContext, CSQueue queue, String label) {
    float sumAbsoluteCapacity = 0f;
    Set<String> resources = getResourceNames(queue, label);
    for (String resourceName : resources) {
      sumAbsoluteCapacity += updateContext.getRelativeResourceRatio(
          queue.getQueuePath(), label).getValue(resourceName);
    }

    queue.getQueueCapacities().setAbsoluteCapacity(sumAbsoluteCapacity
        / resources.size());
  }

  @Override
  protected QueueCapacityType getCapacityType() {
    return QueueCapacityType.PERCENTAGE;
  }
}
