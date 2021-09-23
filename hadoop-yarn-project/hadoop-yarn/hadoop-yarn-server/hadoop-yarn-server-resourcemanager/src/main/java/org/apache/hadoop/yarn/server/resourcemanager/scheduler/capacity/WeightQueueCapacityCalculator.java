package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType.PERCENTAGE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType.WEIGHT;

public class WeightQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public void calculateChildQueueResources(QueueHierarchyUpdateContext updateContext, CSQueue parentQueue, String label) {

  }

  @Override
  protected QueueCapacityVector.QueueCapacityType getCapacityType() {
    return null;
  }

  @Override
  public void setup(CSQueue queue, CapacitySchedulerConfiguration conf, String label) {
    float sumWeight = 0;
    QueueCapacityVector capacityVector = queue.getConfiguredCapacityVector(label);
    for (String resourceName : capacityVector.getResourceNamesByCapacityType(WEIGHT)) {
      sumWeight += capacityVector.getResource(resourceName).getResourceValue();
    }

    queue.getQueueCapacities().setWeight(label, sumWeight);
  }

  public void calculateResources(QueueHierarchyUpdateContext updateContext, CSQueue queue, QueueCapacityVectorEntry configuredCapacityResource, String label) {
    QueueBranchContext.CapacitySum capacitySum = updateContext.getQueueBranchContext(queue.getParent().getQueuePath()).getSumByLabel(label);

    float capacityMultiplier = 1 - capacitySum.getSum(configuredCapacityResource.getResourceName(), PERCENTAGE);
    float normalizedWeight = configuredCapacityResource.getResourceValue() /
        capacitySum.getSum(configuredCapacityResource.getResourceName(), WEIGHT)
        * capacityMultiplier;

    float parentAbsoluteCapacity = queue.getParent() != null ?
        updateContext.getRelativeResourceRatio(
            queue.getParent().getQueuePath(), label).getValue(
                configuredCapacityResource.getResourceName()) : 1;
    float queueAbsoluteCapacity = parentAbsoluteCapacity * normalizedWeight;
    long resource = Math.round(queueAbsoluteCapacity *
        updateContext.getUpdatedClusterResource().getResourceValue(
            configuredCapacityResource.getResourceName()));

    queue.getQueueResourceQuotas().getEffectiveMinResource(label)
        .setResourceValue(configuredCapacityResource.getResourceName(), resource);

    updateContext.getRelativeResourceRatio(queue.getQueuePath(), label).setValue(
        configuredCapacityResource.getResourceName(), queueAbsoluteCapacity);
  }

  @Override
  public void setMetrics(QueueHierarchyUpdateContext updateContext, CSQueue queue, String label) {
    float sumNormalizedWeight = 0;
    for (String resourceName : queue.getConfiguredCapacityVector(label).getResourceNamesByCapacityType(WEIGHT)) {
      sumNormalizedWeight += updateContext.getRelativeResourceRatio(queue.getQueuePath(), label).getValue(resourceName);
    }

    queue.getQueueCapacities().setNormalizedWeight(label, sumNormalizedWeight);
  }

}
