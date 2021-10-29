package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType.WEIGHT;

public class WeightQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  protected void calculateResourcePrerequisites(QueueHierarchyUpdateContext updateContext, CSQueue parentQueue) {
    super.calculateResourcePrerequisites(updateContext, parentQueue);

    for (CSQueue childQueue : parentQueue.getChildQueues()) {
      for (String label : childQueue.getConfiguredNodeLabels()) {
        for (String resourceName : childQueue.getConfiguredCapacityVector(label)
            .getResourceNamesByCapacityType(getCapacityType())) {
          updateContext.getQueueBranchContext(parentQueue.getQueuePath())
              .incrementWeight(label, resourceName, childQueue.getConfiguredCapacityVector(label)
                  .getResource(resourceName).getResourceValue());
          updateContext.getQueueBranchContext(parentQueue.getQueuePath()).incrementMaxWeight(
              label, resourceName, childQueue.getConfiguredMaximumCapacityVector(label)
                  .getResource(resourceName).getResourceValue());
        }
      }
    }
  }

  @Override
  protected float calculateMinimumResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry) {
    CSQueue parentQueue = childQueue.getParent();
    String resourceName = capacityVectorEntry.getResourceName();
    float normalizedWeight = capacityVectorEntry.getResourceValue()
        / updateContext.getQueueBranchContext(parentQueue.getQueuePath())
        .getSumWeightsByResource(label, resourceName);

    float remainingResource = updateContext.getQueueBranchContext(
            parentQueue.getQueuePath()).getRemainingResource(label)
        .getValue(resourceName);
    float remainingPerEffectiveResourceRatio = remainingResource / parentQueue.getEffectiveCapacity(
        label).getResourceValue(resourceName);

    float parentAbsoluteCapacity = updateContext.getAbsoluteMinCapacity(
        parentQueue.getQueuePath(), label).getValue(resourceName);
    float queueAbsoluteCapacity = parentAbsoluteCapacity *
        remainingPerEffectiveResourceRatio * normalizedWeight;

    // Due to rounding loss it is better to use all remaining resources
    // if no other resource uses weight
    if (normalizedWeight == 1) {
      return remainingResource;
    }

    return updateContext.getUpdatedClusterResource(label).getResourceValue(resourceName)
        * queueAbsoluteCapacity;
  }

  @Override
  protected float calculateMaximumResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry) {
    CSQueue parentQueue = childQueue.getParent();
    String resourceName = capacityVectorEntry.getResourceName();
    float normalizedMaxWeight = capacityVectorEntry.getResourceValue()
        / updateContext.getQueueBranchContext(parentQueue.getQueuePath())
        .getSumMaxWeightsByResource(label, resourceName);

    float parentAbsoluteMaxCapacity = updateContext.getAbsoluteMaxCapacity(
        parentQueue.getQueuePath(), label).getValue(resourceName);
    float absoluteMaxCapacity = parentAbsoluteMaxCapacity * normalizedMaxWeight;

    return updateContext.getUpdatedClusterResource(label).getResourceValue(resourceName)
        * absoluteMaxCapacity;
  }

  @Override
  protected QueueCapacityType getCapacityType() {
    return WEIGHT;
  }

  @Override
  public void setup(CSQueue queue, String label) {
    queue.getQueueCapacities().setWeight(label, sumCapacityValues(queue, label));
  }

  @Override
  public void setMetrics(QueueHierarchyUpdateContext updateContext,
                         CSQueue queue, String label) {
    float sumNormalizedWeight = 0;
    for (String resourceName : getResourceNames(queue, label)) {
      sumNormalizedWeight += updateContext.getAbsoluteMinCapacity(
          queue.getQueuePath(), label).getValue(resourceName);
    }

    queue.getQueueCapacities().setNormalizedWeight(label, sumNormalizedWeight);
  }

}
