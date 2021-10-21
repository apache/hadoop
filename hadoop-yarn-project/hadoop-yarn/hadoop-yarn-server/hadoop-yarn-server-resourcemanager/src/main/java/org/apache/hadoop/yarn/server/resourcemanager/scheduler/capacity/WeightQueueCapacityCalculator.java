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
              .incrementWeight(label, resourceName, childQueue.getConfiguredCapacityVector(label).getResource(resourceName).getResourceValue());
        }
      }
    }
  }

  @Override
  protected long calculateMinimumResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label, QueueCapacityVectorEntry capacityVectorEntry) {
    CSQueue parentQueue = childQueue.getParent();
    String resourceName = capacityVectorEntry.getResourceName();
    float normalizedWeight = capacityVectorEntry.getResourceValue()
        / updateContext.getQueueBranchContext(parentQueue.getQueuePath())
        .getSumWeightsByResource(label, resourceName);

    float remainingPerEffectiveResourceRatio = updateContext.getQueueBranchContext(
            parentQueue.getQueuePath()).getRemainingResource(label)
        .getValue(resourceName) / parentQueue.getEffectiveCapacity(label)
        .getResourceValue(resourceName);

    float parentAbsoluteCapacity = updateContext.getRelativeResourceRatio(
        parentQueue.getQueuePath(), label).getValue(resourceName);
    float queueAbsoluteCapacity = parentAbsoluteCapacity *
        remainingPerEffectiveResourceRatio * normalizedWeight;
    long resource = (long) Math.floor(updateContext.getUpdatedClusterResource(label)
        .getResourceValue(resourceName)
        * queueAbsoluteCapacity);

    updateContext.getRelativeResourceRatio(childQueue.getQueuePath(), label)
        .setValue(capacityVectorEntry.getResourceName(),
            queueAbsoluteCapacity);

    return resource;
  }

  @Override
  protected long calculateMaximumResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label, QueueCapacityVectorEntry capacityVectorEntry) {
    return 0;
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
      sumNormalizedWeight += updateContext.getRelativeResourceRatio(
          queue.getQueuePath(), label).getValue(resourceName);
    }

    queue.getQueueCapacities().setNormalizedWeight(label, sumNormalizedWeight);
  }

}
