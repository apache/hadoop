package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;

import java.util.Set;

public class PercentageQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public void setup(CSQueue queue, String label) {
    float sumCapacity = 0f;
    QueueCapacityVector capacityVector =
        queue.getConfiguredCapacityVector(label);
    for (String resourceName : getResourceNames(queue, label)) {
      sumCapacity += capacityVector.getResource(resourceName).getResourceValue();
    }
  }

  @Override
  protected float calculateMinimumResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry) {
    CSQueue parentQueue = childQueue.getParent();
    String resourceName = capacityVectorEntry.getResourceName();

    float parentAbsoluteCapacity = updateContext.getAbsoluteMinCapacity(
        parentQueue.getQueuePath(), label).getValue(resourceName);
    float remainingPerEffectiveResourceRatio = updateContext.getQueueBranchContext(
            parentQueue.getQueuePath()).getRemainingResource(label)
        .getValue(resourceName) / parentQueue.getEffectiveCapacity(label)
        .getResourceValue(resourceName);
    float absoluteCapacity = parentAbsoluteCapacity *
        remainingPerEffectiveResourceRatio
        * capacityVectorEntry.getResourceValue() / 100;

    return updateContext.getUpdatedClusterResource(label).getResourceValue(resourceName)
        * absoluteCapacity;
}

  @Override
  protected float calculateMaximumResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry) {
    CSQueue parentQueue = childQueue.getParent();
    String resourceName = capacityVectorEntry.getResourceName();

    float parentAbsoluteMaxCapacity = updateContext.getAbsoluteMaxCapacity(
        parentQueue.getQueuePath(), label).getValue(resourceName);
    float absoluteMaxCapacity = parentAbsoluteMaxCapacity
        * capacityVectorEntry.getResourceValue() / 100;

    return updateContext.getUpdatedClusterResource(label).getResourceValue(
        capacityVectorEntry.getResourceName()) * absoluteMaxCapacity;
  }

  @Override
  public void setMetrics(
      QueueHierarchyUpdateContext updateContext, CSQueue queue, String label) {
    float sumAbsoluteCapacity = 0f;
    Set<String> resources = getResourceNames(queue, label);
    for (String resourceName : resources) {
      sumAbsoluteCapacity += updateContext.getAbsoluteMinCapacity(
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
