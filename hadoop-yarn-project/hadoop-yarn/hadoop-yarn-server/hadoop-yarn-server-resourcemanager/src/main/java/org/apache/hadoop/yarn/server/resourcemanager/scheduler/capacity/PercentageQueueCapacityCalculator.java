package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;

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
  public void calculateChildQueueResources(
      QueueHierarchyUpdateContext updateContext, CSQueue parentQueue) {
    super.calculateChildQueueResources(updateContext, parentQueue);

    setChildrenResources(
        parentQueue, updateContext, ((childQueue, label, capacityVectorEntry) -> {
          String resourceName = capacityVectorEntry.getResourceName();
          float parentAbsoluteCapacity = updateContext.getRelativeResourceRatio(
              parentQueue.getQueuePath(), label).getValue(resourceName);
          float remainingPerEffectiveResourceRatio = updateContext.getQueueBranchContext(
                  parentQueue.getQueuePath()).getRemainingResource(label)
              .getValue(resourceName) / parentQueue.getEffectiveCapacity(label)
              .getResourceValue(resourceName);
          float queueAbsoluteCapacity = parentAbsoluteCapacity *
              remainingPerEffectiveResourceRatio
              * capacityVectorEntry.getResourceValue() / 100;

          long resource = (long) Math.floor(updateContext.getUpdatedClusterResource(label)
              .getResourceValue(capacityVectorEntry.getResourceName())
              * queueAbsoluteCapacity);

          updateContext.getRelativeResourceRatio(childQueue.getQueuePath(), label)
              .setValue(capacityVectorEntry.getResourceName(),
                  queueAbsoluteCapacity);
          return resource;
        }));
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
