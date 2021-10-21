package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;

public class AbsoluteResourceCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  protected long calculateMinimumResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry) {
    String resourceName = capacityVectorEntry.getResourceName();
    ResourceVector ratio = updateContext.getNormalizedMinResourceRatio(
        childQueue.getParent().getQueuePath(), label);

    long resource = (long) Math.floor(ratio.getValue(resourceName)
        * capacityVectorEntry.getResourceValue());

    float absolutePercentage = (float) resource
        / updateContext.getUpdatedClusterResource(label).getResourceValue(resourceName);

    updateContext.getRelativeResourceRatio(childQueue.getQueuePath(),
        label).setValue(resourceName, absolutePercentage);
    return resource;
  }

  @Override
  protected long calculateMaximumResource(QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label, QueueCapacityVectorEntry capacityVectorEntry) {
    return 0;
  }

  @Override
  protected QueueCapacityType getCapacityType() {
    return QueueCapacityType.ABSOLUTE;
  }

  @Override
  public void setup(CSQueue queue, String label) {
    Resource minResource = Resource.newInstance(0, 0);

    for (String resourceName : getResourceNames(queue, label)) {
      long resource = (long) queue.getConfiguredCapacityVector(
          label).getResource(resourceName).getResourceValue();
      minResource.setResourceValue(resourceName, minResource.getResourceValue(
          resourceName) + resource);
    }

    queue.getQueueResourceQuotas().setConfiguredMinResource(label, minResource);
  }

  @Override
  public void setMetrics(QueueHierarchyUpdateContext updateContext,
                         CSQueue queue, String label) {
    float sumCapacity = 0f;
    float sumAbsoluteCapacity = 0f;

    for (String resourceName : getResourceNames(queue, label)) {
      sumCapacity += queue.getConfiguredCapacityVector(label).getResource(
          resourceName).getResourceValue() / queue.getParent()
          .getQueueResourceQuotas().getEffectiveMinResource(label)
          .getResourceValue(resourceName);
      sumAbsoluteCapacity += updateContext.getRelativeResourceRatio(
          queue.getQueuePath(), label).getValue(resourceName);
    }

    queue.getQueueCapacities().setCapacity(label, sumCapacity);
    queue.getQueueCapacities().setAbsoluteCapacity(label, sumAbsoluteCapacity);
  }

}
