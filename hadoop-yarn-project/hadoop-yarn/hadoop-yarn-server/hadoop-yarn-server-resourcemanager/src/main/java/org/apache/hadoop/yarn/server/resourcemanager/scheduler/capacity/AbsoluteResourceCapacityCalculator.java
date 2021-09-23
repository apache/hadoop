package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;

public class AbsoluteResourceCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public void calculateChildQueueResources(QueueHierarchyUpdateContext updateContext, CSQueue parentQueue, String label) {

  }

  @Override
  protected QueueCapacityVector.QueueCapacityType getCapacityType() {
    return null;
  }

  @Override
  public void setup(CSQueue queue, CapacitySchedulerConfiguration conf, String label) {
    Resource minResource = Resource.newInstance(0, 0);

    for (String resourceName : queue.getConfiguredCapacityVector(label).getResourceNamesByCapacityType(QueueCapacityVector.QueueCapacityType.ABSOLUTE)) {
      minResource.setResourceValue(resourceName, (long) queue.getConfiguredCapacityVector(
          label).getResource(resourceName).getResourceValue());
    }

    queue.getQueueResourceQuotas().setEffectiveMinResource(label, minResource);
  }

  public void calculateResources(QueueHierarchyUpdateContext updateContext,
                                 CSQueue queue,
                                 QueueCapacityVectorEntry configuredCapacityResource,
                                 String label) {
    if (!CollectionUtils.isEmpty(queue.getChildQueues()) && queue instanceof AbstractCSQueue) {
      defineNormalizedResourceRatio(updateContext, queue, configuredCapacityResource, label);
    }

    ResourceVector ratio = ResourceVector.of(1f);

    if (!queue.getQueuePath().equals(ROOT)) {
      ratio = updateContext.getNormalizedMinResourceRatio(
          queue.getParent().getQueuePath(), label);
    }

    queue.getQueueResourceQuotas().getEffectiveMinResource(label)
        .setResourceValue(configuredCapacityResource.getResourceName(),
        (long) (ratio.getValue(configuredCapacityResource.getResourceName()) * configuredCapacityResource.getResourceValue()));

    float relativeToParentPercentage = (float) queue.getQueueResourceQuotas()
        .getEffectiveMinResource(label).getResourceValue(
            configuredCapacityResource.getResourceName())
        / queue.getParent().getQueueResourceQuotas().getEffectiveMinResource(
            label).getResourceValue(configuredCapacityResource.getResourceName());

    updateContext.getRelativeResourceRatio(queue.getQueuePath(), label).setValue(configuredCapacityResource.getResourceName(), relativeToParentPercentage);
  }

  @Override
  public void setMetrics(QueueHierarchyUpdateContext updateContext,
                         CSQueue queue, String label) {
    float sumCapacity = 0f;
    float sumAbsoluteCapacity = 0f;

    for (String resourceName : queue.getConfiguredCapacityVector(label).getResourceNamesByCapacityType(QueueCapacityVector.QueueCapacityType.ABSOLUTE)) {
      sumCapacity += updateContext.getRelativeResourceRatio(queue.getQueuePath(), label).getValue(resourceName);
      sumAbsoluteCapacity += (float) queue.getQueueResourceQuotas().getEffectiveMinResource(label).getResourceValue(resourceName)
          / updateContext.getUpdatedClusterResource().getResourceValue(resourceName);
    }

    queue.getQueueCapacities().setCapacity(label, sumCapacity);
    queue.getQueueCapacities().setAbsoluteCapacity(label, sumAbsoluteCapacity);
  }

  private void defineNormalizedResourceRatio(
      QueueHierarchyUpdateContext updateContext, CSQueue queue,
                                 QueueCapacityVectorEntry configuredCapacityResource,
      String label
  ) {
    AbstractCSQueue abstractQueue = (AbstractCSQueue) queue;

    Resource resourceByLabel = abstractQueue.labelManager.getResourceByLabel(
        label, updateContext.getUpdatedClusterResource());

    float childrenConfiguredResource = 0;
    long effectiveMinResource = queue.getQueueResourceQuotas().getEffectiveMinResource(label).getResourceValue(configuredCapacityResource.getResourceName());
    long labeledResource = resourceByLabel.getResourceValue(configuredCapacityResource.getResourceName());
    String units = "";

    // Total configured min resources of direct children of this given parent
    // queue

    for (CSQueue childQueue : queue.getChildQueues()) {
      units = childQueue.getQueueResourceQuotas().getConfiguredMinResource(
          label).getResourceInformation(
              configuredCapacityResource.getResourceName()).getUnits();
      childrenConfiguredResource += childQueue.getQueueResourceQuotas()
              .getConfiguredMinResource(label).getResourceValue(
                  configuredCapacityResource.getResourceName());
    }

    // Factor to scale down effective resource: When cluster has sufficient
    // resources, effective_min_resources will be same as configured
    // min_resources.
    float numeratorForMinRatio = labeledResource;

    if (!abstractQueue.getQueuePath().equals(ROOT)) {
      if (effectiveMinResource > childrenConfiguredResource) {
        numeratorForMinRatio = queue.getQueueResourceQuotas()
            .getEffectiveMinResource(label).getResourceValue(
                configuredCapacityResource.getResourceName());
      }
    }

    long convertedValue = UnitsConversionUtil.convert(units,
        resourceByLabel.getResourceInformation(configuredCapacityResource.getResourceName()).getUnits(),
        (long) childrenConfiguredResource);

    if (convertedValue != 0) {
      updateContext.getNormalizedMinResourceRatio(queue.getQueuePath(), label)
          .setValue(configuredCapacityResource.getResourceName(), numeratorForMinRatio / convertedValue);
    }
  }
}
