package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType.WEIGHT;

public class WeightQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public void calculateChildQueueResources(QueueHierarchyUpdateContext updateContext, CSQueue parentQueue) {
    Map<String, Map<String, Float>> sumWeightsPerLabel = summarizeWeights(parentQueue);

    iterateThroughChildrenResources(parentQueue, updateContext,
        ((childQueue, label, capacityVectorEntry) -> {
          String resourceName = capacityVectorEntry.getResourceName();
          float normalizedWeight = capacityVectorEntry.getResourceValue()
              / sumWeightsPerLabel.get(label).get(resourceName);

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

          childQueue.getQueueResourceQuotas().getEffectiveMinResource(label)
              .setResourceValue(capacityVectorEntry.getResourceName(),
                  resource);

          updateContext.getRelativeResourceRatio(childQueue.getQueuePath(), label)
              .setValue(capacityVectorEntry.getResourceName(),
                  queueAbsoluteCapacity);

          return resource;
        }));
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

  private Map<String, Map<String, Float>> summarizeWeights(CSQueue parentQueue) {
    Map<String, Map<String, Float>> sumWeightsPerResource = new HashMap<>();

    for (String label : parentQueue.getConfiguredNodeLabels()) {
      Map<String, Float> sumWeight = new HashMap<>();

      for (CSQueue childQueue : parentQueue.getChildQueues()) {
        for (String resourceName : childQueue.getConfiguredCapacityVector(label).getResourceNamesByCapacityType(getCapacityType())) {
          sumWeight.put(resourceName, sumWeight.getOrDefault(resourceName, 0f)
              + childQueue.getConfiguredCapacityVector(label).getResource(resourceName).getResourceValue());
        }
      }
      sumWeightsPerResource.put(label, sumWeight);
    }

    return sumWeightsPerResource;
  }

}
