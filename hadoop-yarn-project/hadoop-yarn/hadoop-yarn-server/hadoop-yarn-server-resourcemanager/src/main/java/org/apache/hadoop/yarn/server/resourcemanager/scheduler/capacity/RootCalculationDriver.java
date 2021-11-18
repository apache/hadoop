package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * A special case that contains the resource calculation of the root queue.
 */
public final class RootCalculationDriver extends ResourceCalculationDriver {
  private final AbstractQueueCapacityCalculator rootCalculator;

  public RootCalculationDriver(CSQueue rootQueue, QueueCapacityUpdateContext updateContext,
                               AbstractQueueCapacityCalculator rootCalculator,
                               Collection<String> definedResources) {
    super(rootQueue, updateContext, Collections.emptyMap(), definedResources);
    this.rootCalculator = rootCalculator;
  }

  @Override
  public void calculateResources() {
    for (String label : parent.getConfiguredNodeLabels()) {
      for (QueueCapacityVector.QueueCapacityVectorEntry capacityVectorEntry : parent.getConfiguredCapacityVector(label)) {
        currentResourceName = capacityVectorEntry.getResourceName();
        parent.getOrCreateAbsoluteMinCapacityVector(label).setValue(currentResourceName, 1);
        parent.getOrCreateAbsoluteMaxCapacityVector(label).setValue(currentResourceName, 1);

        float minimumResource = rootCalculator.calculateMinimumResource(this, label);
        float maximumResource = rootCalculator.calculateMaximumResource(this, label);
        long roundedMinResource = (long) Math.floor(minimumResource);
        long roundedMaxResource = (long) Math.floor(maximumResource);
        parent.getQueueResourceQuotas().getEffectiveMinResource(label).setResourceValue(
            currentResourceName, roundedMinResource);
        parent.getQueueResourceQuotas().getEffectiveMaxResource(label).setResourceValue(
            currentResourceName, roundedMaxResource);
      }
      rootCalculator.updateCapacitiesAfterCalculation(this, label);
    }

    rootCalculator.calculateResourcePrerequisites(this);
  }
}
