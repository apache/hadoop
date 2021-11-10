/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

<<<<<<< HEAD
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;

=======
>>>>>>> 400f4af9c96 (YARN-11000. Fix weight queue calculation and test issues)
public class PercentageQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public float calculateMinimumResource(
      ResourceCalculationDriver resourceCalculationDriver, String label) {
    CSQueue parentQueue = resourceCalculationDriver.getParent();
    String resourceName = resourceCalculationDriver.getCurrentMinimumCapacityEntry(label).getResourceName();

    float parentAbsoluteCapacity = parentQueue.getOrCreateAbsoluteMinCapacityVector(label).getValue(
        resourceName);
<<<<<<< HEAD
    float remainingPerEffectiveResourceRatio = resourceCalculationDriver.getBatchRemainingResource(
        label).getValue(resourceName) / parentQueue.getEffectiveCapacity(label).getResourceValue(
            resourceName);
    float absoluteCapacity = parentAbsoluteCapacity * remainingPerEffectiveResourceRatio
        * resourceCalculationDriver.getCurrentMinimumCapacityEntry(label).getResourceValue() / 100;
=======
    float remainingPerEffectiveResourceRatio = updateContext.getQueueBranchContext(
            parentQueue.getQueuePath()).getBatchRemainingResources(label)
        .getValue(resourceName) / parentQueue.getEffectiveCapacity(label)
        .getResourceValue(resourceName);
    float absoluteCapacity = parentAbsoluteCapacity *
        remainingPerEffectiveResourceRatio
        * capacityVectorEntry.getResourceValue() / 100;
>>>>>>> 400f4af9c96 (YARN-11000. Fix weight queue calculation and test issues)

    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label)
        .getResourceValue(resourceName) * absoluteCapacity;
}

  @Override
  public float calculateMaximumResource(
      ResourceCalculationDriver resourceCalculationDriver, String label) {
    CSQueue parentQueue = resourceCalculationDriver.getParent();
    String resourceName = resourceCalculationDriver.getCurrentResourceName();

    float parentAbsoluteMaxCapacity = parentQueue.getOrCreateAbsoluteMaxCapacityVector(label)
        .getValue(resourceName);
    float absoluteMaxCapacity = parentAbsoluteMaxCapacity
        * resourceCalculationDriver.getCurrentMaximumCapacityEntry(label).getResourceValue() / 100;

    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label)
        .getResourceValue(resourceName) * absoluteMaxCapacity;
  }

  @Override
  public void updateCapacitiesAfterCalculation(ResourceCalculationDriver resourceCalculationDriver, String label) {
    ((AbstractCSQueue)resourceCalculationDriver.getCurrentChild()).updateAbsoluteCapacities();
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return ResourceUnitCapacityType.PERCENTAGE;
  }
}
