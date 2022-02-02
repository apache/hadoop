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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;

public class AbsoluteResourceCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public double calculateMinimumResource(
      ResourceCalculationDriver resourceCalculationDriver, CalculationContext context, String label) {
    String resourceName = context.getResourceName();
    double normalizedRatio = resourceCalculationDriver.getNormalizedResourceRatios().getOrDefault(
        label, ResourceVector.of(1)).getValue(resourceName);
    double remainingResourceRatio = resourceCalculationDriver.getRemainingRatioOfResource(
        label, resourceName);

    return normalizedRatio * remainingResourceRatio * context.getCurrentMinimumCapacityEntry(
        label).getResourceValue();
  }

  @Override
  public double calculateMaximumResource(
       ResourceCalculationDriver resourceCalculationDriver, CalculationContext context, String label) {
    return context.getCurrentMaximumCapacityEntry(label).getResourceValue();
  }

  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue, String label) {
    CapacitySchedulerQueueCapacityHandler.setQueueCapacities(
        resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label), queue, label);
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return ResourceUnitCapacityType.ABSOLUTE;
  }

}
