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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE;

public class RootQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public void calculateResourcePrerequisites(ResourceCalculationDriver resourceCalculationDriver) {
    AbsoluteResourceCapacityCalculator.setNormalizedResourceRatio(resourceCalculationDriver);
  }

  @Override
  public double calculateMinimumResource(ResourceCalculationDriver resourceCalculationDriver,
                                         CalculationContext context, String label) {
    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label)
        .getResourceValue(context.getResourceName());
  }

  @Override
  public double calculateMaximumResource(ResourceCalculationDriver resourceCalculationDriver,
                                         CalculationContext context, String label) {
    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label)
        .getResourceValue(context.getResourceName());
  }

  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue, String label) {
    queue.getQueueCapacities().setAbsoluteCapacity(label, 1);
    if (queue.getQueueCapacities().getWeight(label) == 1) {
      queue.getQueueCapacities().setNormalizedWeight(label, 1);
    }
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return PERCENTAGE;
  }
}
