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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE;

public class RootQueueCapacityCalculator extends
    AbstractQueueCapacityCalculator {

  @Override
  public float calculateMinimumResource(ResourceCalculationDriver resourceCalculationDriver, String label) {
    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label).getResourceValue(resourceCalculationDriver.getCurrentResourceName());
  }

  @Override
  public float calculateMaximumResource(ResourceCalculationDriver resourceCalculationDriver, String label) {
    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label).getResourceValue(resourceCalculationDriver.getCurrentResourceName());
  }

  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, String label) {
    resourceCalculationDriver.getCurrentChild().getQueueCapacities().setAbsoluteCapacity(label, 1);
    if (resourceCalculationDriver.getCurrentChild().getQueueCapacities().getWeight(label) == 1) {
      resourceCalculationDriver.getCurrentChild().getQueueCapacities().setNormalizedWeight(label, 1);
    }
  }

  @Override
  public QueueCapacityVector.ResourceUnitCapacityType getCapacityType() {
    return PERCENTAGE;
  }
}
