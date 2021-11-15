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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;

public class AbsoluteResourceCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public float calculateMinimumResource(
      QueueCapacityUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry) {
    String resourceName = capacityVectorEntry.getResourceName();
    ResourceVector ratio = updateContext.getQueueBranchContext(childQueue.getParent().getQueuePath())
        .getNormalizedResourceRatios().getOrDefault(label, ResourceVector.of(1));

    return ratio.getValue(resourceName) * capacityVectorEntry.getResourceValue();
  }

  @Override
  public float calculateMaximumResource(
      QueueCapacityUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry) {
    return capacityVectorEntry.getResourceValue();
  }

  @Override
  public void updateCapacitiesAfterCalculation(
      QueueCapacityUpdateContext updateContext, CSQueue queue, String label) {
    setQueueCapacities(updateContext.getUpdatedClusterResource(label), queue, label);
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return ResourceUnitCapacityType.ABSOLUTE;
  }

}
