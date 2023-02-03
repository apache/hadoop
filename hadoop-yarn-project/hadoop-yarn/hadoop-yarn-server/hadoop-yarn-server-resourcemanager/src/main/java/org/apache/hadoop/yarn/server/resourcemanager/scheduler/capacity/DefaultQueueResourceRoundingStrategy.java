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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;

/**
 * The default rounding strategy for resource calculation. Uses floor for all types except WEIGHT,
 * which is always the last type to consider, therefore it is safe to round up.
 */
public class DefaultQueueResourceRoundingStrategy implements QueueResourceRoundingStrategy {
  private final ResourceUnitCapacityType lastCapacityType;

  public DefaultQueueResourceRoundingStrategy(
      ResourceUnitCapacityType[] capacityTypePrecedence) {
    if (capacityTypePrecedence.length == 0) {
      throw new IllegalArgumentException("Capacity type precedence collection is empty");
    }

    lastCapacityType = capacityTypePrecedence[capacityTypePrecedence.length - 1];
  }

  @Override
  public double getRoundedResource(double resourceValue, QueueCapacityVectorEntry capacityVectorEntry) {
    if (capacityVectorEntry.getVectorResourceType().equals(lastCapacityType)) {
      return Math.round(resourceValue);
    } else {
      return Math.floor(resourceValue);
    }
  }
}
