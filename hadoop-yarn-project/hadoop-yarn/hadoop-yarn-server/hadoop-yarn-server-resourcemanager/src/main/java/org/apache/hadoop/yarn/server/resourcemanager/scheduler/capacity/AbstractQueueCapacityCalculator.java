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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;

import java.util.Set;

/**
 * A strategy class to encapsulate queue capacity setup and resource calculation
 * logic.
 */
public abstract class AbstractQueueCapacityCalculator {

  /**
   * Sets all field of the queue based on its configurations.
   * @param queue queue to setup
   * @param conf configuration the setup is based on
   * @param label node label
   */
  public abstract void setup(
      CSQueue queue, CapacitySchedulerConfiguration conf, String label);

  /**
   * Calculate the effective resource for a specific resource.
   * @param updateContext context of the current update phase
   * @param parentQueue the parent whose children will be updated
   * @param label node label
   */
  public abstract void calculateChildQueueResources(
      QueueHierarchyUpdateContext updateContext,
      CSQueue parentQueue, String label);

  /**
   * Set the necessary metrics and statistics.
   * @param updateContext context of the current update phase
   * @param queue queue to update
   * @param label node label
   */
  public abstract void setMetrics(
      QueueHierarchyUpdateContext updateContext, CSQueue queue, String label);

  /**
   * Returns the capacity type the calculator could handle.
   * @return capacity type
   */
  protected abstract QueueCapacityType getCapacityType();

  /**
   * Returns all resource names that are defined for the capacity type that is
   * handled by the calculator.
   * @param queue queue for which the capacity vector is defined
   * @param label node label
   * @return resource names
   */
  protected Set<String> getResourceNames(CSQueue queue, String label) {
    return queue.getConfiguredCapacityVector(label)
        .getResourceNamesByCapacityType(getCapacityType());
  }
}
