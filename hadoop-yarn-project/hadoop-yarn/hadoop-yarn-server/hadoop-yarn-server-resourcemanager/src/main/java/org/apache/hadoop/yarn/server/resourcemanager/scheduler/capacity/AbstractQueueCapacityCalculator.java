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
import java.util.Set;

/**
 * A strategy class to encapsulate queue capacity setup and resource calculation
 * logic.
 */
public abstract class AbstractQueueCapacityCalculator {

  /**
   * Sets the metrics and statistics after effective resource values calculation.
   *
   * @param queue the queue on which the calculations are based
   * @param resourceCalculationDriver driver that contains the intermediate calculation results for
   *                                  a queue branch
   * @param label         node label
   */
  public abstract void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue, String label);


  /**
   * Returns the capacity type the calculator could handle.
   *
   * @return capacity type
   */
  public abstract ResourceUnitCapacityType getCapacityType();

  /**
   * Calculates the minimum effective resource.
   *
   * @param resourceCalculationDriver driver that contains the intermediate calculation results for
   *                                  a queue branch
   * @param context the units evaluated in the current iteration phase
   * @param label         node label
   * @return minimum effective resource
   */
  public abstract double calculateMinimumResource(ResourceCalculationDriver resourceCalculationDriver,
                                                 CalculationContext context,
                                                 String label);

  /**
   * Calculates the maximum effective resource.
   *
   * @param resourceCalculationDriver driver that contains the intermediate calculation results for
   *                                  a queue branch
   * @param context the units evaluated in the current iteration phase
   * @param label         node label
   * @return minimum effective resource
   */
  public abstract double calculateMaximumResource(ResourceCalculationDriver resourceCalculationDriver,
                                                 CalculationContext context,
                                                 String label);

  /**
   * Executes all logic that must be called prior to the effective resource value calculations.
   *
   * @param resourceCalculationDriver driver that contains the parent queue on which the 
   *                                  prerequisite calculation should be made
   */
  public abstract void calculateResourcePrerequisites(
      ResourceCalculationDriver resourceCalculationDriver);

  /**
   * Returns all resource names that are defined for the capacity type that is
   * handled by the calculator.
   *
   * @param queue queue for which the capacity vector is defined
   * @param label node label
   * @return resource names
   */
  protected Set<String> getResourceNames(CSQueue queue, String label) {
    return getResourceNames(queue, label, getCapacityType());
  }

  /**
   * Returns all resource names that are defined for a capacity type.
   *
   * @param queue        queue for which the capacity vector is defined
   * @param label        node label
   * @param capacityType capacity type for which the resource names are defined
   * @return resource names
   */
  protected Set<String> getResourceNames(CSQueue queue, String label,
                                         ResourceUnitCapacityType capacityType) {
    return queue.getConfiguredCapacityVector(label)
        .getResourceNamesByCapacityType(capacityType);
  }
}
