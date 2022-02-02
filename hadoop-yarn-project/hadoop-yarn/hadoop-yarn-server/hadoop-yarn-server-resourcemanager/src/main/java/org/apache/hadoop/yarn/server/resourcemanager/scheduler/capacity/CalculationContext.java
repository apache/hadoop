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

/**
 * A storage class that wraps arguments used in a resource calculation iteration.
 */
public class CalculationContext {
  private final String resourceName;
  private final QueueCapacityVector.ResourceUnitCapacityType capacityType;
  private final CSQueue childQueue;

  public CalculationContext(String resourceName, QueueCapacityVector.ResourceUnitCapacityType capacityType, CSQueue childQueue) {
    this.resourceName = resourceName;
    this.capacityType = capacityType;
    this.childQueue = childQueue;
  }

  public String getResourceName() {
    return resourceName;
  }

  public QueueCapacityVector.ResourceUnitCapacityType getCapacityType() {
    return capacityType;
  }

  public CSQueue getChildQueue() {
    return childQueue;
  }

  /**
   * A shorthand to return the minimum capacity vector entry for the currently evaluated child and
   * resource name.
   *
   * @param label node label
   * @return capacity vector entry
   */
  public QueueCapacityVectorEntry getCurrentMinimumCapacityEntry(String label) {
    return childQueue.getConfiguredCapacityVector(label).getResource(resourceName);
  }

  /**
   * A shorthand to return the maximum capacity vector entry for the currently evaluated child and
   * resource name.
   *
   * @param label node label
   * @return capacity vector entry
   */
  public QueueCapacityVectorEntry getCurrentMaximumCapacityEntry(String label) {
    return childQueue.getConfiguredMaxCapacityVector(label).getResource(resourceName);
  }
}
