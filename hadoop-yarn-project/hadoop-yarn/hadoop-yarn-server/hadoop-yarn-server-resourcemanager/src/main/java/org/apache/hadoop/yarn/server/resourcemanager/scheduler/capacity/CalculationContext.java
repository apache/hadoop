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
 * A storage class that wraps arguments used in a resource calculation iteration.
 */
public class CalculationContext {
  private final String resourceName;
  private final ResourceUnitCapacityType capacityType;
  private final CSQueue queue;

  public CalculationContext(String resourceName, ResourceUnitCapacityType capacityType,
                            CSQueue queue) {
    this.resourceName = resourceName;
    this.capacityType = capacityType;
    this.queue = queue;
  }

  public String getResourceName() {
    return resourceName;
  }

  public ResourceUnitCapacityType getCapacityType() {
    return capacityType;
  }

  public CSQueue getQueue() {
    return queue;
  }

  /**
   * A shorthand to return the minimum capacity vector entry for the currently evaluated child and
   * resource name.
   *
   * @param label node label
   * @return capacity vector entry
   */
  public QueueCapacityVectorEntry getCurrentMinimumCapacityEntry(String label) {
    return queue.getConfiguredCapacityVector(label).getResource(resourceName);
  }

  /**
   * A shorthand to return the maximum capacity vector entry for the currently evaluated child and
   * resource name.
   *
   * @param label node label
   * @return capacity vector entry
   */
  public QueueCapacityVectorEntry getCurrentMaximumCapacityEntry(String label) {
    return queue.getConfiguredMaxCapacityVector(label).getResource(resourceName);
  }
}
