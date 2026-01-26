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
 * Represents an approach on how to convert a calculated resource from floating point to a whole
 * number.
 */
public interface QueueResourceRoundingStrategy {

  /**
   * Returns a whole number converted from the calculated resource value.
   * @param resourceValue calculated resource value
   * @param capacityVectorEntry configured capacity entry
   * @return rounded resource value
   */
  double getRoundedResource(double resourceValue, QueueCapacityVectorEntry capacityVectorEntry);
}
