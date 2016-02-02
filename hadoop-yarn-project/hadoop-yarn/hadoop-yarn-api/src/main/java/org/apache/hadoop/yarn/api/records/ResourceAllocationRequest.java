/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code ResourceAllocationRequest} represents an allocation
 * made for a reservation for the current state of the plan. This can be
 * changed for reasons such as re-planning, but will always be subject to the
 * constraints of the user contract as described by
 * {@link ReservationDefinition}
 * {@link Resource}
 *
 * <p>
 * It includes:
 * <ul>
 *   <li>StartTime of the allocation.</li>
 *   <li>EndTime of the allocation.</li>
 *   <li>{@link Resource} reserved for the allocation.</li>
 * </ul>
 *
 * @see Resource
 */
@Public
@Stable
public abstract class ResourceAllocationRequest {

  /**
   * @param startTime The start time that the capability is reserved for.
   * @param endTime The end time that the capability is reserved for.
   * @param capability {@link Resource} representing the capability of the
   *                                   resource allocation.
   * @return {ResourceAllocationRequest} which represents the capability of
   * the resource allocation for a time interval.
   */
  @Public
  @Stable
  public static ResourceAllocationRequest newInstance(long startTime,
                                     long endTime, Resource capability) {
    ResourceAllocationRequest ra = Records.newRecord(
            ResourceAllocationRequest.class);
    ra.setEndTime(endTime);
    ra.setStartTime(startTime);
    ra.setCapability(capability);
    return ra;
  }

  /**
   * Get the start time that the resource is allocated.
   *
   * @return the start time that the resource is allocated.
   */
  @Public
  @Unstable
  public abstract long getStartTime();

  /**
   * Set the start time that the resource is allocated.
   *
   * @param startTime The start time that the capability is reserved for.
   */
  @Private
  @Unstable
  public abstract void setStartTime(long startTime);

  /**
   * Get the end time that the resource is allocated.
   *
   * @return the end time that the resource is allocated.
   */
  @Public
  @Unstable
  public abstract long getEndTime();

  /**
   * Set the end time that the resource is allocated.
   *
   * @param endTime The end time that the capability is reserved for.
   */
  @Private
  @Unstable
  public abstract void setEndTime(long endTime);

  /**
   * Get the allocated resource.
   *
   * @return the allocated resource.
   */
  @Public
  @Unstable
  public abstract Resource getCapability();

  /**
   * Set the allocated resource.
   *
   * @param resource {@link Resource} representing the capability of the
   *                                   resource allocation.
   */
  @Private
  @Unstable
  public abstract void setCapability(Resource resource);
}
