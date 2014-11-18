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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * This is the interface for policy that validate new
 * {@link ReservationAllocation}s for allocations being added to a {@link Plan}.
 * Individual policies will be enforcing different invariants.
 */
@LimitedPrivate("yarn")
@Unstable
public interface SharingPolicy {

  /**
   * Initialize this policy
   * 
   * @param planQueuePath the name of the queue for this plan
   * @param conf the system configuration
   */
  public void init(String planQueuePath, ReservationSchedulerConfiguration conf);

  /**
   * This method runs the policy validation logic, and return true/false on
   * whether the {@link ReservationAllocation} is acceptable according to this
   * sharing policy.
   * 
   * @param plan the {@link Plan} we validate against
   * @param newAllocation the allocation proposed to be added to the
   *          {@link Plan}
   * @throws PlanningException if the policy is respected if we add this
   *           {@link ReservationAllocation} to the {@link Plan}
   */
  public void validate(Plan plan, ReservationAllocation newAllocation)
      throws PlanningException;

  /**
   * Returns the time range before and after the current reservation considered
   * by this policy. In particular, this informs the archival process for the
   * {@link Plan}, i.e., reservations regarding times before (now - validWindow)
   * can be deleted.
   * 
   * @return validWindow the window of validity considered by the policy.
   */
  public long getValidWindow();

}
