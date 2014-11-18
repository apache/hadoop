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

import java.util.List;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

public interface Planner {

  /**
   * Update the existing {@link Plan}, by adding/removing/updating existing
   * reservations, and adding a subset of the reservation requests in the
   * contracts parameter.
   *
   * @param plan the {@link Plan} to replan
   * @param contracts the list of reservation requests
   * @throws PlanningException
   */
  public void plan(Plan plan, List<ReservationDefinition> contracts)
      throws PlanningException;

  /**
   * Initialize the replanner
   *
   * @param planQueueName the name of the queue for this plan
   * @param conf the scheduler configuration
   */
  void init(String planQueueName, ReservationSchedulerConfiguration conf);
}
