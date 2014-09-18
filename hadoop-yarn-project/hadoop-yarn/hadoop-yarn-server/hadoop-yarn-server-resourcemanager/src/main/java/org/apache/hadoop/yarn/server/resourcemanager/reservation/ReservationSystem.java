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

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

/**
 * This interface is the one implemented by any system that wants to support
 * Reservations i.e. make {@link Resource} allocations in future. Implementors
 * need to bootstrap all configured {@link Plan}s in the active
 * {@link ResourceScheduler} along with their corresponding
 * {@link ReservationAgent} and {@link SharingPolicy}. It is also responsible
 * for managing the {@link PlanFollower} to ensure the {@link Plan}s are in sync
 * with the {@link ResourceScheduler}.
 */
@LimitedPrivate("yarn")
@Unstable
public interface ReservationSystem {

  /**
   * Set RMContext for {@link ReservationSystem}. This method should be called
   * immediately after instantiating a reservation system once.
   * 
   * @param rmContext created by {@link ResourceManager}
   */
  void setRMContext(RMContext rmContext);

  /**
   * Re-initialize the {@link ReservationSystem}.
   * 
   * @param conf configuration
   * @param rmContext current context of the {@link ResourceManager}
   * @throws YarnException
   */
  void reinitialize(Configuration conf, RMContext rmContext)
      throws YarnException;

  /**
   * Get an existing {@link Plan} that has been initialized.
   * 
   * @param planName the name of the {@link Plan}
   * @return the {@link Plan} identified by name
   * 
   */
  Plan getPlan(String planName);

  /**
   * Return a map containing all the plans known to this ReservationSystem
   * (useful for UI)
   * 
   * @return a Map of Plan names and Plan objects
   */
  Map<String, Plan> getAllPlans();

  /**
   * Invokes {@link PlanFollower} to synchronize the specified {@link Plan} with
   * the {@link ResourceScheduler}
   * 
   * @param planName the name of the {@link Plan} to be synchronized
   */
  void synchronizePlan(String planName);

  /**
   * Return the time step (ms) at which the {@link PlanFollower} is invoked
   * 
   * @return the time step (ms) at which the {@link PlanFollower} is invoked
   */
  long getPlanFollowerTimeStep();

  /**
   * Get a new unique {@link ReservationId}.
   * 
   * @return a new unique {@link ReservationId}
   * 
   */
  ReservationId getNewReservationId();

  /**
   * Get the {@link Queue} that an existing {@link ReservationId} is associated
   * with.
   * 
   * @param reservationId the unique id of the reservation
   * @return the name of the associated Queue
   * 
   */
  String getQueueForReservation(ReservationId reservationId);

  /**
   * Set the {@link Queue} that an existing {@link ReservationId} should be
   * associated with.
   * 
   * @param reservationId the unique id of the reservation
   * @param queueName the name of Queue to associate the reservation with
   * 
   */
  void setQueueForReservation(ReservationId reservationId, String queueName);

}
