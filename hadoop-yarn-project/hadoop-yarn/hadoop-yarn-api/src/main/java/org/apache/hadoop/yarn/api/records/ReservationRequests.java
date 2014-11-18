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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@link ReservationRequests} captures the set of resource and constraints the
 * user cares about regarding a reservation.
 * 
 * @see ReservationRequest
 * 
 */
@Public
@Unstable
public abstract class ReservationRequests {

  @Public
  @Unstable
  public static ReservationRequests newInstance(
      List<ReservationRequest> reservationResources,
      ReservationRequestInterpreter type) {
    ReservationRequests reservationRequests =
        Records.newRecord(ReservationRequests.class);
    reservationRequests.setReservationResources(reservationResources);
    reservationRequests.setInterpreter(type);
    return reservationRequests;
  }

  /**
   * Get the list of {@link ReservationRequest} representing the resources
   * required by the application
   * 
   * @return the list of {@link ReservationRequest}
   */
  @Public
  @Unstable
  public abstract List<ReservationRequest> getReservationResources();

  /**
   * Set the list of {@link ReservationRequest} representing the resources
   * required by the application
   * 
   * @param reservationResources the list of {@link ReservationRequest}
   */
  @Public
  @Unstable
  public abstract void setReservationResources(
      List<ReservationRequest> reservationResources);

  /**
   * Get the {@link ReservationRequestInterpreter}, representing how the list of
   * resources should be allocated, this captures temporal ordering and other
   * constraints.
   * 
   * @return the list of {@link ReservationRequestInterpreter}
   */
  @Public
  @Unstable
  public abstract ReservationRequestInterpreter getInterpreter();

  /**
   * Set the {@link ReservationRequestInterpreter}, representing how the list of
   * resources should be allocated, this captures temporal ordering and other
   * constraints.
   * 
   * @param interpreter the {@link ReservationRequestInterpreter} for this
   *          reservation
   */
  @Public
  @Unstable
  public abstract void setInterpreter(ReservationRequestInterpreter interpreter);

}
