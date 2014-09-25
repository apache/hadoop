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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@link ReservationSubmissionRequest} captures the set of requirements the
 * user has to create a reservation.
 * 
 * @see ReservationDefinition
 * 
 */
@Public
@Unstable
public abstract class ReservationSubmissionRequest {

  @Public
  @Unstable
  public static ReservationSubmissionRequest newInstance(
      ReservationDefinition reservationDefinition, String queueName) {
    ReservationSubmissionRequest request =
        Records.newRecord(ReservationSubmissionRequest.class);
    request.setReservationDefinition(reservationDefinition);
    request.setQueue(queueName);
    return request;
  }

  /**
   * Get the {@link ReservationDefinition} representing the user constraints for
   * this reservation
   * 
   * @return the reservation definition representing user constraints
   */
  @Public
  @Unstable
  public abstract ReservationDefinition getReservationDefinition();

  /**
   * Set the {@link ReservationDefinition} representing the user constraints for
   * this reservation
   * 
   * @param reservationDefinition the reservation request representing the
   *          reservation
   */
  @Public
  @Unstable
  public abstract void setReservationDefinition(
      ReservationDefinition reservationDefinition);

  /**
   * Get the name of the {@code Plan} that corresponds to the name of the
   * {@link QueueInfo} in the scheduler to which the reservation will be
   * submitted to.
   * 
   * @return the name of the {@code Plan} that corresponds to the name of the
   *         {@link QueueInfo} in the scheduler to which the reservation will be
   *         submitted to
   */
  @Public
  @Unstable
  public abstract String getQueue();

  /**
   * Set the name of the {@code Plan} that corresponds to the name of the
   * {@link QueueInfo} in the scheduler to which the reservation will be
   * submitted to
   * 
   * @param queueName the name of the parent {@code Plan} that corresponds to
   *          the name of the {@link QueueInfo} in the scheduler to which the
   *          reservation will be submitted to
   */
  @Public
  @Unstable
  public abstract void setQueue(String queueName);

}
