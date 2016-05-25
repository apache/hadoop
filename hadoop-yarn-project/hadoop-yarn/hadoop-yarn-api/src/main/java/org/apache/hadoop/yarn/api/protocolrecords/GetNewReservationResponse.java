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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The response sent by the <code>ResourceManager</code> to the client for
 * a request to get a new {@link ReservationId} for submitting reservations.</p>
 *
 * <p>Clients can submit an reservation with the returned
 * {@link ReservationId}.</p>
 *
 * {@code ApplicationClientProtocol#getNewReservation(GetNewReservationRequest)}
 */
@Public
@Unstable
public abstract class GetNewReservationResponse {

  @Private
  @Unstable
  public static GetNewReservationResponse newInstance(
          ReservationId reservationId) {
    GetNewReservationResponse response =
        Records.newRecord(GetNewReservationResponse.class);
    response.setReservationId(reservationId);
    return response;
  }

  /**
   * Get a new {@link ReservationId} to be used to submit a reservation.
   *
   * @return a {@link ReservationId} representing the unique id to identify
   * a reservation with which it was submitted.
   */
  @Public
  @Unstable
  public abstract ReservationId getReservationId();

  /**
   * Set a new {@link ReservationId} to be used to submit a reservation.
   *
   * @param reservationId a {@link ReservationId} representing the unique id to
   *          identify a reservation with which it was submitted.
   */
  @Private
  @Unstable
  public abstract void setReservationId(ReservationId reservationId);

}
