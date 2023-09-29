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

package org.apache.hadoop.yarn.server.federation.store.utils;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to validate the inputs to
 * {@code FederationReservationHomeSubClusterStore}, allows a fail fast
 * mechanism for invalid user inputs.
 *
 */
public final class FederationReservationHomeSubClusterStoreInputValidator {

  private static final Logger LOG = LoggerFactory
      .getLogger(FederationReservationHomeSubClusterStoreInputValidator.class);

  private FederationReservationHomeSubClusterStoreInputValidator() {
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link AddReservationHomeSubClusterRequest}
   * for adding a new reservation is valid or not.
   *
   * @param request the {@link AddReservationHomeSubClusterRequest} to validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(AddReservationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing AddReservationHomeSubCluster Request."
          + " Please try again by specifying"
          + " an AddReservationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate ReservationHomeSubCluster info
    checkReservationHomeSubCluster(request.getReservationHomeSubCluster());
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link GetReservationHomeSubClusterRequest}
   * for querying reservation's information is valid or not.
   *
   * @param request the {@link GetReservationHomeSubClusterRequest} to validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(GetReservationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing GetReservationHomeSubCluster Request."
          + " Please try again by specifying an Reservation Id information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate Reservation Id
    checkReservationId(request.getReservationId());
  }

  /**
   * Validate if the ReservationHomeSubCluster info are present or not.
   *
   * @param reservationHomeSubCluster the information of the Reservation to be verified
   * @throws FederationStateStoreInvalidInputException if the SubCluster Info are invalid
   */
  private static void checkReservationHomeSubCluster(
      ReservationHomeSubCluster reservationHomeSubCluster)
      throws FederationStateStoreInvalidInputException {
    if (reservationHomeSubCluster == null) {
      String message = "Missing ReservationHomeSubCluster Info."
          + " Please try again by specifying"
          + " an ReservationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate Reservation Id
    checkReservationId(reservationHomeSubCluster.getReservationId());

    // validate subcluster Id
    FederationMembershipStateStoreInputValidator
        .checkSubClusterId(reservationHomeSubCluster.getHomeSubCluster());
  }

  /**
   * Validate if the Reservation id is present or not.
   *
   * @param reservationId the id of the Reservation to be verified
   * @throws FederationStateStoreInvalidInputException if the Reservation Id is invalid
   */
  private static void checkReservationId(ReservationId reservationId)
      throws FederationStateStoreInvalidInputException {
    if (reservationId == null) {
      String message = "Missing ReservationId. Please try again by specifying an ReservationId.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link UpdateReservationHomeSubClusterRequest}
   * for updating an reservation is valid or not.
   *
   * @param request the {@link UpdateReservationHomeSubClusterRequest} to
   *          validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(UpdateReservationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing UpdateReservationHomeSubCluster Request." +
          " Please try again by specifying an ReservationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate ReservationHomeSubCluster info
    checkReservationHomeSubCluster(request.getReservationHomeSubCluster());
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link DeleteReservationHomeSubClusterRequest}
   * for deleting an Reservation is valid or not.
   *
   * @param request the {@link DeleteReservationHomeSubClusterRequest} to
   *          validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(DeleteReservationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing DeleteReservationHomeSubCluster Request." +
          " Please try again by specifying an ReservationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate Reservation Id
    checkReservationId(request.getReservationId());
  }
}
