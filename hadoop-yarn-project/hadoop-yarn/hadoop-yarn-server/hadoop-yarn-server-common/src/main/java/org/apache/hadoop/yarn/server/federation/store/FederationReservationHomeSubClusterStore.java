/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;

/**
 * FederationReservationHomeSubClusterStore maintains the state of all
 * <em>Reservations</em> that have been submitted to the federated cluster.
 *
 * *
 * <p>
 * The mapping details contains:
 * <ul>
 * <li>{@code ReservationId}</li>
 * <li>{@code SubClusterId}</li>
 * </ul>
 *
 */
@Private
@Unstable
public interface FederationReservationHomeSubClusterStore {

  /**
   * Register the home {@code SubClusterId} of the newly submitted
   * {@code ReservationId}. Currently response is empty if the operation was
   * successful, if not an exception reporting reason for a failure. If a
   * mapping for the Reservation already existed, the {@code SubClusterId} in
   * this response will return the existing mapping which might be different
   * from that in the {@code AddReservationHomeSubClusterRequest}.
   *
   * @param request the request to register a new Reservation with its home
   *          sub-cluster
   * @return upon successful registration of the Reservation in the StateStore,
   *         {@code AddReservationHomeSubClusterRequest} containing the home
   *         sub-cluster of the Reservation. Otherwise, an exception reporting
   *         reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  AddReservationHomeSubClusterResponse addReservationHomeSubCluster(
      AddReservationHomeSubClusterRequest request) throws YarnException;

  /**
   * Get information about the Reservation identified by the input
   * {@code ReservationId}.
   *
   * @param request contains the Reservation queried
   * @return {@code ReservationHomeSubCluster} containing the Reservation's home
   *         subcluster
   * @throws YarnException if the request is invalid/fails
   */
  GetReservationHomeSubClusterResponse getReservationHomeSubCluster(
      GetReservationHomeSubClusterRequest request) throws YarnException;

  /**
   * Get the {@code ReservationHomeSubCluster} list representing the mapping of
   * all submitted Reservations to it's home sub-cluster.
   *
   * @param request empty representing all Reservations
   * @return the mapping of all submitted Reservation to it's home sub-cluster
   * @throws YarnException if the request is invalid/fails
   */
  GetReservationsHomeSubClusterResponse getReservationsHomeSubCluster(
      GetReservationsHomeSubClusterRequest request) throws YarnException;

  /**
   * Update the home {@code SubClusterId} of a previously submitted
   * {@code ReservationId}. Currently response is empty if the operation was
   * successful, if not an exception reporting reason for a failure.
   *
   * @param request the request to update the home sub-cluster of a reservation.
   * @return empty on successful update of the Reservation in the StateStore, if
   *         not an exception reporting reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  UpdateReservationHomeSubClusterResponse updateReservationHomeSubCluster(
      UpdateReservationHomeSubClusterRequest request) throws YarnException;


  /**
   * Delete the mapping of home {@code SubClusterId} of a previously submitted
   * {@code ReservationId}. Currently response is empty if the operation was
   * successful, if not an exception reporting reason for a failure.
   *
   * @param request the request to delete the home sub-cluster of a reservation.
   * @return empty on successful update of the Reservation in the StateStore, if
   *         not an exception reporting reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  DeleteReservationHomeSubClusterResponse deleteReservationHomeSubCluster(
      DeleteReservationHomeSubClusterRequest request) throws YarnException;
}
