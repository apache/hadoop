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

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * AddReservationHomeSubClusterResponse contains the answer from the
 * {@code FederationReservationHomeSubClusterStore} to a request to insert a
 * newly generated ReservationId and its owner.
 *
 * The response contains reservation's home sub-cluster as it is stored in the
 * {@code FederationReservationHomeSubClusterStore}. If a mapping for the
 * reservation already existed, the {@code SubClusterId} in this response will
 * return the existing mapping which might be different from that in the
 * {@code AddReservationHomeSubClusterRequest}.
 */
@Private
@Unstable
public abstract class AddReservationHomeSubClusterResponse {

  @Private
  @Unstable
  public static AddReservationHomeSubClusterResponse newInstance(
      SubClusterId homeSubCluster) {
    AddReservationHomeSubClusterResponse response =
        Records.newRecord(AddReservationHomeSubClusterResponse.class);
    response.setHomeSubCluster(homeSubCluster);
    return response;
  }

  /**
   * Set the home sub-cluster that this Reservation has been assigned to.
   *
   * @param homeSubCluster the {@link SubClusterId} of this reservation's home
   *          sub-cluster
   */
  public abstract void setHomeSubCluster(SubClusterId homeSubCluster);

  /**
   * Get the home sub-cluster that this Reservation has been assigned to. This
   * may not match the {@link SubClusterId} in the corresponding response, if
   * the mapping for the request's reservation already existed.
   *
   * @return the {@link SubClusterId} of this reservation's home sub-cluster
   */
  public abstract SubClusterId getHomeSubCluster();
}
