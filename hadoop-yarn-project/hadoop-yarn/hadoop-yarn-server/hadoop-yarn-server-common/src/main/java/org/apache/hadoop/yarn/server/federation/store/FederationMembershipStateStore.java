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
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;

/**
 * FederationMembershipStateStore maintains the state of all
 * <em>subcluster(s)</em> as encapsulated by {@code SubClusterInfo} for all the
 * subcluster(s) that are participating in federation.
 */
@Private
@Unstable
public interface FederationMembershipStateStore {

  /**
   * Register a <em>subcluster</em> by publishing capabilities as represented by
   * {@code SubClusterInfo} to indicate participation in federation. This is
   * typically done during initialization or restart/failover of the
   * subcluster's <code>ResourceManager</code>. Upon successful registration, an
   * identifier for the <em>subcluster</em> which is unique across the federated
   * cluster is returned. The identifier is static, i.e. preserved across
   * restarts and failover.
   *
   * @param registerSubClusterRequest the capabilities of the subcluster that
   *          wants to participate in federation. The subcluster id is also
   *          specified in case registration is triggered by restart/failover
   * @return response empty on successfully if registration was successful
   * @throws YarnException if the request is invalid/fails
   */
  SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest registerSubClusterRequest) throws YarnException;

  /**
   * Deregister a <em>subcluster</em> identified by {@code SubClusterId} to
   * change state in federation. This can be done to mark the sub cluster lost,
   * deregistered, or decommissioned.
   *
   * @param subClusterDeregisterRequest - the request to deregister the
   *          sub-cluster from federation.
   * @return response empty on successfully deregistering the subcluster state
   * @throws YarnException if the request is invalid/fails
   */
  SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest subClusterDeregisterRequest)
      throws YarnException;

  /**
   * Periodic heartbeat from a <code>ResourceManager</code> participating in
   * federation to indicate liveliness. The heartbeat publishes the current
   * capabilities as represented by {@code SubClusterInfo} of the subcluster.
   * Currently response is empty if the operation was successful, if not an
   * exception reporting reason for a failure.
   *
   * @param subClusterHeartbeatRequest the capabilities of the subcluster that
   *          wants to keep alive its participation in federation
   * @return response currently empty on if heartbeat was successfully processed
   * @throws YarnException if the request is invalid/fails
   */
  SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest subClusterHeartbeatRequest)
      throws YarnException;

  /**
   * Get the membership information of <em>subcluster</em> as identified by
   * {@code SubClusterId}. The membership information includes the cluster
   * endpoint and current capabilities as represented by {@code SubClusterInfo}.
   *
   * @param subClusterRequest the subcluster whose information is required
   * @return the {@code SubClusterInfo}, or {@code null} if there is no mapping
   *         for the subcluster
   * @throws YarnException if the request is invalid/fails
   */
  GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest subClusterRequest) throws YarnException;

  /**
   * Get the membership information of all the <em>subclusters</em> that are
   * currently participating in federation. The membership information includes
   * the cluster endpoint and current capabilities as represented by
   * {@code SubClusterInfo}.
   *
   * @param subClustersRequest request for sub-clusters information
   * @return a map of {@code SubClusterInfo} keyed by the {@code SubClusterId}
   * @throws YarnException if the request is invalid/fails
   */
  GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest subClustersRequest) throws YarnException;

}
