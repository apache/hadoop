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
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;

/**
 * FederationApplicationHomeSubClusterStore maintains the state of all
 * <em>Applications</em> that have been submitted to the federated cluster.
 *
 * *
 * <p>
 * The mapping details contains:
 * <ul>
 * <li>{@code ApplicationId}</li>
 * <li>{@code SubClusterId}</li>
 * </ul>
 *
 */
@Private
@Unstable
public interface FederationApplicationHomeSubClusterStore {

  /**
   * Register the home {@code SubClusterId} of the newly submitted
   * {@code ApplicationId}. Currently response is empty if the operation was
   * successful, if not an exception reporting reason for a failure. If a
   * mapping for the application already existed, the {@code SubClusterId} in
   * this response will return the existing mapping which might be different
   * from that in the {@code AddApplicationHomeSubClusterRequest}.
   *
   * @param request the request to register a new application with its home
   *          sub-cluster
   * @return upon successful registration of the application in the StateStore,
   *         {@code AddApplicationHomeSubClusterRequest} containing the home
   *         sub-cluster of the application. Otherwise, an exception reporting
   *         reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException;

  /**
   * Update the home {@code SubClusterId} of a previously submitted
   * {@code ApplicationId}. Currently response is empty if the operation was
   * successful, if not an exception reporting reason for a failure.
   *
   * @param request the request to update the home sub-cluster of an
   *          application.
   * @return empty on successful update of the application in the StateStore, if
   *         not an exception reporting reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubCluster(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException;

  /**
   * Get information about the application identified by the input
   * {@code ApplicationId}.
   *
   * @param request contains the application queried
   * @return {@code ApplicationHomeSubCluster} containing the application's home
   *         subcluster
   * @throws YarnException if the request is invalid/fails
   */
  GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException;

  /**
   * Get the {@code ApplicationHomeSubCluster} list representing the mapping of
   * all submitted applications to it's home sub-cluster.
   *
   * @param request empty representing all applications
   * @return the mapping of all submitted application to it's home sub-cluster
   * @throws YarnException if the request is invalid/fails
   */
  GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException;

  /**
   * Delete the mapping of home {@code SubClusterId} of a previously submitted
   * {@code ApplicationId}. Currently response is empty if the operation was
   * successful, if not an exception reporting reason for a failure.
   *
   * @param request the request to delete the home sub-cluster of an
   *          application.
   * @return empty on successful update of the application in the StateStore, if
   *         not an exception reporting reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  DeleteApplicationHomeSubClusterResponse deleteApplicationHomeSubCluster(
      DeleteApplicationHomeSubClusterRequest request) throws YarnException;

}
