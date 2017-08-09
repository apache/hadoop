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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to validate the inputs to
 * {@code FederationApplicationHomeSubClusterStore}, allows a fail fast
 * mechanism for invalid user inputs.
 *
 */
public final class FederationApplicationHomeSubClusterStoreInputValidator {

  private static final Logger LOG = LoggerFactory
      .getLogger(FederationApplicationHomeSubClusterStoreInputValidator.class);

  private FederationApplicationHomeSubClusterStoreInputValidator() {
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link AddApplicationHomeSubClusterRequest}
   * for adding a new application is valid or not.
   *
   * @param request the {@link AddApplicationHomeSubClusterRequest} to validate
   *          against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(AddApplicationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing AddApplicationHomeSubCluster Request."
          + " Please try again by specifying"
          + " an AddApplicationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate ApplicationHomeSubCluster info
    checkApplicationHomeSubCluster(request.getApplicationHomeSubCluster());
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link UpdateApplicationHomeSubClusterRequest}
   * for updating an application is valid or not.
   *
   * @param request the {@link UpdateApplicationHomeSubClusterRequest} to
   *          validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(UpdateApplicationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing UpdateApplicationHomeSubCluster Request."
          + " Please try again by specifying"
          + " an ApplicationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate ApplicationHomeSubCluster info
    checkApplicationHomeSubCluster(request.getApplicationHomeSubCluster());
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link GetApplicationHomeSubClusterRequest}
   * for querying application's information is valid or not.
   *
   * @param request the {@link GetApplicationHomeSubClusterRequest} to validate
   *          against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(GetApplicationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing GetApplicationHomeSubCluster Request."
          + " Please try again by specifying an Application Id information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate application Id
    checkApplicationId(request.getApplicationId());
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link DeleteApplicationHomeSubClusterRequest}
   * for deleting an application is valid or not.
   *
   * @param request the {@link DeleteApplicationHomeSubClusterRequest} to
   *          validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(DeleteApplicationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing DeleteApplicationHomeSubCluster Request."
          + " Please try again by specifying"
          + " an ApplicationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate application Id
    checkApplicationId(request.getApplicationId());
  }

  /**
   * Validate if the ApplicationHomeSubCluster info are present or not.
   *
   * @param applicationHomeSubCluster the information of the application to be
   *          verified
   * @throws FederationStateStoreInvalidInputException if the SubCluster Info
   *           are invalid
   */
  private static void checkApplicationHomeSubCluster(
      ApplicationHomeSubCluster applicationHomeSubCluster)

      throws FederationStateStoreInvalidInputException {
    if (applicationHomeSubCluster == null) {
      String message = "Missing ApplicationHomeSubCluster Info."
          + " Please try again by specifying"
          + " an ApplicationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
    // validate application Id
    checkApplicationId(applicationHomeSubCluster.getApplicationId());

    // validate subcluster Id
    FederationMembershipStateStoreInputValidator
        .checkSubClusterId(applicationHomeSubCluster.getHomeSubCluster());

  }

  /**
   * Validate if the application id is present or not.
   *
   * @param appId the id of the application to be verified
   * @throws FederationStateStoreInvalidInputException if the application Id is
   *           invalid
   */
  private static void checkApplicationId(ApplicationId appId)
      throws FederationStateStoreInvalidInputException {
    if (appId == null) {
      String message = "Missing Application Id."
          + " Please try again by specifying an Application Id.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }
}
