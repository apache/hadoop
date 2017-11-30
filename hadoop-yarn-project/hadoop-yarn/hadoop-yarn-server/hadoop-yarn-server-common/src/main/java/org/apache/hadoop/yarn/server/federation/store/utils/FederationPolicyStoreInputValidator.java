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

import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to validate the inputs to {@code FederationPolicyStore}, allows
 * a fail fast mechanism for invalid user inputs.
 *
 */
public final class FederationPolicyStoreInputValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationPolicyStoreInputValidator.class);

  private FederationPolicyStoreInputValidator() {
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided
   * {@link GetSubClusterPolicyConfigurationRequest} for querying policy's
   * information is valid or not.
   *
   * @param request the {@link GetSubClusterPolicyConfigurationRequest} to
   *          validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(GetSubClusterPolicyConfigurationRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing GetSubClusterPolicyConfiguration Request."
          + " Please try again by specifying a policy selection information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate queue id
    checkQueue(request.getQueue());
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided
   * {@link SetSubClusterPolicyConfigurationRequest} for adding a new policy is
   * valid or not.
   *
   * @param request the {@link SetSubClusterPolicyConfigurationRequest} to
   *          validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(SetSubClusterPolicyConfigurationRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing SetSubClusterPolicyConfiguration Request."
          + " Please try again by specifying an policy insertion information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate subcluster policy configuration
    checkSubClusterPolicyConfiguration(request.getPolicyConfiguration());
  }

  /**
   * Validate if the SubClusterPolicyConfiguration is valid or not.
   *
   * @param policyConfiguration the policy information to be verified
   * @throws FederationStateStoreInvalidInputException if the policy information
   *           are invalid
   */
  private static void checkSubClusterPolicyConfiguration(
      SubClusterPolicyConfiguration policyConfiguration)
      throws FederationStateStoreInvalidInputException {
    if (policyConfiguration == null) {
      String message = "Missing SubClusterPolicyConfiguration."
          + " Please try again by specifying a SubClusterPolicyConfiguration.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate queue id
    checkQueue(policyConfiguration.getQueue());
    // validate policy type
    checkType(policyConfiguration.getType());

  }

  /**
   * Validate if the queue id is a valid or not.
   *
   * @param queue the queue id of the policy to be verified
   * @throws FederationStateStoreInvalidInputException if the queue id is
   *           invalid
   */
  private static void checkQueue(String queue)
      throws FederationStateStoreInvalidInputException {
    if (queue == null || queue.isEmpty()) {
      String message = "Missing Queue. Please try again by specifying a Queue.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * Validate if the policy type is a valid or not.
   *
   * @param type the type of the policy to be verified
   * @throws FederationStateStoreInvalidInputException if the policy is invalid
   */
  private static void checkType(String type)
      throws FederationStateStoreInvalidInputException {
    if (type == null || type.isEmpty()) {
      String message = "Missing Policy Type."
          + " Please try again by specifying a Policy Type.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

}
