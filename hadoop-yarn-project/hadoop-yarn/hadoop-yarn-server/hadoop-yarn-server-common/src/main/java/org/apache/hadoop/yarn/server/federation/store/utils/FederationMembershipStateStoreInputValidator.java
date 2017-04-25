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

import java.net.URI;

import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to validate the inputs to
 * {@code FederationMembershipStateStore}, allows a fail fast mechanism for
 * invalid user inputs.
 *
 */
public final class FederationMembershipStateStoreInputValidator {

  private static final Logger LOG = LoggerFactory
      .getLogger(FederationMembershipStateStoreInputValidator.class);

  private FederationMembershipStateStoreInputValidator() {
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link SubClusterRegisterRequest} for
   * registration a new subcluster is valid or not.
   *
   * @param request the {@link SubClusterRegisterRequest} to validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(SubClusterRegisterRequest request)
      throws FederationStateStoreInvalidInputException {

    // check if the request is present
    if (request == null) {
      String message = "Missing SubClusterRegister Request."
          + " Please try again by specifying a"
          + " SubCluster Register Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);

    }

    // validate subcluster info
    checkSubClusterInfo(request.getSubClusterInfo());
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link SubClusterDeregisterRequest} for
   * deregistration a subcluster is valid or not.
   *
   * @param request the {@link SubClusterDeregisterRequest} to validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(SubClusterDeregisterRequest request)
      throws FederationStateStoreInvalidInputException {

    // check if the request is present
    if (request == null) {
      String message = "Missing SubClusterDeregister Request."
          + " Please try again by specifying a"
          + " SubCluster Deregister Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate subcluster id
    checkSubClusterId(request.getSubClusterId());
    // validate subcluster state
    checkSubClusterState(request.getState());
    if (!request.getState().isFinal()) {
      String message = "Invalid non-final state: " + request.getState();
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link SubClusterHeartbeatRequest} for
   * heartbeating a subcluster is valid or not.
   *
   * @param request the {@link SubClusterHeartbeatRequest} to validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(SubClusterHeartbeatRequest request)
      throws FederationStateStoreInvalidInputException {

    // check if the request is present
    if (request == null) {
      String message = "Missing SubClusterHeartbeat Request."
          + " Please try again by specifying a"
          + " SubCluster Heartbeat Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate subcluster id
    checkSubClusterId(request.getSubClusterId());
    // validate last heartbeat timestamp
    checkTimestamp(request.getLastHeartBeat());
    // validate subcluster capability
    checkCapability(request.getCapability());
    // validate subcluster state
    checkSubClusterState(request.getState());

  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast). Check if the provided {@link GetSubClusterInfoRequest} for querying
   * subcluster's information is valid or not.
   *
   * @param request the {@link GetSubClusterInfoRequest} to validate against
   * @throws FederationStateStoreInvalidInputException if the request is invalid
   */
  public static void validate(GetSubClusterInfoRequest request)
      throws FederationStateStoreInvalidInputException {

    // check if the request is present
    if (request == null) {
      String message = "Missing GetSubClusterInfo Request."
          + " Please try again by specifying a Get SubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate subcluster id
    checkSubClusterId(request.getSubClusterId());
  }

  /**
   * Validate if all the required fields on {@link SubClusterInfo} are present
   * or not. {@code Capability} will be empty as the corresponding
   * {@code ResourceManager} is in the process of initialization during
   * registration.
   *
   * @param subClusterInfo the information of the subcluster to be verified
   * @throws FederationStateStoreInvalidInputException if the SubCluster Info
   *           are invalid
   */
  public static void checkSubClusterInfo(SubClusterInfo subClusterInfo)
      throws FederationStateStoreInvalidInputException {
    if (subClusterInfo == null) {
      String message = "Missing SubCluster Information."
          + " Please try again by specifying SubCluster Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // validate subcluster id
    checkSubClusterId(subClusterInfo.getSubClusterId());

    // validate AMRM Service address
    checkAddress(subClusterInfo.getAMRMServiceAddress());
    // validate ClientRM Service address
    checkAddress(subClusterInfo.getClientRMServiceAddress());
    // validate RMClient Service address
    checkAddress(subClusterInfo.getRMAdminServiceAddress());
    // validate RMWeb Service address
    checkAddress(subClusterInfo.getRMWebServiceAddress());

    // validate last heartbeat timestamp
    checkTimestamp(subClusterInfo.getLastHeartBeat());
    // validate last start timestamp
    checkTimestamp(subClusterInfo.getLastStartTime());

    // validate subcluster state
    checkSubClusterState(subClusterInfo.getState());

  }

  /**
   * Validate if the timestamp is positive or not.
   *
   * @param timestamp the timestamp to be verified
   * @throws FederationStateStoreInvalidInputException if the timestamp is
   *           invalid
   */
  private static void checkTimestamp(long timestamp)
      throws FederationStateStoreInvalidInputException {
    if (timestamp < 0) {
      String message = "Invalid timestamp information."
          + " Please try again by specifying valid Timestamp Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * Validate if the Capability is present or not.
   *
   * @param capability the capability of the subcluster to be verified
   * @throws FederationStateStoreInvalidInputException if the capability is
   *           invalid
   */
  private static void checkCapability(String capability)
      throws FederationStateStoreInvalidInputException {
    if (capability == null || capability.isEmpty()) {
      String message = "Invalid capability information."
          + " Please try again by specifying valid Capability Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * Validate if the SubCluster Id is present or not.
   *
   * @param subClusterId the identifier of the subcluster to be verified
   * @throws FederationStateStoreInvalidInputException if the SubCluster Id is
   *           invalid
   */
  protected static void checkSubClusterId(SubClusterId subClusterId)
      throws FederationStateStoreInvalidInputException {
    // check if cluster id is present
    if (subClusterId == null) {
      String message = "Missing SubCluster Id information."
          + " Please try again by specifying Subcluster Id information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
    // check if cluster id is valid
    if (subClusterId.getId().isEmpty()) {
      String message = "Invalid SubCluster Id information."
          + " Please try again by specifying valid Subcluster Id.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * Validate if the SubCluster Address is a valid URL or not.
   *
   * @param address the endpoint of the subcluster to be verified
   * @throws FederationStateStoreInvalidInputException if the address is invalid
   */
  private static void checkAddress(String address)
      throws FederationStateStoreInvalidInputException {
    // Ensure url is not null
    if (address == null || address.isEmpty()) {
      String message = "Missing SubCluster Endpoint information."
          + " Please try again by specifying SubCluster Endpoint information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
    // Validate url is well formed
    boolean hasScheme = address.contains("://");
    URI uri = null;
    try {
      uri = hasScheme ? URI.create(address)
          : URI.create("dummyscheme://" + address);
    } catch (IllegalArgumentException e) {
      String message = "The provided SubCluster Endpoint does not contain a"
          + " valid host:port authority: " + address;
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
    String host = uri.getHost();
    int port = uri.getPort();
    String path = uri.getPath();
    if ((host == null) || (port < 0)
        || (!hasScheme && path != null && !path.isEmpty())) {
      String message = "The provided SubCluster Endpoint does not contain a"
          + " valid host:port authority: " + address;
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * Validate if the SubCluster State is present or not.
   *
   * @param state the state of the subcluster to be verified
   * @throws FederationStateStoreInvalidInputException if the SubCluster State
   *           is invalid
   */
  private static void checkSubClusterState(SubClusterState state)
      throws FederationStateStoreInvalidInputException {
    // check sub-cluster state is not empty
    if (state == null) {
      String message = "Missing SubCluster State information."
          + " Please try again by specifying SubCluster State information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

}
