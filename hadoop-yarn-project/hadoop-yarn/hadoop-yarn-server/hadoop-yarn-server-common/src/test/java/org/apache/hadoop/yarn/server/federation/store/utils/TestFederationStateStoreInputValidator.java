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

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for FederationApplicationInputValidator,
 * FederationMembershipInputValidator, and FederationPolicyInputValidator.
 */
public class TestFederationStateStoreInputValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFederationStateStoreInputValidator.class);

  private static SubClusterId subClusterId;
  private static String amRMServiceAddress;
  private static String clientRMServiceAddress;
  private static String rmAdminServiceAddress;
  private static String rmWebServiceAddress;
  private static int lastHeartBeat;
  private static SubClusterState stateNew;
  private static SubClusterState stateLost;
  private static ApplicationId appId;
  private static int lastStartTime;
  private static String capability;
  private static String queue;
  private static String type;
  private static ByteBuffer params;

  private static SubClusterId subClusterIdInvalid;
  private static SubClusterId subClusterIdNull;

  private static int lastHeartBeatNegative;
  private static int lastStartTimeNegative;

  private static SubClusterState stateNull;
  private static ApplicationId appIdNull;

  private static String capabilityNull;
  private static String capabilityEmpty;

  private static String addressNull;
  private static String addressEmpty;
  private static String addressWrong;
  private static String addressWrongPort;

  private static String queueEmpty;
  private static String queueNull;

  private static String typeEmpty;
  private static String typeNull;

  @BeforeClass
  public static void setUp() {
    subClusterId = SubClusterId.newInstance("abc");
    amRMServiceAddress = "localhost:8032";
    clientRMServiceAddress = "localhost:8034";
    rmAdminServiceAddress = "localhost:8031";
    rmWebServiceAddress = "localhost:8088";
    lastHeartBeat = 1000;
    stateNew = SubClusterState.SC_NEW;
    stateLost = SubClusterState.SC_LOST;
    lastStartTime = 1000;
    capability = "Memory VCores";
    appId = ApplicationId.newInstance(lastStartTime, 1);
    queue = "default";
    type = "random";
    params = ByteBuffer.allocate(10);
    params.put((byte) 0xFF);

    subClusterIdInvalid = SubClusterId.newInstance("");
    subClusterIdNull = null;

    lastHeartBeatNegative = -10;
    lastStartTimeNegative = -10;

    stateNull = null;
    appIdNull = null;

    capabilityNull = null;
    capabilityEmpty = "";

    addressNull = null;
    addressEmpty = "";
    addressWrong = "AddressWrong";
    addressWrongPort = "Address:WrongPort";

    queueEmpty = "";
    queueNull = null;

    typeEmpty = "";
    typeNull = null;
  }

  @Test
  public void testValidateSubClusterRegisterRequest() {

    // Execution with valid inputs

    SubClusterInfo subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }

    // Execution with null request

    try {
      SubClusterRegisterRequest request = null;
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubClusterRegister Request."));
    }

    // Execution with null SubClusterInfo

    subClusterInfo = null;
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubCluster Information."));
    }

    // Execution with Null SubClusterId

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterIdNull, amRMServiceAddress,
            clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubCluster Id information."));
    }

    // Execution with Invalid SubClusterId

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterIdInvalid, amRMServiceAddress,
            clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid SubCluster Id information."));
    }

    // Execution with Null State

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNull, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubCluster State information."));
    }

    // Execution with Null Capability

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNew, lastStartTime, capabilityNull);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }

    // Execution with Empty Capability

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNew, lastStartTime, capabilityEmpty);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testValidateSubClusterRegisterRequestTimestamp() {

    // Execution with Negative Last Heartbeat

    SubClusterInfo subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeatNegative, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid timestamp information."));
    }

    // Execution with Negative Last StartTime

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNew, lastStartTimeNegative, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid timestamp information."));
    }
  }

  @Test
  public void testValidateSubClusterRegisterRequestAddress() {
    // Execution with Null Address for amRMServiceAddress

    SubClusterInfo subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, addressNull,
            clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing SubCluster Endpoint information."));
    }

    // Execution with Empty Address for amRMServiceAddress

    subClusterInfo = SubClusterInfo.newInstance(subClusterId, addressEmpty,
        clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
        lastHeartBeat, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing SubCluster Endpoint information."));
    }

    // Execution with Null Address for clientRMServiceAddress

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            addressNull, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing SubCluster Endpoint information."));
    }

    // Execution with Empty Address for clientRMServiceAddress

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            addressEmpty, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing SubCluster Endpoint information."));
    }

    // Execution with Null Address for rmAdminServiceAddress

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            clientRMServiceAddress, addressNull, rmWebServiceAddress,
            lastHeartBeat, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing SubCluster Endpoint information."));
    }

    // Execution with Empty Address for rmAdminServiceAddress

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            clientRMServiceAddress, addressEmpty, rmWebServiceAddress,
            lastHeartBeat, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing SubCluster Endpoint information."));
    }

    // Execution with Null Address for rmWebServiceAddress

    subClusterInfo = SubClusterInfo.newInstance(subClusterId,
        amRMServiceAddress, clientRMServiceAddress, rmAdminServiceAddress,
        addressNull, lastHeartBeat, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing SubCluster Endpoint information."));
    }

    // Execution with Empty Address for rmWebServiceAddress

    subClusterInfo = SubClusterInfo.newInstance(subClusterId,
        amRMServiceAddress, clientRMServiceAddress, rmAdminServiceAddress,
        addressEmpty, lastHeartBeat, stateNew, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing SubCluster Endpoint information."));
    }
  }

  @Test
  public void testValidateSubClusterRegisterRequestAddressInvalid() {

    // Address is not in host:port format for amRMService

    SubClusterInfo subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, addressWrong,
            clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNull, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("valid host:port authority:"));
    }

    // Address is not in host:port format for clientRMService

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            addressWrong, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNull, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("valid host:port authority:"));
    }

    // Address is not in host:port format for rmAdminService

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            clientRMServiceAddress, addressWrong, rmWebServiceAddress,
            lastHeartBeat, stateNull, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("valid host:port authority:"));
    }

    // Address is not in host:port format for rmWebService

    subClusterInfo = SubClusterInfo.newInstance(subClusterId,
        amRMServiceAddress, clientRMServiceAddress, rmAdminServiceAddress,
        addressWrong, lastHeartBeat, stateNull, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("valid host:port authority:"));
    }

    // Port is not an integer for amRMService

    subClusterInfo = SubClusterInfo.newInstance(subClusterId, addressWrongPort,
        clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
        lastHeartBeat, stateNull, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("valid host:port authority:"));
    }

    // Port is not an integer for clientRMService

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            addressWrongPort, rmAdminServiceAddress, rmWebServiceAddress,
            lastHeartBeat, stateNull, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("valid host:port authority:"));
    }

    // Port is not an integer for rmAdminService

    subClusterInfo =
        SubClusterInfo.newInstance(subClusterId, amRMServiceAddress,
            clientRMServiceAddress, addressWrongPort, rmWebServiceAddress,
            lastHeartBeat, stateNull, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("valid host:port authority:"));
    }

    // Port is not an integer for rmWebService

    subClusterInfo = SubClusterInfo.newInstance(subClusterId,
        amRMServiceAddress, clientRMServiceAddress, rmAdminServiceAddress,
        addressWrongPort, lastHeartBeat, stateNull, lastStartTime, capability);
    try {
      SubClusterRegisterRequest request =
          SubClusterRegisterRequest.newInstance(subClusterInfo);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("valid host:port authority:"));
    }

  }

  @Test
  public void testValidateSubClusterDeregisterRequest() {

    // Execution with valid inputs

    try {
      SubClusterDeregisterRequest request =
          SubClusterDeregisterRequest.newInstance(subClusterId, stateLost);
      FederationMembershipStateStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }

    // Execution with null request

    try {
      SubClusterDeregisterRequest request = null;
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubClusterDeregister Request."));
    }

    // Execution with null SubClusterId

    try {
      SubClusterDeregisterRequest request =
          SubClusterDeregisterRequest.newInstance(subClusterIdNull, stateLost);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubCluster Id information."));
    }

    // Execution with invalid SubClusterId

    try {
      SubClusterDeregisterRequest request = SubClusterDeregisterRequest
          .newInstance(subClusterIdInvalid, stateLost);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid SubCluster Id information."));
    }

    // Execution with null SubClusterState

    try {
      SubClusterDeregisterRequest request =
          SubClusterDeregisterRequest.newInstance(subClusterId, stateNull);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubCluster State information."));
    }

    // Execution with invalid SubClusterState

    try {
      SubClusterDeregisterRequest request =
          SubClusterDeregisterRequest.newInstance(subClusterId, stateNew);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(e.getMessage().startsWith("Invalid non-final state: "));
    }
  }

  @Test
  public void testSubClusterHeartbeatRequest() {

    // Execution with valid inputs

    try {
      SubClusterHeartbeatRequest request = SubClusterHeartbeatRequest
          .newInstance(subClusterId, lastHeartBeat, stateLost, capability);
      FederationMembershipStateStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }

    // Execution with null request

    try {
      SubClusterHeartbeatRequest request = null;
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubClusterHeartbeat Request."));
    }

    // Execution with null SubClusterId

    try {
      SubClusterHeartbeatRequest request = SubClusterHeartbeatRequest
          .newInstance(subClusterIdNull, lastHeartBeat, stateLost, capability);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubCluster Id information."));
    }

    // Execution with invalid SubClusterId

    try {
      SubClusterHeartbeatRequest request =
          SubClusterHeartbeatRequest.newInstance(subClusterIdInvalid,
              lastHeartBeat, stateLost, capability);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid SubCluster Id information."));
    }

    // Execution with null SubClusterState

    try {
      SubClusterHeartbeatRequest request = SubClusterHeartbeatRequest
          .newInstance(subClusterId, lastHeartBeat, stateNull, capability);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubCluster State information."));
    }

    // Execution with negative Last Heartbeat

    try {
      SubClusterHeartbeatRequest request =
          SubClusterHeartbeatRequest.newInstance(subClusterId,
              lastHeartBeatNegative, stateLost, capability);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid timestamp information."));
    }

    // Execution with null Capability

    try {
      SubClusterHeartbeatRequest request = SubClusterHeartbeatRequest
          .newInstance(subClusterId, lastHeartBeat, stateLost, capabilityNull);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid capability information."));
    }

    // Execution with empty Capability

    try {
      SubClusterHeartbeatRequest request = SubClusterHeartbeatRequest
          .newInstance(subClusterId, lastHeartBeat, stateLost, capabilityEmpty);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid capability information."));
    }
  }

  @Test
  public void testGetSubClusterInfoRequest() {

    // Execution with valid inputs

    try {
      GetSubClusterInfoRequest request =
          GetSubClusterInfoRequest.newInstance(subClusterId);
      FederationMembershipStateStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }

    // Execution with null request

    try {
      GetSubClusterInfoRequest request = null;
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing GetSubClusterInfo Request."));
    }

    // Execution with null SubClusterId

    try {
      GetSubClusterInfoRequest request =
          GetSubClusterInfoRequest.newInstance(subClusterIdNull);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubCluster Id information."));
    }

    // Execution with invalid SubClusterId

    try {
      GetSubClusterInfoRequest request =
          GetSubClusterInfoRequest.newInstance(subClusterIdInvalid);
      FederationMembershipStateStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid SubCluster Id information."));
    }
  }

  @Test
  public void testAddApplicationHomeSubClusterRequest() {

    // Execution with valid inputs

    ApplicationHomeSubCluster applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId);
    try {
      AddApplicationHomeSubClusterRequest request =
          AddApplicationHomeSubClusterRequest
              .newInstance(applicationHomeSubCluster);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }

    // Execution with null request

    try {
      AddApplicationHomeSubClusterRequest request = null;
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing AddApplicationHomeSubCluster Request."));
    }

    // Execution with null ApplicationHomeSubCluster

    applicationHomeSubCluster = null;
    try {
      AddApplicationHomeSubClusterRequest request =
          AddApplicationHomeSubClusterRequest
              .newInstance(applicationHomeSubCluster);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Missing ApplicationHomeSubCluster Info."));
    }

    // Execution with null SubClusterId

    applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, subClusterIdNull);
    try {
      AddApplicationHomeSubClusterRequest request =
          AddApplicationHomeSubClusterRequest
              .newInstance(applicationHomeSubCluster);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubCluster Id information."));
    }

    // Execution with invalid SubClusterId

    applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, subClusterIdInvalid);
    try {
      AddApplicationHomeSubClusterRequest request =
          AddApplicationHomeSubClusterRequest
              .newInstance(applicationHomeSubCluster);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid SubCluster Id information."));
    }

    // Execution with Null ApplicationId

    applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appIdNull, subClusterId);
    try {
      AddApplicationHomeSubClusterRequest request =
          AddApplicationHomeSubClusterRequest
              .newInstance(applicationHomeSubCluster);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage().startsWith("Missing Application Id."));
    }
  }

  @Test
  public void testUpdateApplicationHomeSubClusterRequest() {

    // Execution with valid inputs

    ApplicationHomeSubCluster applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId);
    try {
      UpdateApplicationHomeSubClusterRequest request =
          UpdateApplicationHomeSubClusterRequest
              .newInstance(applicationHomeSubCluster);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }

    // Execution with null request

    try {
      UpdateApplicationHomeSubClusterRequest request = null;
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing UpdateApplicationHomeSubCluster Request."));
    }

    // Execution with null ApplicationHomeSubCluster

    applicationHomeSubCluster = null;
    try {
      UpdateApplicationHomeSubClusterRequest request =
          UpdateApplicationHomeSubClusterRequest
              .newInstance(applicationHomeSubCluster);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Missing ApplicationHomeSubCluster Info."));
    }

    // Execution with null SubClusteId

    applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, subClusterIdNull);
    try {
      UpdateApplicationHomeSubClusterRequest request =
          UpdateApplicationHomeSubClusterRequest
              .newInstance(applicationHomeSubCluster);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubCluster Id information."));
    }

    // Execution with invalid SubClusterId

    applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, subClusterIdInvalid);
    try {
      UpdateApplicationHomeSubClusterRequest request =
          UpdateApplicationHomeSubClusterRequest
              .newInstance(applicationHomeSubCluster);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      LOG.info(e.getMessage());
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid SubCluster Id information."));
    }

    // Execution with null ApplicationId

    applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appIdNull, subClusterId);
    try {
      UpdateApplicationHomeSubClusterRequest request =
          UpdateApplicationHomeSubClusterRequest
              .newInstance(applicationHomeSubCluster);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage().startsWith("Missing Application Id."));
    }
  }

  @Test
  public void testGetApplicationHomeSubClusterRequest() {

    // Execution with valid inputs

    try {
      GetApplicationHomeSubClusterRequest request =
          GetApplicationHomeSubClusterRequest.newInstance(appId);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }

    // Execution with null request

    try {
      GetApplicationHomeSubClusterRequest request = null;
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing GetApplicationHomeSubCluster Request."));
    }

    // Execution with null ApplicationId

    try {
      GetApplicationHomeSubClusterRequest request =
          GetApplicationHomeSubClusterRequest.newInstance(appIdNull);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage().startsWith("Missing Application Id."));
    }

  }

  @Test
  public void testDeleteApplicationHomeSubClusterRequestNull() {

    // Execution with valid inputs

    try {
      DeleteApplicationHomeSubClusterRequest request =
          DeleteApplicationHomeSubClusterRequest.newInstance(appId);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }

    // Execution with null request

    try {
      DeleteApplicationHomeSubClusterRequest request = null;
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing DeleteApplicationHomeSubCluster Request."));
    }

    // Execution with null ApplicationId

    try {
      DeleteApplicationHomeSubClusterRequest request =
          DeleteApplicationHomeSubClusterRequest.newInstance(appIdNull);
      FederationApplicationHomeSubClusterStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage().startsWith("Missing Application Id."));
    }

  }

  @Test
  public void testGetSubClusterPolicyConfigurationRequest() {

    // Execution with valid inputs

    try {
      GetSubClusterPolicyConfigurationRequest request =
          GetSubClusterPolicyConfigurationRequest.newInstance(queue);
      FederationPolicyStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }

    // Execution with null request

    try {
      GetSubClusterPolicyConfigurationRequest request = null;
      FederationPolicyStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing GetSubClusterPolicyConfiguration Request."));
    }

    // Execution with null queue id

    try {
      GetSubClusterPolicyConfigurationRequest request =
          GetSubClusterPolicyConfigurationRequest.newInstance(queueNull);
      FederationPolicyStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage().startsWith("Missing Queue."));
    }

    // Execution with empty queue id

    try {
      GetSubClusterPolicyConfigurationRequest request =
          GetSubClusterPolicyConfigurationRequest.newInstance(queueEmpty);
      FederationPolicyStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage().startsWith("Missing Queue."));
    }

  }

  @Test
  public void testSetSubClusterPolicyConfigurationRequest() {

    // Execution with valid inputs

    try {
      SubClusterPolicyConfiguration policy =
          SubClusterPolicyConfiguration.newInstance(queue, type, params);
      SetSubClusterPolicyConfigurationRequest request =
          SetSubClusterPolicyConfigurationRequest.newInstance(policy);
      FederationPolicyStoreInputValidator
          .validate(request);
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.fail(e.getMessage());
    }

    // Execution with null request

    try {
      SetSubClusterPolicyConfigurationRequest request = null;
      FederationPolicyStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Missing SetSubClusterPolicyConfiguration Request."));
    }

    // Execution with null SubClusterPolicyConfiguration

    try {
      SubClusterPolicyConfiguration policy = null;
      SetSubClusterPolicyConfigurationRequest request =
          SetSubClusterPolicyConfigurationRequest.newInstance(policy);
      FederationPolicyStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Missing SubClusterPolicyConfiguration."));
    }

    // Execution with null queue id

    try {
      SubClusterPolicyConfiguration policy =
          SubClusterPolicyConfiguration.newInstance(queueNull, type, params);
      SetSubClusterPolicyConfigurationRequest request =
          SetSubClusterPolicyConfigurationRequest.newInstance(policy);
      FederationPolicyStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage().startsWith("Missing Queue."));
    }

    // Execution with empty queue id

    try {
      SubClusterPolicyConfiguration policy =
          SubClusterPolicyConfiguration.newInstance(queueEmpty, type, params);
      SetSubClusterPolicyConfigurationRequest request =
          SetSubClusterPolicyConfigurationRequest.newInstance(policy);
      FederationPolicyStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage().startsWith("Missing Queue."));
    }

    // Execution with null policy type

    try {
      SubClusterPolicyConfiguration policy =
          SubClusterPolicyConfiguration.newInstance(queue, typeNull, params);
      SetSubClusterPolicyConfigurationRequest request =
          SetSubClusterPolicyConfigurationRequest.newInstance(policy);
      FederationPolicyStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage().startsWith("Missing Policy Type."));
    }

    // Execution with empty policy type

    try {
      SubClusterPolicyConfiguration policy =
          SubClusterPolicyConfiguration.newInstance(queue, typeEmpty, params);
      SetSubClusterPolicyConfigurationRequest request =
          SetSubClusterPolicyConfigurationRequest.newInstance(policy);
      FederationPolicyStoreInputValidator
          .validate(request);
      Assert.fail();
    } catch (FederationStateStoreInvalidInputException e) {
      Assert.assertTrue(e.getMessage().startsWith("Missing Policy Type."));
    }
  }

}
