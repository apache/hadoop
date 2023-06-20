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

import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.BasePBImplRecordsTest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationsHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationsHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterInfoRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterInfoResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPoliciesConfigurationsRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPoliciesConfigurationsResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPolicyConfigurationRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPolicyConfigurationResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClustersInfoRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClustersInfoResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SetSubClusterPolicyConfigurationRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SetSubClusterPolicyConfigurationResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterDeregisterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterDeregisterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterHeartbeatRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterHeartbeatResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterRegisterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterRegisterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterRMTokenRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterRMTokenResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetReservationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProto;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.AddApplicationHomeSubClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.AddApplicationHomeSubClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.DeleteApplicationHomeSubClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.DeleteApplicationHomeSubClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetApplicationHomeSubClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetApplicationHomeSubClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetApplicationsHomeSubClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetApplicationsHomeSubClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterInfoRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterInfoResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterPoliciesConfigurationsRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterPoliciesConfigurationsResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterPolicyConfigurationRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterPolicyConfigurationResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClustersInfoRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClustersInfoResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SetSubClusterPolicyConfigurationRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SetSubClusterPolicyConfigurationResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterDeregisterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterDeregisterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterIdPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterInfoPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterRegisterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterRegisterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.UpdateApplicationHomeSubClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.UpdateApplicationHomeSubClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.RouterMasterKeyPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.RouterMasterKeyRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.RouterMasterKeyResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.RouterStoreTokenPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.RouterRMTokenRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.RouterRMTokenResponsePBImpl;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.ApplicationHomeSubClusterPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetReservationHomeSubClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.records.Version;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

/**
 * Test class for federation protocol records.
 */
public class TestFederationProtocolRecords extends BasePBImplRecordsTest {

  @BeforeClass
  public static void setup() throws Exception {
    generateByNewInstance(ApplicationId.class);
    generateByNewInstance(Version.class);
    generateByNewInstance(SubClusterId.class);
    generateByNewInstance(SubClusterInfo.class);
    generateByNewInstance(Priority.class);
    generateByNewInstance(URL.class);
    generateByNewInstance(Resource.class);
    generateByNewInstance(ContainerRetryContext.class);
    generateByNewInstance(LocalResource.class);
    generateByNewInstance(ContainerLaunchContext.class);
    generateByNewInstance(LogAggregationContext.class);
    generateByNewInstance(ApplicationSubmissionContext.class);
    generateByNewInstance(ApplicationHomeSubCluster.class);
    generateByNewInstance(SubClusterPolicyConfiguration.class);
    generateByNewInstance(RouterMasterKey.class);
    generateByNewInstance(YARNDelegationTokenIdentifier.class);
    generateByNewInstance(RouterStoreToken.class);
    generateByNewInstance(ReservationId.class);
  }

  @Test
  public void testSubClusterId() throws Exception {
    validatePBImplRecord(SubClusterIdPBImpl.class, SubClusterIdProto.class);
  }

  @Test
  public void testSubClusterInfo() throws Exception {
    validatePBImplRecord(SubClusterInfoPBImpl.class, SubClusterInfoProto.class);
  }

  @Test
  public void testSubClusterRegisterRequest() throws Exception {
    validatePBImplRecord(SubClusterRegisterRequestPBImpl.class,
        SubClusterRegisterRequestProto.class);
  }

  @Test
  public void testSubClusterRegisterResponse() throws Exception {
    validatePBImplRecord(SubClusterRegisterResponsePBImpl.class,
        SubClusterRegisterResponseProto.class);
  }

  @Test
  public void testSubClusterDeregisterRequest() throws Exception {
    validatePBImplRecord(SubClusterDeregisterRequestPBImpl.class,
        SubClusterDeregisterRequestProto.class);
  }

  @Test
  public void testSubClusterDeregisterResponse() throws Exception {
    validatePBImplRecord(SubClusterDeregisterResponsePBImpl.class,
        SubClusterDeregisterResponseProto.class);
  }

  @Test
  public void testSubClusterHeartbeatRequest() throws Exception {
    validatePBImplRecord(SubClusterHeartbeatRequestPBImpl.class,
        SubClusterHeartbeatRequestProto.class);
  }

  @Test
  public void testSubClusterHeartbeatResponse() throws Exception {
    validatePBImplRecord(SubClusterHeartbeatResponsePBImpl.class,
        SubClusterHeartbeatResponseProto.class);
  }

  @Test
  public void testGetSubClusterRequest() throws Exception {
    validatePBImplRecord(GetSubClusterInfoRequestPBImpl.class,
        GetSubClusterInfoRequestProto.class);
  }

  @Test
  public void testGetSubClusterResponse() throws Exception {
    validatePBImplRecord(GetSubClusterInfoResponsePBImpl.class,
        GetSubClusterInfoResponseProto.class);
  }

  @Test
  public void testGetSubClustersInfoRequest() throws Exception {
    validatePBImplRecord(GetSubClustersInfoRequestPBImpl.class,
        GetSubClustersInfoRequestProto.class);
  }

  @Test
  public void testGetSubClustersInfoResponse() throws Exception {
    validatePBImplRecord(GetSubClustersInfoResponsePBImpl.class,
        GetSubClustersInfoResponseProto.class);
  }

  @Test
  public void testAddApplicationHomeSubClusterRequest() throws Exception {
    validatePBImplRecord(AddApplicationHomeSubClusterRequestPBImpl.class,
        AddApplicationHomeSubClusterRequestProto.class);
  }

  @Test
  public void testAddApplicationHomeSubClusterResponse() throws Exception {
    validatePBImplRecord(AddApplicationHomeSubClusterResponsePBImpl.class,
        AddApplicationHomeSubClusterResponseProto.class);
  }

  @Test
  public void testUpdateApplicationHomeSubClusterRequest() throws Exception {
    validatePBImplRecord(UpdateApplicationHomeSubClusterRequestPBImpl.class,
        UpdateApplicationHomeSubClusterRequestProto.class);
  }

  @Test
  public void testUpdateApplicationHomeSubClusterResponse() throws Exception {
    validatePBImplRecord(UpdateApplicationHomeSubClusterResponsePBImpl.class,
        UpdateApplicationHomeSubClusterResponseProto.class);
  }

  @Test
  public void testGetApplicationHomeSubClusterRequest() throws Exception {
    validatePBImplRecord(GetApplicationHomeSubClusterRequestPBImpl.class,
        GetApplicationHomeSubClusterRequestProto.class);
  }

  @Test
  public void testGetApplicationHomeSubClusterResponse() throws Exception {
    validatePBImplRecord(GetApplicationHomeSubClusterResponsePBImpl.class,
        GetApplicationHomeSubClusterResponseProto.class);
  }

  @Test
  public void testGetApplicationsHomeSubClusterRequest() throws Exception {
    validatePBImplRecord(GetApplicationsHomeSubClusterRequestPBImpl.class,
        GetApplicationsHomeSubClusterRequestProto.class);
  }

  @Test
  public void testGetApplicationsHomeSubClusterResponse() throws Exception {
    validatePBImplRecord(GetApplicationsHomeSubClusterResponsePBImpl.class,
        GetApplicationsHomeSubClusterResponseProto.class);
  }

  @Test
  public void testDeleteApplicationHomeSubClusterRequest() throws Exception {
    validatePBImplRecord(DeleteApplicationHomeSubClusterRequestPBImpl.class,
        DeleteApplicationHomeSubClusterRequestProto.class);
  }

  @Test
  public void testDeleteApplicationHomeSubClusterResponse() throws Exception {
    validatePBImplRecord(DeleteApplicationHomeSubClusterResponsePBImpl.class,
        DeleteApplicationHomeSubClusterResponseProto.class);
  }

  @Test
  public void testGetSubClusterPolicyConfigurationRequest() throws Exception {
    validatePBImplRecord(GetSubClusterPolicyConfigurationRequestPBImpl.class,
        GetSubClusterPolicyConfigurationRequestProto.class);
  }

  @Test
  public void testGetSubClusterPolicyConfigurationResponse() throws Exception {
    validatePBImplRecord(GetSubClusterPolicyConfigurationResponsePBImpl.class,
        GetSubClusterPolicyConfigurationResponseProto.class);
  }

  @Test
  public void testSetSubClusterPolicyConfigurationRequest() throws Exception {
    validatePBImplRecord(SetSubClusterPolicyConfigurationRequestPBImpl.class,
        SetSubClusterPolicyConfigurationRequestProto.class);
  }

  @Test
  public void testSetSubClusterPolicyConfigurationResponse() throws Exception {
    validatePBImplRecord(SetSubClusterPolicyConfigurationResponsePBImpl.class,
        SetSubClusterPolicyConfigurationResponseProto.class);
  }

  @Test
  public void testGetSubClusterPoliciesConfigurationsRequest()
      throws Exception {
    validatePBImplRecord(GetSubClusterPoliciesConfigurationsRequestPBImpl.class,
        GetSubClusterPoliciesConfigurationsRequestProto.class);
  }

  @Test
  public void testGetSubClusterPoliciesConfigurationsResponse()
      throws Exception {
    validatePBImplRecord(
        GetSubClusterPoliciesConfigurationsResponsePBImpl.class,
        GetSubClusterPoliciesConfigurationsResponseProto.class);
  }

  @Test
  public void testRouterMasterKey() throws Exception {
    validatePBImplRecord(RouterMasterKeyPBImpl.class, RouterMasterKeyProto.class);
  }

  @Test
  public void testRouterMasterKeyRequest() throws Exception {
    validatePBImplRecord(RouterMasterKeyRequestPBImpl.class, RouterMasterKeyRequestProto.class);
  }

  @Test
  public void testRouterMasterKeyResponse() throws Exception {
    validatePBImplRecord(RouterMasterKeyResponsePBImpl.class, RouterMasterKeyResponseProto.class);
  }

  @Test
  public void testRouterStoreToken() throws Exception {
    validatePBImplRecord(RouterStoreTokenPBImpl.class, RouterStoreTokenProto.class);
  }

  @Test
  public void testRouterRMTokenRequest() throws Exception {
    validatePBImplRecord(RouterRMTokenRequestPBImpl.class, RouterRMTokenRequestProto.class);
  }

  @Test
  public void testRouterRMTokenResponse() throws Exception {
    validatePBImplRecord(RouterRMTokenResponsePBImpl.class, RouterRMTokenResponseProto.class);
  }

  @Test
  public void testApplicationHomeSubCluster() throws Exception {
    validatePBImplRecord(ApplicationHomeSubClusterPBImpl.class,
        ApplicationHomeSubClusterProto.class);
  }

  @Test
  public void testGetReservationHomeSubClusterRequest() throws Exception {
    validatePBImplRecord(GetReservationHomeSubClusterRequestPBImpl.class,
        GetReservationHomeSubClusterRequestProto.class);
  }

  @Test
  public void testValidateApplicationHomeSubClusterEqual() throws Exception {
    long now = Time.now();

    ApplicationId appId1 = ApplicationId.newInstance(now, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC-1");
    ApplicationHomeSubCluster applicationHomeSubCluster1 =
        ApplicationHomeSubCluster.newInstance(appId1, subClusterId1);

    ApplicationId appId2 = ApplicationId.newInstance(now, 1);
    SubClusterId subClusterId2 = SubClusterId.newInstance("SC-1");
    ApplicationHomeSubCluster applicationHomeSubCluster2 =
        ApplicationHomeSubCluster.newInstance(appId2, subClusterId2);

    assertEquals(applicationHomeSubCluster1, applicationHomeSubCluster2);
  }

  @Test
  public void testValidateReservationHomeSubClusterEqual() throws Exception {
    long now = Time.now();

    ReservationId reservationId1 = ReservationId.newInstance(now, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC-1");
    ReservationHomeSubCluster reservationHomeSubCluster1 =
        ReservationHomeSubCluster.newInstance(reservationId1, subClusterId1);

    ReservationId reservationId2 = ReservationId.newInstance(now, 1);
    SubClusterId subClusterId2 = SubClusterId.newInstance("SC-1");
    ReservationHomeSubCluster reservationHomeSubCluster2 =
        ReservationHomeSubCluster.newInstance(reservationId2, subClusterId2);

    assertEquals(reservationHomeSubCluster1, reservationHomeSubCluster2);
  }

  @Test
  public void testSubClusterIdEqual() throws Exception {
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC-1");
    SubClusterId subClusterId2 = SubClusterId.newInstance("SC-1");
    assertEquals(subClusterId1, subClusterId2);
  }

  @Test
  public void testSubClusterIdInfoEqual() throws Exception {
    SubClusterIdInfo subClusterIdInfo1 = new SubClusterIdInfo("SC-1");
    SubClusterIdInfo subClusterIdInfo2 = new SubClusterIdInfo("SC-1");
    assertEquals(subClusterIdInfo1, subClusterIdInfo2);
  }

  @Test
  public void testSubClusterPolicyConfigurationEqual() throws Exception {

    String queue1 = "queue1";
    WeightedPolicyInfo policyInfo1 = mock(WeightedPolicyInfo.class);
    ByteBuffer buf1 = policyInfo1.toByteBuffer();
    SubClusterPolicyConfiguration configuration1 = SubClusterPolicyConfiguration
        .newInstance(queue1, policyInfo1.getClass().getCanonicalName(), buf1);

    String queue2 = "queue1";
    WeightedPolicyInfo policyInfo2 = mock(WeightedPolicyInfo.class);
    ByteBuffer buf2 = policyInfo1.toByteBuffer();
    SubClusterPolicyConfiguration configuration2 = SubClusterPolicyConfiguration
        .newInstance(queue2, policyInfo2.getClass().getCanonicalName(), buf2);

    assertEquals(configuration1, configuration2);
  }

  @Test
  public void testSubClusterInfoEqual() throws Exception {

    String scAmRMAddress = "5.6.7.8:5";
    String scClientRMAddress = "5.6.7.8:6";
    String scRmAdminAddress = "5.6.7.8:7";
    String scWebAppAddress = "127.0.0.1:8080";
    String capabilityJson = "-";

    SubClusterInfo sc1 =
        SubClusterInfo.newInstance(SubClusterId.newInstance("SC-1"),
        scAmRMAddress, scClientRMAddress, scRmAdminAddress, scWebAppAddress,
        SubClusterState.SC_RUNNING, Time.now(), capabilityJson);

    SubClusterInfo sc2 =
        SubClusterInfo.newInstance(SubClusterId.newInstance("SC-1"),
        scAmRMAddress, scClientRMAddress, scRmAdminAddress, scWebAppAddress,
        SubClusterState.SC_RUNNING, Time.now(), capabilityJson);

    assertEquals(sc1, sc2);
  }

  @Test
  public void testApplicationHomeSubClusterEqual() throws Exception {
    // Case1, We create 2 ApplicationHomeSubCluster,
    // all properties are consistent
    // We expect the result to be equal.
    ApplicationId appId1 = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC");
    ApplicationSubmissionContext context1 =
        ApplicationSubmissionContext.newInstance(appId1, "test", "default",
        Priority.newInstance(0), null, true, true,
        2, Resource.newInstance(10, 2), "test");
    long createTime = Time.now();
    ApplicationHomeSubCluster ahsc1 =
        ApplicationHomeSubCluster.newInstance(appId1, createTime, subClusterId1, context1);

    ApplicationId appId2 = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId2 = SubClusterId.newInstance("SC");
    ApplicationSubmissionContext context2 =
        ApplicationSubmissionContext.newInstance(appId1, "test", "default",
        Priority.newInstance(0), null, true, true,
        2, Resource.newInstance(10, 2), "test");
    ApplicationHomeSubCluster ahsc2 =
        ApplicationHomeSubCluster.newInstance(appId2, createTime, subClusterId2, context2);
    assertEquals(ahsc1, ahsc2);

    // Case2, We create 2 ApplicationHomeSubCluster, appId is different
    // We expect the results to be unequal
    ApplicationId appId3 = ApplicationId.newInstance(2, 1);
    ApplicationSubmissionContext context3 =
        ApplicationSubmissionContext.newInstance(appId3, "test", "default",
        Priority.newInstance(0), null, true, true,
        2, Resource.newInstance(10, 2), "test");
    ApplicationHomeSubCluster ahsc3 =
        ApplicationHomeSubCluster.newInstance(appId3, createTime, subClusterId2, context3);
    assertNotEquals(ahsc1, ahsc3);

    // Case3, We create 2 ApplicationHomeSubCluster, createTime is different
    // We expect the results to be unequal
    long createTime2 = Time.now() + 1000;
    ApplicationHomeSubCluster ahsc4 =
        ApplicationHomeSubCluster.newInstance(appId2, createTime2, subClusterId1, context2);
    assertNotEquals(ahsc1, ahsc4);

    // Case4, We create 2 ApplicationHomeSubCluster, submissionContext is different
    // We expect the results to be unequal
    ApplicationHomeSubCluster ahsc5 =
        ApplicationHomeSubCluster.newInstance(appId2, createTime2, subClusterId2, context3);
    assertNotEquals(ahsc1, ahsc5);
  }
}
