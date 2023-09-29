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

package org.apache.hadoop.yarn.server.federation.store.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreException;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Base class for FederationMembershipStateStore implementations.
 */
public abstract class FederationStateStoreBaseTest {

  private static final MonotonicClock CLOCK = new MonotonicClock();
  private FederationStateStore stateStore;
  private static final int NUM_APPS_10 = 10;
  private static final int NUM_APPS_20 = 20;

  protected abstract FederationStateStore createStateStore();

  protected abstract void checkRouterMasterKey(DelegationKey delegationKey,
      RouterMasterKey routerMasterKey) throws YarnException, IOException, SQLException;

  protected abstract void checkRouterStoreToken(RMDelegationTokenIdentifier identifier,
      RouterStoreToken token) throws YarnException, IOException, SQLException;

  private Configuration conf;

  @Before
  public void before() throws IOException, YarnException {
    stateStore = createStateStore();
    stateStore.init(conf);
  }

  @After
  public void after() throws Exception {
    stateStore.close();
  }

  // Test FederationMembershipStateStore

  @Test
  public void testRegisterSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");

    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);

    long previousTimeStamp =
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();

    SubClusterRegisterResponse result = stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));

    long currentTimeStamp =
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();

    Assert.assertNotNull(result);
    Assert.assertEquals(subClusterInfo, querySubClusterInfo(subClusterId));

    // The saved heartbeat is between the old one and the current timestamp
    Assert.assertTrue(querySubClusterInfo(subClusterId)
        .getLastHeartBeat() <= currentTimeStamp);
    Assert.assertTrue(querySubClusterInfo(subClusterId)
        .getLastHeartBeat() >= previousTimeStamp);
  }

  @Test
  public void testDeregisterSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    registerSubCluster(createSubClusterInfo(subClusterId));

    SubClusterDeregisterRequest deregisterRequest = SubClusterDeregisterRequest
        .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED);

    stateStore.deregisterSubCluster(deregisterRequest);

    Assert.assertEquals(SubClusterState.SC_UNREGISTERED,
        querySubClusterInfo(subClusterId).getState());
  }

  @Test
  public void testDeregisterSubClusterUnknownSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");

    SubClusterDeregisterRequest deregisterRequest = SubClusterDeregisterRequest
        .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED);

    LambdaTestUtils.intercept(YarnException.class,
        "SubCluster SC not found", () -> stateStore.deregisterSubCluster(deregisterRequest));
  }

  @Test
  public void testGetSubClusterInfo() throws Exception {

    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);
    registerSubCluster(subClusterInfo);

    GetSubClusterInfoRequest request =
        GetSubClusterInfoRequest.newInstance(subClusterId);
    Assert.assertEquals(subClusterInfo,
        stateStore.getSubCluster(request).getSubClusterInfo());
  }

  @Test
  public void testGetSubClusterInfoUnknownSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    GetSubClusterInfoRequest request =
        GetSubClusterInfoRequest.newInstance(subClusterId);

    GetSubClusterInfoResponse response = stateStore.getSubCluster(request);
    Assert.assertNull(response);
  }

  @Test
  public void testGetAllSubClustersInfo() throws Exception {

    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    SubClusterInfo subClusterInfo1 = createSubClusterInfo(subClusterId1);

    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");
    SubClusterInfo subClusterInfo2 = createSubClusterInfo(subClusterId2);

    stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo1));
    stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo2));

    stateStore.subClusterHeartbeat(SubClusterHeartbeatRequest
        .newInstance(subClusterId1, SubClusterState.SC_RUNNING, "capability"));
    stateStore.subClusterHeartbeat(SubClusterHeartbeatRequest.newInstance(
        subClusterId2, SubClusterState.SC_UNHEALTHY, "capability"));

    List<SubClusterInfo> subClustersActive =
        stateStore.getSubClusters(GetSubClustersInfoRequest.newInstance(true))
            .getSubClusters();
    List<SubClusterInfo> subClustersAll =
        stateStore.getSubClusters(GetSubClustersInfoRequest.newInstance(false))
            .getSubClusters();

    // SC1 is the only active
    Assert.assertEquals(1, subClustersActive.size());
    SubClusterInfo sc1 = subClustersActive.get(0);
    Assert.assertEquals(subClusterId1, sc1.getSubClusterId());

    // SC1 and SC2 are the SubCluster present into the StateStore

    Assert.assertEquals(2, subClustersAll.size());
    Assert.assertTrue(subClustersAll.contains(sc1));
    subClustersAll.remove(sc1);
    SubClusterInfo sc2 = subClustersAll.get(0);
    Assert.assertEquals(subClusterId2, sc2.getSubClusterId());
  }

  @Test
  public void testSubClusterHeartbeat() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    registerSubCluster(createSubClusterInfo(subClusterId));

    long previousHeartBeat =
        querySubClusterInfo(subClusterId).getLastHeartBeat();

    SubClusterHeartbeatRequest heartbeatRequest = SubClusterHeartbeatRequest
        .newInstance(subClusterId, SubClusterState.SC_RUNNING, "capability");
    stateStore.subClusterHeartbeat(heartbeatRequest);

    long currentTimeStamp =
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();

    Assert.assertEquals(SubClusterState.SC_RUNNING,
        querySubClusterInfo(subClusterId).getState());

    // The saved heartbeat is between the old one and the current timestamp
    Assert.assertTrue(querySubClusterInfo(subClusterId)
        .getLastHeartBeat() <= currentTimeStamp);
    Assert.assertTrue(querySubClusterInfo(subClusterId)
        .getLastHeartBeat() >= previousHeartBeat);
  }

  @Test
  public void testSubClusterHeartbeatUnknownSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterHeartbeatRequest heartbeatRequest = SubClusterHeartbeatRequest
        .newInstance(subClusterId, SubClusterState.SC_RUNNING, "capability");

    LambdaTestUtils.intercept(YarnException.class,
        "SubCluster SC does not exist; cannot heartbeat",
        () -> stateStore.subClusterHeartbeat(heartbeatRequest));
  }

  // Test FederationApplicationHomeSubClusterStore

  @Test
  public void testAddApplicationHomeSubCluster() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    ApplicationHomeSubCluster ahsc =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId);

    AddApplicationHomeSubClusterRequest request =
        AddApplicationHomeSubClusterRequest.newInstance(ahsc);
    AddApplicationHomeSubClusterResponse response =
        stateStore.addApplicationHomeSubCluster(request);

    Assert.assertEquals(subClusterId, response.getHomeSubCluster());
    Assert.assertEquals(subClusterId, queryApplicationHomeSC(appId));

  }

  @Test
  public void testAddApplicationHomeSubClusterAppAlreadyExists()
      throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    addApplicationHomeSC(appId, subClusterId1);

    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");
    ApplicationHomeSubCluster ahsc2 =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId2);

    AddApplicationHomeSubClusterResponse response =
        stateStore.addApplicationHomeSubCluster(
            AddApplicationHomeSubClusterRequest.newInstance(ahsc2));

    Assert.assertEquals(subClusterId1, response.getHomeSubCluster());
    Assert.assertEquals(subClusterId1, queryApplicationHomeSC(appId));

  }

  @Test
  public void testAddApplicationHomeSubClusterAppAlreadyExistsInTheSameSC()
      throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    addApplicationHomeSC(appId, subClusterId1);

    ApplicationHomeSubCluster ahsc2 =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId1);

    AddApplicationHomeSubClusterResponse response =
        stateStore.addApplicationHomeSubCluster(
            AddApplicationHomeSubClusterRequest.newInstance(ahsc2));

    Assert.assertEquals(subClusterId1, response.getHomeSubCluster());
    Assert.assertEquals(subClusterId1, queryApplicationHomeSC(appId));

  }

  @Test
  public void testDeleteApplicationHomeSubCluster() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    addApplicationHomeSC(appId, subClusterId);

    DeleteApplicationHomeSubClusterRequest delRequest =
        DeleteApplicationHomeSubClusterRequest.newInstance(appId);

    DeleteApplicationHomeSubClusterResponse response =
        stateStore.deleteApplicationHomeSubCluster(delRequest);

    Assert.assertNotNull(response);
    try {
      queryApplicationHomeSC(appId);
      Assert.fail();
    } catch (FederationStateStoreException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Application " + appId + " does not exist"));
    }

  }

  @Test
  public void testDeleteApplicationHomeSubClusterUnknownApp() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    DeleteApplicationHomeSubClusterRequest delRequest =
        DeleteApplicationHomeSubClusterRequest.newInstance(appId);

    try {
      stateStore.deleteApplicationHomeSubCluster(delRequest);
      Assert.fail();
    } catch (FederationStateStoreException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Application " + appId.toString() + " does not exist"));
    }
  }

  @Test
  public void testGetApplicationHomeSubCluster() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    addApplicationHomeSC(appId, subClusterId);

    GetApplicationHomeSubClusterRequest getRequest =
        GetApplicationHomeSubClusterRequest.newInstance(appId);

    GetApplicationHomeSubClusterResponse result =
        stateStore.getApplicationHomeSubCluster(getRequest);

    Assert.assertEquals(appId,
        result.getApplicationHomeSubCluster().getApplicationId());
    Assert.assertEquals(subClusterId,
        result.getApplicationHomeSubCluster().getHomeSubCluster());
  }

  @Test
  public void testGetApplicationHomeSubClusterUnknownApp() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    GetApplicationHomeSubClusterRequest request =
        GetApplicationHomeSubClusterRequest.newInstance(appId);

    try {
      stateStore.getApplicationHomeSubCluster(request);
      Assert.fail();
    } catch (FederationStateStoreException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Application " + appId.toString() + " does not exist"));
    }
  }

  @Test
  public void testGetApplicationsHomeSubCluster() throws Exception {
    ApplicationId appId1 = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    ApplicationHomeSubCluster ahsc1 =
        ApplicationHomeSubCluster.newInstance(appId1,  subClusterId1);

    ApplicationId appId2 = ApplicationId.newInstance(1, 2);
    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");
    ApplicationHomeSubCluster ahsc2 =
        ApplicationHomeSubCluster.newInstance(appId2, subClusterId2);

    addApplicationHomeSC(appId1, subClusterId1);
    addApplicationHomeSC(appId2, subClusterId2);

    GetApplicationsHomeSubClusterRequest getRequest =
        GetApplicationsHomeSubClusterRequest.newInstance();

    GetApplicationsHomeSubClusterResponse result =
        stateStore.getApplicationsHomeSubCluster(getRequest);

    Assert.assertEquals(2, result.getAppsHomeSubClusters().size());
    Assert.assertTrue(result.getAppsHomeSubClusters().contains(ahsc1));
    Assert.assertTrue(result.getAppsHomeSubClusters().contains(ahsc2));
  }

  @Test
  public void testGetApplicationsHomeSubClusterEmpty() throws Exception {
    LambdaTestUtils.intercept(YarnException.class,
        "Missing getApplicationsHomeSubCluster request",
        () -> stateStore.getApplicationsHomeSubCluster(null));
  }

  @Test
  public void testGetApplicationsHomeSubClusterFilter() throws Exception {
    // Add ApplicationHomeSC - SC1
    long now = Time.now();

    Set<ApplicationHomeSubCluster> appHomeSubClusters = new HashSet<>();

    for (int i = 0; i < NUM_APPS_10; i++) {
      ApplicationId appId = ApplicationId.newInstance(now, i);
      SubClusterId subClusterId = SubClusterId.newInstance("SC1");
      addApplicationHomeSC(appId, subClusterId);
      ApplicationHomeSubCluster ahsc =
          ApplicationHomeSubCluster.newInstance(appId, subClusterId);
      appHomeSubClusters.add(ahsc);
    }

    // Add ApplicationHomeSC - SC2
    for (int i = 10; i < NUM_APPS_20; i++) {
      ApplicationId appId = ApplicationId.newInstance(now, i);
      SubClusterId subClusterId = SubClusterId.newInstance("SC2");
      addApplicationHomeSC(appId, subClusterId);
    }

    GetApplicationsHomeSubClusterRequest getRequest =
        GetApplicationsHomeSubClusterRequest.newInstance();
    getRequest.setSubClusterId(SubClusterId.newInstance("SC1"));

    GetApplicationsHomeSubClusterResponse result =
        stateStore.getApplicationsHomeSubCluster(getRequest);
    Assert.assertNotNull(result);

    List<ApplicationHomeSubCluster> items = result.getAppsHomeSubClusters();
    Assert.assertNotNull(items);
    Assert.assertEquals(10, items.size());

    for (ApplicationHomeSubCluster item : items) {
      appHomeSubClusters.contains(item);
      Assert.assertTrue(appHomeSubClusters.contains(item));
    }
  }

  @Test
  public void testGetApplicationsHomeSubClusterLimit() throws Exception {
    // Add ApplicationHomeSC - SC1
    long now = Time.now();

    for (int i = 0; i < 50; i++) {
      ApplicationId appId = ApplicationId.newInstance(now, i);
      SubClusterId subClusterId = SubClusterId.newInstance("SC1");
      addApplicationHomeSC(appId, subClusterId);
    }

    GetApplicationsHomeSubClusterRequest getRequest =
        GetApplicationsHomeSubClusterRequest.newInstance();
    getRequest.setSubClusterId(SubClusterId.newInstance("SC1"));
    GetApplicationsHomeSubClusterResponse result =
        stateStore.getApplicationsHomeSubCluster(getRequest);
    Assert.assertNotNull(result);

    // Write 50 records, but get 10 records because the maximum number is limited to 10
    List<ApplicationHomeSubCluster> items = result.getAppsHomeSubClusters();
    Assert.assertNotNull(items);
    Assert.assertEquals(10, items.size());

    GetApplicationsHomeSubClusterRequest getRequest1 =
        GetApplicationsHomeSubClusterRequest.newInstance();
    getRequest1.setSubClusterId(SubClusterId.newInstance("SC2"));
    GetApplicationsHomeSubClusterResponse result1 =
        stateStore.getApplicationsHomeSubCluster(getRequest1);
    Assert.assertNotNull(result1);

    // SC2 data does not exist, so the number of returned records is 0
    List<ApplicationHomeSubCluster> items1 = result1.getAppsHomeSubClusters();
    Assert.assertNotNull(items1);
    Assert.assertEquals(0, items1.size());
  }

  @Test
  public void testUpdateApplicationHomeSubCluster() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    addApplicationHomeSC(appId, subClusterId1);

    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");
    ApplicationHomeSubCluster ahscUpdate =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId2);

    UpdateApplicationHomeSubClusterRequest updateRequest =
        UpdateApplicationHomeSubClusterRequest.newInstance(ahscUpdate);

    UpdateApplicationHomeSubClusterResponse response =
        stateStore.updateApplicationHomeSubCluster(updateRequest);

    Assert.assertNotNull(response);
    Assert.assertEquals(subClusterId2, queryApplicationHomeSC(appId));
  }

  @Test
  public void testUpdateApplicationHomeSubClusterUnknownApp() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    ApplicationHomeSubCluster ahsc =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId1);

    UpdateApplicationHomeSubClusterRequest updateRequest =
        UpdateApplicationHomeSubClusterRequest.newInstance(ahsc);

    try {
      stateStore.updateApplicationHomeSubCluster((updateRequest));
      Assert.fail();
    } catch (FederationStateStoreException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Application " + appId.toString() + " does not exist"));
    }
  }

  // Test FederationPolicyStore

  @Test
  public void testSetPolicyConfiguration() throws Exception {
    SetSubClusterPolicyConfigurationRequest request =
        SetSubClusterPolicyConfigurationRequest
            .newInstance(createSCPolicyConf("Queue", "PolicyType"));

    SetSubClusterPolicyConfigurationResponse result =
        stateStore.setPolicyConfiguration(request);

    Assert.assertNotNull(result);
    Assert.assertEquals(createSCPolicyConf("Queue", "PolicyType"),
        queryPolicy("Queue"));

  }

  @Test
  public void testSetPolicyConfigurationUpdateExisting() throws Exception {
    setPolicyConf("Queue", "PolicyType1");

    SetSubClusterPolicyConfigurationRequest request2 =
        SetSubClusterPolicyConfigurationRequest
            .newInstance(createSCPolicyConf("Queue", "PolicyType2"));
    SetSubClusterPolicyConfigurationResponse result =
        stateStore.setPolicyConfiguration(request2);

    Assert.assertNotNull(result);
    Assert.assertEquals(createSCPolicyConf("Queue", "PolicyType2"),
        queryPolicy("Queue"));
  }

  @Test
  public void testGetPolicyConfiguration() throws Exception {
    setPolicyConf("Queue", "PolicyType");

    GetSubClusterPolicyConfigurationRequest getRequest =
        GetSubClusterPolicyConfigurationRequest.newInstance("Queue");
    GetSubClusterPolicyConfigurationResponse result =
        stateStore.getPolicyConfiguration(getRequest);

    Assert.assertNotNull(result);
    Assert.assertEquals(createSCPolicyConf("Queue", "PolicyType"),
        result.getPolicyConfiguration());

  }

  @Test
  public void testGetPolicyConfigurationUnknownQueue() throws Exception {

    GetSubClusterPolicyConfigurationRequest request =
        GetSubClusterPolicyConfigurationRequest.newInstance("Queue");

    GetSubClusterPolicyConfigurationResponse response =
        stateStore.getPolicyConfiguration(request);
    Assert.assertNull(response);
  }

  @Test
  public void testGetPoliciesConfigurations() throws Exception {
    setPolicyConf("Queue1", "PolicyType1");
    setPolicyConf("Queue2", "PolicyType2");

    GetSubClusterPoliciesConfigurationsResponse response =
        stateStore.getPoliciesConfigurations(
            GetSubClusterPoliciesConfigurationsRequest.newInstance());

    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getPoliciesConfigs());

    Assert.assertEquals(2, response.getPoliciesConfigs().size());

    Assert.assertTrue(response.getPoliciesConfigs()
        .contains(createSCPolicyConf("Queue1", "PolicyType1")));
    Assert.assertTrue(response.getPoliciesConfigs()
        .contains(createSCPolicyConf("Queue2", "PolicyType2")));
  }

  // Convenience methods

  SubClusterInfo createSubClusterInfo(SubClusterId subClusterId) {

    String amRMAddress = "1.2.3.4:1";
    String clientRMAddress = "1.2.3.4:2";
    String rmAdminAddress = "1.2.3.4:3";
    String webAppAddress = "1.2.3.4:4";

    return SubClusterInfo.newInstance(subClusterId, amRMAddress,
        clientRMAddress, rmAdminAddress, webAppAddress, SubClusterState.SC_NEW,
        CLOCK.getTime(), "capability");
  }

  private SubClusterPolicyConfiguration createSCPolicyConf(String queueName,
      String policyType) {
    ByteBuffer bb = ByteBuffer.allocate(100);
    bb.put((byte) 0x02);
    return SubClusterPolicyConfiguration.newInstance(queueName, policyType, bb);
  }

  void addApplicationHomeSC(ApplicationId appId,
      SubClusterId subClusterId) throws YarnException {
    ApplicationHomeSubCluster ahsc =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId);
    AddApplicationHomeSubClusterRequest request =
        AddApplicationHomeSubClusterRequest.newInstance(ahsc);
    stateStore.addApplicationHomeSubCluster(request);
  }

  void addApplicationHomeSC(ApplicationId appId, SubClusterId subClusterId,
      ApplicationSubmissionContext submissionContext) throws YarnException {
    long createTime = Time.now();
    ApplicationHomeSubCluster ahsc = ApplicationHomeSubCluster.newInstance(
        appId, createTime, subClusterId, submissionContext);
    AddApplicationHomeSubClusterRequest request =
         AddApplicationHomeSubClusterRequest.newInstance(ahsc);
    stateStore.addApplicationHomeSubCluster(request);
  }

  private void setPolicyConf(String queue, String policyType)
      throws YarnException {
    SetSubClusterPolicyConfigurationRequest request =
        SetSubClusterPolicyConfigurationRequest
            .newInstance(createSCPolicyConf(queue, policyType));
    stateStore.setPolicyConfiguration(request);
  }

  private void registerSubCluster(SubClusterInfo subClusterInfo)
      throws YarnException {
    stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));
  }

  SubClusterInfo querySubClusterInfo(SubClusterId subClusterId)
      throws YarnException {
    GetSubClusterInfoRequest request =
        GetSubClusterInfoRequest.newInstance(subClusterId);
    return stateStore.getSubCluster(request).getSubClusterInfo();
  }

  SubClusterId queryApplicationHomeSC(ApplicationId appId)
      throws YarnException {
    GetApplicationHomeSubClusterRequest request =
        GetApplicationHomeSubClusterRequest.newInstance(appId);

    GetApplicationHomeSubClusterResponse response =
        stateStore.getApplicationHomeSubCluster(request);

    return response.getApplicationHomeSubCluster().getHomeSubCluster();
  }

  private SubClusterPolicyConfiguration queryPolicy(String queue)
      throws YarnException {
    GetSubClusterPolicyConfigurationRequest request =
        GetSubClusterPolicyConfigurationRequest.newInstance(queue);

    GetSubClusterPolicyConfigurationResponse result =
        stateStore.getPolicyConfiguration(request);
    return result.getPolicyConfiguration();
  }

  protected void setConf(Configuration conf) {
    this.conf = conf;
  }

  protected Configuration getConf() {
    return conf;
  }

  protected FederationStateStore getStateStore() {
    return stateStore;
  }

  SubClusterId queryReservationHomeSC(ReservationId reservationId)
      throws YarnException {

    GetReservationHomeSubClusterRequest request =
        GetReservationHomeSubClusterRequest.newInstance(reservationId);

    GetReservationHomeSubClusterResponse response =
        stateStore.getReservationHomeSubCluster(request);

    return response.getReservationHomeSubCluster().getHomeSubCluster();
  }

  @Test
  public void testAddReservationHomeSubCluster() throws Exception {

    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    SubClusterId subClusterId = SubClusterId.newInstance("SC");

    ReservationHomeSubCluster reservationHomeSubCluster =
        ReservationHomeSubCluster.newInstance(reservationId, subClusterId);

    AddReservationHomeSubClusterRequest request =
        AddReservationHomeSubClusterRequest.newInstance(reservationHomeSubCluster);
    AddReservationHomeSubClusterResponse response =
        stateStore.addReservationHomeSubCluster(request);

    Assert.assertEquals(subClusterId, response.getHomeSubCluster());
    Assert.assertEquals(subClusterId, queryReservationHomeSC(reservationId));
  }

  private void addReservationHomeSC(ReservationId reservationId, SubClusterId subClusterId)
      throws YarnException {

    ReservationHomeSubCluster reservationHomeSubCluster =
        ReservationHomeSubCluster.newInstance(reservationId, subClusterId);
    AddReservationHomeSubClusterRequest request =
        AddReservationHomeSubClusterRequest.newInstance(reservationHomeSubCluster);
    stateStore.addReservationHomeSubCluster(request);
  }

  @Test
  public void testAddReservationHomeSubClusterReservationAlreadyExists() throws Exception {

    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    addReservationHomeSC(reservationId, subClusterId1);

    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");
    ReservationHomeSubCluster reservationHomeSubCluster2 =
        ReservationHomeSubCluster.newInstance(reservationId, subClusterId2);
    AddReservationHomeSubClusterRequest request2 =
        AddReservationHomeSubClusterRequest.newInstance(reservationHomeSubCluster2);
    AddReservationHomeSubClusterResponse response =
        stateStore.addReservationHomeSubCluster(request2);

    Assert.assertNotNull(response);
    Assert.assertEquals(subClusterId1, response.getHomeSubCluster());
    Assert.assertEquals(subClusterId1, queryReservationHomeSC(reservationId));
  }

  @Test
  public void testAddReservationHomeSubClusterAppAlreadyExistsInTheSameSC()
      throws Exception {

    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    addReservationHomeSC(reservationId, subClusterId1);

    ReservationHomeSubCluster reservationHomeSubCluster2 =
        ReservationHomeSubCluster.newInstance(reservationId, subClusterId1);
    AddReservationHomeSubClusterRequest request2 =
        AddReservationHomeSubClusterRequest.newInstance(reservationHomeSubCluster2);
    AddReservationHomeSubClusterResponse response =
        stateStore.addReservationHomeSubCluster(request2);

    Assert.assertNotNull(response);
    Assert.assertEquals(subClusterId1, response.getHomeSubCluster());
    Assert.assertEquals(subClusterId1, queryReservationHomeSC(reservationId));
  }

  @Test
  public void testDeleteReservationHomeSubCluster() throws Exception {

    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC");
    addReservationHomeSC(reservationId, subClusterId1);

    DeleteReservationHomeSubClusterRequest delReservationRequest =
        DeleteReservationHomeSubClusterRequest.newInstance(reservationId);
    DeleteReservationHomeSubClusterResponse delReservationResponse =
        stateStore.deleteReservationHomeSubCluster(delReservationRequest);

    Assert.assertNotNull(delReservationResponse);

    LambdaTestUtils.intercept(YarnException.class,
        "Reservation " + reservationId + " does not exist",
        () -> queryReservationHomeSC(reservationId));
  }

  @Test
  public void testDeleteReservationHomeSubClusterUnknownApp() throws Exception {

    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);

    DeleteReservationHomeSubClusterRequest delReservationRequest =
        DeleteReservationHomeSubClusterRequest.newInstance(reservationId);

    LambdaTestUtils.intercept(YarnException.class,
        "Reservation " + reservationId + " does not exist",
        () -> stateStore.deleteReservationHomeSubCluster(delReservationRequest));
  }

  @Test
  public void testUpdateReservationHomeSubCluster() throws Exception {

    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC");
    addReservationHomeSC(reservationId, subClusterId1);

    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");
    ReservationHomeSubCluster reservationHomeSubCluster =
        ReservationHomeSubCluster.newInstance(reservationId, subClusterId2);

    UpdateReservationHomeSubClusterRequest updateReservationRequest =
        UpdateReservationHomeSubClusterRequest.newInstance(reservationHomeSubCluster);

    UpdateReservationHomeSubClusterResponse updateReservationResponse =
        stateStore.updateReservationHomeSubCluster(updateReservationRequest);

    Assert.assertNotNull(updateReservationResponse);
    Assert.assertEquals(subClusterId2, queryReservationHomeSC(reservationId));
  }

  @Test
  public void testUpdateReservationHomeSubClusterUnknownApp() throws Exception {

    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");

    ReservationHomeSubCluster reservationHomeSubCluster =
        ReservationHomeSubCluster.newInstance(reservationId, subClusterId1);

    UpdateReservationHomeSubClusterRequest updateReservationRequest =
        UpdateReservationHomeSubClusterRequest.newInstance(reservationHomeSubCluster);

    LambdaTestUtils.intercept(YarnException.class,
        "Reservation " + reservationId + " does not exist",
        () -> stateStore.updateReservationHomeSubCluster(updateReservationRequest));
  }

  @Test
  public void testStoreNewMasterKey() throws Exception {
    // store delegation key;
    DelegationKey key = new DelegationKey(1234, 4321, "keyBytes".getBytes());
    Set<DelegationKey> keySet = new HashSet<>();
    keySet.add(key);

    RouterMasterKey routerMasterKey = RouterMasterKey.newInstance(key.getKeyId(),
        ByteBuffer.wrap(key.getEncodedKey()), key.getExpiryDate());
    RouterMasterKeyRequest routerMasterKeyRequest =
        RouterMasterKeyRequest.newInstance(routerMasterKey);
    RouterMasterKeyResponse response = stateStore.storeNewMasterKey(routerMasterKeyRequest);

    Assert.assertNotNull(response);
    RouterMasterKey routerMasterKeyResp = response.getRouterMasterKey();
    Assert.assertNotNull(routerMasterKeyResp);
    Assert.assertEquals(routerMasterKey.getKeyId(), routerMasterKeyResp.getKeyId());
    Assert.assertEquals(routerMasterKey.getKeyBytes(), routerMasterKeyResp.getKeyBytes());
    Assert.assertEquals(routerMasterKey.getExpiryDate(), routerMasterKeyResp.getExpiryDate());

    checkRouterMasterKey(key, routerMasterKey);
  }

  @Test
  public void testGetMasterKeyByDelegationKey() throws YarnException, IOException {
    // store delegation key;
    DelegationKey key = new DelegationKey(5678, 8765, "keyBytes".getBytes());
    Set<DelegationKey> keySet = new HashSet<>();
    keySet.add(key);

    RouterMasterKey routerMasterKey = RouterMasterKey.newInstance(key.getKeyId(),
        ByteBuffer.wrap(key.getEncodedKey()), key.getExpiryDate());
    RouterMasterKeyRequest routerMasterKeyRequest =
        RouterMasterKeyRequest.newInstance(routerMasterKey);
    RouterMasterKeyResponse response = stateStore.storeNewMasterKey(routerMasterKeyRequest);
    Assert.assertNotNull(response);

    RouterMasterKeyResponse routerMasterKeyResponse =
        stateStore.getMasterKeyByDelegationKey(routerMasterKeyRequest);

    Assert.assertNotNull(routerMasterKeyResponse);

    RouterMasterKey routerMasterKeyResp = routerMasterKeyResponse.getRouterMasterKey();
    Assert.assertNotNull(routerMasterKeyResp);
    Assert.assertEquals(routerMasterKey.getKeyId(), routerMasterKeyResp.getKeyId());
    Assert.assertEquals(routerMasterKey.getKeyBytes(), routerMasterKeyResp.getKeyBytes());
    Assert.assertEquals(routerMasterKey.getExpiryDate(), routerMasterKeyResp.getExpiryDate());
  }

  @Test
  public void testRemoveStoredMasterKey() throws YarnException, IOException {
    // store delegation key;
    DelegationKey key = new DelegationKey(1234, 4321, "keyBytes".getBytes());
    Set<DelegationKey> keySet = new HashSet<>();
    keySet.add(key);

    RouterMasterKey routerMasterKey = RouterMasterKey.newInstance(key.getKeyId(),
        ByteBuffer.wrap(key.getEncodedKey()), key.getExpiryDate());
    RouterMasterKeyRequest routerMasterKeyRequest =
        RouterMasterKeyRequest.newInstance(routerMasterKey);
    RouterMasterKeyResponse response = stateStore.storeNewMasterKey(routerMasterKeyRequest);
    Assert.assertNotNull(response);

    RouterMasterKeyResponse masterKeyResponse =
        stateStore.removeStoredMasterKey(routerMasterKeyRequest);
    Assert.assertNotNull(masterKeyResponse);

    RouterMasterKey routerMasterKeyResp = masterKeyResponse.getRouterMasterKey();
    Assert.assertEquals(routerMasterKey.getKeyId(), routerMasterKeyResp.getKeyId());
    Assert.assertEquals(routerMasterKey.getKeyBytes(), routerMasterKeyResp.getKeyBytes());
    Assert.assertEquals(routerMasterKey.getExpiryDate(), routerMasterKeyResp.getExpiryDate());
  }

  @Test
  public void testStoreNewToken() throws IOException, YarnException, SQLException {
    // prepare parameters
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier(
        new Text("owner1"), new Text("renewer1"), new Text("realuser1"));
    int sequenceNumber = 1;
    identifier.setSequenceNumber(sequenceNumber);
    Long renewDate = Time.now();
    String tokenInfo = "tokenInfo";

    // store new rm-token
    RouterStoreToken storeToken = RouterStoreToken.newInstance(identifier, renewDate, tokenInfo);
    RouterRMTokenRequest request = RouterRMTokenRequest.newInstance(storeToken);
    RouterRMTokenResponse routerRMTokenResponse = stateStore.storeNewToken(request);

    // Verify the returned result to ensure that the returned Response is not empty
    // and the returned result is consistent with the input parameters.
    Assert.assertNotNull(routerRMTokenResponse);
    RouterStoreToken storeTokenResp = routerRMTokenResponse.getRouterStoreToken();
    Assert.assertNotNull(storeTokenResp);
    Assert.assertEquals(storeToken.getRenewDate(), storeTokenResp.getRenewDate());
    Assert.assertEquals(storeToken.getTokenIdentifier(), storeTokenResp.getTokenIdentifier());
    Assert.assertEquals(storeToken.getTokenInfo(), storeTokenResp.getTokenInfo());

    checkRouterStoreToken(identifier, storeTokenResp);
  }

  @Test
  public void testUpdateStoredToken() throws IOException, YarnException, SQLException {
    // prepare saveToken parameters
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier(
        new Text("owner2"), new Text("renewer2"), new Text("realuser2"));
    int sequenceNumber = 2;
    String tokenInfo = "tokenInfo";
    identifier.setSequenceNumber(sequenceNumber);
    Long renewDate = Time.now();

    // store new rm-token
    RouterStoreToken storeToken = RouterStoreToken.newInstance(identifier, renewDate, tokenInfo);
    RouterRMTokenRequest request = RouterRMTokenRequest.newInstance(storeToken);
    RouterRMTokenResponse routerRMTokenResponse = stateStore.storeNewToken(request);
    Assert.assertNotNull(routerRMTokenResponse);

    // prepare updateToken parameters
    Long renewDate2 = Time.now();
    String tokenInfo2 = "tokenInfo2";

    // update rm-token
    RouterStoreToken updateToken = RouterStoreToken.newInstance(identifier, renewDate2, tokenInfo2);
    RouterRMTokenRequest updateTokenRequest = RouterRMTokenRequest.newInstance(updateToken);
    RouterRMTokenResponse updateTokenResponse = stateStore.updateStoredToken(updateTokenRequest);

    Assert.assertNotNull(updateTokenResponse);
    RouterStoreToken updateTokenResp = updateTokenResponse.getRouterStoreToken();
    Assert.assertNotNull(updateTokenResp);
    Assert.assertEquals(updateToken.getRenewDate(), updateTokenResp.getRenewDate());
    Assert.assertEquals(updateToken.getTokenIdentifier(), updateTokenResp.getTokenIdentifier());
    Assert.assertEquals(updateToken.getTokenInfo(), updateTokenResp.getTokenInfo());

    checkRouterStoreToken(identifier, updateTokenResp);
  }

  @Test
  public void testRemoveStoredToken() throws IOException, YarnException {
    // prepare saveToken parameters
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier(
        new Text("owner3"), new Text("renewer3"), new Text("realuser3"));
    int sequenceNumber = 3;
    identifier.setSequenceNumber(sequenceNumber);
    Long renewDate = Time.now();
    String tokenInfo = "tokenInfo";

    // store new rm-token
    RouterStoreToken storeToken = RouterStoreToken.newInstance(identifier, renewDate, tokenInfo);
    RouterRMTokenRequest request = RouterRMTokenRequest.newInstance(storeToken);
    RouterRMTokenResponse routerRMTokenResponse = stateStore.storeNewToken(request);
    Assert.assertNotNull(routerRMTokenResponse);

    // remove rm-token
    RouterRMTokenResponse removeTokenResponse = stateStore.removeStoredToken(request);
    Assert.assertNotNull(removeTokenResponse);
    RouterStoreToken removeTokenResp = removeTokenResponse.getRouterStoreToken();
    Assert.assertNotNull(removeTokenResp);
    Assert.assertEquals(removeTokenResp.getRenewDate(), storeToken.getRenewDate());
    Assert.assertEquals(removeTokenResp.getTokenIdentifier(), storeToken.getTokenIdentifier());
  }

  @Test
  public void testGetTokenByRouterStoreToken() throws IOException, YarnException, SQLException {
    // prepare saveToken parameters
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier(
        new Text("owner4"), new Text("renewer4"), new Text("realuser4"));
    int sequenceNumber = 4;
    identifier.setSequenceNumber(sequenceNumber);
    Long renewDate = Time.now();
    String tokenInfo = "tokenInfo";

    // store new rm-token
    RouterStoreToken storeToken = RouterStoreToken.newInstance(identifier, renewDate, tokenInfo);
    RouterRMTokenRequest request = RouterRMTokenRequest.newInstance(storeToken);
    RouterRMTokenResponse routerRMTokenResponse = stateStore.storeNewToken(request);
    Assert.assertNotNull(routerRMTokenResponse);

    // getTokenByRouterStoreToken
    RouterRMTokenResponse getRouterRMTokenResp = stateStore.getTokenByRouterStoreToken(request);
    Assert.assertNotNull(getRouterRMTokenResp);
    RouterStoreToken getStoreTokenResp = getRouterRMTokenResp.getRouterStoreToken();
    Assert.assertNotNull(getStoreTokenResp);
    Assert.assertEquals(getStoreTokenResp.getRenewDate(), storeToken.getRenewDate());
    Assert.assertEquals(storeToken.getTokenInfo(), getStoreTokenResp.getTokenInfo());

    checkRouterStoreToken(identifier, getStoreTokenResp);
  }

  @Test
  public void testGetCurrentVersion() {
    Version version = stateStore.getCurrentVersion();
    assertEquals(1, version.getMajorVersion());
    assertEquals(1, version.getMinorVersion());
  }

  @Test
  public void testStoreVersion() throws Exception {
    stateStore.storeVersion();
    Version version = stateStore.getCurrentVersion();
    assertEquals(1, version.getMajorVersion());
    assertEquals(1, version.getMinorVersion());
  }

  @Test
  public void testLoadVersion() throws Exception {
    stateStore.storeVersion();
    Version version = stateStore.loadVersion();
    assertEquals(1, version.getMajorVersion());
    assertEquals(1, version.getMinorVersion());
  }

  @Test
  public void testCheckVersion() throws Exception {
    stateStore.checkVersion();
  }

  @Test
  public void testGetApplicationHomeSubClusterWithContext() throws Exception {
    FederationStateStore federationStateStore = this.getStateStore();

    ApplicationId appId = ApplicationId.newInstance(1, 3);
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    ApplicationSubmissionContext context =
        ApplicationSubmissionContext.newInstance(appId, "test", "default",
        Priority.newInstance(0), null, true, true,
        2, Resource.newInstance(10, 2), "test");
    addApplicationHomeSC(appId, subClusterId, context);

    GetApplicationHomeSubClusterRequest getRequest =
         GetApplicationHomeSubClusterRequest.newInstance(appId, true);
    GetApplicationHomeSubClusterResponse result =
         federationStateStore.getApplicationHomeSubCluster(getRequest);

    ApplicationHomeSubCluster applicationHomeSubCluster = result.getApplicationHomeSubCluster();

    assertEquals(appId, applicationHomeSubCluster.getApplicationId());
    assertEquals(subClusterId, applicationHomeSubCluster.getHomeSubCluster());
    assertEquals(context, applicationHomeSubCluster.getApplicationSubmissionContext());
  }
}
