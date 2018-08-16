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
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreException;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
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
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Base class for FederationMembershipStateStore implementations.
 */
public abstract class FederationStateStoreBaseTest {

  private static final MonotonicClock CLOCK = new MonotonicClock();
  private FederationStateStore stateStore = createStateStore();

  protected abstract FederationStateStore createStateStore();

  private Configuration conf;

  @Before
  public void before() throws IOException, YarnException {
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
    try {
      stateStore.deregisterSubCluster(deregisterRequest);
      Assert.fail();
    } catch (FederationStateStoreException e) {
      Assert.assertTrue(e.getMessage().startsWith("SubCluster SC not found"));
    }
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

    try {
      stateStore.subClusterHeartbeat(heartbeatRequest);
      Assert.fail();
    } catch (FederationStateStoreException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("SubCluster SC does not exist; cannot heartbeat"));
    }
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
        ApplicationHomeSubCluster.newInstance(appId1, subClusterId1);

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

  private SubClusterInfo createSubClusterInfo(SubClusterId subClusterId) {

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

  private void addApplicationHomeSC(ApplicationId appId,
      SubClusterId subClusterId) throws YarnException {
    ApplicationHomeSubCluster ahsc =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId);
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

  private SubClusterInfo querySubClusterInfo(SubClusterId subClusterId)
      throws YarnException {
    GetSubClusterInfoRequest request =
        GetSubClusterInfoRequest.newInstance(subClusterId);
    return stateStore.getSubCluster(request).getSubClusterInfo();
  }

  private SubClusterId queryApplicationHomeSC(ApplicationId appId)
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

}
