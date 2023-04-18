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

package org.apache.hadoop.yarn.server.resourcemanager.federation;

import java.io.IOException;
import java.io.StringReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreException;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONUnmarshaller;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for FederationStateStoreService.
 */
public class TestFederationRMStateStoreService {

  private final HAServiceProtocol.StateChangeRequestInfo requestInfo =
      new HAServiceProtocol.StateChangeRequestInfo(
          HAServiceProtocol.RequestSource.REQUEST_BY_USER);
  private final SubClusterId subClusterId = SubClusterId.newInstance("SC-1");
  private final GetSubClusterInfoRequest request =
      GetSubClusterInfoRequest.newInstance(subClusterId);

  private Configuration conf;
  private FederationStateStore stateStore;
  private long lastHearbeatTS = 0;
  private JSONJAXBContext jc;
  private JSONUnmarshaller unmarshaller;
  private MockRM mockRM;

  @Before
  public void setUp() throws IOException, YarnException, JAXBException {
    conf = new YarnConfiguration();
    jc = new JSONJAXBContext(
        JSONConfiguration.mapped().rootUnwrapping(false).build(),
        ClusterMetricsInfo.class);
    unmarshaller = jc.createJSONUnmarshaller();

    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.setInt(YarnConfiguration.FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY, 10);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, subClusterId.getId());

    // set up MockRM
    mockRM = new MockRM(conf);
    mockRM.init(conf);
    mockRM.start();
  }

  @After
  public void tearDown() throws Exception {
    unmarshaller = null;
    jc = null;
    mockRM.stop();
    mockRM = null;
  }

  @Test
  public void testFederationStateStoreService() throws Exception {
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, subClusterId.getId());
    final MockRM rm = new MockRM(conf);

    // Initially there should be no entry for the sub-cluster
    rm.init(conf);
    stateStore = rm.getFederationStateStoreService().getStateStoreClient();
    GetSubClusterInfoResponse response = stateStore.getSubCluster(request);
    Assert.assertNull(response);

    // Validate if sub-cluster is registered
    rm.start();
    String capability = checkSubClusterInfo(SubClusterState.SC_NEW);
    Assert.assertTrue(capability.isEmpty());

    // Heartbeat to see if sub-cluster transitions to running
    FederationStateStoreHeartbeat storeHeartbeat =
        rm.getFederationStateStoreService().getStateStoreHeartbeatThread();
    storeHeartbeat.run();
    capability = checkSubClusterInfo(SubClusterState.SC_RUNNING);
    checkClusterMetricsInfo(capability, 0);

    // heartbeat again after adding a node.
    rm.registerNode("127.0.0.1:1234", 4 * 1024);
    storeHeartbeat.run();
    capability = checkSubClusterInfo(SubClusterState.SC_RUNNING);
    checkClusterMetricsInfo(capability, 1);

    // Validate sub-cluster deregistration
    rm.getFederationStateStoreService()
        .deregisterSubCluster(SubClusterDeregisterRequest
            .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED));
    checkSubClusterInfo(SubClusterState.SC_UNREGISTERED);

    // check after failover
    explicitFailover(rm);

    capability = checkSubClusterInfo(SubClusterState.SC_NEW);
    Assert.assertTrue(capability.isEmpty());

    // Heartbeat to see if sub-cluster transitions to running
    storeHeartbeat =
        rm.getFederationStateStoreService().getStateStoreHeartbeatThread();
    storeHeartbeat.run();
    capability = checkSubClusterInfo(SubClusterState.SC_RUNNING);
    checkClusterMetricsInfo(capability, 0);

    // heartbeat again after adding a node.
    rm.registerNode("127.0.0.1:1234", 4 * 1024);
    storeHeartbeat.run();
    capability = checkSubClusterInfo(SubClusterState.SC_RUNNING);
    checkClusterMetricsInfo(capability, 1);

    rm.stop();
  }

  private void explicitFailover(MockRM rm) throws IOException {
    rm.getAdminService().transitionToStandby(requestInfo);
    Assert.assertTrue(rm.getRMContext()
        .getHAServiceState() == HAServiceProtocol.HAServiceState.STANDBY);
    rm.getAdminService().transitionToActive(requestInfo);
    Assert.assertTrue(rm.getRMContext()
        .getHAServiceState() == HAServiceProtocol.HAServiceState.ACTIVE);
    lastHearbeatTS = 0;
    stateStore = rm.getFederationStateStoreService().getStateStoreClient();
  }

  private void checkClusterMetricsInfo(String capability, int numNodes)
      throws JAXBException {
    ClusterMetricsInfo clusterMetricsInfo = unmarshaller.unmarshalFromJSON(
        new StringReader(capability), ClusterMetricsInfo.class);
    Assert.assertEquals(numNodes, clusterMetricsInfo.getTotalNodes());
  }

  private String checkSubClusterInfo(SubClusterState state)
      throws YarnException, UnknownHostException {
    Assert.assertNotNull(stateStore.getSubCluster(request));
    SubClusterInfo response =
        stateStore.getSubCluster(request).getSubClusterInfo();
    Assert.assertEquals(state, response.getState());
    Assert.assertTrue(response.getLastHeartBeat() >= lastHearbeatTS);
    String expectedAddress =
        (response.getClientRMServiceAddress().split(":"))[0];
    Assert.assertEquals(expectedAddress,
        (response.getAMRMServiceAddress().split(":"))[0]);
    Assert.assertEquals(expectedAddress,
        (response.getRMAdminServiceAddress().split(":"))[0]);
    Assert.assertEquals(expectedAddress,
        (response.getRMWebServiceAddress().split(":"))[0]);
    lastHearbeatTS = response.getLastHeartBeat();
    return response.getCapability();
  }

  @Test
  public void testFederationStateStoreServiceInitialHeartbeatDelay() throws Exception {
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.setInt(YarnConfiguration.FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY, 10);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, subClusterId.getId());

    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(FederationStateStoreService.LOG);

    final MockRM rm = new MockRM(conf);

    // Initially there should be no entry for the sub-cluster
    rm.init(conf);
    stateStore = rm.getFederationStateStoreService().getStateStoreClient();
    GetSubClusterInfoResponse response = stateStore.getSubCluster(request);
    Assert.assertNull(response);

    // Validate if sub-cluster is registered
    rm.start();
    String capability = checkSubClusterInfo(SubClusterState.SC_NEW);
    Assert.assertTrue(capability.isEmpty());

    // Heartbeat to see if sub-cluster transitions to running
    FederationStateStoreHeartbeat storeHeartbeat =
        rm.getFederationStateStoreService().getStateStoreHeartbeatThread();
    storeHeartbeat.run();
    capability = checkSubClusterInfo(SubClusterState.SC_RUNNING);
    checkClusterMetricsInfo(capability, 0);

    Assert.assertTrue(logCapture.getOutput().contains(
        "Started federation membership heartbeat with interval: 300 and initial delay: 10"));
    rm.stop();
  }

  @Test
  public void testCleanUpApplication() throws Exception {

    // set yarn configuration
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.setInt(YarnConfiguration.FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY, 10);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, subClusterId.getId());

    // set up MockRM
    final MockRM rm = new MockRM(conf);
    rm.init(conf);
    stateStore = rm.getFederationStateStoreService().getStateStoreClient();
    rm.start();

    // init subCluster Heartbeat,
    // and check that the subCluster is in a running state
    FederationStateStoreService stateStoreService = rm.getFederationStateStoreService();
    FederationStateStoreHeartbeat storeHeartbeat = stateStoreService.getStateStoreHeartbeatThread();
    storeHeartbeat.run();
    checkSubClusterInfo(SubClusterState.SC_RUNNING);

    // generate an application and join the [SC-1] cluster
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    addApplication2StateStore(appId, stateStore);

    // make sure the app can be queried in the stateStore
    GetApplicationHomeSubClusterRequest appRequest =
         GetApplicationHomeSubClusterRequest.newInstance(appId);
    GetApplicationHomeSubClusterResponse response =
         stateStore.getApplicationHomeSubCluster(appRequest);
    Assert.assertNotNull(response);
    ApplicationHomeSubCluster appHomeSubCluster = response.getApplicationHomeSubCluster();
    Assert.assertNotNull(appHomeSubCluster);
    Assert.assertNotNull(appHomeSubCluster.getApplicationId());
    Assert.assertEquals(appId, appHomeSubCluster.getApplicationId());

    // clean up the app.
    boolean cleanUpResult =
        stateStoreService.cleanUpFinishApplicationsWithRetries(appId, true);
    Assert.assertTrue(cleanUpResult);

    // after clean, the app can no longer be queried from the stateStore.
    LambdaTestUtils.intercept(FederationStateStoreException.class,
        "Application " + appId + " does not exist",
        () -> stateStore.getApplicationHomeSubCluster(appRequest));

  }

  @Test
  public void testCleanUpApplicationWhenRMStart() throws Exception {

    // We design such a test case.
    // Step1. We add app01, app02, app03 to the stateStore,
    // But these apps are not in RM's RMContext, they are finished apps
    // Step2. We simulate RM startup, there is only app04 in RMContext.
    // Step3. We wait for 5 seconds, the automatic cleanup thread should clean up finished apps.

    // set yarn configuration.
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.setInt(YarnConfiguration.FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY, 10);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, subClusterId.getId());
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);

    // set up MockRM.
    MockRM rm = new MockRM(conf);
    rm.init(conf);
    stateStore = rm.getFederationStateStoreService().getStateStoreClient();

    // generate an [app01] and join the [SC-1] cluster.
    List<ApplicationId> appIds = new ArrayList<>();
    ApplicationId appId01 = ApplicationId.newInstance(Time.now(), 1);
    addApplication2StateStore(appId01, stateStore);
    appIds.add(appId01);

    // generate an [app02] and join the [SC-1] cluster.
    ApplicationId appId02 = ApplicationId.newInstance(Time.now(), 2);
    addApplication2StateStore(appId02, stateStore);
    appIds.add(appId02);

    // generate an [app03] and join the [SC-1] cluster.
    ApplicationId appId03 = ApplicationId.newInstance(Time.now(), 3);
    addApplication2StateStore(appId03, stateStore);
    appIds.add(appId03);

    // make sure the apps can be queried in the stateStore.
    GetApplicationsHomeSubClusterRequest allRequest =
        GetApplicationsHomeSubClusterRequest.newInstance(subClusterId);
    GetApplicationsHomeSubClusterResponse allResponse =
        stateStore.getApplicationsHomeSubCluster(allRequest);
    Assert.assertNotNull(allResponse);
    List<ApplicationHomeSubCluster> appHomeSCLists = allResponse.getAppsHomeSubClusters();
    Assert.assertNotNull(appHomeSCLists);
    Assert.assertEquals(3, appHomeSCLists.size());

    // app04 exists in both RM memory and stateStore.
    ApplicationId appId04 = ApplicationId.newInstance(Time.now(), 4);
    addApplication2StateStore(appId04, stateStore);
    addApplication2RMAppManager(rm, appId04);

    // start rm.
    rm.start();

    // wait 5s, wait for the thread to finish cleaning up.
    GenericTestUtils.waitFor(() -> {
      int appsSize = 0;
      try {
        List<ApplicationHomeSubCluster> subClusters =
            getApplicationsFromStateStore();
        Assert.assertNotNull(subClusters);
        appsSize = subClusters.size();
      } catch (YarnException e) {
        e.printStackTrace();
      }
      return (appsSize == 1);
    }, 100, 1000 * 5);

    // check the app to make sure the apps(app01,app02,app03) doesn't exist.
    for (ApplicationId appId : appIds) {
      GetApplicationHomeSubClusterRequest appRequest =
          GetApplicationHomeSubClusterRequest.newInstance(appId);
      LambdaTestUtils.intercept(FederationStateStoreException.class,
          "Application " + appId + " does not exist",
          () -> stateStore.getApplicationHomeSubCluster(appRequest));
    }

    if (rm != null) {
      rm.stop();
      rm = null;
    }
  }

  @Test
  public void testCleanUpApplicationWhenRMCompleteOneApp() throws Exception {

    // We design such a test case.
    // Step1. We start RM，Set the RM memory to keep a maximum of 1 completed app.
    // Step2. Register app[01-03] to RM memory & stateStore.
    // Step3. We clean up app01, app02, app03, at this time,
    // app01, app02 should be cleaned up from statestore, app03 should remain in statestore.

    // set yarn configuration.
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.setInt(YarnConfiguration.FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY, 10);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, subClusterId.getId());
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setInt(YarnConfiguration.RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS, 1);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());

    // set up MockRM.
    MockRM rm = new MockRM(conf);
    rm.init(conf);
    stateStore = rm.getFederationStateStoreService().getStateStoreClient();
    rm.start();

    // generate an [app01] and join the [SC-1] cluster.
    List<ApplicationId> appIds = new ArrayList<>();
    ApplicationId appId01 = ApplicationId.newInstance(Time.now(), 1);
    addApplication2StateStore(appId01, stateStore);
    addApplication2RMAppManager(rm, appId01);
    appIds.add(appId01);

    // generate an [app02] and join the [SC-1] cluster.
    ApplicationId appId02 = ApplicationId.newInstance(Time.now(), 2);
    addApplication2StateStore(appId02, stateStore);
    addApplication2RMAppManager(rm, appId02);
    appIds.add(appId02);

    // generate an [app03] and join the [SC-1] cluster.
    ApplicationId appId03 = ApplicationId.newInstance(Time.now(), 3);
    addApplication2StateStore(appId03, stateStore);
    addApplication2RMAppManager(rm, appId03);

    // rmAppManager
    RMAppManager rmAppManager = rm.getRMAppManager();
    rmAppManager.finishApplication4Test(appId01);
    rmAppManager.finishApplication4Test(appId02);
    rmAppManager.finishApplication4Test(appId03);
    rmAppManager.checkAppNumCompletedLimit4Test();

    // app01, app02 should be cleaned from statestore
    // After the query, it should report the error not exist.
    for (ApplicationId appId : appIds) {
      GetApplicationHomeSubClusterRequest appRequest =
          GetApplicationHomeSubClusterRequest.newInstance(appId);
      LambdaTestUtils.intercept(FederationStateStoreException.class,
          "Application " + appId + " does not exist",
          () -> stateStore.getApplicationHomeSubCluster(appRequest));
    }

    // app03 should remain in statestore
    List<ApplicationHomeSubCluster> appHomeScList = getApplicationsFromStateStore();
    Assert.assertNotNull(appHomeScList);
    Assert.assertEquals(1, appHomeScList.size());
    ApplicationHomeSubCluster homeSubCluster = appHomeScList.get(0);
    Assert.assertNotNull(homeSubCluster);
    Assert.assertEquals(appId03, homeSubCluster.getApplicationId());
  }

  private void addApplication2StateStore(ApplicationId appId,
      FederationStateStore fedStateStore) throws YarnException {
    ApplicationHomeSubCluster appHomeSC = ApplicationHomeSubCluster.newInstance(
        appId, subClusterId);
    AddApplicationHomeSubClusterRequest addHomeSCRequest =
        AddApplicationHomeSubClusterRequest.newInstance(appHomeSC);
    fedStateStore.addApplicationHomeSubCluster(addHomeSCRequest);
  }

  private List<ApplicationHomeSubCluster> getApplicationsFromStateStore() throws YarnException {
    // make sure the apps can be queried in the stateStore
    GetApplicationsHomeSubClusterRequest allRequest =
        GetApplicationsHomeSubClusterRequest.newInstance(subClusterId);
    GetApplicationsHomeSubClusterResponse allResponse =
        stateStore.getApplicationsHomeSubCluster(allRequest);
    Assert.assertNotNull(allResponse);
    List<ApplicationHomeSubCluster> appHomeSCLists = allResponse.getAppsHomeSubClusters();
    Assert.assertNotNull(appHomeSCLists);
    return appHomeSCLists;
  }

  private void addApplication2RMAppManager(MockRM rm, ApplicationId appId) {
    RMContext rmContext = rm.getRMContext();
    Map<ApplicationId, RMApp> rmAppMaps = rmContext.getRMApps();
    String user = MockApps.newUserName();
    String name = MockApps.newAppName();
    String queue = MockApps.newQueue();

    YarnScheduler scheduler = mock(YarnScheduler.class);

    ApplicationMasterService masterService =
        new ApplicationMasterService(rmContext, scheduler);

    ApplicationSubmissionContext submissionContext =
        new ApplicationSubmissionContextPBImpl();

    // applicationId will not be used because RMStateStore is mocked,
    // but applicationId is still set for safety
    submissionContext.setApplicationId(appId);
    submissionContext.setPriority(Priority.newInstance(0));

    RMApp application = new RMAppImpl(appId, rmContext, conf, name,
        user, queue, submissionContext, scheduler, masterService,
        System.currentTimeMillis(), "YARN", null,
        new ArrayList<>());

    rmAppMaps.putIfAbsent(application.getApplicationId(), application);
  }


  @Test
  public void testPolicyConfigurationMethod() throws YarnException {

    // This test case tests 3 methods.
    // 1.setPolicyConfiguration
    // 2.getPolicyConfiguration
    // 3.getPolicyConfigurations
    FederationStateStoreService stateStoreService = mockRM.getFederationStateStoreService();

    // set queue basic information (queue1)
    String queue1 = "queue1";
    SubClusterPolicyConfiguration requestPolicyConf1 = getUniformPolicy(queue1);
    SetSubClusterPolicyConfigurationRequest configurationRequest1 =
        SetSubClusterPolicyConfigurationRequest.newInstance(requestPolicyConf1);
    // store policy configuration (queue1)
    stateStoreService.setPolicyConfiguration(configurationRequest1);

    // set queue basic information (queue2)
    String queue2 = "queue2";
    SubClusterPolicyConfiguration requestPolicyConf2 = getUniformPolicy(queue2);
    SetSubClusterPolicyConfigurationRequest configurationRequest2 =
        SetSubClusterPolicyConfigurationRequest.newInstance(requestPolicyConf2);
    // store policy configuration (queue1)
    stateStoreService.setPolicyConfiguration(configurationRequest2);

    // get policy configuration
    GetSubClusterPolicyConfigurationRequest request1 =
        GetSubClusterPolicyConfigurationRequest.newInstance(queue1);
    GetSubClusterPolicyConfigurationResponse response =
        stateStoreService.getPolicyConfiguration(request1);
    Assert.assertNotNull(response);

    SubClusterPolicyConfiguration responsePolicyConf =
        response.getPolicyConfiguration();
    Assert.assertNotNull(responsePolicyConf);
    Assert.assertEquals(requestPolicyConf1, responsePolicyConf);

    // get policy configurations
    GetSubClusterPoliciesConfigurationsRequest policiesRequest1 =
        GetSubClusterPoliciesConfigurationsRequest.newInstance();
    GetSubClusterPoliciesConfigurationsResponse policiesResponse1 =
        stateStoreService.getPoliciesConfigurations(policiesRequest1);
    Assert.assertNotNull(policiesResponse1);

    List<SubClusterPolicyConfiguration> policiesConfigs = policiesResponse1.getPoliciesConfigs();
    Assert.assertNotNull(policiesConfigs);
    Assert.assertEquals(2, policiesConfigs.size());
    Assert.assertTrue(policiesConfigs.contains(requestPolicyConf1));
    Assert.assertTrue(policiesConfigs.contains(requestPolicyConf2));
  }

  public SubClusterPolicyConfiguration getUniformPolicy(String queue)
      throws FederationPolicyInitializationException {
    UniformBroadcastPolicyManager wfp = new UniformBroadcastPolicyManager();
    wfp.setQueue(queue);
    SubClusterPolicyConfiguration fpc = wfp.serializeConf();
    return fpc;
  }

  @Test
  public void testSubClusterMethod() throws YarnException {

    // This test case tests 5 methods.
    // 1.registerSubCluster
    // 2.deregisterSubCluster
    // 3.subClusterHeartbeat
    // 4.getSubCluster
    // 5.getSubClusters

    FederationStateStoreService stateStoreService =
        mockRM.getFederationStateStoreService();

    // registerSubCluster subCluster1
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    SubClusterInfo subClusterInfo1 = createSubClusterInfo(subClusterId1);

    SubClusterRegisterRequest registerRequest1 =
        SubClusterRegisterRequest.newInstance(subClusterInfo1);
    stateStoreService.registerSubCluster(registerRequest1);

    // registerSubCluster subCluster2
    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");
    SubClusterInfo subClusterInfo2 = createSubClusterInfo(subClusterId2);

    SubClusterRegisterRequest registerRequest2 =
        SubClusterRegisterRequest.newInstance(subClusterInfo2);
    stateStoreService.registerSubCluster(registerRequest2);

    // getSubCluster subCluster1
    GetSubClusterInfoRequest subClusterRequest =
        GetSubClusterInfoRequest.newInstance(subClusterId1);
    GetSubClusterInfoResponse subClusterResponse =
        stateStoreService.getSubCluster(subClusterRequest);
    Assert.assertNotNull(subClusterResponse);

    // We query subCluster1, we want to get SubClusterInfo of subCluster1
    SubClusterInfo subClusterInfo1Resp = subClusterResponse.getSubClusterInfo();
    Assert.assertNotNull(subClusterInfo1Resp);
    Assert.assertEquals(subClusterInfo1, subClusterInfo1Resp);

    // We call the getSubClusters method and filter the Active SubCluster
    // subCluster1 and subCluster2 are just registered, they are in NEW state,
    // so we will get 0 active subclusters
    GetSubClustersInfoRequest subClustersInfoRequest =
        GetSubClustersInfoRequest.newInstance(true);
    GetSubClustersInfoResponse subClustersInfoResp =
        stateStoreService.getSubClusters(subClustersInfoRequest);
    Assert.assertNotNull(subClustersInfoResp);
    List<SubClusterInfo> subClusterInfos = subClustersInfoResp.getSubClusters();
    Assert.assertNotNull(subClusterInfos);
    Assert.assertEquals(0, subClusterInfos.size());

    // We let subCluster1 heartbeat and set subCluster1 to Running state
    SubClusterHeartbeatRequest heartbeatRequest =
        SubClusterHeartbeatRequest.newInstance(subClusterId1, SubClusterState.SC_RUNNING,
        "capability");
    SubClusterHeartbeatResponse heartbeatResponse =
        stateStoreService.subClusterHeartbeat(heartbeatRequest);
    Assert.assertNotNull(heartbeatResponse);

    // We call the getSubClusters method again and filter the Active SubCluster
    // We want to get 1 active SubCluster
    GetSubClustersInfoRequest subClustersInfoRequest1 =
        GetSubClustersInfoRequest.newInstance(true);
    GetSubClustersInfoResponse subClustersInfoResp1 =
        stateStoreService.getSubClusters(subClustersInfoRequest1);
    Assert.assertNotNull(subClustersInfoResp1);
    List<SubClusterInfo> subClusterInfos1 = subClustersInfoResp1.getSubClusters();
    Assert.assertNotNull(subClusterInfos1);
    Assert.assertEquals(1, subClusterInfos1.size());
  }

  private SubClusterInfo createSubClusterInfo(SubClusterId clusterId) {

    String amRMAddress = "1.2.3.4:1";
    String clientRMAddress = "1.2.3.4:2";
    String rmAdminAddress = "1.2.3.4:3";
    String webAppAddress = "1.2.3.4:4";

    return SubClusterInfo.newInstance(clusterId, amRMAddress,
        clientRMAddress, rmAdminAddress, webAppAddress, SubClusterState.SC_NEW,
        Time.now(), "capability");
  }
}
