/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.router.webapp;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationListInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.RMQueueAclInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntry;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.NodeIDsInfo;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService.RequestInterceptorChainWrapper;
import org.apache.hadoop.yarn.server.router.clientrm.TestableFederationClientInterceptor;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.router.webapp.cache.RouterAppInfoCacheKey;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDefinitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewReservation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeAllocationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.BulkActivitiesInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationConfInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationRMQueueAclInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationBulkActivitiesInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationSchedulerTypeInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationClusterInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationClusterUserInfo;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.dao.ConfInfo;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_DELEGATION_KEY_UPDATE_INTERVAL_KEY;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_DELEGATION_TOKEN_MAX_LIFETIME_KEY;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_DELEGATION_TOKEN_RENEW_INTERVAL_KEY;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_DELEGATION_TOKEN_REMOVE_SCAN_INTERVAL_DEFAULT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_DELEGATION_TOKEN_REMOVE_SCAN_INTERVAL_KEY;

import static org.apache.hadoop.yarn.server.router.webapp.MockDefaultRequestInterceptorREST.DURATION;
import static org.apache.hadoop.yarn.server.router.webapp.MockDefaultRequestInterceptorREST.NUM_CONTAINERS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Extends the {@code BaseRouterClientRMTest} and overrides methods in order to
 * use the {@code RouterClientRMService} pipeline test cases for testing the
 * {@code FederationInterceptor} class. The tests for
 * {@code RouterClientRMService} has been written cleverly so that it can be
 * reused to validate different request interceptor chains.
 */
public class TestFederationInterceptorREST extends BaseRouterWebServicesTest {

  private final static int NUM_SUBCLUSTER = 4;
  private static final int BAD_REQUEST = 400;
  private static final int ACCEPTED = 202;
  private static final String TEST_USER = "test-user";
  private static final int OK = 200;
  private static String user = "test-user";
  private TestableFederationInterceptorREST interceptor;
  private MemoryFederationStateStore stateStore;
  private FederationStateStoreTestUtil stateStoreUtil;
  private List<SubClusterId> subClusters;
  private static final String TEST_RENEWER = "test-renewer";

  public void setUp() throws YarnException, IOException {

    super.setUpConfig();
    interceptor = new TestableFederationInterceptorREST();

    stateStore = new MemoryFederationStateStore();
    stateStore.init(this.getConf());
    FederationStateStoreFacade.getInstance().reinitialize(stateStore,
        this.getConf());
    stateStoreUtil = new FederationStateStoreTestUtil(stateStore);

    interceptor.setConf(this.getConf());
    interceptor.init(TEST_USER);

    subClusters = new ArrayList<>();

    for (int i = 0; i < NUM_SUBCLUSTER; i++) {
      SubClusterId sc = SubClusterId.newInstance(Integer.toString(i));
      stateStoreUtil.registerSubCluster(sc);
      subClusters.add(sc);
    }

    RouterClientRMService routerClientRMService = new RouterClientRMService();
    routerClientRMService.initUserPipelineMap(getConf());
    long secretKeyInterval = this.getConf().getLong(
        RM_DELEGATION_KEY_UPDATE_INTERVAL_KEY, RM_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
    long tokenMaxLifetime = this.getConf().getLong(
        RM_DELEGATION_TOKEN_MAX_LIFETIME_KEY, RM_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
    long tokenRenewInterval = this.getConf().getLong(
        RM_DELEGATION_TOKEN_RENEW_INTERVAL_KEY, RM_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
    long removeScanInterval = this.getConf().getTimeDuration(
        RM_DELEGATION_TOKEN_REMOVE_SCAN_INTERVAL_KEY,
        RM_DELEGATION_TOKEN_REMOVE_SCAN_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    RouterDelegationTokenSecretManager tokenSecretManager = new RouterDelegationTokenSecretManager(
        secretKeyInterval, tokenMaxLifetime, tokenRenewInterval, removeScanInterval);
    tokenSecretManager.startThreads();
    routerClientRMService.setRouterDTSecretManager(tokenSecretManager);

    TestableFederationClientInterceptor clientInterceptor =
        new TestableFederationClientInterceptor();
    clientInterceptor.setConf(this.getConf());
    clientInterceptor.init(TEST_RENEWER);
    clientInterceptor.setTokenSecretManager(tokenSecretManager);
    RequestInterceptorChainWrapper wrapper = new RequestInterceptorChainWrapper();
    wrapper.init(clientInterceptor);
    routerClientRMService.getUserPipelineMap().put(TEST_RENEWER, wrapper);
    interceptor.setRouterClientRMService(routerClientRMService);

    for (SubClusterId subCluster : subClusters) {
      SubClusterInfo subClusterInfo = stateStoreUtil.querySubClusterInfo(subCluster);
      interceptor.getOrCreateInterceptorForSubCluster(
          subCluster, subClusterInfo.getRMWebServiceAddress());
    }

    interceptor.setupResourceManager();

  }

  @Override
  public void tearDown() {
    interceptor.shutdown();
    super.tearDown();
  }

  @Override
  protected YarnConfiguration createConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.set(YarnConfiguration.ROUTER_WEBAPP_DEFAULT_INTERCEPTOR_CLASS,
        MockDefaultRequestInterceptorREST.class.getName());
    String mockPassThroughInterceptorClass =
        PassThroughRESTRequestInterceptor.class.getName();

    // Create a request interceptor pipeline for testing. The last one in the
    // chain is the federation interceptor that calls the mock resource manager.
    // The others in the chain will simply forward it to the next one in the
    // chain
    conf.set(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + ","
            + TestableFederationInterceptorREST.class.getName());

    conf.set(YarnConfiguration.FEDERATION_POLICY_MANAGER,
        UniformBroadcastPolicyManager.class.getName());

    // Disable StateStoreFacade cache
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);

    // Open AppsInfo Cache
    conf.setBoolean(YarnConfiguration.ROUTER_APPSINFO_ENABLED, true);
    conf.setInt(YarnConfiguration.ROUTER_APPSINFO_CACHED_COUNT, 10);

    return conf;
  }

  /**
   * This test validates the correctness of GetNewApplication. The return
   * ApplicationId has to belong to one of the SubCluster in the cluster.
   */
  @Test
  public void testGetNewApplication() throws IOException, InterruptedException {

    Response response = interceptor.createNewApplication(null);

    Assert.assertNotNull(response);
    NewApplication ci = (NewApplication) response.getEntity();
    Assert.assertNotNull(ci);
    ApplicationId appId = ApplicationId.fromString(ci.getApplicationId());
    Assert.assertTrue(appId.getClusterTimestamp() < NUM_SUBCLUSTER);
    Assert.assertTrue(appId.getClusterTimestamp() >= 0);
  }

  /**
   * This test validates the correctness of SubmitApplication. The application
   * has to be submitted to one of the SubCluster in the cluster.
   */
  @Test
  public void testSubmitApplication()
      throws YarnException, IOException, InterruptedException {

    ApplicationId appId =
        ApplicationId.newInstance(Time.now(), 1);

    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    Response response = interceptor.submitApplication(context, null);
    Assert.assertEquals(ACCEPTED, response.getStatus());
    SubClusterId ci = (SubClusterId) response.getEntity();

    Assert.assertNotNull(response);
    SubClusterId scIdResult = stateStoreUtil.queryApplicationHomeSC(appId);
    Assert.assertNotNull(scIdResult);
    Assert.assertTrue(subClusters.contains(scIdResult));
    Assert.assertEquals(ci, scIdResult);
  }

  /**
   * This test validates the correctness of SubmitApplication in case of
   * multiple submission. The first retry has to be submitted to the same
   * SubCluster of the first attempt.
   */
  @Test
  public void testSubmitApplicationMultipleSubmission()
      throws YarnException, IOException, InterruptedException {

    ApplicationId appId =
        ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    // First attempt
    Response response = interceptor.submitApplication(context, null);
    Assert.assertNotNull(response);
    Assert.assertEquals(ACCEPTED, response.getStatus());

    SubClusterId scIdResult = stateStoreUtil.queryApplicationHomeSC(appId);
    Assert.assertNotNull(scIdResult);

    // First retry
    response = interceptor.submitApplication(context, null);

    Assert.assertNotNull(response);
    Assert.assertEquals(ACCEPTED, response.getStatus());
    SubClusterId scIdResult2 = stateStoreUtil.queryApplicationHomeSC(appId);
    Assert.assertNotNull(scIdResult2);
    Assert.assertEquals(scIdResult, scIdResult2);
  }

  /**
   * This test validates the correctness of SubmitApplication in case of empty
   * request.
   */
  @Test
  public void testSubmitApplicationEmptyRequest() throws IOException, InterruptedException {

    // ApplicationSubmissionContextInfo null
    Response response = interceptor.submitApplication(null, null);

    Assert.assertEquals(BAD_REQUEST, response.getStatus());

    // ApplicationSubmissionContextInfo empty
    response = interceptor
        .submitApplication(new ApplicationSubmissionContextInfo(), null);

    Assert.assertEquals(BAD_REQUEST, response.getStatus());

    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    response = interceptor.submitApplication(context, null);
    Assert.assertEquals(BAD_REQUEST, response.getStatus());
  }

  /**
   * This test validates the correctness of SubmitApplication in case of
   * application in wrong format.
   */
  @Test
  public void testSubmitApplicationWrongFormat() throws IOException, InterruptedException {

    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    context.setApplicationId("Application_wrong_id");
    Response response = interceptor.submitApplication(context, null);
    Assert.assertEquals(BAD_REQUEST, response.getStatus());
  }

  /**
   * This test validates the correctness of ForceKillApplication in case the
   * application exists in the cluster.
   */
  @Test
  public void testForceKillApplication()
      throws YarnException, IOException, InterruptedException {

    ApplicationId appId =
        ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    // Submit the application we are going to kill later
    Response response = interceptor.submitApplication(context, null);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    AppState appState = new AppState("KILLED");

    Response responseKill =
        interceptor.updateAppState(appState, null, appId.toString());
    Assert.assertNotNull(responseKill);
  }

  /**
   * This test validates the correctness of ForceKillApplication in case of
   * application does not exist in StateStore.
   */
  @Test
  public void testForceKillApplicationNotExists()
      throws YarnException, IOException, InterruptedException {

    ApplicationId appId =
        ApplicationId.newInstance(Time.now(), 1);
    AppState appState = new AppState("KILLED");

    Response response =
        interceptor.updateAppState(appState, null, appId.toString());
    Assert.assertEquals(BAD_REQUEST, response.getStatus());

  }

  /**
   * This test validates the correctness of ForceKillApplication in case of
   * application in wrong format.
   */
  @Test
  public void testForceKillApplicationWrongFormat()
      throws YarnException, IOException, InterruptedException {

    AppState appState = new AppState("KILLED");
    Response response =
        interceptor.updateAppState(appState, null, "Application_wrong_id");
    Assert.assertEquals(BAD_REQUEST, response.getStatus());
  }

  /**
   * This test validates the correctness of ForceKillApplication in case of
   * empty request.
   */
  @Test
  public void testForceKillApplicationEmptyRequest()
      throws YarnException, IOException, InterruptedException {
    ApplicationId appId =
        ApplicationId.newInstance(Time.now(), 1);

    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    // Submit the application we are going to kill later
    interceptor.submitApplication(context, null);

    Response response =
        interceptor.updateAppState(null, null, appId.toString());
    Assert.assertEquals(BAD_REQUEST, response.getStatus());

  }

  /**
   * This test validates the correctness of GetApplicationReport in case the
   * application exists in the cluster.
   */
  @Test
  public void testGetApplicationReport()
      throws YarnException, IOException, InterruptedException {

    ApplicationId appId =
        ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    // Submit the application we want the report later
    Response response = interceptor.submitApplication(context, null);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    AppInfo responseGet = interceptor.getApp(null, appId.toString(), null);

    Assert.assertNotNull(responseGet);
  }

  /**
   * This test validates the correctness of GetApplicationReport in case the
   * application does not exist in StateStore.
   */
  @Test
  public void testGetApplicationNotExists() {

    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);

    AppInfo response = interceptor.getApp(null, appId.toString(), null);

    Assert.assertNull(response);
  }

  /**
   * This test validates the correctness of GetApplicationReport in case of
   * application in wrong format.
   */
  @Test
  public void testGetApplicationWrongFormat() {

    AppInfo response = interceptor.getApp(null, "Application_wrong_id", null);

    Assert.assertNull(response);
  }

  /**
   * This test validates the correctness of GetApplicationsReport in case each
   * subcluster provided one application.
   */
  @Test
  public void testGetApplicationsReport() {

    AppsInfo responseGet = interceptor.getApps(null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null);

    Assert.assertNotNull(responseGet);
    Assert.assertEquals(NUM_SUBCLUSTER, responseGet.getApps().size());
    // The merged operations is tested in TestRouterWebServiceUtil
  }

  /**
   * This test validates the correctness of GetNodes in case each subcluster
   * provided one node with the LastHealthUpdate set to the SubClusterId. The
   * expected result would be the NodeInfo from the last SubCluster that has
   * LastHealthUpdate equal to Num_SubCluster -1.
   */
  @Test
  public void testGetNode() {

    NodeInfo responseGet = interceptor.getNode("testGetNode");

    Assert.assertNotNull(responseGet);
    Assert.assertEquals(NUM_SUBCLUSTER - 1, responseGet.getLastHealthUpdate());
  }

  /**
   * This test validates the correctness of GetNodes in case each subcluster
   * provided one node.
   */
  @Test
  public void testGetNodes() {

    NodesInfo responseGet = interceptor.getNodes(null);

    Assert.assertNotNull(responseGet);
    Assert.assertEquals(NUM_SUBCLUSTER, responseGet.getNodes().size());
    // The remove duplicate operations is tested in TestRouterWebServiceUtil
  }

  /**
   * This test validates the correctness of updateNodeResource().
   */
  @Test
  public void testUpdateNodeResource() {
    List<NodeInfo> nodes = interceptor.getNodes(null).getNodes();
    Assert.assertFalse(nodes.isEmpty());
    final String nodeId = nodes.get(0).getNodeId();
    ResourceOptionInfo resourceOption = new ResourceOptionInfo(
        ResourceOption.newInstance(
            Resource.newInstance(2048, 3), 1000));
    ResourceInfo resource = interceptor.updateNodeResource(
        null, nodeId, resourceOption);
    Assert.assertNotNull(resource);
    Assert.assertEquals(2048, resource.getMemorySize());
    Assert.assertEquals(3, resource.getvCores());
  }

  /**
   * This test validates the correctness of getClusterMetricsInfo in case each
   * SubCluster provided a ClusterMetricsInfo with appsSubmitted set to the
   * SubClusterId. The expected result would be appSubmitted equals to the sum
   * of SubClusterId. SubClusterId in this case is an integer.
   */
  @Test
  public void testGetClusterMetrics() {

    ClusterMetricsInfo responseGet = interceptor.getClusterMetricsInfo();

    Assert.assertNotNull(responseGet);
    int expectedAppSubmitted = 0;
    for (int i = 0; i < NUM_SUBCLUSTER; i++) {
      expectedAppSubmitted += i;
    }
    Assert.assertEquals(expectedAppSubmitted, responseGet.getAppsSubmitted());
    // The merge operations is tested in TestRouterWebServiceUtil
  }

  /**
   * This test validates the correctness of GetApplicationState in case the
   * application exists in the cluster.
   */
  @Test
  public void testGetApplicationState()
      throws YarnException, IOException, InterruptedException {

    ApplicationId appId =
        ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    // Submit the application we want the report later
    Response response = interceptor.submitApplication(context, null);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    AppState responseGet = interceptor.getAppState(null, appId.toString());

    Assert.assertNotNull(responseGet);
    Assert.assertEquals(MockDefaultRequestInterceptorREST.APP_STATE_RUNNING,
        responseGet.getState());
  }

  /**
   * This test validates the correctness of GetApplicationState in case the
   * application does not exist in StateStore.
   */
  @Test
  public void testGetApplicationStateNotExists() throws IOException {

    ApplicationId appId =
        ApplicationId.newInstance(Time.now(), 1);

    AppState response = interceptor.getAppState(null, appId.toString());

    Assert.assertNull(response);
  }

  /**
   * This test validates the correctness of GetApplicationState in case of
   * application in wrong format.
   */
  @Test
  public void testGetApplicationStateWrongFormat()
      throws IOException {

    AppState response = interceptor.getAppState(null, "Application_wrong_id");

    Assert.assertNull(response);
  }

  /**
   * This test validates the creation of new interceptor in case of a
   * RMSwitchover in a subCluster.
   */
  @Test
  public void testRMSwitchoverOfOneSC() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance(Integer.toString(0));

    interceptor.getClusterMetricsInfo();
    Assert.assertEquals("http://1.2.3.4:4", interceptor
            .getInterceptorForSubCluster(subClusterId).getWebAppAddress());

    //Register the first subCluster with secondRM simulating RMSwitchover
    registerSubClusterWithSwitchoverRM(subClusterId);

    interceptor.getClusterMetricsInfo();
    Assert.assertEquals("http://5.6.7.8:8", interceptor
            .getInterceptorForSubCluster(subClusterId).getWebAppAddress());
  }

  private void registerSubClusterWithSwitchoverRM(SubClusterId subClusterId)
          throws YarnException {
    String amRMAddress = "5.6.7.8:5";
    String clientRMAddress = "5.6.7.8:6";
    String rmAdminAddress = "5.6.7.8:7";
    String webAppAddress = "5.6.7.8:8";

    SubClusterInfo subClusterInfo = SubClusterInfo.newInstance(subClusterId,
            amRMAddress, clientRMAddress, rmAdminAddress, webAppAddress,
            SubClusterState.SC_RUNNING, new MonotonicClock().getTime(),
            "capability");
    stateStore.registerSubCluster(
            SubClusterRegisterRequest.newInstance(subClusterInfo));
  }

  @Test
  public void testGetContainers()
      throws YarnException, IOException, InterruptedException {

    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    // Submit the application we want the report later
    Response response = interceptor.submitApplication(context, null);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    ApplicationAttemptId appAttempt = ApplicationAttemptId.newInstance(appId, 1);

    ContainersInfo responseGet = interceptor.getContainers(
        null, null, appId.toString(), appAttempt.toString());

    Assert.assertEquals(4, responseGet.getContainers().size());
  }

  @Test
  public void testGetContainersNotExists() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, the appAttemptId is empty or null.",
        () -> interceptor.getContainers(null, null, appId.toString(), null));
  }

  @Test
  public void testGetContainersWrongFormat() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationAttemptId appAttempt = ApplicationAttemptId.newInstance(appId, 1);

    // Test Case 1: appId is wrong format, appAttemptId is accurate.
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid ApplicationId prefix: Application_wrong_id. " +
        "The valid ApplicationId should start with prefix application",
        () -> interceptor.getContainers(null, null, "Application_wrong_id", appAttempt.toString()));

    // Test Case2: appId is accurate, appAttemptId is wrong format.
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid AppAttemptId prefix: AppAttempt_wrong_id",
        () -> interceptor.getContainers(null, null, appId.toString(), "AppAttempt_wrong_id"));
  }

  @Test
  public void testGetNodeToLabels() throws IOException {
    NodeToLabelsInfo info = interceptor.getNodeToLabels(null);
    HashMap<String, NodeLabelsInfo> map = info.getNodeToLabels();
    Assert.assertNotNull(map);
    Assert.assertEquals(2, map.size());

    NodeLabelsInfo node1Value = map.getOrDefault("node1", null);
    Assert.assertNotNull(node1Value);
    Assert.assertEquals(1, node1Value.getNodeLabelsName().size());
    Assert.assertEquals("CPU", node1Value.getNodeLabelsName().get(0));

    NodeLabelsInfo node2Value = map.getOrDefault("node2", null);
    Assert.assertNotNull(node2Value);
    Assert.assertEquals(1, node2Value.getNodeLabelsName().size());
    Assert.assertEquals("GPU", node2Value.getNodeLabelsName().get(0));
  }

  @Test
  public void testGetLabelsToNodes() throws Exception {
    LabelsToNodesInfo labelsToNodesInfo = interceptor.getLabelsToNodes(null);
    Map<NodeLabelInfo, NodeIDsInfo> map = labelsToNodesInfo.getLabelsToNodes();
    Assert.assertNotNull(map);
    Assert.assertEquals(3, map.size());

    NodeLabel labelX = NodeLabel.newInstance("x", false);
    NodeLabelInfo nodeLabelInfoX = new NodeLabelInfo(labelX);
    NodeIDsInfo nodeIDsInfoX = map.get(nodeLabelInfoX);
    Assert.assertNotNull(nodeIDsInfoX);
    Assert.assertEquals(2, nodeIDsInfoX.getNodeIDs().size());
    Resource resourceX =
        nodeIDsInfoX.getPartitionInfo().getResourceAvailable().getResource();
    Assert.assertNotNull(resourceX);
    Assert.assertEquals(4*10, resourceX.getVirtualCores());
    Assert.assertEquals(4*20*1024, resourceX.getMemorySize());

    NodeLabel labelY = NodeLabel.newInstance("y", false);
    NodeLabelInfo nodeLabelInfoY = new NodeLabelInfo(labelY);
    NodeIDsInfo nodeIDsInfoY = map.get(nodeLabelInfoY);
    Assert.assertNotNull(nodeIDsInfoY);
    Assert.assertEquals(2, nodeIDsInfoY.getNodeIDs().size());
    Resource resourceY =
        nodeIDsInfoY.getPartitionInfo().getResourceAvailable().getResource();
    Assert.assertNotNull(resourceY);
    Assert.assertEquals(4*20, resourceY.getVirtualCores());
    Assert.assertEquals(4*40*1024, resourceY.getMemorySize());
  }

  @Test
  public void testGetClusterNodeLabels() throws Exception {
    NodeLabelsInfo nodeLabelsInfo = interceptor.getClusterNodeLabels(null);
    Assert.assertNotNull(nodeLabelsInfo);
    Assert.assertEquals(2, nodeLabelsInfo.getNodeLabelsName().size());

    List<String> nodeLabelsName = nodeLabelsInfo.getNodeLabelsName();
    Assert.assertNotNull(nodeLabelsName);
    Assert.assertTrue(nodeLabelsName.contains("cpu"));
    Assert.assertTrue(nodeLabelsName.contains("gpu"));

    ArrayList<NodeLabelInfo> nodeLabelInfos = nodeLabelsInfo.getNodeLabelsInfo();
    Assert.assertNotNull(nodeLabelInfos);
    Assert.assertEquals(2, nodeLabelInfos.size());
    NodeLabelInfo cpuNodeLabelInfo = new NodeLabelInfo("cpu", false);
    Assert.assertTrue(nodeLabelInfos.contains(cpuNodeLabelInfo));
    NodeLabelInfo gpuNodeLabelInfo = new NodeLabelInfo("gpu", false);
    Assert.assertTrue(nodeLabelInfos.contains(gpuNodeLabelInfo));
  }

  @Test
  public void testGetLabelsOnNode() throws Exception {
    NodeLabelsInfo nodeLabelsInfo = interceptor.getLabelsOnNode(null, "node1");
    Assert.assertNotNull(nodeLabelsInfo);
    Assert.assertEquals(2, nodeLabelsInfo.getNodeLabelsName().size());

    List<String> nodeLabelsName = nodeLabelsInfo.getNodeLabelsName();
    Assert.assertNotNull(nodeLabelsName);
    Assert.assertTrue(nodeLabelsName.contains("x"));
    Assert.assertTrue(nodeLabelsName.contains("y"));

    // null request
    interceptor.setAllowPartialResult(false);
    NodeLabelsInfo nodeLabelsInfo2 = interceptor.getLabelsOnNode(null, "node2");
    Assert.assertNotNull(nodeLabelsInfo2);
    Assert.assertEquals(0, nodeLabelsInfo2.getNodeLabelsName().size());
  }

  @Test
  public void testGetContainer() throws Exception {
    //
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId appContainerId = ContainerId.newContainerId(appAttemptId, 1);
    String applicationId = appId.toString();
    String attemptId = appAttemptId.toString();
    String containerId = appContainerId.toString();

    // Submit application to multiSubCluster
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(applicationId);
    Assert.assertNotNull(interceptor.submitApplication(context, null));

    // Test Case1: Wrong ContainerId
    LambdaTestUtils.intercept(IllegalArgumentException.class, "Invalid ContainerId prefix: 0",
        () -> interceptor.getContainer(null, null, applicationId, attemptId, "0"));

    // Test Case2: Correct ContainerId

    ContainerInfo containerInfo = interceptor.getContainer(null, null, applicationId,
        attemptId, containerId);
    Assert.assertNotNull(containerInfo);
  }

  @Test
  public void testGetAppAttempts() throws IOException, InterruptedException {
    // Submit application to multiSubCluster
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    Assert.assertNotNull(interceptor.submitApplication(context, null));

    AppAttemptsInfo appAttemptsInfo = interceptor.getAppAttempts(null, appId.toString());
    Assert.assertNotNull(appAttemptsInfo);

    ArrayList<AppAttemptInfo> attemptLists = appAttemptsInfo.getAttempts();
    Assert.assertNotNull(appAttemptsInfo);
    Assert.assertEquals(2, attemptLists.size());

    AppAttemptInfo attemptInfo1 = attemptLists.get(0);
    Assert.assertNotNull(attemptInfo1);
    Assert.assertEquals(0, attemptInfo1.getAttemptId());
    Assert.assertEquals("AppAttemptId_0", attemptInfo1.getAppAttemptId());
    Assert.assertEquals("LogLink_0", attemptInfo1.getLogsLink());
    Assert.assertEquals(1659621705L, attemptInfo1.getFinishedTime());

    AppAttemptInfo attemptInfo2 = attemptLists.get(1);
    Assert.assertNotNull(attemptInfo2);
    Assert.assertEquals(0, attemptInfo2.getAttemptId());
    Assert.assertEquals("AppAttemptId_1", attemptInfo2.getAppAttemptId());
    Assert.assertEquals("LogLink_1", attemptInfo2.getLogsLink());
    Assert.assertEquals(1659621705L, attemptInfo2.getFinishedTime());
  }

  @Test
  public void testGetAppAttempt() throws IOException, InterruptedException {

    // Generate ApplicationId information
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    // Generate ApplicationAttemptId information
    Assert.assertNotNull(interceptor.submitApplication(context, null));
    ApplicationAttemptId expectAppAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    String appAttemptId = expectAppAttemptId.toString();

    org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo
        appAttemptInfo = interceptor.getAppAttempt(null, null, appId.toString(), appAttemptId);

    Assert.assertNotNull(appAttemptInfo);
    Assert.assertEquals(expectAppAttemptId.toString(), appAttemptInfo.getAppAttemptId());
    Assert.assertEquals("url", appAttemptInfo.getTrackingUrl());
    Assert.assertEquals("oUrl", appAttemptInfo.getOriginalTrackingUrl());
    Assert.assertEquals(124, appAttemptInfo.getRpcPort());
    Assert.assertEquals("host", appAttemptInfo.getHost());
  }

  @Test
  public void testGetAppTimeout() throws IOException, InterruptedException {

    // Generate ApplicationId information
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    // Generate ApplicationAttemptId information
    Assert.assertNotNull(interceptor.submitApplication(context, null));

    ApplicationTimeoutType appTimeoutType = ApplicationTimeoutType.LIFETIME;
    AppTimeoutInfo appTimeoutInfo =
        interceptor.getAppTimeout(null, appId.toString(), appTimeoutType.toString());
    Assert.assertNotNull(appTimeoutInfo);
    Assert.assertEquals(10, appTimeoutInfo.getRemainingTimeInSec());
    Assert.assertEquals("UNLIMITED", appTimeoutInfo.getExpireTime());
    Assert.assertEquals(appTimeoutType, appTimeoutInfo.getTimeoutType());
  }

  @Test
  public void testGetAppTimeouts() throws IOException, InterruptedException {

    // Generate ApplicationId information
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    // Generate ApplicationAttemptId information
    Assert.assertNotNull(interceptor.submitApplication(context, null));

    AppTimeoutsInfo appTimeoutsInfo = interceptor.getAppTimeouts(null, appId.toString());
    Assert.assertNotNull(appTimeoutsInfo);

    List<AppTimeoutInfo> timeouts = appTimeoutsInfo.getAppTimeouts();
    Assert.assertNotNull(timeouts);
    Assert.assertEquals(1, timeouts.size());

    AppTimeoutInfo resultAppTimeout = timeouts.get(0);
    Assert.assertNotNull(resultAppTimeout);
    Assert.assertEquals(10, resultAppTimeout.getRemainingTimeInSec());
    Assert.assertEquals("UNLIMITED", resultAppTimeout.getExpireTime());
    Assert.assertEquals(ApplicationTimeoutType.LIFETIME, resultAppTimeout.getTimeoutType());
  }

  @Test
  public void testUpdateApplicationTimeout() throws IOException, InterruptedException,
      YarnException {

    // Generate ApplicationId information
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    // Generate ApplicationAttemptId information
    Assert.assertNotNull(interceptor.submitApplication(context, null));

    long newLifetime = 10L;
    // update 10L seconds more to timeout
    String timeout = Times.formatISO8601(Time.now() + newLifetime * 1000);
    AppTimeoutInfo paramAppTimeOut = new AppTimeoutInfo();
    paramAppTimeOut.setExpiryTime(timeout);
    // RemainingTime = Math.max((timeoutInMillis - System.currentTimeMillis()) / 1000, 0))
    paramAppTimeOut.setRemainingTime(newLifetime);
    paramAppTimeOut.setTimeoutType(ApplicationTimeoutType.LIFETIME);

    Response response =
        interceptor.updateApplicationTimeout(paramAppTimeOut, null, appId.toString());
    Assert.assertNotNull(response);
    AppTimeoutInfo entity = (AppTimeoutInfo) response.getEntity();
    Assert.assertNotNull(entity);
    Assert.assertEquals(paramAppTimeOut.getExpireTime(), entity.getExpireTime());
    Assert.assertEquals(paramAppTimeOut.getTimeoutType(), entity.getTimeoutType());
    Assert.assertEquals(paramAppTimeOut.getRemainingTimeInSec(), entity.getRemainingTimeInSec());
  }

  @Test
  public void testUpdateApplicationPriority() throws IOException, InterruptedException,
      YarnException {

    // Submit application to multiSubCluster
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());
    context.setPriority(20);

    // Submit the application we are going to kill later
    Assert.assertNotNull(interceptor.submitApplication(context, null));

    int iPriority = 10;
    // Set Priority for application
    Response response = interceptor.updateApplicationPriority(
        new AppPriority(iPriority), null, appId.toString());

    Assert.assertNotNull(response);
    AppPriority entity = (AppPriority) response.getEntity();
    Assert.assertNotNull(entity);
    Assert.assertEquals(iPriority, entity.getPriority());
  }

  @Test
  public void testGetAppPriority() throws IOException, InterruptedException {

    // Submit application to multiSubCluster
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    int priority = 40;
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());
    context.setPriority(priority);

    // Submit the application we are going to kill later
    Assert.assertNotNull(interceptor.submitApplication(context, null));

    // Set Priority for application
    AppPriority appPriority = interceptor.getAppPriority(null, appId.toString());
    Assert.assertNotNull(appPriority);
    Assert.assertEquals(priority, appPriority.getPriority());
  }

  @Test
  public void testUpdateAppQueue() throws IOException, InterruptedException,
      YarnException {

    String oldQueue = "oldQueue";
    String newQueue = "newQueue";

    // Submit application to multiSubCluster
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());
    context.setQueue(oldQueue);

    // Submit the application
    Assert.assertNotNull(interceptor.submitApplication(context, null));

    // Set New Queue for application
    Response response = interceptor.updateAppQueue(new AppQueue(newQueue),
        null, appId.toString());

    Assert.assertNotNull(response);
    AppQueue appQueue = (AppQueue) response.getEntity();
    Assert.assertEquals(newQueue, appQueue.getQueue());

    // Get AppQueue by application
    AppQueue queue = interceptor.getAppQueue(null, appId.toString());
    Assert.assertNotNull(queue);
    Assert.assertEquals(newQueue, queue.getQueue());
  }

  @Test
  public void testGetAppQueue() throws IOException, InterruptedException {
    String queueName = "queueName";

    // Submit application to multiSubCluster
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());
    context.setQueue(queueName);

    Assert.assertNotNull(interceptor.submitApplication(context, null));

    // Get Queue by application
    AppQueue queue = interceptor.getAppQueue(null, appId.toString());
    Assert.assertNotNull(queue);
    Assert.assertEquals(queueName, queue.getQueue());
  }

  @Test
  public void testGetAppsInfoCache() {

    AppsInfo responseGet = interceptor.getApps(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    Assert.assertNotNull(responseGet);

    RouterAppInfoCacheKey cacheKey = RouterAppInfoCacheKey.newInstance(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);

    LRUCacheHashMap<RouterAppInfoCacheKey, AppsInfo> appsInfoCache =
        interceptor.getAppInfosCaches();
    Assert.assertNotNull(appsInfoCache);
    Assert.assertFalse(appsInfoCache.isEmpty());
    Assert.assertEquals(1, appsInfoCache.size());
    Assert.assertTrue(appsInfoCache.containsKey(cacheKey));

    AppsInfo cacheResult = appsInfoCache.get(cacheKey);
    Assert.assertNotNull(cacheResult);
    Assert.assertEquals(responseGet, cacheResult);
  }

  @Test
  public void testGetAppStatistics() throws IOException, InterruptedException, YarnException {

    // Submit application to multiSubCluster
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());
    context.setApplicationType("MapReduce");
    context.setQueue("queue");

    Assert.assertNotNull(interceptor.submitApplication(context, null));

    GetApplicationHomeSubClusterRequest request =
        GetApplicationHomeSubClusterRequest.newInstance(appId);
    GetApplicationHomeSubClusterResponse response =
        stateStore.getApplicationHomeSubCluster(request);

    Assert.assertNotNull(response);
    ApplicationHomeSubCluster homeSubCluster = response.getApplicationHomeSubCluster();

    DefaultRequestInterceptorREST interceptorREST =
        interceptor.getInterceptorForSubCluster(homeSubCluster.getHomeSubCluster());

    MockDefaultRequestInterceptorREST mockInterceptorREST =
        (MockDefaultRequestInterceptorREST) interceptorREST;
    mockInterceptorREST.updateApplicationState(YarnApplicationState.RUNNING,
        appId.toString());

    Set<String> stateQueries = new HashSet<>();
    stateQueries.add(YarnApplicationState.RUNNING.name());

    Set<String> typeQueries = new HashSet<>();
    typeQueries.add("MapReduce");

    ApplicationStatisticsInfo response2 =
        interceptor.getAppStatistics(null, stateQueries, typeQueries);

    Assert.assertNotNull(response2);
    Assert.assertFalse(response2.getStatItems().isEmpty());

    StatisticsItemInfo result = response2.getStatItems().get(0);
    Assert.assertEquals(1, result.getCount());
    Assert.assertEquals(YarnApplicationState.RUNNING, result.getState());
    Assert.assertEquals("MapReduce", result.getType());
  }

  @Test
  public void testGetAppActivities() throws IOException, InterruptedException {
    // Submit application to multiSubCluster
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());
    context.setApplicationType("MapReduce");
    context.setQueue("queue");

    Assert.assertNotNull(interceptor.submitApplication(context, null));
    Set<String> prioritiesSet = Collections.singleton("0");
    Set<String> allocationRequestIdsSet = Collections.singleton("0");

    AppActivitiesInfo appActivitiesInfo =
        interceptor.getAppActivities(null, appId.toString(), String.valueOf(Time.now()),
        prioritiesSet, allocationRequestIdsSet, null, "-1", null, false);

    Assert.assertNotNull(appActivitiesInfo);
    Assert.assertEquals(appId.toString(), appActivitiesInfo.getApplicationId());
    Assert.assertEquals(10, appActivitiesInfo.getAllocations().size());
  }

  @Test
  public void testListReservation() throws Exception {

    // submitReservation
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    submitReservation(reservationId);

    // Call the listReservation method
    String applyReservationId = reservationId.toString();
    Response listReservationResponse = interceptor.listReservation(
        QUEUE_DEDICATED_FULL, applyReservationId, -1, -1, false, null);
    Assert.assertNotNull(listReservationResponse);
    Assert.assertNotNull(listReservationResponse.getStatus());
    Status status = Status.fromStatusCode(listReservationResponse.getStatus());
    Assert.assertEquals(Status.OK, status);

    Object entity = listReservationResponse.getEntity();
    Assert.assertNotNull(entity);
    Assert.assertNotNull(entity instanceof ReservationListInfo);

    Assert.assertTrue(entity instanceof ReservationListInfo);
    ReservationListInfo listInfo = (ReservationListInfo) entity;
    Assert.assertNotNull(listInfo);

    List<ReservationInfo> reservationInfoList = listInfo.getReservations();
    Assert.assertNotNull(reservationInfoList);
    Assert.assertEquals(1, reservationInfoList.size());

    ReservationInfo reservationInfo = reservationInfoList.get(0);
    Assert.assertNotNull(reservationInfo);
    Assert.assertEquals(applyReservationId, reservationInfo.getReservationId());

    ReservationDefinitionInfo definitionInfo = reservationInfo.getReservationDefinition();
    Assert.assertNotNull(definitionInfo);

    ReservationRequestsInfo reservationRequestsInfo = definitionInfo.getReservationRequests();
    Assert.assertNotNull(reservationRequestsInfo);

    ArrayList<ReservationRequestInfo> reservationRequestInfoList =
        reservationRequestsInfo.getReservationRequest();
    Assert.assertNotNull(reservationRequestInfoList);
    Assert.assertEquals(1, reservationRequestInfoList.size());

    ReservationRequestInfo reservationRequestInfo = reservationRequestInfoList.get(0);
    Assert.assertNotNull(reservationRequestInfo);
    Assert.assertEquals(4, reservationRequestInfo.getNumContainers());

    ResourceInfo resourceInfo = reservationRequestInfo.getCapability();
    Assert.assertNotNull(resourceInfo);

    int vCore = resourceInfo.getvCores();
    long memory = resourceInfo.getMemorySize();
    Assert.assertEquals(1, vCore);
    Assert.assertEquals(1024, memory);
  }

  @Test
  public void testCreateNewReservation() throws Exception {
    Response response = interceptor.createNewReservation(null);
    Assert.assertNotNull(response);

    Object entity = response.getEntity();
    Assert.assertNotNull(entity);
    Assert.assertTrue(entity instanceof NewReservation);

    NewReservation newReservation = (NewReservation) entity;
    Assert.assertNotNull(newReservation);
    Assert.assertTrue(newReservation.getReservationId().contains("reservation"));
  }

  @Test
  public void testSubmitReservation() throws Exception {

    // submit reservation
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 2);
    Response response = submitReservation(reservationId);
    Assert.assertNotNull(response);
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), response.getStatus());

    String applyReservationId = reservationId.toString();
    Response reservationResponse = interceptor.listReservation(
        QUEUE_DEDICATED_FULL, applyReservationId, -1, -1, false, null);
    Assert.assertNotNull(reservationResponse);

    Object entity = reservationResponse.getEntity();
    Assert.assertNotNull(entity);
    Assert.assertNotNull(entity instanceof ReservationListInfo);

    Assert.assertTrue(entity instanceof ReservationListInfo);
    ReservationListInfo listInfo = (ReservationListInfo) entity;
    Assert.assertNotNull(listInfo);

    List<ReservationInfo> reservationInfos = listInfo.getReservations();
    Assert.assertNotNull(reservationInfos);
    Assert.assertEquals(1, reservationInfos.size());

    ReservationInfo reservationInfo = reservationInfos.get(0);
    Assert.assertNotNull(reservationInfo);
    Assert.assertEquals(reservationInfo.getReservationId(), applyReservationId);
  }

  @Test
  public void testUpdateReservation() throws Exception {
    // submit reservation
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 3);
    Response response = submitReservation(reservationId);
    Assert.assertNotNull(response);
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), response.getStatus());

    // update reservation
    ReservationSubmissionRequest resSubRequest =
        getReservationSubmissionRequest(reservationId, 6, 2048, 2);
    ReservationDefinition reservationDefinition = resSubRequest.getReservationDefinition();
    ReservationDefinitionInfo reservationDefinitionInfo =
        new ReservationDefinitionInfo(reservationDefinition);

    ReservationUpdateRequestInfo updateRequestInfo = new ReservationUpdateRequestInfo();
    updateRequestInfo.setReservationId(reservationId.toString());
    updateRequestInfo.setReservationDefinition(reservationDefinitionInfo);
    Response updateReservationResp = interceptor.updateReservation(updateRequestInfo, null);
    Assert.assertNotNull(updateReservationResp);
    Assert.assertEquals(Status.OK.getStatusCode(), updateReservationResp.getStatus());

    String applyReservationId = reservationId.toString();
    Response reservationResponse = interceptor.listReservation(
            QUEUE_DEDICATED_FULL, applyReservationId, -1, -1, false, null);
    Assert.assertNotNull(reservationResponse);

    Object entity = reservationResponse.getEntity();
    Assert.assertNotNull(entity);
    Assert.assertNotNull(entity instanceof ReservationListInfo);

    Assert.assertTrue(entity instanceof ReservationListInfo);
    ReservationListInfo listInfo = (ReservationListInfo) entity;
    Assert.assertNotNull(listInfo);

    List<ReservationInfo> reservationInfos = listInfo.getReservations();
    Assert.assertNotNull(reservationInfos);
    Assert.assertEquals(1, reservationInfos.size());

    ReservationInfo reservationInfo = reservationInfos.get(0);
    Assert.assertNotNull(reservationInfo);
    Assert.assertEquals(reservationInfo.getReservationId(), applyReservationId);

    ReservationDefinitionInfo resDefinitionInfo = reservationInfo.getReservationDefinition();
    Assert.assertNotNull(resDefinitionInfo);

    ReservationRequestsInfo reservationRequestsInfo = resDefinitionInfo.getReservationRequests();
    Assert.assertNotNull(reservationRequestsInfo);

    ArrayList<ReservationRequestInfo> reservationRequestInfoList =
            reservationRequestsInfo.getReservationRequest();
    Assert.assertNotNull(reservationRequestInfoList);
    Assert.assertEquals(1, reservationRequestInfoList.size());

    ReservationRequestInfo reservationRequestInfo = reservationRequestInfoList.get(0);
    Assert.assertNotNull(reservationRequestInfo);
    Assert.assertEquals(6, reservationRequestInfo.getNumContainers());

    ResourceInfo resourceInfo = reservationRequestInfo.getCapability();
    Assert.assertNotNull(resourceInfo);

    int vCore = resourceInfo.getvCores();
    long memory = resourceInfo.getMemorySize();
    Assert.assertEquals(2, vCore);
    Assert.assertEquals(2048, memory);
  }

  @Test
  public void testDeleteReservation() throws Exception {
    // submit reservation
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 4);
    Response response = submitReservation(reservationId);
    Assert.assertNotNull(response);
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), response.getStatus());

    String applyResId = reservationId.toString();
    Response reservationResponse = interceptor.listReservation(
        QUEUE_DEDICATED_FULL, applyResId, -1, -1, false, null);
    Assert.assertNotNull(reservationResponse);

    ReservationDeleteRequestInfo deleteRequestInfo =
        new ReservationDeleteRequestInfo();
    deleteRequestInfo.setReservationId(applyResId);
    Response delResponse = interceptor.deleteReservation(deleteRequestInfo, null);
    Assert.assertNotNull(delResponse);

    LambdaTestUtils.intercept(Exception.class,
        "reservationId with id: " + reservationId + " not found",
        () -> interceptor.listReservation(QUEUE_DEDICATED_FULL, applyResId, -1, -1, false, null));
  }

  private Response submitReservation(ReservationId reservationId)
       throws IOException, InterruptedException {
    ReservationSubmissionRequestInfo resSubmissionRequestInfo =
        getReservationSubmissionRequestInfo(reservationId);
    return interceptor.submitReservation(resSubmissionRequestInfo, null);
  }

  public static ReservationSubmissionRequestInfo getReservationSubmissionRequestInfo(
      ReservationId reservationId) {

    ReservationSubmissionRequest resSubRequest =
        getReservationSubmissionRequest(reservationId, NUM_CONTAINERS, 1024, 1);
    ReservationDefinition reservationDefinition = resSubRequest.getReservationDefinition();

    ReservationSubmissionRequestInfo resSubmissionRequestInfo =
        new ReservationSubmissionRequestInfo();
    resSubmissionRequestInfo.setQueue(resSubRequest.getQueue());
    resSubmissionRequestInfo.setReservationId(reservationId.toString());
    ReservationDefinitionInfo reservationDefinitionInfo =
        new ReservationDefinitionInfo(reservationDefinition);
    resSubmissionRequestInfo.setReservationDefinition(reservationDefinitionInfo);

    return resSubmissionRequestInfo;
  }

  public static ReservationSubmissionRequest getReservationSubmissionRequest(
      ReservationId reservationId, int numContainers, int memory, int vcore) {

    // arrival time from which the resource(s) can be allocated.
    long arrival = Time.now();

    // deadline by when the resource(s) must be allocated.
    // The reason for choosing 1.05 is that this gives an integer
    // DURATION * 0.05 = 3000(ms)
    // deadline = arrival + 3000ms
    long deadline = (long) (arrival + 1.05 * DURATION);

    return createSimpleReservationRequest(
        reservationId, numContainers, arrival, deadline, DURATION, memory, vcore);
  }

  public static ReservationSubmissionRequest createSimpleReservationRequest(
      ReservationId reservationId, int numContainers, long arrival,
      long deadline, long duration, int memory, int vcore) {
    // create a request with a single atomic ask
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(memory, vcore), numContainers, 1, duration);
    ReservationRequests reqs = ReservationRequests.newInstance(
        Collections.singletonList(r), ReservationRequestInterpreter.R_ALL);
    ReservationDefinition rDef = ReservationDefinition.newInstance(
        arrival, deadline, reqs, "testClientRMService#reservation", "0", Priority.UNDEFINED);
    return ReservationSubmissionRequest.newInstance(rDef, QUEUE_DEDICATED_FULL, reservationId);
  }

  @Test
  public void testWebAddressWithScheme() {
    // The style of the web address reported by the subCluster in the heartbeat is 0.0.0.0:8000
    // We design the following 2 test cases:
    String webAppAddress = "0.0.0.0:8000";

    // 1. We try to disable Https, at this point we should get the following link:
    // http://0.0.0.0:8000
    String expectedHttpWebAddress = "http://0.0.0.0:8000";
    String webAppAddressWithScheme =
        WebAppUtils.getHttpSchemePrefix(this.getConf()) + webAppAddress;
    Assert.assertEquals(expectedHttpWebAddress, webAppAddressWithScheme);

    // 2. We try to enable Https，at this point we should get the following link:
    // https://0.0.0.0:8000
    Configuration configuration = this.getConf();
    configuration.set(YarnConfiguration.YARN_HTTP_POLICY_KEY,
        HttpConfig.Policy.HTTPS_ONLY.name());
    String expectedHttpsWebAddress = "https://0.0.0.0:8000";
    String webAppAddressWithScheme2 =
        WebAppUtils.getHttpSchemePrefix(this.getConf()) + webAppAddress;
    Assert.assertEquals(expectedHttpsWebAddress, webAppAddressWithScheme2);
  }

  @Test
  public void testCheckUserAccessToQueue() throws Exception {

    interceptor.setAllowPartialResult(false);

    // Case 1: Only queue admin user can access other user's information
    HttpServletRequest mockHsr = mockHttpServletRequestByUserName("non-admin");
    String errorMsg1 = "User=non-admin doesn't haven access to queue=queue " +
        "so it cannot check ACLs for other users.";
    LambdaTestUtils.intercept(YarnRuntimeException.class, errorMsg1,
        () -> interceptor.checkUserAccessToQueue("queue", "jack",
        QueueACL.SUBMIT_APPLICATIONS.name(), mockHsr));

    // Case 2: request an unknown ACL causes BAD_REQUEST
    HttpServletRequest mockHsr1 = mockHttpServletRequestByUserName("admin");
    String errorMsg2 = "Specified queueAclType=XYZ_ACL is not a valid type, " +
        "valid queue acl types={SUBMIT_APPLICATIONS/ADMINISTER_QUEUE}";
    LambdaTestUtils.intercept(YarnRuntimeException.class, errorMsg2,
        () -> interceptor.checkUserAccessToQueue("queue", "jack", "XYZ_ACL", mockHsr1));

    // We design a test, admin user has ADMINISTER_QUEUE, SUBMIT_APPLICATIONS permissions,
    // yarn user has SUBMIT_APPLICATIONS permissions, other users have no permissions

    // Case 3: get FORBIDDEN for rejected ACL
    checkUserAccessToQueueFailed("queue", "jack", QueueACL.SUBMIT_APPLICATIONS, "admin");
    checkUserAccessToQueueFailed("queue", "jack", QueueACL.ADMINISTER_QUEUE, "admin");

    // Case 4: get OK for listed ACLs
    checkUserAccessToQueueSuccess("queue", "admin", QueueACL.ADMINISTER_QUEUE, "admin");
    checkUserAccessToQueueSuccess("queue", "admin", QueueACL.SUBMIT_APPLICATIONS, "admin");

    // Case 5: get OK only for SUBMIT_APP acl for "yarn" user
    checkUserAccessToQueueFailed("queue", "yarn", QueueACL.ADMINISTER_QUEUE, "admin");
    checkUserAccessToQueueSuccess("queue", "yarn", QueueACL.SUBMIT_APPLICATIONS, "admin");

    interceptor.setAllowPartialResult(true);
  }

  private void checkUserAccessToQueueSuccess(String queue, String userName,
      QueueACL queueACL, String mockUser) throws AuthorizationException {
    HttpServletRequest mockHsr = mockHttpServletRequestByUserName(mockUser);
    RMQueueAclInfo aclInfo =
        interceptor.checkUserAccessToQueue(queue, userName, queueACL.name(), mockHsr);
    Assert.assertNotNull(aclInfo);
    Assert.assertTrue(aclInfo instanceof FederationRMQueueAclInfo);
    FederationRMQueueAclInfo fedAclInfo = (FederationRMQueueAclInfo) aclInfo;
    List<RMQueueAclInfo> aclInfos = fedAclInfo.getList();
    Assert.assertNotNull(aclInfos);
    Assert.assertEquals(4, aclInfos.size());
    for (RMQueueAclInfo rMQueueAclInfo : aclInfos) {
      Assert.assertTrue(rMQueueAclInfo.isAllowed());
    }
  }

  private void checkUserAccessToQueueFailed(String queue, String userName,
      QueueACL queueACL, String mockUser) throws AuthorizationException {
    HttpServletRequest mockHsr = mockHttpServletRequestByUserName(mockUser);
    RMQueueAclInfo aclInfo =
        interceptor.checkUserAccessToQueue(queue, userName, queueACL.name(), mockHsr);
    Assert.assertNotNull(aclInfo);
    Assert.assertTrue(aclInfo instanceof FederationRMQueueAclInfo);
    FederationRMQueueAclInfo fedAclInfo = (FederationRMQueueAclInfo) aclInfo;
    List<RMQueueAclInfo> aclInfos = fedAclInfo.getList();
    Assert.assertNotNull(aclInfos);
    Assert.assertEquals(4, aclInfos.size());
    for (RMQueueAclInfo rMQueueAclInfo : aclInfos) {
      Assert.assertFalse(rMQueueAclInfo.isAllowed());
      String expectDiagnostics = "User=" + userName +
          " doesn't have access to queue=queue with acl-type=" + queueACL.name();
      Assert.assertEquals(expectDiagnostics, rMQueueAclInfo.getDiagnostics());
    }
  }

  private HttpServletRequest mockHttpServletRequestByUserName(String username) {
    HttpServletRequest mockHsr = mock(HttpServletRequest.class);
    when(mockHsr.getRemoteUser()).thenReturn(username);
    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn(username);
    when(mockHsr.getUserPrincipal()).thenReturn(principal);
    return mockHsr;
  }

  @Test
  public void testCheckFederationInterceptorRESTClient() {
    SubClusterId subClusterId = SubClusterId.newInstance("SC-1");
    String webAppSocket = "SC-1:WebAddress";
    String webAppAddress = "http://" + webAppSocket;

    Configuration configuration = new Configuration();
    FederationInterceptorREST rest = new FederationInterceptorREST();
    rest.setConf(configuration);
    rest.init("router");

    DefaultRequestInterceptorREST interceptorREST =
        rest.getOrCreateInterceptorForSubCluster(subClusterId, webAppSocket);

    Assert.assertNotNull(interceptorREST);
    Assert.assertNotNull(interceptorREST.getClient());
    Assert.assertEquals(webAppAddress, interceptorREST.getWebAppAddress());
  }

  @Test
  public void testInvokeConcurrent() throws IOException, YarnException {

    // We design such a test case, we call the interceptor's getNodes interface,
    // this interface will generate the following test data
    // subCluster0 Node 0
    // subCluster1 Node 1
    // subCluster2 Node 2
    // subCluster3 Node 3
    // We use the returned data to verify whether the subClusterId
    // of the multi-thread call can match the node data
    Map<SubClusterInfo, NodesInfo> subClusterInfoNodesInfoMap =
        interceptor.invokeConcurrentGetNodeLabel();
    Assert.assertNotNull(subClusterInfoNodesInfoMap);
    Assert.assertEquals(4, subClusterInfoNodesInfoMap.size());

    subClusterInfoNodesInfoMap.forEach((subClusterInfo, nodesInfo) -> {
      String subClusterId = subClusterInfo.getSubClusterId().getId();
      List<NodeInfo> nodeInfos = nodesInfo.getNodes();
      Assert.assertNotNull(nodeInfos);
      Assert.assertEquals(1, nodeInfos.size());

      String expectNodeId = "Node " + subClusterId;
      String nodeId = nodeInfos.get(0).getNodeId();
      Assert.assertEquals(expectNodeId, nodeId);
    });
  }

  @Test
  public void testGetSchedulerInfo() {
    // In this test case, we will get the return results of 4 sub-clusters.
    SchedulerTypeInfo typeInfo = interceptor.getSchedulerInfo();
    Assert.assertNotNull(typeInfo);
    Assert.assertTrue(typeInfo instanceof FederationSchedulerTypeInfo);

    FederationSchedulerTypeInfo federationSchedulerTypeInfo =
        (FederationSchedulerTypeInfo) typeInfo;
    Assert.assertNotNull(federationSchedulerTypeInfo);
    List<SchedulerTypeInfo> schedulerTypeInfos = federationSchedulerTypeInfo.getList();
    Assert.assertNotNull(schedulerTypeInfos);
    Assert.assertEquals(4, schedulerTypeInfos.size());
    List<String> subClusterIds = subClusters.stream().map(SubClusterId::getId).
        collect(Collectors.toList());

    for (SchedulerTypeInfo schedulerTypeInfo : schedulerTypeInfos) {
      Assert.assertNotNull(schedulerTypeInfo);

      // 1. Whether the returned subClusterId is in the subCluster list
      String subClusterId = schedulerTypeInfo.getSubClusterId();
      Assert.assertTrue(subClusterIds.contains(subClusterId));

      // 2. We test CapacityScheduler, the returned type should be CapacityScheduler.
      SchedulerInfo schedulerInfo = schedulerTypeInfo.getSchedulerInfo();
      Assert.assertNotNull(schedulerInfo);
      Assert.assertTrue(schedulerInfo instanceof CapacitySchedulerInfo);
      CapacitySchedulerInfo capacitySchedulerInfo = (CapacitySchedulerInfo) schedulerInfo;
      Assert.assertNotNull(capacitySchedulerInfo);

      // 3. The parent queue name should be root
      String queueName = capacitySchedulerInfo.getQueueName();
      Assert.assertEquals("root", queueName);

      // 4. schedulerType should be CapacityScheduler
      String schedulerType = capacitySchedulerInfo.getSchedulerType();
      Assert.assertEquals("Capacity Scheduler", schedulerType);

      // 5. queue path should be root
      String queuePath = capacitySchedulerInfo.getQueuePath();
      Assert.assertEquals("root", queuePath);

      // 6. mockRM has 2 test queues, [root.a, root.b]
      List<String> queues = Lists.newArrayList("root.a", "root.b");
      CapacitySchedulerQueueInfoList csSchedulerQueueInfoList = capacitySchedulerInfo.getQueues();
      Assert.assertNotNull(csSchedulerQueueInfoList);
      List<CapacitySchedulerQueueInfo> csQueueInfoList =
          csSchedulerQueueInfoList.getQueueInfoList();
      Assert.assertEquals(2, csQueueInfoList.size());
      for (CapacitySchedulerQueueInfo csQueueInfo : csQueueInfoList) {
        Assert.assertNotNull(csQueueInfo);
        Assert.assertTrue(queues.contains(csQueueInfo.getQueuePath()));
      }
    }
  }

  @Test
  public void testPostDelegationTokenErrorHsr() throws Exception {
    // Prepare delegationToken data
    DelegationToken token = new DelegationToken();
    token.setRenewer(TEST_RENEWER);

    HttpServletRequest request = mock(HttpServletRequest.class);

    // If we don't set token
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, the tokenData or hsr is null.",
        () -> interceptor.postDelegationToken(null, request));

    // If we don't set hsr
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, the tokenData or hsr is null.",
        () -> interceptor.postDelegationToken(token, null));

    // If we don't set renewUser, we will get error message.
    LambdaTestUtils.intercept(AuthorizationException.class,
        "Unable to obtain user name, user not authenticated",
        () -> interceptor.postDelegationToken(token, request));

    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn(TEST_RENEWER);
    when(request.getRemoteUser()).thenReturn(TEST_RENEWER);
    when(request.getUserPrincipal()).thenReturn(principal);

    // If we don't set the authentication type, we will get error message.
    Response response = interceptor.postDelegationToken(token, request);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getStatus(), Status.FORBIDDEN.getStatusCode());
    String errMsg = "Delegation token operations can only be carried out on a " +
        "Kerberos authenticated channel. Expected auth type is kerberos, got type null";
    Object entity = response.getEntity();
    Assert.assertNotNull(entity);
    Assert.assertTrue(entity instanceof String);
    String entityMsg = String.valueOf(entity);
    Assert.assertTrue(errMsg.contains(entityMsg));
  }

  @Test
  public void testPostDelegationToken() throws Exception {
    Long now = Time.now();

    DelegationToken token = new DelegationToken();
    token.setRenewer(TEST_RENEWER);

    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn(TEST_RENEWER);

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(TEST_RENEWER);
    when(request.getUserPrincipal()).thenReturn(principal);
    when(request.getAuthType()).thenReturn("kerberos");

    Response response = interceptor.postDelegationToken(token, request);
    Assert.assertNotNull(response);

    Object entity = response.getEntity();
    Assert.assertNotNull(entity);
    Assert.assertTrue(entity instanceof DelegationToken);

    DelegationToken dtoken = (DelegationToken) entity;
    Assert.assertEquals(TEST_RENEWER, dtoken.getRenewer());
    Assert.assertEquals(TEST_RENEWER, dtoken.getOwner());
    Assert.assertEquals("RM_DELEGATION_TOKEN", dtoken.getKind());
    Assert.assertNotNull(dtoken.getToken());
    Assert.assertTrue(dtoken.getNextExpirationTime() > now);
  }

  @Test
  public void testPostDelegationTokenExpirationError() throws Exception {

    // If we don't set hsr
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, the hsr is null.",
        () -> interceptor.postDelegationTokenExpiration(null));

    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn(TEST_RENEWER);

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(TEST_RENEWER);
    when(request.getUserPrincipal()).thenReturn(principal);
    when(request.getAuthType()).thenReturn("kerberos");

    // If we don't set the header.
    String errorMsg = "Header 'Hadoop-YARN-RM-Delegation-Token' containing encoded token not found";
    LambdaTestUtils.intercept(BadRequestException.class, errorMsg,
        () -> interceptor.postDelegationTokenExpiration(request));
  }

  @Test
  public void testPostDelegationTokenExpiration() throws Exception {

    DelegationToken token = new DelegationToken();
    token.setRenewer(TEST_RENEWER);

    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn(TEST_RENEWER);

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(TEST_RENEWER);
    when(request.getUserPrincipal()).thenReturn(principal);
    when(request.getAuthType()).thenReturn("kerberos");

    Response response = interceptor.postDelegationToken(token, request);
    Assert.assertNotNull(response);
    Object entity = response.getEntity();
    Assert.assertNotNull(entity);
    Assert.assertTrue(entity instanceof DelegationToken);
    DelegationToken dtoken = (DelegationToken) entity;

    final String yarnTokenHeader = "Hadoop-YARN-RM-Delegation-Token";
    when(request.getHeader(yarnTokenHeader)).thenReturn(dtoken.getToken());

    Response renewResponse = interceptor.postDelegationTokenExpiration(request);
    Assert.assertNotNull(renewResponse);

    Object renewEntity = renewResponse.getEntity();
    Assert.assertNotNull(renewEntity);
    Assert.assertTrue(renewEntity instanceof DelegationToken);

    // renewDelegation, we only return renewDate, other values are NULL.
    DelegationToken renewDToken = (DelegationToken) renewEntity;
    Assert.assertNull(renewDToken.getRenewer());
    Assert.assertNull(renewDToken.getOwner());
    Assert.assertNull(renewDToken.getKind());
    Assert.assertTrue(renewDToken.getNextExpirationTime() > dtoken.getNextExpirationTime());
  }

  @Test
  public void testCancelDelegationToken() throws Exception {
    DelegationToken token = new DelegationToken();
    token.setRenewer(TEST_RENEWER);

    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn(TEST_RENEWER);

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(TEST_RENEWER);
    when(request.getUserPrincipal()).thenReturn(principal);
    when(request.getAuthType()).thenReturn("kerberos");

    Response response = interceptor.postDelegationToken(token, request);
    Assert.assertNotNull(response);
    Object entity = response.getEntity();
    Assert.assertNotNull(entity);
    Assert.assertTrue(entity instanceof DelegationToken);
    DelegationToken dtoken = (DelegationToken) entity;

    final String yarnTokenHeader = "Hadoop-YARN-RM-Delegation-Token";
    when(request.getHeader(yarnTokenHeader)).thenReturn(dtoken.getToken());

    Response cancelResponse = interceptor.cancelDelegationToken(request);
    Assert.assertNotNull(cancelResponse);
    Assert.assertEquals(response.getStatus(), Status.OK.getStatusCode());
  }

  @Test
  public void testReplaceLabelsOnNodes() throws Exception {
    // subCluster0 -> node0:0 -> label:NodeLabel0
    // subCluster1 -> node1:1 -> label:NodeLabel1
    // subCluster2 -> node2:2 -> label:NodeLabel2
    // subCluster3 -> node3:3 -> label:NodeLabel3
    NodeToLabelsEntryList nodeToLabelsEntryList = new NodeToLabelsEntryList();
    for (int i = 0; i < NUM_SUBCLUSTER; i++) {
      // labels
      List<String> labels = new ArrayList<>();
      labels.add("NodeLabel" + i);
      // nodes
      String nodeId = "node" + i + ":" + i;
      NodeToLabelsEntry nodeToLabelsEntry = new NodeToLabelsEntry(nodeId, labels);
      List<NodeToLabelsEntry> nodeToLabelsEntries = nodeToLabelsEntryList.getNodeToLabels();
      nodeToLabelsEntries.add(nodeToLabelsEntry);
    }

    // one of the results:
    // subCluster#0:Success;subCluster#1:Success;subCluster#3:Success;subCluster#2:Success;
    // We can't confirm the complete return order.
    Response response = interceptor.replaceLabelsOnNodes(nodeToLabelsEntryList, null);
    Assert.assertNotNull(response);
    Assert.assertEquals(200, response.getStatus());

    Object entityObject = response.getEntity();
    Assert.assertNotNull(entityObject);

    String entityValue = String.valueOf(entityObject);
    String[] entities = entityValue.split(",");
    Assert.assertNotNull(entities);
    Assert.assertEquals(4, entities.length);
    String expectValue =
        "subCluster-0:Success,subCluster-1:Success,subCluster-2:Success,subCluster-3:Success,";
    for (String entity : entities) {
      Assert.assertTrue(expectValue.contains(entity));
    }
  }

  @Test
  public void testReplaceLabelsOnNodesError() throws Exception {
    // newNodeToLabels is null
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, newNodeToLabels must not be empty.",
        () -> interceptor.replaceLabelsOnNodes(null, null));

    // nodeToLabelsEntryList is Empty
    NodeToLabelsEntryList nodeToLabelsEntryList = new NodeToLabelsEntryList();
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, nodeToLabelsEntries must not be empty.",
        () -> interceptor.replaceLabelsOnNodes(nodeToLabelsEntryList, null));
  }

  @Test
  public void testReplaceLabelsOnNode() throws Exception {
    // subCluster3 -> node3:3 -> label:NodeLabel3
    String nodeId = "node3:3";
    Set<String> labels = Collections.singleton("NodeLabel3");

    // We expect the following result: subCluster#3:Success;
    String expectValue = "subCluster#3:Success;";
    Response response = interceptor.replaceLabelsOnNode(labels, null, nodeId);
    Assert.assertNotNull(response);
    Assert.assertEquals(200, response.getStatus());

    Object entityObject = response.getEntity();
    Assert.assertNotNull(entityObject);

    String entityValue = String.valueOf(entityObject);
    Assert.assertNotNull(entityValue);
    Assert.assertEquals(expectValue, entityValue);
  }

  @Test
  public void testReplaceLabelsOnNodeError() throws Exception {
    // newNodeToLabels is null
    String nodeId = "node3:3";
    Set<String> labels = Collections.singleton("NodeLabel3");
    Set<String> labelsEmpty = new HashSet<>();

    // nodeId is null
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, nodeId must not be null or empty.",
        () -> interceptor.replaceLabelsOnNode(labels, null, null));

    // labels is null
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, newNodeLabelsName must not be empty.",
        () -> interceptor.replaceLabelsOnNode(null, null, nodeId));

    // labels is empty
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, newNodeLabelsName must not be empty.",
        () -> interceptor.replaceLabelsOnNode(labelsEmpty, null, nodeId));
  }

  @Test
  public void testDumpSchedulerLogs() throws Exception {
    HttpServletRequest mockHsr = mockHttpServletRequestByUserName("admin");
    String dumpSchedulerLogsMsg = interceptor.dumpSchedulerLogs("1", mockHsr);

    // We cannot guarantee the calling order of the sub-clusters,
    // We guarantee that the returned result contains the information of each subCluster.
    Assert.assertNotNull(dumpSchedulerLogsMsg);
    subClusters.forEach(subClusterId -> {
      String subClusterMsg =
          "subClusterId" + subClusterId + " : Capacity scheduler logs are being created.; ";
      Assert.assertTrue(dumpSchedulerLogsMsg.contains(subClusterMsg));
    });
  }

  @Test
  public void testDumpSchedulerLogsError() throws Exception {
    HttpServletRequest mockHsr = mockHttpServletRequestByUserName("admin");

    // time is empty
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, the time is empty or null.",
        () -> interceptor.dumpSchedulerLogs(null, mockHsr));

    // time is negative
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "time must be greater than 0.",
        () -> interceptor.dumpSchedulerLogs("-1", mockHsr));

    // time is non-numeric
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "time must be a number.",
        () -> interceptor.dumpSchedulerLogs("abc", mockHsr));
  }

  @Test
  public void testGetActivitiesNormal() {
    ActivitiesInfo activitiesInfo = interceptor.getActivities(null, "1", "DIAGNOSTIC");
    Assert.assertNotNull(activitiesInfo);

    String nodeId = activitiesInfo.getNodeId();
    Assert.assertNotNull(nodeId);
    Assert.assertEquals("1", nodeId);

    String diagnostic = activitiesInfo.getDiagnostic();
    Assert.assertNotNull(diagnostic);
    Assert.assertTrue(StringUtils.contains(diagnostic, "Diagnostic"));

    long timestamp = activitiesInfo.getTimestamp();
    Assert.assertEquals(1673081972L, timestamp);

    List<NodeAllocationInfo> allocationInfos = activitiesInfo.getAllocations();
    Assert.assertNotNull(allocationInfos);
    Assert.assertEquals(1, allocationInfos.size());
  }

  @Test
  public void testGetActivitiesError() throws Exception {
    // nodeId is empty
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "'nodeId' must not be empty.",
        () -> interceptor.getActivities(null, "", "DIAGNOSTIC"));

    // groupBy is empty
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "'groupBy' must not be empty.",
        () -> interceptor.getActivities(null, "1", ""));

    // groupBy value is wrong
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Got invalid groupBy: TEST1, valid groupBy types: [DIAGNOSTIC]",
        () -> interceptor.getActivities(null, "1", "TEST1"));
  }

  @Test
  public void testGetBulkActivitiesNormal() throws InterruptedException {
    BulkActivitiesInfo bulkActivitiesInfo =
        interceptor.getBulkActivities(null, "DIAGNOSTIC", 5);
    Assert.assertNotNull(bulkActivitiesInfo);

    Assert.assertTrue(bulkActivitiesInfo instanceof FederationBulkActivitiesInfo);

    FederationBulkActivitiesInfo federationBulkActivitiesInfo =
        (FederationBulkActivitiesInfo) bulkActivitiesInfo;
    Assert.assertNotNull(federationBulkActivitiesInfo);

    List<BulkActivitiesInfo> activitiesInfos = federationBulkActivitiesInfo.getList();
    Assert.assertNotNull(activitiesInfos);
    Assert.assertEquals(4, activitiesInfos.size());

    for (BulkActivitiesInfo activitiesInfo : activitiesInfos) {
      Assert.assertNotNull(activitiesInfo);
      List<ActivitiesInfo> activitiesInfoList = activitiesInfo.getActivities();
      Assert.assertNotNull(activitiesInfoList);
      Assert.assertEquals(5, activitiesInfoList.size());
    }
  }

  @Test
  public void testGetBulkActivitiesError() throws Exception {
    // activitiesCount < 0
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "'activitiesCount' must not be negative.",
        () -> interceptor.getBulkActivities(null, "DIAGNOSTIC", -1));

    // groupBy value is wrong
    LambdaTestUtils.intercept(YarnRuntimeException.class,
        "Got invalid groupBy: TEST1, valid groupBy types: [DIAGNOSTIC]",
        () -> interceptor.getBulkActivities(null, "TEST1", 1));

    // groupBy is empty
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "'groupBy' must not be empty.",
        () -> interceptor.getBulkActivities(null, "", 1));
  }

  @Test
  public void testAddToClusterNodeLabels1() throws Exception {
    // In this test, we try to add ALL label, all subClusters will return success.
    NodeLabelsInfo nodeLabelsInfo = new NodeLabelsInfo();
    NodeLabelInfo nodeLabelInfo = new NodeLabelInfo("ALL", true);
    nodeLabelsInfo.getNodeLabelsInfo().add(nodeLabelInfo);

    Response response = interceptor.addToClusterNodeLabels(nodeLabelsInfo, null);
    Assert.assertNotNull(response);

    Object entityObj = response.getEntity();
    Assert.assertNotNull(entityObj);

    String entity = String.valueOf(entityObj);
    String[] entities = StringUtils.split(entity, ",");
    Assert.assertNotNull(entities);
    Assert.assertEquals(4, entities.length);

    // The order in which the cluster returns messages is uncertain,
    // we confirm the result by contains
    String expectedMsg =
        "SubCluster-0:SUCCESS,SubCluster-1:SUCCESS,SubCluster-2:SUCCESS,SubCluster-3:SUCCESS";
    Arrays.stream(entities).forEach(item -> Assert.assertTrue(expectedMsg.contains(item)));
  }

  @Test
  public void testAddToClusterNodeLabels2() throws Exception {
    // In this test, we try to add A0 label,
    // subCluster0 will return success, and other subClusters will return null
    NodeLabelsInfo nodeLabelsInfo = new NodeLabelsInfo();
    NodeLabelInfo nodeLabelInfo = new NodeLabelInfo("A0", true);
    nodeLabelsInfo.getNodeLabelsInfo().add(nodeLabelInfo);

    Response response = interceptor.addToClusterNodeLabels(nodeLabelsInfo, null);
    Assert.assertNotNull(response);

    Object entityObj = response.getEntity();
    Assert.assertNotNull(entityObj);

    String expectedValue = "SubCluster-0:SUCCESS,";
    String entity = String.valueOf(entityObj);
    Assert.assertTrue(entity.contains(expectedValue));
  }

  @Test
  public void testAddToClusterNodeLabelsError() throws Exception {
    // the newNodeLabels is null
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, the newNodeLabels is null.",
        () -> interceptor.addToClusterNodeLabels(null, null));

    // the nodeLabelsInfo is null
    NodeLabelsInfo nodeLabelsInfo = new NodeLabelsInfo();
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, the nodeLabelsInfo is null or empty.",
        () -> interceptor.addToClusterNodeLabels(nodeLabelsInfo, null));

    // error nodeLabelsInfo
    NodeLabelsInfo nodeLabelsInfo1 = new NodeLabelsInfo();
    NodeLabelInfo nodeLabelInfo1 = new NodeLabelInfo("A", true);
    nodeLabelsInfo1.getNodeLabelsInfo().add(nodeLabelInfo1);
    LambdaTestUtils.intercept(YarnRuntimeException.class, "addToClusterNodeLabels Error",
        () -> interceptor.addToClusterNodeLabels(nodeLabelsInfo1, null));
  }

  @Test
  public void testRemoveFromClusterNodeLabels1() throws Exception {
    Set<String> oldNodeLabels = Sets.newHashSet();
    oldNodeLabels.add("ALL");

    Response response = interceptor.removeFromClusterNodeLabels(oldNodeLabels, null);
    Assert.assertNotNull(response);

    Object entityObj = response.getEntity();
    Assert.assertNotNull(entityObj);

    String entity = String.valueOf(entityObj);
    String[] entities = StringUtils.split(entity, ",");
    Assert.assertNotNull(entities);
    Assert.assertEquals(4, entities.length);

    // The order in which the cluster returns messages is uncertain,
    // we confirm the result by contains
    String expectedMsg =
        "SubCluster-0:SUCCESS,SubCluster-1:SUCCESS,SubCluster-2:SUCCESS,SubCluster-3:SUCCESS";
    Arrays.stream(entities).forEach(item -> Assert.assertTrue(expectedMsg.contains(item)));
  }

  @Test
  public void testRemoveFromClusterNodeLabels2() throws Exception {
    Set<String> oldNodeLabels = Sets.newHashSet();
    oldNodeLabels.add("A0");

    Response response = interceptor.removeFromClusterNodeLabels(oldNodeLabels, null);
    Assert.assertNotNull(response);

    Object entityObj = response.getEntity();
    Assert.assertNotNull(entityObj);

    String expectedValue = "SubCluster-0:SUCCESS,";
    String entity = String.valueOf(entityObj);
    Assert.assertTrue(entity.contains(expectedValue));
  }

  @Test
  public void testRemoveFromClusterNodeLabelsError() throws Exception {
    // the oldNodeLabels is null
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, the oldNodeLabels is null or empty.",
        () -> interceptor.removeFromClusterNodeLabels(null, null));

    // the oldNodeLabels is empty
    Set<String> oldNodeLabels = Sets.newHashSet();
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, the oldNodeLabels is null or empty.",
        () -> interceptor.removeFromClusterNodeLabels(oldNodeLabels, null));

    // error oldNodeLabels
    Set<String> oldNodeLabels1 = Sets.newHashSet();
    oldNodeLabels1.add("A1");
    LambdaTestUtils.intercept(YarnRuntimeException.class, "removeFromClusterNodeLabels Error",
        () -> interceptor.removeFromClusterNodeLabels(oldNodeLabels1, null));
  }

  @Test
  public void testGetSchedulerConfiguration() throws Exception {
    Response response = interceptor.getSchedulerConfiguration(null);
    Assert.assertNotNull(response);
    Assert.assertEquals(OK, response.getStatus());

    Object entity = response.getEntity();
    Assert.assertNotNull(entity);
    Assert.assertTrue(entity instanceof FederationConfInfo);

    FederationConfInfo federationConfInfo = FederationConfInfo.class.cast(entity);
    List<ConfInfo> confInfos = federationConfInfo.getList();
    Assert.assertNotNull(confInfos);
    Assert.assertEquals(4, confInfos.size());

    List<String> errors = federationConfInfo.getErrorMsgs();
    Assert.assertEquals(0, errors.size());

    Set<String> subClusterSet = subClusters.stream()
        .map(subClusterId -> subClusterId.getId()).collect(Collectors.toSet());

    for (ConfInfo confInfo : confInfos) {
      List<ConfInfo.ConfItem> confItems = confInfo.getItems();
      Assert.assertNotNull(confItems);
      Assert.assertTrue(confItems.size() > 0);
      Assert.assertTrue(subClusterSet.contains(confInfo.getSubClusterId()));
    }
  }

  @Test
  public void testGetClusterUserInfo() {
    String requestUserName = "test-user";
    HttpServletRequest hsr = mock(HttpServletRequest.class);
    when(hsr.getRemoteUser()).thenReturn(requestUserName);
    ClusterUserInfo clusterUserInfo = interceptor.getClusterUserInfo(hsr);

    Assert.assertNotNull(clusterUserInfo);
    Assert.assertTrue(clusterUserInfo instanceof FederationClusterUserInfo);

    FederationClusterUserInfo federationClusterUserInfo =
        (FederationClusterUserInfo) clusterUserInfo;

    List<ClusterUserInfo> fedClusterUserInfoList = federationClusterUserInfo.getList();
    Assert.assertNotNull(fedClusterUserInfoList);
    Assert.assertEquals(4, fedClusterUserInfoList.size());

    List<String> subClusterIds = subClusters.stream().map(
        subClusterId -> subClusterId.getId()).collect(Collectors.toList());
    MockRM mockRM = interceptor.getMockRM();

    for (ClusterUserInfo fedClusterUserInfo : fedClusterUserInfoList) {
      // Check subClusterId
      String subClusterId = fedClusterUserInfo.getSubClusterId();
      Assert.assertNotNull(subClusterId);
      Assert.assertTrue(subClusterIds.contains(subClusterId));

      // Check requestedUser
      String requestedUser = fedClusterUserInfo.getRequestedUser();
      Assert.assertNotNull(requestedUser);
      Assert.assertEquals(requestUserName, requestedUser);

      // Check rmLoginUser
      String rmLoginUser = fedClusterUserInfo.getRmLoginUser();
      Assert.assertNotNull(rmLoginUser);
      Assert.assertEquals(mockRM.getRMLoginUser(), rmLoginUser);
    }
  }

  @Test
  public void testUpdateSchedulerConfigurationErrorMsg() throws Exception {
    SchedConfUpdateInfo mutationInfo = new SchedConfUpdateInfo();
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, the subClusterId is empty or null.",
        () -> interceptor.updateSchedulerConfiguration(mutationInfo, null));

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Parameter error, the schedConfUpdateInfo is empty or null.",
        () -> interceptor.updateSchedulerConfiguration(null, null));
  }

  @Test
  public void testUpdateSchedulerConfiguration()
      throws AuthorizationException, InterruptedException {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.setSubClusterId("1");
    Map<String, String> goodUpdateMap = new HashMap<>();
    goodUpdateMap.put("goodKey", "goodVal");
    QueueConfigInfo goodUpdateInfo = new
        QueueConfigInfo("root.default", goodUpdateMap);
    updateInfo.getUpdateQueueInfo().add(goodUpdateInfo);
    Response response = interceptor.updateSchedulerConfiguration(updateInfo, null);

    Assert.assertNotNull(response);
    Assert.assertEquals(OK, response.getStatus());

    String expectMsg = "Configuration change successfully applied.";
    Object entity = response.getEntity();
    Assert.assertNotNull(entity);

    String entityMsg = String.valueOf(entity);
    Assert.assertEquals(expectMsg, entityMsg);
  }

  @Test
  public void testGetClusterInfo() {
    ClusterInfo clusterInfos = interceptor.getClusterInfo();
    Assert.assertNotNull(clusterInfos);
    Assert.assertTrue(clusterInfos instanceof FederationClusterInfo);

    FederationClusterInfo federationClusterInfos =
        (FederationClusterInfo) (clusterInfos);

    List<ClusterInfo> fedClusterInfosList = federationClusterInfos.getList();
    Assert.assertNotNull(fedClusterInfosList);
    Assert.assertEquals(4, fedClusterInfosList.size());

    List<String> subClusterIds = subClusters.stream().map(
        subClusterId -> subClusterId.getId()).collect(Collectors.toList());

    MockRM mockRM = interceptor.getMockRM();
    String yarnVersion = YarnVersionInfo.getVersion();

    for (ClusterInfo clusterInfo : fedClusterInfosList) {
      String subClusterId = clusterInfo.getSubClusterId();
      // Check subClusterId
      Assert.assertTrue(subClusterIds.contains(subClusterId));

      // Check state
      String clusterState = mockRM.getServiceState().toString();
      Assert.assertEquals(clusterState, clusterInfo.getState());

      // Check rmStateStoreName
      String rmStateStoreName =
          mockRM.getRMContext().getStateStore().getClass().getName();
      Assert.assertEquals(rmStateStoreName, clusterInfo.getRMStateStore());

      // Check RM Version
      Assert.assertEquals(yarnVersion, clusterInfo.getRMVersion());

      // Check haZooKeeperConnectionState
      String rmHAZookeeperConnectionState = mockRM.getRMContext().getHAZookeeperConnectionState();
      Assert.assertEquals(rmHAZookeeperConnectionState,
          clusterInfo.getHAZookeeperConnectionState());
    }
  }
}