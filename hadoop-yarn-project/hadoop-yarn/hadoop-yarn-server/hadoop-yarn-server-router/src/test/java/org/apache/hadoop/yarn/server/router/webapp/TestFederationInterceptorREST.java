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
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
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
import org.apache.hadoop.yarn.server.resourcemanager.webapp.NodeIDsInfo;
import org.apache.hadoop.yarn.server.router.webapp.cache.RouterAppInfoCacheKey;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDefinitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.router.webapp.MockDefaultRequestInterceptorREST.QUEUE_DEDICATED_FULL;

/**
 * Extends the {@code BaseRouterClientRMTest} and overrides methods in order to
 * use the {@code RouterClientRMService} pipeline test cases for testing the
 * {@code FederationInterceptor} class. The tests for
 * {@code RouterClientRMService} has been written cleverly so that it can be
 * reused to validate different request interceptor chains.
 */
public class TestFederationInterceptorREST extends BaseRouterWebServicesTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFederationInterceptorREST.class);
  private final static int NUM_SUBCLUSTER = 4;
  private static final int BAD_REQUEST = 400;
  private static final int ACCEPTED = 202;
  private static String user = "test-user";
  private TestableFederationInterceptorREST interceptor;
  private MemoryFederationStateStore stateStore;
  private FederationStateStoreTestUtil stateStoreUtil;
  private List<SubClusterId> subClusters;

  @Override
  public void setUp() {
    super.setUpConfig();
    interceptor = new TestableFederationInterceptorREST();

    stateStore = new MemoryFederationStateStore();
    stateStore.init(this.getConf());
    FederationStateStoreFacade.getInstance().reinitialize(stateStore,
        this.getConf());
    stateStoreUtil = new FederationStateStoreTestUtil(stateStore);

    interceptor.setConf(this.getConf());
    interceptor.init(user);

    subClusters = new ArrayList<>();

    try {
      for (int i = 0; i < NUM_SUBCLUSTER; i++) {
        SubClusterId sc = SubClusterId.newInstance(Integer.toString(i));
        stateStoreUtil.registerSubCluster(sc);
        subClusters.add(sc);
      }
    } catch (YarnException e) {
      LOG.error(e.getMessage());
      Assert.fail();
    }

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
  public void testGetNewApplication()
      throws YarnException, IOException, InterruptedException {

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
  public void testSubmitApplicationEmptyRequest()
      throws YarnException, IOException, InterruptedException {

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
  public void testSubmitApplicationWrongFormat()
      throws YarnException, IOException, InterruptedException {

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
  public void testGetApplicationNotExists()
      throws YarnException, IOException, InterruptedException {

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
  public void testGetApplicationWrongFormat()
      throws YarnException, IOException, InterruptedException {

    AppInfo response = interceptor.getApp(null, "Application_wrong_id", null);

    Assert.assertNull(response);
  }

  /**
   * This test validates the correctness of GetApplicationsReport in case each
   * subcluster provided one application.
   */
  @Test
  public void testGetApplicationsReport()
      throws YarnException, IOException, InterruptedException {

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
  public void testGetApplicationStateNotExists()
      throws YarnException, IOException, InterruptedException {

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
      throws YarnException, IOException, InterruptedException {

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
  public void testGetContainersNotExists() {
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ContainersInfo response = interceptor.getContainers(null, null, appId.toString(), null);
    Assert.assertTrue(response.getContainers().isEmpty());
  }

  @Test
  public void testGetContainersWrongFormat() {
    ContainersInfo response = interceptor.getContainers(null, null, "Application_wrong_id", null);

    Assert.assertNotNull(response);
    Assert.assertTrue(response.getContainers().isEmpty());

    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    response = interceptor.getContainers(null, null, appId.toString(), "AppAttempt_wrong_id");

    Assert.assertTrue(response.getContainers().isEmpty());
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
    NodeLabelsInfo nodeLabelsInfo2 = interceptor.getLabelsOnNode(null, "node2");
    Assert.assertNotNull(nodeLabelsInfo2);
    Assert.assertEquals(0, nodeLabelsInfo2.getNodeLabelsName().size());
  }

  @Test
  public void testGetContainer()
      throws IOException, InterruptedException, YarnException {
    // Submit application to multiSubCluster
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    Assert.assertNotNull(interceptor.submitApplication(context, null));

    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);

    ContainerInfo containerInfo = interceptor.getContainer(null, null,
        appId.toString(), appAttemptId.toString(), "0");
    Assert.assertNotNull(containerInfo);
  }

  @Test
  public void testGetAppAttempts()
      throws IOException, InterruptedException, YarnException {
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
  public void testGetAppAttempt()
      throws IOException, InterruptedException, YarnException {

    // Generate ApplicationId information
    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    ApplicationSubmissionContextInfo context = new ApplicationSubmissionContextInfo();
    context.setApplicationId(appId.toString());

    // Generate ApplicationAttemptId information
    Assert.assertNotNull(interceptor.submitApplication(context, null));
    ApplicationAttemptId expectAppAttemptId = ApplicationAttemptId.newInstance(appId, 1);

    org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo
        appAttemptInfo = interceptor.getAppAttempt(null, null, appId.toString(), "1");

    Assert.assertNotNull(appAttemptInfo);
    Assert.assertEquals(expectAppAttemptId.toString(), appAttemptInfo.getAppAttemptId());
    Assert.assertEquals("url", appAttemptInfo.getTrackingUrl());
    Assert.assertEquals("oUrl", appAttemptInfo.getOriginalTrackingUrl());
    Assert.assertEquals(124, appAttemptInfo.getRpcPort());
    Assert.assertEquals("host", appAttemptInfo.getHost());
  }

  @Test
  public void testGetAppTimeout() throws IOException, InterruptedException, YarnException {

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
  public void testGetAppTimeouts() throws IOException, InterruptedException, YarnException {

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
  public void testGetAppPriority() throws IOException, InterruptedException,
      YarnException {

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
  public void testGetAppQueue() throws IOException, InterruptedException, YarnException {
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
  public void testGetAppsInfoCache() throws IOException, InterruptedException, YarnException {

    AppsInfo responseGet = interceptor.getApps(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    Assert.assertNotNull(responseGet);

    RouterAppInfoCacheKey cacheKey = RouterAppInfoCacheKey.newInstance(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);

    LRUCacheHashMap<RouterAppInfoCacheKey, AppsInfo> appsInfoCache =
        interceptor.getAppInfosCaches();
    Assert.assertNotNull(appsInfoCache);
    Assert.assertTrue(!appsInfoCache.isEmpty());
    Assert.assertEquals(1, appsInfoCache.size());
    Assert.assertTrue(appsInfoCache.containsKey(cacheKey));

    AppsInfo cacheResult = appsInfoCache.get(cacheKey);
    Assert.assertNotNull(cacheResult);
    Assert.assertEquals(responseGet, cacheResult);
  }

  @Test
  public void testGetAppStatistics() throws IOException, InterruptedException, YarnException {
    AppState appStateRUNNING = new AppState(YarnApplicationState.RUNNING.name());

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

    // Add ReservationId In stateStore
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    SubClusterId homeSubClusterId = subClusters.get(0);
    ReservationHomeSubCluster reservationHomeSubCluster =
        ReservationHomeSubCluster.newInstance(reservationId, homeSubClusterId);
    AddReservationHomeSubClusterRequest request =
        AddReservationHomeSubClusterRequest.newInstance(reservationHomeSubCluster);
    stateStore.addReservationHomeSubCluster(request);

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
}
