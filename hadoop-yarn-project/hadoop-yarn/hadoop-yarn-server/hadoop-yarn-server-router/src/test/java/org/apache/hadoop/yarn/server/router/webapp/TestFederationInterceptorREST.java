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

import javax.ws.rs.core.Response;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
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
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends the {@code BaseRouterClientRMTest} and overrides methods in order to
 * use the {@code RouterClientRMService} pipeline test cases for testing the
 * {@code FederationInterceptor} class. The tests for
 * {@code RouterClientRMService} has been written cleverly so that it can be
 * reused to validate different request intercepter chains.
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

    // Create a request intercepter pipeline for testing. The last one in the
    // chain is the federation intercepter that calls the mock resource manager.
    // The others in the chain will simply forward it to the next one in the
    // chain
    conf.set(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + ","
            + TestableFederationInterceptorREST.class.getName());

    conf.set(YarnConfiguration.FEDERATION_POLICY_MANAGER,
        UniformBroadcastPolicyManager.class.getName());

    // Disable StateStoreFacade cache
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);

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
        ApplicationId.newInstance(System.currentTimeMillis(), 1);

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
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
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
   * This test validates the correctness of SubmitApplication in case of of
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
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
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
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
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
        ApplicationId.newInstance(System.currentTimeMillis(), 1);

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
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
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
        null, null, null, null, null, null, null, null, null);

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
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
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
        ApplicationId.newInstance(System.currentTimeMillis(), 1);

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

}
