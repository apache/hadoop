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
package org.apache.hadoop.yarn.server.router.subcluster.fair;

import static javax.servlet.http.HttpServletResponse.SC_ACCEPTED;
import static javax.servlet.http.HttpServletResponse.SC_SERVICE_UNAVAILABLE;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewReservation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.router.subcluster.TestFederationSubCluster;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationClusterInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationClusterUserInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationSchedulerTypeInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RM_WEB_SERVICE_PATH;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.INFO;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.CLUSTER_USER_INFO;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.METRICS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.SCHEDULER;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.NODES;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.STATES;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.NODES_NODEID;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.NODES_NODEID_REPLACE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.SCHEDULER_ACTIVITIES;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_NEW_APPLICATION;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APP_STATISTICS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APP_ID;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_APPID_APPATTEMPTS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_APPID_STATE;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_APPID_PRIORITY;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_APPID_QUEUE;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_TIMEOUTS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_TIMEOUT;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RESERVATION_NEW;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RESERVATION_SUBMIT;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RESERVATION_UPDATE;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RESERVATION_DELETE;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.GET_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.NODES_NODEID_GETLABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.LABEL_MAPPINGS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.ADD_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.GET_NODE_TO_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.REMOVE_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.REPLACE_NODE_TO_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.NODE_RESOURCE;
import static org.apache.hadoop.yarn.server.router.subcluster.TestFederationSubCluster.format;
import static org.apache.hadoop.yarn.server.router.webapp.HTTPMethods.POST;
import static org.apache.hadoop.yarn.server.router.webapp.HTTPMethods.PUT;
import static org.apache.http.HttpStatus.SC_OK;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestYarnFederationWithFairScheduler {
  private static TestFederationSubCluster testFederationSubCluster;
  private static Set<String> subClusters;
  private static final String ROUTER_WEB_ADDRESS = "http://localhost:28089";
  private static final String SC1_RM_WEB_ADDRESS = "http://localhost:38088";
  private static final String SC2_RM_WEB_ADDRESS = "http://localhost:48088";

  @BeforeClass
  public static void setUp()
      throws IOException, InterruptedException, YarnException, TimeoutException {
    testFederationSubCluster = new TestFederationSubCluster();
    testFederationSubCluster.startFederationSubCluster(2182,
        "38032,38030,38031,38088,38033,SC-1,127.0.0.1:2182,fair-scheduler",
        "48032,48030,48031,48088,48033,SC-2,127.0.0.1:2182,fair-scheduler",
        "28050,28052,28089,127.0.0.1:2182");
    subClusters = Sets.newHashSet();
    subClusters.add("SC-1");
    subClusters.add("SC-2");
  }

  @AfterClass
  public static void shutDown() throws Exception {
    testFederationSubCluster.stop();
  }

  @Test
  public void testGetClusterInfo() throws InterruptedException, IOException {
    FederationClusterInfo federationClusterInfo =
        TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS, RM_WEB_SERVICE_PATH,
        FederationClusterInfo.class, null, null);
    List<ClusterInfo> clusterInfos = federationClusterInfo.getList();
    assertNotNull(clusterInfos);
    assertEquals(2, clusterInfos.size());
    for (ClusterInfo clusterInfo : clusterInfos) {
      assertNotNull(clusterInfo);
      assertTrue(subClusters.contains(clusterInfo.getSubClusterId()));
    }
  }

  @Test
  public void testInfo() throws InterruptedException, IOException {
    FederationClusterInfo federationClusterInfo =
        TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS, RM_WEB_SERVICE_PATH + INFO,
        FederationClusterInfo.class, null, null);
    List<ClusterInfo> clusterInfos = federationClusterInfo.getList();
    assertNotNull(clusterInfos);
    assertEquals(2, clusterInfos.size());
    for (ClusterInfo clusterInfo : clusterInfos) {
      assertNotNull(clusterInfo);
      assertTrue(subClusters.contains(clusterInfo.getSubClusterId()));
    }
  }

  @Test
  public void testClusterUserInfo() throws Exception {
    FederationClusterUserInfo federationClusterUserInfo =
        TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + CLUSTER_USER_INFO,
        FederationClusterUserInfo.class, null, null);
    List<ClusterUserInfo> clusterUserInfos = federationClusterUserInfo.getList();
    assertNotNull(clusterUserInfos);
    assertEquals(2, clusterUserInfos.size());
    for (ClusterUserInfo clusterUserInfo : clusterUserInfos) {
      assertNotNull(clusterUserInfo);
      assertTrue(subClusters.contains(clusterUserInfo.getSubClusterId()));
    }
  }

  @Test
  public void testMetricsInfo() throws Exception {
    // It takes time to start the sub-cluster.
    // We need to wait for the sub-cluster to be completely started,
    // so we need to set the waiting time.
    // The resources of the two sub-clusters we registered are 24C and 12G,
    // so the resources that the Router should collect are 48C and 24G.
    GenericTestUtils.waitFor(() -> {
      try {
        ClusterMetricsInfo clusterMetricsInfo =
            TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
            RM_WEB_SERVICE_PATH + METRICS, ClusterMetricsInfo.class, null, null);
        assertNotNull(clusterMetricsInfo);
        return (48 == clusterMetricsInfo.getTotalVirtualCores() &&
            24576 == clusterMetricsInfo.getTotalMB());
      } catch (Exception e) {
        return false;
      }
    }, 5000, 50 * 5000);
  }

  @Test
  public void testSchedulerInfo() throws Exception {
    FederationSchedulerTypeInfo schedulerTypeInfo =
        TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + SCHEDULER, FederationSchedulerTypeInfo.class, null, null);
    assertNotNull(schedulerTypeInfo);
    List<SchedulerTypeInfo> schedulerTypeInfos = schedulerTypeInfo.getList();
    assertNotNull(schedulerTypeInfos);
    assertEquals(2, schedulerTypeInfos.size());
    for (SchedulerTypeInfo schedulerTypeInfoItem : schedulerTypeInfos) {
      assertNotNull(schedulerTypeInfoItem);
      assertTrue(subClusters.contains(schedulerTypeInfoItem.getSubClusterId()));
      FairSchedulerQueueInfo rootQueueInfo =
          ((FairSchedulerInfo) schedulerTypeInfoItem.getSchedulerInfo()).getRootQueueInfo();
      assertNotNull(rootQueueInfo);
      assertEquals("fair", rootQueueInfo.getSchedulingPolicy());
    }
  }

  @Test
  public void testNodesEmpty() throws Exception {
    // We are in 2 sub-clusters, each with 3 nodes, so our Router should correspond to 6 nodes.
    GenericTestUtils.waitFor(() -> {
      try {
        NodesInfo nodesInfo =
            TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
            RM_WEB_SERVICE_PATH + NODES, NodesInfo.class, null, null);
        assertNotNull(nodesInfo);
        ArrayList<NodeInfo> nodes = nodesInfo.getNodes();
        assertNotNull(nodes);
        return (6 == nodes.size());
      } catch (Exception e) {
        return false;
      }
    }, 5000, 50 * 5000);
  }

  @Test
  public void testNodesLost() throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        NodesInfo nodesInfo =
            TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
            RM_WEB_SERVICE_PATH + NODES, NodesInfo.class, STATES, "LOST");
        assertNotNull(nodesInfo);
        ArrayList<NodeInfo> nodes = nodesInfo.getNodes();
        assertNotNull(nodes);
        return nodes.isEmpty();
      } catch (Exception e) {
        return false;
      }
    }, 5000, 50 * 5000);
  }

  @Test
  public void testNode() throws Exception {
    String rm1NodeId = testFederationSubCluster.getNodeId(SC1_RM_WEB_ADDRESS);
    NodeInfo nodeInfo =
        TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(NODES_NODEID, rm1NodeId),
        NodeInfo.class, null, null);
    assertNotNull(nodeInfo);
    assertEquals(rm1NodeId, nodeInfo.getNodeId());

    String rm2NodeId = testFederationSubCluster.getNodeId(SC2_RM_WEB_ADDRESS);
    NodeInfo nodeInfo2 =
        TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(NODES_NODEID, rm2NodeId),
        NodeInfo.class, null, null);
    assertNotNull(nodeInfo2);
    assertEquals(rm2NodeId, nodeInfo2.getNodeId());
  }

  @Test
  public void testUpdateNodeResource() throws Exception {
    // wait until a node shows up and check the resources
    GenericTestUtils.waitFor(() -> testFederationSubCluster.getNodeId(SC1_RM_WEB_ADDRESS) != null,
        100, 5 * 1000);
    String rm1NodeId = testFederationSubCluster.getNodeId(SC1_RM_WEB_ADDRESS);

    // assert memory and default vcores
    NodeInfo nodeInfo = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(NODES_NODEID, rm1NodeId),
        NodeInfo.class, null, null);
    assertEquals(4096, nodeInfo.getTotalResource().getMemorySize());
    assertEquals(8, nodeInfo.getTotalResource().getvCores());

    Resource resource = Resource.newInstance(4096, 5);
    ResourceOptionInfo resourceOption = new ResourceOptionInfo(
        ResourceOption.newInstance(resource, 1000));
    ClientResponse routerResponse = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(NODE_RESOURCE, rm1NodeId),
        null, null, resourceOption, POST);
    JSONObject json = routerResponse.getEntity(JSONObject.class);
    JSONObject totalResource = json.getJSONObject("resourceInfo");
    assertEquals(resource.getMemorySize(), totalResource.getLong("memory"));
    assertEquals(resource.getVirtualCores(), totalResource.getLong("vCores"));

    // assert updated memory and cores
    NodeInfo nodeInfo1 = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(NODES_NODEID, rm1NodeId),
        NodeInfo.class, null, null);
    assertEquals(4096, nodeInfo1.getTotalResource().getMemorySize());
    assertEquals(5, nodeInfo1.getTotalResource().getvCores());
  }

  @Test
  public void testActivies() throws Exception {
    // wait until a node shows up and check the resources
    GenericTestUtils.waitFor(() -> testFederationSubCluster.getNodeId(SC1_RM_WEB_ADDRESS) != null,
        100, 5 * 1000);
    String rm1NodeId = testFederationSubCluster.getNodeId(SC1_RM_WEB_ADDRESS);

    ActivitiesInfo activitiesInfo = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + SCHEDULER_ACTIVITIES, ActivitiesInfo.class, "nodeId", rm1NodeId);

    assertNotNull(activitiesInfo);
    assertEquals(rm1NodeId, activitiesInfo.getNodeId());
    assertEquals("Not Capacity Scheduler", activitiesInfo.getDiagnostic());
  }

  @Test
  public void testAppActivities() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    AppActivitiesInfo appActivitiesInfo =
        TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + "/scheduler/app-activities/" + appId,
        AppActivitiesInfo.class, APP_ID, appId);
    assertNotNull(appActivitiesInfo);
    assertEquals(appId, appActivitiesInfo.getApplicationId());
    assertEquals("Not Capacity Scheduler", appActivitiesInfo.getDiagnostic());
  }

  @Test
  public void testAppStatistics() throws Exception {
    ApplicationStatisticsInfo applicationStatisticsInfo =
        TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + APP_STATISTICS, ApplicationStatisticsInfo.class, STATES, "RUNNING");
    assertNotNull(applicationStatisticsInfo);
    ArrayList<StatisticsItemInfo> statItems = applicationStatisticsInfo.getStatItems();
    assertNotNull(statItems);
    assertEquals(1, statItems.size());
  }

  @Test
  public void testNewApplication() throws Exception {
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + APPS_NEW_APPLICATION, null,
        null, null, POST);
    assertEquals(SC_OK, response.getStatus());
    NewApplication ci = response.getEntity(NewApplication.class);
    assertNotNull(ci);
  }

  @Test
  public void testSubmitApplication() {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    assertNotNull(appId);
  }

  @Test
  public void testApps() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    assertNotNull(appId);
    AppsInfo appsInfo = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + APPS, AppsInfo.class, null, null);
    assertNotNull(appsInfo);
    assertEquals(1, appsInfo.getApps().size());
  }

  @Test
  public void testAppAttempt() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    assertNotNull(appId);
    AppAttemptsInfo appAttemptsInfo = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(APPS_APPID_APPATTEMPTS, appId),
        AppAttemptsInfo.class, null, null);
    assertNotNull(appAttemptsInfo);
    ArrayList<AppAttemptInfo> attempts = appAttemptsInfo.getAttempts();
    assertNotNull(attempts);
    assertEquals(1, attempts.size());
  }

  @Test
  public void testAppState() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    assertNotNull(appId);
    AppState appState = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(APPS_APPID_STATE, appId),
        AppState.class, null, null);
    assertNotNull(appState);
    String state = appState.getState();
    assertNotNull(state);
    assertEquals("ACCEPTED", state);
  }

  @Test
  public void testUpdateAppState() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    assertNotNull(appId);
    AppState appState = new AppState("KILLED");
    String pathApp = RM_WEB_SERVICE_PATH + format(APPS_APPID_STATE, appId);
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        pathApp, null, null, appState, PUT);
    assertNotNull(response);
    assertEquals(SC_ACCEPTED, response.getStatus());
    AppState appState1 = response.getEntity(AppState.class);
    assertNotNull(appState1);
    assertNotNull(appState1.getState());
    assertEquals("KILLING", appState1.getState());
  }

  @Test
  public void testAppPriority() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    assertNotNull(appId);
    AppPriority appPriority = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(APPS_APPID_PRIORITY, appId),
        AppPriority.class, null, null);
    assertNotNull(appPriority);
    assertEquals(0, appPriority.getPriority());
  }

  @Test
  public void testUpdateAppPriority() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    AppPriority appPriority = new AppPriority(1);
    // FairScheduler does not support Update Application Priority.
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(APPS_APPID_PRIORITY, appId),
        null, null, appPriority, PUT);
    assertEquals(SC_SERVICE_UNAVAILABLE, response.getStatus());
  }

  @Test
  public void testAppQueue() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    AppQueue appQueue = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(APPS_APPID_QUEUE, appId),
        AppQueue.class, null, null);
    assertNotNull(appQueue);
    String queue = appQueue.getQueue();
    assertEquals("root.dr_dot_who", queue);
  }

  @Test
  public void testUpdateAppQueue() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    AppQueue appQueue = new AppQueue("root.a");
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(APPS_APPID_QUEUE, appId),
        null, null, appQueue, PUT);
    assertEquals(SC_OK, response.getStatus());
    AppQueue appQueue1 = response.getEntity(AppQueue.class);
    assertNotNull(appQueue1);
    String queue1 = appQueue1.getQueue();
    assertEquals("root.a", queue1);
  }

  @Test
  public void testAppTimeouts() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    AppTimeoutsInfo appTimeoutsInfo = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(APPS_TIMEOUTS, appId),
        AppTimeoutsInfo.class, null, null);
    assertNotNull(appTimeoutsInfo);
    ArrayList<AppTimeoutInfo> appTimeouts = appTimeoutsInfo.getAppTimeouts();
    assertNotNull(appTimeouts);
    assertEquals(1, appTimeouts.size());
    AppTimeoutInfo appTimeoutInfo = appTimeouts.get(0);
    assertNotNull(appTimeoutInfo);
    assertEquals(ApplicationTimeoutType.LIFETIME, appTimeoutInfo.getTimeoutType());
    assertEquals("UNLIMITED", appTimeoutInfo.getExpireTime());
  }

  @Test
  public void testAppTimeout() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    String pathApp = RM_WEB_SERVICE_PATH + format(APPS_TIMEOUTS, appId);
    AppTimeoutInfo appTimeoutInfo = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        pathApp + "/" + "LIFETIME", AppTimeoutInfo.class, null, null);
    assertNotNull(appTimeoutInfo);
  }

  @Test
  public void testUpdateAppTimeouts() throws Exception {
    String appId = testFederationSubCluster.submitApplication(ROUTER_WEB_ADDRESS);
    AppTimeoutInfo appTimeoutInfo = new AppTimeoutInfo();
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(APPS_TIMEOUT, appId),
        null, null, appTimeoutInfo, PUT);
    assertEquals(SC_BAD_REQUEST, response.getStatus());
    String entity = response.getEntity(String.class);
    assertNotNull(entity);
  }

  @Test
  public void testNewReservation() throws Exception {
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + RESERVATION_NEW,
        null, null, null, POST);
    assertEquals(SC_OK, response.getStatus());
    NewReservation ci = response.getEntity(NewReservation.class);
    assertNotNull(ci);
  }

  @Test
  public void testSubmitReservation() throws Exception {
    ReservationSubmissionRequestInfo context = new ReservationSubmissionRequestInfo();
    NewReservation newReservationId =
        testFederationSubCluster.getNewReservationId(ROUTER_WEB_ADDRESS);
    context.setReservationId(newReservationId.getReservationId());
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + RESERVATION_SUBMIT, null, null, context, POST);
    assertEquals(SC_BAD_REQUEST, response.getStatus());
    String entity = response.getEntity(String.class);
    assertNotNull(entity);
  }

  @Test
  public void testUpdateReservation() throws Exception {
    NewReservation newReservationId =
        testFederationSubCluster.getNewReservationId(ROUTER_WEB_ADDRESS);
    String reservationId = newReservationId.getReservationId();
    ReservationUpdateRequestInfo context = new ReservationUpdateRequestInfo();
    context.setReservationId(reservationId);
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + RESERVATION_UPDATE, null, null, context, POST);
    assertEquals(SC_BAD_REQUEST, response.getStatus());
    String entity = response.getEntity(String.class);
    assertNotNull(entity);
  }

  @Test
  public void testDeleteReservation() throws Exception {
    NewReservation newReservationId =
        testFederationSubCluster.getNewReservationId(ROUTER_WEB_ADDRESS);
    String reservationId = newReservationId.getReservationId();
    ReservationDeleteRequestInfo context = new ReservationDeleteRequestInfo();
    context.setReservationId(reservationId);
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + RESERVATION_DELETE, null, null, context, POST);
    assertEquals(SC_SERVICE_UNAVAILABLE, response.getStatus());
    String entity = response.getEntity(String.class);
    assertNotNull(entity);
  }

  @Test
  public void testGetClusterNodeLabels() throws Exception {
    NodeLabelsInfo nodeLabelsInfo = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + GET_NODE_LABELS, NodeLabelsInfo.class, null, null);
    assertNotNull(nodeLabelsInfo);
  }

  @Test
  public void testGetLabelsOnNode() throws Exception {
    String rm1NodeId = testFederationSubCluster.getNodeId(SC1_RM_WEB_ADDRESS);
    NodeLabelsInfo nodeLabelsInfo = TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + format(NODES_NODEID_GETLABELS, rm1NodeId),
        NodeLabelsInfo.class, null, null);
    assertNotNull(nodeLabelsInfo);
  }

  @Test
  public void testGetLabelsMappingEmpty() throws Exception {
    LabelsToNodesInfo labelsToNodesInfo =
        TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + LABEL_MAPPINGS, LabelsToNodesInfo.class, null, null);
    assertNotNull(labelsToNodesInfo);
  }

  @Test
  public void testGetLabelsMapping() throws Exception {
    LabelsToNodesInfo labelsToNodesInfo = TestFederationSubCluster.performGetCalls(
        ROUTER_WEB_ADDRESS, RM_WEB_SERVICE_PATH + LABEL_MAPPINGS,
        LabelsToNodesInfo.class, LABELS, "label1");
    assertNotNull(labelsToNodesInfo);
  }

  @Test
  public void testAddToClusterNodeLabels() throws Exception {
    List<NodeLabel> nodeLabels = new ArrayList<>();
    nodeLabels.add(NodeLabel.newInstance("default"));
    NodeLabelsInfo context = new NodeLabelsInfo(nodeLabels);
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + ADD_NODE_LABELS, null, null, context, POST);
    assertEquals(SC_OK, response.getStatus());
    String entity = response.getEntity(String.class);
    assertNotNull(entity);
  }

  @Test
  public void testGetNodeToLabels() throws Exception {
    NodeToLabelsInfo nodeToLabelsInfo = TestFederationSubCluster.performGetCalls(
        ROUTER_WEB_ADDRESS, RM_WEB_SERVICE_PATH + GET_NODE_TO_LABELS,
        NodeToLabelsInfo.class, null, null);
    assertNotNull(nodeToLabelsInfo);
  }

  @Test
  public void testRemoveFromClusterNodeLabels() throws Exception {
    testFederationSubCluster.addNodeLabel(ROUTER_WEB_ADDRESS);
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + REMOVE_NODE_LABELS,
        LABELS, "default", null, POST);
    assertEquals(SC_OK, response.getStatus());
    String entity = response.getEntity(String.class);
    assertNotNull(entity);
  }

  @Test
  public void testReplaceLabelsOnNodes() throws Exception {
    testFederationSubCluster.addNodeLabel(ROUTER_WEB_ADDRESS);
    NodeToLabelsEntryList context = new NodeToLabelsEntryList();
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        RM_WEB_SERVICE_PATH + REPLACE_NODE_TO_LABELS,
        null, null, context, POST);
    String entity = response.getEntity(String.class);
    assertNotNull(entity);
  }

  @Test
  public void testReplaceLabelsOnNode() throws Exception {
    String rm1NodeId = testFederationSubCluster.getNodeId(SC1_RM_WEB_ADDRESS);
    String pathNode = RM_WEB_SERVICE_PATH +
        format(NODES_NODEID_REPLACE_LABELS, rm1NodeId);
    testFederationSubCluster.addNodeLabel(ROUTER_WEB_ADDRESS);
    ClientResponse response = TestFederationSubCluster.performCall(ROUTER_WEB_ADDRESS,
        pathNode, LABELS, "default", null, POST);
    assertEquals(SC_OK, response.getStatus());
    String entity = response.getEntity(String.class);
    assertNotNull(entity);
  }
}
