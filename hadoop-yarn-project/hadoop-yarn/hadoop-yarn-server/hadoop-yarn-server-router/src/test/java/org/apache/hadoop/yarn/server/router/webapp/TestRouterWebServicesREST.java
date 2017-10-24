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

package org.apache.hadoop.yarn.server.router.webapp;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServiceProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewReservation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.codehaus.jettison.json.JSONException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;

import net.jcip.annotations.NotThreadSafe;

/**
 * This test validate E2E the correctness of the RouterWebServices. It starts
 * Router, RM and NM in 3 different processes to avoid servlet conflicts. Each
 * test creates a REST call to Router and validate that the operation complete
 * successfully.
 */
@NotThreadSafe
public class TestRouterWebServicesREST {

  private static String userName = "test";

  private static JavaProcess rm;
  private static JavaProcess nm;
  private static JavaProcess router;

  private static Configuration conf;

  private static final int STATUS_OK = 200;
  private static final int STATUS_ACCEPTED = 202;
  private static final int STATUS_BADREQUEST = 400;
  private static final int STATUS_ERROR = 500;

  /**
   * Wait until the webservice is up and running.
   */
  private static void waitWebAppRunning(String address, String path) {
    while (true) {
      Client clientToRouter = Client.create();
      WebResource toRouter = clientToRouter.resource(address).path(path);
      try {
        ClientResponse response = toRouter.accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
        if (response.getStatus() == STATUS_OK) {
          // process is up and running
          return;
        }
      } catch (ClientHandlerException e) {
        // process is not up and running
        continue;
      }
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new YarnConfiguration();
    rm = new JavaProcess(ResourceManager.class);
    router = new JavaProcess(Router.class);
    nm = new JavaProcess(NodeManager.class);

    // The tests cannot start if all the service are not up and running.
    waitWebAppRunning(WebAppUtils.getRMWebAppURLWithScheme(conf),
        RMWSConsts.RM_WEB_SERVICE_PATH);

    waitWebAppRunning(WebAppUtils.getRouterWebAppURLWithScheme(conf),
        RMWSConsts.RM_WEB_SERVICE_PATH);

    waitWebAppRunning("http://" + WebAppUtils.getNMWebAppURLWithoutScheme(conf),
        "/ws/v1/node");
  }

  @AfterClass
  public static void stop() throws Exception {
    nm.stop();
    router.stop();
    rm.stop();
  }

  /**
   * Performs 2 GET calls one to RM and the one to Router. In positive case, it
   * returns the 2 answers in a list.
   */
  private static <T> List<T> performGetCalls(String path,
      final Class<T> returnType, String queryName, String queryValue)
      throws IOException, InterruptedException {
    Client clientToRouter = Client.create();
    WebResource toRouter = clientToRouter
        .resource(WebAppUtils.getRouterWebAppURLWithScheme(conf)).path(path);

    Client clientToRM = Client.create();
    WebResource toRM = clientToRM
        .resource(WebAppUtils.getRMWebAppURLWithScheme(conf)).path(path);

    final Builder toRouterBuilder;
    final Builder toRMBuilder;

    if (queryValue != null && queryName != null) {
      toRouterBuilder = toRouter.queryParam(queryName, queryValue)
          .accept(MediaType.APPLICATION_XML);
      toRMBuilder = toRM.queryParam(queryName, queryValue)
          .accept(MediaType.APPLICATION_XML);
    } else {
      toRouterBuilder = toRouter.accept(MediaType.APPLICATION_XML);
      toRMBuilder = toRM.accept(MediaType.APPLICATION_XML);
    }

    return UserGroupInformation.createRemoteUser(userName)
        .doAs(new PrivilegedExceptionAction<List<T>>() {
          @Override
          public List<T> run() throws Exception {
            ClientResponse response = toRouterBuilder.get(ClientResponse.class);
            ClientResponse response2 = toRMBuilder.get(ClientResponse.class);
            if (response.getStatus() == STATUS_OK
                && response2.getStatus() == STATUS_OK) {
              List<T> responses = new ArrayList<T>();
              responses.add(response.getEntity(returnType));
              responses.add(response2.getEntity(returnType));
              return responses;
            } else {
              Assert.fail();
            }
            return null;
          }
        });
  }

  /**
   * Performs a POST/PUT/DELETE call to Router and returns the ClientResponse.
   */
  private static ClientResponse performCall(final String webAddress,
      final String queryKey, final String queryValue, final Object context,
      final HTTPMethods method) throws IOException, InterruptedException {

    return UserGroupInformation.createRemoteUser(userName)
        .doAs(new PrivilegedExceptionAction<ClientResponse>() {
          @Override
          public ClientResponse run() throws Exception {
            Client clientToRouter = Client.create();
            WebResource toRouter = clientToRouter
                .resource(WebAppUtils.getRouterWebAppURLWithScheme(conf))
                .path(webAddress);

            WebResource toRouterWR;
            if (queryKey != null && queryValue != null) {
              toRouterWR = toRouter.queryParam(queryKey, queryValue);
            } else {
              toRouterWR = toRouter;
            }

            Builder builder = null;
            if (context != null) {
              builder = toRouterWR.entity(context, MediaType.APPLICATION_JSON);
              builder = builder.accept(MediaType.APPLICATION_JSON);
            } else {
              builder = toRouter.accept(MediaType.APPLICATION_JSON);
            }

            ClientResponse response = null;

            switch (method) {
            case DELETE:
              response = builder.delete(ClientResponse.class);
              break;
            case POST:
              response = builder.post(ClientResponse.class);
              break;
            case PUT:
              response = builder.put(ClientResponse.class);
              break;
            default:
              break;
            }

            return response;
          }
        });
  }

  /**
   * This test validates the correctness of {@link RMWebServiceProtocol#get()}
   * inside Router.
   */
  @Test(timeout = 1000)
  public void testInfoXML() throws JSONException, Exception {

    List<ClusterInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH, ClusterInfo.class, null, null);

    ClusterInfo routerResponse = responses.get(0);
    ClusterInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getRMVersion(),
        routerResponse.getRMVersion());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getClusterInfo()} inside Router.
   */
  @Test(timeout = 1000)
  public void testClusterInfoXML() throws JSONException, Exception {

    List<ClusterInfo> responses =
        performGetCalls(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.INFO,
            ClusterInfo.class, null, null);

    ClusterInfo routerResponse = responses.get(0);
    ClusterInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getRMVersion(),
        routerResponse.getRMVersion());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getClusterMetricsInfo()} inside Router.
   */
  @Test(timeout = 1000)
  public void testMetricsInfoXML() throws JSONException, Exception {

    List<ClusterMetricsInfo> responses =
        performGetCalls(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS,
            ClusterMetricsInfo.class, null, null);

    ClusterMetricsInfo routerResponse = responses.get(0);
    ClusterMetricsInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getActiveNodes(),
        routerResponse.getActiveNodes());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getSchedulerInfo()} inside Router.
   */
  @Test(timeout = 1000)
  public void testSchedulerInfoXML() throws JSONException, Exception {

    List<SchedulerTypeInfo> responses =
        performGetCalls(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER,
            SchedulerTypeInfo.class, null, null);

    SchedulerTypeInfo routerResponse = responses.get(0);
    SchedulerTypeInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getSchedulerInfo().getSchedulerType(),
        routerResponse.getSchedulerInfo().getSchedulerType());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getNodes()} inside Router.
   */
  @Test(timeout = 1000)
  public void testNodesXML() throws JSONException, Exception {

    List<NodesInfo> responses =
        performGetCalls(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES,
            NodesInfo.class, RMWSConsts.STATES, "LOST");

    NodesInfo routerResponse = responses.get(0);
    NodesInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getNodes().size(),
        routerResponse.getNodes().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getNode()} inside Router.
   */
  @Test(timeout = 1000)
  public void testNodeXML() throws JSONException, Exception {

    List<NodeInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES + "/" + getNodeId(),
        NodeInfo.class, null, null);

    NodeInfo routerResponse = responses.get(0);
    NodeInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getVersion(), routerResponse.getVersion());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getActivities()} inside Router.
   */
  @Test(timeout = 1000)
  public void testActiviesXML() throws JSONException, Exception {

    List<ActivitiesInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_ACTIVITIES,
        ActivitiesInfo.class, null, null);

    ActivitiesInfo routerResponse = responses.get(0);
    ActivitiesInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppActivities()} inside Router.
   */
  @Test(timeout = 1000)
  public void testAppActivitiesXML() throws JSONException, Exception {

    String appId = submitApplication();

    List<AppActivitiesInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_APP_ACTIVITIES,
        AppActivitiesInfo.class, RMWSConsts.APP_ID, appId);

    AppActivitiesInfo routerResponse = responses.get(0);
    AppActivitiesInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppStatistics()} inside Router.
   */
  @Test(timeout = 1000)
  public void testAppStatisticsXML() throws JSONException, Exception {

    submitApplication();

    List<ApplicationStatisticsInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APP_STATISTICS,
        ApplicationStatisticsInfo.class, RMWSConsts.STATES, "RUNNING");

    ApplicationStatisticsInfo routerResponse = responses.get(0);
    ApplicationStatisticsInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getStatItems().size(),
        routerResponse.getStatItems().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#dumpSchedulerLogs()} inside Router.
   */
  @Test(timeout = 1000)
  public void testDumpSchedulerLogsXML() throws JSONException, Exception {

    // Test with a wrong HTTP method
    ClientResponse badResponse =
        performCall(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_LOGS,
            null, null, null, HTTPMethods.PUT);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    ClientResponse response =
        performCall(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_LOGS,
            RMWSConsts.TIME, "1", null, HTTPMethods.POST);

    if (response.getStatus() != HttpServletResponse.SC_NO_CONTENT) {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#createNewApplication()} inside Router.
   */
  @Test(timeout = 1000)
  public void testNewApplicationXML() throws JSONException, Exception {

    // Test with a wrong HTTP method
    ClientResponse badResponse = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS_NEW_APPLICATION, null,
        null, null, HTTPMethods.PUT);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    ClientResponse response = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS_NEW_APPLICATION, null,
        null, null, HTTPMethods.POST);

    if (response.getStatus() == STATUS_OK) {
      NewApplication ci = response.getEntity(NewApplication.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }

  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#submitApplication()} inside Router.
   */
  @Test(timeout = 1000)
  public void testSubmitApplicationXML() throws JSONException, Exception {

    // Test with a wrong HTTP method
    ClientResponse badResponse =
        performCall(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS, null,
            null, null, HTTPMethods.PUT);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    context.setApplicationId(getNewApplicationId().getApplicationId());

    ClientResponse response =
        performCall(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS, null,
            null, context, HTTPMethods.POST);

    if (response.getStatus() == STATUS_ACCEPTED) {
      String ci = response.getEntity(String.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }

  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getApps()} inside Router.
   */
  @Test(timeout = 1000)
  public void testAppsXML() throws JSONException, Exception {

    submitApplication();

    List<AppsInfo> responses =
        performGetCalls(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS,
            AppsInfo.class, null, null);

    AppsInfo routerResponse = responses.get(0);
    AppsInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getApps().size(),
        rmResponse.getApps().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getApp()} inside Router.
   */
  @Test(timeout = 1000)
  public void testAppXML() throws JSONException, Exception {

    String appId = submitApplication();

    List<AppInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId,
        AppInfo.class, null, null);

    AppInfo routerResponse = responses.get(0);
    AppInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getAMHostHttpAddress(),
        rmResponse.getAMHostHttpAddress());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppAttempts()} inside Router.
   */
  @Test(timeout = 1000)
  public void testAppAttemptXML() throws JSONException, Exception {

    String appId = submitApplication();

    List<AppAttemptsInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId + "/"
            + RMWSConsts.ATTEMPTS,
        AppAttemptsInfo.class, null, null);

    AppAttemptsInfo routerResponse = responses.get(0);
    AppAttemptsInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getAttempts().size(),
        rmResponse.getAttempts().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppState()} inside Router.
   */
  @Test(timeout = 1000)
  public void testAppStateXML() throws JSONException, Exception {

    String appId = submitApplication();

    List<AppState> responses =
        performGetCalls(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/"
            + appId + "/" + RMWSConsts.STATE, AppState.class, null, null);

    AppState routerResponse = responses.get(0);
    AppState rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getState(), rmResponse.getState());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#updateAppState()} inside Router.
   */
  @Test(timeout = 1000)
  public void testUpdateAppStateXML() throws JSONException, Exception {

    String appId = submitApplication();

    // Test with a wrong HTTP method
    ClientResponse badResponse = performCall(RMWSConsts.RM_WEB_SERVICE_PATH
        + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.STATE, null, null,
        null, HTTPMethods.POST);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    AppState appState = new AppState("KILLED");

    ClientResponse response = performCall(RMWSConsts.RM_WEB_SERVICE_PATH
        + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.STATE, null, null,
        appState, HTTPMethods.PUT);

    if (response.getStatus() == STATUS_ACCEPTED) {
      AppState ci = response.getEntity(AppState.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppPriority()} inside Router.
   */
  @Test(timeout = 1000)
  public void testAppPriorityXML() throws JSONException, Exception {

    String appId = submitApplication();

    List<AppPriority> responses =
        performGetCalls(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/"
            + appId + "/" + RMWSConsts.PRIORITY, AppPriority.class, null, null);

    AppPriority routerResponse = responses.get(0);
    AppPriority rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getPriority(), rmResponse.getPriority());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#updateApplicationPriority()} inside Router.
   */
  @Test(timeout = 1000)
  public void testUpdateAppPriorityXML() throws JSONException, Exception {

    String appId = submitApplication();

    // Test with a wrong HTTP method
    ClientResponse badResponse = performCall(RMWSConsts.RM_WEB_SERVICE_PATH
        + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.PRIORITY, null, null,
        null, HTTPMethods.POST);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    AppPriority appPriority = new AppPriority(1);

    ClientResponse response =
        performCall(
            RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId + "/"
                + RMWSConsts.PRIORITY,
            null, null, appPriority, HTTPMethods.PUT);

    if (response.getStatus() == STATUS_OK) {
      AppPriority ci = response.getEntity(AppPriority.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppQueue()} inside Router.
   */
  @Test(timeout = 1000)
  public void testAppQueueXML() throws JSONException, Exception {

    String appId = submitApplication();

    List<AppQueue> responses =
        performGetCalls(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/"
            + appId + "/" + RMWSConsts.QUEUE, AppQueue.class, null, null);

    AppQueue routerResponse = responses.get(0);
    AppQueue rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getQueue(), rmResponse.getQueue());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#updateAppQueue()} inside Router.
   */
  @Test(timeout = 1000)
  public void testUpdateAppQueueXML() throws JSONException, Exception {

    String appId = submitApplication();

    // Test with a wrong HTTP method
    ClientResponse badResponse = performCall(RMWSConsts.RM_WEB_SERVICE_PATH
        + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.QUEUE, null, null,
        null, HTTPMethods.POST);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    AppQueue appQueue = new AppQueue("default");

    ClientResponse response = performCall(RMWSConsts.RM_WEB_SERVICE_PATH
        + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.QUEUE, null, null,
        appQueue, HTTPMethods.PUT);

    if (response.getStatus() == STATUS_OK) {
      AppQueue ci = response.getEntity(AppQueue.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppTimeouts()} inside Router.
   */
  @Test(timeout = 1000)
  public void testAppTimeoutsXML() throws JSONException, Exception {

    String appId = submitApplication();

    List<AppTimeoutsInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId + "/"
            + RMWSConsts.TIMEOUTS,
        AppTimeoutsInfo.class, null, null);

    AppTimeoutsInfo routerResponse = responses.get(0);
    AppTimeoutsInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getAppTimeouts().size(),
        rmResponse.getAppTimeouts().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppTimeout()} inside Router.
   */
  @Test(timeout = 1000)
  public void testAppTimeoutXML() throws JSONException, Exception {

    String appId = submitApplication();

    List<AppTimeoutInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId + "/"
            + RMWSConsts.TIMEOUTS + "/" + "LIFETIME",
        AppTimeoutInfo.class, null, null);

    AppTimeoutInfo routerResponse = responses.get(0);
    AppTimeoutInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getExpireTime(), rmResponse.getExpireTime());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#updateApplicationTimeout()} inside Router.
   */
  @Test(timeout = 1000)
  public void testUpdateAppTimeoutsXML() throws JSONException, Exception {

    String appId = submitApplication();

    // Test with a wrong HTTP method
    ClientResponse badResponse = performCall(RMWSConsts.RM_WEB_SERVICE_PATH
        + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.TIMEOUT, null, null,
        null, HTTPMethods.POST);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    // Create a bad request
    AppTimeoutInfo appTimeoutInfo = new AppTimeoutInfo();

    ClientResponse response =
        performCall(
            RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId + "/"
                + RMWSConsts.TIMEOUT,
            null, null, appTimeoutInfo, HTTPMethods.PUT);

    if (response.getStatus() == STATUS_BADREQUEST) {
      String ci = response.getEntity(String.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#createNewReservation()} inside Router.
   */
  @Test(timeout = 1000)
  public void testNewReservationXML() throws JSONException, Exception {

    // Test with a wrong HTTP method
    ClientResponse badResponse =
        performCall(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_NEW,
            null, null, null, HTTPMethods.PUT);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    ClientResponse response =
        performCall(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_NEW,
            null, null, null, HTTPMethods.POST);

    if (response.getStatus() == STATUS_OK) {
      NewReservation ci = response.getEntity(NewReservation.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#submitReservation()} inside Router.
   */
  @Test(timeout = 1000)
  public void testSubmitReservationXML() throws JSONException, Exception {

    // Test with a wrong HTTP method
    ClientResponse badResponse = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_SUBMIT, null,
        null, null, HTTPMethods.PUT);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    ReservationSubmissionRequestInfo context =
        new ReservationSubmissionRequestInfo();
    context.setReservationId(getNewReservationId().getReservationId());
    // ReservationDefinition is null

    ClientResponse response = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_SUBMIT, null,
        null, context, HTTPMethods.POST);

    if (response.getStatus() == STATUS_BADREQUEST) {
      String ci = response.getEntity(String.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#updateReservation()} inside Router.
   */
  @Test(timeout = 1000)
  public void testUpdateReservationXML() throws JSONException, Exception {

    // Test with a wrong HTTP method
    ClientResponse badResponse = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_UPDATE, null,
        null, null, HTTPMethods.PUT);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    String reservationId = getNewReservationId().getReservationId();
    ReservationUpdateRequestInfo context = new ReservationUpdateRequestInfo();
    context.setReservationId(reservationId);

    ClientResponse response = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_UPDATE, null,
        null, context, HTTPMethods.POST);

    if (response.getStatus() == STATUS_BADREQUEST) {
      String ci = response.getEntity(String.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#deleteReservation()} inside Router.
   */
  @Test(timeout = 1000)
  public void testDeleteReservationXML() throws JSONException, Exception {

    // Test with a wrong HTTP method
    ClientResponse badResponse = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_DELETE, null,
        null, null, HTTPMethods.PUT);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    String reservationId = getNewReservationId().getReservationId();
    ReservationDeleteRequestInfo context = new ReservationDeleteRequestInfo();
    context.setReservationId(reservationId);

    ClientResponse response = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_DELETE, null,
        null, context, HTTPMethods.POST);

    if (response.getStatus() == STATUS_BADREQUEST) {
      String ci = response.getEntity(String.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getNodeToLabels()} inside Router.
   */
  @Test(timeout = 1000)
  public void testGetNodeToLabelsXML() throws JSONException, Exception {

    List<NodeToLabelsInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.GET_NODE_TO_LABELS,
        NodeToLabelsInfo.class, null, null);

    NodeToLabelsInfo routerResponse = responses.get(0);
    NodeToLabelsInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getNodeToLabels().size(),
        rmResponse.getNodeToLabels().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getClusterNodeLabels()} inside Router.
   */
  @Test(timeout = 1000)
  public void testGetClusterNodeLabelsXML() throws JSONException, Exception {

    List<NodeLabelsInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.GET_NODE_LABELS,
        NodeLabelsInfo.class, null, null);

    NodeLabelsInfo routerResponse = responses.get(0);
    NodeLabelsInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getNodeLabels().size(),
        rmResponse.getNodeLabels().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getLabelsOnNode()} inside Router.
   */
  @Test(timeout = 1000)
  public void testGetLabelsOnNodeXML() throws JSONException, Exception {

    List<NodeLabelsInfo> responses =
        performGetCalls(
            RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES + "/"
                + getNodeId() + "/" + RMWSConsts.GET_LABELS,
            NodeLabelsInfo.class, null, null);

    NodeLabelsInfo routerResponse = responses.get(0);
    NodeLabelsInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getNodeLabels().size(),
        rmResponse.getNodeLabels().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getLabelsToNodes()} inside Router.
   */
  @Test(timeout = 1000)
  public void testGetLabelsMappingXML() throws JSONException, Exception {

    List<LabelsToNodesInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.LABEL_MAPPINGS,
        LabelsToNodesInfo.class, null, null);

    LabelsToNodesInfo routerResponse = responses.get(0);
    LabelsToNodesInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getLabelsToNodes().size(),
        rmResponse.getLabelsToNodes().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#addToClusterNodeLabels()} inside Router.
   */
  @Test(timeout = 1000)
  public void testAddToClusterNodeLabelsXML() throws JSONException, Exception {

    // Test with a wrong HTTP method
    ClientResponse badResponse =
        performCall(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.ADD_NODE_LABELS,
            null, null, null, HTTPMethods.PUT);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    List<NodeLabel> nodeLabels = new ArrayList<NodeLabel>();
    nodeLabels.add(NodeLabel.newInstance("default"));
    NodeLabelsInfo context = new NodeLabelsInfo(nodeLabels);

    ClientResponse response =
        performCall(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.ADD_NODE_LABELS,
            null, null, context, HTTPMethods.POST);

    if (response.getStatus() == STATUS_OK) {
      String ci = response.getEntity(String.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#removeFromCluserNodeLabels()} inside Router.
   */
  @Test(timeout = 1000)
  public void testRemoveFromCluserNodeLabelsXML()
      throws JSONException, Exception {

    // Test with a wrong HTTP method
    ClientResponse badResponse = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.REMOVE_NODE_LABELS, null,
        null, null, HTTPMethods.PUT);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    addNodeLabel();

    ClientResponse response = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.REMOVE_NODE_LABELS,
        RMWSConsts.LABELS, "default", null, HTTPMethods.POST);

    if (response.getStatus() == STATUS_OK) {
      String ci = response.getEntity(String.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#replaceLabelsOnNodes()} inside Router.
   */
  @Test(timeout = 1000)
  public void testReplaceLabelsOnNodesXML() throws JSONException, Exception {

    // Test with a wrong HTTP method
    ClientResponse badResponse = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.REPLACE_NODE_TO_LABELS,
        null, null, null, HTTPMethods.PUT);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    addNodeLabel();

    NodeToLabelsEntryList context = new NodeToLabelsEntryList();

    ClientResponse response = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.REPLACE_NODE_TO_LABELS,
        null, null, context, HTTPMethods.POST);

    if (response.getStatus() == STATUS_OK) {
      String ci = response.getEntity(String.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#replaceLabelsOnNode()} inside Router.
   */
  @Test(timeout = 1000)
  public void testReplaceLabelsOnNodeXML() throws JSONException, Exception {

    // Test with a wrong HTTP method
    ClientResponse badResponse =
        performCall(
            RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES + "/"
                + getNodeId() + "/replace-labels",
            null, null, null, HTTPMethods.PUT);

    Assert.assertEquals(STATUS_ERROR, badResponse.getStatus());

    // Test with the correct HTTP method

    addNodeLabel();

    ClientResponse response = performCall(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES + "/" + getNodeId()
            + "/replace-labels",
        RMWSConsts.LABELS, "default", null, HTTPMethods.POST);

    if (response.getStatus() == STATUS_OK) {
      String ci = response.getEntity(String.class);
      Assert.assertNotNull(ci);
    } else {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness of {@link WebServices#getAppAttempt}
   * inside Router.
   */
  @Test(timeout = 1000)
  public void testGetAppAttemptXML() throws JSONException, Exception {

    String appId = submitApplication();

    List<AppAttemptInfo> responses = performGetCalls(
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId + "/"
            + RMWSConsts.APPATTEMPTS + "/" + getAppAttempt(appId),
        AppAttemptInfo.class, null, null);

    AppAttemptInfo routerResponse = responses.get(0);
    AppAttemptInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getAppAttemptId(),
        rmResponse.getAppAttemptId());
  }

  /**
   * This test validates the correctness of {@link WebServices#getContainers}
   * inside Router.
   */
  @Test(timeout = 1000)
  public void testGetContainersXML() throws JSONException, Exception {

    String appId = submitApplication();

    List<ContainersInfo> responses =
        performGetCalls(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/"
            + appId + "/" + RMWSConsts.APPATTEMPTS + "/" + getAppAttempt(appId)
            + "/" + RMWSConsts.CONTAINERS, ContainersInfo.class, null, null);

    ContainersInfo routerResponse = responses.get(0);
    ContainersInfo rmResponse = responses.get(1);

    Assert.assertNotNull(routerResponse);
    Assert.assertNotNull(rmResponse);

    Assert.assertEquals(rmResponse.getContainers().size(),
        rmResponse.getContainers().size());
  }

  private String getNodeId() {
    Client clientToRM = Client.create();
    WebResource toRM =
        clientToRM.resource(WebAppUtils.getRMWebAppURLWithScheme(conf))
            .path(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES);
    ClientResponse response =
        toRM.accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
    NodesInfo ci = response.getEntity(NodesInfo.class);
    return ci.getNodes().get(0).getNodeId();
  }

  private NewApplication getNewApplicationId() {
    Client clientToRM = Client.create();
    WebResource toRM =
        clientToRM.resource(WebAppUtils.getRMWebAppURLWithScheme(conf)).path(
            RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS_NEW_APPLICATION);
    ClientResponse response =
        toRM.accept(MediaType.APPLICATION_XML).post(ClientResponse.class);
    return response.getEntity(NewApplication.class);
  }

  private String submitApplication() {
    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    String appId = getNewApplicationId().getApplicationId();
    context.setApplicationId(appId);

    Client clientToRouter = Client.create();
    WebResource toRM =
        clientToRouter.resource(WebAppUtils.getRMWebAppURLWithScheme(conf))
            .path(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS);
    toRM.entity(context, MediaType.APPLICATION_XML)
        .accept(MediaType.APPLICATION_XML).post(ClientResponse.class);
    return appId;
  }

  private NewReservation getNewReservationId() {
    Client clientToRM = Client.create();
    WebResource toRM =
        clientToRM.resource(WebAppUtils.getRMWebAppURLWithScheme(conf))
            .path(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_NEW);
    ClientResponse response =
        toRM.accept(MediaType.APPLICATION_XML).post(ClientResponse.class);
    return response.getEntity(NewReservation.class);
  }

  private String addNodeLabel() {
    Client clientToRM = Client.create();
    WebResource toRM =
        clientToRM.resource(WebAppUtils.getRMWebAppURLWithScheme(conf))
            .path(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.ADD_NODE_LABELS);
    List<NodeLabel> nodeLabels = new ArrayList<NodeLabel>();
    nodeLabels.add(NodeLabel.newInstance("default"));
    NodeLabelsInfo context = new NodeLabelsInfo(nodeLabels);
    ClientResponse response = toRM.entity(context, MediaType.APPLICATION_XML)
        .accept(MediaType.APPLICATION_XML).post(ClientResponse.class);
    return response.getEntity(String.class);
  }

  private String getAppAttempt(String appId) {
    Client clientToRM = Client.create();
    WebResource toRM =
        clientToRM.resource(WebAppUtils.getRMWebAppURLWithScheme(conf))
            .path(RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId
                + "/" + RMWSConsts.ATTEMPTS);
    ClientResponse response =
        toRM.accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
    AppAttemptsInfo ci = response.getEntity(AppAttemptsInfo.class);
    return ci.getAttempts().get(0).getAppAttemptId();
  }

}
