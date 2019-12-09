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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.webapp.RouterWebServices.RequestInterceptorChainWrapper;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;

/**
 * Base class for all the RouterRMAdminService test cases. It provides utility
 * methods that can be used by the concrete test case classes.
 *
 */
public abstract class BaseRouterWebServicesTest {

  private YarnConfiguration conf;

  private Router router;
  public final static int TEST_MAX_CACHE_SIZE = 10;

  private RouterWebServices routerWebService;

  @Before
  public void setUp() {
    this.conf = createConfiguration();

    router = spy(new Router());
    Mockito.doNothing().when(router).startWepApp();
    routerWebService = new RouterWebServices(router, conf);
    routerWebService.setResponse(mock(HttpServletResponse.class));

    router.init(conf);
    router.start();
  }

  protected YarnConfiguration createConfiguration() {
    YarnConfiguration config = new YarnConfiguration();
    String mockPassThroughInterceptorClass =
        PassThroughRESTRequestInterceptor.class.getName();

    // Create a request intercepter pipeline for testing. The last one in the
    // chain will call the mock resource manager. The others in the chain will
    // simply forward it to the next one in the chain
    config.set(YarnConfiguration.ROUTER_WEBAPP_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + "," + mockPassThroughInterceptorClass
            + "," + mockPassThroughInterceptorClass + ","
            + MockRESTRequestInterceptor.class.getName());

    config.setInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
        TEST_MAX_CACHE_SIZE);
    return config;
  }

  @After
  public void tearDown() {
    if (router != null) {
      router.stop();
    }
  }

  public void setUpConfig() {
    this.conf = createConfiguration();
  }

  protected Configuration getConf() {
    return this.conf;
  }

  protected RouterWebServices getRouterWebServices() {
    Assert.assertNotNull(this.routerWebService);
    return this.routerWebService;
  }

  protected ClusterInfo get(String user)
      throws IOException, InterruptedException {
    // HSR is not used here
    return routerWebService.get();
  }

  protected ClusterInfo getClusterInfo(String user)
      throws IOException, InterruptedException {
    // HSR is not used here
    return routerWebService.getClusterInfo();
  }

  protected ClusterMetricsInfo getClusterMetricsInfo(String user)
      throws IOException, InterruptedException {
    // HSR is not used here
    return routerWebService.getClusterMetricsInfo();
  }

  protected SchedulerTypeInfo getSchedulerInfo(String user)
      throws IOException, InterruptedException {
    // HSR is not used here
    return routerWebService.getSchedulerInfo();
  }

  protected String dumpSchedulerLogs(String user)
      throws IOException, InterruptedException {
    return routerWebService.dumpSchedulerLogs(null,
        createHttpServletRequest(user));
  }

  protected NodesInfo getNodes(String user)
      throws IOException, InterruptedException {
    return routerWebService.getNodes(null);
  }

  protected NodeInfo getNode(String user)
      throws IOException, InterruptedException {
    return routerWebService.getNode(null);
  }

  protected AppsInfo getApps(String user)
      throws IOException, InterruptedException {
    return routerWebService.getApps(createHttpServletRequest(user), null, null,
        null, null, null, null, null, null, null, null, null, null, null);
  }

  protected ActivitiesInfo getActivities(String user)
      throws IOException, InterruptedException {
    return routerWebService.getActivities(
        createHttpServletRequest(user), null);
  }

  protected AppActivitiesInfo getAppActivities(String user)
      throws IOException, InterruptedException {
    return routerWebService.getAppActivities(
        createHttpServletRequest(user), null, null);
  }

  protected ApplicationStatisticsInfo getAppStatistics(String user)
      throws IOException, InterruptedException {
    return routerWebService.getAppStatistics(
        createHttpServletRequest(user), null, null);
  }

  protected AppInfo getApp(String user)
      throws IOException, InterruptedException {
    return routerWebService.getApp(createHttpServletRequest(user), null, null);
  }

  protected AppState getAppState(String user)
      throws IOException, InterruptedException {
    return routerWebService.getAppState(createHttpServletRequest(user), null);
  }

  protected Response updateAppState(String user) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    return routerWebService.updateAppState(
        null, createHttpServletRequest(user), null);
  }

  protected NodeToLabelsInfo getNodeToLabels(String user)
      throws IOException, InterruptedException {
    return routerWebService.getNodeToLabels(createHttpServletRequest(user));
  }

  protected LabelsToNodesInfo getLabelsToNodes(String user)
      throws IOException, InterruptedException {
    return routerWebService.getLabelsToNodes(null);
  }

  protected Response replaceLabelsOnNodes(String user) throws Exception {
    return routerWebService.replaceLabelsOnNodes(
        null, createHttpServletRequest(user));
  }

  protected Response replaceLabelsOnNode(String user) throws Exception {
    return routerWebService.replaceLabelsOnNode(
        null, createHttpServletRequest(user), null);
  }

  protected NodeLabelsInfo getClusterNodeLabels(String user)
      throws IOException, InterruptedException {
    return routerWebService.getClusterNodeLabels(
        createHttpServletRequest(user));
  }

  protected Response addToClusterNodeLabels(String user) throws Exception {
    return routerWebService.addToClusterNodeLabels(
        null, createHttpServletRequest(user));
  }

  protected Response removeFromCluserNodeLabels(String user) throws Exception {
    return routerWebService.removeFromCluserNodeLabels(
        null, createHttpServletRequest(user));
  }

  protected NodeLabelsInfo getLabelsOnNode(String user)
      throws IOException, InterruptedException {
    return routerWebService.getLabelsOnNode(
        createHttpServletRequest(user), null);
  }

  protected AppPriority getAppPriority(String user)
      throws IOException, InterruptedException {
    return routerWebService.getAppPriority(
        createHttpServletRequest(user), null);
  }

  protected Response updateApplicationPriority(String user)
      throws AuthorizationException, YarnException, InterruptedException,
      IOException {
    return routerWebService.updateApplicationPriority(
        null, createHttpServletRequest(user), null);
  }

  protected AppQueue getAppQueue(String user)
      throws IOException, InterruptedException {
    return routerWebService.getAppQueue(createHttpServletRequest(user), null);
  }

  protected Response updateAppQueue(String user) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    return routerWebService.updateAppQueue(
        null, createHttpServletRequest(user), null);
  }

  protected Response createNewApplication(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return routerWebService.createNewApplication(
        createHttpServletRequest(user));
  }

  protected Response submitApplication(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return routerWebService.submitApplication(
        null, createHttpServletRequest(user));
  }

  protected Response postDelegationToken(String user)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {
    return routerWebService.postDelegationToken(
        null, createHttpServletRequest(user));
  }

  protected Response postDelegationTokenExpiration(String user)
      throws AuthorizationException, IOException, Exception {
    return routerWebService.postDelegationTokenExpiration(
        createHttpServletRequest(user));
  }

  protected Response cancelDelegationToken(String user)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {
    return routerWebService.cancelDelegationToken(
        createHttpServletRequest(user));
  }

  protected Response createNewReservation(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return routerWebService.createNewReservation(
        createHttpServletRequest(user));
  }

  protected Response submitReservation(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return routerWebService.submitReservation(
        null, createHttpServletRequest(user));
  }

  protected Response updateReservation(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return routerWebService.updateReservation(
        null, createHttpServletRequest(user));
  }

  protected Response deleteReservation(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return routerWebService.deleteReservation(
        null, createHttpServletRequest(user));
  }

  protected Response listReservation(String user) throws Exception {
    return routerWebService.listReservation(
        null, null, 0, 0, false, createHttpServletRequest(user));
  }

  protected AppTimeoutInfo getAppTimeout(String user)
      throws IOException, InterruptedException {
    return routerWebService.getAppTimeout(
        createHttpServletRequest(user), null, null);
  }

  protected AppTimeoutsInfo getAppTimeouts(String user)
      throws IOException, InterruptedException {
    return routerWebService.getAppTimeouts(
        createHttpServletRequest(user), null);
  }

  protected Response updateApplicationTimeout(String user)
      throws AuthorizationException, YarnException, InterruptedException,
      IOException {
    return routerWebService.updateApplicationTimeout(
        null, createHttpServletRequest(user), null);
  }

  protected AppAttemptsInfo getAppAttempts(String user)
      throws IOException, InterruptedException {
    return routerWebService.getAppAttempts(
        createHttpServletRequest(user), null);
  }

  protected AppAttemptInfo getAppAttempt(String user)
      throws IOException, InterruptedException {
    return routerWebService.getAppAttempt(
        createHttpServletRequest(user), null, null, null);
  }

  protected ContainersInfo getContainers(String user)
      throws IOException, InterruptedException {
    return routerWebService.getContainers(
        createHttpServletRequest(user), null, null, null);
  }

  protected ContainerInfo getContainer(String user)
      throws IOException, InterruptedException {
    return routerWebService.getContainer(
        createHttpServletRequest(user), null, null, null, null);
  }

  protected RequestInterceptorChainWrapper getInterceptorChain(String user)
      throws IOException, InterruptedException {
    HttpServletRequest request = createHttpServletRequest(user);
    return routerWebService.getInterceptorChain(request);
  }

  private HttpServletRequest createHttpServletRequest(String user) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(user);
    return request;
  }
}
