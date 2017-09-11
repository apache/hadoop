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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
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
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<ClusterInfo>() {
          @Override
          public ClusterInfo run() throws Exception {
            return routerWebService.get();
          }
        });
  }

  protected ClusterInfo getClusterInfo(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<ClusterInfo>() {
          @Override
          public ClusterInfo run() throws Exception {
            return routerWebService.getClusterInfo();
          }
        });
  }

  protected ClusterMetricsInfo getClusterMetricsInfo(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<ClusterMetricsInfo>() {
          @Override
          public ClusterMetricsInfo run() throws Exception {
            return routerWebService.getClusterMetricsInfo();
          }
        });
  }

  protected SchedulerTypeInfo getSchedulerInfo(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<SchedulerTypeInfo>() {
          @Override
          public SchedulerTypeInfo run() throws Exception {
            return routerWebService.getSchedulerInfo();
          }
        });
  }

  protected String dumpSchedulerLogs(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<String>() {
          @Override
          public String run() throws Exception {
            return routerWebService.dumpSchedulerLogs(null, null);
          }
        });
  }

  protected NodesInfo getNodes(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<NodesInfo>() {
          @Override
          public NodesInfo run() throws Exception {
            return routerWebService.getNodes(null);
          }
        });
  }

  protected NodeInfo getNode(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<NodeInfo>() {
          @Override
          public NodeInfo run() throws Exception {
            return routerWebService.getNode(null);
          }
        });
  }

  protected AppsInfo getApps(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<AppsInfo>() {
          @Override
          public AppsInfo run() throws Exception {
            return routerWebService.getApps(null, null, null, null, null, null,
                null, null, null, null, null, null, null, null);
          }
        });
  }

  protected ActivitiesInfo getActivities(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<ActivitiesInfo>() {
          @Override
          public ActivitiesInfo run() throws Exception {
            return routerWebService.getActivities(null, null);
          }
        });
  }

  protected AppActivitiesInfo getAppActivities(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<AppActivitiesInfo>() {
          @Override
          public AppActivitiesInfo run() throws Exception {
            return routerWebService.getAppActivities(null, null, null);
          }
        });
  }

  protected ApplicationStatisticsInfo getAppStatistics(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<ApplicationStatisticsInfo>() {
          @Override
          public ApplicationStatisticsInfo run() throws Exception {
            return routerWebService.getAppStatistics(null, null, null);
          }
        });
  }

  protected AppInfo getApp(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<AppInfo>() {
          @Override
          public AppInfo run() throws Exception {
            return routerWebService.getApp(null, null, null);
          }
        });
  }

  protected AppState getAppState(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<AppState>() {
          @Override
          public AppState run() throws Exception {
            return routerWebService.getAppState(null, null);
          }
        });
  }

  protected Response updateAppState(String user) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.updateAppState(null, null, null);
          }
        });
  }

  protected NodeToLabelsInfo getNodeToLabels(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<NodeToLabelsInfo>() {
          @Override
          public NodeToLabelsInfo run() throws Exception {
            return routerWebService.getNodeToLabels(null);
          }
        });
  }

  protected LabelsToNodesInfo getLabelsToNodes(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<LabelsToNodesInfo>() {
          @Override
          public LabelsToNodesInfo run() throws Exception {
            return routerWebService.getLabelsToNodes(null);
          }
        });
  }

  protected Response replaceLabelsOnNodes(String user) throws Exception {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.replaceLabelsOnNodes(null, null);
          }
        });
  }

  protected Response replaceLabelsOnNode(String user) throws Exception {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.replaceLabelsOnNode(null, null, null);
          }
        });
  }

  protected NodeLabelsInfo getClusterNodeLabels(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<NodeLabelsInfo>() {
          @Override
          public NodeLabelsInfo run() throws Exception {
            return routerWebService.getClusterNodeLabels(null);
          }
        });
  }

  protected Response addToClusterNodeLabels(String user) throws Exception {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.addToClusterNodeLabels(null, null);
          }
        });
  }

  protected Response removeFromCluserNodeLabels(String user) throws Exception {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.removeFromCluserNodeLabels(null, null);
          }
        });
  }

  protected NodeLabelsInfo getLabelsOnNode(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<NodeLabelsInfo>() {
          @Override
          public NodeLabelsInfo run() throws Exception {
            return routerWebService.getLabelsOnNode(null, null);
          }
        });
  }

  protected AppPriority getAppPriority(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<AppPriority>() {
          @Override
          public AppPriority run() throws Exception {
            return routerWebService.getAppPriority(null, null);
          }
        });
  }

  protected Response updateApplicationPriority(String user)
      throws AuthorizationException, YarnException, InterruptedException,
      IOException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.updateApplicationPriority(null, null, null);
          }
        });
  }

  protected AppQueue getAppQueue(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<AppQueue>() {
          @Override
          public AppQueue run() throws Exception {
            return routerWebService.getAppQueue(null, null);
          }
        });
  }

  protected Response updateAppQueue(String user) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.updateAppQueue(null, null, null);
          }
        });
  }

  protected Response createNewApplication(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.createNewApplication(null);
          }
        });
  }

  protected Response submitApplication(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.submitApplication(null, null);
          }
        });
  }

  protected Response postDelegationToken(String user)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.postDelegationToken(null, null);
          }
        });
  }

  protected Response postDelegationTokenExpiration(String user)
      throws AuthorizationException, IOException, Exception {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.postDelegationTokenExpiration(null);
          }
        });
  }

  protected Response cancelDelegationToken(String user)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.cancelDelegationToken(null);
          }
        });
  }

  protected Response createNewReservation(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.createNewReservation(null);
          }
        });
  }

  protected Response submitReservation(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.submitReservation(null, null);
          }
        });
  }

  protected Response updateReservation(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.updateReservation(null, null);
          }
        });
  }

  protected Response deleteReservation(String user)
      throws AuthorizationException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.deleteReservation(null, null);
          }
        });
  }

  protected Response listReservation(String user) throws Exception {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.listReservation(null, null, 0, 0, false,
                null);
          }
        });
  }

  protected AppTimeoutInfo getAppTimeout(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<AppTimeoutInfo>() {
          @Override
          public AppTimeoutInfo run() throws Exception {
            return routerWebService.getAppTimeout(null, null, null);
          }
        });
  }

  protected AppTimeoutsInfo getAppTimeouts(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<AppTimeoutsInfo>() {
          @Override
          public AppTimeoutsInfo run() throws Exception {
            return routerWebService.getAppTimeouts(null, null);
          }
        });
  }

  protected Response updateApplicationTimeout(String user)
      throws AuthorizationException, YarnException, InterruptedException,
      IOException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<Response>() {
          @Override
          public Response run() throws Exception {
            return routerWebService.updateApplicationTimeout(null, null, null);
          }
        });
  }

  protected AppAttemptsInfo getAppAttempts(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<AppAttemptsInfo>() {
          @Override
          public AppAttemptsInfo run() throws Exception {
            return routerWebService.getAppAttempts(null, null);
          }
        });
  }

  protected AppAttemptInfo getAppAttempt(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<AppAttemptInfo>() {
          @Override
          public AppAttemptInfo run() throws Exception {
            return routerWebService.getAppAttempt(null, null, null, null);
          }
        });
  }

  protected ContainersInfo getContainers(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<ContainersInfo>() {
          @Override
          public ContainersInfo run() throws Exception {
            return routerWebService.getContainers(null, null, null, null);
          }
        });
  }

  protected ContainerInfo getContainer(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<ContainerInfo>() {
          @Override
          public ContainerInfo run() throws Exception {
            return routerWebService.getContainer(null, null, null, null, null);
          }
        });
  }

  protected RequestInterceptorChainWrapper getInterceptorChain(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<RequestInterceptorChainWrapper>() {
          @Override
          public RequestInterceptorChainWrapper run() throws Exception {
            return routerWebService.getInterceptorChain();
          }
        });
  }

}
