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
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.hadoop.security.UserGroupInformation;
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
import org.apache.hadoop.yarn.server.router.webapp.RouterWebServices.RequestInterceptorChainWrapper;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to validate the WebService interceptor model inside the Router.
 */
public class TestRouterWebServices extends BaseRouterWebServicesTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterWebServices.class);

  private String user = "test1";

  /**
   * Test that all requests get forwarded to the last interceptor in the chain
   * get back the responses.
   */
  @Test
  public void testRouterWebServicesE2E() throws Exception {

    ClusterInfo clusterInfo = get(user);
    Assert.assertNotNull(clusterInfo);

    ClusterInfo clusterInfo2 = getClusterInfo(user);
    Assert.assertNotNull(clusterInfo2);

    ClusterMetricsInfo clusterMetricsInfo = getClusterMetricsInfo(user);
    Assert.assertNotNull(clusterMetricsInfo);

    SchedulerTypeInfo schedulerTypeInfo = getSchedulerInfo(user);
    Assert.assertNotNull(schedulerTypeInfo);

    String dumpResult = dumpSchedulerLogs(user);
    Assert.assertNotNull(dumpResult);

    NodesInfo nodesInfo = getNodes(user);
    Assert.assertNotNull(nodesInfo);

    NodeInfo nodeInfo = getNode(user);
    Assert.assertNotNull(nodeInfo);

    AppsInfo appsInfo = getApps(user);
    Assert.assertNotNull(appsInfo);

    ActivitiesInfo activitiesInfo = getActivities(user);
    Assert.assertNotNull(activitiesInfo);

    AppActivitiesInfo appActiviesInfo = getAppActivities(user);
    Assert.assertNotNull(appActiviesInfo);

    ApplicationStatisticsInfo applicationStatisticsInfo =
        getAppStatistics(user);
    Assert.assertNotNull(applicationStatisticsInfo);

    AppInfo appInfo = getApp(user);
    Assert.assertNotNull(appInfo);

    AppState appState = getAppState(user);
    Assert.assertNotNull(appState);

    Response response = updateAppState(user);
    Assert.assertNotNull(response);

    NodeToLabelsInfo nodeToLabelsInfo = getNodeToLabels(user);
    Assert.assertNotNull(nodeToLabelsInfo);

    LabelsToNodesInfo labelsToNodesInfo = getLabelsToNodes(user);
    Assert.assertNotNull(labelsToNodesInfo);

    Response response2 = replaceLabelsOnNodes(user);
    Assert.assertNotNull(response2);

    Response response3 = replaceLabelsOnNode(user);
    Assert.assertNotNull(response3);

    NodeLabelsInfo nodeLabelsInfo = getClusterNodeLabels(user);
    Assert.assertNotNull(nodeLabelsInfo);

    Response response4 = addToClusterNodeLabels(user);
    Assert.assertNotNull(response4);

    Response response5 = removeFromClusterNodeLabels(user);
    Assert.assertNotNull(response5);

    NodeLabelsInfo nodeLabelsInfo2 = getLabelsOnNode(user);
    Assert.assertNotNull(nodeLabelsInfo2);

    AppPriority appPriority = getAppPriority(user);
    Assert.assertNotNull(appPriority);

    Response response6 = updateApplicationPriority(user);
    Assert.assertNotNull(response6);

    AppQueue appQueue = getAppQueue(user);
    Assert.assertNotNull(appQueue);

    Response response7 = updateAppQueue(user);
    Assert.assertNotNull(response7);

    Response response8 = createNewApplication(user);
    Assert.assertNotNull(response8);

    Response response9 = submitApplication(user);
    Assert.assertNotNull(response9);

    Response response10 = postDelegationToken(user);
    Assert.assertNotNull(response10);

    Response response11 = postDelegationTokenExpiration(user);
    Assert.assertNotNull(response11);

    Response response12 = cancelDelegationToken(user);
    Assert.assertNotNull(response12);

    Response response13 = createNewReservation(user);
    Assert.assertNotNull(response13);

    Response response14 = submitReservation(user);
    Assert.assertNotNull(response14);

    Response response15 = updateReservation(user);
    Assert.assertNotNull(response15);

    Response response16 = deleteReservation(user);
    Assert.assertNotNull(response16);

    Response response17 = listReservation(user);
    Assert.assertNotNull(response17);

    AppTimeoutInfo appTimeoutInfo = getAppTimeout(user);
    Assert.assertNotNull(appTimeoutInfo);

    AppTimeoutsInfo appTimeoutsInfo = getAppTimeouts(user);
    Assert.assertNotNull(appTimeoutsInfo);

    Response response18 = updateApplicationTimeout(user);
    Assert.assertNotNull(response18);

    AppAttemptsInfo appAttemptsInfo = getAppAttempts(user);
    Assert.assertNotNull(appAttemptsInfo);

    AppAttemptInfo appAttemptInfo = getAppAttempt(user);
    Assert.assertNotNull(appAttemptInfo);

    ContainersInfo containersInfo = getContainers(user);
    Assert.assertNotNull(containersInfo);

    ContainerInfo containerInfo = getContainer(user);
    Assert.assertNotNull(containerInfo);

    Response response19 = updateSchedulerConfiguration(user);
    Assert.assertNotNull(response19);

    Response response20 = getSchedulerConfiguration(user);
    Assert.assertNotNull(response20);
  }

  /**
   * Tests if the pipeline is created properly.
   */
  @Test
  public void testRequestInterceptorChainCreation() throws Exception {
    RESTRequestInterceptor root =
        super.getRouterWebServices().createRequestInterceptorChain();
    int index = 0;
    while (root != null) {
      // The current pipeline is:
      // PassThroughRESTRequestInterceptor - index = 0
      // PassThroughRESTRequestInterceptor - index = 1
      // PassThroughRESTRequestInterceptor - index = 2
      // MockRESTRequestInterceptor - index = 3
      switch (index) {
      case 0: // Fall to the next case
      case 1: // Fall to the next case
      case 2:
        // If index is equal to 0,1 or 2 we fall in this check
        Assert.assertEquals(PassThroughRESTRequestInterceptor.class.getName(),
            root.getClass().getName());
        break;
      case 3:
        Assert.assertEquals(MockRESTRequestInterceptor.class.getName(),
            root.getClass().getName());
        break;
      default:
        Assert.fail();
      }
      root = root.getNextInterceptor();
      index++;
    }
    Assert.assertEquals("The number of interceptors in chain does not match", 4,
        index);
  }

  /**
   * Test if the different chains for users are generated, and LRU cache is
   * working as expected.
   */
  @Test
  public void testUsersChainMapWithLRUCache()
      throws YarnException, IOException, InterruptedException {
    getInterceptorChain("test1");
    getInterceptorChain("test2");
    getInterceptorChain("test3");
    getInterceptorChain("test4");
    getInterceptorChain("test5");
    getInterceptorChain("test6");
    getInterceptorChain("test7");
    getInterceptorChain("test8");

    Map<String, RequestInterceptorChainWrapper> pipelines =
        getRouterWebServices().getPipelines();
    Assert.assertEquals(8, pipelines.size());

    getInterceptorChain("test9");
    getInterceptorChain("test10");
    getInterceptorChain("test1");
    getInterceptorChain("test11");

    // The cache max size is defined in TEST_MAX_CACHE_SIZE
    Assert.assertEquals(10, pipelines.size());

    RequestInterceptorChainWrapper chain = pipelines.get("test1");
    Assert.assertNotNull("test1 should not be evicted", chain);

    chain = pipelines.get("test2");
    Assert.assertNull("test2 should have been evicted", chain);
  }

  /**
   * This test validates if the RESTRequestInterceptor chain for the user
   * can build and init correctly when a multi-client process begins to
   * request RouterWebServices for the same user simultaneously.
   */
  @Test
  public void testWebPipelineConcurrent() throws InterruptedException {
    final String user = "test1";

    /*
     * ClientTestThread is a thread to simulate a client request to get a
     * RESTRequestInterceptor for the user.
     */
    class ClientTestThread extends Thread {
      private RESTRequestInterceptor interceptor;
      @Override public void run() {
        try {
          interceptor = pipeline();
        } catch (IOException | InterruptedException e) {
          e.printStackTrace();
        }
      }
      private RESTRequestInterceptor pipeline()
          throws IOException, InterruptedException {
        return UserGroupInformation.createRemoteUser(user).doAs(
            new PrivilegedExceptionAction<RESTRequestInterceptor>() {
              @Override
              public RESTRequestInterceptor run() throws Exception {
                RequestInterceptorChainWrapper wrapper =
                    getInterceptorChain(user);
                RESTRequestInterceptor interceptor =
                    wrapper.getRootInterceptor();
                Assert.assertNotNull(interceptor);
                LOG.info("init web interceptor success for user" + user);
                return interceptor;
              }
            });
      }
    }

    /*
     * We start the first thread. It should not finish initing a chainWrapper
     * before the other thread starts. In this way, the second thread can
     * init at the same time of the first one. In the end, we validate that
     * the 2 threads get the same chainWrapper without going into error.
     */
    ClientTestThread client1 = new ClientTestThread();
    ClientTestThread client2 = new ClientTestThread();
    client1.start();
    client2.start();
    client1.join();
    client2.join();

    Assert.assertNotNull(client1.interceptor);
    Assert.assertNotNull(client2.interceptor);
    Assert.assertSame(client1.interceptor, client2.interceptor);
  }

}
