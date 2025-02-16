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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
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
    Assertions.assertNotNull(clusterInfo);

    ClusterInfo clusterInfo2 = getClusterInfo(user);
    Assertions.assertNotNull(clusterInfo2);

    ClusterMetricsInfo clusterMetricsInfo = getClusterMetricsInfo(user);
    Assertions.assertNotNull(clusterMetricsInfo);

    SchedulerTypeInfo schedulerTypeInfo = getSchedulerInfo(user);
    Assertions.assertNotNull(schedulerTypeInfo);

    String dumpResult = dumpSchedulerLogs(user);
    Assertions.assertNotNull(dumpResult);

    NodesInfo nodesInfo = getNodes(user);
    Assertions.assertNotNull(nodesInfo);

    NodeInfo nodeInfo = getNode(user);
    Assertions.assertNotNull(nodeInfo);

    AppsInfo appsInfo = getApps(user);
    Assertions.assertNotNull(appsInfo);

    ActivitiesInfo activitiesInfo = getActivities(user);
    Assertions.assertNotNull(activitiesInfo);

    AppActivitiesInfo appActiviesInfo = getAppActivities(user);
    Assertions.assertNotNull(appActiviesInfo);

    ApplicationStatisticsInfo applicationStatisticsInfo =
        getAppStatistics(user);
    Assertions.assertNotNull(applicationStatisticsInfo);

    AppInfo appInfo = getApp(user);
    Assertions.assertNotNull(appInfo);

    AppState appState = getAppState(user);
    Assertions.assertNotNull(appState);

    Response response = updateAppState(user);
    Assertions.assertNotNull(response);

    NodeToLabelsInfo nodeToLabelsInfo = getNodeToLabels(user);
    Assertions.assertNotNull(nodeToLabelsInfo);

    LabelsToNodesInfo labelsToNodesInfo = getLabelsToNodes(user);
    Assertions.assertNotNull(labelsToNodesInfo);

    Response response2 = replaceLabelsOnNodes(user);
    Assertions.assertNotNull(response2);

    Response response3 = replaceLabelsOnNode(user);
    Assertions.assertNotNull(response3);

    NodeLabelsInfo nodeLabelsInfo = getClusterNodeLabels(user);
    Assertions.assertNotNull(nodeLabelsInfo);

    Response response4 = addToClusterNodeLabels(user);
    Assertions.assertNotNull(response4);

    Response response5 = removeFromClusterNodeLabels(user);
    Assertions.assertNotNull(response5);

    NodeLabelsInfo nodeLabelsInfo2 = getLabelsOnNode(user);
    Assertions.assertNotNull(nodeLabelsInfo2);

    AppPriority appPriority = getAppPriority(user);
    Assertions.assertNotNull(appPriority);

    Response response6 = updateApplicationPriority(user);
    Assertions.assertNotNull(response6);

    AppQueue appQueue = getAppQueue(user);
    Assertions.assertNotNull(appQueue);

    Response response7 = updateAppQueue(user);
    Assertions.assertNotNull(response7);

    Response response8 = createNewApplication(user);
    Assertions.assertNotNull(response8);

    Response response9 = submitApplication(user);
    Assertions.assertNotNull(response9);

    Response response10 = postDelegationToken(user);
    Assertions.assertNotNull(response10);

    Response response11 = postDelegationTokenExpiration(user);
    Assertions.assertNotNull(response11);

    Response response12 = cancelDelegationToken(user);
    Assertions.assertNotNull(response12);

    Response response13 = createNewReservation(user);
    Assertions.assertNotNull(response13);

    Response response14 = submitReservation(user);
    Assertions.assertNotNull(response14);

    Response response15 = updateReservation(user);
    Assertions.assertNotNull(response15);

    Response response16 = deleteReservation(user);
    Assertions.assertNotNull(response16);

    Response response17 = listReservation(user);
    Assertions.assertNotNull(response17);

    AppTimeoutInfo appTimeoutInfo = getAppTimeout(user);
    Assertions.assertNotNull(appTimeoutInfo);

    AppTimeoutsInfo appTimeoutsInfo = getAppTimeouts(user);
    Assertions.assertNotNull(appTimeoutsInfo);

    Response response18 = updateApplicationTimeout(user);
    Assertions.assertNotNull(response18);

    AppAttemptsInfo appAttemptsInfo = getAppAttempts(user);
    Assertions.assertNotNull(appAttemptsInfo);

    AppAttemptInfo appAttemptInfo = getAppAttempt(user);
    Assertions.assertNotNull(appAttemptInfo);

    ContainersInfo containersInfo = getContainers(user);
    Assertions.assertNotNull(containersInfo);

    ContainerInfo containerInfo = getContainer(user);
    Assertions.assertNotNull(containerInfo);

    Response response19 = updateSchedulerConfiguration(user);
    Assertions.assertNotNull(response19);

    Response response20 = getSchedulerConfiguration(user);
    Assertions.assertNotNull(response20);
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
        Assertions.assertEquals(PassThroughRESTRequestInterceptor.class.getName(),
            root.getClass().getName());
        break;
      case 3:
        Assertions.assertEquals(MockRESTRequestInterceptor.class.getName(),
            root.getClass().getName());
        break;
      default:
        Assertions.fail();
      }
      root = root.getNextInterceptor();
      index++;
    }
    Assertions.assertEquals(4
,         index, "The number of interceptors in chain does not match");
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
    Assertions.assertEquals(8, pipelines.size());

    getInterceptorChain("test9");
    getInterceptorChain("test10");
    getInterceptorChain("test1");
    getInterceptorChain("test11");

    // The cache max size is defined in TEST_MAX_CACHE_SIZE
    Assertions.assertEquals(10, pipelines.size());

    RequestInterceptorChainWrapper chain = pipelines.get("test1");
    Assertions.assertNotNull(chain, "test1 should not be evicted");

    chain = pipelines.get("test2");
    Assertions.assertNull(chain, "test2 should have been evicted");
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
                Assertions.assertNotNull(interceptor);
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

    Assertions.assertNotNull(client1.interceptor);
    Assertions.assertNotNull(client2.interceptor);
    Assertions.assertSame(client1.interceptor, client2.interceptor);
  }

}
