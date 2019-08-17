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

package org.apache.hadoop.yarn.server.router.clientrm;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService.RequestInterceptorChainWrapper;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to validate the ClientRM Service inside the Router.
 */
public class TestRouterClientRMService extends BaseRouterClientRMTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterClientRMService.class);

  /**
   * Tests if the pipeline is created properly.
   */
  @Test
  public void testRequestInterceptorChainCreation() throws Exception {
    ClientRequestInterceptor root =
        super.getRouterClientRMService().createRequestInterceptorChain();
    int index = 0;
    while (root != null) {
      // The current pipeline is:
      // PassThroughClientRequestInterceptor - index = 0
      // PassThroughClientRequestInterceptor - index = 1
      // PassThroughClientRequestInterceptor - index = 2
      // MockClientRequestInterceptor - index = 3
      switch (index) {
      case 0: // Fall to the next case
      case 1: // Fall to the next case
      case 2:
        // If index is equal to 0,1 or 2 we fall in this check
        Assert.assertEquals(PassThroughClientRequestInterceptor.class.getName(),
            root.getClass().getName());
        break;
      case 3:
        Assert.assertEquals(MockClientRequestInterceptor.class.getName(),
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
   * Test if the RouterClientRM forwards all the requests to the MockRM and get
   * back the responses.
   */
  @Test
  public void testRouterClientRMServiceE2E() throws Exception {

    String user = "test1";

    LOG.info("testRouterClientRMServiceE2E - Get New Application");

    GetNewApplicationResponse responseGetNewApp = getNewApplication(user);
    Assert.assertNotNull(responseGetNewApp);

    LOG.info("testRouterClientRMServiceE2E - Submit Application");

    SubmitApplicationResponse responseSubmitApp =
        submitApplication(responseGetNewApp.getApplicationId(), user);
    Assert.assertNotNull(responseSubmitApp);

    LOG.info("testRouterClientRMServiceE2E - Kill Application");

    KillApplicationResponse responseKillApp =
        forceKillApplication(responseGetNewApp.getApplicationId(), user);
    Assert.assertNotNull(responseKillApp);

    LOG.info("testRouterClientRMServiceE2E - Get Cluster Metrics");

    GetClusterMetricsResponse responseGetClusterMetrics =
        getClusterMetrics(user);
    Assert.assertNotNull(responseGetClusterMetrics);

    LOG.info("testRouterClientRMServiceE2E - Get Cluster Nodes");

    GetClusterNodesResponse responseGetClusterNodes = getClusterNodes(user);
    Assert.assertNotNull(responseGetClusterNodes);

    LOG.info("testRouterClientRMServiceE2E - Get Queue Info");

    GetQueueInfoResponse responseGetQueueInfo = getQueueInfo(user);
    Assert.assertNotNull(responseGetQueueInfo);

    LOG.info("testRouterClientRMServiceE2E - Get Queue User");

    GetQueueUserAclsInfoResponse responseGetQueueUser = getQueueUserAcls(user);
    Assert.assertNotNull(responseGetQueueUser);

    LOG.info("testRouterClientRMServiceE2E - Get Cluster Node");

    GetClusterNodeLabelsResponse responseGetClusterNode =
        getClusterNodeLabels(user);
    Assert.assertNotNull(responseGetClusterNode);

    LOG.info("testRouterClientRMServiceE2E - Move Application Across Queues");

    MoveApplicationAcrossQueuesResponse responseMoveApp =
        moveApplicationAcrossQueues(user, responseGetNewApp.getApplicationId());
    Assert.assertNotNull(responseMoveApp);

    LOG.info("testRouterClientRMServiceE2E - Get New Reservation");

    GetNewReservationResponse getNewReservationResponse =
        getNewReservation(user);

    LOG.info("testRouterClientRMServiceE2E - Submit Reservation");

    ReservationSubmissionResponse responseSubmitReser =
        submitReservation(user, getNewReservationResponse.getReservationId());
    Assert.assertNotNull(responseSubmitReser);

    LOG.info("testRouterClientRMServiceE2E - Update Reservation");

    ReservationUpdateResponse responseUpdateReser =
        updateReservation(user, getNewReservationResponse.getReservationId());
    Assert.assertNotNull(responseUpdateReser);

    LOG.info("testRouterClientRMServiceE2E - Delete Reservation");

    ReservationDeleteResponse responseDeleteReser =
        deleteReservation(user, getNewReservationResponse.getReservationId());
    Assert.assertNotNull(responseDeleteReser);
  }

  /**
   * Test if the different chains for users are generated, and LRU cache is
   * working as expected.
   */
  @Test
  public void testUsersChainMapWithLRUCache()
      throws YarnException, IOException, InterruptedException {

    Map<String, RequestInterceptorChainWrapper> pipelines;
    RequestInterceptorChainWrapper chain;

    getNewApplication("test1");
    getNewApplication("test2");
    getNewApplication("test3");
    getNewApplication("test4");
    getNewApplication("test5");
    getNewApplication("test6");
    getNewApplication("test7");
    getNewApplication("test8");

    pipelines = super.getRouterClientRMService().getPipelines();
    Assert.assertEquals(8, pipelines.size());

    getNewApplication("test9");
    getNewApplication("test10");
    getNewApplication("test1");
    getNewApplication("test11");

    // The cache max size is defined in
    // BaseRouterClientRMTest.TEST_MAX_CACHE_SIZE
    Assert.assertEquals(10, pipelines.size());

    chain = pipelines.get("test1");
    Assert.assertNotNull("test1 should not be evicted", chain);

    chain = pipelines.get("test2");
    Assert.assertNull("test2 should have been evicted", chain);
  }

  /**
   * This test validates if the ClientRequestInterceptor chain for the user
   * can build and init correctly when a multi-client process begins to
   * request RouterClientRMService for the same user simultaneously.
   */
  @Test
  public void testClientPipelineConcurrent() throws InterruptedException {
    final String user = "test1";

    /*
     * ClientTestThread is a thread to simulate a client request to get a
     * ClientRequestInterceptor for the user.
     */
    class ClientTestThread extends Thread {
      private ClientRequestInterceptor interceptor;
      @Override public void run() {
        try {
          interceptor = pipeline();
        } catch (IOException | InterruptedException e) {
          e.printStackTrace();
        }
      }
      private ClientRequestInterceptor pipeline()
          throws IOException, InterruptedException {
        return UserGroupInformation.createRemoteUser(user).doAs(
            new PrivilegedExceptionAction<ClientRequestInterceptor>() {
              @Override
              public ClientRequestInterceptor run() throws Exception {
                RequestInterceptorChainWrapper wrapper =
                    getRouterClientRMService().getInterceptorChain();
                ClientRequestInterceptor interceptor =
                    wrapper.getRootInterceptor();
                Assert.assertNotNull(interceptor);
                LOG.info("init client interceptor success for user " + user);
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
    Assert.assertTrue(client1.interceptor == client2.interceptor);
  }

}
