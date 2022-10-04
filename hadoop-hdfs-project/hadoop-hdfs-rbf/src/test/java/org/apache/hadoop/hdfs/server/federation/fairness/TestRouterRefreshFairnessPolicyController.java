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

package org.apache.hadoop.hdfs.server.federation.fairness;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX;
import static org.junit.Assert.assertEquals;

public class TestRouterRefreshFairnessPolicyController {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterRefreshFairnessPolicyController.class);
  private final GenericTestUtils.LogCapturer controllerLog =
      GenericTestUtils.LogCapturer.captureLogs(AbstractRouterRpcFairnessPolicyController.LOG);

  private StateStoreDFSCluster cluster;

  @BeforeClass
  public static void setLogLevel() {
    GenericTestUtils.setLogLevel(AbstractRouterRpcFairnessPolicyController.LOG, Level.DEBUG);
  }

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Before
  public void setupCluster() throws Exception {
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder().stateStore().rpc().build();

    // Handlers concurrent:ns0 = 3:3
    conf.setClass(RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        StaticRouterRpcFairnessPolicyController.class, RouterRpcFairnessPolicyController.class);
    conf.setInt(RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY, 9);
    // Allow metrics
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_METRICS_ENABLE, true);

    // Datanodes not needed for this test.
    cluster.setNumDatanodesPerNameservice(0);

    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
  }

  @Test
  public void testRefreshNonexistentHandlerClass() {
    MiniRouterDFSCluster.RouterContext routerContext = cluster.getRandomRouter();
    routerContext.getConf().set(RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        "org.apache.hadoop.hdfs.server.federation.fairness.ThisControllerDoesNotExist");
    assertEquals(StaticRouterRpcFairnessPolicyController.class.getCanonicalName(),
        routerContext.getRouterRpcClient()
            .refreshFairnessPolicyController(routerContext.getConf()));
  }

  @Test
  public void testRefreshClassDoesNotImplementControllerInterface() {
    MiniRouterDFSCluster.RouterContext routerContext = cluster.getRandomRouter();
    routerContext.getConf()
        .set(RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS, "java.lang.String");
    assertEquals(StaticRouterRpcFairnessPolicyController.class.getCanonicalName(),
        routerContext.getRouterRpcClient()
            .refreshFairnessPolicyController(routerContext.getConf()));
  }

  @Test
  public void testRefreshSuccessful() {
    MiniRouterDFSCluster.RouterContext routerContext = cluster.getRandomRouter();

    routerContext.getConf().set(RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        StaticRouterRpcFairnessPolicyController.class.getCanonicalName());
    assertEquals(StaticRouterRpcFairnessPolicyController.class.getCanonicalName(),
        routerContext.getRouterRpcClient()
            .refreshFairnessPolicyController(routerContext.getConf()));

    routerContext.getConf().set(RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        NoRouterRpcFairnessPolicyController.class.getCanonicalName());
    assertEquals(NoRouterRpcFairnessPolicyController.class.getCanonicalName(),
        routerContext.getRouterRpcClient()
            .refreshFairnessPolicyController(routerContext.getConf()));
  }

  @Test
  public void testConcurrentRefreshRequests() throws InterruptedException {
    MiniRouterDFSCluster.RouterContext routerContext = cluster.getRandomRouter();
    RouterRpcClient client = Mockito.spy(routerContext.getRouterRpcClient());
    controllerLog.clearOutput();

    // Spawn 100 concurrent refresh requests
    Thread[] threads = new Thread[100];
    for (int i = 0; i < 100; i++) {
      threads[i] = new Thread(() ->
          client.refreshFairnessPolicyController(routerContext.getConf()));
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    // There should be 100 controller shutdowns. All controllers created should be shut down.
    assertEquals(100, StringUtils.countMatches(controllerLog.getOutput(),
        "Shutting down router fairness policy controller"));
    controllerLog.clearOutput();
  }

  @Test
  public void testRefreshStaticChangeHandlers() throws Exception {
    // Setup and mock
    MiniRouterDFSCluster.RouterContext routerContext = cluster.getRandomRouter();
    RouterRpcClient client = Mockito.spy(routerContext.getRouterRpcClient());
    final long sleepTime = 3000;
    Mockito.doAnswer(invocationOnMock -> {
      Thread.sleep(sleepTime);
      return null;
    }).when(client)
        .invokeMethod(Mockito.any(), Mockito.any(), Mockito.anyBoolean(),
            Mockito.any(), Mockito.any(), Mockito.any());

    // No calls yet
    assertEquals("{}",
        routerContext.getRouterRpcServer().getRPCMetrics().getProxyOpPermitAcceptedPerNs());
    List<Thread> preRefreshInvocations = makeDummyInvocations(client, 4, "ns0");

    Thread.sleep(2000);
    // 3 permits acquired, calls will take 3s to finish and release permits
    // 1 invocation rejected
    assertEquals("{\"ns0\":3}",
        routerContext.getRouterRpcServer().getRPCMetrics().getProxyOpPermitAcceptedPerNs());
    assertEquals("{\"ns0\":1}",
        routerContext.getRouterRpcServer().getRPCMetrics().getProxyOpPermitRejectedPerNs());

    Configuration conf = routerContext.getConf();
    final int newNs0Permits = 2;
    final int newNs1Permits = 4;
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns0", newNs0Permits);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns1", newNs1Permits);
    Thread threadRefreshController = new Thread(() -> client.
        refreshFairnessPolicyController(routerContext.getConf()));
    threadRefreshController.start();
    threadRefreshController.join();

    // Wait for all dummy invocation threads to finish
    for (Thread thread : preRefreshInvocations) {
      thread.join();
    }

    // Controller should now have 2:4 handlers for ns0:ns1
    // Make 4 calls to ns0 and 6 calls to ns1 so that each will fail twice
    StaticRouterRpcFairnessPolicyController controller =
        (StaticRouterRpcFairnessPolicyController) client.getRouterRpcFairnessPolicyController();
    System.out.println(controller.getAvailableHandlerOnPerNs());
    List<Thread> ns0Invocations = makeDummyInvocations(client, newNs0Permits + 2, "ns0");
    List<Thread> ns1Invocations = makeDummyInvocations(client, newNs1Permits + 2, "ns1");

    // Wait for these threads to finish
    for (Thread thread : ns0Invocations) {
      thread.join();
    }
    for (Thread thread : ns1Invocations) {
      thread.join();
    }
    assertEquals("{\"ns0\":5,\"ns1\":4}",
        routerContext.getRouterRpcServer().getRPCMetrics().getProxyOpPermitAcceptedPerNs());
    assertEquals("{\"ns0\":3,\"ns1\":2}",
        routerContext.getRouterRpcServer().getRPCMetrics().getProxyOpPermitRejectedPerNs());
  }

  private List<Thread> makeDummyInvocations(RouterRpcClient client, final int nThreads,
      final String namespace) {
    RemoteMethod dummyMethod = Mockito.mock(RemoteMethod.class);
    List<Thread> threadAcquirePermits = new ArrayList<>();
    for (int i = 0; i < nThreads; i++) {
      Thread threadAcquirePermit = new Thread(() -> {
        try {
          client.invokeSingle(namespace, dummyMethod);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      threadAcquirePermits.add(threadAcquirePermit);
      threadAcquirePermit.start();
    }
    return threadAcquirePermits;
  }
}
