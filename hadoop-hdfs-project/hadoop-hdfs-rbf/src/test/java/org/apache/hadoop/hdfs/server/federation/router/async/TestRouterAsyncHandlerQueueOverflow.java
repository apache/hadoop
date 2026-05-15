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
package org.apache.hadoop.hdfs.server.federation.router.async;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterAsyncRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RemoteParam;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapterMockitoUtil;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_MAX_ASYNCCALL_PERMIT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_QUEUE_SIZE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_RESPONDER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

public class TestRouterAsyncHandlerQueueOverflow {
  /**
   * Federated HDFS cluster.
   */
  private static MiniRouterDFSCluster cluster;
  private static String ns0;
  private static RouterRpcServer routerRpcServer;
  private static RouterAsyncRpcClient asyncRpcClient;

  private final static int QUEUE_CAP = 2;
  private static CountDownLatch testLatch;

  @BeforeAll
  public static void setUpCluster() throws Exception {
    cluster = new MiniRouterDFSCluster(true, 2, 3, DEFAULT_HEARTBEAT_INTERVAL_MS, 1000);
    cluster.startCluster();

    // Making one Namenode active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
        cluster.switchToObserver(ns, NAMENODES[2]);
      }
    }
    // Start routers with only an RPC service
    Configuration routerConf = new RouterConfigBuilder().metrics().rpc().build();

    routerConf.setInt(DFS_ROUTER_ASYNC_RPC_QUEUE_SIZE, QUEUE_CAP);
    routerConf.setInt(DFS_ROUTER_ASYNC_RPC_HANDLER_COUNT_KEY, 1);
    routerConf.setInt(DFS_ROUTER_ASYNC_RPC_RESPONDER_COUNT_KEY, 1);
    routerConf.set(DFS_ROUTER_MONITOR_NAMENODE,
        cluster.getNameservices().get(0) + "," + cluster.getNameservices().get(1));
    routerConf.setClass(DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        RouterAsyncRpcFairnessPolicyController.class, RouterRpcFairnessPolicyController.class);
    routerConf.setInt(DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT, 60000);
    routerConf.setInt(DFS_ROUTER_ASYNC_RPC_MAX_ASYNCCALL_PERMIT_KEY, 1);
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();

    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    cluster.waitActiveNamespaces();

    testLatch = new CountDownLatch(1);
    ns0 = cluster.getNameservices().get(0);
    MiniRouterDFSCluster.NamenodeContext nn0 = cluster.getNamenode(ns0, null);
    FSNamesystem spyNamesystem = NameNodeAdapterMockitoUtil.spyOnNamesystem(nn0.getNamenode());
    // Mock one slow operation. Any public interface from FSNamesystem will do.
    Mockito.doAnswer(invocationOnMock -> {
      String invokePath = invocationOnMock.getArgument(1);
      if (invokePath.startsWith("/veryBigOperation")) {
        testLatch.await();
      } else {
        return invocationOnMock.callRealMethod();
      }
      return null;
    }).when(spyNamesystem).getFilesBlockingDecom(anyLong(), anyString());

    MiniRouterDFSCluster.RouterContext router = cluster.getRandomRouter();
    routerRpcServer = router.getRouterRpcServer();
    routerRpcServer.initAsyncThreadPools(routerConf);
    asyncRpcClient = new RouterAsyncRpcClient(routerConf, router.getRouter(),
        routerRpcServer.getNamenodeResolver(), routerRpcServer.getRPCMonitor(),
        routerRpcServer.getRouterStateIdContext());
  }

  @AfterAll
  public static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  @Timeout(value = 10)
  public void testInvokeMethodQueueOverflow() throws Exception {
    RemoteMethod method =
        new RemoteMethod("listOpenFiles", new Class<?>[] {long.class, EnumSet.class, String.class},
            0, EnumSet.of(OpenFilesIterator.OpenFilesType.BLOCKING_DECOMMISSION),
            new RemoteParam());
    UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    Class<?> protocol = method.getProtocol();
    String bigPath = "/veryBigOperation";
    Object[] params =
        new Object[] {0, EnumSet.of(OpenFilesIterator.OpenFilesType.BLOCKING_DECOMMISSION),
            bigPath};
    List<? extends FederationNamenodeContext> namenodes =
        asyncRpcClient.getOrderedNamenodes(ns0, true);
    // Downstream namespace processing this huge request
    asyncRpcClient.invokeMethod(ugi, namenodes, true, protocol, method.getMethod(), params);
    ThreadPoolExecutor nsExecutor = routerRpcServer.getAsyncExecutorForNamespace(ns0);
    // Successfully sent this request downstream, but all subsequent ones will get stuck
    GenericTestUtils.waitFor(() -> nsExecutor.getQueue().isEmpty(), 50, 500);
    GenericTestUtils.waitFor(() -> nsExecutor.getCompletedTaskCount() == 1, 50, 500);

    // Async handler handling, blocking at acquirePermit
    asyncRpcClient.invokeMethod(ugi, namenodes, true, protocol, method.getMethod(), params);
    GenericTestUtils.waitFor(() -> nsExecutor.getQueue().isEmpty(), 50, 500);
    GenericTestUtils.waitFor(() -> nsExecutor.getCompletedTaskCount() == 1, 50, 500);
    // Stuck in queue
    asyncRpcClient.invokeMethod(ugi, namenodes, true, protocol, method.getMethod(), params);
    assertEquals(1, nsExecutor.getQueue().size());
    // Insert one more call, also stuck in queue
    asyncRpcClient.invokeMethod(ugi, namenodes, true, protocol, method.getMethod(), params);
    assertEquals(2, nsExecutor.getQueue().size());
    // Queue full, rejected
    asyncRpcClient.invokeMethod(ugi, namenodes, true, protocol, method.getMethod(), params);
    assertEquals(2, nsExecutor.getQueue().size());
    String expectedMsg = "Namespace '" + ns0 + "' is overloaded (queue size: " + QUEUE_CAP + ")";
    LambdaTestUtils.intercept(StandbyException.class, expectedMsg,
        () -> syncReturn(FileStatus.class));
    // Unstuck the namenode so we can terminate this test
    testLatch.countDown();
  }
}
