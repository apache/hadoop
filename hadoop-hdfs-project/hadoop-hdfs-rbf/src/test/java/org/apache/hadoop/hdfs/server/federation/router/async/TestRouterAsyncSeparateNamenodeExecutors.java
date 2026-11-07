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

import java.util.concurrent.ThreadPoolExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_OBSERVER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_RESPONDER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class TestRouterAsyncSeparateNamenodeExecutors {

  private MiniRouterDFSCluster cluster;
  private Configuration routerConf;
  private String ns0;
  private MiniRouterDFSCluster.RouterContext router;
  private RouterRpcServer routerRpcServer;
  private final String testFile = "/test.file";

  private void setUpCluster(boolean useSeparateObserverExecutor) throws Exception {
    cluster = new MiniRouterDFSCluster(true, 1, 3, DEFAULT_HEARTBEAT_INTERVAL_MS, 1000);
    // Don't need DNs for this suite
    cluster.setNumDatanodesPerNameservice(0);
    cluster.startCluster();
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
        cluster.switchToObserver(ns, NAMENODES[2]);
      }
    }
    // Start routers with only an RPC service
    routerConf = new RouterConfigBuilder().metrics().rpc().build();

    routerConf.setInt(DFS_ROUTER_CLIENT_THREADS_SIZE, 1);
    routerConf.setBoolean(DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    routerConf.setInt(DFS_ROUTER_ASYNC_RPC_HANDLER_COUNT_KEY, 1);
    routerConf.setInt(DFS_ROUTER_ASYNC_RPC_RESPONDER_COUNT_KEY, 1);
    routerConf.setBoolean(DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY, true);
    if (useSeparateObserverExecutor) {
      routerConf.setInt(DFS_ROUTER_ASYNC_RPC_OBSERVER_HANDLER_COUNT_KEY, 1);
    }
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();

    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    cluster.waitActiveNamespaces();
    ns0 = cluster.getNameservices().get(0);

    router = cluster.getRandomRouter();
    routerRpcServer = router.getRouterRpcServer();
    routerRpcServer.initAsyncThreadPools(routerConf);
    MockResolver resolver = (MockResolver) router.getRouter().getSubclusterResolver();
    resolver.addLocation("/", ns0, "/");
  }

  @AfterEach
  public void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testInvokeMethodsSeparateExecutors() throws Exception {
    setUpCluster(true);

    ThreadPoolExecutor activeExecutor = routerRpcServer.getAsyncExecutorForNamespace(ns0, false);
    ThreadPoolExecutor observerExecutor = routerRpcServer.getAsyncExecutorForNamespace(ns0, true);
    DFSClient routerClient = router.getClient();

    // Send a mkdirs, it should be proxied to active
    long activeTaskCount = activeExecutor.getCompletedTaskCount();
    routerClient.mkdirs("/testDir");
    long finalActiveTaskCount1 = activeTaskCount;
    GenericTestUtils.waitFor(
        () -> activeExecutor.getCompletedTaskCount() == finalActiveTaskCount1 + 1, 50, 1000);
    activeTaskCount = activeExecutor.getCompletedTaskCount();

    // Set a getFileInfo, it should be proxied to observer and not active
    long observerTaskCount = observerExecutor.getCompletedTaskCount();
    routerClient.getFileInfo("/testDir");
    long finalObserverTaskCount1 = observerTaskCount;
    GenericTestUtils.waitFor(
        () -> observerExecutor.getCompletedTaskCount() == finalObserverTaskCount1 + 1, 50, 1000);
    assertEquals(activeTaskCount, activeExecutor.getCompletedTaskCount());
    observerTaskCount = observerExecutor.getCompletedTaskCount();

    // Send a createFile, it should be proxied twice (once for mkdirs, once for create) to active
    routerClient.create(testFile, true).close();
    long finalActiveTaskCount2 = activeTaskCount;
    GenericTestUtils.waitFor(
        () -> activeExecutor.getCompletedTaskCount() == finalActiveTaskCount2 + 2, 50, 1000);
    assertEquals(observerTaskCount, observerExecutor.getCompletedTaskCount());
  }

  @Test
  public void testObserverUsesActiveExecutorWhenSeparateExecutorDisabled() throws Exception {
    setUpCluster(false);

    ThreadPoolExecutor activeExecutor = routerRpcServer.getAsyncExecutorForNamespace(ns0, false);
    ThreadPoolExecutor observerExecutor = routerRpcServer.getAsyncExecutorForNamespace(ns0, true);
    ThreadPoolExecutor defaultExecutor =
        routerRpcServer.getAsyncExecutorForNamespace("unknown", false);
    assertNotSame(defaultExecutor, observerExecutor);
    assertSame(activeExecutor, observerExecutor);

    DFSClient routerClient = router.getClient();
    long activeTaskCount = activeExecutor.getCompletedTaskCount();
    // mkdirs goes to active
    routerClient.mkdirs("/testDir");
    long finalActiveTaskCount1 = activeTaskCount;
    GenericTestUtils.waitFor(
        () -> activeExecutor.getCompletedTaskCount() == finalActiveTaskCount1 + 1, 50, 1000);

    // getFileInfo also goes to active
    activeTaskCount = activeExecutor.getCompletedTaskCount();
    routerClient.getFileInfo("/testDir");
    long finalActiveTaskCount2 = activeTaskCount;
    GenericTestUtils.waitFor(
        () -> activeExecutor.getCompletedTaskCount() == finalActiveTaskCount2 + 1, 50, 1000);
    assertSame(activeExecutor, observerExecutor);
  }
}
