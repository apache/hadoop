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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.After;
import org.junit.Test;

public class TestObserverWithRouter {

  private MiniRouterDFSCluster cluster;

  public void startUpCluster(int numberOfObserver) throws Exception {
    int numberOfNamenode = 2 + numberOfObserver;
    Configuration conf = new Configuration(false);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_ENABLE, true);
    conf.setInt(RBFConfigKeys.DFS_ROUTER_OBSERVER_AUTO_MSYNC_PERIOD, 0);
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    conf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");
    cluster = new MiniRouterDFSCluster(true, 1, numberOfNamenode);
    cluster.addNamenodeOverrides(conf);
    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Making one Namenodes active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
        for (int i = 2; i < numberOfNamenode; i++) {
          cluster.switchToObserver(ns, NAMENODES[i]);
        }
      }
    }

    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();

    cluster.addRouterOverrides(conf);
    cluster.addRouterOverrides(routerConf);

    // Start routers with only an RPC service
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    // Setup the mount table
    cluster.installMockLocations();

    cluster.waitActiveNamespaces();
  }

  @After
  public void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testObserverRead() throws Exception {
    startUpCluster(1);
    RouterContext routerContext = cluster.getRandomRouter();
    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertTrue("First namenode should be observer", namenodes.get(0).getState()
        .equals(FederationNamenodeServiceState.OBSERVER));
    FileSystem fileSystem = routerContext.getFileSystem();
    Path path = new Path("/testFile");
    // Send Create call to active
    fileSystem.create(path).close();

    // Send read request to observer
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create, complete, msync call should send to active
    assertEquals("Three call should send to active", 3, rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    // getBlockLocations should send to observer
    assertEquals("One call should send to observer", 1, rpcCountForObserver);
    fileSystem.close();
  }

  @Test
  public void testReadWhenObserverIsDown() throws Exception {
    startUpCluster(1);
    RouterContext routerContext = cluster.getRandomRouter();
    FileSystem fileSystem = routerContext.getFileSystem();
    Path path = new Path("/testFile1");
    // Send Create call to active
    fileSystem.create(path).close();

    // Stop observer NN
    int nnIndex = stopObserver(1);

    assertNotEquals("No observer found", 3, nnIndex);

    // Send read request
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create, complete, msync, getBlockLocation call should send to active
    assertEquals("Four call should send to active", 4,
        rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    assertEquals("No call should send to observer", 0,
        rpcCountForObserver);
    fileSystem.close();
  }

  @Test
  public void disableObserverReadFromClient() throws Exception {
    startUpCluster(1);
    RouterContext routerContext = cluster.getRandomRouter();
    Configuration conf = routerContext.getConf();
    conf.setBoolean(HdfsClientConfigKeys.DFS_OBSERVER_READ_ENABLE, false);
    FileSystem fileSystem = routerContext.getFileSystem();
    Path path = new Path("/testFile2");
    // Send Create call to active
    fileSystem.create(path).close();

    // Send read request
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter()
        .getRpcServer().getRPCMetrics().getActiveProxyOps();
    // Create, close, getBlockLocation call should send to active
    assertEquals("Three call should send to active", 3,
        rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter()
        .getRpcServer().getRPCMetrics().getObserverProxyOps();
    assertEquals("No call should send to observer", 0,
        rpcCountForObserver);
    fileSystem.close();
  }

  @Test
  public void testMultipleObserver() throws Exception {
    startUpCluster(2);
    RouterContext routerContext = cluster.getRandomRouter();
    FileSystem fileSystem = routerContext.getFileSystem();
    Path path = new Path("/testFile1");
    // Send Create call to active
    fileSystem.create(path).close();

    // Stop one observer NN
    stopObserver(1);

    // Send read request
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();

    long expectedActiveRpc = 3;
    long expectedObserverRpc = 1;

    // Create, complete, msync call should send to active
    assertEquals("Three call should send to active",
        expectedActiveRpc, rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter()
        .getRpcServer().getRPCMetrics().getObserverProxyOps();
    // getBlockLocation call should send to observer
    assertEquals("Read should be success with another observer",
        expectedObserverRpc, rpcCountForObserver);

    // Stop one observer NN
    stopObserver(1);

    // Send read request
    fileSystem.open(path).close();

    rpcCountForActive = routerContext.getRouter()
        .getRpcServer().getRPCMetrics().getActiveProxyOps();

    // msync, getBlockLocation call should send to active
    expectedActiveRpc += 2;
    assertEquals("Two call should send to active", expectedActiveRpc,
        rpcCountForActive);
    expectedObserverRpc += 0;
    rpcCountForObserver = routerContext.getRouter()
        .getRpcServer().getRPCMetrics().getObserverProxyOps();
    assertEquals("No call should send to observer",
        expectedObserverRpc, rpcCountForObserver);
    fileSystem.close();
  }

  private int stopObserver(int num) {
    int nnIndex;
    for (nnIndex = 0; nnIndex < cluster.getNamenodes().size(); nnIndex++) {
      NameNode nameNode = cluster.getCluster().getNameNode(nnIndex);
      if (nameNode != null && nameNode.isObserverState()) {
        cluster.getCluster().shutdownNameNode(nnIndex);
        num--;
        if (num == 0) {
          break;
        }
      }
    }
    return nnIndex;
  }

  // test router observer with multiple to know which observer NN received
  // requests
  @Test
  public void testMultipleObserverRouter() throws Exception {
    StateStoreDFSCluster innerCluster = null;
    RouterContext routerContext;
    MembershipNamenodeResolver resolver;

    String ns0;
    String ns1;
    //create 4NN, One Active One Standby and Two Observers
    innerCluster = new StateStoreDFSCluster(true, 4, 4, TimeUnit.SECONDS.toMillis(5),
        TimeUnit.SECONDS.toMillis(5));
    Configuration routerConf =
        new RouterConfigBuilder().stateStore().admin().rpc()
            .enableLocalHeartbeat(true).heartbeat().build();

    StringBuilder sb = new StringBuilder();
    ns0 = innerCluster.getNameservices().get(0);
    MiniRouterDFSCluster.NamenodeContext context =
        innerCluster.getNamenodes(ns0).get(1);
    routerConf.set(DFS_NAMESERVICE_ID, ns0);
    routerConf.set(DFS_HA_NAMENODE_ID_KEY, context.getNamenodeId());

    // Specify namenodes (ns1.nn0,ns1.nn1) to monitor
    sb = new StringBuilder();
    ns1 = innerCluster.getNameservices().get(1);
    for (MiniRouterDFSCluster.NamenodeContext ctx : innerCluster.getNamenodes(ns1)) {
      String suffix = ctx.getConfSuffix();
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(suffix);
    }
    routerConf.set(DFS_ROUTER_MONITOR_NAMENODE, sb.toString());
    routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_ENABLE, true);
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_OBSERVER_AUTO_MSYNC_PERIOD, 0);
    routerConf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    routerConf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");

    innerCluster.addNamenodeOverrides(routerConf);
    innerCluster.addRouterOverrides(routerConf);
    innerCluster.startCluster();

    if (innerCluster.isHighAvailability()) {
      for (String ns : innerCluster.getNameservices()) {
        innerCluster.switchToActive(ns, NAMENODES[0]);
        innerCluster.switchToStandby(ns, NAMENODES[1]);
        for (int i = 2; i < 4; i++) {
          innerCluster.switchToObserver(ns, NAMENODES[i]);
        }
      }
    }
    innerCluster.startRouters();
    innerCluster.waitClusterUp();

    routerContext = innerCluster.getRandomRouter();
    resolver = (MembershipNamenodeResolver) routerContext.getRouter()
        .getNamenodeResolver();

    resolver.loadCache(true);
    List<? extends FederationNamenodeContext> namespaceInfo0 =
        resolver.getNamenodesForNameserviceId(ns0, true);
    List<? extends FederationNamenodeContext> namespaceInfo1 =
        resolver.getNamenodesForNameserviceId(ns1, true);
    assertEquals(namespaceInfo0.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    assertEquals(namespaceInfo0.get(1).getState(),
        FederationNamenodeServiceState.OBSERVER);
    assertNotEquals(namespaceInfo0.get(0).getNamenodeId(),
        namespaceInfo0.get(1).getNamenodeId());
    assertEquals(namespaceInfo1.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
  }

  @Test
  public void testUnavaliableObserverNN() throws Exception {
    startUpCluster(2);
    RouterContext routerContext = cluster.getRandomRouter();
    FileSystem fileSystem = routerContext.getFileSystem();

    stopObserver(2);

    fileSystem.listStatus(new Path("/"));

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();

    // msync, getBlockLocation  call should send to active when observer
    // is stoped.
    assertEquals("Two call should send to active",
        2, rpcCountForActive);

    fileSystem.close();

    boolean hasUnavailable = false;
    for(String ns : cluster.getNameservices()) {
      List<? extends FederationNamenodeContext> nns = routerContext.getRouter()
          .getNamenodeResolver().getNamenodesForNameserviceId(ns, false);
      for(FederationNamenodeContext nn : nns) {
        if(FederationNamenodeServiceState.UNAVAILABLE == nn.getState()) {
          hasUnavailable = true;
        }
      }
    }
    // After communicate with unavailable observer namenode,
    // we will update state to unavailable.
    assertTrue("There Must has unavailable NN", hasUnavailable);
  }
}