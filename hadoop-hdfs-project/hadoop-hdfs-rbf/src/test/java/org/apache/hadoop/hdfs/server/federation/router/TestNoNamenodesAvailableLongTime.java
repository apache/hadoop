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

import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Lists;
import org.junit.After;
import org.junit.Test;


import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE;
import static org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * When failover occurs, the router may record that the ns has no active namenode
 * even if there is actually an active namenode.
 * Only when the router updates the cache next time can the memory status be updated,
 * causing the router to report NoNamenodesAvailableException for a long time,
 *
 * @see org.apache.hadoop.hdfs.server.federation.router.NoNamenodesAvailableException
 */
public class TestNoNamenodesAvailableLongTime {

  // router load cache interval 10s
  private static final long CACHE_FLUSH_INTERVAL_MS = 10000;
  private StateStoreDFSCluster cluster;
  private FileSystem fileSystem;
  private RouterContext routerContext;
  private FederationRPCMetrics rpcMetrics;

  @After
  public void cleanup() throws IOException {
    rpcMetrics = null;
    routerContext = null;
    if (fileSystem != null) {
      fileSystem.close();
      fileSystem = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Set up state store cluster.
   *
   * @param numNameservices number of name services
   * @param numberOfObserver number of observer
   * @param useObserver whether to use observer
   */
  private void setupCluster(int numNameservices, int numberOfObserver, boolean useObserver)
      throws Exception {
    if (!useObserver) {
      numberOfObserver = 0;
    }
    int numberOfNamenode = 2 + numberOfObserver;
    cluster = new StateStoreDFSCluster(true, numNameservices, numberOfNamenode,
        DEFAULT_HEARTBEAT_INTERVAL_MS, CACHE_FLUSH_INTERVAL_MS);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .metrics()
        .admin()
        .rpc()
        .heartbeat()
        .build();

    // Set router observer related configs
    if (useObserver) {
      routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY, true);
      routerConf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
      routerConf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");
    }

    // Reduce the number of RPC clients threads to overload the Router easy
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE, 4);

    // No need for datanodes
    cluster.setNumDatanodesPerNameservice(0);
    cluster.addRouterOverrides(routerConf);

    cluster.startCluster();

    // Making one Namenode active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        List<MiniRouterDFSCluster.NamenodeContext>  nnList = cluster.getNamenodes(ns);
        cluster.switchToActive(ns, nnList.get(0).getNamenodeId());
        cluster.switchToStandby(ns, nnList.get(1).getNamenodeId());
        for (int i = 2; i < numberOfNamenode; i++) {
          cluster.switchToObserver(ns, nnList.get(i).getNamenodeId());
        }
      }
    }

    cluster.startRouters();
    cluster.waitClusterUp();
  }

  /**
   * Initialize the test environment and start the cluster so that
   * there is no active namenode record in the router cache,
   * but the second non-observer namenode in the router cache is actually active.
   */
  private void initEnv(int numberOfObserver, boolean useObserver) throws Exception {
    setupCluster(1, numberOfObserver, useObserver);
    // Transition all namenodes in the cluster are standby.
    transitionActiveToStandby();
    //
    allRoutersHeartbeat();
    allRoutersLoadCache();

    List<MiniRouterDFSCluster.NamenodeContext> namenodes = cluster.getNamenodes();

    // Make sure all namenodes are in standby state
    for (MiniRouterDFSCluster.NamenodeContext namenodeContext : namenodes) {
      assertNotEquals(ACTIVE.ordinal(), namenodeContext.getNamenode().getNameNodeState());
    }

    routerContext = cluster.getRandomRouter();

    // Get the second namenode in the router cache and make it active
    setSecondNonObserverNamenodeInTheRouterCacheActive(numberOfObserver, false);
    allRoutersHeartbeat();

    // Get router metrics
    rpcMetrics = routerContext.getRouter().getRpcServer().getRPCMetrics();

    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", useObserver));

    // Retries is 2 (see FailoverOnNetworkExceptionRetry#shouldRetry, will fail
    // when reties > max.attempts), so total access is 3.
    routerContext.getConf().setInt("dfs.client.retry.max.attempts", 1);

    if (useObserver) {
      fileSystem = routerContext.getFileSystemWithObserverReadProxyProvider();
    } else {
      fileSystem = routerContext.getFileSystemWithConfiguredFailoverProxyProvider();
    }
  }

  /**
   * If NoNamenodesAvailableException occurs due to
   * {@link RouterRpcClient#isUnavailableException(IOException) unavailable exception},
   * should rotated Cache.
   */
  @Test
  public void testShouldRotatedCache() throws Exception {
    // 2 namenodes: 1 active, 1 standby.
    // But there is no active namenode in router cache.
    initEnv(0, false);
    // At this time, the router has recorded 2 standby namenodes in memory.
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", false));

    Path path = new Path("/test.file");
    // The first create operation will cause NoNamenodesAvailableException and RotatedCache.
    // After retrying, create and complete operation will be executed successfully.
    fileSystem.create(path);
    assertEquals(1, rpcMetrics.getProxyOpNoNamenodes());

    // At this time, the router has recorded 2 standby namenodes in memory,
    // the operation can be successful without waiting for the router load cache.
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", false));
  }

  /**
   * If a request still fails even if it is sent to active,
   * then the change operation itself is illegal,
   * the cache should not be rotated due to illegal operations.
   */
  @Test
  public void testShouldNotBeRotatedCache() throws Exception {
    testShouldRotatedCache();
    long proxyOpNoNamenodes = rpcMetrics.getProxyOpNoNamenodes();
    Path path = new Path("/test.file");
    /*
     * we have put the actually active namenode at the front of the cache by rotating the cache.
     * Therefore, the setPermission operation does not cause NoNamenodesAvailableException.
     */
    fileSystem.setPermission(path, FsPermission.createImmutable((short)0640));
    assertEquals(proxyOpNoNamenodes, rpcMetrics.getProxyOpNoNamenodes());

    // At this time, the router has recorded 2 standby namenodes in memory
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", false));

    /*
     * Even if the router transfers the illegal request to active,
     * NoNamenodesAvailableException will still be generated.
     * Therefore, rotated cache is not needed.
     */
    List<AclEntry> aclSpec = Lists.newArrayList(aclEntry(DEFAULT, USER, "foo", ALL));
    try {
      fileSystem.setAcl(path, aclSpec);
    }catch (RemoteException e) {
      assertTrue(e.getMessage().contains(
          "org.apache.hadoop.hdfs.server.federation.router.NoNamenodesAvailableException: " +
          "No namenodes available under nameservice ns0"));
      assertTrue(e.getMessage().contains(
          "org.apache.hadoop.hdfs.protocol.AclException: Invalid ACL: " +
          "only directories may have a default ACL. Path: /test.file"));
    }
    // Retries is 2 (see FailoverOnNetworkExceptionRetry#shouldRetry, will fail
    // when reties > max.attempts), so total access is 3.
    assertEquals(proxyOpNoNamenodes + 3, rpcMetrics.getProxyOpNoNamenodes());
    proxyOpNoNamenodes = rpcMetrics.getProxyOpNoNamenodes();

    // So legal operations can be accessed normally without reporting NoNamenodesAvailableException.
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", false));
    fileSystem.getFileStatus(path);
    assertEquals(proxyOpNoNamenodes, rpcMetrics.getProxyOpNoNamenodes());

    // At this time, the router has recorded 2 standby namenodes in memory,
    // the operation can be successful without waiting for the router load cache.
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", false));
  }

  /**
   * In the observer scenario, NoNamenodesAvailableException occurs,
   * the operation can be successful without waiting for the router load cache.
   */
  @Test
  public void testUseObserver() throws Exception {
    // 4 namenodes: 2 observers, 1 active, 1 standby.
    // But there is no active namenode in router cache.
    initEnv(2, true);

    Path path = new Path("/");
    // At this time, the router has recorded 2 standby namenodes in memory.
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", true));

    // The first msync operation will cause NoNamenodesAvailableException and RotatedCache.
    // After retrying, msync and getFileInfo operation will be executed successfully.
    fileSystem.getFileStatus(path);
    assertEquals(1, rpcMetrics.getObserverProxyOps());
    assertEquals(1, rpcMetrics.getProxyOpNoNamenodes());

    // At this time, the router has recorded 2 standby namenodes in memory,
    // the operation can be successful without waiting for the router load cache.
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", true));
  }

  /**
   * In a multi-observer environment, if at least one observer is normal,
   * read requests can still succeed even if NoNamenodesAvailableException occurs.
   */
  @Test
  public void testAtLeastOneObserverNormal() throws Exception {
    // 4 namenodes: 2 observers, 1 active, 1 standby.
    // But there is no active namenode in router cache.
    initEnv(2, true);
    // Shutdown one observer.
    stopObserver(1);

    /*
     * The first msync operation will cause NoNamenodesAvailableException and RotatedCache.
     * After retrying, msync operation will be executed successfully.
     * Each read request will shuffle the observer,
     * if the getFileInfo operation is sent to the downed observer,
     * it will cause NoNamenodesAvailableException,
     * at this time, the request can be retried to the normal observer,
     * no NoNamenodesAvailableException will be generated and the operation will be successful.
     */
    fileSystem.getFileStatus(new Path("/"));
    assertEquals(1, rpcMetrics.getProxyOpNoNamenodes());
    assertEquals(1, rpcMetrics.getObserverProxyOps());

    // At this time, the router has recorded 2 standby namenodes in memory,
    // the operation can be successful without waiting for the router load cache.
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", true));
  }

  /**
   * If all obervers are down, read requests can succeed,
   * even if a NoNamenodesAvailableException occurs.
   */
  @Test
  public void testAllObserverAbnormality() throws Exception {
    // 4 namenodes: 2 observers, 1 active, 1 standby.
    // But there is no active namenode in router cache.
    initEnv(2, true);
    // Shutdown all observers.
    stopObserver(2);

    /*
     * The first msync operation will cause NoNamenodesAvailableException and RotatedCache.
     * After retrying, msync operation will be executed successfully.
     * The getFileInfo operation retried 2 namenodes, both causing UnavailableException,
     * and continued to retry to the standby namenode,
     * causing NoNamenodesAvailableException and RotatedCache,
     * and the execution was successful after retrying.
     */
    fileSystem.getFileStatus(new Path("/"));
    assertEquals(2, rpcMetrics.getProxyOpFailureCommunicate());
    assertEquals(2, rpcMetrics.getProxyOpNoNamenodes());

    // At this time, the router has recorded 2 standby namenodes in memory,
    // the operation can be successful without waiting for the router load cache.
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", true));
  }

  /**
   * Determine whether cache of the router has an active namenode.
   *
   * @return true if no active namenode, otherwise false.
   */
  private boolean routerCacheNoActiveNamenode(
      RouterContext context, String nsId, boolean useObserver) throws IOException {
    List<? extends FederationNamenodeContext> namenodes
        = context.getRouter().getNamenodeResolver().getNamenodesForNameserviceId(nsId, useObserver);
    for (FederationNamenodeContext namenode : namenodes) {
      if (namenode.getState() == FederationNamenodeServiceState.ACTIVE){
        return false;
      }
    }
    return true;
  }

  /**
   * All routers in the cluster force loadcache.
   */
  private void allRoutersLoadCache() {
    for (MiniRouterDFSCluster.RouterContext context : cluster.getRouters()) {
      // Update service cache
      context.getRouter().getStateStore().refreshCaches(true);
    }
  }

  /**
   * Set the second non-observer state namenode in the router cache to active.
   */
  private void setSecondNonObserverNamenodeInTheRouterCacheActive(
      int numberOfObserver, boolean useObserver) throws IOException {
    List<? extends FederationNamenodeContext> ns0 = routerContext.getRouter()
        .getNamenodeResolver()
        .getNamenodesForNameserviceId("ns0", useObserver);

    String nsId = ns0.get(numberOfObserver+1).getNamenodeId();
    cluster.switchToActive("ns0", nsId);
    assertEquals(ACTIVE.ordinal(),
        cluster.getNamenode("ns0", nsId).getNamenode().getNameNodeState());

  }

  /**
   * All routers in the cluster force heartbeat.
   */
  private void allRoutersHeartbeat() throws IOException {
    for (RouterContext context : cluster.getRouters()) {
      // Manually trigger the heartbeat, but the router does not manually load the cache
      Collection<NamenodeHeartbeatService> heartbeatServices = context
          .getRouter().getNamenodeHeartbeatServices();
      for (NamenodeHeartbeatService service : heartbeatServices) {
        service.periodicInvoke();
      }
    }
  }

  /**
   * Transition the active namenode in the cluster to standby.
   */
  private void transitionActiveToStandby() {
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        List<MiniRouterDFSCluster.NamenodeContext>  nnList = cluster.getNamenodes(ns);
        for (MiniRouterDFSCluster.NamenodeContext namenodeContext : nnList) {
          if (namenodeContext.getNamenode().isActiveState()) {
            cluster.switchToStandby(ns, namenodeContext.getNamenodeId());
          }
        }
      }
    }
  }

  /**
   * Shutdown observer namenode in the cluster.
   *
   * @param num The number of shutdown observer.
   */
  private void stopObserver(int num) {
    int nnIndex;
    int numNns = cluster.getNamenodes().size();
    for (nnIndex = 0; nnIndex < numNns && num > 0; nnIndex++) {
      NameNode nameNode = cluster.getCluster().getNameNode(nnIndex);
      if (nameNode != null && nameNode.isObserverState()) {
        cluster.getCluster().shutdownNameNode(nnIndex);
        num--;
      }
    }
  }
}
