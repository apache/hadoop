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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_HANDLER_COUNT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_RESPONDER_COUNT;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.syncReturn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Used to test the functionality of {@link RouterAsyncRpcClient}.
 */
public class TestRouterAsyncRpcClient {
  private static Configuration routerConf;
  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;
  private static String ns0;
  private static String ns1;

  /** Random Router for this federated cluster. */
  private MiniRouterDFSCluster.RouterContext router;
  private FileSystem routerFs;
  private RouterRpcServer routerRpcServer;
  private RouterAsyncRpcClient asyncRpcClient;
  private FederationRPCMetrics rpcMetrics;
  private final String testFile = "/test.file";

  /**
   * Start a cluster using a router service that includes 2 namespaces,
   * 6 namenodes and 6 datanodes.
   */
  @BeforeClass
  public static void setUpCluster() throws Exception {
    cluster = new MiniRouterDFSCluster(true, 2, 3,
        DEFAULT_HEARTBEAT_INTERVAL_MS, 1000);
    cluster.setNumDatanodesPerNameservice(3);

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
    routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();

    // Reduce the number of RPC clients threads to overload the Router easy
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE, 1);
    routerConf.setInt(DFS_ROUTER_RPC_ASYNC_HANDLER_COUNT, 1);
    routerConf.setInt(DFS_ROUTER_RPC_ASYNC_RESPONDER_COUNT, 1);
    // We decrease the DN cache times to make the test faster
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
    cluster.addRouterOverrides(routerConf);
    // Start routers with only an RPC service
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    cluster.waitActiveNamespaces();
    ns0 = cluster.getNameservices().get(0);
    ns1 = cluster.getNameservices().get(1);
  }

  @AfterClass
  public static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Initialize the mount table, create a RouterAsyncRpcClient object, and create test file.
   */
  @Before
  public void setup() throws Exception {
    // Create mock locations
    installMockLocations();

    router = cluster.getRandomRouter();
    rpcMetrics = router.getRouter().getRpcServer().getRPCMetrics();
    routerFs = router.getFileSystem();
    routerRpcServer = router.getRouterRpcServer();
    routerRpcServer.initAsyncThreadPool();

    // Create a RouterAsyncRpcClient object
    asyncRpcClient = new RouterAsyncRpcClient(
        routerConf, router.getRouter(), routerRpcServer.getNamenodeResolver(),
        routerRpcServer.getRPCMonitor(),
        routerRpcServer.getRouterStateIdContext());

    // Create a test file
    FSDataOutputStream fsDataOutputStream = routerFs.create(
        new Path(testFile), true);
    fsDataOutputStream.write(new byte[1024]);
    fsDataOutputStream.close();
  }

  @After
  public void down() throws IOException {
    // clear client context
    CallerContext.setCurrent(null);
    cluster.switchToActive(ns0, NAMENODES[0]);
    asyncRpcClient.getNamenodeResolver().updateActiveNamenode(
        ns0, NetUtils.createSocketAddr(cluster
            .getNamenode(ns0, NAMENODES[0]).getRpcAddress()));
    // Delete the test file
    boolean delete = routerFs.delete(new Path(testFile));
    assertTrue(delete);
    if (routerFs != null) {
      routerFs.close();
    }
  }

  /**
   * Test the functionality of the asynchronous invokeSingle method.
   */
  @Test
  public void testInvokeSingle() throws Exception {
    long proxyOps = rpcMetrics.getProxyOps();
    long activeProxyOps = rpcMetrics.getActiveProxyOps();
    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getTransactionID");
    asyncRpcClient.invokeSingle(ns0, method);
    long id = syncReturn(Long.class);
    assertTrue(id > 0);
    assertEquals(proxyOps + 1, rpcMetrics.getProxyOps());
    assertEquals(activeProxyOps + 1, rpcMetrics.getActiveProxyOps());
    assertTrue(rpcMetrics.getProcessingAvg() > 0);
    assertTrue(rpcMetrics.getProxyAvg() > 0);
  }

  /**
   * Test the functionality of the asynchronous invokeAll and invokeConcurrent methods.
   */
  @Test
  public void testInvokeAll() throws Exception {
    long proxyOps = rpcMetrics.getProxyOps();
    long activeProxyOps = rpcMetrics.getActiveProxyOps();
    final List<RemoteLocation> locations =
        routerRpcServer.getLocationsForPath("/multDes/dir", false);
    RemoteMethod method = new RemoteMethod("mkdirs",
        new Class<?>[] {String.class, FsPermission.class, boolean.class},
        new RemoteParam(), new FsPermission(ALL, ALL, ALL), false);
    asyncRpcClient.invokeAll(locations, method);
    LambdaTestUtils.intercept(FileNotFoundException.class,
        "Parent directory doesn't exist: /multDes",
        () -> syncReturn(boolean.class));
    assertEquals(proxyOps + 2, rpcMetrics.getProxyOps());
    assertEquals(activeProxyOps + 2, rpcMetrics.getActiveProxyOps());

    proxyOps = rpcMetrics.getProxyOps();
    activeProxyOps = rpcMetrics.getActiveProxyOps();
    method = new RemoteMethod("mkdirs",
        new Class<?>[] {String.class, FsPermission.class, boolean.class},
        new RemoteParam(), new FsPermission(ALL, ALL, ALL), true);
    asyncRpcClient.invokeAll(locations, method);
    Boolean success = syncReturn(Boolean.class);
    assertTrue(success);
    assertEquals(proxyOps + 2, rpcMetrics.getProxyOps());
    assertEquals(activeProxyOps + 2, rpcMetrics.getActiveProxyOps());

    FileStatus[] fileStatuses = routerFs.listStatus(new Path("/multDes"));
    assertNotNull(fileStatuses);
    assertTrue(rpcMetrics.getProcessingAvg() > 0);
    assertTrue(rpcMetrics.getProxyAvg() > 0);
  }

  /**
   * Test the functionality of the asynchronous invokeMethod method.
   */
  @Test
  public void testInvokeMethod() throws Exception {
    long proxyOps = rpcMetrics.getProxyOps();
    long activeProxyOps = rpcMetrics.getActiveProxyOps();
    RemoteMethod method = new RemoteMethod("getFileInfo",
        new Class<?>[] {String.class}, new RemoteParam());
    UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    Class<?> protocol = method.getProtocol();
    Object[] params = new String[]{testFile};
    List<? extends FederationNamenodeContext> namenodes =
        asyncRpcClient.getOrderedNamenodes(ns0, false);
    asyncRpcClient.invokeMethod(ugi, namenodes, false,
        protocol, method.getMethod(), params);
    FileStatus fileStatus = syncReturn(FileStatus.class);
    assertEquals(1024, fileStatus.getLen());
    assertEquals(proxyOps + 1, rpcMetrics.getProxyOps());
    assertEquals(activeProxyOps + 1, rpcMetrics.getActiveProxyOps());

    LambdaTestUtils.intercept(IOException.class,
        "No namenodes to invoke",
        () -> asyncRpcClient.invokeMethod(ugi, new ArrayList<>(), false,
            protocol, method.getMethod(), params));

    proxyOps = rpcMetrics.getProxyOps();
    activeProxyOps = rpcMetrics.getActiveProxyOps();
    asyncRpcClient.invokeMethod(ugi, namenodes.subList(1, 3), false,
        protocol, method.getMethod(), params);
    LambdaTestUtils.intercept(StandbyException.class,
        "No namenode available to invoke getFileInfo",
        () -> syncReturn(FileStatus.class));
    assertEquals(proxyOps, rpcMetrics.getProxyOps());
    assertEquals(activeProxyOps, rpcMetrics.getActiveProxyOps());

    cluster.switchToStandby(ns0, NAMENODES[0]);
    asyncRpcClient.getNamenodeResolver().updateUnavailableNamenode(
        ns0, NetUtils.createSocketAddr(namenodes.get(0).getRpcAddress()));
    asyncRpcClient.invokeMethod(ugi, namenodes, false,
        protocol, method.getMethod(), params);
    LambdaTestUtils.intercept(RetriableException.class,
        "No namenodes available under nameservice ns0",
        () -> syncReturn(FileStatus.class));
    assertEquals(1, rpcMetrics.getProxyOpNoNamenodes());

    asyncRpcClient.invokeMethod(ugi, namenodes, false,
        null, method.getMethod(), params);
    LambdaTestUtils.intercept(StandbyException.class,
        "Cannot get a connection",
        () -> syncReturn(FileStatus.class));
    assertEquals(1, rpcMetrics.getProxyOpFailureCommunicate());
  }

  /**
   * Test the functionality of the asynchronous invokeSequential method.
   */
  @Test
  public void testInvokeSequential() throws Exception {
    List<RemoteLocation> locations =
        routerRpcServer.getLocationsForPath(testFile, false, false);
    RemoteMethod remoteMethod = new RemoteMethod("getBlockLocations",
        new Class<?>[] {String.class, long.class, long.class},
        new RemoteParam(), 0, 1024);
    asyncRpcClient.invokeSequential(locations, remoteMethod,
        LocatedBlocks.class, null);
    LocatedBlocks locatedBlocks = syncReturn(LocatedBlocks.class);
    assertEquals(1024, locatedBlocks.getFileLength());
    assertEquals(1, locatedBlocks.getLocatedBlocks().size());
  }

  /**
   * Initialize the mount information.
   */
  private void installMockLocations() {
    List<MiniRouterDFSCluster.RouterContext> routers = cluster.getRouters();

    for (MiniRouterDFSCluster.RouterContext rc : routers) {
      Router r = rc.getRouter();
      MockResolver resolver = (MockResolver) r.getSubclusterResolver();
      resolver.addLocation("/", ns0, "/");
      resolver.addLocation("/multDes", ns0, "/multDes");
      resolver.addLocation("/multDes", ns1, "/multDes");
    }
  }
}
