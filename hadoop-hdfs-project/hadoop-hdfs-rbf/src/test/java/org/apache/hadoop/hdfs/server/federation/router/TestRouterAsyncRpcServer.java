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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.ipc.CallerContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_HANDLER_COUNT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_RESPONDER_COUNT;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.syncReturn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Used to test the async functionality of {@link RouterRpcServer}.
 */
public class TestRouterAsyncRpcServer {
  private static Configuration routerConf;
  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;
  private static String ns0;

  /** Random Router for this federated cluster. */
  private MiniRouterDFSCluster.RouterContext router;
  private FileSystem routerFs;
  private RouterRpcServer asyncRouterRpcServer;

  @BeforeClass
  public static void setUpCluster() throws Exception {
    cluster = new MiniRouterDFSCluster(true, 1, 2,
        DEFAULT_HEARTBEAT_INTERVAL_MS, 1000);
    cluster.setNumDatanodesPerNameservice(3);

    cluster.startCluster();

    // Making one Namenode active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
      }
    }
    // Start routers with only an RPC service
    routerConf = new RouterConfigBuilder()
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
  }

  @AfterClass
  public static void shutdownCluster() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() throws IOException {
    router = cluster.getRandomRouter();
    routerFs = router.getFileSystem();
    RouterRpcServer routerRpcServer = router.getRouterRpcServer();
    routerRpcServer.initAsyncThreadPool();
    RouterAsyncRpcClient asyncRpcClient = new RouterAsyncRpcClient(
        routerConf, router.getRouter(), routerRpcServer.getNamenodeResolver(),
        routerRpcServer.getRPCMonitor(),
        routerRpcServer.getRouterStateIdContext());
    asyncRouterRpcServer = Mockito.spy(routerRpcServer);
    Mockito.when(asyncRouterRpcServer.getRPCClient()).thenReturn(asyncRpcClient);

    // Create mock locations
    MockResolver resolver = (MockResolver) router.getRouter().getSubclusterResolver();
    resolver.addLocation("/", ns0, "/");
    FsPermission permission = new FsPermission("705");
    routerFs.mkdirs(new Path("/testdir"), permission);
  }

  @After
  public void tearDown() throws IOException {
    // clear client context
    CallerContext.setCurrent(null);
    boolean delete = routerFs.delete(new Path("/testdir"));
    assertTrue(delete);
    if (routerFs != null) {
      routerFs.close();
    }
  }

  /**
   * Test that the async RPC server can invoke a method at an available Namenode.
   */
  @Test
  public void testInvokeAtAvailableNsAsync() throws Exception {
    RemoteMethod method = new RemoteMethod("getStoragePolicies");
    asyncRouterRpcServer.invokeAtAvailableNsAsync(method, BlockStoragePolicy[].class);
    BlockStoragePolicy[] storagePolicies = syncReturn(BlockStoragePolicy[].class);
    assertEquals(8, storagePolicies.length);
  }

  /**
   * Test get create location async.
   */
  @Test
  public void testGetCreateLocationAsync() throws Exception {
    final List<RemoteLocation> locations =
        asyncRouterRpcServer.getLocationsForPath("/testdir", true);
    asyncRouterRpcServer.getCreateLocationAsync("/testdir", locations);
    RemoteLocation remoteLocation = syncReturn(RemoteLocation.class);
    assertNotNull(remoteLocation);
    assertEquals(ns0, remoteLocation.getNameserviceId());
  }

  /**
   * Test get datanode report async.
   */
  @Test
  public void testGetDatanodeReportAsync() throws Exception {
    asyncRouterRpcServer.getDatanodeReportAsync(
        HdfsConstants.DatanodeReportType.ALL, true, 0);
    DatanodeInfo[] datanodeInfos = syncReturn(DatanodeInfo[].class);
    assertEquals(3, datanodeInfos.length);

    // Get the namespace where the datanode is located
    asyncRouterRpcServer.getDatanodeStorageReportMapAsync(HdfsConstants.DatanodeReportType.ALL);
    Map<String, DatanodeStorageReport[]> map = syncReturn(Map.class);
    assertEquals(1, map.size());
    assertEquals(3, map.get(ns0).length);

    DatanodeInfo[] slowDatanodeReport1 =
        asyncRouterRpcServer.getSlowDatanodeReport(true, 0);

    asyncRouterRpcServer.getSlowDatanodeReportAsync(true, 0);
    DatanodeInfo[] slowDatanodeReport2 = syncReturn(DatanodeInfo[].class);
    assertEquals(slowDatanodeReport1, slowDatanodeReport2);
  }
}
