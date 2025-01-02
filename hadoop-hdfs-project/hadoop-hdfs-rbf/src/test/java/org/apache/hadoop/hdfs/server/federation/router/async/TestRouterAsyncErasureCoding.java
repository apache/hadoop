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
package org.apache.hadoop.hdfs.server.federation.router.async;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.ipc.CallerContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_HANDLER_COUNT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_RESPONDER_COUNT;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestRouterAsyncErasureCoding {
  private static Configuration routerConf;
  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;
  private static String ns0;

  /** Random Router for this federated cluster. */
  private MiniRouterDFSCluster.RouterContext router;
  private FileSystem routerFs;
  private RouterRpcServer routerRpcServer;
  private AsyncErasureCoding asyncErasureCoding;

  private final String testfilePath = "/testdir/testAsyncErasureCoding.file";

  @BeforeClass
  public static void setUpCluster() throws Exception {
    cluster = new MiniRouterDFSCluster(true, 1, 2,
        DEFAULT_HEARTBEAT_INTERVAL_MS, 1000);
    cluster.setNumDatanodesPerNameservice(3);
    cluster.setRacks(
        new String[] {"/rack1", "/rack2", "/rack3"});
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
    routerRpcServer = router.getRouterRpcServer();
    routerRpcServer.initAsyncThreadPool();
    RouterAsyncRpcClient asyncRpcClient = new RouterAsyncRpcClient(
        routerConf, router.getRouter(), routerRpcServer.getNamenodeResolver(),
        routerRpcServer.getRPCMonitor(),
        routerRpcServer.getRouterStateIdContext(),
        routerRpcServer.getAsyncRouterHandler());
    RouterRpcServer spy = Mockito.spy(routerRpcServer);
    Mockito.when(spy.getRPCClient()).thenReturn(asyncRpcClient);
    asyncErasureCoding = new AsyncErasureCoding(spy);

    // Create mock locations
    MockResolver resolver = (MockResolver) router.getRouter().getSubclusterResolver();
    resolver.addLocation("/", ns0, "/");
    FsPermission permission = new FsPermission("705");
    routerFs.mkdirs(new Path("/testdir"), permission);
    FSDataOutputStream fsDataOutputStream = routerFs.create(
        new Path(testfilePath), true);
    fsDataOutputStream.write(new byte[1024]);
    fsDataOutputStream.close();
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

  @Test
  public void testRouterAsyncErasureCoding() throws Exception {
    String ecPolicyName = StripedFileTestUtil.getDefaultECPolicy().getName();
    HdfsFileStatus fileInfo = cluster.getNamenodes().get(0).getClient().getFileInfo(testfilePath);
    assertNotNull(fileInfo);

    asyncErasureCoding.setErasureCodingPolicy("/testdir", ecPolicyName);
    syncReturn(null);

    asyncErasureCoding.getErasureCodingPolicy("/testdir");
    ErasureCodingPolicy ecPolicy = syncReturn(ErasureCodingPolicy.class);
    assertEquals(StripedFileTestUtil.getDefaultECPolicy().getName(), ecPolicy.getName());

    asyncErasureCoding.getErasureCodingPolicies();
    ErasureCodingPolicyInfo[] erasureCodingPolicies = syncReturn(ErasureCodingPolicyInfo[].class);
    int numECPolicies = erasureCodingPolicies.length;
    ErasureCodingPolicyInfo[] erasureCodingPoliciesFromNameNode =
        cluster.getNamenodes().get(0).getClient().getErasureCodingPolicies();

    assertArrayEquals(erasureCodingPoliciesFromNameNode, erasureCodingPolicies);

    asyncErasureCoding.getErasureCodingCodecs();
    Map<String, String> erasureCodingCodecs = syncReturn(Map.class);
    Map<String, String> erasureCodingCodecsFromNameNode =
        cluster.getNamenodes().get(0).getClient().getErasureCodingCodecs();

    assertEquals(erasureCodingCodecs, erasureCodingCodecsFromNameNode);

    // RS-12-4-1024k
    final ECSchema schema = new ECSchema("rs", 12, 4);
    ErasureCodingPolicy erasureCodingPolicy = new ErasureCodingPolicy(schema, 1024 * 1024);
    asyncErasureCoding.addErasureCodingPolicies(new ErasureCodingPolicy[]{erasureCodingPolicy});
    AddErasureCodingPolicyResponse[] response = syncReturn(AddErasureCodingPolicyResponse[].class);
    assertEquals(response[0].isSucceed(), true);

    asyncErasureCoding.getErasureCodingPolicies();
    ErasureCodingPolicyInfo[] erasureCodingPolicies2 = syncReturn(ErasureCodingPolicyInfo[].class);
    int numNewECPolicies = erasureCodingPolicies2.length;
    assertEquals(numECPolicies + 1, numNewECPolicies);

    asyncErasureCoding.getECTopologyResultForPolicies(
        new String[]{"RS-6-3-1024k", "RS-12-4-1024k"});
    ECTopologyVerifierResult ecTResultForPolicies = syncReturn(ECTopologyVerifierResult.class);
    assertEquals(false, ecTResultForPolicies.isSupported());

    asyncErasureCoding.getECTopologyResultForPolicies(
        new String[]{"XOR-2-1-1024k"});
    ECTopologyVerifierResult ecTResultForPolicies2 = syncReturn(ECTopologyVerifierResult.class);
    assertEquals(true, ecTResultForPolicies2.isSupported());
  }
}
