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
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.ipc.CallerContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_HANDLER_COUNT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_RESPONDER_COUNT;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.syncReturn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRouterAsyncCacheAdmin {
  private static Configuration routerConf;
  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;
  private static String ns0;

  /** Random Router for this federated cluster. */
  private MiniRouterDFSCluster.RouterContext router;
  private FileSystem routerFs;
  private RouterRpcServer routerRpcServer;
  private RouterAsyncCacheAdmin asyncCacheAdmin;

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
    routerRpcServer = router.getRouterRpcServer();
    routerRpcServer.initAsyncThreadPool();
    RouterAsyncRpcClient asyncRpcClient = new RouterAsyncRpcClient(
        routerConf, router.getRouter(), routerRpcServer.getNamenodeResolver(),
        routerRpcServer.getRPCMonitor(),
        routerRpcServer.getRouterStateIdContext());
    RouterRpcServer spy = Mockito.spy(routerRpcServer);
    Mockito.when(spy.getRPCClient()).thenReturn(asyncRpcClient);
    asyncCacheAdmin = new RouterAsyncCacheAdmin(spy);

    // Create mock locations
    MockResolver resolver = (MockResolver) router.getRouter().getSubclusterResolver();
    resolver.addLocation("/", ns0, "/");
    FSDataOutputStream fsDataOutputStream = routerFs.create(
        new Path("/testCache.file"), true);
    fsDataOutputStream.write(new byte[1024]);
    fsDataOutputStream.close();
  }

  @After
  public void tearDown() throws IOException {
    // clear client context
    CallerContext.setCurrent(null);
    boolean delete = routerFs.delete(new Path("/testCache.file"));
    assertTrue(delete);
    if (routerFs != null) {
      routerFs.close();
    }
  }

  @Test
  public void testRouterAsyncCacheAdmin() throws Exception {
    asyncCacheAdmin.addCachePool(new CachePoolInfo("pool"));
    syncReturn(null);

    CacheDirectiveInfo path = new CacheDirectiveInfo.Builder().
        setPool("pool").
        setPath(new Path("/testCache.file")).
        build();
    asyncCacheAdmin.addCacheDirective(path, EnumSet.of(CacheFlag.FORCE));
    long result = syncReturn(long.class);
    assertEquals(1, result);

    asyncCacheAdmin.listCachePools("");
    BatchedEntries<CachePoolEntry> cachePoolEntries = syncReturn(BatchedEntries.class);
    assertEquals("pool", cachePoolEntries.get(0).getInfo().getPoolName());

    CacheDirectiveInfo filter = new CacheDirectiveInfo.Builder().
        setPool("pool").
        build();
    asyncCacheAdmin.listCacheDirectives(0, filter);
    BatchedEntries<CacheDirectiveEntry> cacheDirectiveEntries = syncReturn(BatchedEntries.class);
    assertEquals(new Path("/testCache.file"), cacheDirectiveEntries.get(0).getInfo().getPath());

    CachePoolInfo pool = new CachePoolInfo("pool").setOwnerName("pool_user");
    asyncCacheAdmin.modifyCachePool(pool);
    syncReturn(null);

    asyncCacheAdmin.listCachePools("");
    cachePoolEntries = syncReturn(BatchedEntries.class);
    assertEquals("pool_user", cachePoolEntries.get(0).getInfo().getOwnerName());

    path = new CacheDirectiveInfo.Builder().
        setPool("pool").
        setPath(new Path("/testCache.file")).
        setReplication((short) 2).
        setId(1L).
        build();
    asyncCacheAdmin.modifyCacheDirective(path, EnumSet.of(CacheFlag.FORCE));
    syncReturn(null);

    asyncCacheAdmin.listCacheDirectives(0, filter);
    cacheDirectiveEntries = syncReturn(BatchedEntries.class);
    assertEquals(Short.valueOf((short) 2), cacheDirectiveEntries.get(0).getInfo().getReplication());

    asyncCacheAdmin.removeCacheDirective(1L);
    syncReturn(null);
    asyncCacheAdmin.removeCachePool("pool");
    syncReturn(null);
  }
}