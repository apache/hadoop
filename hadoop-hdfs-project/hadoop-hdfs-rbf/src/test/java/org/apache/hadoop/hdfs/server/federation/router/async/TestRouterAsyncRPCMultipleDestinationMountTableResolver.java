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
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.resolver.order.LocalResolver;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterClientProtocol;
import org.apache.hadoop.hdfs.server.federation.router.RouterQuotaUsage;
import org.apache.hadoop.hdfs.server.federation.router.TestRouterRPCMultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests router async rpc with multiple destination mount table resolver.
 */
public class TestRouterAsyncRPCMultipleDestinationMountTableResolver extends
    TestRouterRPCMultipleDestinationMountTableResolver {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestRouterAsyncRPCMultipleDestinationMountTableResolver.class);

  @BeforeAll
  public static void setUp() throws Exception {

    // Build and start a federated cluster.
    cluster = new StateStoreDFSCluster(false, 3,
        MultipleDestinationMountTableResolver.class);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .quota()
        .rpc()
        .build();
    routerConf.setBoolean(DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);

    Configuration hdfsConf = new Configuration(false);
    hdfsConf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);

    cluster.addRouterOverrides(routerConf);
    cluster.addNamenodeOverrides(hdfsConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    routerContext = cluster.getRandomRouter();
    resolver =
        (MountTableResolver) routerContext.getRouter().getSubclusterResolver();
    nnFs0 = (DistributedFileSystem) cluster
        .getNamenode(cluster.getNameservices().get(0), null).getFileSystem();
    nnFs1 = (DistributedFileSystem) cluster
        .getNamenode(cluster.getNameservices().get(1), null).getFileSystem();
    nnFs2 = (DistributedFileSystem) cluster
        .getNamenode(cluster.getNameservices().get(2), null).getFileSystem();
    routerFs = (DistributedFileSystem) routerContext.getFileSystem();
    rpcServer =routerContext.getRouter().getRpcServer();
  }

  @Test
  public void testLocalResolverGetDatanodesSubcluster() throws IOException {
    String testPath = "/testLocalResolverGetDatanodesSubcluster";
    Path path = new Path(testPath);
    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", testPath);
    destMap.put("ns1", testPath);
    nnFs0.mkdirs(path);
    nnFs1.mkdirs(path);
    MountTable addEntry =
        MountTable.newInstance(testPath, destMap);
    addEntry.setQuota(new RouterQuotaUsage.Builder().build());
    addEntry.setDestOrder(DestinationOrder.LOCAL);
    assertTrue(addMountTable(addEntry));

    Map<String, String> datanodesSubcluster = null;
    try {
      MultipleDestinationMountTableResolver resolver =
          (MultipleDestinationMountTableResolver) routerContext.getRouter().getSubclusterResolver();
      LocalResolver localResolver =
          (LocalResolver) resolver.getOrderedResolver(DestinationOrder.LOCAL);
      datanodesSubcluster = localResolver.getDatanodesSubcluster();
    } catch (Exception e) {
      LOG.info("Exception occurs when testLocalResolverGetDatanodesSubcluster.", e);
    } finally {
      RouterClient client = routerContext.getAdminClient();
      MountTableManager mountTableManager = client.getMountTableManager();
      RemoveMountTableEntryRequest req2 =
          RemoveMountTableEntryRequest.newInstance(testPath);
      mountTableManager.removeMountTableEntry(req2);
      nnFs0.delete(new Path(testPath), true);
      nnFs1.delete(new Path(testPath), true);
    }
    assertNotNull(datanodesSubcluster);
    assertFalse(datanodesSubcluster.isEmpty());
  }

  @Override
  @Test
  public void testInvokeAtAvailableNs() throws IOException {
    // Create a mount point with multiple destinations.
    Path path = new Path("/testInvokeAtAvailableNs");
    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", "/testInvokeAtAvailableNs");
    destMap.put("ns1", "/testInvokeAtAvailableNs");
    nnFs0.mkdirs(path);
    nnFs1.mkdirs(path);
    MountTable addEntry =
        MountTable.newInstance("/testInvokeAtAvailableNs", destMap);
    addEntry.setQuota(new RouterQuotaUsage.Builder().build());
    addEntry.setDestOrder(DestinationOrder.RANDOM);
    addEntry.setFaultTolerant(true);
    assertTrue(addMountTable(addEntry));

    // Make one subcluster unavailable.
    MiniDFSCluster dfsCluster = cluster.getCluster();
    dfsCluster.shutdownNameNode(0);
    dfsCluster.shutdownNameNode(1);
    try {
      // Verify that #invokeAtAvailableNs works by calling #getServerDefaults.
      RemoteMethod method = new RemoteMethod("getServerDefaults");
      FsServerDefaults serverDefaults = null;
      rpcServer.invokeAtAvailableNsAsync(method, FsServerDefaults.class);
      try {
        serverDefaults = syncReturn(FsServerDefaults.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      assertNotNull(serverDefaults);
    } finally {
      dfsCluster.restartNameNode(0);
      dfsCluster.restartNameNode(1);
    }
  }

  @Override
  @Test
  public void testIsMultiDestDir() throws Exception {
    RouterClientProtocol client =
        routerContext.getRouter().getRpcServer().getClientProtocolModule();
    setupOrderMountPath(DestinationOrder.HASH_ALL);
    // Should be true only for directory and false for all other cases.
    client.isMultiDestDirectory("/mount/dir");
    assertTrue(syncReturn(boolean.class));
    client.isMultiDestDirectory("/mount/nodir");
    assertFalse(syncReturn(boolean.class));
    client.isMultiDestDirectory("/mount/dir/file");
    assertFalse(syncReturn(boolean.class));
    routerFs.createSymlink(new Path("/mount/dir/file"),
        new Path("/mount/dir/link"), true);
    client.isMultiDestDirectory("/mount/dir/link");
    assertFalse(syncReturn(boolean.class));
    routerFs.createSymlink(new Path("/mount/dir/dir"),
        new Path("/mount/dir/linkDir"), true);
    client.isMultiDestDirectory("/mount/dir/linkDir");
    assertFalse(syncReturn(boolean.class));
    resetTestEnvironment();
    // Test single directory destination. Should be false for the directory.
    setupOrderMountPath(DestinationOrder.HASH);
    client.isMultiDestDirectory("/mount/dir");
    assertFalse(syncReturn(boolean.class));
  }
}
