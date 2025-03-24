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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test a router end-to-end including the MountTable using async rpc.
 */
public class TestRouterAsyncMountTable {
  public static final Logger LOG = LoggerFactory.getLogger(TestRouterAsyncMountTable.class);

  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static MountTableResolver mountTable;
  private static FileSystem routerFs;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    // Build and start a federated cluster.
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    conf.setInt(RBFConfigKeys.DFS_ROUTER_ADMIN_MAX_COMPONENT_LENGTH_KEY, 20);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    routerContext = cluster.getRandomRouter();
    routerFs = routerContext.getFileSystem();
    Router router = routerContext.getRouter();
    mountTable = (MountTableResolver) router.getSubclusterResolver();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @After
  public void clearMountTable() throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    GetMountTableEntriesRequest req1 =
        GetMountTableEntriesRequest.newInstance("/");
    GetMountTableEntriesResponse response =
        mountTableManager.getMountTableEntries(req1);
    for (MountTable entry : response.getEntries()) {
      RemoveMountTableEntryRequest req2 =
          RemoveMountTableEntryRequest.newInstance(entry.getSourcePath());
      mountTableManager.removeMountTableEntry(req2);
    }
    mountTable.setDefaultNSEnable(true);
  }

  /**
   * Add a mount table entry to the mount table through the admin API.
   * @param entry Mount table entry to add.
   * @return If it was succesfully added.
   * @throws IOException Problems adding entries.
   */
  private boolean addMountTable(final MountTable entry) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(entry);
    AddMountTableEntryResponse addResponse =
        mountTableManager.addMountTableEntry(addRequest);

    // Reload the Router cache.
    mountTable.loadCache(true);

    return addResponse.getStatus();
  }

  @Test
  public void testGetEnclosingRoot() throws Exception {
    // Add a read only entry.
    MountTable readOnlyEntry = MountTable.newInstance(
        "/readonly", Collections.singletonMap("ns0", "/testdir"));
    readOnlyEntry.setReadOnly(true);
    assertTrue(addMountTable(readOnlyEntry));
    assertEquals(routerFs.getEnclosingRoot(new Path("/readonly")), new Path("/readonly"));

    assertEquals(routerFs.getEnclosingRoot(new Path("/regular")), new Path("/"));
    assertEquals(routerFs.getEnclosingRoot(new Path("/regular")),
        routerFs.getEnclosingRoot(routerFs.getEnclosingRoot(new Path("/regular"))));

    // Add a regular entry.
    MountTable regularEntry = MountTable.newInstance(
        "/regular", Collections.singletonMap("ns0", "/testdir"));
    assertTrue(addMountTable(regularEntry));
    assertEquals(routerFs.getEnclosingRoot(new Path("/regular")), new Path("/regular"));

    // Path does not need to exist.
    assertEquals(routerFs.getEnclosingRoot(new Path("/regular/pathDNE")), new Path("/regular"));
  }

  @Test
  public void testListNonExistPath() throws Exception {
    mountTable.setDefaultNSEnable(false);
    LambdaTestUtils.intercept(FileNotFoundException.class,
        "File /base does not exist.",
        "Expect FileNotFoundException.",
        () -> routerFs.listStatus(new Path("/base")));
  }
}
