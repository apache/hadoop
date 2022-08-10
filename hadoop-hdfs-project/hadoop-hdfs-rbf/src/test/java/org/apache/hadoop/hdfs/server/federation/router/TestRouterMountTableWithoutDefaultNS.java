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
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RouterResolveException;
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

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test a router end-to-end including the MountTable without default nameservice.
 */
public class TestRouterMountTableWithoutDefaultNS {
  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static MountTableResolver mountTable;
  private static ClientProtocol routerProtocol;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    conf.setInt(RBFConfigKeys.DFS_ROUTER_ADMIN_MAX_COMPONENT_LENGTH_KEY, 20);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_DEFAULT_NAMESERVICE_ENABLE, false);
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    // Get the end points
    routerContext = cluster.getRandomRouter();
    Router router = routerContext.getRouter();
    routerProtocol = routerContext.getClient().getNamenode();
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
    GetMountTableEntriesRequest req1 = GetMountTableEntriesRequest.newInstance("/");
    GetMountTableEntriesResponse response = mountTableManager.getMountTableEntries(req1);
    for (MountTable entry : response.getEntries()) {
      RemoveMountTableEntryRequest req2 =
          RemoveMountTableEntryRequest.newInstance(entry.getSourcePath());
      mountTableManager.removeMountTableEntry(req2);
    }
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
    AddMountTableEntryRequest addRequest = AddMountTableEntryRequest.newInstance(entry);
    AddMountTableEntryResponse addResponse = mountTableManager.addMountTableEntry(addRequest);

    // Reload the Router cache
    mountTable.loadCache(true);

    return addResponse.getStatus();
  }

  /**
   * Verify that RBF that disable default nameservice should support
   * get information about ancestor mount points.
   */
  @Test
  public void testGetFileInfoWithSubMountPoint() throws IOException {
    MountTable addEntry = MountTable.newInstance("/testdir/1",
        Collections.singletonMap("ns0", "/testdir/1"));
    assertTrue(addMountTable(addEntry));
    HdfsFileStatus finfo = routerProtocol.getFileInfo("/testdir");
    assertNotNull(finfo);
    assertEquals("supergroup", finfo.getGroup());
    assertTrue(finfo.isDirectory());
  }

  /**
   * Verify that RBF doesn't support get the file information
   * with no location and sub mount points.
   */
  @Test
  public void testGetFileInfoWithoutSubMountPoint() throws Exception {
    MountTable addEntry = MountTable.newInstance("/testdir/1",
        Collections.singletonMap("ns0", "/testdir/1"));
    assertTrue(addMountTable(addEntry));
    LambdaTestUtils.intercept(RouterResolveException.class,
        () -> routerContext.getRouter().getRpcServer().getFileInfo("/testdir2"));
  }
}
