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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RouterResolveException;
import org.apache.hadoop.hdfs.server.federation.router.NoLocationException;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.router.TestRouterMountTableWithoutDefaultNS;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RemoteException;
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

public class TestRouterAsyncMountTableWithoutDefaultNS extends TestRouterMountTableWithoutDefaultNS {
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
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    // Get the end points
    nnFs0 = cluster.getNamenode("ns0", null).getFileSystem();
    nnFs1 = cluster.getNamenode("ns1", null).getFileSystem();
    routerContext = cluster.getRandomRouter();
    routerFs = routerContext.getFileSystem();
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
   * Verify that RBF doesn't support get the file information
   * with no location and sub mount points.
   */
  @Test
  public void testGetFileInfoWithoutSubMountPoint() throws Exception {
    MountTable addEntry = MountTable.newInstance("/testdir/1",
        Collections.singletonMap("ns0", "/testdir/1"));
    assertTrue(addMountTable(addEntry));
    LambdaTestUtils.intercept(RemoteException.class,
        "org.apache.hadoop.hdfs.server.federation.resolver.RouterResolveException",
        () -> routerFs.getFileStatus(new Path("/testdir2")));
  }

  @Test
  public void testGetContentSummary() throws Exception {
    try {
      // Add mount table entry.
      MountTable addEntry = MountTable.newInstance("/testA",
          Collections.singletonMap("ns0", "/testA"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testA/testB",
          Collections.singletonMap("ns0", "/testA/testB"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testA/testB/testC",
          Collections.singletonMap("ns1", "/testA/testB/testC"));
      assertTrue(addMountTable(addEntry));

      writeData(nnFs0, new Path("/testA/testB/file1"), 1024 * 1024);
      writeData(nnFs1, new Path("/testA/testB/testC/file2"), 1024 * 1024);
      writeData(nnFs1, new Path("/testA/testB/testC/file3"), 1024 * 1024);

      ContentSummary summary = routerFs.getContentSummary(new Path("/testA"));
      assertEquals(3, summary.getFileCount());
      assertEquals(1024 * 1024 * 3, summary.getLength());

      LambdaTestUtils.intercept(RemoteException.class,
          "org.apache.hadoop.hdfs.server.federation.router.NoLocationException",
          () -> routerFs.getContentSummary(new Path("/testB")));
    } finally {
      nnFs0.delete(new Path("/testA"), true);
      nnFs1.delete(new Path("/testA"), true);
    }
  }

  /**
   * Verify that RBF that disable default nameservice should support
   * get information about ancestor mount points.
   */
  @Test
  public void testGetContentSummaryWithSubMountPoint() throws IOException {
    MountTable addEntry = MountTable.newInstance("/testdir/1/2",
        Collections.singletonMap("ns0", "/testdir/1/2"));
    assertTrue(addMountTable(addEntry));

    try {
      writeData(nnFs0, new Path("/testdir/1/2/3"), 10 * 1024 * 1024);
      ContentSummary summaryFromRBF = routerFs.getContentSummary(new Path("/testdir"));
      assertNotNull(summaryFromRBF);
      assertEquals(1, summaryFromRBF.getFileCount());
      assertEquals(10 * 1024 * 1024, summaryFromRBF.getLength());
    } finally {
      nnFs0.delete(new Path("/testdir"), true);
    }
  }
}
