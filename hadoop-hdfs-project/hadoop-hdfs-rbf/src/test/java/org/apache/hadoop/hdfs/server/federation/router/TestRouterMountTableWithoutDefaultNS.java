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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RouterResolveException;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.federation.router.TestRouterConstants.ASYNC_MODE;
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterConstants.SYNC_MODE;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test a router end-to-end including the MountTable without default nameservice.
 */
public class TestRouterMountTableWithoutDefaultNS {
  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static MountTableResolver mountTable;
  private static ClientProtocol routerProtocol;
  private static FileSystem nnFs0;
  private static FileSystem nnFs1;

  public static StateStoreDFSCluster getCluster() {
    return cluster;
  }

  public static void setCluster(StateStoreDFSCluster cluster) {
    TestRouterMountTableWithoutDefaultNS.cluster = cluster;
  }

  public static RouterContext getRouterContext() {
    return routerContext;
  }

  public static void setRouterContext(RouterContext routerContext) {
    TestRouterMountTableWithoutDefaultNS.routerContext = routerContext;
  }

  public static void globalSetUp(String rpcMode) throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    conf.setInt(RBFConfigKeys.DFS_ROUTER_ADMIN_MAX_COMPONENT_LENGTH_KEY, 20);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_DEFAULT_NAMESERVICE_ENABLE, false);
    if (rpcMode.equals(ASYNC_MODE)) {
      conf.setBoolean(RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    }
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    // Get the end points
    nnFs0 = cluster.getNamenode("ns0", null).getFileSystem();
    nnFs1 = cluster.getNamenode("ns1", null).getFileSystem();
    routerContext = cluster.getRandomRouter();
    Router router = routerContext.getRouter();
    routerProtocol = routerContext.getClient().getNamenode();
    mountTable = (MountTableResolver) router.getSubclusterResolver();
  }

  public static void clearMountTable() throws IOException {
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

  @Nested
  @ExtendWith(RouterServerHelperInTestRouterMountTableWithoutDefaultNS.class)
  class TestWithAsyncRouterRpc {
    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testGetFileInfoWithSubMountPointAsync() throws IOException {
      testGetFileInfoWithSubMountPoint();
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testGetFileInfoWithoutSubMountPointAsync() throws Exception {
      testGetFileInfoWithoutSubMountPoint(ASYNC_MODE);
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testGetContentSummaryWithSubMountPointAsync() throws Exception {
      testGetContentSummaryWithSubMountPoint(ASYNC_MODE);
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testGetAllLocationsAsync() throws IOException {
      testGetAllLocations();
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testGetLocationsForContentSummaryAsync() throws Exception {
      testGetLocationsForContentSummary();
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testGetContentSummaryAsync() throws Exception {
      testGetContentSummary(ASYNC_MODE);
    }
  }

  @Nested
  @ExtendWith(RouterServerHelperInTestRouterMountTableWithoutDefaultNS.class)
  class TestWithSyncRouterRpc {
    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testGetFileInfoWithSubMountPointSync() throws IOException {
      testGetFileInfoWithSubMountPoint();
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testGetFileInfoWithoutSubMountPointSync() throws Exception {
      testGetFileInfoWithoutSubMountPoint(SYNC_MODE);
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testGetContentSummaryWithSubMountPointSync() throws Exception {
      testGetContentSummaryWithSubMountPoint(SYNC_MODE);
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testGetAllLocationsSync() throws IOException {
      testGetAllLocations();
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testGetLocationsForContentSummarySync() throws Exception {
      testGetLocationsForContentSummary();
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testGetContentSummarySync() throws Exception {
      testGetContentSummary(SYNC_MODE);
    }
  }

  /**
   * Verify that RBF that disable default nameservice should support
   * get information about ancestor mount points.
   */
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
  public void testGetFileInfoWithoutSubMountPoint(String rpcMode) throws Exception {
    MountTable addEntry = MountTable.newInstance("/testdir/1",
        Collections.singletonMap("ns0", "/testdir/1"));
    assertTrue(addMountTable(addEntry));
    LambdaTestUtils.intercept(RouterResolveException.class,
        () -> {
          if (rpcMode.equals(ASYNC_MODE)) {
            routerContext.getRouter().getRpcServer().getFileInfo("/testdir2");
            syncReturn(HdfsFileStatus.class);
          } else {
            routerContext.getRouter().getRpcServer().getFileInfo("/testdir2");
          }
        });
  }

  /**
   * Verify that RBF that disable default nameservice should support
   * get information about ancestor mount points.
   */
  public void testGetContentSummaryWithSubMountPoint(String rpcMode) throws Exception {
    MountTable addEntry = MountTable.newInstance("/testdir/1/2",
        Collections.singletonMap("ns0", "/testdir/1/2"));
    assertTrue(addMountTable(addEntry));

    try {
      writeData(nnFs0, new Path("/testdir/1/2/3"), 10 * 1024 * 1024);

      RouterRpcServer routerRpcServer = routerContext.getRouterRpcServer();
      ContentSummary summaryFromRBF;
      if (rpcMode.equals(ASYNC_MODE)) {
        routerRpcServer.getContentSummary("/testdir");
        summaryFromRBF = syncReturn(ContentSummary.class);
      } else {
        summaryFromRBF = routerRpcServer.getContentSummary("/testdir");
      }
      assertNotNull(summaryFromRBF);
      assertEquals(1, summaryFromRBF.getFileCount());
      assertEquals(10 * 1024 * 1024, summaryFromRBF.getLength());
    } finally {
      nnFs0.delete(new Path("/testdir"), true);
    }
  }

  public void testGetAllLocations() throws IOException {
    // Add mount table entry.
    MountTable addEntry = MountTable.newInstance("/testA",
        Collections.singletonMap("ns0", "/testA"));
    assertTrue(addMountTable(addEntry));
    addEntry = MountTable.newInstance("/testA/testB",
        Collections.singletonMap("ns1", "/testA/testB"));
    assertTrue(addMountTable(addEntry));
    addEntry = MountTable.newInstance("/testA/testB/testC",
        Collections.singletonMap("ns2", "/testA/testB/testC"));
    assertTrue(addMountTable(addEntry));

    RouterClientProtocol protocol = routerContext.getRouterRpcServer().getClientProtocolModule();
    Map<String, List<RemoteLocation>> locations = protocol.getAllLocations("/testA");
    assertEquals(3, locations.size());
  }

  public void testGetLocationsForContentSummary() throws Exception {
    // Add mount table entry.
    MountTable addEntry = MountTable.newInstance("/testA/testB",
        Collections.singletonMap("ns0", "/testA/testB"));
    assertTrue(addMountTable(addEntry));
    addEntry = MountTable.newInstance("/testA/testB/testC",
        Collections.singletonMap("ns1", "/testA/testB/testC"));
    assertTrue(addMountTable(addEntry));

    RouterClientProtocol protocol = routerContext.getRouterRpcServer().getClientProtocolModule();
    List<RemoteLocation> locations = protocol.getLocationsForContentSummary("/testA");
    assertEquals(2, locations.size());

    for (RemoteLocation location : locations) {
      String nsId = location.getNameserviceId();
      if ("ns0".equals(nsId)) {
        assertEquals("/testA/testB", location.getDest());
      } else if ("ns1".equals(nsId)) {
        assertEquals("/testA/testB/testC", location.getDest());
      } else {
        fail("Unexpected NS " + nsId);
      }
    }

    LambdaTestUtils.intercept(NoLocationException.class,
        () -> protocol.getLocationsForContentSummary("/testB"));
  }

  public void testGetContentSummary(String rpcMode) throws Exception {
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

      RouterRpcServer routerRpcServer = routerContext.getRouterRpcServer();
      ContentSummary summary;
      if (rpcMode.equals(ASYNC_MODE)) {
        routerRpcServer.getContentSummary("/testA");
        summary = syncReturn(ContentSummary.class);
      } else {
        summary = routerRpcServer.getContentSummary("/testA");
      }
      assertEquals(3, summary.getFileCount());
      assertEquals(1024 * 1024 * 3, summary.getLength());

      LambdaTestUtils.intercept(NoLocationException.class,
          () -> routerRpcServer.getContentSummary("/testB"));
    } finally {
      nnFs0.delete(new Path("/testA"), true);
      nnFs1.delete(new Path("/testA"), true);
    }
  }

  void writeData(FileSystem fs, Path path, int fileLength) throws IOException {
    try (FSDataOutputStream outputStream = fs.create(path)) {
      for (int writeSize = 0; writeSize < fileLength; writeSize++) {
        outputStream.write(writeSize);
      }
    }
  }
}

class RouterServerHelperInTestRouterMountTableWithoutDefaultNS implements
    AfterAllCallback, BeforeEachCallback, AfterEachCallback {
  public static final ThreadLocal<RouterServerHelperInTestRouterMountTableWithoutDefaultNS>
      TEST_ROUTER_SERVER_TL = new InheritableThreadLocal<>();

  @Override
  public void afterAll(ExtensionContext context) {
    if (TestRouterMountTableWithoutDefaultNS.getCluster() != null) {
      TestRouterMountTableWithoutDefaultNS.getCluster().stopRouter(
          TestRouterMountTableWithoutDefaultNS.getRouterContext());
      TestRouterMountTableWithoutDefaultNS.getCluster().shutdown();
      TestRouterMountTableWithoutDefaultNS.setCluster(null);
    }
    TEST_ROUTER_SERVER_TL.remove();
  }

  @Override
  public void afterEach(ExtensionContext context) throws IOException {
    TestRouterMountTableWithoutDefaultNS.clearMountTable();
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    Method testMethod = context.getRequiredTestMethod();
    ValueSource enumAnnotation = testMethod.getAnnotation(ValueSource.class);
    if (enumAnnotation != null) {
      String[] strings = enumAnnotation.strings();
      for (String rpcMode : strings) {
        if (TEST_ROUTER_SERVER_TL.get() == null) {
          TestRouterMountTableWithoutDefaultNS.globalSetUp(rpcMode);
        }
      }
    }
    TEST_ROUTER_SERVER_TL.set(RouterServerHelperInTestRouterMountTableWithoutDefaultNS.this);
  }
}