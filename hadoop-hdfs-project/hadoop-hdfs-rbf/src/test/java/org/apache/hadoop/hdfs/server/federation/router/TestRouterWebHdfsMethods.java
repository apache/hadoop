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

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createMountTableEntry;
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterConstants.ASYNC_MODE;
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterConstants.SYNC_MODE;
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterWebHdfsMethods.clearMountTable;
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterWebHdfsMethods.cluster;
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterWebHdfsMethods.globalSetUp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test suite for Router Web Hdfs methods.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public class TestRouterWebHdfsMethods {
  static final Logger LOG =
      LoggerFactory.getLogger(TestRouterWebHdfsMethods.class);

  protected static StateStoreDFSCluster cluster;
  protected static RouterContext routerContext;
  protected static MountTableResolver mountTable;
  protected static String httpUri;

  public static void globalSetUp(String rpcMode) throws Exception {
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .rpc()
        .http()
        .admin()
        .build();
    if (rpcMode.equals(ASYNC_MODE)) {
      routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    }

    cluster.addRouterOverrides(routerConf);
    cluster.setIndependentDNs();
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
    routerContext = cluster.getRandomRouter();
    httpUri = "http://" + routerContext.getHttpAddress();
    Router router = routerContext.getRouter();
    mountTable = (MountTableResolver) router.getSubclusterResolver();
  }

  @Nested
  @ExtendWith(RouterServerHelperInTestRouterWebHdfsMethods.class)
  class TestWithAsyncRouterRpc {
    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testWebHdfsCreateAsync(String rpcMode) throws Exception {
      testWebHdfsCreate();
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testWebHdfsCreateWithMountsAsync(String rpcMode) throws Exception {
      testWebHdfsCreate();
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testGetNsFromDataNodeNetworkLocationAsync(String rpcMode) {
      testGetNsFromDataNodeNetworkLocation();
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testWebHdfsCreateWithInvalidPathAsync(String rpcMode) throws Exception {
      testWebHdfsCreateWithInvalidPath();
    }

  }

  @Nested
  @ExtendWith(RouterServerHelperInTestRouterWebHdfsMethods.class)
  class TestWithSyncRouterRpc {
    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testWebHdfsCreateSync(String rpcMode) throws Exception {
      testWebHdfsCreate();
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testWebHdfsCreateWithMountsSync(String rpcMode) throws Exception {
      testWebHdfsCreateWithMounts();
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testGetNsFromDataNodeNetworkLocationSync(String rpcMode) {
      testGetNsFromDataNodeNetworkLocation();
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testWebHdfsCreateWithInvalidPathSync(String rpcMode) throws Exception {
      testWebHdfsCreateWithInvalidPath();
    }
  }

  public void testWebHdfsCreate() throws Exception {
    // the file is created at default ns (ns0)
    String path = "/tmp/file";
    URL url = new URL(getUri(path));
    LOG.info("URL: {}", url);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    verifyFile("ns0", path, true);
    verifyFile("ns1", path, false);
    conn.disconnect();
  }

  public void testWebHdfsCreateWithMounts() throws Exception {
    // the file is created at mounted ns (ns1)
    String mountPoint = "/tmp-ns1";
    String path = "/tmp-ns1/file";
    createMountTableEntry(
        routerContext.getRouter(), mountPoint,
        DestinationOrder.RANDOM, Collections.singletonList("ns1"));
    URL url = new URL(getUri(path));
    LOG.info("URL: {}", url);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    verifyFile("ns1", path, true);
    verifyFile("ns0", path, false);
    conn.disconnect();
  }

  private String getUri(String path) {
    final String user = System.getProperty("user.name");
    final StringBuilder uri = new StringBuilder(httpUri);
    uri.append("/webhdfs/v1").
        append(path).
        append("?op=CREATE").
        append("&user.name=" + user);
    return uri.toString();
  }

  private void verifyFile(String ns, String path, boolean shouldExist)
      throws Exception {
    FileSystem fs = cluster.getNamenode(ns, null).getFileSystem();
    try {
      fs.getFileStatus(new Path(path));
      if (!shouldExist) {
        fail(path + " should not exist in ns " + ns);
      }
    } catch (FileNotFoundException e) {
      if (shouldExist) {
        fail(path + " should exist in ns " + ns);
      }
    }
  }

  public void testGetNsFromDataNodeNetworkLocation() {
    assertEquals("ns0", RouterWebHdfsMethods
        .getNsFromDataNodeNetworkLocation("/ns0/rack-info1"));
    assertEquals("ns0", RouterWebHdfsMethods
        .getNsFromDataNodeNetworkLocation("/ns0/row1/rack-info1"));
    assertEquals("", RouterWebHdfsMethods
        .getNsFromDataNodeNetworkLocation("/row0"));
    assertEquals("", RouterWebHdfsMethods
        .getNsFromDataNodeNetworkLocation("whatever-rack-info1"));
  }

  public void testWebHdfsCreateWithInvalidPath() throws Exception {
    // A path name include duplicated slashes.
    String path = "//tmp//file";
    assertResponse(path);
  }

  private void assertResponse(String path) throws IOException {
    URL url = new URL(getUri(path));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    // Assert response code.
    assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, conn.getResponseCode());
    // Assert exception.
    Map<?, ?> response = WebHdfsFileSystem.jsonParse(conn, true);
    assertEquals("InvalidPathException",
        ((LinkedHashMap) response.get("RemoteException")).get("exception"));
    conn.disconnect();
  }

  public static void clearMountTable() throws IOException {
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
}

class RouterServerHelperInTestRouterWebHdfsMethods implements
    BeforeEachCallback, AfterAllCallback {

  private static final ThreadLocal<RouterServerHelperInTestRouterWebHdfsMethods>
      TEST_ROUTER_SERVER_TL = new InheritableThreadLocal<>();

  @Override
  public void afterAll(ExtensionContext context) throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    TEST_ROUTER_SERVER_TL.remove();
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    Method testMethod = context.getRequiredTestMethod();
    ValueSource enumAnnotation = testMethod.getAnnotation(ValueSource.class);
    if (enumAnnotation != null) {
      String[] strings = enumAnnotation.strings();
      for (String rpcMode : strings) {
        if (TEST_ROUTER_SERVER_TL.get() == null) {
          globalSetUp(rpcMode);
        }
      }
    }
    TEST_ROUTER_SERVER_TL.set(RouterServerHelperInTestRouterWebHdfsMethods.this);
    cluster.deleteAllFiles();
    clearMountTable();
  }
}
