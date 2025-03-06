/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.hadoop.hdfs.server.federation.router.TestRouterConstants.ASYNC_MODE;
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterConstants.SYNC_MODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createFile;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyFileExists;

/**
 * The the RPC interface of the {@link Router} implemented by
 * {@link RouterRpcServer}.
 * Tests covering the functionality of RouterRPCServer with
 * single nameService.
 */
public class TestRouterRpcSingleNS {

  /**
   * Federated HDFS cluster.
   */
  private static MiniRouterDFSCluster cluster;

  /**
   * Random Router for this federated cluster.
   */
  private static MiniRouterDFSCluster.RouterContext router;

  /**
   * Random nameservice in the federated cluster.
   */
  private static String ns;
  /**
   * First namenode in the nameservice.
   */
  private static MiniRouterDFSCluster.NamenodeContext namenode;

  /**
   * Client interface to the Router.
   */
  private static ClientProtocol routerProtocol;
  /**
   * Client interface to the Namenode.
   */
  private static ClientProtocol nnProtocol;

  /**
   * NameNodeProtocol interface to the Router.
   */
  private static NamenodeProtocol routerNamenodeProtocol;
  /**
   * NameNodeProtocol interface to the Namenode.
   */
  private static NamenodeProtocol nnNamenodeProtocol;

  /**
   * Filesystem interface to the Router.
   */
  private static FileSystem routerFS;
  /**
   * Filesystem interface to the Namenode.
   */
  private static FileSystem nnFS;

  /**
   * File in the Router.
   */
  private static String routerFile;

  /**
   * File in the Namenode.
   */
  private static String nnFile;

  public static MiniRouterDFSCluster getCluster() {
    return cluster;
  }

  public static void setCluster(MiniRouterDFSCluster cluster) {
    TestRouterRpcSingleNS.cluster = cluster;
  }

  public MiniRouterDFSCluster.RouterContext getRouter() {
    return router;
  }

  public static String getNs() {
    return ns;
  }

  public static String getNnFile() {
    return nnFile;
  }

  public static void setNnFile(String nnFile) {
    TestRouterRpcSingleNS.nnFile = nnFile;
  }

  public static String getRouterFile() {
    return routerFile;
  }

  public static void setRouterFile(String routerFile) {
    TestRouterRpcSingleNS.routerFile = routerFile;
  }

  public static FileSystem getNnFS() {
    return nnFS;
  }

  public static void setNnFS(FileSystem nnFS) {
    TestRouterRpcSingleNS.nnFS = nnFS;
  }

  public static void globalSetUp(String rpcMode) throws Exception {
    cluster = new MiniRouterDFSCluster(false, 1);
    cluster.setNumDatanodesPerNameservice(2);

    // Start NNs and DNs and wait until ready.
    cluster.startCluster();

    // Start routers with only an RPC service.
    Configuration routerConf = new RouterConfigBuilder().metrics().rpc()
        .build();
    // We decrease the DN cache times to make the test faster.
    routerConf.setTimeDuration(RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1,
        TimeUnit.SECONDS);
    if (rpcMode.equals(ASYNC_MODE)) {
      routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    }
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();
    cluster.waitClusterUp();

    // Register and verify all NNs with all routers.
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
  }

  public static void setRouter(MiniRouterDFSCluster.RouterContext r)
      throws IOException, URISyntaxException {
    router = r;
    routerProtocol = r.getClient().getNamenode();
    routerFS = r.getFileSystem();
    routerNamenodeProtocol = NameNodeProxies.createProxy(router.getConf(),
        router.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
  }

  public static void setNs(String nameservice) {
    ns = nameservice;
  }

  public static void setNamenode(MiniRouterDFSCluster.NamenodeContext nn)
      throws IOException, URISyntaxException {
    namenode = nn;
    nnProtocol = nn.getClient().getNamenode();
    nnFS = nn.getFileSystem();

    // Namenode from the default namespace.
    String ns0 = cluster.getNameservices().get(0);
    MiniRouterDFSCluster.NamenodeContext nn0 = cluster.getNamenode(ns0, null);
    nnNamenodeProtocol = NameNodeProxies.createProxy(nn0.getConf(),
        nn0.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
  }

  @Nested
  @ExtendWith(RouterServerHelperInTestRouterRpcSingleNS.class)
  class TestWithSyncRouterRpc {
    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testGetCurrentTXIDandRollEditsSync() throws IOException {
      testGetCurrentTXIDandRollEdits();
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testSaveNamespaceSync() throws IOException {
      testSaveNamespace();
    }
  }

  @Nested
  @ExtendWith(RouterServerHelperInTestRouterRpcSingleNS.class)
  class TestWithAsyncRouterRpc {
    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testGetCurrentTXIDandRollEditsAsync() throws IOException {
      testGetCurrentTXIDandRollEdits();
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testSaveNamespaceAsync() throws IOException {
      testSaveNamespace();
    }
  }

  public void testGetCurrentTXIDandRollEdits() throws IOException {
    Long rollEdits = routerProtocol.rollEdits();
    Long currentTXID = routerProtocol.getCurrentEditLogTxid();

    assertEquals(rollEdits, currentTXID);
  }

  public void testSaveNamespace() throws IOException {
    cluster.getCluster().getFileSystem()
        .setSafeMode(SafeModeAction.ENTER);
    Boolean saveNamespace = routerProtocol.saveNamespace(0, 0);

    assertTrue(saveNamespace);
    cluster.getCluster().getFileSystem().setSafeMode(SafeModeAction.LEAVE);
  }
}

class RouterServerHelperInTestRouterRpcSingleNS implements
    AfterAllCallback, BeforeEachCallback {
  public static final ThreadLocal<RouterServerHelperInTestRouterRpcSingleNS>
      TEST_ROUTER_SERVER_TL = new InheritableThreadLocal<>();

  @Override
  public void afterAll(ExtensionContext context) {
    TestRouterRpcSingleNS.getCluster().shutdown();
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
          TestRouterRpcSingleNS.globalSetUp(rpcMode);
        }
      }
    }
    TestRouterRpcSingleNS.getCluster().waitClusterUp();
    // Create mock locations.
    TestRouterRpcSingleNS.getCluster().installMockLocations();

    // Delete all files via the NNs and verify.
    TestRouterRpcSingleNS.getCluster().deleteAllFiles();

    // Create test fixtures on NN.
    TestRouterRpcSingleNS.getCluster().createTestDirectoriesNamenode();

    // Wait to ensure NN has fully created its test directories.
    Thread.sleep(100);

    // Random router for this test.
    MiniRouterDFSCluster.RouterContext rndRouter = TestRouterRpcSingleNS.getCluster()
        .getRandomRouter();
    TestRouterRpcSingleNS.setRouter(rndRouter);

    // Pick a namenode for this test.
    String ns0 = TestRouterRpcSingleNS.getCluster().getNameservices().get(0);
    TestRouterRpcSingleNS.setNs(ns0);
    TestRouterRpcSingleNS.setNamenode(TestRouterRpcSingleNS.getCluster().getNamenode(ns0, null));

    // Create a test file on the NN.
    Random rnd = new Random();
    String randomFile = "testfile-" + rnd.nextInt();
    TestRouterRpcSingleNS.setNnFile(
        TestRouterRpcSingleNS.getCluster().getNamenodeTestDirectoryForNS(
            TestRouterRpcSingleNS.getNs()) + "/" + randomFile);
    TestRouterRpcSingleNS.setRouterFile(TestRouterRpcSingleNS.getCluster().
        getFederatedTestDirectoryForNS(TestRouterRpcSingleNS.getNs()) + "/"
        + randomFile);

    createFile(TestRouterRpcSingleNS.getNnFS(),
        TestRouterRpcSingleNS.getNnFile(), 32);
    verifyFileExists(TestRouterRpcSingleNS.getNnFS(), TestRouterRpcSingleNS.getNnFile());
    TEST_ROUTER_SERVER_TL.set(RouterServerHelperInTestRouterRpcSingleNS.this);
  }
}