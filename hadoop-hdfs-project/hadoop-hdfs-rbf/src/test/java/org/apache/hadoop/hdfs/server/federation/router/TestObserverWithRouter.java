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

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.ClientGSIContext;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RouterFederatedStateProto;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.RouterObserverReadProxyProvider;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.TestInfo;


public class TestObserverWithRouter {
  private static final int NUM_NAMESERVICES = 2;
  private static final String SKIP_BEFORE_EACH_CLUSTER_STARTUP = "SkipBeforeEachClusterStartup";
  private MiniRouterDFSCluster cluster;
  private RouterContext routerContext;
  private FileSystem fileSystem;

  @BeforeEach
  void init(TestInfo info) throws Exception {
    if (info.getTags().contains(SKIP_BEFORE_EACH_CLUSTER_STARTUP)) {
      return;
    }
    startUpCluster(2, null);
  }

  @AfterEach
  public void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }

    routerContext = null;

    if (fileSystem != null) {
      fileSystem.close();
      fileSystem = null;
    }
  }

  public void startUpCluster(int numberOfObserver, Configuration confOverrides) throws Exception {
    int numberOfNamenode = 2 + numberOfObserver;
    Configuration conf = new Configuration(false);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    conf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");
    conf.setBoolean(DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, true);
    if (confOverrides != null) {
      confOverrides
          .iterator()
          .forEachRemaining(entry -> conf.set(entry.getKey(), entry.getValue()));
    }
    cluster = new MiniRouterDFSCluster(true, NUM_NAMESERVICES, numberOfNamenode);
    cluster.addNamenodeOverrides(conf);
    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Making one Namenode active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
        for (int i = 2; i < numberOfNamenode; i++) {
          cluster.switchToObserver(ns, NAMENODES[i]);
        }
      }
    }

    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();

    cluster.addRouterOverrides(conf);
    cluster.addRouterOverrides(routerConf);

    // Start routers with only an RPC service
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    // Setup the mount table
    cluster.installMockLocations();

    cluster.waitActiveNamespaces();
    routerContext  = cluster.getRandomRouter();
  }

  public enum ConfigSetting {
    USE_NAMENODE_PROXY_FLAG,
    USE_ROUTER_OBSERVER_READ_PROXY_PROVIDER
  }

  private Configuration getConfToEnableObserverReads(ConfigSetting configSetting) {
    Configuration conf = new Configuration();
    switch (configSetting) {
    case USE_NAMENODE_PROXY_FLAG:
      conf.setBoolean(HdfsClientConfigKeys.DFS_RBF_OBSERVER_READ_ENABLE, true);
      break;
    case USE_ROUTER_OBSERVER_READ_PROXY_PROVIDER:
      conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX +
          "." +
          routerContext.getRouter()
              .getRpcServerAddress()
              .getHostName(), RouterObserverReadProxyProvider.class.getName());
      break;
    default:
      Assertions.fail("Unknown config setting: " + configSetting);
    }
    return conf;
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  public void testObserverRead(ConfigSetting configSetting) throws Exception {
    fileSystem = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));
    internalTestObserverRead();
  }

  /**
   * Tests that without adding config to use ObserverProxyProvider, the client shouldn't
   * have reads served by Observers.
   * Fixes regression in HDFS-13522.
   */
  @Test
  public void testReadWithoutObserverClientConfigurations() throws Exception {
    fileSystem = routerContext.getFileSystem();
    assertThrows(AssertionError.class, this::internalTestObserverRead);
  }

  public void internalTestObserverRead()
      throws Exception {
    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertEquals("First namenode should be observer", namenodes.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    Path path = new Path("/testFile");
    // Send create call
    fileSystem.create(path).close();

    // Send read request
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create and complete calls should be sent to active
    assertEquals("Two calls should be sent to active", 2, rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    // getBlockLocations should be sent to observer
    assertEquals("One call should be sent to observer", 1, rpcCountForObserver);
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testObserverReadWithoutFederatedStatePropagation(ConfigSetting configSetting)
      throws Exception {
    Configuration confOverrides = new Configuration(false);
    confOverrides.setInt(RBFConfigKeys.DFS_ROUTER_OBSERVER_FEDERATED_STATE_PROPAGATION_MAXSIZE, 0);
    startUpCluster(2, confOverrides);
    fileSystem = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));
    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertEquals("First namenode should be observer", namenodes.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    Path path = new Path("/testFile");
    // Send Create call to active
    fileSystem.create(path).close();

    // Send read request to observer. The router will msync to the active namenode.
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create, complete and getBlockLocations calls should be sent to active
    assertEquals("Three calls should be sent to active", 3, rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    assertEquals("No call should be sent to observer", 0, rpcCountForObserver);
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testDisablingObserverReadUsingNameserviceOverride(ConfigSetting configSetting)
      throws Exception {
    // Disable observer reads using per-nameservice override
    Configuration confOverrides = new Configuration(false);
    confOverrides.set(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_OVERRIDES, "ns0");
    startUpCluster(2, confOverrides);
    fileSystem = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));

    Path path = new Path("/testFile");
    fileSystem.create(path).close();
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create, complete and read calls should be sent to active
    assertEquals("Three calls should be sent to active", 3, rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    assertEquals("Zero calls should be sent to observer", 0, rpcCountForObserver);
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  public void testReadWhenObserverIsDown(ConfigSetting configSetting) throws Exception {
    fileSystem = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));
    Path path = new Path("/testFile1");
    // Send Create call to active
    fileSystem.create(path).close();

    // Stop observer NN
    int nnIndex = stopObserver(1);
    assertNotEquals("No observer found", 3, nnIndex);
    nnIndex = stopObserver(1);
    assertNotEquals("No observer found", 4, nnIndex);

    // Send read request
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create, complete and getBlockLocation calls should be sent to active
    assertEquals("Three calls should be sent to active", 3,
        rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    assertEquals("No call should send to observer", 0,
        rpcCountForObserver);
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  public void testMultipleObserver(ConfigSetting configSetting) throws Exception {
    fileSystem = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));
    Path path = new Path("/testFile1");
    // Send Create call to active
    fileSystem.create(path).close();

    // Stop one observer NN
    stopObserver(1);

    // Send read request
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();

    long expectedActiveRpc = 2;
    long expectedObserverRpc = 1;

    // Create and complete calls should be sent to active
    assertEquals("Two calls should be sent to active",
        expectedActiveRpc, rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter()
        .getRpcServer().getRPCMetrics().getObserverProxyOps();
    // getBlockLocation call should send to observer
    assertEquals("Read should be success with another observer",
        expectedObserverRpc, rpcCountForObserver);

    // Stop one observer NN
    stopObserver(1);

    // Send read request
    fileSystem.open(path).close();

    rpcCountForActive = routerContext.getRouter()
        .getRpcServer().getRPCMetrics().getActiveProxyOps();

    // getBlockLocation call should be sent to active
    expectedActiveRpc += 1;
    assertEquals("One call should be sent to active", expectedActiveRpc,
        rpcCountForActive);
    expectedObserverRpc += 0;
    rpcCountForObserver = routerContext.getRouter()
        .getRpcServer().getRPCMetrics().getObserverProxyOps();
    assertEquals("No call should send to observer",
        expectedObserverRpc, rpcCountForObserver);
  }

  private int stopObserver(int num) {
    int nnIndex;
    for (nnIndex = 0; nnIndex < cluster.getNamenodes().size(); nnIndex++) {
      NameNode nameNode = cluster.getCluster().getNameNode(nnIndex);
      if (nameNode != null && nameNode.isObserverState()) {
        cluster.getCluster().shutdownNameNode(nnIndex);
        num--;
        if (num == 0) {
          break;
        }
      }
    }
    return nnIndex;
  }

  // test router observer with multiple to know which observer NN received
  // requests
  @Test
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testMultipleObserverRouter() throws Exception {
    StateStoreDFSCluster innerCluster;
    MembershipNamenodeResolver resolver;

    String ns0;
    String ns1;
    //create 4NN, One Active One Standby and Two Observers
    innerCluster = new StateStoreDFSCluster(true, 4, 4, TimeUnit.SECONDS.toMillis(5),
        TimeUnit.SECONDS.toMillis(5));
    Configuration routerConf =
        new RouterConfigBuilder().stateStore().admin().rpc()
            .enableLocalHeartbeat(true).heartbeat().build();

    StringBuilder sb = new StringBuilder();
    ns0 = innerCluster.getNameservices().get(0);
    MiniRouterDFSCluster.NamenodeContext context =
        innerCluster.getNamenodes(ns0).get(1);
    routerConf.set(DFS_NAMESERVICE_ID, ns0);
    routerConf.set(DFS_HA_NAMENODE_ID_KEY, context.getNamenodeId());

    // Specify namenodes (ns1.nn0,ns1.nn1) to monitor
    ns1 = innerCluster.getNameservices().get(1);
    for (MiniRouterDFSCluster.NamenodeContext ctx : innerCluster.getNamenodes(ns1)) {
      String suffix = ctx.getConfSuffix();
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(suffix);
    }
    routerConf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE, sb.toString());
    routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY, true);
    routerConf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    routerConf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");

    innerCluster.addNamenodeOverrides(routerConf);
    innerCluster.addRouterOverrides(routerConf);
    innerCluster.startCluster();

    if (innerCluster.isHighAvailability()) {
      for (String ns : innerCluster.getNameservices()) {
        innerCluster.switchToActive(ns, NAMENODES[0]);
        innerCluster.switchToStandby(ns, NAMENODES[1]);
        for (int i = 2; i < 4; i++) {
          innerCluster.switchToObserver(ns, NAMENODES[i]);
        }
      }
    }
    innerCluster.startRouters();
    innerCluster.waitClusterUp();

    routerContext = innerCluster.getRandomRouter();
    resolver = (MembershipNamenodeResolver) routerContext.getRouter()
        .getNamenodeResolver();

    resolver.loadCache(true);
    List<? extends FederationNamenodeContext> namespaceInfo0 =
        resolver.getNamenodesForNameserviceId(ns0, true);
    List<? extends FederationNamenodeContext> namespaceInfo1 =
        resolver.getNamenodesForNameserviceId(ns1, true);
    assertEquals(namespaceInfo0.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    assertEquals(namespaceInfo0.get(1).getState(),
        FederationNamenodeServiceState.OBSERVER);
    assertNotEquals(namespaceInfo0.get(0).getNamenodeId(),
        namespaceInfo0.get(1).getNamenodeId());
    assertEquals(namespaceInfo1.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);

    innerCluster.shutdown();
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  public void testUnavailableObserverNN(ConfigSetting configSetting) throws Exception {
    fileSystem = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));
    stopObserver(2);

    Path path = new Path("/testFile");
    // Send Create call to active
    fileSystem.create(path).close();

    // Send read request.
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();

    // Create, complete and getBlockLocations
    // calls should be sent to active.
    assertEquals("Three calls should be send to active",
        3, rpcCountForActive);


    boolean hasUnavailable = false;
    for(String ns : cluster.getNameservices()) {
      List<? extends FederationNamenodeContext> nns = routerContext.getRouter()
          .getNamenodeResolver().getNamenodesForNameserviceId(ns, false);
      for(FederationNamenodeContext nn : nns) {
        if(FederationNamenodeServiceState.UNAVAILABLE == nn.getState()) {
          hasUnavailable = true;
        }
      }
    }
    // After attempting to communicate with unavailable observer namenode,
    // its state is updated to unavailable.
    assertTrue("There must be unavailable namenodes", hasUnavailable);
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  public void testRouterMsync(ConfigSetting configSetting) throws Exception {
    fileSystem = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));
    Path path = new Path("/testFile");

    // Send Create call to active
    fileSystem.create(path).close();
    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create and complete calls should be sent to active
    assertEquals("Two calls should be sent to active", 2,
        rpcCountForActive);

    // Send msync
    fileSystem.msync();
    rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // 2 msync calls should be sent. One to each active namenode in the two namespaces.
    assertEquals("Four calls should be sent to active", 4,
        rpcCountForActive);
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  public void testSingleRead(ConfigSetting configSetting) throws Exception {
    fileSystem = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));
    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertEquals("First namenode should be observer", namenodes.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    Path path = new Path("/");

    long rpcCountForActive;
    long rpcCountForObserver;

    // Send read request
    fileSystem.listFiles(path, false);
    fileSystem.close();

    rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // getListingCall sent to active.
    assertEquals("Only one call should be sent to active", 1, rpcCountForActive);

    rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    // getList call should be sent to observer
    assertEquals("No calls should be sent to observer", 0, rpcCountForObserver);
  }

  @Test
  public void testSingleReadUsingObserverReadProxyProvider() throws Exception {
    fileSystem = routerContext.getFileSystemWithObserverReadProxyProvider();
    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertEquals("First namenode should be observer", namenodes.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    Path path = new Path("/");

    long rpcCountForActive;
    long rpcCountForObserver;

    // Send read request
    fileSystem.listFiles(path, false);
    fileSystem.close();

    rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Two msync calls to the active namenodes.
    assertEquals("Two calls should be sent to active", 2, rpcCountForActive);

    rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    // getList call should be sent to observer
    assertEquals("One call should be sent to observer", 1, rpcCountForObserver);
  }

  @Test
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testClientReceiveResponseState() {
    ClientGSIContext clientGSIContext = new ClientGSIContext();

    Map<String, Long> mockMapping = new HashMap<>();
    mockMapping.put("ns0", 10L);
    RouterFederatedStateProto.Builder builder = RouterFederatedStateProto.newBuilder();
    mockMapping.forEach(builder::putNamespaceStateIds);
    RpcHeaderProtos.RpcResponseHeaderProto header = RpcHeaderProtos.RpcResponseHeaderProto
        .newBuilder()
        .setCallId(1)
        .setStatus(RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.SUCCESS)
        .setRouterFederatedState(builder.build().toByteString())
        .build();
    clientGSIContext.receiveResponseState(header);

    Map<String, Long> mockLowerMapping = new HashMap<>();
    mockLowerMapping.put("ns0", 8L);
    builder = RouterFederatedStateProto.newBuilder();
    mockLowerMapping.forEach(builder::putNamespaceStateIds);
    header = RpcHeaderProtos.RpcResponseHeaderProto.newBuilder()
        .setRouterFederatedState(builder.build().toByteString())
        .setCallId(2)
        .setStatus(RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.SUCCESS)
        .build();
    clientGSIContext.receiveResponseState(header);

    Map<String, Long> latestFederateState = ClientGSIContext.getRouterFederatedStateMap(
        clientGSIContext.getRouterFederatedState());
    Assertions.assertEquals(1, latestFederateState.size());
    Assertions.assertEquals(10L, latestFederateState.get("ns0"));
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  public void testStateIdProgressionInRouter(ConfigSetting configSetting) throws Exception {
    Path rootPath = new Path("/");
    fileSystem  = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));
    RouterStateIdContext routerStateIdContext = routerContext
        .getRouterRpcServer()
        .getRouterStateIdContext();
    for (int i = 0; i < 10; i++) {
      fileSystem.create(new Path(rootPath, "file" + i)).close();
    }

    // Get object storing state of the namespace in the shared RouterStateIdContext
    LongAccumulator namespaceStateId  = routerStateIdContext.getNamespaceStateId("ns0");
    assertEquals("Router's shared should have progressed.", 21, namespaceStateId.get());
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testSharedStateInRouterStateIdContext(ConfigSetting configSetting) throws Exception {
    Path rootPath = new Path("/");
    long cleanupPeriodMs = 1000;

    Configuration conf = new Configuration(false);
    conf.setLong(RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_POOL_CLEAN, cleanupPeriodMs);
    conf.setLong(RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_CLEAN_MS, cleanupPeriodMs / 10);
    startUpCluster(1, conf);
    fileSystem  = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));
    RouterStateIdContext routerStateIdContext = routerContext.getRouterRpcServer()
        .getRouterStateIdContext();

    // First read goes to active and creates connection pool for this user to active
    fileSystem.listStatus(rootPath);
    // Second read goes to observer and creates connection pool for this user to observer
    fileSystem.listStatus(rootPath);
    // Get object storing state of the namespace in the shared RouterStateIdContext
    LongAccumulator namespaceStateId1  = routerStateIdContext.getNamespaceStateId("ns0");

    // Wait for connection pools to expire and be cleaned up.
    Thread.sleep(cleanupPeriodMs * 2);

    // Third read goes to observer.
    // New connection pool to observer is created since existing one expired.
    fileSystem.listStatus(rootPath);
    fileSystem.close();
    // Get object storing state of the namespace in the shared RouterStateIdContext
    LongAccumulator namespaceStateId2  = routerStateIdContext.getNamespaceStateId("ns0");

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    long rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();

    // First list status goes to active
    assertEquals("One call should be sent to active", 1, rpcCountForActive);
    // Last two listStatuses  go to observer.
    assertEquals("Two calls should be sent to observer", 2, rpcCountForObserver);

    Assertions.assertSame(namespaceStateId1, namespaceStateId2,
        "The same object should be used in the shared RouterStateIdContext");
  }


  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testRouterStateIdContextCleanup(ConfigSetting configSetting) throws Exception {
    Path rootPath = new Path("/");
    long recordExpiry = TimeUnit.SECONDS.toMillis(1);

    Configuration confOverride = new Configuration(false);
    confOverride.setLong(RBFConfigKeys.FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS, recordExpiry);

    startUpCluster(1, confOverride);
    fileSystem  = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));
    RouterStateIdContext routerStateIdContext = routerContext.getRouterRpcServer()
        .getRouterStateIdContext();

    fileSystem.listStatus(rootPath);
    List<String> namespace1 = routerStateIdContext.getNamespaces();
    fileSystem.close();

    MockResolver mockResolver = (MockResolver) routerContext.getRouter().getNamenodeResolver();
    mockResolver.cleanRegistrations();
    mockResolver.setDisableRegistration(true);
    Thread.sleep(recordExpiry * 2);

    List<String> namespace2 = routerStateIdContext.getNamespaces();
    assertEquals(1, namespace1.size());
    assertEquals("ns0", namespace1.get(0));
    assertTrue(namespace2.isEmpty());
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testPeriodicStateRefreshUsingActiveNamenode(ConfigSetting configSetting)
      throws Exception {
    Path rootPath = new Path("/");

    Configuration confOverride = new Configuration(false);
    confOverride.set(RBFConfigKeys.DFS_ROUTER_OBSERVER_STATE_ID_REFRESH_PERIOD_KEY, "500ms");
    confOverride.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "3s");
    startUpCluster(1, confOverride);

    fileSystem  = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));
    fileSystem.listStatus(rootPath);
    int initialLengthOfRootListing = fileSystem.listStatus(rootPath).length;

    DFSClient activeClient = cluster.getNamenodes("ns0")
        .stream()
        .filter(nnContext -> nnContext.getNamenode().isActiveState())
        .findFirst().orElseThrow(() -> new IllegalStateException("No active namenode."))
        .getClient();

    for (int i = 0; i < 10; i++) {
      activeClient.mkdirs("/dir" + i, null, false);
    }
    activeClient.close();

    // Wait long enough for state in router to be considered stale.
    GenericTestUtils.waitFor(
        () -> !routerContext
            .getRouterRpcClient()
            .isNamespaceStateIdFresh("ns0"),
        100,
        10000,
        "Timeout: Namespace state was never considered stale.");
    FileStatus[] rootFolderAfterMkdir = fileSystem.listStatus(rootPath);
    assertEquals("List-status should show newly created directories.",
        initialLengthOfRootListing + 10, rootFolderAfterMkdir.length);
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  public void testAutoMsyncEqualsZero(ConfigSetting configSetting) throws Exception {
    Configuration clientConfiguration = getConfToEnableObserverReads(configSetting);
    clientConfiguration.setLong("dfs.client.failover.observer.auto-msync-period." +
        routerContext.getRouter().getRpcServerAddress().getHostName(), 0);
    fileSystem = routerContext.getFileSystem(clientConfiguration);

    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertEquals("First namenode should be observer", namenodes.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    Path path = new Path("/");

    long rpcCountForActive;
    long rpcCountForObserver;

    // Send read requests
    int numListings = 15;
    for (int i = 0; i < numListings; i++) {
      fileSystem.listFiles(path, false);
    }
    fileSystem.close();

    rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();

    rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();

    switch (configSetting) {
    case USE_NAMENODE_PROXY_FLAG:
      // First read goes to active.
      assertEquals("Calls sent to the active", 1, rpcCountForActive);
      // The rest of the reads are sent to the observer.
      assertEquals("Reads sent to observer", numListings - 1, rpcCountForObserver);
      break;
    case USE_ROUTER_OBSERVER_READ_PROXY_PROVIDER:
      // An msync is sent to each active namenode for each read.
      // Total msyncs will be (numListings * num_of_nameservices).
      assertEquals("Msyncs sent to the active namenodes",
          NUM_NAMESERVICES * numListings, rpcCountForActive);
      // All reads should be sent of the observer.
      assertEquals("Reads sent to observer", numListings, rpcCountForObserver);
      break;
    default:
      Assertions.fail("Unknown config setting: " + configSetting);
    }
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  public void testAutoMsyncNonZero(ConfigSetting configSetting) throws Exception {
    Configuration clientConfiguration = getConfToEnableObserverReads(configSetting);
    clientConfiguration.setLong("dfs.client.failover.observer.auto-msync-period." +
        routerContext.getRouter().getRpcServerAddress().getHostName(), 3000);
    fileSystem = routerContext.getFileSystem(clientConfiguration);

    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertEquals("First namenode should be observer", namenodes.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    Path path = new Path("/");

    long rpcCountForActive;
    long rpcCountForObserver;

    fileSystem.listFiles(path, false);
    fileSystem.listFiles(path, false);
    Thread.sleep(5000);
    fileSystem.listFiles(path, false);
    fileSystem.close();

    rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();

    rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();

    switch (configSetting) {
    case USE_NAMENODE_PROXY_FLAG:
      // First read goes to active.
      assertEquals("Calls sent to the active", 1, rpcCountForActive);
      // The rest of the reads are sent to the observer.
      assertEquals("Reads sent to observer", 2, rpcCountForObserver);
      break;
    case USE_ROUTER_OBSERVER_READ_PROXY_PROVIDER:
      // 4 msyncs expected. 2 for the first read, and 2 for the third read
      // after the auto-msync period has elapsed during the sleep.
      assertEquals("Msyncs sent to the active namenodes",
          4, rpcCountForActive);
      // All three reads should be sent of the observer.
      assertEquals("Reads sent to observer", 3, rpcCountForObserver);
      break;
    default:
      Assertions.fail("Unknown config setting: " + configSetting);
    }
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  public void testThatWriteDoesntBypassNeedForMsync(ConfigSetting configSetting) throws Exception {
    Configuration clientConfiguration = getConfToEnableObserverReads(configSetting);
    clientConfiguration.setLong("dfs.client.failover.observer.auto-msync-period." +
        routerContext.getRouter().getRpcServerAddress().getHostName(), 3000);
    fileSystem = routerContext.getFileSystem(clientConfiguration);

    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertEquals("First namenode should be observer", namenodes.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    Path path = new Path("/");

    long rpcCountForActive;
    long rpcCountForObserver;

    fileSystem.listFiles(path, false);
    Thread.sleep(5000);
    fileSystem.mkdirs(new Path(path, "mkdirLocation"));
    fileSystem.listFiles(path, false);
    fileSystem.close();

    rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();

    rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();

    switch (configSetting) {
    case USE_NAMENODE_PROXY_FLAG:
      // First listing and mkdir go to the active.
      assertEquals("Calls sent to the active namenodes", 2, rpcCountForActive);
      // Second listing goes to the observer.
      assertEquals("Read sent to observer", 1, rpcCountForObserver);
      break;
    case USE_ROUTER_OBSERVER_READ_PROXY_PROVIDER:
      // 5 calls to the active namenodes expected. 4 msync and a mkdir.
      // Each of the 2 reads results in an msync to 2 nameservices.
      // The mkdir also goes to the active.
      assertEquals("Calls sent to the active namenodes",
          5, rpcCountForActive);
      // Both reads should be sent of the observer.
      assertEquals("Reads sent to observer", 2, rpcCountForObserver);
      break;
    default:
      Assertions.fail("Unknown config setting: " + configSetting);
    }
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testMsyncOnlyToNamespaceWithObserver(ConfigSetting configSetting) throws Exception {
    Configuration confOverride = new Configuration(false);
    String namespaceWithObserverReadsDisabled = "ns0";
    // Disable observer reads for ns0
    confOverride.set(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_OVERRIDES,
        namespaceWithObserverReadsDisabled);
    startUpCluster(1, confOverride);
    fileSystem = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));

    // Send msync request
    fileSystem.msync();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // There should only be one call to the namespace that has an observer.
    assertEquals("Only one call to the namespace with an observer", 1, rpcCountForActive);
  }

  @EnumSource(ConfigSetting.class)
  @ParameterizedTest
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testMsyncWithNoNamespacesEligibleForCRS(ConfigSetting configSetting)
      throws Exception {
    Configuration confOverride = new Configuration(false);
    // Disable observer reads for all namespaces.
    confOverride.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY, false);
    startUpCluster(1, confOverride);
    fileSystem = routerContext.getFileSystem(getConfToEnableObserverReads(configSetting));

    // Send msync request.
    fileSystem.msync();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // There should no calls to any namespace.
    assertEquals("No calls to any namespace", 0, rpcCountForActive);
  }
}
