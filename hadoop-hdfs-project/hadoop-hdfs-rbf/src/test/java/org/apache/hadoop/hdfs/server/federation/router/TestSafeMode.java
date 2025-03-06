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
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterConstants.ASYNC_MODE;
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterConstants.SYNC_MODE;
import static org.apache.hadoop.hdfs.server.federation.router.TestSafeMode.setUp;

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test the SafeMode.
 */
public class TestSafeMode {

  private static StateStoreDFSCluster cluster;
  public static void setUp(String rpcMode) throws Exception {
    cluster = new StateStoreDFSCluster(true, 2);

    Configuration routerConf = new RouterConfigBuilder()
        .rpc()
        .heartbeat()
        .stateStore()
        .build();
    if (rpcMode.equals(ASYNC_MODE)) {
      routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    }
    cluster.addRouterOverrides(routerConf);

    // Start NNs and DNs and wait until ready.
    cluster.startCluster();
    // Start routers with only an RPC service.
    cluster.startRouters();

    // Register and verify all NNs with all routers.
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    // Making one Namenodes active per nameservice.
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
      }
    }
    cluster.waitActiveNamespaces();
  }

  @Nested
  @ExtendWith(RouterServerHelperInTestSafeMode.class)
  class TestWithAsyncRouterRpc {
    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testProxySetSafemodeWithAsyncRouter(String rpcMode) throws Exception {
      testProxySetSafemode();
    }
  }

  @Nested
  @ExtendWith(RouterServerHelperInTestSafeMode.class)
  class TestWithSyncRouterRpc {
    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testProxySetSafemodeWithSyncRouter(String rpcMode) throws Exception {
      testProxySetSafemode();
    }
  }

  public void testProxySetSafemode() throws Exception {
    RouterContext routerContext = cluster.getRandomRouter();
    ClientProtocol routerProtocol = routerContext.getClient().getNamenode();
    routerProtocol.setSafeMode(SafeModeAction.SAFEMODE_GET, true);
    routerProtocol.setSafeMode(SafeModeAction.SAFEMODE_GET, false);
  }

  public static StateStoreDFSCluster getCluster() {
    return cluster;
  }

  public static void setCluster(StateStoreDFSCluster cluster) {
    TestSafeMode.cluster = cluster;
  }
}

class RouterServerHelperInTestSafeMode implements BeforeEachCallback, AfterAllCallback {

  private static final ThreadLocal<RouterServerHelperInTestSafeMode> TEST_ROUTER_SERVER_TL =
      new InheritableThreadLocal<>();

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    Method testMethod = context.getRequiredTestMethod();
    ValueSource enumAnnotation = testMethod.getAnnotation(ValueSource.class);
    if (enumAnnotation != null) {
      String[] strings = enumAnnotation.strings();
      for (String rpcMode : strings) {
        if (TEST_ROUTER_SERVER_TL.get() == null) {
          setUp(rpcMode);
        }
      }
    }
    TEST_ROUTER_SERVER_TL.set(RouterServerHelperInTestSafeMode.this);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (TestSafeMode.getCluster() != null) {
      TestSafeMode.getCluster().shutdown();
      TestSafeMode.setCluster(null);
    }
    TEST_ROUTER_SERVER_TL.remove();
  }
}
