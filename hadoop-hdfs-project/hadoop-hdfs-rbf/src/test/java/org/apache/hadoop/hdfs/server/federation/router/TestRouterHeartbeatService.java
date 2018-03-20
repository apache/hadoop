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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.store.RouterStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.hdfs.server.federation.store.records.StateStoreVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.waitStateStore;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for router heartbeat service.
 */
public class TestRouterHeartbeatService {
  private Router router;
  private final String routerId = "router1";
  private TestingServer testingServer;
  private CuratorFramework curatorFramework;

  @Before
  public void setup() throws Exception {
    router = new Router();
    router.setRouterId(routerId);
    Configuration conf = new Configuration();
    conf.setInt(RBFConfigKeys.DFS_ROUTER_CACHE_TIME_TO_LIVE_MS, 1);
    Configuration routerConfig =
        new RouterConfigBuilder(conf).stateStore().build();
    routerConfig.setLong(RBFConfigKeys.FEDERATION_STORE_CONNECTION_TEST_MS,
        TimeUnit.HOURS.toMillis(1));
    routerConfig.setClass(RBFConfigKeys.FEDERATION_STORE_DRIVER_CLASS,
        StateStoreZooKeeperImpl.class, StateStoreDriver.class);

    testingServer = new TestingServer();
    String connectStr = testingServer.getConnectString();
    curatorFramework = CuratorFrameworkFactory.builder()
        .connectString(connectStr)
        .retryPolicy(new RetryNTimes(100, 100))
        .build();
    curatorFramework.start();
    routerConfig.set(CommonConfigurationKeys.ZK_ADDRESS, connectStr);
    router.init(routerConfig);
    router.start();


    waitStateStore(router.getStateStore(), TimeUnit.SECONDS.toMicros(10));
  }

  @Test
  public void testStateStoreUnavailable() throws IOException {
    curatorFramework.close();
    testingServer.stop();
    router.getStateStore().stop();
    // The driver is not ready
    assertFalse(router.getStateStore().isDriverReady());

    // Do a heartbeat, and no exception thrown out
    RouterHeartbeatService heartbeatService =
        new RouterHeartbeatService(router);
    heartbeatService.updateStateStore();
  }

  @Test
  public void testStateStoreAvailable() throws Exception {
    // The driver is ready
    StateStoreService stateStore = router.getStateStore();
    assertTrue(router.getStateStore().isDriverReady());
    RouterStore routerStore = router.getRouterStateManager();

    // No record about this router
    stateStore.refreshCaches(true);
    GetRouterRegistrationRequest request =
        GetRouterRegistrationRequest.newInstance(routerId);
    GetRouterRegistrationResponse response =
        router.getRouterStateManager().getRouterRegistration(request);
    RouterState routerState = response.getRouter();
    String id = routerState.getRouterId();
    StateStoreVersion version = routerState.getStateStoreVersion();
    assertNull(id);
    assertNull(version);

    // Do a heartbeat
    RouterHeartbeatService heartbeatService =
        new RouterHeartbeatService(router);
    heartbeatService.updateStateStore();

    // We should have a record
    stateStore.refreshCaches(true);
    request = GetRouterRegistrationRequest.newInstance(routerId);
    response = routerStore.getRouterRegistration(request);
    routerState = response.getRouter();
    id = routerState.getRouterId();
    version = routerState.getStateStoreVersion();
    assertNotNull(id);
    assertNotNull(version);
  }

  @After
  public void tearDown() throws IOException {
    if (curatorFramework != null) {
      curatorFramework.close();
    }
    if (testingServer != null) {
      testingServer.stop();
    }
    if (router != null) {
      router.shutDown();
    }
  }
}
