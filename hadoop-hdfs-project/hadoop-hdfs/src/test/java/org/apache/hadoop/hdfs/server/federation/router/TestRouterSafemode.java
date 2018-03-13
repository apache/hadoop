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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_CACHE_TIME_TO_LIVE_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_SAFEMODE_EXPIRATION;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_SAFEMODE_EXTENSION;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.deleteStateStore;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.getStateStoreConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the safe mode for the {@link Router} controlled by
 * {@link RouterSafemodeService}.
 */
public class TestRouterSafemode {

  private Router router;
  private static Configuration conf;

  @BeforeClass
  public static void create() throws IOException {
    // Wipe state store
    deleteStateStore();
    // Configuration that supports the state store
    conf = getStateStoreConfiguration();
    // 2 sec startup standby
    conf.setTimeDuration(DFS_ROUTER_SAFEMODE_EXTENSION,
        TimeUnit.SECONDS.toMillis(2), TimeUnit.MILLISECONDS);
    // 1 sec cache refresh
    conf.setTimeDuration(DFS_ROUTER_CACHE_TIME_TO_LIVE_MS,
        TimeUnit.SECONDS.toMillis(1), TimeUnit.MILLISECONDS);
    // 2 sec post cache update before entering safemode (2 intervals)
    conf.setTimeDuration(DFS_ROUTER_SAFEMODE_EXPIRATION,
        TimeUnit.SECONDS.toMillis(2), TimeUnit.MILLISECONDS);

    conf.set(DFSConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY, "0.0.0.0");
    conf.set(DFSConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFSConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFSConfigKeys.DFS_ROUTER_ADMIN_BIND_HOST_KEY, "0.0.0.0");
    conf.set(DFSConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFSConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_KEY, "127.0.0.1:0");

    // RPC + State Store + Safe Mode only
    conf = new RouterConfigBuilder(conf)
        .rpc()
        .safemode()
        .stateStore()
        .metrics()
        .build();
  }

  @AfterClass
  public static void destroy() {
  }

  @Before
  public void setup() throws IOException, URISyntaxException {
    router = new Router();
    router.init(conf);
    router.start();
  }

  @After
  public void cleanup() throws IOException {
    if (router != null) {
      router.stop();
      router = null;
    }
  }

  @Test
  public void testSafemodeService() throws IOException {
    RouterSafemodeService server = new RouterSafemodeService(router);
    server.init(conf);
    assertEquals(STATE.INITED, server.getServiceState());
    server.start();
    assertEquals(STATE.STARTED, server.getServiceState());
    server.stop();
    assertEquals(STATE.STOPPED, server.getServiceState());
    server.close();
  }

  @Test
  public void testRouterExitSafemode()
      throws InterruptedException, IllegalStateException, IOException {

    assertTrue(router.getRpcServer().isInSafeMode());
    verifyRouter(RouterServiceState.SAFEMODE);

    // Wait for initial time in milliseconds
    long interval =
        conf.getTimeDuration(DFS_ROUTER_SAFEMODE_EXTENSION,
            TimeUnit.SECONDS.toMillis(2), TimeUnit.MILLISECONDS) +
        conf.getTimeDuration(DFS_ROUTER_CACHE_TIME_TO_LIVE_MS,
            TimeUnit.SECONDS.toMillis(1), TimeUnit.MILLISECONDS);
    Thread.sleep(interval);

    assertFalse(router.getRpcServer().isInSafeMode());
    verifyRouter(RouterServiceState.RUNNING);
  }

  @Test
  public void testRouterEnterSafemode()
      throws IllegalStateException, IOException, InterruptedException {

    // Verify starting state
    assertTrue(router.getRpcServer().isInSafeMode());
    verifyRouter(RouterServiceState.SAFEMODE);

    // We should be in safe mode for DFS_ROUTER_SAFEMODE_EXTENSION time
    long interval0 = conf.getTimeDuration(DFS_ROUTER_SAFEMODE_EXTENSION,
        TimeUnit.SECONDS.toMillis(2), TimeUnit.MILLISECONDS) - 1000;
    long t0 = Time.now();
    while (Time.now() - t0 < interval0) {
      verifyRouter(RouterServiceState.SAFEMODE);
      Thread.sleep(100);
    }

    // We wait some time for the state to propagate
    long interval1 = 1000 + 2 * conf.getTimeDuration(
        DFS_ROUTER_CACHE_TIME_TO_LIVE_MS, TimeUnit.SECONDS.toMillis(1),
        TimeUnit.MILLISECONDS);
    Thread.sleep(interval1);

    // Running
    assertFalse(router.getRpcServer().isInSafeMode());
    verifyRouter(RouterServiceState.RUNNING);

    // Disable cache
    router.getStateStore().stopCacheUpdateService();

    // Wait until the State Store cache is stale in milliseconds
    long interval2 =
        conf.getTimeDuration(DFS_ROUTER_SAFEMODE_EXPIRATION,
            TimeUnit.SECONDS.toMillis(2), TimeUnit.MILLISECONDS) +
        conf.getTimeDuration(DFS_ROUTER_CACHE_TIME_TO_LIVE_MS,
            TimeUnit.SECONDS.toMillis(1), TimeUnit.MILLISECONDS);
    Thread.sleep(interval2);

    // Safemode
    assertTrue(router.getRpcServer().isInSafeMode());
    verifyRouter(RouterServiceState.SAFEMODE);
  }

  @Test
  public void testRouterRpcSafeMode()
      throws IllegalStateException, IOException {

    assertTrue(router.getRpcServer().isInSafeMode());
    verifyRouter(RouterServiceState.SAFEMODE);

    // If the Router is in Safe Mode, we should get a SafeModeException
    boolean exception = false;
    try {
      router.getRpcServer().delete("/testfile.txt", true);
      fail("We should have thrown a safe mode exception");
    } catch (RouterSafeModeException sme) {
      exception = true;
    }
    assertTrue("We should have thrown a safe mode exception", exception);
  }

  private void verifyRouter(RouterServiceState status)
      throws IllegalStateException, IOException {
    assertEquals(status, router.getRouterState());
  }
}
