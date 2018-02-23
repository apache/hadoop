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
package org.apache.hadoop.hdfs.server.federation.store;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyException;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.clearRecords;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the basic {@link StateStoreService} {@link RouterStore} functionality.
 */
public class TestStateStoreRouterState extends TestStateStoreBase {

  private static RouterStore routerStore;

  @BeforeClass
  public static void create() {
    // Reduce expirations to 5 seconds
    getConf().setTimeDuration(
        DFSConfigKeys.FEDERATION_STORE_ROUTER_EXPIRATION_MS,
        5, TimeUnit.SECONDS);
  }

  @Before
  public void setup() throws IOException, InterruptedException {

    if (routerStore == null) {
      routerStore =
          getStateStore().getRegisteredRecordStore(RouterStore.class);
    }

    // Clear router status registrations
    assertTrue(clearRecords(getStateStore(), RouterState.class));
  }

  @Test
  public void testStateStoreDisconnected() throws Exception {

    // Close the data store driver
    getStateStore().closeDriver();
    assertEquals(false, getStateStore().isDriverReady());

    // Test all APIs that access the data store to ensure they throw the correct
    // exception.
    GetRouterRegistrationRequest getSingleRequest =
        GetRouterRegistrationRequest.newInstance();
    verifyException(routerStore, "getRouterRegistration",
        StateStoreUnavailableException.class,
        new Class[] {GetRouterRegistrationRequest.class},
        new Object[] {getSingleRequest});

    GetRouterRegistrationsRequest getRequest =
        GetRouterRegistrationsRequest.newInstance();
    routerStore.loadCache(true);
    verifyException(routerStore, "getRouterRegistrations",
        StateStoreUnavailableException.class,
        new Class[] {GetRouterRegistrationsRequest.class},
        new Object[] {getRequest});

    RouterHeartbeatRequest hbRequest = RouterHeartbeatRequest.newInstance(
        RouterState.newInstance("test", 0, RouterServiceState.UNINITIALIZED));
    verifyException(routerStore, "routerHeartbeat",
        StateStoreUnavailableException.class,
        new Class[] {RouterHeartbeatRequest.class},
        new Object[] {hbRequest});
  }

  //
  // Router
  //
  @Test
  public void testUpdateRouterStatus()
      throws IllegalStateException, IOException {

    long dateStarted = Time.now();
    String address = "testaddress";

    // Set
    RouterHeartbeatRequest request = RouterHeartbeatRequest.newInstance(
        RouterState.newInstance(
            address, dateStarted, RouterServiceState.RUNNING));
    assertTrue(routerStore.routerHeartbeat(request).getStatus());

    // Verify
    GetRouterRegistrationRequest getRequest =
        GetRouterRegistrationRequest.newInstance(address);
    RouterState record =
        routerStore.getRouterRegistration(getRequest).getRouter();
    assertNotNull(record);
    assertEquals(RouterServiceState.RUNNING, record.getStatus());
    assertEquals(address, record.getAddress());
    assertEquals(FederationUtil.getCompileInfo(), record.getCompileInfo());
    // Build version may vary a bit
    assertFalse(record.getVersion().isEmpty());
  }

  @Test
  public void testRouterStateExpired()
      throws IOException, InterruptedException {

    long dateStarted = Time.now();
    String address = "testaddress";

    RouterHeartbeatRequest request = RouterHeartbeatRequest.newInstance(
        RouterState.newInstance(
            address, dateStarted, RouterServiceState.RUNNING));
    // Set
    assertTrue(routerStore.routerHeartbeat(request).getStatus());

    // Verify
    GetRouterRegistrationRequest getRequest =
        GetRouterRegistrationRequest.newInstance(address);
    RouterState record =
        routerStore.getRouterRegistration(getRequest).getRouter();
    assertNotNull(record);

    // Wait past expiration (set to 5 sec in config)
    Thread.sleep(6000);

    // Verify expired
    RouterState r = routerStore.getRouterRegistration(getRequest).getRouter();
    assertEquals(RouterServiceState.EXPIRED, r.getStatus());

    // Heartbeat again and this shouldn't be EXPIRED anymore
    assertTrue(routerStore.routerHeartbeat(request).getStatus());
    r = routerStore.getRouterRegistration(getRequest).getRouter();
    assertEquals(RouterServiceState.RUNNING, r.getStatus());
  }

  @Test
  public void testGetAllRouterStates()
      throws StateStoreUnavailableException, IOException {

    // Set 2 entries
    RouterHeartbeatRequest heartbeatRequest1 =
        RouterHeartbeatRequest.newInstance(
            RouterState.newInstance(
                "testaddress1", Time.now(), RouterServiceState.RUNNING));
    assertTrue(routerStore.routerHeartbeat(heartbeatRequest1).getStatus());

    RouterHeartbeatRequest heartbeatRequest2 =
        RouterHeartbeatRequest.newInstance(
            RouterState.newInstance(
                "testaddress2", Time.now(), RouterServiceState.RUNNING));
    assertTrue(routerStore.routerHeartbeat(heartbeatRequest2).getStatus());

    // Verify
    routerStore.loadCache(true);
    GetRouterRegistrationsRequest request =
        GetRouterRegistrationsRequest.newInstance();
    List<RouterState> entries =
        routerStore.getRouterRegistrations(request).getRouters();
    assertEquals(2, entries.size());
    Collections.sort(entries);
    assertEquals("testaddress1", entries.get(0).getAddress());
    assertEquals("testaddress2", entries.get(1).getAddress());
    assertEquals(RouterServiceState.RUNNING, entries.get(0).getStatus());
    assertEquals(RouterServiceState.RUNNING, entries.get(1).getStatus());
  }
}
