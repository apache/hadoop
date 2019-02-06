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
package org.apache.hadoop.hdfs.server.federation.metrics;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMESERVICES;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.clearAllRecords;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.createMockMountTable;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.createMockRegistrationForNamenode;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.synchronizeRecords;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.waitStateStore;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.RouterStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.hdfs.server.federation.store.records.StateStoreVersion;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;

/**
 * Test the basic metrics functionality.
 */
public class TestMetricsBase {

  private StateStoreService stateStore;
  private MembershipStore membershipStore;
  private RouterStore routerStore;
  private Router router;
  private Configuration routerConfig;

  private List<MembershipState> activeMemberships;
  private List<MembershipState> standbyMemberships;
  private List<MountTable> mockMountTable;
  private List<RouterState> mockRouters;
  private List<String> nameservices;

  @Before
  public void setupBase() throws Exception {

    if (router == null) {
      routerConfig = new RouterConfigBuilder()
          .stateStore()
          .metrics()
          .http()
          .build();
      router = new Router();
      router.init(routerConfig);
      router.setRouterId("routerId");
      router.start();
      stateStore = router.getStateStore();

      membershipStore =
          stateStore.getRegisteredRecordStore(MembershipStore.class);
      routerStore = stateStore.getRegisteredRecordStore(RouterStore.class);

      // Read all data and load all caches
      waitStateStore(stateStore, 10000);
      createFixtures();
      stateStore.refreshCaches(true);
      Thread.sleep(1000);
    }
  }

  @After
  public void tearDownBase() throws IOException {
    if (router != null) {
      router.stop();
      router.close();
      router = null;
    }
  }

  private void createFixtures() throws IOException {
    // Clear all records
    clearAllRecords(stateStore);

    nameservices = new ArrayList<>();
    nameservices.add(NAMESERVICES[0]);
    nameservices.add(NAMESERVICES[1]);

    // 2 NNs per NS
    activeMemberships = new ArrayList<>();
    standbyMemberships = new ArrayList<>();

    for (String nameservice : nameservices) {
      MembershipState namenode1 = createMockRegistrationForNamenode(
          nameservice, NAMENODES[0], FederationNamenodeServiceState.ACTIVE);
      NamenodeHeartbeatRequest request1 =
          NamenodeHeartbeatRequest.newInstance(namenode1);
      assertTrue(membershipStore.namenodeHeartbeat(request1).getResult());
      activeMemberships.add(namenode1);

      MembershipState namenode2 = createMockRegistrationForNamenode(
          nameservice, NAMENODES[1], FederationNamenodeServiceState.STANDBY);
      NamenodeHeartbeatRequest request2 =
          NamenodeHeartbeatRequest.newInstance(namenode2);
      assertTrue(membershipStore.namenodeHeartbeat(request2).getResult());
      standbyMemberships.add(namenode2);
    }

    // Add 2 mount table memberships
    mockMountTable = createMockMountTable(nameservices);
    synchronizeRecords(stateStore, mockMountTable, MountTable.class);

    // Add 2 router memberships in addition to the running router.
    long t1 = Time.now();
    mockRouters = new ArrayList<>();
    RouterState router1 = RouterState.newInstance(
        "router1", t1, RouterServiceState.RUNNING);
    router1.setStateStoreVersion(StateStoreVersion.newInstance(
        t1 - 1000, t1 - 2000));
    RouterHeartbeatRequest heartbeatRequest =
        RouterHeartbeatRequest.newInstance(router1);
    assertTrue(routerStore.routerHeartbeat(heartbeatRequest).getStatus());

    GetRouterRegistrationRequest getRequest =
        GetRouterRegistrationRequest.newInstance("router1");
    GetRouterRegistrationResponse getResponse =
        routerStore.getRouterRegistration(getRequest);
    RouterState routerState1 = getResponse.getRouter();
    mockRouters.add(routerState1);

    long t2 = Time.now();
    RouterState router2 = RouterState.newInstance(
        "router2", t2, RouterServiceState.RUNNING);
    router2.setStateStoreVersion(StateStoreVersion.newInstance(
        t2 - 6000, t2 - 7000));
    heartbeatRequest.setRouter(router2);
    assertTrue(routerStore.routerHeartbeat(heartbeatRequest).getStatus());
    getRequest.setRouterId("router2");
    getResponse = routerStore.getRouterRegistration(getRequest);
    RouterState routerState2 = getResponse.getRouter();
    mockRouters.add(routerState2);
  }

  protected Router getRouter() {
    return router;
  }

  protected List<MountTable> getMockMountTable() {
    return mockMountTable;
  }

  protected List<MembershipState> getActiveMemberships() {
    return activeMemberships;
  }

  protected List<MembershipState> getStandbyMemberships() {
    return standbyMemberships;
  }

  protected List<String> getNameservices() {
    return nameservices;
  }

  protected List<RouterState> getMockRouters() {
    return mockRouters;
  }

  protected StateStoreService getStateStore() {
    return stateStore;
  }
}
