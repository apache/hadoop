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
package org.apache.hadoop.hdfs.server.federation.resolver;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMESERVICES;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.ROUTERS;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createNamenodeReport;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyException;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.clearRecords;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.getStateStoreConfiguration;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.newStateStore;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.waitStateStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the basic {@link ActiveNamenodeResolver} functionality.
 */
public class TestNamenodeResolver {

  private static StateStoreService stateStore;
  private static ActiveNamenodeResolver namenodeResolver;

  @BeforeClass
  public static void create() throws Exception {

    Configuration conf = getStateStoreConfiguration();

    // Reduce expirations to 5 seconds
    conf.setLong(
        RBFConfigKeys.FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS,
        TimeUnit.SECONDS.toMillis(5));

    stateStore = newStateStore(conf);
    assertNotNull(stateStore);

    namenodeResolver = new MembershipNamenodeResolver(conf, stateStore);
    namenodeResolver.setRouterId(ROUTERS[0]);
  }

  @AfterClass
  public static void destroy() throws Exception {
    stateStore.stop();
    stateStore.close();
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    // Wait for state store to connect
    stateStore.loadDriver();
    waitStateStore(stateStore, 10000);

    // Clear NN registrations
    boolean cleared = clearRecords(stateStore, MembershipState.class);
    assertTrue(cleared);
  }

  @Test
  public void testStateStoreDisconnected() throws Exception {

    // Add an entry to the store
    NamenodeStatusReport report = createNamenodeReport(
        NAMESERVICES[0], NAMENODES[0], HAServiceState.ACTIVE);
    assertTrue(namenodeResolver.registerNamenode(report));

    // Close the data store driver
    stateStore.closeDriver();
    assertFalse(stateStore.isDriverReady());

    // Flush the caches
    stateStore.refreshCaches(true);

    // Verify commands fail due to no cached data and no state store
    // connectivity.
    List<? extends FederationNamenodeContext> nns =
        namenodeResolver.getNamenodesForBlockPoolId(NAMESERVICES[0]);
    assertNull(nns);

    verifyException(namenodeResolver, "registerNamenode",
        StateStoreUnavailableException.class,
        new Class[] {NamenodeStatusReport.class}, new Object[] {report});
  }

  /**
   * Verify the first registration on the resolver.
   *
   * @param nsId Nameservice identifier.
   * @param nnId Namenode identifier within the nemeservice.
   * @param resultsCount Number of results expected.
   * @param state Expected state for the first one.
   * @throws IOException If we cannot get the namenodes.
   */
  private void verifyFirstRegistration(String nsId, String nnId,
      int resultsCount, FederationNamenodeServiceState state)
          throws IOException {
    List<? extends FederationNamenodeContext> namenodes =
        namenodeResolver.getNamenodesForNameserviceId(nsId);
    if (resultsCount == 0) {
      assertNull(namenodes);
    } else {
      assertEquals(resultsCount, namenodes.size());
      if (namenodes.size() > 0) {
        FederationNamenodeContext namenode = namenodes.get(0);
        assertEquals(state, namenode.getState());
        assertEquals(nnId, namenode.getNamenodeId());
      }
    }
  }

  @Test
  public void testRegistrationExpired()
      throws InterruptedException, IOException {

    // Populate the state store with a single NN element
    // 1) ns0:nn0 - Active
    // Wait for the entry to expire without heartbeating
    // Verify the NN entry is not accessible once expired.
    NamenodeStatusReport report = createNamenodeReport(
        NAMESERVICES[0], NAMENODES[0], HAServiceState.ACTIVE);
    assertTrue(namenodeResolver.registerNamenode(report));

    // Load cache
    stateStore.refreshCaches(true);

    // Verify
    verifyFirstRegistration(
        NAMESERVICES[0], NAMENODES[0], 1,
        FederationNamenodeServiceState.ACTIVE);

    // Wait past expiration (set in conf to 5 seconds)
    Thread.sleep(6000);
    // Reload cache
    stateStore.refreshCaches(true);

    // Verify entry is now expired and is no longer in the cache
    verifyFirstRegistration(
        NAMESERVICES[0], NAMENODES[0], 0,
        FederationNamenodeServiceState.ACTIVE);

    // Heartbeat again, updates dateModified
    assertTrue(namenodeResolver.registerNamenode(report));
    // Reload cache
    stateStore.refreshCaches(true);

    // Verify updated entry is marked active again and accessible to RPC server
    verifyFirstRegistration(
        NAMESERVICES[0], NAMENODES[0], 1,
        FederationNamenodeServiceState.ACTIVE);
  }

  @Test
  public void testRegistrationNamenodeSelection()
      throws InterruptedException, IOException {

    // 1) ns0:nn0 - Active
    // 2) ns0:nn1 - Standby (newest)
    // Verify the selected entry is the active entry
    assertTrue(namenodeResolver.registerNamenode(
        createNamenodeReport(
            NAMESERVICES[0], NAMENODES[0], HAServiceState.ACTIVE)));
    Thread.sleep(100);
    assertTrue(namenodeResolver.registerNamenode(
        createNamenodeReport(
            NAMESERVICES[0], NAMENODES[1], HAServiceState.STANDBY)));

    stateStore.refreshCaches(true);

    verifyFirstRegistration(
        NAMESERVICES[0], NAMENODES[0], 2,
        FederationNamenodeServiceState.ACTIVE);

    // 1) ns0:nn0 - Expired (stale)
    // 2) ns0:nn1 - Standby (newest)
    // Verify the selected entry is the standby entry as the active entry is
    // stale
    assertTrue(namenodeResolver.registerNamenode(
        createNamenodeReport(
            NAMESERVICES[0], NAMENODES[0], HAServiceState.ACTIVE)));

    // Expire active registration
    Thread.sleep(6000);

    // Refresh standby registration
    assertTrue(namenodeResolver.registerNamenode(createNamenodeReport(
        NAMESERVICES[0], NAMENODES[1], HAServiceState.STANDBY)));

    // Verify that standby is selected (active is now expired)
    stateStore.refreshCaches(true);
    verifyFirstRegistration(NAMESERVICES[0], NAMENODES[1], 1,
        FederationNamenodeServiceState.STANDBY);

    // 1) ns0:nn0 - Active
    // 2) ns0:nn1 - Unavailable (newest)
    // Verify the selected entry is the active entry
    assertTrue(namenodeResolver.registerNamenode(createNamenodeReport(
        NAMESERVICES[0], NAMENODES[0], HAServiceState.ACTIVE)));
    Thread.sleep(100);
    assertTrue(namenodeResolver.registerNamenode(createNamenodeReport(
        NAMESERVICES[0], NAMENODES[1], null)));
    stateStore.refreshCaches(true);
    verifyFirstRegistration(NAMESERVICES[0], NAMENODES[0], 2,
        FederationNamenodeServiceState.ACTIVE);

    // 1) ns0:nn0 - Unavailable (newest)
    // 2) ns0:nn1 - Standby
    // Verify the selected entry is the standby entry
    assertTrue(namenodeResolver.registerNamenode(createNamenodeReport(
        NAMESERVICES[0], NAMENODES[1], HAServiceState.STANDBY)));
    Thread.sleep(1000);
    assertTrue(namenodeResolver.registerNamenode(createNamenodeReport(
        NAMESERVICES[0], NAMENODES[0], null)));

    stateStore.refreshCaches(true);
    verifyFirstRegistration(NAMESERVICES[0], NAMENODES[1], 2,
        FederationNamenodeServiceState.STANDBY);

    // 1) ns0:nn0 - Active (oldest)
    // 2) ns0:nn1 - Standby
    // 3) ns0:nn2 - Active (newest)
    // Verify the selected entry is the newest active entry
    assertTrue(namenodeResolver.registerNamenode(
        createNamenodeReport(NAMESERVICES[0], NAMENODES[0], null)));
    Thread.sleep(100);
    assertTrue(namenodeResolver.registerNamenode(createNamenodeReport(
        NAMESERVICES[0], NAMENODES[1], HAServiceState.STANDBY)));
    Thread.sleep(100);
    assertTrue(namenodeResolver.registerNamenode(createNamenodeReport(
        NAMESERVICES[0], NAMENODES[2], HAServiceState.ACTIVE)));

    stateStore.refreshCaches(true);
    verifyFirstRegistration(NAMESERVICES[0], NAMENODES[2], 3,
        FederationNamenodeServiceState.ACTIVE);

    // 1) ns0:nn0 - Standby (oldest)
    // 2) ns0:nn1 - Standby (newest)
    // 3) ns0:nn2 - Standby
    // Verify the selected entry is the newest standby entry
    assertTrue(namenodeResolver.registerNamenode(createNamenodeReport(
        NAMESERVICES[0], NAMENODES[0], HAServiceState.STANDBY)));
    assertTrue(namenodeResolver.registerNamenode(createNamenodeReport(
        NAMESERVICES[0], NAMENODES[2], HAServiceState.STANDBY)));
    Thread.sleep(1500);
    assertTrue(namenodeResolver.registerNamenode(createNamenodeReport(
        NAMESERVICES[0], NAMENODES[1], HAServiceState.STANDBY)));

    stateStore.refreshCaches(true);
    verifyFirstRegistration(NAMESERVICES[0], NAMENODES[1], 3,
        FederationNamenodeServiceState.STANDBY);
  }

  @Test
  public void testCacheUpdateOnNamenodeStateUpdate() throws IOException {
    // Create a namenode initially registering in standby state.
    assertTrue(namenodeResolver.registerNamenode(
        createNamenodeReport(NAMESERVICES[0], NAMENODES[0],
            HAServiceState.STANDBY)));
    stateStore.refreshCaches(true);
    // Check whether the namenpde state is reported correct as standby.
    FederationNamenodeContext namenode =
        namenodeResolver.getNamenodesForNameserviceId(NAMESERVICES[0]).get(0);
    assertEquals(FederationNamenodeServiceState.STANDBY, namenode.getState());
    String rpcAddr = namenode.getRpcAddress();
    InetSocketAddress inetAddr = getInetSocketAddress(rpcAddr);

    // If the namenode state changes and it serves request,
    // RouterRpcClient calls updateActiveNamenode to update the state to active,
    // Check whether correct updated state is returned post update.
    namenodeResolver.updateActiveNamenode(NAMESERVICES[0], inetAddr);
    FederationNamenodeContext namenode1 =
        namenodeResolver.getNamenodesForNameserviceId(NAMESERVICES[0]).get(0);
    assertEquals("The namenode state should be ACTIVE post update.",
        FederationNamenodeServiceState.ACTIVE, namenode1.getState());
  }

  @Test
  public void testCacheUpdateOnNamenodeStateUpdateWithIp()
      throws IOException {
    final String rpcAddress = "127.0.0.1:10000";
    assertTrue(namenodeResolver.registerNamenode(
        createNamenodeReport(NAMESERVICES[0], NAMENODES[0], rpcAddress,
            HAServiceState.STANDBY)));
    stateStore.refreshCaches(true);

    InetSocketAddress inetAddr = getInetSocketAddress(rpcAddress);
    namenodeResolver.updateActiveNamenode(NAMESERVICES[0], inetAddr);
    FederationNamenodeContext namenode =
        namenodeResolver.getNamenodesForNameserviceId(NAMESERVICES[0]).get(0);
    assertEquals("The namenode state should be ACTIVE post update.",
        FederationNamenodeServiceState.ACTIVE, namenode.getState());
  }

  /**
   * Creates InetSocketAddress from the given RPC address.
   * @param rpcAddr RPC address (host:port).
   * @return InetSocketAddress corresponding to the specified RPC address.
   */
  private static InetSocketAddress getInetSocketAddress(String rpcAddr) {
    String[] rpcAddrArr = rpcAddr.split(":");
    int port = Integer.parseInt(rpcAddrArr[1]);
    String hostname = rpcAddrArr[0];
    return new InetSocketAddress(hostname, port);
  }
}