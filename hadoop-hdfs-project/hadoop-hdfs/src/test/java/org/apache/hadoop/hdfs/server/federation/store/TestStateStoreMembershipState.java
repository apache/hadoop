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

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMESERVICES;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.ROUTERS;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyException;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.clearRecords;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.createMockRegistrationForNamenode;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.synchronizeRecords;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateNamenodeRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the basic {@link MembershipStore} membership functionality.
 */
public class TestStateStoreMembershipState extends TestStateStoreBase {

  private static MembershipStore membershipStore;

  @BeforeClass
  public static void create() {
    // Reduce expirations to 5 seconds
    getConf().setLong(
        DFSConfigKeys.FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS,
        TimeUnit.SECONDS.toMillis(5));
  }

  @Before
  public void setup() throws IOException, InterruptedException {

    membershipStore =
        getStateStore().getRegisteredRecordStore(MembershipStore.class);

    // Clear NN registrations
    assertTrue(clearRecords(getStateStore(), MembershipState.class));
  }

  @Test
  public void testNamenodeStateOverride() throws Exception {
    // Populate the state store
    // 1) ns0:nn0 - Standby
    String ns = "ns0";
    String nn = "nn0";
    MembershipState report = createRegistration(
        ns, nn, ROUTERS[1], FederationNamenodeServiceState.STANDBY);
    assertTrue(namenodeHeartbeat(report));

    // Load data into cache and calculate quorum
    assertTrue(getStateStore().loadCache(MembershipStore.class, true));

    MembershipState existingState = getNamenodeRegistration(ns, nn);
    assertEquals(
        FederationNamenodeServiceState.STANDBY, existingState.getState());

    // Override cache
    UpdateNamenodeRegistrationRequest request =
        UpdateNamenodeRegistrationRequest.newInstance(
            ns, nn, FederationNamenodeServiceState.ACTIVE);
    assertTrue(membershipStore.updateNamenodeRegistration(request).getResult());

    MembershipState newState = getNamenodeRegistration(ns, nn);
    assertEquals(FederationNamenodeServiceState.ACTIVE, newState.getState());
  }

  @Test
  public void testStateStoreDisconnected() throws Exception {

    // Close the data store driver
    getStateStore().closeDriver();
    assertFalse(getStateStore().isDriverReady());

    NamenodeHeartbeatRequest hbRequest = NamenodeHeartbeatRequest.newInstance();
    hbRequest.setNamenodeMembership(
        createMockRegistrationForNamenode(
            "test", "test", FederationNamenodeServiceState.UNAVAILABLE));
    verifyException(membershipStore, "namenodeHeartbeat",
        StateStoreUnavailableException.class,
        new Class[] {NamenodeHeartbeatRequest.class},
        new Object[] {hbRequest });

    // Information from cache, no exception should be triggered for these
    // TODO - should cached info expire at some point?
    GetNamenodeRegistrationsRequest getRequest =
        GetNamenodeRegistrationsRequest.newInstance();
    verifyException(membershipStore,
        "getNamenodeRegistrations", null,
        new Class[] {GetNamenodeRegistrationsRequest.class},
        new Object[] {getRequest});

    verifyException(membershipStore,
        "getExpiredNamenodeRegistrations", null,
        new Class[] {GetNamenodeRegistrationsRequest.class},
        new Object[] {getRequest});

    UpdateNamenodeRegistrationRequest overrideRequest =
        UpdateNamenodeRegistrationRequest.newInstance();
    verifyException(membershipStore,
        "updateNamenodeRegistration", null,
        new Class[] {UpdateNamenodeRegistrationRequest.class},
        new Object[] {overrideRequest});
  }

  private void registerAndLoadRegistrations(
      List<MembershipState> registrationList) throws IOException {
    // Populate
    assertTrue(synchronizeRecords(
        getStateStore(), registrationList, MembershipState.class));

    // Load into cache
    assertTrue(getStateStore().loadCache(MembershipStore.class, true));
  }

  private MembershipState createRegistration(String ns, String nn,
      String router, FederationNamenodeServiceState state) throws IOException {
    MembershipState record = MembershipState.newInstance(
        router, ns,
        nn, "testcluster", "testblock-" + ns, "testrpc-"+ ns + nn,
        "testservice-"+ ns + nn, "testlifeline-"+ ns + nn,
        "testweb-" + ns + nn, state, false);
    return record;
  }

  @Test
  public void testRegistrationMajorityQuorum()
      throws InterruptedException, IOException {

    // Populate the state store with a set of non-matching elements
    // 1) ns0:nn0 - Standby (newest)
    // 2) ns0:nn0 - Active (oldest)
    // 3) ns0:nn0 - Active (2nd oldest)
    // 4) ns0:nn0 - Active (3nd oldest element, newest active element)
    // Verify the selected entry is the newest majority opinion (4)
    String ns = "ns0";
    String nn = "nn0";

    // Active - oldest
    MembershipState report = createRegistration(
        ns, nn, ROUTERS[1], FederationNamenodeServiceState.ACTIVE);
    assertTrue(namenodeHeartbeat(report));
    Thread.sleep(1000);

    // Active - 2nd oldest
    report = createRegistration(
        ns, nn, ROUTERS[2], FederationNamenodeServiceState.ACTIVE);
    assertTrue(namenodeHeartbeat(report));
    Thread.sleep(1000);

    // Active - 3rd oldest, newest active element
    report = createRegistration(
        ns, nn, ROUTERS[3], FederationNamenodeServiceState.ACTIVE);
    assertTrue(namenodeHeartbeat(report));

    // standby - newest overall
    report = createRegistration(
        ns, nn, ROUTERS[0], FederationNamenodeServiceState.STANDBY);
    assertTrue(namenodeHeartbeat(report));

    // Load and calculate quorum
    assertTrue(getStateStore().loadCache(MembershipStore.class, true));

    // Verify quorum entry
    MembershipState quorumEntry = getNamenodeRegistration(
        report.getNameserviceId(), report.getNamenodeId());
    assertNotNull(quorumEntry);
    assertEquals(quorumEntry.getRouterId(), ROUTERS[3]);
  }

  @Test
  public void testRegistrationQuorumExcludesExpired()
      throws InterruptedException, IOException {

    // Populate the state store with some expired entries and verify the expired
    // entries are ignored.
    // 1) ns0:nn0 - Active
    // 2) ns0:nn0 - Expired
    // 3) ns0:nn0 - Expired
    // 4) ns0:nn0 - Expired
    // Verify the selected entry is the active entry
    List<MembershipState> registrationList = new ArrayList<>();
    String ns = "ns0";
    String nn = "nn0";
    String rpcAddress = "testrpcaddress";
    String serviceAddress = "testserviceaddress";
    String lifelineAddress = "testlifelineaddress";
    String blockPoolId = "testblockpool";
    String clusterId = "testcluster";
    String webAddress = "testwebaddress";
    boolean safemode = false;

    // Active
    MembershipState record = MembershipState.newInstance(
        ROUTERS[0], ns, nn, clusterId, blockPoolId,
        rpcAddress, serviceAddress, lifelineAddress, webAddress,
        FederationNamenodeServiceState.ACTIVE, safemode);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(
        ROUTERS[1], ns, nn, clusterId, blockPoolId,
        rpcAddress, serviceAddress, lifelineAddress, webAddress,
        FederationNamenodeServiceState.EXPIRED, safemode);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(
        ROUTERS[2], ns, nn, clusterId, blockPoolId,
        rpcAddress, serviceAddress, lifelineAddress, webAddress,
        FederationNamenodeServiceState.EXPIRED, safemode);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(
        ROUTERS[3], ns, nn, clusterId, blockPoolId,
        rpcAddress, serviceAddress, lifelineAddress, webAddress,
        FederationNamenodeServiceState.EXPIRED, safemode);
    registrationList.add(record);
    registerAndLoadRegistrations(registrationList);

    // Verify quorum entry chooses active membership
    MembershipState quorumEntry = getNamenodeRegistration(
        record.getNameserviceId(), record.getNamenodeId());
    assertNotNull(quorumEntry);
    assertEquals(ROUTERS[0], quorumEntry.getRouterId());
  }

  @Test
  public void testRegistrationQuorumAllExpired() throws IOException {

    // 1) ns0:nn0 - Expired (oldest)
    // 2) ns0:nn0 - Expired
    // 3) ns0:nn0 - Expired
    // 4) ns0:nn0 - Expired
    // Verify no entry is either selected or cached
    List<MembershipState> registrationList = new ArrayList<>();
    String ns = NAMESERVICES[0];
    String nn = NAMENODES[0];
    String rpcAddress = "testrpcaddress";
    String serviceAddress = "testserviceaddress";
    String lifelineAddress = "testlifelineaddress";
    String blockPoolId = "testblockpool";
    String clusterId = "testcluster";
    String webAddress = "testwebaddress";
    boolean safemode = false;
    long startingTime = Time.now();

    // Expired
    MembershipState record = MembershipState.newInstance(
        ROUTERS[0], ns, nn, clusterId, blockPoolId,
        rpcAddress, webAddress, lifelineAddress, webAddress,
        FederationNamenodeServiceState.EXPIRED, safemode);
    record.setDateModified(startingTime - 10000);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(
        ROUTERS[1], ns, nn, clusterId, blockPoolId,
        rpcAddress, serviceAddress, lifelineAddress, webAddress,
        FederationNamenodeServiceState.EXPIRED, safemode);
    record.setDateModified(startingTime);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(
        ROUTERS[2], ns, nn, clusterId, blockPoolId,
        rpcAddress, serviceAddress, lifelineAddress, webAddress,
        FederationNamenodeServiceState.EXPIRED, safemode);
    record.setDateModified(startingTime);
    registrationList.add(record);

    // Expired
    record = MembershipState.newInstance(
        ROUTERS[3], ns, nn, clusterId, blockPoolId,
        rpcAddress, serviceAddress, lifelineAddress, webAddress,
        FederationNamenodeServiceState.EXPIRED, safemode);
    record.setDateModified(startingTime);
    registrationList.add(record);

    registerAndLoadRegistrations(registrationList);

    // Verify no entry is found for this nameservice
    assertNull(getNamenodeRegistration(
        record.getNameserviceId(), record.getNamenodeId()));
  }

  @Test
  public void testRegistrationNoQuorum()
      throws InterruptedException, IOException {

    // Populate the state store with a set of non-matching elements
    // 1) ns0:nn0 - Standby (newest)
    // 2) ns0:nn0 - Standby (oldest)
    // 3) ns0:nn0 - Active (2nd oldest)
    // 4) ns0:nn0 - Active (3nd oldest element, newest active element)
    // Verify the selected entry is the newest entry (1)
    MembershipState report1 = createRegistration(
        NAMESERVICES[0], NAMENODES[0], ROUTERS[1],
        FederationNamenodeServiceState.STANDBY);
    assertTrue(namenodeHeartbeat(report1));
    Thread.sleep(100);
    MembershipState report2 = createRegistration(
        NAMESERVICES[0], NAMENODES[0], ROUTERS[2],
        FederationNamenodeServiceState.ACTIVE);
    assertTrue(namenodeHeartbeat(report2));
    Thread.sleep(100);
    MembershipState report3 = createRegistration(
        NAMESERVICES[0], NAMENODES[0], ROUTERS[3],
        FederationNamenodeServiceState.ACTIVE);
    assertTrue(namenodeHeartbeat(report3));
    Thread.sleep(100);
    MembershipState report4 = createRegistration(
        NAMESERVICES[0], NAMENODES[0], ROUTERS[0],
        FederationNamenodeServiceState.STANDBY);
    assertTrue(namenodeHeartbeat(report4));

    // Load and calculate quorum
    assertTrue(getStateStore().loadCache(MembershipStore.class, true));

    // Verify quorum entry uses the newest data, even though it is standby
    MembershipState quorumEntry = getNamenodeRegistration(
        report1.getNameserviceId(), report1.getNamenodeId());
    assertNotNull(quorumEntry);
    assertEquals(ROUTERS[0], quorumEntry.getRouterId());
    assertEquals(
        FederationNamenodeServiceState.STANDBY, quorumEntry.getState());
  }

  @Test
  public void testRegistrationExpired()
      throws InterruptedException, IOException {

    // Populate the state store with a single NN element
    // 1) ns0:nn0 - Active
    // Wait for the entry to expire without heartbeating
    // Verify the NN entry is populated as EXPIRED internally in the state store

    MembershipState report = createRegistration(
        NAMESERVICES[0], NAMENODES[0], ROUTERS[0],
        FederationNamenodeServiceState.ACTIVE);
    assertTrue(namenodeHeartbeat(report));

    // Load cache
    assertTrue(getStateStore().loadCache(MembershipStore.class, true));

    // Verify quorum and entry
    MembershipState quorumEntry = getNamenodeRegistration(
        report.getNameserviceId(), report.getNamenodeId());
    assertNotNull(quorumEntry);
    assertEquals(ROUTERS[0], quorumEntry.getRouterId());
    assertEquals(FederationNamenodeServiceState.ACTIVE, quorumEntry.getState());

    // Wait past expiration (set in conf to 5 seconds)
    Thread.sleep(6000);
    // Reload cache
    assertTrue(getStateStore().loadCache(MembershipStore.class, true));

    // Verify entry is now expired and is no longer in the cache
    quorumEntry = getNamenodeRegistration(NAMESERVICES[0], NAMENODES[0]);
    assertNull(quorumEntry);

    // Verify entry is now expired and can't be used by RPC service
    quorumEntry = getNamenodeRegistration(
        report.getNameserviceId(), report.getNamenodeId());
    assertNull(quorumEntry);

    // Heartbeat again, updates dateModified
    assertTrue(namenodeHeartbeat(report));
    // Reload cache
    assertTrue(getStateStore().loadCache(MembershipStore.class, true));

    // Verify updated entry marked as active and is accessible to RPC server
    quorumEntry = getNamenodeRegistration(
        report.getNameserviceId(), report.getNamenodeId());
    assertNotNull(quorumEntry);
    assertEquals(ROUTERS[0], quorumEntry.getRouterId());
    assertEquals(FederationNamenodeServiceState.ACTIVE, quorumEntry.getState());
  }

  /**
   * Get a single namenode membership record from the store.
   *
   * @param nsId The HDFS nameservice ID to search for
   * @param nnId The HDFS namenode ID to search for
   * @return The single NamenodeMembershipRecord that matches the query or null
   *         if not found.
   * @throws IOException if the query could not be executed.
   */
  private MembershipState getNamenodeRegistration(
      final String nsId, final String nnId) throws IOException {

    MembershipState partial = MembershipState.newInstance();
    partial.setNameserviceId(nsId);
    partial.setNamenodeId(nnId);
    GetNamenodeRegistrationsRequest request =
        GetNamenodeRegistrationsRequest.newInstance(partial);
    GetNamenodeRegistrationsResponse response =
        membershipStore.getNamenodeRegistrations(request);

    List<MembershipState> results = response.getNamenodeMemberships();
    if (results != null && results.size() == 1) {
      MembershipState record = results.get(0);
      return record;
    }
    return null;
  }

  /**
   * Register a namenode heartbeat with the state store.
   *
   * @param store FederationMembershipStateStore instance to retrieve the
   *          membership data records.
   * @param namenode A fully populated namenode membership record to be
   *          committed to the data store.
   * @return True if successful, false otherwise.
   * @throws IOException if the state store query could not be performed.
   */
  private boolean namenodeHeartbeat(MembershipState namenode)
      throws IOException {

    NamenodeHeartbeatRequest request =
        NamenodeHeartbeatRequest.newInstance(namenode);
    NamenodeHeartbeatResponse response =
        membershipStore.namenodeHeartbeat(request);
    return response.getResult();
  }
}