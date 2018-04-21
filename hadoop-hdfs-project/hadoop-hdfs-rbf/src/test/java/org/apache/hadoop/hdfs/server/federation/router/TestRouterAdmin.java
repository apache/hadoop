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

import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.synchronizeRecords;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.impl.DisabledNameserviceStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.MountTableStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.DisableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.DisableNameserviceResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnableNameserviceResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDisabledNameservicesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDisabledNameservicesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The administrator interface of the {@link Router} implemented by
 * {@link RouterAdminServer}.
 */
public class TestRouterAdmin {

  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  public static final String RPC_BEAN =
      "Hadoop:service=Router,name=FederationRPC";
  private static List<MountTable> mockMountTable;
  private static StateStoreService stateStore;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new StateStoreDFSCluster(false, 1);
    // Build and start a router with State Store + admin + RPC
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    cluster.addRouterOverrides(conf);
    cluster.startRouters();
    routerContext = cluster.getRandomRouter();
    mockMountTable = cluster.generateMockMountTable();
    Router router = routerContext.getRouter();
    stateStore = router.getStateStore();
  }

  @AfterClass
  public static void tearDown() {
    cluster.stopRouter(routerContext);
  }

  @Before
  public void testSetup() throws Exception {
    assertTrue(
        synchronizeRecords(stateStore, mockMountTable, MountTable.class));
  }

  @Test
  public void testAddMountTable() throws IOException {
    MountTable newEntry = MountTable.newInstance(
        "/testpath", Collections.singletonMap("ns0", "/testdir"),
        Time.now(), Time.now());

    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTable = client.getMountTableManager();

    // Existing mount table size
    List<MountTable> records = getMountTableEntries(mountTable);
    assertEquals(records.size(), mockMountTable.size());

    // Add
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTable.addMountTableEntry(addRequest);
    assertTrue(addResponse.getStatus());

    // New mount table size
    List<MountTable> records2 = getMountTableEntries(mountTable);
    assertEquals(records2.size(), mockMountTable.size() + 1);
  }

  @Test
  public void testAddDuplicateMountTable() throws IOException {
    MountTable newEntry = MountTable.newInstance("/testpath",
        Collections.singletonMap("ns0", "/testdir"), Time.now(), Time.now());

    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTable = client.getMountTableManager();

    // Existing mount table size
    List<MountTable> entries1 = getMountTableEntries(mountTable);
    assertEquals(entries1.size(), mockMountTable.size());

    // Add
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTable.addMountTableEntry(addRequest);
    assertTrue(addResponse.getStatus());

    // New mount table size
    List<MountTable> entries2 = getMountTableEntries(mountTable);
    assertEquals(entries2.size(), mockMountTable.size() + 1);

    // Add again, should fail
    AddMountTableEntryResponse addResponse2 =
        mountTable.addMountTableEntry(addRequest);
    assertFalse(addResponse2.getStatus());
  }

  @Test
  public void testAddReadOnlyMountTable() throws IOException {
    MountTable newEntry = MountTable.newInstance(
        "/readonly", Collections.singletonMap("ns0", "/testdir"),
        Time.now(), Time.now());
    newEntry.setReadOnly(true);

    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTable = client.getMountTableManager();

    // Existing mount table size
    List<MountTable> records = getMountTableEntries(mountTable);
    assertEquals(records.size(), mockMountTable.size());

    // Add
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTable.addMountTableEntry(addRequest);
    assertTrue(addResponse.getStatus());

    // New mount table size
    List<MountTable> records2 = getMountTableEntries(mountTable);
    assertEquals(records2.size(), mockMountTable.size() + 1);

    // Check that we have the read only entry
    MountTable record = getMountTableEntry("/readonly");
    assertEquals("/readonly", record.getSourcePath());
    assertTrue(record.isReadOnly());

    // Removing the new entry
    RemoveMountTableEntryRequest removeRequest =
        RemoveMountTableEntryRequest.newInstance("/readonly");
    RemoveMountTableEntryResponse removeResponse =
        mountTable.removeMountTableEntry(removeRequest);
    assertTrue(removeResponse.getStatus());
  }

  @Test
  public void testAddOrderMountTable() throws IOException {
    testAddOrderMountTable(DestinationOrder.HASH);
    testAddOrderMountTable(DestinationOrder.LOCAL);
    testAddOrderMountTable(DestinationOrder.RANDOM);
    testAddOrderMountTable(DestinationOrder.HASH_ALL);
  }

  private void testAddOrderMountTable(final DestinationOrder order)
      throws IOException {
    final String mnt = "/" + order;
    MountTable newEntry = MountTable.newInstance(
        mnt, Collections.singletonMap("ns0", "/testdir"),
        Time.now(), Time.now());
    newEntry.setDestOrder(order);

    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTable = client.getMountTableManager();

    // Add
    AddMountTableEntryRequest addRequest;
    AddMountTableEntryResponse addResponse;
    addRequest = AddMountTableEntryRequest.newInstance(newEntry);
    addResponse = mountTable.addMountTableEntry(addRequest);
    assertTrue(addResponse.getStatus());

    // Check that we have the read only entry
    MountTable record = getMountTableEntry(mnt);
    assertEquals(mnt, record.getSourcePath());
    assertEquals(order, record.getDestOrder());

    // Removing the new entry
    RemoveMountTableEntryRequest removeRequest =
        RemoveMountTableEntryRequest.newInstance(mnt);
    RemoveMountTableEntryResponse removeResponse =
        mountTable.removeMountTableEntry(removeRequest);
    assertTrue(removeResponse.getStatus());
  }

  @Test
  public void testRemoveMountTable() throws IOException {

    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTable = client.getMountTableManager();

    // Existing mount table size
    List<MountTable> entries1 = getMountTableEntries(mountTable);
    assertEquals(entries1.size(), mockMountTable.size());

    // Remove an entry
    RemoveMountTableEntryRequest removeRequest =
        RemoveMountTableEntryRequest.newInstance("/");
    mountTable.removeMountTableEntry(removeRequest);

    // New mount table size
    List<MountTable> entries2 = getMountTableEntries(mountTable);
    assertEquals(entries2.size(), mockMountTable.size() - 1);
  }

  @Test
  public void testEditMountTable() throws IOException {

    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTable = client.getMountTableManager();

    // Verify starting condition
    MountTable entry = getMountTableEntry("/");
    assertEquals(
        Collections.singletonList(new RemoteLocation("ns0", "/", "/")),
        entry.getDestinations());

    // Edit the entry for /
    MountTable updatedEntry = MountTable.newInstance(
        "/", Collections.singletonMap("ns1", "/"), Time.now(), Time.now());
    UpdateMountTableEntryRequest updateRequest =
        UpdateMountTableEntryRequest.newInstance(updatedEntry);
    mountTable.updateMountTableEntry(updateRequest);

    // Verify edited condition
    entry = getMountTableEntry("/");
    assertEquals(
        Collections.singletonList(new RemoteLocation("ns1", "/", "/")),
        entry.getDestinations());
  }

  @Test
  public void testGetMountTable() throws IOException {

    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTable = client.getMountTableManager();

    // Verify size of table
    List<MountTable> entries = getMountTableEntries(mountTable);
    assertEquals(mockMountTable.size(), entries.size());

    // Verify all entries are present
    int matches = 0;
    for (MountTable e : entries) {
      for (MountTable entry : mockMountTable) {
        assertEquals(e.getDestinations().size(), 1);
        assertNotNull(e.getDateCreated());
        assertNotNull(e.getDateModified());
        if (entry.getSourcePath().equals(e.getSourcePath())) {
          matches++;
        }
      }
    }
    assertEquals(matches, mockMountTable.size());
  }

  @Test
  public void testGetSingleMountTableEntry() throws IOException {
    MountTable entry = getMountTableEntry("/ns0");
    assertNotNull(entry);
    assertEquals(entry.getSourcePath(), "/ns0");
  }

  /**
   * Gets an existing mount table record in the state store.
   *
   * @param mount The mount point of the record to remove.
   * @return The matching record if found, null if it is not found.
   * @throws IOException If the state store could not be accessed.
   */
  private MountTable getMountTableEntry(final String mount) throws IOException {
    // Refresh the cache
    stateStore.loadCache(MountTableStoreImpl.class, true);

    GetMountTableEntriesRequest request =
        GetMountTableEntriesRequest.newInstance(mount);
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTable = client.getMountTableManager();
    List<MountTable> results = getMountTableEntries(mountTable, request);
    if (results.size() > 0) {
      // First result is sorted to have the shortest mount string length
      return results.get(0);
    }
    return null;
  }

  private List<MountTable> getMountTableEntries(MountTableManager mountTable)
      throws IOException {
    GetMountTableEntriesRequest request =
        GetMountTableEntriesRequest.newInstance("/");
    return getMountTableEntries(mountTable, request);
  }

  private List<MountTable> getMountTableEntries(MountTableManager mountTable,
      GetMountTableEntriesRequest request) throws IOException {
    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesResponse response =
        mountTable.getMountTableEntries(request);
    return response.getEntries();
  }

  @Test
  public void testNameserviceManager() throws IOException {

    RouterClient client = routerContext.getAdminClient();
    NameserviceManager nsManager = client.getNameserviceManager();

    // There shouldn't be any name service disabled
    Set<String> disabled = getDisabledNameservices(nsManager);
    assertTrue(disabled.isEmpty());

    // Disable one and see it
    DisableNameserviceRequest disableReq =
        DisableNameserviceRequest.newInstance("ns0");
    DisableNameserviceResponse disableResp =
        nsManager.disableNameservice(disableReq);
    assertTrue(disableResp.getStatus());
    // Refresh the cache
    disabled = getDisabledNameservices(nsManager);
    assertEquals(1, disabled.size());
    assertTrue(disabled.contains("ns0"));

    // Enable one and we should have no disabled name services
    EnableNameserviceRequest enableReq =
        EnableNameserviceRequest.newInstance("ns0");
    EnableNameserviceResponse enableResp =
        nsManager.enableNameservice(enableReq);
    assertTrue(enableResp.getStatus());
    disabled = getDisabledNameservices(nsManager);
    assertTrue(disabled.isEmpty());
  }

  private Set<String> getDisabledNameservices(NameserviceManager nsManager)
      throws IOException {
    stateStore.loadCache(DisabledNameserviceStoreImpl.class, true);
    GetDisabledNameservicesRequest getReq =
        GetDisabledNameservicesRequest.newInstance();
    GetDisabledNameservicesResponse response =
        nsManager.getDisabledNameservices(getReq);
    return response.getNameservices();
  }
}