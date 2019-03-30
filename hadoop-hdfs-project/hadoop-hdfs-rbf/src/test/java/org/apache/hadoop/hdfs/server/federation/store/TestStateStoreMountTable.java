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

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMESERVICES;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyException;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.clearRecords;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.createMockMountTable;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.synchronizeRecords;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the basic {@link StateStoreService}
 * {@link MountTableStore} functionality.
 */
public class TestStateStoreMountTable extends TestStateStoreBase {

  private static List<String> nameservices;
  private static MountTableStore mountStore;

  @BeforeClass
  public static void create() throws IOException {
    nameservices = new ArrayList<>();
    nameservices.add(NAMESERVICES[0]);
    nameservices.add(NAMESERVICES[1]);
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    mountStore =
        getStateStore().getRegisteredRecordStore(MountTableStore.class);
    // Clear Mount table registrations
    assertTrue(clearRecords(getStateStore(), MountTable.class));
  }

  @Test
  public void testStateStoreDisconnected() throws Exception {

    // Close the data store driver
    getStateStore().closeDriver();
    assertFalse(getStateStore().isDriverReady());

    // Test APIs that access the store to check they throw the correct exception
    MountTable entry = MountTable.newInstance(
        "/mnt", Collections.singletonMap("ns0", "/tmp"));
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(entry);
    verifyException(mountStore, "addMountTableEntry",
        StateStoreUnavailableException.class,
        new Class[] {AddMountTableEntryRequest.class},
        new Object[] {addRequest});

    UpdateMountTableEntryRequest updateRequest =
        UpdateMountTableEntryRequest.newInstance(entry);
    verifyException(mountStore, "updateMountTableEntry",
        StateStoreUnavailableException.class,
        new Class[] {UpdateMountTableEntryRequest.class},
        new Object[] {updateRequest});

    RemoveMountTableEntryRequest removeRequest =
        RemoveMountTableEntryRequest.newInstance();
    verifyException(mountStore, "removeMountTableEntry",
        StateStoreUnavailableException.class,
        new Class[] {RemoveMountTableEntryRequest.class},
        new Object[] {removeRequest});

    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance();
    mountStore.loadCache(true);
    verifyException(mountStore, "getMountTableEntries",
        StateStoreUnavailableException.class,
        new Class[] {GetMountTableEntriesRequest.class},
        new Object[] {getRequest});
  }

  @Test
  public void testSynchronizeMountTable() throws IOException {
    // Synchronize and get mount table entries
    List<MountTable> entries = createMockMountTable(nameservices);
    assertTrue(synchronizeRecords(getStateStore(), entries, MountTable.class));
    for (MountTable e : entries) {
      mountStore.loadCache(true);
      MountTable entry = getMountTableEntry(e.getSourcePath());
      assertNotNull(entry);
      assertEquals(e.getDefaultLocation().getDest(),
          entry.getDefaultLocation().getDest());
    }
  }

  @Test
  public void testAddMountTableEntry() throws IOException {

    // Add 1
    List<MountTable> entries = createMockMountTable(nameservices);
    List<MountTable> entries1 = getMountTableEntries("/").getRecords();
    assertEquals(0, entries1.size());
    MountTable entry0 = entries.get(0);
    AddMountTableEntryRequest request =
        AddMountTableEntryRequest.newInstance(entry0);
    AddMountTableEntryResponse response =
        mountStore.addMountTableEntry(request);
    assertTrue(response.getStatus());

    mountStore.loadCache(true);
    List<MountTable> entries2 = getMountTableEntries("/").getRecords();
    assertEquals(1, entries2.size());
  }

  @Test
  public void testRemoveMountTableEntry() throws IOException {

    // Add many
    List<MountTable> entries = createMockMountTable(nameservices);
    synchronizeRecords(getStateStore(), entries, MountTable.class);
    mountStore.loadCache(true);
    List<MountTable> entries1 = getMountTableEntries("/").getRecords();
    assertEquals(entries.size(), entries1.size());

    // Remove 1
    RemoveMountTableEntryRequest request =
        RemoveMountTableEntryRequest.newInstance();
    request.setSrcPath(entries.get(0).getSourcePath());
    assertTrue(mountStore.removeMountTableEntry(request).getStatus());

    // Verify remove
    mountStore.loadCache(true);
    List<MountTable> entries2 = getMountTableEntries("/").getRecords();
    assertEquals(entries.size() - 1, entries2.size());
  }

  @Test
  public void testUpdateMountTableEntry() throws IOException {

    // Add 1
    List<MountTable> entries = createMockMountTable(nameservices);
    MountTable entry0 = entries.get(0);
    String srcPath = entry0.getSourcePath();
    String nsId = entry0.getDefaultLocation().getNameserviceId();
    AddMountTableEntryRequest request =
        AddMountTableEntryRequest.newInstance(entry0);
    AddMountTableEntryResponse response =
        mountStore.addMountTableEntry(request);
    assertTrue(response.getStatus());

    // Verify
    mountStore.loadCache(true);
    MountTable matchingEntry0 = getMountTableEntry(srcPath);
    assertNotNull(matchingEntry0);
    assertEquals(nsId, matchingEntry0.getDefaultLocation().getNameserviceId());

    // Edit destination nameservice for source path
    Map<String, String> destMap =
        Collections.singletonMap("testnameservice", "/");
    MountTable replacement =
        MountTable.newInstance(srcPath, destMap);
    UpdateMountTableEntryRequest updateRequest =
        UpdateMountTableEntryRequest.newInstance(replacement);
    UpdateMountTableEntryResponse updateResponse =
        mountStore.updateMountTableEntry(updateRequest);
    assertTrue(updateResponse.getStatus());

    // Verify
    mountStore.loadCache(true);
    MountTable matchingEntry1 = getMountTableEntry(srcPath);
    assertNotNull(matchingEntry1);
    assertEquals("testnameservice",
        matchingEntry1.getDefaultLocation().getNameserviceId());
  }

  /**
   * Gets an existing mount table record in the state store.
   *
   * @param mount The mount point of the record to remove.
   * @return The matching record if found, null if it is not found.
   * @throws IOException If the state store could not be accessed.
   */
  private MountTable getMountTableEntry(String mount) throws IOException {
    GetMountTableEntriesRequest request =
        GetMountTableEntriesRequest.newInstance(mount);
    GetMountTableEntriesResponse response =
        mountStore.getMountTableEntries(request);
    List<MountTable> results = response.getEntries();
    if (results.size() > 0) {
      // First result is sorted to have the shortest mount string length
      return results.get(0);
    }
    return null;
  }

  /**
   * Fetch all mount table records beneath a root path.
   *
   * @param store FederationMountTableStore instance to commit the data.
   * @param mount The root search path, enter "/" to return all mount table
   *          records.
   *
   * @return A list of all mount table records found below the root mount.
   *
   * @throws IOException If the state store could not be accessed.
   */
  private QueryResult<MountTable> getMountTableEntries(String mount)
      throws IOException {
    if (mount == null) {
      throw new IOException("Please specify a root search path");
    }
    GetMountTableEntriesRequest request =
        GetMountTableEntriesRequest.newInstance();
    request.setSrcPath(mount);
    GetMountTableEntriesResponse response =
        mountStore.getMountTableEntries(request);
    List<MountTable> records = response.getEntries();
    long timestamp = response.getTimestamp();
    return new QueryResult<MountTable>(records, timestamp);
  }
}