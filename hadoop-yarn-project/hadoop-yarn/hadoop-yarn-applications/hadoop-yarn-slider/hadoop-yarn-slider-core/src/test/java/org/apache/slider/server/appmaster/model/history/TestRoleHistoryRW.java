/*
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

package org.apache.slider.server.appmaster.model.history;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.model.mock.MockRoleHistory;
import org.apache.slider.server.appmaster.state.NodeEntry;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.avro.LoadedRoleHistory;
import org.apache.slider.server.avro.RoleHistoryWriter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test fole history reading and writing.
 */
public class TestRoleHistoryRW extends BaseMockAppStateTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRoleHistoryRW.class);

  private static long time = System.currentTimeMillis();
  public static final String HISTORY_V1_6_ROLE =
      "org/apache/slider/server/avro/history-v01-6-role.json";
  public static final String HISTORY_V1_3_ROLE =
      "org/apache/slider/server/avro/history-v01-3-role.json";
  public static final String HISTORY_V1B_1_ROLE =
      "org/apache/slider/server/avro/history_v01b_1_role.json";

  private RoleStatus role0Status;
  private RoleStatus role1Status;

  static final ProviderRole PROVIDER_ROLE3 = new ProviderRole(
      "role3",
      3,
      PlacementPolicy.STRICT,
      3,
      3,
      ResourceKeys.DEF_YARN_LABEL_EXPRESSION);

  @Override
  public String getTestName() {
    return "TestHistoryRW";
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    role0Status = getRole0Status();
    role1Status = getRole1Status();
  }

  //@Test
  public void testWriteReadEmpty() throws Throwable {
    RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
    roleHistory.onStart(fs, historyPath);
    Path history = roleHistory.saveHistory(time++);
    assertTrue(fs.getFileStatus(history).isFile());
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    historyWriter.read(fs, history);
  }

  //@Test
  public void testWriteReadData() throws Throwable {
    RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
    assertFalse(roleHistory.onStart(fs, historyPath));
    String addr = "localhost";
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr);
    NodeEntry ne1 = instance.getOrCreate(0);
    ne1.setLastUsed(0xf00d);

    Path history = roleHistory.saveHistory(time++);
    assertTrue(fs.getFileStatus(history).isFile());
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    RoleHistory rh2 = new MockRoleHistory(MockFactory.ROLES);


    LoadedRoleHistory loadedRoleHistory = historyWriter.read(fs, history);
    assertTrue(0 < loadedRoleHistory.size());
    rh2.rebuild(loadedRoleHistory);
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr);
    assertNotNull(ni2);
    NodeEntry ne2 = ni2.get(0);
    assertNotNull(ne2);
    assertEquals(ne2.getLastUsed(), ne1.getLastUsed());
  }

  //@Test
  public void testWriteReadActiveData() throws Throwable {
    RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
    roleHistory.onStart(fs, historyPath);
    String addr = "localhost";
    String addr2 = "rack1server5";
    NodeInstance localhost = roleHistory.getOrCreateNodeInstance(addr);
    NodeEntry orig1 = localhost.getOrCreate(role0Status.getKey());
    orig1.setLastUsed(0x10);
    NodeInstance rack1server5 = roleHistory.getOrCreateNodeInstance(addr2);
    NodeEntry orig2 = rack1server5.getOrCreate(role1Status.getKey());
    orig2.setLive(3);
    assertFalse(orig2.isAvailable());
    NodeEntry orig3 = localhost.getOrCreate(role1Status.getKey());
    orig3.setLastUsed(0x20);
    orig3.setLive(1);
    assertFalse(orig3.isAvailable());
    orig3.release();
    assertTrue(orig3.isAvailable());
    roleHistory.dump();

    long savetime = 0x0001000;
    Path history = roleHistory.saveHistory(savetime);
    assertTrue(fs.getFileStatus(history).isFile());
    describe("Loaded");
    LOG.info("testWriteReadActiveData in {}", history);
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    RoleHistory rh2 = new MockRoleHistory(MockFactory.ROLES);
    LoadedRoleHistory loadedRoleHistory = historyWriter.read(fs, history);
    assertEquals(3, loadedRoleHistory.size());
    rh2.rebuild(loadedRoleHistory);
    rh2.dump();

    assertEquals(2, rh2.getClusterSize());
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr);
    assertNotNull(ni2);
    NodeEntry loadedNE = ni2.get(role0Status.getKey());
    assertEquals(loadedNE.getLastUsed(), orig1.getLastUsed());
    NodeInstance ni2b = rh2.getExistingNodeInstance(addr2);
    assertNotNull(ni2b);
    NodeEntry loadedNE2 = ni2b.get(role1Status.getKey());
    assertNotNull(loadedNE2);
    assertEquals(loadedNE2.getLastUsed(), savetime);
    assertEquals(rh2.getThawedDataTime(), savetime);

    // now start it
    rh2.buildRecentNodeLists();
    describe("starting");
    rh2.dump();
    List<NodeInstance> available0 = rh2.cloneRecentNodeList(role0Status
        .getKey());
    assertEquals(1, available0.size());

    NodeInstance entry = available0.get(0);
    assertEquals(entry.hostname, "localhost");
    assertEquals(entry, localhost);
    List<NodeInstance> available1 = rh2.cloneRecentNodeList(role1Status
        .getKey());
    assertEquals(2, available1.size());
    //and verify that even if last used was set, the save time is picked up
    assertEquals(entry.get(role1Status.getKey()).getLastUsed(), roleHistory
        .getSaveTime());

  }

  //@Test
  public void testWriteThaw() throws Throwable {
    RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
    assertFalse(roleHistory.onStart(fs, historyPath));
    String addr = "localhost";
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr);
    NodeEntry ne1 = instance.getOrCreate(0);
    ne1.setLastUsed(0xf00d);

    Path history = roleHistory.saveHistory(time++);
    long savetime =roleHistory.getSaveTime();
    assertTrue(fs.getFileStatus(history).isFile());
    RoleHistory rh2 = new MockRoleHistory(MockFactory.ROLES);
    assertTrue(rh2.onStart(fs, historyPath));
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr);
    assertNotNull(ni2);
    NodeEntry ne2 = ni2.get(0);
    assertNotNull(ne2);
    assertEquals(ne2.getLastUsed(), ne1.getLastUsed());
    assertEquals(rh2.getThawedDataTime(), savetime);
  }


  //@Test
  public void testPurgeOlderEntries() throws Throwable {
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    time = 1;
    Path file1 = touch(historyWriter, time++);
    Path file2 = touch(historyWriter, time++);
    Path file3 = touch(historyWriter, time++);
    Path file4 = touch(historyWriter, time++);
    Path file5 = touch(historyWriter, time++);
    Path file6 = touch(historyWriter, time++);

    assertEquals(0, historyWriter.purgeOlderHistoryEntries(fs, file1));
    assertEquals(1, historyWriter.purgeOlderHistoryEntries(fs, file2));
    assertEquals(0, historyWriter.purgeOlderHistoryEntries(fs, file2));
    assertEquals(3, historyWriter.purgeOlderHistoryEntries(fs, file5));
    assertEquals(1, historyWriter.purgeOlderHistoryEntries(fs, file6));
    try {
      // make an impossible assertion that will fail if the method
      // actually completes
      assertEquals(-1, historyWriter.purgeOlderHistoryEntries(fs, file1));
    } catch (FileNotFoundException ignored) {
      //  expected
    }

  }

  public Path touch(RoleHistoryWriter historyWriter, long timeMs)
      throws IOException {
    Path path = historyWriter.createHistoryFilename(historyPath, timeMs);
    FSDataOutputStream out = fs.create(path);
    out.close();
    return path;
  }

  //@Test
  public void testSkipEmptyFileOnRead() throws Throwable {
    describe("verify that empty histories are skipped on read; old histories " +
            "purged");
    RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
    roleHistory.onStart(fs, historyPath);
    time = 0;
    Path oldhistory = roleHistory.saveHistory(time++);

    String addr = "localhost";
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr);
    NodeEntry ne1 = instance.getOrCreate(0);
    ne1.setLastUsed(0xf00d);

    Path goodhistory = roleHistory.saveHistory(time++);

    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    Path touched = touch(historyWriter, time++);

    RoleHistory rh2 = new MockRoleHistory(MockFactory.ROLES);
    assertTrue(rh2.onStart(fs, historyPath));
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr);
    assertNotNull(ni2);

    //and assert the older file got purged
    assertFalse(fs.exists(oldhistory));
    assertTrue(fs.exists(goodhistory));
    assertTrue(fs.exists(touched));
  }

  //@Test
  public void testSkipBrokenFileOnRead() throws Throwable {
    describe("verify that empty histories are skipped on read; old histories " +
            "purged");
    RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
    roleHistory.onStart(fs, historyPath);
    time = 0;
    Path oldhistory = roleHistory.saveHistory(time++);

    String addr = "localhost";
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr);
    NodeEntry ne1 = instance.getOrCreate(0);
    ne1.setLastUsed(0xf00d);

    Path goodhistory = roleHistory.saveHistory(time++);

    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    Path badfile = historyWriter.createHistoryFilename(historyPath, time++);
    FSDataOutputStream out = fs.create(badfile);
    out.writeBytes("{broken:true}");
    out.close();

    RoleHistory rh2 = new MockRoleHistory(MockFactory.ROLES);
    describe("IGNORE STACK TRACE BELOW");

    assertTrue(rh2.onStart(fs, historyPath));

    describe("IGNORE STACK TRACE ABOVE");
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr);
    assertNotNull(ni2);

    //and assert the older file got purged
    assertFalse(fs.exists(oldhistory));
    assertTrue(fs.exists(goodhistory));
    assertTrue(fs.exists(badfile));
  }

  /**
   * Test that a v1 JSON file can be read. Here the number of roles
   * matches the current state.
   * @throws Throwable
   */
  //@Test
  public void testReloadDataV13Role() throws Throwable {
    String source = HISTORY_V1_3_ROLE;
    RoleHistoryWriter writer = new RoleHistoryWriter();

    LoadedRoleHistory loadedRoleHistory = writer.read(source);
    assertEquals(4, loadedRoleHistory.size());
    RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
    assertEquals(0, roleHistory.rebuild(loadedRoleHistory));
  }

  /**
   * Test that a v1 JSON file can be read. Here more roles than expected
   * @throws Throwable
   */
  //@Test
  public void testReloadDataV16Role() throws Throwable {
    String source = HISTORY_V1_6_ROLE;
    RoleHistoryWriter writer = new RoleHistoryWriter();

    LoadedRoleHistory loadedRoleHistory = writer.read(source);
    assertEquals(6, loadedRoleHistory.size());
    RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
    assertEquals(3, roleHistory.rebuild(loadedRoleHistory));
  }

  /**
   * Test that a v1 JSON file can be read. Here the number of roles
   * is less than the current state.
   * @throws Throwable
   */
  //@Test
  public void testReloadLessRoles() throws Throwable {
    String source = HISTORY_V1_3_ROLE;
    RoleHistoryWriter writer = new RoleHistoryWriter();

    LoadedRoleHistory loadedRoleHistory = writer.read(source);
    assertEquals(4, loadedRoleHistory.size());
    List<ProviderRole> expandedRoles = new ArrayList(MockFactory.ROLES);
    expandedRoles.add(PROVIDER_ROLE3);
    RoleHistory roleHistory = new MockRoleHistory(expandedRoles);
    assertEquals(0, roleHistory.rebuild(loadedRoleHistory));
  }

  /**
   * Test that a v1b JSON file can be read. Here more roles than expected
   * @throws Throwable
   */
  //@Test
  public void testReloadDataV1B1Role() throws Throwable {
    String source = HISTORY_V1B_1_ROLE;
    RoleHistoryWriter writer = new RoleHistoryWriter();

    LoadedRoleHistory loadedRoleHistory = writer.read(source);
    assertEquals(1, loadedRoleHistory.size());
    assertEquals(2, loadedRoleHistory.roleMap.size());
    RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
    assertEquals(0, roleHistory.rebuild(loadedRoleHistory));

  }
}
