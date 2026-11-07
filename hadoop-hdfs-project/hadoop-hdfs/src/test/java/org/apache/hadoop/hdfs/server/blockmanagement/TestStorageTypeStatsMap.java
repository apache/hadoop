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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.Map;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for the nodesInService count in StorageTypeStats / DatanodeStats.
 *
 * Regression test for HDFS-17840: StorageTypeStats.nodesInService was
 * incorrectly reset to zero when the storage-type map entry was prematurely
 * removed.  The entry was removed whenever nodesInService reached 0, even
 * when non-in-service (e.g. decommissioning) nodes still used the storage
 * type.  Subsequent in-service heartbeats then started from a fresh entry,
 * under-counting the real number of in-service nodes.
 *
 * The fix tracks totalNodes (all nodes, regardless of admin state) and only
 * removes the entry when totalNodes == 0.
 */
public class TestStorageTypeStatsMap {

  private DatanodeStats stats;

  @BeforeEach
  public void setUp() {
    stats = new DatanodeStats();
  }

  /**
   * Creates a DatanodeStorageInfo with the given StorageType and returns its
   * owning DatanodeDescriptor (which starts in the NORMAL / in-service state).
   */
  private static DatanodeDescriptor makeNode(String id, StorageType type) {
    DatanodeStorageInfo si = DFSTestUtil.createDatanodeStorageInfo(
        id, "127.0.0." + id.hashCode() % 200, "/rack", "host-" + id, type, null);
    return si.getDatanodeDescriptor();
  }

  /** Return the StorageTypeStats for the given type, or null if absent. */
  private StorageTypeStats getStats(StorageType type) {
    Map<StorageType, StorageTypeStats> m = stats.getStatsMap();
    return m.get(type);
  }

  /**
   * Basic smoke test: two in-service nodes, both using DISK.
   * After adding both, nodesInService should be 2.
   * After removing one, it should be 1.
   */
  @Test
  public void testBasicAddRemove() {
    DatanodeDescriptor a = makeNode("a", StorageType.DISK);
    DatanodeDescriptor b = makeNode("b", StorageType.DISK);

    stats.add(a);
    stats.add(b);
    assertNotNull(getStats(StorageType.DISK));
    assertEquals(2, getStats(StorageType.DISK).getNodesInService());

    stats.subtract(b);
    assertNotNull(getStats(StorageType.DISK));
    assertEquals(1, getStats(StorageType.DISK).getNodesInService());

    stats.subtract(a);
    assertNull(getStats(StorageType.DISK),
        "Entry should be removed when no nodes remain");
  }

  /**
   * HDFS-17840 regression: when one of two in-service nodes transitions to
   * decommission-in-progress, the entry must NOT be removed, and
   * nodesInService must remain correct for the still-in-service node.
   */
  @Test
  public void testEntryNotRemovedWhenDecommissioningNodeRemains() {
    DatanodeDescriptor a = makeNode("aa", StorageType.DISK);
    DatanodeDescriptor b = makeNode("bb", StorageType.DISK);

    stats.add(a);
    stats.add(b);
    assertEquals(2, getStats(StorageType.DISK).getNodesInService());

    // Simulate A transitioning to decommission-in-progress (as HeartbeatManager
    // does in startDecommission): subtract, change state, add.
    stats.subtract(a);
    a.startDecommission();   // adminState -> DECOMMISSION_INPROGRESS
    stats.add(a);

    // The DISK entry must still exist and B must still be counted.
    assertNotNull(getStats(StorageType.DISK),
        "DISK entry must survive when a decommissioning node still uses it");
    assertEquals(1, getStats(StorageType.DISK).getNodesInService(),
        "Only the in-service node B should be counted");

    // Now B heartbeats (subtract + add with unchanged state).
    stats.subtract(b);
    stats.add(b);
    assertEquals(1, getStats(StorageType.DISK).getNodesInService(),
        "nodesInService must remain 1 after B's heartbeat");
  }

  /**
   * HDFS-17840 regression: when the last in-service node transitions to
   * decommissioning (nodesInService drops to 0), the entry must still NOT be
   * removed as long as that decommissioning node is alive.  A subsequent
   * in-service node's heartbeat must then see the correct count (1, not 0).
   */
  @Test
  public void testEntryNotRemovedWhenLastInServiceDecommissions() {
    DatanodeDescriptor a = makeNode("a1", StorageType.DISK);
    DatanodeDescriptor b = makeNode("b1", StorageType.DISK);

    stats.add(a);
    stats.add(b);
    assertEquals(2, getStats(StorageType.DISK).getNodesInService());

    // Decommission A.
    stats.subtract(a);
    a.startDecommission();
    stats.add(a);
    assertEquals(1, getStats(StorageType.DISK).getNodesInService());

    // Decommission B (last in-service node).
    stats.subtract(b);
    b.startDecommission();
    stats.add(b);

    // nodesInService == 0, but A and B are still alive and using DISK.
    assertNotNull(getStats(StorageType.DISK),
        "DISK entry must survive when decommissioning nodes still use it");
    assertEquals(0, getStats(StorageType.DISK).getNodesInService());

    // Now a new node C registers (in-service).
    DatanodeDescriptor c = makeNode("c1", StorageType.DISK);
    stats.add(c);
    assertEquals(1, getStats(StorageType.DISK).getNodesInService(),
        "New in-service node must be counted correctly");

    // C heartbeats.
    stats.subtract(c);
    stats.add(c);
    assertEquals(1, getStats(StorageType.DISK).getNodesInService(),
        "nodesInService must stay 1 after C's heartbeat");
  }

  /**
   * Entry should be removed only after all nodes (in-service and
   * decommissioning) are fully removed via removeDatanode (subtract only).
   */
  @Test
  public void testEntryRemovedOnlyWhenAllNodesGone() {
    DatanodeDescriptor a = makeNode("x1", StorageType.DISK);
    DatanodeDescriptor b = makeNode("x2", StorageType.DISK);

    stats.add(a);
    stats.add(b);

    // Decommission A.
    stats.subtract(a);
    a.startDecommission();
    stats.add(a);

    // Remove B (simulating removeDatanode).
    stats.subtract(b);
    assertNotNull(getStats(StorageType.DISK),
        "DISK entry must remain because A is still using it");

    // Remove A too.
    stats.subtract(a);
    assertNull(getStats(StorageType.DISK),
        "DISK entry must be removed once no nodes use it");
  }
}
