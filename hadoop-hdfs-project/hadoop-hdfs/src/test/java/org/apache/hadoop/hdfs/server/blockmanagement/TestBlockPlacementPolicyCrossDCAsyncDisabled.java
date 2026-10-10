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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.Node;
import org.junit.jupiter.api.Test;

/**
 * This class tests the BlockPlacementPolicyCrossDC with async cross-DC write disabled (default).
 *
 * When async cross-DC write is disabled (default behavior), all replicas (both local and remote
 * datacenter) are returned synchronously. BlockPlacementPolicyCrossDC distributes replicas
 * across multiple datacenters by placing approximately half of replicas in the local datacenter
 * and the remaining replicas in remote datacenter(s).
 */
public class TestBlockPlacementPolicyCrossDCAsyncDisabled extends BaseReplicationPolicyCrossDCTest {

  /**
   * Creates a multi-datacenter topology with 2 datacenters:
   * - /dc1/r1: 2 datanodes
   * - /dc1/r2: 2 datanodes
   * - /dc2/r1: 2 datanodes
   * - /dc2/r2: 2 datanodes
   */
  @Override
  DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf) {
    conf.set(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_PREFERRED_DATACENTER_KEY, "/dc1");
    final String[] racks = {
        "/dc1/r1",  // datanode 0
        "/dc1/r1",  // datanode 1
        "/dc1/r2",  // datanode 2
        "/dc1/r2",  // datanode 3
        "/dc2/r1",  // datanode 4
        "/dc2/r2"   // datanode 5
    };
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    return DFSTestUtil.toDatanodeDescriptor(storages);
  }

  /**
   * Test replication factor 3:
   * - Writer at /dc1/r1
   * - Expected: 2 replicas in dc1, 1 replica in dc2
   */
  @Test
  public void testChooseTargetWithReplicationFactor3() throws Exception {
    DatanodeStorageInfo[] targets = chooseTarget(3, dataNodes[0]);

    assertEquals(3, targets.length);

    // Count replicas per datacenter
    int dc1Count = 0;
    int dc2Count = 0;

    for (DatanodeStorageInfo target : targets) {
      String location = target.getDatanodeDescriptor().getNetworkLocation();
      if (location.startsWith("/dc1")) {
        dc1Count++;
      } else if (location.startsWith("/dc2")) {
        dc2Count++;
      }
    }

    // For replication factor 3 with async disabled: ceil(3/2) = 2 in local DC, floor(3/2) = 1 in remote DC
    assertEquals(2, dc1Count, "Should have 2 replicas in local datacenter");
    assertEquals(1, dc2Count, "Should have 1 replica in remote datacenter");
  }

  /**
   * Test replication factor 5 with async cross-DC write disabled (default):
   * - Writer at /dc1/r1
   * - Expected: 3 replicas in dc1, 2 replicas in dc2 (all returned synchronously)
   */
  @Test
  public void testChooseTargetWithReplicationFactor5() throws Exception {
    DatanodeStorageInfo[] targets = chooseTarget(5, dataNodes[0]);

    assertEquals(5, targets.length);

    // Count replicas per datacenter
    int dc1Count = 0;
    int dc2Count = 0;

    for (DatanodeStorageInfo target : targets) {
      String location = target.getDatanodeDescriptor().getNetworkLocation();
      if (location.startsWith("/dc1")) {
        dc1Count++;
      } else if (location.startsWith("/dc2")) {
        dc2Count++;
      }
    }

    // For replication factor 5 with async disabled: ceil(5/2) = 3 in local DC, floor(5/2) = 2 in remote DC
    assertEquals(3, dc1Count, "Should have 3 replicas in local datacenter");
    assertEquals(2, dc2Count, "Should have 2 replicas in remote datacenter");
  }

  /**
   * Test replication factor 2:
   * - Writer at /dc1/r1
   * - Expected: 1 replica in dc1, 1 replica in dc2
   */
  @Test
  public void testChooseTargetWithReplicationFactor2() throws Exception {
    DatanodeStorageInfo[] targets = chooseTarget(2, dataNodes[0]);

    assertEquals(2, targets.length);

    // Count replicas per datacenter
    int dc1Count = 0;
    int dc2Count = 0;

    for (DatanodeStorageInfo target : targets) {
      String location = target.getDatanodeDescriptor().getNetworkLocation();
      if (location.startsWith("/dc1")) {
        dc1Count++;
      } else if (location.startsWith("/dc2")) {
        dc2Count++;
      }
    }

    // For replication factor 2 with async disabled: ceil(2/2) = 1 in local DC, floor(2/2) = 1 in remote DC
    assertEquals(1, dc1Count, "Should have 1 replica in local datacenter");
    assertEquals(1, dc2Count, "Should have 1 replica in remote datacenter");
  }

  /**
   * Test that replicas are placed on different racks within datacenter
   */
  @Test
  public void testChooseTargetOnDifferentRacks() throws Exception {
    DatanodeStorageInfo[] targets = chooseTarget(3, dataNodes[0]);

    assertEquals(3, targets.length);

    // Collect all racks used
    Set<String> racks = new HashSet<>();
    for (DatanodeStorageInfo target : targets) {
      racks.add(target.getDatanodeDescriptor().getNetworkLocation());
    }

    // Should use at least 2 different racks
    assertTrue(racks.size() >= 2, "Should use at least 2 different racks");
  }

  /**
   * Test with excluded nodes
   */
  @Test
  public void testChooseTargetWithExcludedNodes() throws Exception {
    Set<Node> excludedNodes = new HashSet<>();
    excludedNodes.add(dataNodes[0]);
    excludedNodes.add(dataNodes[1]);

    DatanodeStorageInfo[] targets = chooseTarget(3, dataNodes[2],
        new ArrayList<DatanodeStorageInfo>(), new HashSet<>(excludedNodes));

    assertEquals(3, targets.length);

    // Verify excluded nodes are not in targets
    for (DatanodeStorageInfo target : targets) {
      assertFalse(excludedNodes.contains(target.getDatanodeDescriptor()),
          "Excluded node should not be in targets");
    }
  }

  /**
   * Test with writer from dc2
   */
  @Test
  public void testChooseTargetFromDC2() throws Exception {
    for (DatanodeDescriptor target : dataNodes) {
      System.out.println(target.getNetworkLocation());
    }

    // Writer is in dc2/r1
    DatanodeStorageInfo[] targets = chooseTarget(3, dataNodes[4]);

    assertEquals(3, targets.length);

    // Count replicas per datacenter
    int dc1Count = 0;
    int dc2Count = 0;

    for (DatanodeStorageInfo target : targets) {
      String location = target.getDatanodeDescriptor().getNetworkLocation();
      if (location.startsWith("/dc1")) {
        dc1Count++;
      } else if (location.startsWith("/dc2")) {
        dc2Count++;
      }
    }

    // When writer is in dc2, dc2 becomes the local datacenter
    // For replication factor 3: 2 in local DC (dc2), 1 in remote DC (dc1)
    // dc1 preferred dfs.block.replicator.cross.dc.preferred.datacenter=/dc1
    assertEquals(1, dc2Count, "Should have 1 replicas in local datacenter (dc2)");
    assertEquals(2, dc1Count, "Should have 2 replica in remote datacenter (dc1)");
  }

  /**
   * Test that first replica is placed on the writer node
   */
  @Test
  public void testFirstReplicaOnWriter() throws Exception {
    DatanodeStorageInfo[] targets = chooseTarget(3, dataNodes[0]);

    assertNotNull(targets);
    assertTrue(targets.length > 0);

    // First replica should be on the same node as writer (or same rack)
    String writerLocation = dataNodes[0].getNetworkLocation();
    String firstReplicaLocation = targets[0].getDatanodeDescriptor().getNetworkLocation();

    assertTrue(writerLocation.substring(0, 4).equals(firstReplicaLocation.substring(0, 4)),
        "First replica should be in same datacenter as writer");
  }

  /**
   * Test with single replica (replication factor 1)
   */
  @Test
  public void testSingleReplica() throws Exception {
    DatanodeStorageInfo[] targets = chooseTarget(1, dataNodes[0]);

    assertEquals(1, targets.length);

    // Single replica should be in the local datacenter
    String location = targets[0].getDatanodeDescriptor().getNetworkLocation();
    assertTrue(location.startsWith("/dc1"), "Single replica should be in dc1");
  }

  /**
   * Test with already chosen nodes (partial replica list)
   */
  @Test
  public void testChooseTargetWithChosenNodes() throws Exception {
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
    chosenNodes.add(storages[0]); // Add first node from dc1/r1

    // Request 2 more replicas
    DatanodeStorageInfo[] targets = replicator.chooseTarget(filename, 2,
        dataNodes[0], chosenNodes, false, null, BLOCK_SIZE,
        org.apache.hadoop.hdfs.TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

    assertEquals(2, targets.length);

    // Verify targets don't include already chosen node
    for (DatanodeStorageInfo target : targets) {
      assertFalse(target.equals(storages[0]),
          "Should not include already chosen node");
    }
  }

  /**
   * Test replication factor 4 with async cross-DC write disabled (default):
   * - Writer at /dc1/r1
   * - When async mode is disabled, all replicas (local and remote) are returned
   * - Expected: 4 replicas total (2 in dc1, 2 in dc2)
   */
  @Test
  public void testChooseTargetWithReplicationFactor4() throws Exception {
    DatanodeStorageInfo[] targets = chooseTarget(4, dataNodes[0]);

    // When async mode is disabled, all replicas should be returned
    assertEquals(4, targets.length, "Should return all replicas");

    // Count replicas per datacenter
    int dc1Count = 0;
    int dc2Count = 0;

    for (DatanodeStorageInfo target : targets) {
      String location = target.getDatanodeDescriptor().getNetworkLocation();
      if (location.startsWith("/dc1")) {
        dc1Count++;
      } else if (location.startsWith("/dc2")) {
        dc2Count++;
      }
    }

    // For replication factor 4 with async disabled: 2 in local DC, 2 in remote DC
    assertEquals(2, dc1Count, "Should have 2 replicas in local datacenter");
    assertEquals(2, dc2Count, "Should have 2 replicas in remote datacenter");
  }

  /**
   * Test that all chosen targets are unique
   */
  @Test
  public void testUniqueTargets() throws Exception {
    DatanodeStorageInfo[] targets = chooseTarget(5, dataNodes[0]);

    Set<DatanodeDescriptor> uniqueNodes = new HashSet<>();
    for (DatanodeStorageInfo target : targets) {
      uniqueNodes.add(target.getDatanodeDescriptor());
    }

    assertEquals(targets.length, uniqueNodes.size(),
        "All targets should be on different datanodes");
  }

  /**
   * Test that cross-datacenter distribution works with maximum replicas
   */
  @Test
  public void testMaximumReplicas() throws Exception {
    // We have 6 datanodes total, request 6 replicas
    DatanodeStorageInfo[] targets = chooseTarget(6, dataNodes[0]);

    assertEquals(6, targets.length);

    // Count replicas per datacenter
    int dc1Count = 0;
    int dc2Count = 0;

    for (DatanodeStorageInfo target : targets) {
      String location = target.getDatanodeDescriptor().getNetworkLocation();
      if (location.startsWith("/dc1")) {
        dc1Count++;
      } else if (location.startsWith("/dc2")) {
        dc2Count++;
      }
    }

    // For replication factor 6: 6/2 = 3 in local DC
    // however dc1 4, dc2 2 nodes.
    assertEquals(4, dc1Count, "Should have 4 replicas in local datacenter");
    assertEquals(2, dc2Count, "Should have 2 replicas in remote datacenter");
  }

  /**
   * Helper method to count replicas per datacenter.
   */
  private int[] countReplicasPerDatacenter(List<DatanodeStorageInfo> replicas) {
    int dc1Count = 0;
    int dc2Count = 0;

    for (DatanodeStorageInfo storage : replicas) {
      String location = storage.getDatanodeDescriptor().getNetworkLocation();
      if (location.startsWith("/dc1")) {
        dc1Count++;
      } else if (location.startsWith("/dc2")) {
        dc2Count++;
      }
    }

    return new int[]{dc1Count, dc2Count};
  }

  /**
   * Test chooseReplicasToDelete: reduce from 6 replicas to 3 (balanced distribution).
   * Current: dc1(3), dc2(3) → Expected after deletion: dc1(2), dc2(1)
   */
  @Test
  public void testChooseReplicasToDeleteBalancedReduction() throws Exception {
    // Create 6 replicas: 3 in dc1, 3 in dc2
    List<DatanodeStorageInfo> availableReplicas = new ArrayList<>();
    availableReplicas.add(storages[0]); // dc1/r1
    availableReplicas.add(storages[1]); // dc1/r1
    availableReplicas.add(storages[2]); // dc1/r2
    availableReplicas.add(storages[3]); // dc1/r2 (will be removed)
    availableReplicas.add(storages[4]); // dc2/r1
    availableReplicas.add(storages[5]); // dc2/r2

    List<DatanodeStorageInfo> delCandidates = new ArrayList<>(availableReplicas);
    int expectedNumOfReplicas = 3;

    // Execute chooseReplicasToDelete
    List<DatanodeStorageInfo> toDelete = replicator.chooseReplicasToDelete(
        availableReplicas, delCandidates, expectedNumOfReplicas,
        new ArrayList<StorageType>(), null, null);

    // Should delete 3 replicas (6 - 3 = 3)
    assertEquals(3, toDelete.size(), "Should delete 3 replicas");

    // Calculate remaining replicas
    List<DatanodeStorageInfo> remaining = new ArrayList<>(availableReplicas);
    remaining.removeAll(toDelete);

    // Count remaining replicas per datacenter
    int[] counts = countReplicasPerDatacenter(remaining);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(2), dc2(1) - maintaining cross-DC policy
    assertEquals(2, dc1Count, "Should have 2 replicas remaining in dc1");
    assertEquals(1, dc2Count, "Should have 1 replica remaining in dc2");
  }

  /**
   * Test chooseReplicasToDelete: reduce from 4 replicas to 3 (even distribution).
   * Current: dc1(2), dc2(2) → Expected after deletion: dc1(2), dc2(1)
   */
  @Test
  public void testChooseReplicasToDeleteEvenReduction() throws Exception {
    // Create 4 replicas: 2 in dc1, 2 in dc2
    List<DatanodeStorageInfo> availableReplicas = new ArrayList<>();
    availableReplicas.add(storages[0]); // dc1/r1
    availableReplicas.add(storages[2]); // dc1/r2
    availableReplicas.add(storages[4]); // dc2/r1
    availableReplicas.add(storages[5]); // dc2/r2

    List<DatanodeStorageInfo> delCandidates = new ArrayList<>(availableReplicas);
    int expectedNumOfReplicas = 3;

    // Execute chooseReplicasToDelete
    List<DatanodeStorageInfo> toDelete = replicator.chooseReplicasToDelete(
        availableReplicas, delCandidates, expectedNumOfReplicas,
        new ArrayList<StorageType>(), null, null);

    // Should delete 1 replica (4 - 3 = 1)
    assertEquals(1, toDelete.size(), "Should delete 1 replica");

    // Calculate remaining replicas
    List<DatanodeStorageInfo> remaining = new ArrayList<>(availableReplicas);
    remaining.removeAll(toDelete);

    // Count remaining replicas per datacenter
    int[] counts = countReplicasPerDatacenter(remaining);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(2), dc2(1) - maintaining cross-DC policy
    assertEquals(2, dc1Count, "Should have 2 replicas remaining in dc1");
    assertEquals(1, dc2Count, "Should have 1 replica remaining in dc2");
  }

  /**
   * Test chooseReplicasToDelete: reduce from 5 replicas to 3 (unbalanced distribution).
   * Current: dc1(4), dc2(1) → Expected after deletion: dc1(2), dc2(1)
   */
  @Test
  public void testChooseReplicasToDeleteUnbalancedReduction() throws Exception {
    // Create 5 replicas: 4 in dc1, 1 in dc2 (over-represented in dc1)
    List<DatanodeStorageInfo> availableReplicas = new ArrayList<>();
    availableReplicas.add(storages[0]); // dc1/r1
    availableReplicas.add(storages[1]); // dc1/r1
    availableReplicas.add(storages[2]); // dc1/r2
    availableReplicas.add(storages[3]); // dc1/r2
    availableReplicas.add(storages[4]); // dc2/r1

    List<DatanodeStorageInfo> delCandidates = new ArrayList<>(availableReplicas);
    int expectedNumOfReplicas = 3;

    // Execute chooseReplicasToDelete
    List<DatanodeStorageInfo> toDelete = replicator.chooseReplicasToDelete(
        availableReplicas, delCandidates, expectedNumOfReplicas,
        new ArrayList<StorageType>(), null, null);

    // Should delete 2 replicas (5 - 3 = 2)
    assertEquals(2, toDelete.size(), "Should delete 2 replicas");

    // Calculate remaining replicas
    List<DatanodeStorageInfo> remaining = new ArrayList<>(availableReplicas);
    remaining.removeAll(toDelete);

    // Count remaining replicas per datacenter
    int[] counts = countReplicasPerDatacenter(remaining);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(2), dc2(1) - should delete from over-represented dc1
    assertEquals(2, dc1Count, "Should have 2 replicas remaining in dc1");
    assertEquals(1, dc2Count, "Should have 1 replica remaining in dc2");

    // Verify that all deleted replicas are from dc1
    for (DatanodeStorageInfo deleted : toDelete) {
      String location = deleted.getDatanodeDescriptor().getNetworkLocation();
      assertTrue(location.startsWith("/dc1"), "Deleted replica should be from dc1");
    }
  }

  /**
   * Test chooseReplicasToDelete: reduce from 5 replicas to 4 (minimal reduction).
   * Current: dc1(3), dc2(2) → Expected after deletion: dc1(2), dc2(2)
   */
  @Test
  public void testChooseReplicasToDeleteMinimalReduction() throws Exception {
    // Create 5 replicas: 3 in dc1, 2 in dc2
    List<DatanodeStorageInfo> availableReplicas = new ArrayList<>();
    availableReplicas.add(storages[0]); // dc1/r1
    availableReplicas.add(storages[1]); // dc1/r1
    availableReplicas.add(storages[2]); // dc1/r2
    availableReplicas.add(storages[4]); // dc2/r1
    availableReplicas.add(storages[5]); // dc2/r2

    List<DatanodeStorageInfo> delCandidates = new ArrayList<>(availableReplicas);
    int expectedNumOfReplicas = 4;

    // Execute chooseReplicasToDelete
    List<DatanodeStorageInfo> toDelete = replicator.chooseReplicasToDelete(
        availableReplicas, delCandidates, expectedNumOfReplicas,
        new ArrayList<StorageType>(), null, null);

    // Should delete 1 replica (5 - 4 = 1)
    assertEquals(1, toDelete.size(), "Should delete 1 replica");

    // Calculate remaining replicas
    List<DatanodeStorageInfo> remaining = new ArrayList<>(availableReplicas);
    remaining.removeAll(toDelete);

    // Count remaining replicas per datacenter
    int[] counts = countReplicasPerDatacenter(remaining);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(2), dc2(2) - should delete from over-represented dc1
    assertEquals(2, dc1Count, "Should have 2 replicas remaining in dc1");
    assertEquals(2, dc2Count, "Should have 2 replicas remaining in dc2");

    // Verify that deleted replica is from dc1
    DatanodeStorageInfo deleted = toDelete.get(0);
    String location = deleted.getDatanodeDescriptor().getNetworkLocation();
    assertTrue(location.startsWith("/dc1"), "Deleted replica should be from dc1");
  }

  /**
   * Test chooseReplicasToDelete: datacenter diversity is maintained.
   * Verify that after deletion, the block still satisfies cross-DC placement policy.
   */
  @Test
  public void testChooseReplicasToDeleteMaintainsDiversity() throws Exception {
    // Create 6 replicas: 3 in dc1, 3 in dc2
    List<DatanodeStorageInfo> availableReplicas = new ArrayList<>();
    availableReplicas.add(storages[0]); // dc1/r1
    availableReplicas.add(storages[1]); // dc1/r1
    availableReplicas.add(storages[2]); // dc1/r2
    availableReplicas.add(storages[4]); // dc2/r1
    availableReplicas.add(storages[5]); // dc2/r2
    availableReplicas.add(storages[3]); // dc1/r2

    List<DatanodeStorageInfo> delCandidates = new ArrayList<>(availableReplicas);
    int expectedNumOfReplicas = 3;

    // Execute chooseReplicasToDelete
    List<DatanodeStorageInfo> toDelete = replicator.chooseReplicasToDelete(
        availableReplicas, delCandidates, expectedNumOfReplicas,
        new ArrayList<StorageType>(), null, null);

    // Calculate remaining replicas
    List<DatanodeStorageInfo> remaining = new ArrayList<>(availableReplicas);
    remaining.removeAll(toDelete);

    // Convert to DatanodeInfo array for verifyBlockPlacement
    DatanodeInfo[] remainingNodes = new DatanodeInfo[remaining.size()];
    for (int i = 0; i < remaining.size(); i++) {
      remainingNodes[i] = remaining.get(i).getDatanodeDescriptor();
    }

    // Verify block placement policy is satisfied
    BlockPlacementStatus status = replicator.verifyBlockPlacement(
        remainingNodes, expectedNumOfReplicas);

    assertTrue(status.isPlacementPolicySatisfied(),
        "Block placement policy should be satisfied after deletion");
  }

  /**
   * Test chooseReplicasToDelete: no deletion needed when already at target.
   * Current: dc1(2), dc2(1) → Expected: no deletion
   */
  @Test
  public void testChooseReplicasToDeleteNoDeletion() throws Exception {
    // Create 3 replicas: 2 in dc1, 1 in dc2 (already at target)
    List<DatanodeStorageInfo> availableReplicas = new ArrayList<>();
    availableReplicas.add(storages[0]); // dc1/r1
    availableReplicas.add(storages[2]); // dc1/r2
    availableReplicas.add(storages[4]); // dc2/r1

    List<DatanodeStorageInfo> delCandidates = new ArrayList<>(availableReplicas);
    int expectedNumOfReplicas = 3;

    // Execute chooseReplicasToDelete
    List<DatanodeStorageInfo> toDelete = replicator.chooseReplicasToDelete(
        availableReplicas, delCandidates, expectedNumOfReplicas,
        new ArrayList<StorageType>(), null, null);

    // Should not delete any replicas
    assertEquals(0, toDelete.size(), "Should not delete any replicas");
  }

  /**
   * Test under-replication: add replicas to reach target while maintaining cross-DC policy.
   * Current: dc1(1), dc2(1) → Target RF=3 → Should add 1 more to dc1
   */
  @Test
  public void testUnderReplicationBalanced() throws Exception {
    // Existing replicas: 1 in dc1, 1 in dc2
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
    chosenNodes.add(storages[0]); // dc1/r1
    chosenNodes.add(storages[4]); // dc2/r1

    // Request 1 more replica to reach RF=3
    DatanodeStorageInfo[] newTargets = replicator.chooseTarget(filename, 1,
        dataNodes[0], chosenNodes, false, null, BLOCK_SIZE,
        org.apache.hadoop.hdfs.TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

    assertEquals(1, newTargets.length, "Should add 1 replica");

    // Combine existing and new replicas
    List<DatanodeStorageInfo> allReplicas = new ArrayList<>(chosenNodes);
    allReplicas.addAll(Arrays.asList(newTargets));

    // Count replicas per datacenter
    int[] counts = countReplicasPerDatacenter(allReplicas);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(2), dc2(1) - maintaining cross-DC policy
    assertEquals(2, dc1Count, "Should have 2 replicas in dc1");
    assertEquals(1, dc2Count, "Should have 1 replica in dc2");
  }

  /**
   * Test under-replication: add replicas when one DC is missing.
   * Current: dc1(2), dc2(0) → Target RF=3 → Should add 1 to dc2
   */
  @Test
  public void testUnderReplicationUnbalanced() throws Exception {
    // Existing replicas: 2 in dc1, 0 in dc2 (unbalanced)
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
    chosenNodes.add(storages[0]); // dc1/r1
    chosenNodes.add(storages[2]); // dc1/r2

    // Request 1 more replica to reach RF=3
    DatanodeStorageInfo[] newTargets = replicator.chooseTarget(filename, 1,
        dataNodes[0], chosenNodes, false, null, BLOCK_SIZE,
        org.apache.hadoop.hdfs.TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

    assertEquals(1, newTargets.length, "Should add 1 replica");

    // Combine existing and new replicas
    List<DatanodeStorageInfo> allReplicas = new ArrayList<>(chosenNodes);
    allReplicas.addAll(Arrays.asList(newTargets));

    // Count replicas per datacenter
    int[] counts = countReplicasPerDatacenter(allReplicas);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(2), dc2(1) - new replica should go to dc2
    assertEquals(2, dc1Count, "Should have 2 replicas in dc1");
    assertEquals(1, dc2Count, "Should have 1 replica in dc2");

    // Verify the new replica is in dc2
    String newLocation = newTargets[0].getDatanodeDescriptor().getNetworkLocation();
    assertTrue(newLocation.startsWith("/dc2"), "New replica should be in dc2");
  }

  /**
   * Test under-replication: add multiple replicas from severely under-replicated state.
   * Current: dc1(1), dc2(0) → Target RF=3 → Should add dc1(1), dc2(1)
   */
  @Test
  public void testUnderReplicationSevere() throws Exception {
    // Existing replica: 1 in dc1, 0 in dc2 (severely under-replicated)
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
    chosenNodes.add(storages[0]); // dc1/r1

    // Request 2 more replicas to reach RF=3
    DatanodeStorageInfo[] newTargets = replicator.chooseTarget(filename, 2,
        dataNodes[0], chosenNodes, false, null, BLOCK_SIZE,
        org.apache.hadoop.hdfs.TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

    assertEquals(2, newTargets.length, "Should add 2 replicas");

    // Combine existing and new replicas
    List<DatanodeStorageInfo> allReplicas = new ArrayList<>(chosenNodes);
    allReplicas.addAll(Arrays.asList(newTargets));

    // Count replicas per datacenter
    int[] counts = countReplicasPerDatacenter(allReplicas);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(2), dc2(1) - maintaining cross-DC policy
    assertEquals(2, dc1Count, "Should have 2 replicas in dc1");
    assertEquals(1, dc2Count, "Should have 1 replica in dc2");
  }

  /**
   * Test under-replication: add replicas to reach RF=5 from RF=3.
   * Current: dc1(2), dc2(1) → Target RF=5 → Should add dc1(1), dc2(1)
   */
  @Test
  public void testUnderReplicationScaleUp() throws Exception {
    // Existing replicas: 2 in dc1, 1 in dc2 (current RF=3)
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
    chosenNodes.add(storages[0]); // dc1/r1
    chosenNodes.add(storages[2]); // dc1/r2
    chosenNodes.add(storages[4]); // dc2/r1

    // Request 2 more replicas to reach RF=5
    DatanodeStorageInfo[] newTargets = replicator.chooseTarget(filename, 2,
        null, chosenNodes, false, null, BLOCK_SIZE,
        org.apache.hadoop.hdfs.TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

    assertEquals(2, newTargets.length, "Should add 2 replicas");

    // Combine existing and new replicas
    List<DatanodeStorageInfo> allReplicas = new ArrayList<>(chosenNodes);
    allReplicas.addAll(Arrays.asList(newTargets));

    // Count replicas per datacenter
    int[] counts = countReplicasPerDatacenter(allReplicas);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(3), dc2(2) - maintaining cross-DC policy for RF=5
    assertEquals(3, dc1Count, "Should have 3 replicas in dc1");
    assertEquals(2, dc2Count, "Should have 2 replicas in dc2");
  }

  /**
   * Test under-replication: verify block placement is satisfied after adding replicas.
   * Current: dc1(1), dc2(0) → Target RF=3 → Verify placement policy satisfied
   */
  @Test
  public void testUnderReplicationVerifyPlacement() throws Exception {
    // Existing replica: 1 in dc1, 0 in dc2
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
    chosenNodes.add(storages[0]); // dc1/r1

    // Request 2 more replicas to reach RF=3
    DatanodeStorageInfo[] newTargets = replicator.chooseTarget(filename, 2,
        dataNodes[0], chosenNodes, false, null, BLOCK_SIZE,
        org.apache.hadoop.hdfs.TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

    // Combine existing and new replicas
    List<DatanodeStorageInfo> allReplicas = new ArrayList<>(chosenNodes);
    allReplicas.addAll(Arrays.asList(newTargets));

    // Convert to DatanodeInfo array for verifyBlockPlacement
    DatanodeInfo[] allNodes = new DatanodeInfo[allReplicas.size()];
    for (int i = 0; i < allReplicas.size(); i++) {
      allNodes[i] = allReplicas.get(i).getDatanodeDescriptor();
    }

    // Verify block placement policy is satisfied
    BlockPlacementStatus status = replicator.verifyBlockPlacement(
        allNodes, 3);

    assertTrue(status.isPlacementPolicySatisfied(),
        "Block placement policy should be satisfied after replication");
  }

  /**
   * Test under-replication with node failure: one replica lost from dc2.
   * Current: dc1(2), dc2(0) [lost 1 from dc2] → Target RF=3 → Should restore to dc2
   */
  @Test
  public void testUnderReplicationAfterNodeFailure() throws Exception {
    // Simulate: had dc1(2), dc2(1), but dc2 replica was lost
    // Current state: dc1(2), dc2(0)
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
    chosenNodes.add(storages[0]); // dc1/r1
    chosenNodes.add(storages[2]); // dc1/r2
    // storages[4] was lost (dc2/r1)

    // Request 1 replica to restore RF=3
    DatanodeStorageInfo[] newTargets = replicator.chooseTarget(filename, 1,
        dataNodes[0], chosenNodes, false, null, BLOCK_SIZE,
        org.apache.hadoop.hdfs.TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

    assertEquals(1, newTargets.length, "Should add 1 replica");

    // Combine existing and new replicas
    List<DatanodeStorageInfo> allReplicas = new ArrayList<>(chosenNodes);
    allReplicas.addAll(Arrays.asList(newTargets));

    // Count replicas per datacenter
    int[] counts = countReplicasPerDatacenter(allReplicas);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(2), dc2(1) - new replica should restore dc2
    assertEquals(2, dc1Count, "Should have 2 replicas in dc1");
    assertEquals(1, dc2Count, "Should have 1 replica in dc2");

    // Verify the new replica is in dc2 (restoring cross-DC diversity)
    String newLocation = newTargets[0].getDatanodeDescriptor().getNetworkLocation();
    assertTrue(newLocation.startsWith("/dc2"), "New replica should restore dc2");
  }

  /**
   * Test replication factor increase with unbalanced initial distribution.
   * Bug case: When existing replicas are DC2(1), DC1(2), increasing RF from 3 to 4
   * should add a replica to DC2, not DC1.
   *
   * Initial state: DC2/RACK3(1), DC1/RACK1(2)
   * Expected after RF increase to 4: DC2(2), DC1(2)
   */
  @Test
  public void testReplicationFactorIncreaseUnbalanced() throws Exception {
    // Existing replicas: 1 in dc2, 2 in dc1 (unbalanced - dc2 is minority)
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
    chosenNodes.add(storages[4]); // dc2/r1
    chosenNodes.add(storages[0]); // dc1/r1
    chosenNodes.add(storages[1]); // dc1/r1

    // Request 1 more replica to reach RF=4
    DatanodeStorageInfo[] newTargets = replicator.chooseTarget(filename, 1,
        null, chosenNodes, false, null, BLOCK_SIZE,
        org.apache.hadoop.hdfs.TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

    assertEquals(1, newTargets.length, "Should add 1 replica");

    // Combine existing and new replicas
    List<DatanodeStorageInfo> allReplicas = new ArrayList<>(chosenNodes);
    allReplicas.addAll(Arrays.asList(newTargets));

    // Count replicas per datacenter
    int[] counts = countReplicasPerDatacenter(allReplicas);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(2), dc2(2) - new replica should go to dc2 to balance
    assertEquals(2, dc1Count, "Should have 2 replicas in dc1");
    assertEquals(2, dc2Count, "Should have 2 replicas in dc2");

    // Verify the new replica is in dc2 (balancing the distribution)
    String newLocation = newTargets[0].getDatanodeDescriptor().getNetworkLocation();
    assertTrue(newLocation.startsWith("/dc2"),
        "New replica should be in dc2 to balance distribution");
  }

  /**
   * Test replication factor increase from 3 to 5 with initial unbalanced state.
   * Initial: DC2(1), DC1(2)
   * Expected after RF increase to 5: DC1(3), DC2(2)
   */
  @Test
  public void testReplicationFactorIncrease3to5Unbalanced() throws Exception {
    // Existing replicas: 1 in dc2, 2 in dc1
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
    chosenNodes.add(storages[4]); // dc2/r1
    chosenNodes.add(storages[0]); // dc1/r1
    chosenNodes.add(storages[1]); // dc1/r1

    // Request 2 more replicas to reach RF=5
    DatanodeStorageInfo[] newTargets = replicator.chooseTarget(filename, 2,
        dataNodes[0], chosenNodes, false, null, BLOCK_SIZE,
        org.apache.hadoop.hdfs.TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

    assertEquals(2, newTargets.length, "Should add 2 replicas");

    // Combine existing and new replicas
    List<DatanodeStorageInfo> allReplicas = new ArrayList<>(chosenNodes);
    allReplicas.addAll(Arrays.asList(newTargets));

    // Count replicas per datacenter
    int[] counts = countReplicasPerDatacenter(allReplicas);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(3), dc2(2) - should add 1 to dc1, 1 to dc2
    assertEquals(3, dc1Count, "Should have 3 replicas in dc1");
    assertEquals(2, dc2Count, "Should have 2 replicas in dc2");

    // Count new replicas per datacenter
    int newDc1Count = 0;
    int newDc2Count = 0;
    for (DatanodeStorageInfo newTarget : newTargets) {
      String location = newTarget.getDatanodeDescriptor().getNetworkLocation();
      if (location.startsWith("/dc1")) {
        newDc1Count++;
      } else if (location.startsWith("/dc2")) {
        newDc2Count++;
      }
    }

    // Should add 1 to each DC to maintain balance
    assertEquals(1, newDc1Count, "Should add 1 replica to dc1");
    assertEquals(1, newDc2Count, "Should add 1 replica to dc2");
  }

  /**
   * Test replication factor increase with balanced initial state.
   * Initial: DC1(2), DC2(1) - already following policy for RF=3
   * Expected after RF increase to 6: DC1(4), DC2(2)
   */
  @Test
  public void testReplicationFactorIncreaseBalanced() throws Exception {
    // Existing replicas: 2 in dc1, 1 in dc2 (balanced for RF=3)
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
    chosenNodes.add(storages[0]); // dc1/r1
    chosenNodes.add(storages[2]); // dc1/r2
    chosenNodes.add(storages[4]); // dc2/r1

    // Request 3 more replicas to reach RF=6
    DatanodeStorageInfo[] newTargets = replicator.chooseTarget(filename, 3,
        dataNodes[0], chosenNodes, false, null, BLOCK_SIZE,
        org.apache.hadoop.hdfs.TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

    assertEquals(3, newTargets.length, "Should add 3 replicas");

    // Combine existing and new replicas
    List<DatanodeStorageInfo> allReplicas = new ArrayList<>(chosenNodes);
    allReplicas.addAll(Arrays.asList(newTargets));

    // Count replicas per datacenter
    int[] counts = countReplicasPerDatacenter(allReplicas);
    int dc1Count = counts[0];
    int dc2Count = counts[1];

    // Expected: dc1(4), dc2(2) for RF=6
    // We have 4 nodes in dc1, 2 nodes in dc2
    assertEquals(4, dc1Count, "Should have 4 replicas in dc1");
    assertEquals(2, dc2Count, "Should have 2 replicas in dc2");
  }
}