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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.junit.jupiter.api.Test;

/**
 * This class tests the BlockPlacementPolicyCrossDC with async cross-DC write enabled.
 *
 * When async cross-DC write is enabled, only replicas in the writer's local datacenter
 * are returned synchronously, while remote datacenter replicas are written asynchronously
 * in the background.
 */
public class TestBlockPlacementPolicyCrossDCAsyncEnabled extends BaseReplicationPolicyCrossDCTest {

  /**
   * Creates a multi-datacenter topology with 2 datacenters and enables async cross-DC write:
   * - /dc1/r1: 2 datanodes
   * - /dc1/r2: 2 datanodes
   * - /dc2/r1: 2 datanodes
   * - /dc2/r2: 2 datanodes
   */
  @Override
  DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf) {
    conf.set(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_PREFERRED_DATACENTER_KEY, "/dc1");
    // Enable async cross-DC write
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CROSS_DC_ASYNC_ENABLED_KEY, true);

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
   * Test replication factor 3 with async cross-DC write enabled:
   * - Writer at /dc1/r1
   * - When async mode is enabled, only local datacenter replicas are returned
   * - Expected: 2 replicas in dc1 only (dc2 replicas written asynchronously)
   */
  @Test
  public void testChooseTargetWithReplicationFactor3() {
    DatanodeStorageInfo[] targets = chooseTarget(3, dataNodes[0]);

    // When async mode is enabled, only local DC replicas should be returned
    // For replication factor 3: ceil(3/2) = 2 in local DC
    assertEquals(2, targets.length, "Should return only local datacenter replicas");

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

    assertEquals(2, dc1Count, "Should have 2 replicas in local datacenter");
    assertEquals(0, dc2Count, "Should have 0 replicas in remote datacenter");
  }

  /**
   * Test replication factor 4 with async cross-DC write enabled:
   * - Writer at /dc1/r1
   * - When async mode is enabled, only local datacenter replicas are returned
   * - Expected: 2 replicas in dc1 only (dc2 replicas written asynchronously)
   */
  @Test
  public void testChooseTargetWithReplicationFactor4() {
    DatanodeStorageInfo[] targets = chooseTarget(4, dataNodes[0]);
    // When async mode is enabled, only local DC replicas should be returned
    // For replication factor 4: ceil(4/2) = 2 in local DC
    assertEquals(2, targets.length, "Should return only local datacenter replicas");

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

    assertEquals(2, dc1Count, "Should have 2 replicas in local datacenter");
    assertEquals(0, dc2Count, "Should have 0 replicas in remote datacenter");
  }

  /**
   * Test replication factor 5 with async cross-DC write enabled:
   * - Writer at /dc1/r1
   * - When async mode is enabled, only local datacenter replicas are returned
   * - Expected: 3 replicas in dc1 only (dc2 replicas written asynchronously)
   */
  @Test
  public void testChooseTargetWithReplicationFactor5() {
    DatanodeStorageInfo[] targets = chooseTarget(5, dataNodes[0]);

    // When async mode is enabled, only local DC replicas should be returned
    // For replication factor 5: ceil(5/2) = 3 in local DC
    assertEquals(3, targets.length, "Should return only local datacenter replicas");

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

    assertEquals(3, dc1Count, "Should have 3 replicas in local datacenter");
    assertEquals(0, dc2Count, "Should have 0 replicas in remote datacenter");
  }

  /**
   * Test replication factor 2 with async cross-DC write enabled:
   * - Writer at /dc1/r1
   * - When async mode is enabled, only local datacenter replicas are returned
   * - Expected: 1 replica in dc1 only (dc2 replicas written asynchronously)
   */
  @Test
  public void testChooseTargetWithReplicationFactor2() {
    DatanodeStorageInfo[] targets = chooseTarget(2, dataNodes[0]);

    // When async mode is enabled, only local DC replicas should be returned
    // For replication factor 2: ceil(2/2) = 1 in local DC
    assertEquals(1, targets.length, "Should return only local datacenter replicas");

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

    assertEquals(1, dc1Count, "Should have 1 replica in local datacenter");
    assertEquals(0, dc2Count, "Should have 0 replicas in remote datacenter");
  }

  /**
   * Test with writer from dc2 with async cross-DC write enabled:
   * - Writer at /dc2/r1
   * - When async mode is enabled, only local datacenter (dc2) replicas are returned
   * - Expected: 2 replicas in dc2 only (dc1 replicas written asynchronously)
   */
  @Test
  public void testChooseTargetFromDC2() {
    DatanodeStorageInfo[] targets = chooseTarget(3, dataNodes[4]);

    // When async mode is enabled and writer is in dc2, only dc2 replicas should be returned
    // For replication factor 3: ceil(3/2) = 2 in local DC (dc2)
    assertEquals(2, targets.length, "Should return only local datacenter replicas");

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

    assertEquals(2, dc2Count, "Should have 2 replicas in local datacenter (dc2)");
    assertEquals(0, dc1Count, "Should have 0 replicas in remote datacenter (dc1)");

    BlockPlacementStatus status = replicator.verifyBlockPlacement(DatanodeStorageInfo.toDatanodeInfos(targets), 3);

    assertFalse(status.isPlacementPolicySatisfied(),
        "Policy should NOT be satisfied when block is not in preferred DC");

    // Should require 2 additional replicas for preferred DC
    assertEquals(2, status.getAdditionalReplicasRequired(),
        "Should require 2 additional replicas (min(ceil(3/2), 2) - 0 = 2)");
  }

  /**
   * Test with single replica (replication factor 1) with async cross-DC write enabled:
   * - Writer at /dc1/r1
   * - Expected: 1 replica in dc1 only
   */
  @Test
  public void testSingleReplica() {
    DatanodeStorageInfo[] targets = chooseTarget(1, dataNodes[0]);

    assertEquals(1, targets.length, "Should return 1 replica");

    // Single replica should be in the local datacenter
    String location = targets[0].getDatanodeDescriptor().getNetworkLocation();
    assertTrue(location.startsWith("/dc1"), "Single replica should be in dc1");
  }

  /**
   * Test maximum replicas (6) with async cross-DC write enabled:
   * - Writer at /dc1/r1
   * - When async mode is enabled, only local datacenter replicas are returned
   * - Expected: 3 replicas in dc1 only (dc2 replicas written asynchronously)
   */
  @Test
  public void testMaximumReplicas() {
    DatanodeStorageInfo[] targets = chooseTarget(6, dataNodes[0]);

    // When async mode is enabled, only local DC replicas should be returned
    // For replication factor 6: ceil(6/2) = 3 in local DC
    assertEquals(3, targets.length, "Should return only local datacenter replicas");

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

    assertEquals(3, dc1Count, "Should have 3 replicas in local datacenter");
    assertEquals(0, dc2Count, "Should have 0 replicas in remote datacenter");
  }
}
