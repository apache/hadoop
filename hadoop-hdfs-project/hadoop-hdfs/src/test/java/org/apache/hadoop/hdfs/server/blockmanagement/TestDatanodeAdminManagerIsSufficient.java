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

import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.DECOMMISSIONING;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.EXCESS;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.LIVE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DatanodeAdminManager#isSufficient}.
 *
 * Covers the fix for HDFS-17722: when a standby NameNode incorrectly marks a
 * replica as excess due to a timing race during decommissioning, excess replicas
 * (which are physically present on disk) must be counted toward the replication
 * sufficiency check so that decommission can still complete.
 */
public class TestDatanodeAdminManagerIsSufficient {

  private BlockManager blockManager;
  private DatanodeAdminManager adminManager;
  private BlockInfo block;
  private INodeFile blockCollection;

  // Replication factor used across tests
  private static final int RF = 2;

  @BeforeEach
  public void setUp() {
    blockManager = mock(BlockManager.class);
    HeartbeatManager hbManager = mock(HeartbeatManager.class);
    adminManager = new DatanodeAdminManager(mock(
        org.apache.hadoop.hdfs.server.namenode.Namesystem.class),
        blockManager, hbManager);

    block = mock(BlockInfo.class);
    blockCollection = mock(INodeFile.class);

    // Non-UC block by default
    when(blockCollection.isUnderConstruction()).thenReturn(false);

    // Default RF = 2
    when(blockManager.getDefaultStorageNum(any(BlockInfo.class))).thenReturn(RF);
    when(blockManager.getMinStorageNum(any(BlockInfo.class))).thenReturn((short) 1);
  }

  /**
   * Sets up BlockManager mocks to simulate a block that does NOT already have
   * enough effective replicas, and has numExpected > numLive.
   */
  private void setupInsufficientEffectiveReplicas(int numExpected) {
    when(blockManager.hasEnoughEffectiveReplicas(any(), any(), anyInt()))
        .thenReturn(false);
    when(blockManager.getExpectedLiveRedundancyNum(any(BlockInfo.class), any()))
        .thenReturn((short) numExpected);
  }

  private NumberReplicas replicas(int live, int excess, int decommissioning) {
    NumberReplicas nr = new NumberReplicas();
    nr.add(LIVE, live);
    nr.add(EXCESS, excess);
    nr.add(DECOMMISSIONING, decommissioning);
    return nr;
  }

  /**
   * HDFS-17722: The bug scenario.
   * Standby NN has: live=1, excess=1, decommissioning=1, RF=2.
   * live alone (1) < RF (2), but live + excess (2) >= RF (2).
   * The fix must return true so decommission can complete.
   */
  @Test
  public void testExcessReplicaCountsTowardSufficiency() {
    NumberReplicas nr = replicas(1, 1, 1);
    setupInsufficientEffectiveReplicas(RF);
    // live=1 satisfies minStorage (min=1)
    when(blockManager.hasMinStorage(any(BlockInfo.class), anyInt())).thenReturn(true);

    assertTrue(adminManager.isSufficient(block, blockCollection, nr,
        true, false),
        "live=1 + excess=1 should be sufficient for decommission with RF=2");
  }

  /**
   * Normal case: live=2, excess=0, decommissioning=1, RF=2.
   * Should still return true (baseline — not broken by the fix).
   */
  @Test
  public void testNormalDecommissionStillSufficient() {
    NumberReplicas nr = replicas(2, 0, 1);
    // live=2 == RF=2 → hasEnoughEffectiveReplicas returns true immediately
    when(blockManager.hasEnoughEffectiveReplicas(any(), any(), anyInt()))
        .thenReturn(true);

    assertTrue(adminManager.isSufficient(block, blockCollection, nr,
        true, false),
        "live=2 with no excess should be sufficient for decommission with RF=2");
  }

  /**
   * Safety guard: live=0, excess=2, decommissioning=1, RF=2.
   * live + excess = 2 >= RF, but live=0 < minStorage(1).
   * Must return false — no guaranteed durable copy exists.
   */
  @Test
  public void testNoLiveReplicaBlocksDecommission() {
    NumberReplicas nr = replicas(0, 2, 1);
    setupInsufficientEffectiveReplicas(RF);
    // live=0 fails minStorage check
    when(blockManager.hasMinStorage(any(BlockInfo.class), anyInt())).thenReturn(false);

    assertFalse(adminManager.isSufficient(block, blockCollection, nr,
        true, false),
        "live=0 should block decommission even if excess=2 covers RF");
  }

  /**
   * Insufficient even counting excess: live=0, excess=1, RF=2.
   * live + excess = 1 < RF(2) AND live=0 < minStorage. Must return false.
   */
  @Test
  public void testInsufficientEvenWithExcess() {
    NumberReplicas nr = replicas(0, 1, 1);
    setupInsufficientEffectiveReplicas(RF);
    when(blockManager.hasMinStorage(any(BlockInfo.class), anyInt())).thenReturn(false);

    assertFalse(adminManager.isSufficient(block, blockCollection, nr,
        true, false),
        "live=0 + excess=1 should not be sufficient for decommission with RF=2");
  }

  /**
   * Excess pushes total over RF but live barely meets min:
   * live=1, excess=2, decommissioning=1, RF=2.
   * live + excess = 3 >= RF(2), live=1 >= min(1) → sufficient.
   */
  @Test
  public void testExcessAboveRFWithMinLive() {
    NumberReplicas nr = replicas(1, 2, 1);
    setupInsufficientEffectiveReplicas(RF);
    when(blockManager.hasMinStorage(any(BlockInfo.class), anyInt())).thenReturn(true);

    assertTrue(adminManager.isSufficient(block, blockCollection, nr,
        true, false),
        "live=1 + excess=2 should be sufficient for decommission with RF=2");
  }
}
