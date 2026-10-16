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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link IncrementalBlockReportManager}'s OOM protection: the
 * pending-block-count cap enforced via
 * {@link IncrementalBlockReportManager#clearIBRsIfNeeded()}.
 */
public class TestIncrementalBlockReportManager {

  private DataNodeMetrics mockMetrics;
  private DatanodeStorage storage;

  @BeforeEach
  public void setUp() {
    mockMetrics = mock(DataNodeMetrics.class);
    storage = new DatanodeStorage("storage-1");
  }

  private void addBlocks(IncrementalBlockReportManager ibr,
      DatanodeStorage st, int startId, int count) {
    for (int i = 0; i < count; i++) {
      Block block = new Block(startId + i, 1024, 1000 + startId + i);
      ibr.addRDBI(new ReceivedDeletedBlockInfo(
          block, BlockStatus.RECEIVED_BLOCK, null), st);
    }
  }

  /**
   * The pending block counter must be maintained accurately (in O(1)) as
   * blocks are added, deduplicated and cleared.
   */
  @Test
  public void testPendingBlockCountAccounting() {
    IncrementalBlockReportManager ibr =
        new IncrementalBlockReportManager(0, 0, mockMetrics);

    addBlocks(ibr, storage, 0, 5);
    assertEquals(5, ibr.getPendingBlockCount());

    // Re-adding an existing block (same Block key) must not increase count.
    ibr.addRDBI(new ReceivedDeletedBlockInfo(
        new Block(0, 1024, 1000), BlockStatus.DELETED_BLOCK, null), storage);
    assertEquals(5, ibr.getPendingBlockCount(),
        "Re-adding an existing block must not change the count");

    ibr.clearIBRs();
    assertEquals(0, ibr.getPendingBlockCount());
  }

  /**
   * The counter must be consistent across multiple storages.
   */
  @Test
  public void testPendingBlockCountMultipleStorages() {
    IncrementalBlockReportManager ibr =
        new IncrementalBlockReportManager(0, 0, mockMetrics);
    DatanodeStorage s1 = new DatanodeStorage("s1");
    DatanodeStorage s2 = new DatanodeStorage("s2");

    addBlocks(ibr, s1, 0, 5);
    addBlocks(ibr, s2, 100, 8);
    assertEquals(13, ibr.getPendingBlockCount());
  }

  /**
   * When the pending block count reaches the configured cap,
   * {@code clearIBRsIfNeeded()} must clear the queue and report that it did.
   */
  @Test
  public void testClearOnSizeCap() {
    final long cap = 100;
    IncrementalBlockReportManager ibr =
        new IncrementalBlockReportManager(0, cap, mockMetrics);

    // Below the cap: nothing should be cleared.
    addBlocks(ibr, storage, 0, (int) cap - 1);
    assertFalse(ibr.clearIBRsIfNeeded(),
        "Queue below cap must not be cleared");
    assertEquals(cap - 1, ibr.getPendingBlockCount());

    // Reach the cap: the queue must be cleared.
    addBlocks(ibr, storage, 1000, 1);
    assertEquals(cap, ibr.getPendingBlockCount());
    assertTrue(ibr.clearIBRsIfNeeded(),
        "Queue at cap must be cleared");
    assertEquals(0, ibr.getPendingBlockCount(),
        "Queue must be empty after clearing");
  }

  /**
   * The cap disabled (0): the queue is never cleared regardless of size,
   * preserving the historical behavior.
   */
  @Test
  public void testCapDisabled() {
    IncrementalBlockReportManager ibr =
        new IncrementalBlockReportManager(0, 0, mockMetrics);
    addBlocks(ibr, storage, 0, 5000);
    assertFalse(ibr.clearIBRsIfNeeded(),
        "With the cap disabled the queue must never be cleared");
    assertEquals(5000, ibr.getPendingBlockCount());
  }
}
