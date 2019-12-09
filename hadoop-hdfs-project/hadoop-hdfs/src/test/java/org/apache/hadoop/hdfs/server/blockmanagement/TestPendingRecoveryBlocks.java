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

import org.apache.hadoop.hdfs.protocol.Block;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class contains unit tests for PendingRecoveryBlocks.java functionality.
 */
public class TestPendingRecoveryBlocks {

  private PendingRecoveryBlocks pendingRecoveryBlocks;
  private final long recoveryTimeout = 1000L;

  private final BlockInfo blk1 = getBlock(1);
  private final BlockInfo blk2 = getBlock(2);
  private final BlockInfo blk3 = getBlock(3);

  @Before
  public void setUp() {
    pendingRecoveryBlocks =
        Mockito.spy(new PendingRecoveryBlocks(recoveryTimeout));
  }

  BlockInfo getBlock(long blockId) {
    return new BlockInfoContiguous(new Block(blockId), (short) 0);
  }

  @Test
  public void testAddDifferentBlocks() {
    assertTrue(pendingRecoveryBlocks.add(blk1));
    assertTrue(pendingRecoveryBlocks.isUnderRecovery(blk1));
    assertTrue(pendingRecoveryBlocks.add(blk2));
    assertTrue(pendingRecoveryBlocks.isUnderRecovery(blk2));
    assertTrue(pendingRecoveryBlocks.add(blk3));
    assertTrue(pendingRecoveryBlocks.isUnderRecovery(blk3));
  }

  @Test
  public void testAddAndRemoveBlocks() {
    // Add blocks
    assertTrue(pendingRecoveryBlocks.add(blk1));
    assertTrue(pendingRecoveryBlocks.add(blk2));

    // Remove blk1
    pendingRecoveryBlocks.remove(blk1);

    // Adding back blk1 should succeed
    assertTrue(pendingRecoveryBlocks.add(blk1));
  }

  @Test
  public void testAddBlockWithPreviousRecoveryTimedOut() {
    // Add blk
    Mockito.doReturn(0L).when(pendingRecoveryBlocks).getTime();
    assertTrue(pendingRecoveryBlocks.add(blk1));

    // Should fail, has not timed out yet
    Mockito.doReturn(recoveryTimeout / 2).when(pendingRecoveryBlocks).getTime();
    assertFalse(pendingRecoveryBlocks.add(blk1));

    // Should succeed after timing out
    Mockito.doReturn(recoveryTimeout * 2).when(pendingRecoveryBlocks).getTime();
    assertTrue(pendingRecoveryBlocks.add(blk1));
  }
}
