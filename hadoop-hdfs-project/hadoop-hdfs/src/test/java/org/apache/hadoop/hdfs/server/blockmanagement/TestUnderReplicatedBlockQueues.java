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

import java.util.Iterator;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TestUnderReplicatedBlockQueues {

  private final ErasureCodingPolicy ecPolicy =
      ErasureCodingPolicyManager.getSystemDefaultPolicy();

  private BlockInfo genBlockInfo(long id) {
    return new BlockInfoContiguous(new Block(id), (short) 3);
  }

  private BlockInfo genStripedBlockInfo(long id, long numBytes) {
    BlockInfoStriped sblk =  new BlockInfoStriped(new Block(id), ecPolicy);
    sblk.setNumBytes(numBytes);
    return sblk;
  }

  /**
   * Test that adding blocks with different replication counts puts them
   * into different queues
   * @throws Throwable if something goes wrong
   */
  @Test
  public void testBlockPriorities() throws Throwable {
    UnderReplicatedBlocks queues = new UnderReplicatedBlocks();
    BlockInfo block1 = genBlockInfo(1);
    BlockInfo block2 = genBlockInfo(2);
    BlockInfo block_very_under_replicated = genBlockInfo(3);
    BlockInfo block_corrupt = genBlockInfo(4);
    BlockInfo block_corrupt_repl_one = genBlockInfo(5);

    //add a block with a single entry
    assertAdded(queues, block1, 1, 0, 3);

    assertEquals(1, queues.getUnderReplicatedBlockCount());
    assertEquals(1, queues.size());
    assertInLevel(queues, block1, UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY);
    //repeated additions fail
    assertFalse(queues.add(block1, 1, 0, 0, 3));

    //add a second block with two replicas
    assertAdded(queues, block2, 2, 0, 3);
    assertEquals(2, queues.getUnderReplicatedBlockCount());
    assertEquals(2, queues.size());
    assertInLevel(queues, block2, UnderReplicatedBlocks.QUEUE_UNDER_REPLICATED);
    //now try to add a block that is corrupt
    assertAdded(queues, block_corrupt, 0, 0, 3);
    assertEquals(3, queues.size());
    assertEquals(2, queues.getUnderReplicatedBlockCount());
    assertEquals(1, queues.getCorruptBlockSize());
    assertInLevel(queues, block_corrupt,
                  UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);

    //insert a very under-replicated block
    assertAdded(queues, block_very_under_replicated, 4, 0, 25);
    assertInLevel(queues, block_very_under_replicated,
                  UnderReplicatedBlocks.QUEUE_VERY_UNDER_REPLICATED);

    //insert a corrupt block with replication factor 1
    assertAdded(queues, block_corrupt_repl_one, 0, 0, 1);
    assertEquals(2, queues.getCorruptBlockSize());
    assertEquals(1, queues.getCorruptReplOneBlockSize());
    queues.update(block_corrupt_repl_one, 0, 0, 0, 3, 0, 2);
    assertEquals(0, queues.getCorruptReplOneBlockSize());
    queues.update(block_corrupt, 0, 0, 0, 1, 0, -2);
    assertEquals(1, queues.getCorruptReplOneBlockSize());
    queues.update(block_very_under_replicated, 0, 0, 0, 1, -4, -24);
    assertEquals(2, queues.getCorruptReplOneBlockSize());
  }

  @Test
  public void testStripedBlockPriorities() throws Throwable {
    int dataBlkNum = ecPolicy.getNumDataUnits();
    int parityBlkNUm = ecPolicy.getNumParityUnits();
    doTestStripedBlockPriorities(1, parityBlkNUm);
    doTestStripedBlockPriorities(dataBlkNum, parityBlkNUm);
  }

  private void doTestStripedBlockPriorities(int dataBlkNum, int parityBlkNum)
      throws Throwable {
    int groupSize = dataBlkNum + parityBlkNum;
    long numBytes = ecPolicy.getCellSize() * dataBlkNum;
    UnderReplicatedBlocks queues = new UnderReplicatedBlocks();
    int numUR = 0;
    int numCorrupt = 0;

    // add under replicated blocks
    for (int i = 0; dataBlkNum + i < groupSize; i++) {
      BlockInfo block = genStripedBlockInfo(-100 - 100 * i, numBytes);
      assertAdded(queues, block, dataBlkNum + i, 0, groupSize);
      numUR++;
      assertEquals(numUR, queues.getUnderReplicatedBlockCount());
      assertEquals(numUR + numCorrupt, queues.size());
      if (i == 0) {
        assertInLevel(queues, block,
            UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY);
      } else if (i * 3 < parityBlkNum + 1) {
        assertInLevel(queues, block,
            UnderReplicatedBlocks.QUEUE_VERY_UNDER_REPLICATED);
      } else {
        assertInLevel(queues, block,
            UnderReplicatedBlocks.QUEUE_UNDER_REPLICATED);
      }
    }

    // add a corrupted block
    BlockInfo block_corrupt = genStripedBlockInfo(-10, numBytes);
    assertEquals(numCorrupt, queues.getCorruptBlockSize());
    assertAdded(queues, block_corrupt, dataBlkNum - 1, 0, groupSize);
    numCorrupt++;
    assertEquals(numUR + numCorrupt, queues.size());
    assertEquals(numUR, queues.getUnderReplicatedBlockCount());
    assertEquals(numCorrupt, queues.getCorruptBlockSize());
    assertInLevel(queues, block_corrupt,
        UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
  }

  private void assertAdded(UnderReplicatedBlocks queues,
                           BlockInfo block,
                           int curReplicas,
                           int decomissionedReplicas,
                           int expectedReplicas) {
    assertTrue("Failed to add " + block,
               queues.add(block,
                          curReplicas, 0,
                          decomissionedReplicas,
                          expectedReplicas));
  }

  /**
   * Determine whether or not a block is in a level without changing the API.
   * Instead get the per-level iterator and run though it looking for a match.
   * If the block is not found, an assertion is thrown.
   *
   * This is inefficient, but this is only a test case.
   * @param queues queues to scan
   * @param block block to look for
   * @param level level to select
   */
  private void assertInLevel(UnderReplicatedBlocks queues,
                             Block block,
                             int level) {
    final Iterator<BlockInfo> bi = queues.iterator(level);
    while (bi.hasNext()) {
      Block next = bi.next();
      if (block.equals(next)) {
        return;
      }
    }
    fail("Block " + block + " not found in level " + level);
  }
}
