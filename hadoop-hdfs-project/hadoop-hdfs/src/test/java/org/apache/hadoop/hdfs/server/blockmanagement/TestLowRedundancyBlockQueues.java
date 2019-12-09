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

import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Test {@link LowRedundancyBlocks}.
 */
@RunWith(Parameterized.class)
public class TestLowRedundancyBlockQueues {

  private final ErasureCodingPolicy ecPolicy;

  public TestLowRedundancyBlockQueues(ErasureCodingPolicy policy) {
    ecPolicy = policy;
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Collection<Object[]> policies() {
    return StripedFileTestUtil.getECPolicies();
  }

  private BlockInfo genBlockInfo(long id) {
    return new BlockInfoContiguous(new Block(id), (short) 3);
  }

  private BlockInfo genStripedBlockInfo(long id, long numBytes) {
    BlockInfoStriped sblk =  new BlockInfoStriped(new Block(id), ecPolicy);
    sblk.setNumBytes(numBytes);
    return sblk;
  }

  private void verifyBlockStats(LowRedundancyBlocks queues,
      int lowRedundancyReplicaCount, int corruptReplicaCount,
      int corruptReplicationOneCount, int lowRedundancyStripedCount,
      int corruptStripedCount, int highestPriorityReplicatedBlockCount,
      int highestPriorityECBlockCount) {
    assertEquals("Low redundancy replica count incorrect!",
        lowRedundancyReplicaCount, queues.getLowRedundancyBlocks());
    assertEquals("Corrupt replica count incorrect!",
        corruptReplicaCount, queues.getCorruptBlocks());
    assertEquals("Corrupt replica one count incorrect!",
        corruptReplicationOneCount,
        queues.getCorruptReplicationOneBlocks());
    assertEquals("Low redundancy striped blocks count incorrect!",
        lowRedundancyStripedCount, queues.getLowRedundancyECBlockGroups());
    assertEquals("Corrupt striped blocks count incorrect!",
        corruptStripedCount, queues.getCorruptECBlockGroups());
    assertEquals("Low Redundancy count incorrect!",
        lowRedundancyReplicaCount + lowRedundancyStripedCount,
        queues.getLowRedundancyBlockCount());
    assertEquals("LowRedundancyBlocks queue size incorrect!",
        (lowRedundancyReplicaCount + corruptReplicaCount +
        lowRedundancyStripedCount + corruptStripedCount), queues.size());
    assertEquals("Highest priority replicated low redundancy " +
            "blocks count is incorrect!",
        highestPriorityReplicatedBlockCount,
        queues.getHighestPriorityReplicatedBlockCount());
    assertEquals("Highest priority erasure coded low redundancy " +
            "blocks count is incorrect!",
        highestPriorityECBlockCount,
        queues.getHighestPriorityECBlockCount());
  }

  /**
   * Test that adding blocks with different replication counts puts them
   * into different queues.
   * @throws Throwable if something goes wrong
   */
  @Test
  public void testBlockPriorities() throws Throwable {
    LowRedundancyBlocks queues = new LowRedundancyBlocks();
    BlockInfo block1 = genBlockInfo(1);
    BlockInfo block2 = genBlockInfo(2);
    BlockInfo block_very_low_redundancy = genBlockInfo(3);
    BlockInfo block_corrupt = genBlockInfo(4);
    BlockInfo block_corrupt_repl_one = genBlockInfo(5);

    // Add a block with a single entry
    assertAdded(queues, block1, 1, 0, 3);
    assertInLevel(queues, block1, LowRedundancyBlocks.QUEUE_HIGHEST_PRIORITY);
    verifyBlockStats(queues, 1, 0, 0, 0, 0, 1, 0);

    // Repeated additions fail
    assertFalse(queues.add(block1, 1, 0, 0, 3));
    verifyBlockStats(queues, 1, 0, 0, 0, 0, 1, 0);

    // Add a second block with two replicas
    assertAdded(queues, block2, 2, 0, 3);
    assertInLevel(queues, block2, LowRedundancyBlocks.QUEUE_LOW_REDUNDANCY);
    verifyBlockStats(queues, 2, 0, 0, 0, 0, 1, 0);

    // Now try to add a block that is corrupt
    assertAdded(queues, block_corrupt, 0, 0, 3);
    assertInLevel(queues, block_corrupt,
                  LowRedundancyBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
    verifyBlockStats(queues, 2, 1, 0, 0, 0, 1, 0);

    // Insert a very insufficiently redundancy block
    assertAdded(queues, block_very_low_redundancy, 4, 0, 25);
    assertInLevel(queues, block_very_low_redundancy,
                  LowRedundancyBlocks.QUEUE_VERY_LOW_REDUNDANCY);
    verifyBlockStats(queues, 3, 1, 0, 0, 0, 1, 0);

    // Insert a corrupt block with replication factor 1
    assertAdded(queues, block_corrupt_repl_one, 0, 0, 1);
    verifyBlockStats(queues, 3, 2, 1, 0, 0, 1, 0);

    // Bump up the expected count for corrupt replica one block from 1 to 3
    queues.update(block_corrupt_repl_one, 0, 0, 0, 3, 0, 2);
    verifyBlockStats(queues, 3, 2, 0, 0, 0, 1, 0);

    // Reduce the expected replicas to 1
    queues.update(block_corrupt, 0, 0, 0, 1, 0, -2);
    verifyBlockStats(queues, 3, 2, 1, 0, 0, 1, 0);
    queues.update(block_very_low_redundancy, 0, 0, 0, 1, -4, -24);
    verifyBlockStats(queues, 2, 3, 2, 0, 0, 1, 0);

    // Reduce the expected replicas to 1 for block1
    queues.update(block1, 1, 0, 0, 1, 0, 0);
    verifyBlockStats(queues, 2, 3, 2, 0, 0, 0, 0);
  }

  @Test
  public void testRemoveWithWrongPriority() {
    final LowRedundancyBlocks queues = new LowRedundancyBlocks();
    final BlockInfo corruptBlock = genBlockInfo(1);
    assertAdded(queues, corruptBlock, 0, 0, 3);
    assertInLevel(queues, corruptBlock,
        LowRedundancyBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
    verifyBlockStats(queues, 0, 1, 0, 0, 0, 0, 0);

    // Remove with wrong priority
    queues.remove(corruptBlock, LowRedundancyBlocks.QUEUE_LOW_REDUNDANCY);
    // Verify the number of corrupt block is decremented
    verifyBlockStats(queues, 0, 0, 0, 0, 0, 0, 0);
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
    LowRedundancyBlocks queues = new LowRedundancyBlocks();
    int numUR = 0;
    int numCorrupt = 0;

    // add low redundancy blocks
    for (int i = 0; dataBlkNum + i < groupSize; i++) {
      BlockInfo block = genStripedBlockInfo(-100 - 100 * i, numBytes);
      assertAdded(queues, block, dataBlkNum + i, 0, groupSize);
      numUR++;
      assertEquals(numUR, queues.getLowRedundancyBlockCount());
      assertEquals(numUR + numCorrupt, queues.size());
      if (i == 0) {
        assertInLevel(queues, block,
            LowRedundancyBlocks.QUEUE_HIGHEST_PRIORITY);
      } else if (i * 3 < parityBlkNum + 1) {
        assertInLevel(queues, block,
            LowRedundancyBlocks.QUEUE_VERY_LOW_REDUNDANCY);
      } else {
        assertInLevel(queues, block,
            LowRedundancyBlocks.QUEUE_LOW_REDUNDANCY);
      }
      verifyBlockStats(queues, 0, 0, 0, numUR, 0, 0, 1);
    }

    // add a corrupted block
    BlockInfo block_corrupt = genStripedBlockInfo(-10, numBytes);
    assertEquals(numCorrupt, queues.getCorruptBlockSize());
    verifyBlockStats(queues, 0, 0, 0, numUR, numCorrupt, 0, 1);

    assertAdded(queues, block_corrupt, dataBlkNum - 1, 0, groupSize);
    numCorrupt++;
    verifyBlockStats(queues, 0, 0, 0, numUR, numCorrupt, 0, 1);

    assertInLevel(queues, block_corrupt,
        LowRedundancyBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
  }

  private void assertAdded(LowRedundancyBlocks queues,
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
  private void assertInLevel(LowRedundancyBlocks queues,
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
