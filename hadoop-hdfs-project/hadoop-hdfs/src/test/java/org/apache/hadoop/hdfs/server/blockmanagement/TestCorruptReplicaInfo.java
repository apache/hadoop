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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap.Reason;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


/**
 * This test makes sure that 
 *   CorruptReplicasMap::numBlocksWithCorruptReplicas and
 *   CorruptReplicasMap::getCorruptReplicaBlockIds
 *   return the correct values
 */
public class TestCorruptReplicaInfo {
  
  private static final Logger LOG = LoggerFactory.getLogger(
      TestCorruptReplicaInfo.class);
  private final Map<Long, BlockInfo> replicaMap = new HashMap<>();
  private final Map<Long, BlockInfo> stripedBlocksMap = new HashMap<>();

  // Allow easy block creation by block id. Return existing
  // replica block if one with same block id already exists.
  private BlockInfo getReplica(Long blockId) {
    if (!replicaMap.containsKey(blockId)) {
      short replFactor = 3;
      replicaMap.put(blockId,
          new BlockInfoContiguous(new Block(blockId, 0, 0), replFactor));
    }
    return replicaMap.get(blockId);
  }

  private BlockInfo getReplica(int blkId) {
    return getReplica(Long.valueOf(blkId));
  }

  private BlockInfo getStripedBlock(int blkId) {
    Long stripedBlockId = (1L << 63) + blkId;
    assertTrue(BlockIdManager.isStripedBlockID(stripedBlockId));
    if (!stripedBlocksMap.containsKey(stripedBlockId)) {
      stripedBlocksMap.put(stripedBlockId,
          new BlockInfoStriped(new Block(stripedBlockId, 1024, 0),
              StripedFileTestUtil.getDefaultECPolicy()));
    }
    return stripedBlocksMap.get(stripedBlockId);
  }

  private void verifyCorruptBlocksCount(CorruptReplicasMap corruptReplicasMap,
      long expectedReplicaCount, long expectedStripedBlockCount) {
    long totalExpectedCorruptBlocks = expectedReplicaCount +
        expectedStripedBlockCount;
      assertEquals(
              totalExpectedCorruptBlocks, corruptReplicasMap.size(), "Unexpected total corrupt blocks count!");
      assertEquals(
              expectedReplicaCount, corruptReplicasMap.getCorruptBlocks(), "Unexpected replica blocks count!");
      assertEquals(
              expectedStripedBlockCount,
              corruptReplicasMap.getCorruptECBlockGroups(), "Unexpected striped blocks count!");
  }
  
  @Test
  public void testCorruptReplicaInfo()
      throws IOException, InterruptedException {
    CorruptReplicasMap crm = new CorruptReplicasMap();
    BlockIdManager bim = Mockito.mock(BlockIdManager.class);
    when(bim.isLegacyBlock(any(Block.class))).thenReturn(false);
    when(bim.isStripedBlock(any(Block.class))).thenCallRealMethod();
    assertTrue(!bim.isLegacyBlock(new Block(-1)));

      // Make sure initial values are returned correctly
      assertEquals(
              0, crm.size(), "Total number of corrupt blocks must initially be 0!");
      assertEquals(
              0, crm.getCorruptBlocks(), "Number of corrupt replicas must initially be 0!");
      assertEquals(
              0, crm.getCorruptECBlockGroups(), "Number of corrupt striped block groups must initially be 0!");
      assertNull(
              crm.getCorruptBlockIdsForTesting(bim, BlockType.CONTIGUOUS, -1, null), "Param n cannot be less than 0");
      assertNull(
              crm.getCorruptBlockIdsForTesting(bim, BlockType.CONTIGUOUS, 101, null), "Param n cannot be greater than 100");
    long[] l = crm.getCorruptBlockIdsForTesting(
        bim, BlockType.CONTIGUOUS, 0, null);
      assertNotNull(l, "n = 0 must return non-null");
      assertEquals(0, l.length, "n = 0 must return an empty list");

    // Create a list of block ids. A list is used to allow easy
    // validation of the output of getCorruptReplicaBlockIds.
    final int blockCount = 140;
    long[] replicaIds = new long[blockCount];
    long[] stripedIds = new long[blockCount];
    for (int i = 0; i < blockCount; i++) {
      replicaIds[i] = getReplica(i).getBlockId();
      stripedIds[i] = getStripedBlock(i).getBlockId();
    }

    DatanodeDescriptor dn1 = DFSTestUtil.getLocalDatanodeDescriptor();
    DatanodeDescriptor dn2 = DFSTestUtil.getLocalDatanodeDescriptor();

    // Add to corrupt blocks map.
    // Replicas
    addToCorruptReplicasMap(crm, getReplica(0), dn1);
    verifyCorruptBlocksCount(crm, 1, 0);
    addToCorruptReplicasMap(crm, getReplica(1), dn1);
    verifyCorruptBlocksCount(crm, 2, 0);
    addToCorruptReplicasMap(crm, getReplica(1), dn2);
    verifyCorruptBlocksCount(crm, 2, 0);

    // Striped blocks
    addToCorruptReplicasMap(crm, getStripedBlock(0), dn1);
    verifyCorruptBlocksCount(crm, 2, 1);
    addToCorruptReplicasMap(crm, getStripedBlock(1), dn1);
    verifyCorruptBlocksCount(crm, 2, 2);
    addToCorruptReplicasMap(crm, getStripedBlock(1), dn2);
    verifyCorruptBlocksCount(crm, 2, 2);

    // Remove from corrupt blocks map.
    // Replicas
    crm.removeFromCorruptReplicasMap(getReplica(1));
    verifyCorruptBlocksCount(crm, 1, 2);
    crm.removeFromCorruptReplicasMap(getReplica(0));
    verifyCorruptBlocksCount(crm, 0, 2);

    // Striped blocks
    crm.removeFromCorruptReplicasMap(getStripedBlock(1));
    verifyCorruptBlocksCount(crm, 0, 1);
    crm.removeFromCorruptReplicasMap(getStripedBlock(0));
    verifyCorruptBlocksCount(crm, 0, 0);

    for (int blockId = 0; blockId  < blockCount; blockId++) {
      addToCorruptReplicasMap(crm, getReplica(blockId), dn1);
      addToCorruptReplicasMap(crm, getStripedBlock(blockId), dn1);
    }

      assertEquals(
              2 * blockCount, crm.size(), "Number of corrupt blocks not returning correctly");
      assertTrue(
              Arrays.equals(Arrays.copyOfRange(replicaIds, 0, 5),
                      crm.getCorruptBlockIdsForTesting(
                              bim, BlockType.CONTIGUOUS, 5, null)), "First five corrupt replica blocks ids are not right!");
      assertTrue(
              Arrays.equals(Arrays.copyOfRange(stripedIds, 0, 5),
                      crm.getCorruptBlockIdsForTesting(
                              bim, BlockType.STRIPED, 5, null)), "First five corrupt striped blocks ids are not right!");

      assertTrue(
              Arrays.equals(Arrays.copyOfRange(replicaIds, 7, 17),
                      crm.getCorruptBlockIdsForTesting(
                              bim, BlockType.CONTIGUOUS, 10, 7L)), "10 replica blocks after 7 not returned correctly!");
      assertTrue(
              Arrays.equals(Arrays.copyOfRange(stripedIds, 7, 17),
                      crm.getCorruptBlockIdsForTesting(bim, BlockType.STRIPED,
                              10, getStripedBlock(7).getBlockId())), "10 striped blocks after 7 not returned correctly!");
  }
  
  private static void addToCorruptReplicasMap(CorruptReplicasMap crm,
      BlockInfo blk, DatanodeDescriptor dn) {
    crm.addToCorruptReplicasMap(blk, dn, "TEST", Reason.NONE, blk.isStriped());
  }
}
