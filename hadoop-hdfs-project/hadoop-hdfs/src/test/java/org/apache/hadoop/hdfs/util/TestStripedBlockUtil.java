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

package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.parseStripedBlockGroup;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.getInternalBlockLength;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestStripedBlockUtil {
  private final short DATA_BLK_NUM = HdfsConstants.NUM_DATA_BLOCKS;
  private final short PARITY_BLK_NUM = HdfsConstants.NUM_PARITY_BLOCKS;
  private final short BLK_GROUP_SIZE = DATA_BLK_NUM + PARITY_BLK_NUM;
  private final int CELLSIZE = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;

  private LocatedStripedBlock createDummyLocatedBlock() {
    final long blockGroupID = -1048576;
    DatanodeInfo[] locs = new DatanodeInfo[BLK_GROUP_SIZE];
    String[] storageIDs = new String[BLK_GROUP_SIZE];
    StorageType[] storageTypes = new StorageType[BLK_GROUP_SIZE];
    int[] indices = new int[BLK_GROUP_SIZE];
    for (int i = 0; i < BLK_GROUP_SIZE; i++) {
      indices[i] = (i + 2) % DATA_BLK_NUM;
      // Location port always equal to logical index of a block,
      // for easier verification
      locs[i] = DFSTestUtil.getLocalDatanodeInfo(indices[i]);
      storageIDs[i] = locs[i].getDatanodeUuid();
      storageTypes[i] = StorageType.DISK;
    }
    return new LocatedStripedBlock(new ExtendedBlock("pool", blockGroupID),
        locs, storageIDs, storageTypes, indices, 0, false, null);
  }

  @Test
  public void testParseDummyStripedBlock() {
    LocatedStripedBlock lsb = createDummyLocatedBlock();
    LocatedBlock[] blocks = parseStripedBlockGroup(
        lsb, CELLSIZE, DATA_BLK_NUM, PARITY_BLK_NUM);
    assertEquals(DATA_BLK_NUM + PARITY_BLK_NUM, blocks.length);
    for (int i = 0; i < DATA_BLK_NUM; i++) {
      assertFalse(blocks[i].isStriped());
      assertEquals(i,
          BlockIdManager.getBlockIndex(blocks[i].getBlock().getLocalBlock()));
      assertEquals(i * CELLSIZE, blocks[i].getStartOffset());
      assertEquals(1, blocks[i].getLocations().length);
      assertEquals(i, blocks[i].getLocations()[0].getIpcPort());
      assertEquals(i, blocks[i].getLocations()[0].getXferPort());
    }
  }

  private void verifyInternalBlocks (long numBytesInGroup, long[] expected) {
    for (int i = 1; i < BLK_GROUP_SIZE; i++) {
      assertEquals(expected[i],
          getInternalBlockLength(numBytesInGroup, CELLSIZE, DATA_BLK_NUM, i));
    }
  }

  @Test
  public void testGetInternalBlockLength () {
    // A small delta that is smaller than a cell
    final int delta = 10;
    assert delta < CELLSIZE;

    // Block group is smaller than a cell
    verifyInternalBlocks(CELLSIZE - delta,
        new long[] {CELLSIZE - delta, 0, 0, 0, 0, 0,
            CELLSIZE - delta, CELLSIZE - delta, CELLSIZE - delta});

    // Block group is exactly as large as a cell
    verifyInternalBlocks(CELLSIZE,
        new long[] {CELLSIZE, 0, 0, 0, 0, 0,
            CELLSIZE, CELLSIZE, CELLSIZE});

    // Block group is a little larger than a cell
    verifyInternalBlocks(CELLSIZE + delta,
        new long[] {CELLSIZE, delta, 0, 0, 0, 0,
            CELLSIZE, CELLSIZE, CELLSIZE});

    // Block group contains multiple stripes and ends at stripe boundary
    verifyInternalBlocks(2 * DATA_BLK_NUM * CELLSIZE,
        new long[] {2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE,
            2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE,
            2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE});

    // Block group contains multiple stripes and ends at cell boundary
    // (not ending at stripe boundary)
    verifyInternalBlocks(2 * DATA_BLK_NUM * CELLSIZE + CELLSIZE,
        new long[] {3 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE,
            2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE,
            3 * CELLSIZE, 3 * CELLSIZE, 3 * CELLSIZE});

    // Block group contains multiple stripes and doesn't end at cell boundary
    verifyInternalBlocks(2 * DATA_BLK_NUM * CELLSIZE - delta,
        new long[] {2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE,
            2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE - delta,
            2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE});
  }

}
