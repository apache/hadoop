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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;

import com.google.common.base.Preconditions;

/**
 * Utility class for analyzing striped block groups
 */
@InterfaceAudience.Private
public class StripedBlockUtil {

  /**
   * This method parses a striped block group into individual blocks.
   *
   * @param bg The striped block group
   * @param cellSize The size of a striping cell
   * @param dataBlkNum The number of data blocks
   * @return An array containing the blocks in the group
   */
  public static LocatedBlock[] parseStripedBlockGroup(LocatedStripedBlock bg,
      int cellSize, int dataBlkNum, int parityBlkNum) {
    int locatedBGSize = bg.getBlockIndices().length;
    // TODO not considering missing blocks for now, only identify data blocks
    LocatedBlock[] lbs = new LocatedBlock[dataBlkNum + parityBlkNum];
    for (short i = 0; i < locatedBGSize; i++) {
      final int idx = bg.getBlockIndices()[i];
      if (idx < (dataBlkNum + parityBlkNum) && lbs[idx] == null) {
        lbs[idx] = constructInternalBlock(bg, i, cellSize,
            dataBlkNum, idx);
      }
    }
    return lbs;
  }

  /**
   * This method creates an internal block at the given index of a block group
   *
   * @param idxInReturnedLocs The index in the stored locations in the
   *                          {@link LocatedStripedBlock} object
   * @param idxInBlockGroup The logical index in the striped block group
   * @return The constructed internal block
   */
  public static LocatedBlock constructInternalBlock(LocatedStripedBlock bg,
      int idxInReturnedLocs, int cellSize, int dataBlkNum,
      int idxInBlockGroup) {
    final ExtendedBlock blk = new ExtendedBlock(bg.getBlock());
    blk.setBlockId(bg.getBlock().getBlockId() + idxInBlockGroup);
    blk.setNumBytes(getInternalBlockLength(bg.getBlockSize(),
        cellSize, dataBlkNum, idxInBlockGroup));

    return new LocatedBlock(blk,
        new DatanodeInfo[]{bg.getLocations()[idxInReturnedLocs]},
        new String[]{bg.getStorageIDs()[idxInReturnedLocs]},
        new StorageType[]{bg.getStorageTypes()[idxInReturnedLocs]},
        bg.getStartOffset() + idxInBlockGroup * cellSize, bg.isCorrupt(),
        null);
  }

  /**
   * Get the size of an internal block at the given index of a block group
   *
   * @param dataSize Size of the block group only counting data blocks
   * @param cellSize The size of a striping cell
   * @param numDataBlocks The number of data blocks
   * @param i The logical index in the striped block group
   * @return The size of the internal block at the specified index
   */
  public static long getInternalBlockLength(long dataSize,
      int cellSize, int numDataBlocks, int i) {
    Preconditions.checkArgument(dataSize >= 0);
    Preconditions.checkArgument(cellSize > 0);
    Preconditions.checkArgument(numDataBlocks > 0);
    Preconditions.checkArgument(i >= 0);
    // Size of each stripe (only counting data blocks)
    final int stripeSize = cellSize * numDataBlocks;
    // If block group ends at stripe boundary, each internal block has an equal
    // share of the group
    final int lastStripeDataLen = (int)(dataSize % stripeSize);
    if (lastStripeDataLen == 0) {
      return dataSize / numDataBlocks;
    }

    final int numStripes = (int) ((dataSize - 1) / stripeSize + 1);
    return (numStripes - 1L)*cellSize
        + lastCellSize(lastStripeDataLen, cellSize, numDataBlocks, i);
  }
  
  private static int lastCellSize(int size, int cellSize, int numDataBlocks,
      int i) {
    if (i < numDataBlocks) {
      // parity block size (i.e. i >= numDataBlocks) is the same as 
      // the first data block size (i.e. i = 0).
      size -= i*cellSize;
      if (size < 0) {
        size = 0;
      }
    }
    return size > cellSize? cellSize: size;
  }

  /**
   * Given a byte's offset in an internal block, calculate the offset in
   * the block group
   */
  public static long offsetInBlkToOffsetInBG(int cellSize, int dataBlkNum,
      long offsetInBlk, int idxInBlockGroup) {
    int cellIdxInBlk = (int) (offsetInBlk / cellSize);
    return cellIdxInBlk * cellSize * dataBlkNum // n full stripes before offset
        + idxInBlockGroup * cellSize // m full cells before offset
        + offsetInBlk % cellSize; // partial cell
  }

}
