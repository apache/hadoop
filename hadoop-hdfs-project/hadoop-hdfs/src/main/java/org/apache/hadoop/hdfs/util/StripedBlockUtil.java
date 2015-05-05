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

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    final ExtendedBlock blk = constructInternalBlock(
        bg.getBlock(), cellSize, dataBlkNum, idxInBlockGroup);

    return new LocatedBlock(blk,
        new DatanodeInfo[]{bg.getLocations()[idxInReturnedLocs]},
        new String[]{bg.getStorageIDs()[idxInReturnedLocs]},
        new StorageType[]{bg.getStorageTypes()[idxInReturnedLocs]},
        bg.getStartOffset() + idxInBlockGroup * cellSize, bg.isCorrupt(),
        null);
  }

  /**
   * This method creates an internal {@link ExtendedBlock} at the given index
   * of a block group.
   */
  public static ExtendedBlock constructInternalBlock(ExtendedBlock blockGroup,
      int cellSize, int dataBlkNum, int idxInBlockGroup) {
    ExtendedBlock block = new ExtendedBlock(blockGroup);
    block.setBlockId(blockGroup.getBlockId() + idxInBlockGroup);
    block.setNumBytes(getInternalBlockLength(blockGroup.getNumBytes(),
        cellSize, dataBlkNum, idxInBlockGroup));
    return block;
  }
  
  /**
   * This method creates an internal {@link ExtendedBlock} at the given index
   * of a block group, for both data and parity block.
   */
  public static ExtendedBlock constructStripedBlock(ExtendedBlock blockGroup,
      int cellSize, int dataBlkNum, int idxInBlockGroup) {
    ExtendedBlock block = new ExtendedBlock(blockGroup);
    block.setBlockId(blockGroup.getBlockId() + idxInBlockGroup);
    block.setNumBytes(getStripedBlockLength(blockGroup.getNumBytes(), cellSize,
        dataBlkNum, idxInBlockGroup));
    return block;
  }

  /**
   * Returns an internal block length at the given index of a block group,
   * for both data and parity block.
   */
  public static long getStripedBlockLength(long numBytes, int cellSize,
      int dataBlkNum, int idxInBlockGroup) {
    // parity block length is the same as the first striped block length. 
    return StripedBlockUtil.getInternalBlockLength(
        numBytes, cellSize, dataBlkNum, 
        idxInBlockGroup < dataBlkNum ? idxInBlockGroup : 0);
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

  /**
   * This method plans the read portion from each block in the stripe
   * @param dataBlkNum The number of data blocks in the striping group
   * @param cellSize The size of each striping cell
   * @param startInBlk Starting offset in the striped block
   * @param len Length of the read request
   * @param bufOffset  Initial offset in the result buffer
   * @return array of {@link ReadPortion}, each representing the portion of I/O
   *         for an individual block in the group
   */
  @VisibleForTesting
  public static ReadPortion[] planReadPortions(final int dataBlkNum,
      final int cellSize, final long startInBlk, final int len, int bufOffset) {
    ReadPortion[] results = new ReadPortion[dataBlkNum];
    for (int i = 0; i < dataBlkNum; i++) {
      results[i] = new ReadPortion();
    }

    // cellIdxInBlk is the index of the cell in the block
    // E.g., cell_3 is the 2nd cell in blk_0
    int cellIdxInBlk = (int) (startInBlk / (cellSize * dataBlkNum));

    // blkIdxInGroup is the index of the block in the striped block group
    // E.g., blk_2 is the 3rd block in the group
    final int blkIdxInGroup = (int) (startInBlk / cellSize % dataBlkNum);
    results[blkIdxInGroup].setStartOffsetInBlock(cellSize * cellIdxInBlk +
        startInBlk % cellSize);
    boolean crossStripe = false;
    for (int i = 1; i < dataBlkNum; i++) {
      if (blkIdxInGroup + i >= dataBlkNum && !crossStripe) {
        cellIdxInBlk++;
        crossStripe = true;
      }
      results[(blkIdxInGroup + i) % dataBlkNum].setStartOffsetInBlock(
          cellSize * cellIdxInBlk);
    }

    int firstCellLen = Math.min(cellSize - (int) (startInBlk % cellSize), len);
    results[blkIdxInGroup].offsetsInBuf.add(bufOffset);
    results[blkIdxInGroup].lengths.add(firstCellLen);
    results[blkIdxInGroup].addReadLength(firstCellLen);

    int i = (blkIdxInGroup + 1) % dataBlkNum;
    for (int done = firstCellLen; done < len; done += cellSize) {
      ReadPortion rp = results[i];
      rp.offsetsInBuf.add(done + bufOffset);
      final int readLen = Math.min(len - done, cellSize);
      rp.lengths.add(readLen);
      rp.addReadLength(readLen);
      i = (i + 1) % dataBlkNum;
    }
    return results;
  }

  /**
   * Get the next completed striped read task
   *
   * @return {@link StripedReadResult} indicating the status of the read task
   *          succeeded, and the block index of the task. If the method times
   *          out without getting any completed read tasks, -1 is returned as
   *          block index.
   * @throws InterruptedException
   */
  public static StripedReadResult getNextCompletedStripedRead(
      CompletionService<Void> readService, Map<Future<Void>, Integer> futures,
      final long threshold) throws InterruptedException {
    Preconditions.checkArgument(!futures.isEmpty());
    Preconditions.checkArgument(threshold > 0);
    Future<Void> future = null;
    try {
      future = readService.poll(threshold, TimeUnit.MILLISECONDS);
      if (future != null) {
        future.get();
        return new StripedReadResult(futures.remove(future),
            StripedReadResult.SUCCESSFUL);
      } else {
        return new StripedReadResult(StripedReadResult.TIMEOUT);
      }
    } catch (ExecutionException e) {
      return new StripedReadResult(futures.remove(future),
          StripedReadResult.FAILED);
    } catch (CancellationException e) {
      return new StripedReadResult(futures.remove(future),
          StripedReadResult.CANCELLED);
    }
  }

  /**
   * Get the total usage of the striped blocks, which is the total of data
   * blocks and parity blocks
   *
   * @param numDataBlkBytes
   *          Size of the block group only counting data blocks
   * @param dataBlkNum
   *          The number of data blocks
   * @param parityBlkNum
   *          The number of parity blocks
   * @param cellSize
   *          The size of a striping cell
   * @return The total usage of data blocks and parity blocks
   */
  public static long spaceConsumedByStripedBlock(long numDataBlkBytes,
      int dataBlkNum, int parityBlkNum, int cellSize) {
    int parityIndex = dataBlkNum + 1;
    long numParityBlkBytes = getInternalBlockLength(numDataBlkBytes, cellSize,
        dataBlkNum, parityIndex) * parityBlkNum;
    return numDataBlkBytes + numParityBlkBytes;
  }

  /**
   * This class represents the portion of I/O associated with each block in the
   * striped block group.
   */
  public static class ReadPortion {
    /**
     * startOffsetInBlock
     *     |
     *     v
     *     |<-lengths[0]->|<-  lengths[1]  ->|<-lengths[2]->|
     * +------------------+------------------+----------------+
     * |      cell_0      |      cell_3      |     cell_6     |  <- blk_0
     * +------------------+------------------+----------------+
     *   _/                \_______________________
     *  |                                          |
     *  v offsetsInBuf[0]                          v offsetsInBuf[1]
     * +------------------------------------------------------+
     * |  cell_0     |      cell_1 and cell_2      |cell_3 ...|   <- buf
     * |  (partial)  |    (from blk_1 and blk_2)   |          |
     * +------------------------------------------------------+
     */
    private long startOffsetInBlock = 0;
    private int readLength = 0;
    public final List<Integer> offsetsInBuf = new ArrayList<>();
    public final List<Integer> lengths = new ArrayList<>();

    public int[] getOffsets() {
      int[] offsets = new int[offsetsInBuf.size()];
      for (int i = 0; i < offsets.length; i++) {
        offsets[i] = offsetsInBuf.get(i);
      }
      return offsets;
    }

    public int[] getLengths() {
      int[] lens = new int[this.lengths.size()];
      for (int i = 0; i < lens.length; i++) {
        lens[i] = this.lengths.get(i);
      }
      return lens;
    }

    public long getStartOffsetInBlock() {
      return startOffsetInBlock;
    }

    public int getReadLength() {
      return readLength;
    }

    public void setStartOffsetInBlock(long startOffsetInBlock) {
      this.startOffsetInBlock = startOffsetInBlock;
    }

    void addReadLength(int extraLength) {
      this.readLength += extraLength;
    }
  }

  /**
   * This class represents result from a striped read request.
   * If the task was successful or the internal computation failed,
   * an index is also returned.
   */
  public static class StripedReadResult {
    public static final int SUCCESSFUL = 0x01;
    public static final int FAILED = 0x02;
    public static final int TIMEOUT = 0x04;
    public static final int CANCELLED = 0x08;

    public final int index;
    public final int state;

    public StripedReadResult(int state) {
      Preconditions.checkArgument(state == TIMEOUT,
          "Only timeout result should return negative index.");
      this.index = -1;
      this.state = state;
    }

    public StripedReadResult(int index, int state) {
      Preconditions.checkArgument(state != TIMEOUT,
          "Timeout result should return negative index.");
      this.index = index;
      this.state = state;
    }
  }
}
