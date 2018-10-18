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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.DFSStripedOutputStream;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * When accessing a file in striped layout, operations on logical byte ranges
 * in the file need to be mapped to physical byte ranges on block files stored
 * on DataNodes. This utility class facilities this mapping by defining and
 * exposing a number of striping-related concepts. The most basic ones are
 * illustrated in the following diagram. Unless otherwise specified, all
 * range-related calculations are inclusive (the end offset of the previous
 * range should be 1 byte lower than the start offset of the next one).
 */
 /*
 *  | <----  Block Group ----> |   <- Block Group: logical unit composing
 *  |                          |        striped HDFS files.
 *  blk_0      blk_1       blk_2   <- Internal Blocks: each internal block
 *    |          |           |          represents a physically stored local
 *    v          v           v          block file
 * +------+   +------+   +------+
 * |cell_0|   |cell_1|   |cell_2|  <- {@link StripingCell} represents the
 * +------+   +------+   +------+       logical order that a Block Group should
 * |cell_3|   |cell_4|   |cell_5|       be accessed: cell_0, cell_1, ...
 * +------+   +------+   +------+
 * |cell_6|   |cell_7|   |cell_8|
 * +------+   +------+   +------+
 * |cell_9|
 * +------+  <- A cell contains cellSize bytes of data
 */
@InterfaceAudience.Private
public class StripedBlockUtil {

  public static final Logger LOG =
      LoggerFactory.getLogger(StripedBlockUtil.class);

  /**
   * Struct holding the read statistics. This is used when reads are done
   * asynchronously, to allow the async threads return the read stats and let
   * the main reading thread to update the stats. This is important for the
   * ThreadLocal stats for the main reading thread to be correct.
   */
  public static class BlockReadStats {
    private final int bytesRead;
    private final boolean isShortCircuit;
    private final int networkDistance;

    public BlockReadStats(int numBytesRead, boolean shortCircuit,
        int distance) {
      bytesRead = numBytesRead;
      isShortCircuit = shortCircuit;
      networkDistance = distance;
    }

    public int getBytesRead() {
      return bytesRead;
    }

    public boolean isShortCircuit() {
      return isShortCircuit;
    }

    public int getNetworkDistance() {
      return networkDistance;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("bytesRead=").append(bytesRead);
      sb.append(',');
      sb.append("isShortCircuit=").append(isShortCircuit);
      sb.append(',');
      sb.append("networkDistance=").append(networkDistance);
      return sb.toString();
    }
  }

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
    LocatedBlock[] lbs = new LocatedBlock[dataBlkNum + parityBlkNum];
    for (short i = 0; i < locatedBGSize; i++) {
      final int idx = bg.getBlockIndices()[i];
      // for now we do not use redundant replica of an internal block
      if (idx < (dataBlkNum + parityBlkNum) && lbs[idx] == null) {
        lbs[idx] = constructInternalBlock(bg, i, cellSize,
            dataBlkNum, idx);
      }
    }
    return lbs;
  }

  /**
   * This method creates an internal block at the given index of a block group.
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
    final LocatedBlock locatedBlock;
    if (idxInReturnedLocs < bg.getLocations().length) {
      locatedBlock = new LocatedBlock(blk,
          new DatanodeInfo[]{bg.getLocations()[idxInReturnedLocs]},
          new String[]{bg.getStorageIDs()[idxInReturnedLocs]},
          new StorageType[]{bg.getStorageTypes()[idxInReturnedLocs]},
          bg.getStartOffset(), bg.isCorrupt(), null);
    } else {
      locatedBlock = new LocatedBlock(blk, null, null, null,
          bg.getStartOffset(), bg.isCorrupt(), null);
    }
    Token<BlockTokenIdentifier>[] blockTokens = bg.getBlockTokens();
    if (idxInReturnedLocs < blockTokens.length) {
      locatedBlock.setBlockToken(blockTokens[idxInReturnedLocs]);
    }
    return locatedBlock;
  }

  public static ExtendedBlock constructInternalBlock(
      ExtendedBlock blockGroup, ErasureCodingPolicy ecPolicy,
      int idxInBlockGroup) {
    return constructInternalBlock(blockGroup, ecPolicy.getCellSize(),
        ecPolicy.getNumDataUnits(), idxInBlockGroup);
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

  public static long getInternalBlockLength(long dataSize,
                                            ErasureCodingPolicy ecPolicy,
                                            int idxInBlockGroup) {
    return getInternalBlockLength(dataSize, ecPolicy.getCellSize(),
        ecPolicy.getNumDataUnits(), idxInBlockGroup);
  }

  /**
   * Get the size of an internal block at the given index of a block group.
   *
   * @param dataSize Size of the block group only counting data blocks
   * @param cellSize The size of a striping cell
   * @param numDataBlocks The number of data blocks
   * @param idxInBlockGroup The logical index in the striped block group
   * @return The size of the internal block at the specified index
   */
  public static long getInternalBlockLength(long dataSize,
      int cellSize, int numDataBlocks, int idxInBlockGroup) {
    Preconditions.checkArgument(dataSize >= 0);
    Preconditions.checkArgument(cellSize > 0);
    Preconditions.checkArgument(numDataBlocks > 0);
    Preconditions.checkArgument(idxInBlockGroup >= 0);
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
        + lastCellSize(lastStripeDataLen, cellSize,
        numDataBlocks, idxInBlockGroup);
  }

  /**
   * Compute the safe length given the internal block lengths.
   *
   * @param ecPolicy The EC policy used for the block group
   * @param blockLens The lengths of internal blocks
   * @return The safe length
   */
  public static long getSafeLength(ErasureCodingPolicy ecPolicy,
      long[] blockLens) {
    final int cellSize = ecPolicy.getCellSize();
    final int dataBlkNum = ecPolicy.getNumDataUnits();
    Preconditions.checkArgument(blockLens.length >= dataBlkNum);
    final int stripeSize = dataBlkNum * cellSize;
    long[] cpy = Arrays.copyOf(blockLens, blockLens.length);
    Arrays.sort(cpy);
    // full stripe is a stripe has at least dataBlkNum full cells.
    // lastFullStripeIdx is the index of the last full stripe.
    int lastFullStripeIdx =
        (int) (cpy[cpy.length - dataBlkNum] / cellSize);
    return lastFullStripeIdx * stripeSize; // return the safeLength
    // TODO: Include lastFullStripeIdx+1 stripe in safeLength, if there exists
    // such a stripe (and it must be partial).
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
   * the block group.
   */
  public static long offsetInBlkToOffsetInBG(int cellSize, int dataBlkNum,
      long offsetInBlk, int idxInBlockGroup) {
    int cellIdxInBlk = (int) (offsetInBlk / cellSize);
    return cellIdxInBlk * cellSize * dataBlkNum // n full stripes before offset
        + idxInBlockGroup * cellSize // m full cells before offset
        + offsetInBlk % cellSize; // partial cell
  }

  /**
   * Get the next completed striped read task.
   *
   * @return {@link StripingChunkReadResult} indicating the status of the read
   *          task succeeded, and the block index of the task. If the method
   *          times out without getting any completed read tasks, -1 is
   *          returned as block index.
   * @throws InterruptedException
   */
  public static StripingChunkReadResult getNextCompletedStripedRead(
      CompletionService<BlockReadStats> readService,
      Map<Future<BlockReadStats>, Integer> futures,
      final long timeoutMillis) throws InterruptedException {
    Preconditions.checkArgument(!futures.isEmpty());
    Future<BlockReadStats> future = null;
    try {
      if (timeoutMillis > 0) {
        future = readService.poll(timeoutMillis, TimeUnit.MILLISECONDS);
      } else {
        future = readService.take();
      }
      if (future != null) {
        final BlockReadStats stats = future.get();
        return new StripingChunkReadResult(futures.remove(future),
            StripingChunkReadResult.SUCCESSFUL, stats);
      } else {
        return new StripingChunkReadResult(StripingChunkReadResult.TIMEOUT);
      }
    } catch (ExecutionException e) {
      LOG.debug("Exception during striped read task", e);
      return new StripingChunkReadResult(futures.remove(future),
          StripingChunkReadResult.FAILED);
    } catch (CancellationException e) {
      LOG.debug("Exception during striped read task", e);
      return new StripingChunkReadResult(futures.remove(future),
          StripingChunkReadResult.CANCELLED);
    }
  }

  /**
   * Get the total usage of the striped blocks, which is the total of data
   * blocks and parity blocks.
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
   * Similar functionality with {@link #divideByteRangeIntoStripes}, but is used
   * by stateful read and uses ByteBuffer as reading target buffer. Besides the
   * read range is within a single stripe thus the calculation logic is simpler.
   */
  public static AlignedStripe[] divideOneStripe(ErasureCodingPolicy ecPolicy,
      int cellSize, LocatedStripedBlock blockGroup, long rangeStartInBlockGroup,
      long rangeEndInBlockGroup, ByteBuffer buf) {
    final int dataBlkNum = ecPolicy.getNumDataUnits();
    // Step 1: map the byte range to StripingCells
    StripingCell[] cells = getStripingCellsOfByteRange(ecPolicy, cellSize,
        blockGroup, rangeStartInBlockGroup, rangeEndInBlockGroup);

    // Step 2: get the unmerged ranges on each internal block
    VerticalRange[] ranges = getRangesForInternalBlocks(ecPolicy, cellSize,
        cells);

    // Step 3: merge into stripes
    AlignedStripe[] stripes = mergeRangesForInternalBlocks(ecPolicy, ranges);

    // Step 4: calculate each chunk's position in destination buffer. Since the
    // whole read range is within a single stripe, the logic is simpler here.
    int bufOffset =
        (int) (rangeStartInBlockGroup % ((long) cellSize * dataBlkNum));
    for (StripingCell cell : cells) {
      long cellStart = cell.idxInInternalBlk * cellSize + cell.offset;
      long cellEnd = cellStart + cell.size - 1;
      for (AlignedStripe s : stripes) {
        long stripeEnd = s.getOffsetInBlock() + s.getSpanInBlock() - 1;
        long overlapStart = Math.max(cellStart, s.getOffsetInBlock());
        long overlapEnd = Math.min(cellEnd, stripeEnd);
        int overLapLen = (int) (overlapEnd - overlapStart + 1);
        if (overLapLen > 0) {
          Preconditions.checkState(s.chunks[cell.idxInStripe] == null);
          final int pos = (int) (bufOffset + overlapStart - cellStart);
          buf.position(pos);
          buf.limit(pos + overLapLen);
          s.chunks[cell.idxInStripe] = new StripingChunk(buf.slice());
        }
      }
      bufOffset += cell.size;
    }

    // Step 5: prepare ALLZERO blocks
    prepareAllZeroChunks(blockGroup, stripes, cellSize, dataBlkNum);
    return stripes;
  }

  /**
   * This method divides a requested byte range into an array of inclusive
   * {@link AlignedStripe}.
   * @param ecPolicy The codec policy for the file, which carries the numbers
   *                 of data / parity blocks
   * @param cellSize Cell size of stripe
   * @param blockGroup The striped block group
   * @param rangeStartInBlockGroup The byte range's start offset in block group
   * @param rangeEndInBlockGroup The byte range's end offset in block group
   * @param buf Destination buffer of the read operation for the byte range
   *
   * At most 5 stripes will be generated from each logical range, as
   * demonstrated in the header of {@link AlignedStripe}.
   */
  public static AlignedStripe[] divideByteRangeIntoStripes(
      ErasureCodingPolicy ecPolicy,
      int cellSize, LocatedStripedBlock blockGroup,
      long rangeStartInBlockGroup, long rangeEndInBlockGroup, ByteBuffer buf) {

    // Step 0: analyze range and calculate basic parameters
    final int dataBlkNum = ecPolicy.getNumDataUnits();

    // Step 1: map the byte range to StripingCells
    StripingCell[] cells = getStripingCellsOfByteRange(ecPolicy, cellSize,
        blockGroup, rangeStartInBlockGroup, rangeEndInBlockGroup);

    // Step 2: get the unmerged ranges on each internal block
    VerticalRange[] ranges = getRangesForInternalBlocks(ecPolicy, cellSize,
        cells);

    // Step 3: merge into at most 5 stripes
    AlignedStripe[] stripes = mergeRangesForInternalBlocks(ecPolicy, ranges);

    // Step 4: calculate each chunk's position in destination buffer
    calcualteChunkPositionsInBuf(cellSize, stripes, cells, buf);

    // Step 5: prepare ALLZERO blocks
    prepareAllZeroChunks(blockGroup, stripes, cellSize, dataBlkNum);

    return stripes;
  }

  /**
   * Map the logical byte range to a set of inclusive {@link StripingCell}
   * instances, each representing the overlap of the byte range to a cell
   * used by {@link DFSStripedOutputStream} in encoding.
   */
  @VisibleForTesting
  private static StripingCell[] getStripingCellsOfByteRange(
      ErasureCodingPolicy ecPolicy,
      int cellSize, LocatedStripedBlock blockGroup,
      long rangeStartInBlockGroup, long rangeEndInBlockGroup) {
    Preconditions.checkArgument(
        rangeStartInBlockGroup <= rangeEndInBlockGroup &&
            rangeEndInBlockGroup < blockGroup.getBlockSize(),
        "start=%s end=%s blockSize=%s", rangeStartInBlockGroup,
        rangeEndInBlockGroup, blockGroup.getBlockSize());
    long len = rangeEndInBlockGroup - rangeStartInBlockGroup + 1;
    int firstCellIdxInBG = (int) (rangeStartInBlockGroup / cellSize);
    int lastCellIdxInBG = (int) (rangeEndInBlockGroup / cellSize);
    int numCells = lastCellIdxInBG - firstCellIdxInBG + 1;
    StripingCell[] cells = new StripingCell[numCells];

    final int firstCellOffset = (int) (rangeStartInBlockGroup % cellSize);
    final int firstCellSize =
        (int) Math.min(cellSize - (rangeStartInBlockGroup % cellSize), len);
    cells[0] = new StripingCell(ecPolicy, firstCellSize, firstCellIdxInBG,
        firstCellOffset);
    if (lastCellIdxInBG != firstCellIdxInBG) {
      final int lastCellSize = (int) (rangeEndInBlockGroup % cellSize) + 1;
      cells[numCells - 1] = new StripingCell(ecPolicy, lastCellSize,
          lastCellIdxInBG, 0);
    }

    for (int i = 1; i < numCells - 1; i++) {
      cells[i] = new StripingCell(ecPolicy, cellSize, i + firstCellIdxInBG, 0);
    }

    return cells;
  }

  /**
   * Given a logical byte range, mapped to each {@link StripingCell}, calculate
   * the physical byte range (inclusive) on each stored internal block.
   */
  @VisibleForTesting
  private static VerticalRange[] getRangesForInternalBlocks(
      ErasureCodingPolicy ecPolicy,
      int cellSize, StripingCell[] cells) {
    int dataBlkNum = ecPolicy.getNumDataUnits();
    int parityBlkNum = ecPolicy.getNumParityUnits();

    VerticalRange[] ranges = new VerticalRange[dataBlkNum + parityBlkNum];

    long earliestStart = Long.MAX_VALUE;
    long latestEnd = -1;
    for (StripingCell cell : cells) {
      // iterate through all cells and update the list of StripeRanges
      if (ranges[cell.idxInStripe] == null) {
        ranges[cell.idxInStripe] = new VerticalRange(
            cell.idxInInternalBlk * cellSize + cell.offset, cell.size);
      } else {
        ranges[cell.idxInStripe].spanInBlock += cell.size;
      }
      VerticalRange range = ranges[cell.idxInStripe];
      if (range.offsetInBlock < earliestStart) {
        earliestStart = range.offsetInBlock;
      }
      if (range.offsetInBlock + range.spanInBlock - 1 > latestEnd) {
        latestEnd = range.offsetInBlock + range.spanInBlock - 1;
      }
    }

    // Each parity block should be fetched at maximum range of all data blocks
    for (int i = dataBlkNum; i < dataBlkNum + parityBlkNum; i++) {
      ranges[i] = new VerticalRange(earliestStart,
          latestEnd - earliestStart + 1);
    }

    return ranges;
  }

  /**
   * Merge byte ranges on each internal block into a set of inclusive
   * {@link AlignedStripe} instances.
   */
  private static AlignedStripe[] mergeRangesForInternalBlocks(
      ErasureCodingPolicy ecPolicy, VerticalRange[] ranges) {
    int dataBlkNum = ecPolicy.getNumDataUnits();
    int parityBlkNum = ecPolicy.getNumParityUnits();
    List<AlignedStripe> stripes = new ArrayList<>();
    SortedSet<Long> stripePoints = new TreeSet<>();
    for (VerticalRange r : ranges) {
      if (r != null) {
        stripePoints.add(r.offsetInBlock);
        stripePoints.add(r.offsetInBlock + r.spanInBlock);
      }
    }

    long prev = -1;
    for (long point : stripePoints) {
      if (prev >= 0) {
        stripes.add(new AlignedStripe(prev, point - prev,
            dataBlkNum + parityBlkNum));
      }
      prev = point;
    }
    return stripes.toArray(new AlignedStripe[stripes.size()]);
  }

  /**
   * Cell indexing convention defined in {@link StripingCell}.
   */
  private static void calcualteChunkPositionsInBuf(int cellSize,
      AlignedStripe[] stripes, StripingCell[] cells, ByteBuffer buf) {
    /*
     *     | <--------------- AlignedStripe --------------->|
     *
     *     |<- length_0 ->|<--  length_1  -->|<- length_2 ->|
     * +------------------+------------------+----------------+
     * |    cell_0_0_0    |    cell_3_1_0    |   cell_6_2_0   |  <- blk_0
     * +------------------+------------------+----------------+
     *   _/                \_______________________
     *  |                                          |
     *  v offset_0                                 v offset_1
     * +----------------------------------------------------------+
     * |  cell_0_0_0 |  cell_1_0_1 and cell_2_0_2  |cell_3_1_0 ...|   <- buf
     * |  (partial)  |    (from blk_1 and blk_2)   |              |
     * +----------------------------------------------------------+
     */
    int done = 0;
    for (StripingCell cell : cells) {
      long cellStart = cell.idxInInternalBlk * cellSize + cell.offset;
      long cellEnd = cellStart + cell.size - 1;
      StripingChunk chunk;
      for (AlignedStripe s : stripes) {
        long stripeEnd = s.getOffsetInBlock() + s.getSpanInBlock() - 1;
        long overlapStart = Math.max(cellStart, s.getOffsetInBlock());
        long overlapEnd = Math.min(cellEnd, stripeEnd);
        int overLapLen = (int) (overlapEnd - overlapStart + 1);
        if (overLapLen <= 0) {
          continue;
        }
        chunk = s.chunks[cell.idxInStripe];
        if (chunk == null) {
          chunk = new StripingChunk();
          s.chunks[cell.idxInStripe] = chunk;
        }
        chunk.getChunkBuffer().addSlice(buf,
            (int) (done + overlapStart - cellStart), overLapLen);
      }
      done += cell.size;
    }
  }

  /**
   * If a {@link StripingChunk} maps to a byte range beyond an internal block's
   * size, the chunk should be treated as zero bytes in decoding.
   */
  private static void prepareAllZeroChunks(LocatedStripedBlock blockGroup,
      AlignedStripe[] stripes, int cellSize, int dataBlkNum) {
    for (AlignedStripe s : stripes) {
      for (int i = 0; i < dataBlkNum; i++) {
        long internalBlkLen = getInternalBlockLength(blockGroup.getBlockSize(),
            cellSize, dataBlkNum, i);
        if (internalBlkLen <= s.getOffsetInBlock()) {
          Preconditions.checkState(s.chunks[i] == null);
          s.chunks[i] = new StripingChunk(StripingChunk.ALLZERO);
        }
      }
    }
  }

  /**
   * Cell is the unit of encoding used in {@link DFSStripedOutputStream}. This
   * size impacts how a logical offset in the file or block group translates
   * to physical byte offset in a stored internal block. The StripingCell util
   * class facilitates this calculation. Each StripingCell is inclusive with
   * its start and end offsets -- e.g., the end logical offset of cell_0_0_0
   * should be 1 byte lower than the start logical offset of cell_1_0_1.
   *
   * A StripingCell is a special instance of {@link StripingChunk} whose offset
   * and size align with the cell used when writing data.
   * TODO: consider parity cells
   */
  /*  | <------- Striped Block Group -------> |
   *    blk_0          blk_1          blk_2
   *      |              |              |
   *      v              v              v
   * +----------+   +----------+   +----------+
   * |cell_0_0_0|   |cell_1_0_1|   |cell_2_0_2|
   * +----------+   +----------+   +----------+
   * |cell_3_1_0|   |cell_4_1_1|   |cell_5_1_2| <- {@link #idxInBlkGroup} = 5
   * +----------+   +----------+   +----------+    {@link #idxInInternalBlk} = 1
   *                                               {@link #idxInStripe} = 2
   */
  @VisibleForTesting
  public static class StripingCell {
    final ErasureCodingPolicy ecPolicy;
    /** Logical order in a block group, used when doing I/O to a block group. */
    private final long idxInBlkGroup;
    private final long idxInInternalBlk;
    private final int idxInStripe;
    /**
     * When a logical byte range is mapped to a set of cells, it might
     * partially overlap with the first and last cells. This field and the
     * {@link #size} variable represent the start offset and size of the
     * overlap.
     */
    private final long offset;
    private final int size;

    StripingCell(ErasureCodingPolicy ecPolicy, int cellSize, long idxInBlkGroup,
        long offset) {
      this.ecPolicy = ecPolicy;
      this.idxInBlkGroup = idxInBlkGroup;
      this.idxInInternalBlk = idxInBlkGroup / ecPolicy.getNumDataUnits();
      this.idxInStripe = (int)(idxInBlkGroup -
          this.idxInInternalBlk * ecPolicy.getNumDataUnits());
      this.offset = offset;
      this.size = cellSize;
    }

    int getIdxInStripe() {
      return idxInStripe;
    }

    @Override
    public String toString() {
      return String.format("StripingCell(idxInBlkGroup=%d, " +
          "idxInInternalBlk=%d, idxInStrip=%d, offset=%d, size=%d)",
          idxInBlkGroup, idxInInternalBlk, idxInStripe, offset, size);
    }
  }

  /**
   * Given a requested byte range on a striped block group, an AlignedStripe
   * represents an inclusive {@link VerticalRange} that is aligned with both
   * the byte range and boundaries of all internal blocks. As illustrated in
   * the diagram, any given byte range on a block group leads to 1~5
   * AlignedStripe's.
   *
   * An AlignedStripe is the basic unit of reading from a striped block group,
   * because within the AlignedStripe, all internal blocks can be processed in
   * a uniform manner.
   *
   * The coverage of an AlignedStripe on an internal block is represented as a
   * {@link StripingChunk}.
   *
   * To simplify the logic of reading a logical byte range from a block group,
   * a StripingChunk is either completely in the requested byte range or
   * completely outside the requested byte range.
   */
  /*
   * |<-------- Striped Block Group -------->|
   * blk_0   blk_1   blk_2      blk_3   blk_4
   *                 +----+  |  +----+  +----+
   *                 |full|  |  |    |  |    | <- AlignedStripe0:
   *         +----+  |~~~~|  |  |~~~~|  |~~~~|      1st cell is partial
   *         |part|  |    |  |  |    |  |    | <- AlignedStripe1: byte range
   * +----+  +----+  +----+  |  |~~~~|  |~~~~|      doesn't start at 1st block
   * |full|  |full|  |full|  |  |    |  |    |
   * |cell|  |cell|  |cell|  |  |    |  |    | <- AlignedStripe2 (full stripe)
   * |    |  |    |  |    |  |  |    |  |    |
   * +----+  +----+  +----+  |  |~~~~|  |~~~~|
   * |full|  |part|          |  |    |  |    | <- AlignedStripe3: byte range
   * |~~~~|  +----+          |  |~~~~|  |~~~~|      doesn't end at last block
   * |    |                  |  |    |  |    | <- AlignedStripe4:
   * +----+                  |  +----+  +----+      last cell is partial
   *                         |
   * <---- data blocks ----> | <--- parity -->
   */
  public static class AlignedStripe {
    public VerticalRange range;
    /** status of each chunk in the stripe. */
    public final StripingChunk[] chunks;
    public int fetchedChunksNum = 0;
    public int missingChunksNum = 0;

    public AlignedStripe(long offsetInBlock, long length, int width) {
      Preconditions.checkArgument(offsetInBlock >= 0 && length >= 0,
          "OffsetInBlock(%s) and length(%s) must be non-negative",
          offsetInBlock, length);
      this.range = new VerticalRange(offsetInBlock, length);
      this.chunks = new StripingChunk[width];
    }

    public boolean include(long pos) {
      return range.include(pos);
    }

    public long getOffsetInBlock() {
      return range.offsetInBlock;
    }

    public long getSpanInBlock() {
      return range.spanInBlock;
    }

    @Override
    public String toString() {
      return "AlignedStripe(Offset=" + range.offsetInBlock + ", length=" +
          range.spanInBlock + ", fetchedChunksNum=" + fetchedChunksNum +
          ", missingChunksNum=" + missingChunksNum + ")";
    }
  }

  /**
   * A simple utility class representing an arbitrary vertical inclusive range
   * starting at {@link #offsetInBlock} and lasting for {@link #spanInBlock}
   * bytes in an internal block. Note that VerticalRange doesn't necessarily
   * align with {@link StripingCell}.
   */
  /*
   * |<- Striped Block Group ->|
   *  blk_0
   *    |
   *    v
   * +-----+
   * |~~~~~| <-- {@link #offsetInBlock}
   * |     |  ^
   * |     |  |
   * |     |  | {@link #spanInBlock}
   * |     |  v
   * |~~~~~| ---
   * |     |
   * +-----+
   */
  public static class VerticalRange {
    /** start offset in the block group (inclusive). */
    public long offsetInBlock;
    /** length of the stripe range. */
    public long spanInBlock;

    public VerticalRange(long offsetInBlock, long length) {
      Preconditions.checkArgument(offsetInBlock >= 0 && length >= 0,
          "OffsetInBlock(%s) and length(%s) must be non-negative",
          offsetInBlock, length);
      this.offsetInBlock = offsetInBlock;
      this.spanInBlock = length;
    }

    /** whether a position is in the range. */
    public boolean include(long pos) {
      return pos >= offsetInBlock && pos < offsetInBlock + spanInBlock;
    }

    @Override
    public String toString() {
      return String.format("VerticalRange(offsetInBlock=%d, spanInBlock=%d)",
          this.offsetInBlock, this.spanInBlock);
    }
  }

  /**
   * Indicates the coverage of an {@link AlignedStripe} on an internal block,
   * and the state of the chunk in the context of the read request.
   */
  /* |<---------------- Striped Block Group --------------->|
   *   blk_0        blk_1        blk_2          blk_3   blk_4
   *                           +---------+  |  +----+  +----+
   *     null         null     |REQUESTED|  |  |null|  |null| <- AlignedStripe0
   *              +---------+  |---------|  |  |----|  |----|
   *     null     |REQUESTED|  |REQUESTED|  |  |null|  |null| <- AlignedStripe1
   * +---------+  +---------+  +---------+  |  +----+  +----+
   * |REQUESTED|  |REQUESTED|    ALLZERO    |  |null|  |null| <- AlignedStripe2
   * +---------+  +---------+               |  +----+  +----+
   * <----------- data blocks ------------> | <--- parity -->
   */
  public static class StripingChunk {
    /** Chunk has been successfully fetched */
    public static final int FETCHED = 0x01;
    /** Chunk has encountered failed when being fetched */
    public static final int MISSING = 0x02;
    /** Chunk being fetched (fetching task is in-flight) */
    public static final int PENDING = 0x04;
    /**
     * Chunk is requested either by application or for decoding, need to
     * schedule read task
     */
    public static final int REQUESTED = 0X08;
    /**
     * Internal block is short and has no overlap with chunk. Chunk considered
     * all-zero bytes in codec calculations.
     */
    public static final int ALLZERO = 0X0f;

    /**
     * If a chunk is completely in requested range, the state transition is:
     * REQUESTED (when AlignedStripe created) -&gt; PENDING -&gt;
     * {FETCHED | MISSING}
     * If a chunk is completely outside requested range (including parity
     * chunks), state transition is:
     * null (AlignedStripe created) -&gt;REQUESTED (upon failure) -&gt;
     * PENDING ...
     */
    public int state = REQUESTED;

    private final ChunkByteBuffer chunkBuffer;
    private final ByteBuffer byteBuffer;

    public StripingChunk() {
      this.chunkBuffer = new ChunkByteBuffer();
      byteBuffer = null;
    }

    public StripingChunk(ByteBuffer buf) {
      this.chunkBuffer = null;
      this.byteBuffer = buf;
    }

    public StripingChunk(int state) {
      this.chunkBuffer = null;
      this.byteBuffer = null;
      this.state = state;
    }

    public boolean useByteBuffer(){
      return byteBuffer != null;
    }

    public boolean useChunkBuffer() {
      return chunkBuffer != null;
    }

    public ByteBuffer getByteBuffer() {
      assert byteBuffer != null;
      return byteBuffer;
    }

    public ChunkByteBuffer getChunkBuffer() {
      assert chunkBuffer != null;
      return chunkBuffer;
    }
  }

  /**
   * A utility to manage ByteBuffer slices for a reader.
   */
  public static class ChunkByteBuffer {
    private final List<ByteBuffer> slices;

    ChunkByteBuffer() {
      this.slices = new ArrayList<>();
    }

    public void addSlice(ByteBuffer buffer, int offset, int len) {
      ByteBuffer tmp = buffer.duplicate();
      tmp.position(buffer.position() + offset);
      tmp.limit(buffer.position() + offset + len);
      slices.add(tmp.slice());
    }

    public ByteBuffer getSlice(int i) {
      return slices.get(i);
    }

    public List<ByteBuffer> getSlices() {
      return slices;
    }

    /**
     *  Note: target will be ready-to-read state after the call.
     */
    public void copyTo(ByteBuffer target) {
      for (ByteBuffer slice : slices) {
        slice.flip();
        target.put(slice);
      }
      target.flip();
    }

    public void copyFrom(ByteBuffer src) {
      ByteBuffer tmp;
      int len;
      for (ByteBuffer slice : slices) {
        len = slice.remaining();
        tmp = src.duplicate();
        tmp.limit(tmp.position() + len);
        slice.put(tmp);
        src.position(src.position() + len);
      }
    }
  }

  /**
   * This class represents result from a striped read request.
   * If the task was successful or the internal computation failed,
   * an index is also returned.
   */
  public static class StripingChunkReadResult {
    public static final int SUCCESSFUL = 0x01;
    public static final int FAILED = 0x02;
    public static final int TIMEOUT = 0x04;
    public static final int CANCELLED = 0x08;

    public final int index;
    public final int state;
    private final BlockReadStats readStats;

    public StripingChunkReadResult(int state) {
      Preconditions.checkArgument(state == TIMEOUT,
          "Only timeout result should return negative index.");
      this.index = -1;
      this.state = state;
      this.readStats = null;
    }

    public StripingChunkReadResult(int index, int state) {
      this(index, state, null);
    }

    public StripingChunkReadResult(int index, int state, BlockReadStats stats) {
      Preconditions.checkArgument(state != TIMEOUT,
          "Timeout result should return negative index.");
      this.index = index;
      this.state = state;
      this.readStats = stats;
    }

    public BlockReadStats getReadStats() {
      return readStats;
    }

    @Override
    public String toString() {
      return "(index=" + index + ", state =" + state + ", readStats ="
          + readStats + ")";
    }
  }

  /** Used to indicate the buffered data's range in the block group. */
  public static class StripeRange {
    /** start offset in the block group (inclusive). */
    final long offsetInBlock;
    /** length of the stripe range. */
    final long length;

    public StripeRange(long offsetInBlock, long length) {
      Preconditions.checkArgument(offsetInBlock >= 0 && length >= 0,
          "Offset(%s) and length(%s) must be non-negative", offsetInBlock,
          length);
      this.offsetInBlock = offsetInBlock;
      this.length = length;
    }

    public boolean include(long pos) {
      return pos >= offsetInBlock && pos < offsetInBlock + length;
    }

    public long getLength() {
      return length;
    }

    @Override
    public String toString() {
      return String.format("StripeRange(offsetInBlock=%d, length=%d)",
          offsetInBlock, length);
    }
  }

  /**
   * Check if the information such as IDs and generation stamps in block-i
   * match the block group.
   */
  public static void checkBlocks(ExtendedBlock blockGroup,
      int i, ExtendedBlock blocki) throws IOException {
    if (!blocki.getBlockPoolId().equals(blockGroup.getBlockPoolId())) {
      throw new IOException("Block pool IDs mismatched: block" + i + "="
          + blocki + ", expected block group=" + blockGroup);
    }
    if (blocki.getBlockId() - i != blockGroup.getBlockId()) {
      throw new IOException("Block IDs mismatched: block" + i + "="
          + blocki + ", expected block group=" + blockGroup);
    }
    if (blocki.getGenerationStamp() != blockGroup.getGenerationStamp()) {
      throw new IOException("Generation stamps mismatched: block" + i + "="
          + blocki + ", expected block group=" + blockGroup);
    }
  }

  public static int getBlockIndex(Block reportedBlock) {
    long BLOCK_GROUP_INDEX_MASK = 15;
    return (int) (reportedBlock.getBlockId() &
        BLOCK_GROUP_INDEX_MASK);
  }
}
