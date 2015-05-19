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
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSStripedOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawDecoder;

import java.util.*;
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
 *
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
   * Get the next completed striped read task
   *
   * @return {@link StripingChunkReadResult} indicating the status of the read task
   *          succeeded, and the block index of the task. If the method times
   *          out without getting any completed read tasks, -1 is returned as
   *          block index.
   * @throws InterruptedException
   */
  public static StripingChunkReadResult getNextCompletedStripedRead(
      CompletionService<Void> readService, Map<Future<Void>, Integer> futures,
      final long threshold) throws InterruptedException {
    Preconditions.checkArgument(!futures.isEmpty());
    Future<Void> future = null;
    try {
      if (threshold > 0) {
        future = readService.poll(threshold, TimeUnit.MILLISECONDS);
      } else {
        future = readService.take();
      }
      if (future != null) {
        future.get();
        return new StripingChunkReadResult(futures.remove(future),
            StripingChunkReadResult.SUCCESSFUL);
      } else {
        return new StripingChunkReadResult(StripingChunkReadResult.TIMEOUT);
      }
    } catch (ExecutionException e) {
      DFSClient.LOG.error("ExecutionException " + e);
      return new StripingChunkReadResult(futures.remove(future),
          StripingChunkReadResult.FAILED);
    } catch (CancellationException e) {
      return new StripingChunkReadResult(futures.remove(future),
          StripingChunkReadResult.CANCELLED);
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
   * Initialize the decoding input buffers based on the chunk states in an
   * AlignedStripe
   */
  public static byte[][] initDecodeInputs(AlignedStripe alignedStripe,
      int dataBlkNum, int parityBlkNum) {
    byte[][] decodeInputs =
        new byte[dataBlkNum + parityBlkNum][(int) alignedStripe.getSpanInBlock()];
    for (int i = 0; i < alignedStripe.chunks.length; i++) {
      StripingChunk chunk = alignedStripe.chunks[i];
      if (chunk == null) {
        alignedStripe.chunks[i] = new StripingChunk(decodeInputs[i]);
        alignedStripe.chunks[i].offsetsInBuf.add(0);
        alignedStripe.chunks[i].lengthsInBuf.add((int) alignedStripe.getSpanInBlock());
      } else if (chunk.state == StripingChunk.FETCHED) {
        int posInBuf = 0;
        for (int j = 0; j < chunk.offsetsInBuf.size(); j++) {
          System.arraycopy(chunk.buf, chunk.offsetsInBuf.get(j),
              decodeInputs[i], posInBuf, chunk.lengthsInBuf.get(j));
          posInBuf += chunk.lengthsInBuf.get(j);
        }
      } else if (chunk.state == StripingChunk.ALLZERO) {
        Arrays.fill(decodeInputs[i], (byte)0);
      }
    }
    return decodeInputs;
  }

  /**
   * Decode based on the given input buffers and schema
   */
  public static void decodeAndFillBuffer(final byte[][] decodeInputs, byte[] buf,
      AlignedStripe alignedStripe, int dataBlkNum, int parityBlkNum) {
    int[] decodeIndices = new int[parityBlkNum];
    int pos = 0;
    for (int i = 0; i < alignedStripe.chunks.length; i++) {
      if (alignedStripe.chunks[i].state != StripingChunk.FETCHED &&
          alignedStripe.chunks[i].state != StripingChunk.ALLZERO) {
        decodeIndices[pos++] = i;
      }
    }

    byte[][] outputs = new byte[parityBlkNum][(int) alignedStripe.getSpanInBlock()];
    RSRawDecoder rsRawDecoder = new RSRawDecoder();
    rsRawDecoder.initialize(dataBlkNum, parityBlkNum, (int) alignedStripe.getSpanInBlock());
    rsRawDecoder.decode(decodeInputs, decodeIndices, outputs);

    for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
      StripingChunk chunk = alignedStripe.chunks[i];
      if (chunk.state == StripingChunk.MISSING) {
        int srcPos = 0;
        for (int j = 0; j < chunk.offsetsInBuf.size(); j++) {
          //TODO: workaround (filling fixed bytes), to remove after HADOOP-11938
//          System.arraycopy(outputs[i], srcPos, buf, chunk.offsetsInBuf.get(j),
//              chunk.lengthsInBuf.get(j));
          Arrays.fill(buf, chunk.offsetsInBuf.get(j),
              chunk.offsetsInBuf.get(j) + chunk.lengthsInBuf.get(j), (byte)7);
          srcPos += chunk.lengthsInBuf.get(j);
        }
      }
    }
  }

  /**
   * This method divides a requested byte range into an array of inclusive
   * {@link AlignedStripe}.
   * @param ecSchema The codec schema for the file, which carries the numbers
   *                 of data / parity blocks, as well as cell size
   * @param blockGroup The striped block group
   * @param rangeStartInBlockGroup The byte range's start offset in block group
   * @param rangeEndInBlockGroup The byte range's end offset in block group
   * @param buf Destination buffer of the read operation for the byte range
   * @param offsetInBuf Start offset into the destination buffer
   *
   * At most 5 stripes will be generated from each logical range, as
   * demonstrated in the header of {@link AlignedStripe}.
   */
  public static AlignedStripe[] divideByteRangeIntoStripes (
      ECSchema ecSchema, LocatedStripedBlock blockGroup,
      long rangeStartInBlockGroup, long rangeEndInBlockGroup, byte[] buf,
      int offsetInBuf) {
    // TODO: change ECSchema naming to use cell size instead of chunk size

    // Step 0: analyze range and calculate basic parameters
    int cellSize = ecSchema.getChunkSize();
    int dataBlkNum = ecSchema.getNumDataUnits();

    // Step 1: map the byte range to StripingCells
    StripingCell[] cells = getStripingCellsOfByteRange(ecSchema, blockGroup,
        rangeStartInBlockGroup, rangeEndInBlockGroup);

    // Step 2: get the unmerged ranges on each internal block
    VerticalRange[] ranges = getRangesForInternalBlocks(ecSchema, cells);

    // Step 3: merge into at most 5 stripes
    AlignedStripe[] stripes = mergeRangesForInternalBlocks(ecSchema, ranges);

    // Step 4: calculate each chunk's position in destination buffer
    calcualteChunkPositionsInBuf(ecSchema, stripes, cells, buf, offsetInBuf);

    // Step 5: prepare ALLZERO blocks
    prepareAllZeroChunks(blockGroup, buf, stripes, cellSize, dataBlkNum);

    return stripes;
  }

  /**
   * Map the logical byte range to a set of inclusive {@link StripingCell}
   * instances, each representing the overlap of the byte range to a cell
   * used by {@link DFSStripedOutputStream} in encoding
   */
  @VisibleForTesting
  private static StripingCell[] getStripingCellsOfByteRange(ECSchema ecSchema,
      LocatedStripedBlock blockGroup,
      long rangeStartInBlockGroup, long rangeEndInBlockGroup) {
    Preconditions.checkArgument(
        rangeStartInBlockGroup <= rangeEndInBlockGroup &&
            rangeEndInBlockGroup < blockGroup.getBlockSize());
    int cellSize = ecSchema.getChunkSize();
    int len = (int) (rangeEndInBlockGroup - rangeStartInBlockGroup + 1);
    int firstCellIdxInBG = (int) (rangeStartInBlockGroup / cellSize);
    int lastCellIdxInBG = (int) (rangeEndInBlockGroup / cellSize);
    int numCells = lastCellIdxInBG - firstCellIdxInBG + 1;
    StripingCell[] cells = new StripingCell[numCells];
    cells[0] = new StripingCell(ecSchema, firstCellIdxInBG);
    cells[numCells - 1] = new StripingCell(ecSchema, lastCellIdxInBG);

    cells[0].offset = (int) (rangeStartInBlockGroup % cellSize);
    cells[0].size =
        Math.min(cellSize - (int) (rangeStartInBlockGroup % cellSize), len);
    if (lastCellIdxInBG != firstCellIdxInBG) {
      cells[numCells - 1].size = (int) (rangeEndInBlockGroup % cellSize) + 1;
    }

    for (int i = 1; i < numCells - 1; i++) {
      cells[i] = new StripingCell(ecSchema, i + firstCellIdxInBG);
    }

    return cells;
  }

  /**
   * Given a logical start offset in a block group, calculate the physical
   * start offset into each stored internal block.
   */
  public static long[] getStartOffsetsForInternalBlocks(
      ECSchema ecSchema, LocatedStripedBlock blockGroup,
      long rangeStartInBlockGroup) {
    Preconditions.checkArgument(
        rangeStartInBlockGroup < blockGroup.getBlockSize());
    int dataBlkNum = ecSchema.getNumDataUnits();
    int parityBlkNum = ecSchema.getNumParityUnits();
    int cellSize = ecSchema.getChunkSize();
    long[] startOffsets = new long[dataBlkNum + parityBlkNum];
    Arrays.fill(startOffsets, -1L);
    int firstCellIdxInBG = (int) (rangeStartInBlockGroup / cellSize);
    StripingCell firstCell = new StripingCell(ecSchema, firstCellIdxInBG);
    firstCell.offset = (int) (rangeStartInBlockGroup % cellSize);
    startOffsets[firstCell.idxInStripe] =
        firstCell.idxInInternalBlk * cellSize + firstCell.offset;
    long earliestStart = startOffsets[firstCell.idxInStripe];
    for (int i = 1; i < dataBlkNum; i++) {
      int idx = firstCellIdxInBG + i;
      if (idx * cellSize >= blockGroup.getBlockSize()) {
        break;
      }
      StripingCell cell = new StripingCell(ecSchema, idx);
      startOffsets[cell.idxInStripe] = cell.idxInInternalBlk * cellSize;
      if (startOffsets[cell.idxInStripe] < earliestStart) {
        earliestStart = startOffsets[cell.idxInStripe];
      }
    }
    for (int i = dataBlkNum; i < dataBlkNum + parityBlkNum; i++) {
      startOffsets[i] = earliestStart;
    }
    return startOffsets;
  }

  /**
   * Given a logical byte range, mapped to each {@link StripingCell}, calculate
   * the physical byte range (inclusive) on each stored internal block.
   */
  @VisibleForTesting
  private static VerticalRange[] getRangesForInternalBlocks(ECSchema ecSchema,
      StripingCell[] cells) {
    int cellSize = ecSchema.getChunkSize();
    int dataBlkNum = ecSchema.getNumDataUnits();
    int parityBlkNum = ecSchema.getNumParityUnits();

    VerticalRange ranges[] = new VerticalRange[dataBlkNum + parityBlkNum];

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
      ECSchema ecSchema, VerticalRange[] ranges) {
    int dataBlkNum = ecSchema.getNumDataUnits();
    int parityBlkNum = ecSchema.getNumParityUnits();
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

  private static void calcualteChunkPositionsInBuf(ECSchema ecSchema,
      AlignedStripe[] stripes, StripingCell[] cells, byte[] buf,
      int offsetInBuf) {
    /**
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
     *
     * Cell indexing convention defined in {@link StripingCell}
     */
    int cellSize = ecSchema.getChunkSize();
    int done = 0;
    for (StripingCell cell : cells) {
      long cellStart = cell.idxInInternalBlk * cellSize + cell.offset;
      long cellEnd = cellStart + cell.size - 1;
      for (AlignedStripe s : stripes) {
        long stripeEnd = s.getOffsetInBlock() + s.getSpanInBlock() - 1;
        long overlapStart = Math.max(cellStart, s.getOffsetInBlock());
        long overlapEnd = Math.min(cellEnd, stripeEnd);
        int overLapLen = (int) (overlapEnd - overlapStart + 1);
        if (overLapLen <= 0) {
          continue;
        }
        if (s.chunks[cell.idxInStripe] == null) {
          s.chunks[cell.idxInStripe] = new StripingChunk(buf);
        }

        s.chunks[cell.idxInStripe].offsetsInBuf.
            add((int)(offsetInBuf + done + overlapStart - cellStart));
        s.chunks[cell.idxInStripe].lengthsInBuf.add(overLapLen);
      }
      done += cell.size;
    }
  }

  /**
   * If a {@link StripingChunk} maps to a byte range beyond an internal block's
   * size, the chunk should be treated as zero bytes in decoding.
   */
  private static void prepareAllZeroChunks(LocatedStripedBlock blockGroup,
      byte[] buf, AlignedStripe[] stripes, int cellSize, int dataBlkNum) {
    for (AlignedStripe s : stripes) {
      for (int i = 0; i < dataBlkNum; i++) {
        long internalBlkLen = getInternalBlockLength(blockGroup.getBlockSize(),
            cellSize, dataBlkNum, i);
        if (internalBlkLen <= s.getOffsetInBlock()) {
          Preconditions.checkState(s.chunks[i] == null);
          s.chunks[i] = new StripingChunk(buf);
          s.chunks[i].state = StripingChunk.ALLZERO;
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
   *  | <------- Striped Block Group -------> |
   *    blk_0          blk_1          blk_2
   *      |              |              |
   *      v              v              v
   * +----------+   +----------+   +----------+
   * |cell_0_0_0|   |cell_1_0_1|   |cell_2_0_2|
   * +----------+   +----------+   +----------+
   * |cell_3_1_0|   |cell_4_1_1|   |cell_5_1_2| <- {@link #idxInBlkGroup} = 5
   * +----------+   +----------+   +----------+    {@link #idxInInternalBlk} = 1
   *                                               {@link #idxInStripe} = 2
   * A StripingCell is a special instance of {@link StripingChunk} whose offset
   * and size align with the cell used when writing data.
   * TODO: consider parity cells
   */
  @VisibleForTesting
  static class StripingCell {
    public final ECSchema schema;
    /** Logical order in a block group, used when doing I/O to a block group */
    final int idxInBlkGroup;
    final int idxInInternalBlk;
    final int idxInStripe;
    /**
     * When a logical byte range is mapped to a set of cells, it might
     * partially overlap with the first and last cells. This field and the
     * {@link #size} variable represent the start offset and size of the
     * overlap.
     */
    int offset;
    int size;

    StripingCell(ECSchema ecSchema, int idxInBlkGroup) {
      this.schema = ecSchema;
      this.idxInBlkGroup = idxInBlkGroup;
      this.idxInInternalBlk = idxInBlkGroup / ecSchema.getNumDataUnits();
      this.idxInStripe = idxInBlkGroup -
          this.idxInInternalBlk * ecSchema.getNumDataUnits();
      this.offset = 0;
      this.size = ecSchema.getChunkSize();
    }

    StripingCell(ECSchema ecSchema, int idxInInternalBlk,
        int idxInStripe) {
      this.schema = ecSchema;
      this.idxInInternalBlk = idxInInternalBlk;
      this.idxInStripe = idxInStripe;
      this.idxInBlkGroup =
          idxInInternalBlk * ecSchema.getNumDataUnits() + idxInStripe;
      this.offset = 0;
      this.size = ecSchema.getChunkSize();
    }
  }

  /**
   * Given a requested byte range on a striped block group, an AlignedStripe
   * represents an inclusive {@link VerticalRange} that is aligned with both
   * the byte range and boundaries of all internal blocks. As illustrated in
   * the diagram, any given byte range on a block group leads to 1~5
   * AlignedStripe's.
   *
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
   * <---- data blocks ----> | <--- parity --->
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
  public static class AlignedStripe {
    public VerticalRange range;
    /** status of each chunk in the stripe */
    public final StripingChunk[] chunks;
    public int fetchedChunksNum = 0;
    public int missingChunksNum = 0;

    public AlignedStripe(long offsetInBlock, long length, int width) {
      Preconditions.checkArgument(offsetInBlock >= 0 && length >= 0);
      this.range = new VerticalRange(offsetInBlock, length);
      this.chunks = new StripingChunk[width];
    }

    public AlignedStripe(VerticalRange range, int width) {
      this.range = range;
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
      return "Offset=" + range.offsetInBlock + ", length=" + range.spanInBlock +
          ", fetchedChunksNum=" + fetchedChunksNum +
          ", missingChunksNum=" + missingChunksNum;
    }
  }

  /**
   * A simple utility class representing an arbitrary vertical inclusive range
   * starting at {@link #offsetInBlock} and lasting for {@link #spanInBlock}
   * bytes in an internal block. Note that VerticalRange doesn't necessarily
   * align with {@link StripingCell}.
   *
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
    /** start offset in the block group (inclusive) */
    public long offsetInBlock;
    /** length of the stripe range */
    public long spanInBlock;

    public VerticalRange(long offsetInBlock, long length) {
      Preconditions.checkArgument(offsetInBlock >= 0 && length >= 0);
      this.offsetInBlock = offsetInBlock;
      this.spanInBlock = length;
    }

    /** whether a position is in the range */
    public boolean include(long pos) {
      return pos >= offsetInBlock && pos < offsetInBlock + spanInBlock;
    }
  }

  /**
   * Indicates the coverage of an {@link AlignedStripe} on an internal block,
   * and the state of the chunk in the context of the read request.
   *
   * |<---------------- Striped Block Group --------------->|
   *   blk_0        blk_1        blk_2          blk_3   blk_4
   *                           +---------+  |  +----+  +----+
   *     null         null     |REQUESTED|  |  |null|  |null| <- AlignedStripe0
   *              +---------+  |---------|  |  |----|  |----|
   *     null     |REQUESTED|  |REQUESTED|  |  |null|  |null| <- AlignedStripe1
   * +---------+  +---------+  +---------+  |  +----+  +----+
   * |REQUESTED|  |REQUESTED|    ALLZERO    |  |null|  |null| <- AlignedStripe2
   * +---------+  +---------+               |  +----+  +----+
   * <----------- data blocks ------------> | <--- parity --->
   *
   * The class also carries {@link #buf}, {@link #offsetsInBuf}, and
   * {@link #lengthsInBuf} to define how read task for this chunk should
   * deliver the returned data.
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
     * REQUESTED (when AlignedStripe created) -> PENDING -> {FETCHED | MISSING}
     * If a chunk is completely outside requested range (including parity
     * chunks), state transition is:
     * null (AlignedStripe created) -> REQUESTED (upon failure) -> PENDING ...
     */
    public int state = REQUESTED;
    public byte[] buf;
    public List<Integer> offsetsInBuf;
    public List<Integer> lengthsInBuf;

    public StripingChunk(byte[] buf) {
      this.buf = buf;
      this.offsetsInBuf = new ArrayList<>();
      this.lengthsInBuf = new ArrayList<>();
    }

    public int[] getOffsets() {
      int[] offsets = new int[offsetsInBuf.size()];
      for (int i = 0; i < offsets.length; i++) {
        offsets[i] = offsetsInBuf.get(i);
      }
      return offsets;
    }

    public int[] getLengths() {
      int[] lens = new int[this.lengthsInBuf.size()];
      for (int i = 0; i < lens.length; i++) {
        lens[i] = this.lengthsInBuf.get(i);
      }
      return lens;
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

    public StripingChunkReadResult(int state) {
      Preconditions.checkArgument(state == TIMEOUT,
          "Only timeout result should return negative index.");
      this.index = -1;
      this.state = state;
    }

    public StripingChunkReadResult(int index, int state) {
      Preconditions.checkArgument(state != TIMEOUT,
          "Timeout result should return negative index.");
      this.index = index;
      this.state = state;
    }

    @Override
    public String toString() {
      return "(index=" + index + ", state =" + state + ")";
    }
  }
}
