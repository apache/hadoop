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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.*;

import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Need to cover the following combinations:
 * 1. Block group size:
 *  1.1 One byte
 *  1.2 Smaller than cell
 *  1.3 One full cell
 *  1.4 x full cells, where x is smaller than number of data blocks
 *  1.5 x full cells plus a partial cell
 *  1.6 One full stripe
 *  1.7 One full stripe plus a partial cell
 *  1.8 One full stripe plus x full cells
 *  1.9 One full stripe plus x full cells plus a partial cell
 *  1.10 y full stripes, but smaller than full block group size
 *  1.11 Full block group size
 *
 * 2. Byte range start
 *  2.1 Zero
 *  2.2 Within first cell
 *  2.3 End of first cell
 *  2.4 Start of a middle* cell in the first stripe (* neither first or last)
 *  2.5 End of middle cell in the first stripe
 *  2.6 Within a middle cell in the first stripe
 *  2.7 Start of the last cell in the first stripe
 *  2.8 Within the last cell in the first stripe
 *  2.9 End of the last cell in the first stripe
 *  2.10 Start of a middle stripe
 *  2.11 Within a middle stripe
 *  2.12 End of a middle stripe
 *  2.13 Start of the last stripe
 *  2.14 Within the last stripe
 *  2.15 End of the last stripe (last byte)
 *
 * 3. Byte range length: same settings as block group size
 *
 * We should test in total 11 x 15 x 11 = 1815 combinations
 * TODO: test parity block logic
 */
public class TestStripedBlockUtil {
  // use hard coded policy - see HDFS-9816
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final short dataBlocks = (short) ecPolicy.getNumDataUnits();
  private final short parityBlocks = (short) ecPolicy.getNumParityUnits();
  private final short groupSize = (short) (dataBlocks + parityBlocks);
  private final int cellSize = ecPolicy.getCellSize();
  private final int stripeSize = dataBlocks * cellSize;
  /** number of full stripes in a full block group */
  private final int stripesPerBlock = 16;
  private final Random random = new Random();

  private int[] blockGroupSizes;
  private int[] byteRangeStartOffsets;
  private int[] byteRangeSizes;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup(){
    blockGroupSizes = new int[]{1, getDelta(cellSize), cellSize,
        getDelta(dataBlocks) * cellSize,
        getDelta(dataBlocks) * cellSize + getDelta(cellSize),
        stripeSize, stripeSize + getDelta(cellSize),
        stripeSize + getDelta(dataBlocks) * cellSize,
        stripeSize + getDelta(dataBlocks) * cellSize + getDelta(cellSize),
        getDelta(stripesPerBlock) * stripeSize,
        stripesPerBlock * stripeSize};
    byteRangeStartOffsets = new int[] {0, getDelta(cellSize), cellSize - 1};
    byteRangeSizes = new int[]{1, getDelta(cellSize), cellSize,
        getDelta(dataBlocks) * cellSize,
        getDelta(dataBlocks) * cellSize + getDelta(cellSize),
        stripeSize, stripeSize + getDelta(cellSize),
        stripeSize + getDelta(dataBlocks) * cellSize,
        stripeSize + getDelta(dataBlocks) * cellSize + getDelta(cellSize),
        getDelta(stripesPerBlock) * stripeSize,
        stripesPerBlock * stripeSize};
  }

  private int getDelta(int size) {
    return 1 + random.nextInt(size - 2);
  }
  private byte hashIntToByte(int i) {
    int BYTE_MASK = 0xff;
    return (byte) (((i + 13) * 29) & BYTE_MASK);
  }

  private LocatedStripedBlock createDummyLocatedBlock(long bgSize) {
    final long blockGroupID = -1048576;
    DatanodeInfo[] locs = new DatanodeInfo[groupSize];
    String[] storageIDs = new String[groupSize];
    StorageType[] storageTypes = new StorageType[groupSize];
    byte[] indices = new byte[groupSize];
    for (int i = 0; i < groupSize; i++) {
      indices[i] = (byte) ((i + 2) % dataBlocks);
      // Location port always equal to logical index of a block,
      // for easier verification
      locs[i] = DFSTestUtil.getLocalDatanodeInfo(indices[i]);
      storageIDs[i] = locs[i].getDatanodeUuid();
      storageTypes[i] = StorageType.DISK;
    }
    return new LocatedStripedBlock(new ExtendedBlock("pool", blockGroupID,
        bgSize, 1001), locs, storageIDs, storageTypes, indices, 0, false,
        null);
  }

  private byte[][] createInternalBlkBuffers(int bgSize) {
    byte[][] bufs = new byte[dataBlocks + parityBlocks][];
    int[] pos = new int[dataBlocks + parityBlocks];
    for (int i = 0; i < dataBlocks + parityBlocks; i++) {
      int bufSize = (int) getInternalBlockLength(
          bgSize, cellSize, dataBlocks, i);
      bufs[i] = new byte[bufSize];
      pos[i] = 0;
    }
    int done = 0;
    while (done < bgSize) {
      Preconditions.checkState(done % cellSize == 0);
      StripingCell cell =
          new StripingCell(ecPolicy, cellSize, done / cellSize, 0);
      int idxInStripe = cell.getIdxInStripe();
      int size = Math.min(cellSize, bgSize - done);
      for (int i = 0; i < size; i++) {
        bufs[idxInStripe][pos[idxInStripe] + i] = hashIntToByte(done + i);
      }
      done += size;
      pos[idxInStripe] += size;
    }

    return bufs;
  }

  @Test
  public void testLocatedStripedBlockType() {
    LocatedStripedBlock lsb =
        new LocatedStripedBlock(null, null, null, null, null, 0, false, null);
    assertEquals(BlockType.STRIPED, lsb.getBlockType());
  }

  @Test
  public void testParseDummyStripedBlock() {
    LocatedStripedBlock lsb = createDummyLocatedBlock(
        stripeSize * stripesPerBlock);
    LocatedBlock[] blocks = parseStripedBlockGroup(
        lsb, cellSize, dataBlocks, parityBlocks);
    assertEquals(dataBlocks + parityBlocks, blocks.length);
    for (int i = 0; i < dataBlocks; i++) {
      assertFalse(blocks[i].isStriped());
      assertEquals(i,
          BlockIdManager.getBlockIndex(blocks[i].getBlock().getLocalBlock()));
      assertEquals(0, blocks[i].getStartOffset());
      assertEquals(1, blocks[i].getLocations().length);
      assertEquals(i, blocks[i].getLocations()[0].getIpcPort());
      assertEquals(i, blocks[i].getLocations()[0].getXferPort());
    }
  }

  private void verifyInternalBlocks (int numBytesInGroup, int[] expected) {
    for (int i = 1; i < groupSize; i++) {
      assertEquals(expected[i],
          getInternalBlockLength(numBytesInGroup, cellSize, dataBlocks, i));
    }
  }

  @Test
  public void testGetInternalBlockLength () {
    // A small delta that is smaller than a cell
    final int delta = 10;

    // Block group is smaller than a cell
    verifyInternalBlocks(cellSize - delta,
        new int[] {cellSize - delta, 0, 0, 0, 0, 0,
            cellSize - delta, cellSize - delta, cellSize - delta});

    // Block group is exactly as large as a cell
    verifyInternalBlocks(cellSize,
        new int[] {cellSize, 0, 0, 0, 0, 0,
            cellSize, cellSize, cellSize});

    // Block group is a little larger than a cell
    verifyInternalBlocks(cellSize + delta,
        new int[] {cellSize, delta, 0, 0, 0, 0,
            cellSize, cellSize, cellSize});

    // Block group contains multiple stripes and ends at stripe boundary
    verifyInternalBlocks(2 * dataBlocks * cellSize,
        new int[] {2 * cellSize, 2 * cellSize, 2 * cellSize,
            2 * cellSize, 2 * cellSize, 2 * cellSize,
            2 * cellSize, 2 * cellSize, 2 * cellSize});

    // Block group contains multiple stripes and ends at cell boundary
    // (not ending at stripe boundary)
    verifyInternalBlocks(2 * dataBlocks * cellSize + cellSize,
        new int[] {3 * cellSize, 2 * cellSize, 2 * cellSize,
            2 * cellSize, 2 * cellSize, 2 * cellSize,
            3 * cellSize, 3 * cellSize, 3 * cellSize});

    // Block group contains multiple stripes and doesn't end at cell boundary
    verifyInternalBlocks(2 * dataBlocks * cellSize - delta,
        new int[] {2 * cellSize, 2 * cellSize, 2 * cellSize,
            2 * cellSize, 2 * cellSize, 2 * cellSize - delta,
            2 * cellSize, 2 * cellSize, 2 * cellSize});
  }

  /**
   * Test dividing a byte range into aligned stripes and verify the aligned
   * ranges can be translated back to the byte range.
   */
  @Test
  public void testDivideByteRangeIntoStripes() {
    ByteBuffer assembled =
        ByteBuffer.allocate(stripesPerBlock * stripeSize);
    for (int bgSize : blockGroupSizes) {
      LocatedStripedBlock blockGroup = createDummyLocatedBlock(bgSize);
      byte[][] internalBlkBufs = createInternalBlkBuffers(bgSize);
      for (int brStart : byteRangeStartOffsets) {
        for (int brSize : byteRangeSizes) {
          if (brStart + brSize > bgSize) {
            continue;
          }
          AlignedStripe[] stripes = divideByteRangeIntoStripes(ecPolicy,
              cellSize, blockGroup, brStart, brStart + brSize - 1, assembled);

          for (AlignedStripe stripe : stripes) {
            for (int i = 0; i < dataBlocks; i++) {
              StripingChunk chunk = stripe.chunks[i];
              if (chunk == null || chunk.state != StripingChunk.REQUESTED) {
                continue;
              }
              int done = 0;
              int len;
              for (ByteBuffer slice : chunk.getChunkBuffer().getSlices()) {
                len = slice.remaining();
                slice.put(internalBlkBufs[i],
                    (int) stripe.getOffsetInBlock() + done, len);
                done += len;
              }
            }
          }
          for (int i = 0; i < brSize; i++) {
            if (hashIntToByte(brStart + i) != assembled.get(i)) {
              System.out.println("Oops");
            }
            assertEquals("Byte at " + (brStart + i) + " should be the same",
                hashIntToByte(brStart + i), assembled.get(i));
          }
        }
      }
    }
  }

  /**
   * Test dividing a byte range that located above the 2GB range, which is
   * {@link Integer#MAX_VALUE}.
   *
   * HDFS-12860 occurs when {@link VerticalRange#offsetInBlock} is larger than
   * {@link Integer#MAX_VALUE}
   *
   * Take RS-6-3-1024k EC policy as example:
   *  <li>cellSize = 1MB</li>
   *  <li>The first {@link VerticalRange#offsetInBlock} that is larger than
   *  {@link Integer#MAX_VALUE} is Math.ceilInteger.MAX_VALUE / cellSize = 2048
   *  </li>
   *  <li>The first offset in block group that causes HDFS-12860 is:
   *  2048 * cellSize * dataBlocks (6)</li>
   */
  @Test
  public void testDivideOneStripeLargeBlockSize() {
    ByteBuffer buffer = ByteBuffer.allocate(stripeSize);

    // This offset will cause overflow before HDFS-12860.
    long offsetInInternalBlk = Integer.MAX_VALUE / cellSize + 10;
    long rangeStartInBlockGroup = offsetInInternalBlk * dataBlocks * cellSize;
    long rangeEndInBlockGroup = rangeStartInBlockGroup +
        dataBlocks / 2 * cellSize - 1;
    // each block is 4GB, each block group has 4GB * (6 + 3) = 36GB.
    long blockGroupSize = 4096L * cellSize * groupSize;
    LocatedStripedBlock blockGroup = createDummyLocatedBlock(blockGroupSize);
    AlignedStripe[] stripes = StripedBlockUtil.divideOneStripe(ecPolicy,
        cellSize, blockGroup, rangeStartInBlockGroup, rangeEndInBlockGroup,
        buffer);
    long offset = offsetInInternalBlk * cellSize;
    assertTrue(offset > Integer.MAX_VALUE);
    assertEquals(offset, stripes[0].range.offsetInBlock);
    assertEquals(1, stripes.length);
  }
}
