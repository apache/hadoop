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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.*;

import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
  private final ErasureCodingPolicy EC_POLICY =
      ErasureCodingPolicyManager.getSystemPolicies()[0];
  private final short DATA_BLK_NUM = (short) EC_POLICY.getNumDataUnits();
  private final short PARITY_BLK_NUM = (short) EC_POLICY.getNumParityUnits();
  private final short BLK_GROUP_WIDTH = (short) (DATA_BLK_NUM + PARITY_BLK_NUM);
  private final int CELLSIZE = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private final int FULL_STRIPE_SIZE = DATA_BLK_NUM * CELLSIZE;
  /** number of full stripes in a full block group */
  private final int BLK_GROUP_STRIPE_NUM = 16;
  private final Random random = new Random();

  private int[] blockGroupSizes;
  private int[] byteRangeStartOffsets;
  private int[] byteRangeSizes;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup(){
    blockGroupSizes = new int[]{1, getDelta(CELLSIZE), CELLSIZE,
        getDelta(DATA_BLK_NUM) * CELLSIZE,
        getDelta(DATA_BLK_NUM) * CELLSIZE + getDelta(CELLSIZE),
        FULL_STRIPE_SIZE, FULL_STRIPE_SIZE + getDelta(CELLSIZE),
        FULL_STRIPE_SIZE + getDelta(DATA_BLK_NUM) * CELLSIZE,
        FULL_STRIPE_SIZE + getDelta(DATA_BLK_NUM) * CELLSIZE + getDelta(CELLSIZE),
        getDelta(BLK_GROUP_STRIPE_NUM) * FULL_STRIPE_SIZE,
        BLK_GROUP_STRIPE_NUM * FULL_STRIPE_SIZE};
    byteRangeStartOffsets = new int[] {0, getDelta(CELLSIZE), CELLSIZE - 1};
    byteRangeSizes = new int[]{1, getDelta(CELLSIZE), CELLSIZE,
        getDelta(DATA_BLK_NUM) * CELLSIZE,
        getDelta(DATA_BLK_NUM) * CELLSIZE + getDelta(CELLSIZE),
        FULL_STRIPE_SIZE, FULL_STRIPE_SIZE + getDelta(CELLSIZE),
        FULL_STRIPE_SIZE + getDelta(DATA_BLK_NUM) * CELLSIZE,
        FULL_STRIPE_SIZE + getDelta(DATA_BLK_NUM) * CELLSIZE + getDelta(CELLSIZE),
        getDelta(BLK_GROUP_STRIPE_NUM) * FULL_STRIPE_SIZE,
        BLK_GROUP_STRIPE_NUM * FULL_STRIPE_SIZE};
  }

  private int getDelta(int size) {
    return 1 + random.nextInt(size - 2);
  }
  private byte hashIntToByte(int i) {
    int BYTE_MASK = 0xff;
    return (byte) (((i + 13) * 29) & BYTE_MASK);
  }

  private LocatedStripedBlock createDummyLocatedBlock(int bgSize) {
    final long blockGroupID = -1048576;
    DatanodeInfo[] locs = new DatanodeInfo[BLK_GROUP_WIDTH];
    String[] storageIDs = new String[BLK_GROUP_WIDTH];
    StorageType[] storageTypes = new StorageType[BLK_GROUP_WIDTH];
    byte[] indices = new byte[BLK_GROUP_WIDTH];
    for (int i = 0; i < BLK_GROUP_WIDTH; i++) {
      indices[i] = (byte) ((i + 2) % DATA_BLK_NUM);
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
    byte[][] bufs = new byte[DATA_BLK_NUM + PARITY_BLK_NUM][];
    int[] pos = new int[DATA_BLK_NUM + PARITY_BLK_NUM];
    for (int i = 0; i < DATA_BLK_NUM + PARITY_BLK_NUM; i++) {
      int bufSize = (int) getInternalBlockLength(
          bgSize, CELLSIZE, DATA_BLK_NUM, i);
      bufs[i] = new byte[bufSize];
      pos[i] = 0;
    }
    int done = 0;
    while (done < bgSize) {
      Preconditions.checkState(done % CELLSIZE == 0);
      StripingCell cell = new StripingCell(EC_POLICY, CELLSIZE, done / CELLSIZE, 0);
      int idxInStripe = cell.idxInStripe;
      int size = Math.min(CELLSIZE, bgSize - done);
      for (int i = 0; i < size; i++) {
        bufs[idxInStripe][pos[idxInStripe] + i] = hashIntToByte(done + i);
      }
      done += size;
      pos[idxInStripe] += size;
    }

    return bufs;
  }

  @Test
  public void testParseDummyStripedBlock() {
    LocatedStripedBlock lsb = createDummyLocatedBlock(
        BLK_GROUP_STRIPE_NUM * FULL_STRIPE_SIZE);
    LocatedBlock[] blocks = parseStripedBlockGroup(
        lsb, CELLSIZE, DATA_BLK_NUM, PARITY_BLK_NUM);
    assertEquals(DATA_BLK_NUM + PARITY_BLK_NUM, blocks.length);
    for (int i = 0; i < DATA_BLK_NUM; i++) {
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
    for (int i = 1; i < BLK_GROUP_WIDTH; i++) {
      assertEquals(expected[i],
          getInternalBlockLength(numBytesInGroup, CELLSIZE, DATA_BLK_NUM, i));
    }
  }

  @Test
  public void testGetInternalBlockLength () {
    // A small delta that is smaller than a cell
    final int delta = 10;

    // Block group is smaller than a cell
    verifyInternalBlocks(CELLSIZE - delta,
        new int[] {CELLSIZE - delta, 0, 0, 0, 0, 0,
            CELLSIZE - delta, CELLSIZE - delta, CELLSIZE - delta});

    // Block group is exactly as large as a cell
    verifyInternalBlocks(CELLSIZE,
        new int[] {CELLSIZE, 0, 0, 0, 0, 0,
            CELLSIZE, CELLSIZE, CELLSIZE});

    // Block group is a little larger than a cell
    verifyInternalBlocks(CELLSIZE + delta,
        new int[] {CELLSIZE, delta, 0, 0, 0, 0,
            CELLSIZE, CELLSIZE, CELLSIZE});

    // Block group contains multiple stripes and ends at stripe boundary
    verifyInternalBlocks(2 * DATA_BLK_NUM * CELLSIZE,
        new int[] {2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE,
            2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE,
            2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE});

    // Block group contains multiple stripes and ends at cell boundary
    // (not ending at stripe boundary)
    verifyInternalBlocks(2 * DATA_BLK_NUM * CELLSIZE + CELLSIZE,
        new int[] {3 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE,
            2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE,
            3 * CELLSIZE, 3 * CELLSIZE, 3 * CELLSIZE});

    // Block group contains multiple stripes and doesn't end at cell boundary
    verifyInternalBlocks(2 * DATA_BLK_NUM * CELLSIZE - delta,
        new int[] {2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE,
            2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE - delta,
            2 * CELLSIZE, 2 * CELLSIZE, 2 * CELLSIZE});
  }

  /**
   * Test dividing a byte range into aligned stripes and verify the aligned
   * ranges can be translated back to the byte range.
   */
  @Test
  public void testDivideByteRangeIntoStripes() {
    ByteBuffer assembled =
        ByteBuffer.allocate(BLK_GROUP_STRIPE_NUM * FULL_STRIPE_SIZE);
    for (int bgSize : blockGroupSizes) {
      LocatedStripedBlock blockGroup = createDummyLocatedBlock(bgSize);
      byte[][] internalBlkBufs = createInternalBlkBuffers(bgSize);
      for (int brStart : byteRangeStartOffsets) {
        for (int brSize : byteRangeSizes) {
          if (brStart + brSize > bgSize) {
            continue;
          }
          AlignedStripe[] stripes = divideByteRangeIntoStripes(EC_POLICY,
              CELLSIZE, blockGroup, brStart, brStart + brSize - 1, assembled);

          for (AlignedStripe stripe : stripes) {
            for (int i = 0; i < DATA_BLK_NUM; i++) {
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
}
