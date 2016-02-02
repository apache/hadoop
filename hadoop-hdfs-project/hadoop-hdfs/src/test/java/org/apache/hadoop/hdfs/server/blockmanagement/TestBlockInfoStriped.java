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

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo.AddBlockResult;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hdfs.StripedFileTestUtil.NUM_DATA_BLOCKS;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.NUM_PARITY_BLOCKS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test {@link BlockInfoStriped}
 */
public class TestBlockInfoStriped {
  private static final int TOTAL_NUM_BLOCKS = NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS;
  private static final long BASE_ID = -1600;
  private static final Block baseBlock = new Block(BASE_ID);
  private static final ErasureCodingPolicy testECPolicy
      = ErasureCodingPolicyManager.getSystemDefaultPolicy();
  private final BlockInfoStriped info = new BlockInfoStriped(baseBlock,
      testECPolicy);

  private Block[] createReportedBlocks(int num) {
    Block[] blocks = new Block[num];
    for (int i = 0; i < num; i++) {
      blocks[i] = new Block(BASE_ID + i);
    }
    return blocks;
  }

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  /**
   * Test adding storage and reported block
   */
  @Test
  public void testAddStorage() {
    // first add NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS storages, i.e., a complete
    // group of blocks/storages
    DatanodeStorageInfo[] storageInfos = DFSTestUtil.createDatanodeStorageInfos(
        TOTAL_NUM_BLOCKS);
    Block[] blocks = createReportedBlocks(TOTAL_NUM_BLOCKS);
    int i = 0;
    for (; i < storageInfos.length; i += 2) {
      info.addStorage(storageInfos[i], blocks[i]);
      Assert.assertEquals(i/2 + 1, info.numNodes());
    }
    i /= 2;
    for (int j = 1; j < storageInfos.length; j += 2) {
      Assert.assertTrue(info.addStorage(storageInfos[j], blocks[j]));
      Assert.assertEquals(i + (j+1)/2, info.numNodes());
    }

    // check
    byte[] indices = (byte[]) Whitebox.getInternalState(info, "indices");
    Assert.assertEquals(TOTAL_NUM_BLOCKS, info.getCapacity());
    Assert.assertEquals(TOTAL_NUM_BLOCKS, indices.length);
    i = 0;
    for (DatanodeStorageInfo storage : storageInfos) {
      int index = info.findStorageInfo(storage);
      Assert.assertEquals(i++, index);
      Assert.assertEquals(index, indices[index]);
    }

    // the same block is reported from the same storage twice
    i = 0;
    for (DatanodeStorageInfo storage : storageInfos) {
      Assert.assertTrue(info.addStorage(storage, blocks[i++]));
    }
    Assert.assertEquals(TOTAL_NUM_BLOCKS, info.getCapacity());
    Assert.assertEquals(TOTAL_NUM_BLOCKS, info.numNodes());
    Assert.assertEquals(TOTAL_NUM_BLOCKS, indices.length);
    i = 0;
    for (DatanodeStorageInfo storage : storageInfos) {
      int index = info.findStorageInfo(storage);
      Assert.assertEquals(i++, index);
      Assert.assertEquals(index, indices[index]);
    }

    // the same block is reported from another storage
    DatanodeStorageInfo[] storageInfos2 = DFSTestUtil.createDatanodeStorageInfos(
        TOTAL_NUM_BLOCKS * 2);
    // only add the second half of info2
    for (i = TOTAL_NUM_BLOCKS; i < storageInfos2.length; i++) {
      info.addStorage(storageInfos2[i], blocks[i % TOTAL_NUM_BLOCKS]);
      Assert.assertEquals(i + 1, info.getCapacity());
      Assert.assertEquals(i + 1, info.numNodes());
      indices = (byte[]) Whitebox.getInternalState(info, "indices");
      Assert.assertEquals(i + 1, indices.length);
    }
    for (i = TOTAL_NUM_BLOCKS; i < storageInfos2.length; i++) {
      int index = info.findStorageInfo(storageInfos2[i]);
      Assert.assertEquals(i++, index);
      Assert.assertEquals(index - TOTAL_NUM_BLOCKS, indices[index]);
    }
  }

  @Test
  public void testRemoveStorage() {
    // first add TOTAL_NUM_BLOCKS into the BlockInfoStriped
    DatanodeStorageInfo[] storages = DFSTestUtil.createDatanodeStorageInfos(
        TOTAL_NUM_BLOCKS);
    Block[] blocks = createReportedBlocks(TOTAL_NUM_BLOCKS);
    for (int i = 0; i < storages.length; i++) {
      info.addStorage(storages[i], blocks[i]);
    }

    // remove two storages
    info.removeStorage(storages[0]);
    info.removeStorage(storages[2]);

    // check
    Assert.assertEquals(TOTAL_NUM_BLOCKS, info.getCapacity());
    Assert.assertEquals(TOTAL_NUM_BLOCKS - 2, info.numNodes());
    byte[] indices = (byte[]) Whitebox.getInternalState(info, "indices");
    for (int i = 0; i < storages.length; i++) {
      int index = info.findStorageInfo(storages[i]);
      if (i != 0 && i != 2) {
        Assert.assertEquals(i, index);
        Assert.assertEquals(index, indices[index]);
      } else {
        Assert.assertEquals(-1, index);
        Assert.assertEquals(-1, indices[i]);
      }
    }

    // the same block is reported from another storage
    DatanodeStorageInfo[] storages2 = DFSTestUtil.createDatanodeStorageInfos(
        TOTAL_NUM_BLOCKS * 2);
    for (int i = TOTAL_NUM_BLOCKS; i < storages2.length; i++) {
      info.addStorage(storages2[i], blocks[i % TOTAL_NUM_BLOCKS]);
    }
    // now we should have 8 storages
    Assert.assertEquals(TOTAL_NUM_BLOCKS * 2 - 2, info.numNodes());
    Assert.assertEquals(TOTAL_NUM_BLOCKS * 2 - 2, info.getCapacity());
    indices = (byte[]) Whitebox.getInternalState(info, "indices");
    Assert.assertEquals(TOTAL_NUM_BLOCKS * 2 - 2, indices.length);
    int j = TOTAL_NUM_BLOCKS;
    for (int i = TOTAL_NUM_BLOCKS; i < storages2.length; i++) {
      int index = info.findStorageInfo(storages2[i]);
      if (i == TOTAL_NUM_BLOCKS || i == TOTAL_NUM_BLOCKS + 2) {
        Assert.assertEquals(i - TOTAL_NUM_BLOCKS, index);
      } else {
        Assert.assertEquals(j++, index);
      }
    }

    // remove the storages from storages2
    for (int i = 0; i < TOTAL_NUM_BLOCKS; i++) {
      info.removeStorage(storages2[i + TOTAL_NUM_BLOCKS]);
    }
    // now we should have 3 storages
    Assert.assertEquals(TOTAL_NUM_BLOCKS - 2, info.numNodes());
    Assert.assertEquals(TOTAL_NUM_BLOCKS * 2 - 2, info.getCapacity());
    indices = (byte[]) Whitebox.getInternalState(info, "indices");
    Assert.assertEquals(TOTAL_NUM_BLOCKS * 2 - 2, indices.length);
    for (int i = 0; i < TOTAL_NUM_BLOCKS; i++) {
      if (i == 0 || i == 2) {
        int index = info.findStorageInfo(storages2[i + TOTAL_NUM_BLOCKS]);
        Assert.assertEquals(-1, index);
      } else {
        int index = info.findStorageInfo(storages[i]);
        Assert.assertEquals(i, index);
      }
    }
    for (int i = TOTAL_NUM_BLOCKS; i < TOTAL_NUM_BLOCKS * 2 - 2; i++) {
      Assert.assertEquals(-1, indices[i]);
      Assert.assertNull(info.getDatanode(i));
    }
  }

  @Test
  public void testWrite() {
    long blkID = 1;
    long numBytes = 1;
    long generationStamp = 1;
    ByteBuffer byteBuffer = ByteBuffer.allocate(Long.SIZE / Byte.SIZE * 3);
    byteBuffer.putLong(blkID).putLong(numBytes).putLong(generationStamp);

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(byteStream);
    BlockInfoStriped blk = new BlockInfoStriped(new Block(blkID, numBytes,
        generationStamp), testECPolicy);

    try {
      blk.write(out);
    } catch(Exception ex) {
      fail("testWrite error:" + ex.getMessage());
    }
    assertEquals(byteBuffer.array().length, byteStream.toByteArray().length);
    assertArrayEquals(byteBuffer.array(), byteStream.toByteArray());
  }
}
