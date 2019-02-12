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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Test {@link BlockInfoStriped}.
 */
@RunWith(Parameterized.class)
public class TestBlockInfoStriped {
  private static final long BASE_ID = -1600;
  private final Block baseBlock = new Block(BASE_ID);
  private final ErasureCodingPolicy testECPolicy;
  private final int totalBlocks;
  private final BlockInfoStriped info;

  public TestBlockInfoStriped(ErasureCodingPolicy policy) {
    testECPolicy = policy;
    totalBlocks = testECPolicy.getNumDataUnits()
        + testECPolicy.getNumParityUnits();
    info = new BlockInfoStriped(baseBlock, testECPolicy);
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Collection<Object[]> policies() {
    return StripedFileTestUtil.getECPolicies();
  }

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
   * Test adding storage and reported block.
   */
  @Test
  public void testAddStorage() {
    // first add NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS storages, i.e., a complete
    // group of blocks/storages
    DatanodeStorageInfo[] storageInfos = DFSTestUtil.createDatanodeStorageInfos(
        totalBlocks);
    Block[] blocks = createReportedBlocks(totalBlocks);
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
    Assert.assertEquals(totalBlocks, info.getCapacity());
    Assert.assertEquals(totalBlocks, indices.length);
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
    Assert.assertEquals(totalBlocks, info.getCapacity());
    Assert.assertEquals(totalBlocks, info.numNodes());
    Assert.assertEquals(totalBlocks, indices.length);
    i = 0;
    for (DatanodeStorageInfo storage : storageInfos) {
      int index = info.findStorageInfo(storage);
      Assert.assertEquals(i++, index);
      Assert.assertEquals(index, indices[index]);
    }

    // the same block is reported from another storage
    DatanodeStorageInfo[] storageInfos2 =
        DFSTestUtil.createDatanodeStorageInfos(totalBlocks * 2);
    // only add the second half of info2
    for (i = totalBlocks; i < storageInfos2.length; i++) {
      info.addStorage(storageInfos2[i], blocks[i % totalBlocks]);
      Assert.assertEquals(i + 1, info.getCapacity());
      Assert.assertEquals(i + 1, info.numNodes());
      indices = (byte[]) Whitebox.getInternalState(info, "indices");
      Assert.assertEquals(i + 1, indices.length);
    }
    for (i = totalBlocks; i < storageInfos2.length; i++) {
      int index = info.findStorageInfo(storageInfos2[i]);
      Assert.assertEquals(i++, index);
      Assert.assertEquals(index - totalBlocks, indices[index]);
    }
  }

  @Test
  public void testRemoveStorage() {
    // first add TOTAL_NUM_BLOCKS into the BlockInfoStriped
    DatanodeStorageInfo[] storages = DFSTestUtil.createDatanodeStorageInfos(
        totalBlocks);
    Block[] blocks = createReportedBlocks(totalBlocks);
    for (int i = 0; i < storages.length; i++) {
      info.addStorage(storages[i], blocks[i]);
    }

    // remove two storages
    info.removeStorage(storages[0]);
    info.removeStorage(storages[2]);

    // check
    Assert.assertEquals(totalBlocks, info.getCapacity());
    Assert.assertEquals(totalBlocks - 2, info.numNodes());
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
        totalBlocks * 2);
    for (int i = totalBlocks; i < storages2.length; i++) {
      info.addStorage(storages2[i], blocks[i % totalBlocks]);
    }
    // now we should have 8 storages
    Assert.assertEquals(totalBlocks * 2 - 2, info.numNodes());
    Assert.assertEquals(totalBlocks * 2 - 2, info.getCapacity());
    indices = (byte[]) Whitebox.getInternalState(info, "indices");
    Assert.assertEquals(totalBlocks * 2 - 2, indices.length);
    int j = totalBlocks;
    for (int i = totalBlocks; i < storages2.length; i++) {
      int index = info.findStorageInfo(storages2[i]);
      if (i == totalBlocks || i == totalBlocks + 2) {
        Assert.assertEquals(i - totalBlocks, index);
      } else {
        Assert.assertEquals(j++, index);
      }
    }

    // remove the storages from storages2
    for (int i = 0; i < totalBlocks; i++) {
      info.removeStorage(storages2[i + totalBlocks]);
    }
    // now we should have 3 storages
    Assert.assertEquals(totalBlocks - 2, info.numNodes());
    Assert.assertEquals(totalBlocks * 2 - 2, info.getCapacity());
    indices = (byte[]) Whitebox.getInternalState(info, "indices");
    Assert.assertEquals(totalBlocks * 2 - 2, indices.length);
    for (int i = 0; i < totalBlocks; i++) {
      if (i == 0 || i == 2) {
        int index = info.findStorageInfo(storages2[i + totalBlocks]);
        Assert.assertEquals(-1, index);
      } else {
        int index = info.findStorageInfo(storages[i]);
        Assert.assertEquals(i, index);
      }
    }
    for (int i = totalBlocks; i < totalBlocks * 2 - 2; i++) {
      Assert.assertEquals(-1, indices[i]);
      Assert.assertNull(info.getDatanode(i));
    }
  }

  @Test
  public void testGetBlockInfo() throws IllegalArgumentException, Exception {
    int dataBlocks = testECPolicy.getNumDataUnits();
    int parityBlocks = testECPolicy.getNumParityUnits();
    int totalSize = dataBlocks + parityBlocks;
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    Configuration conf = new Configuration();
    try (MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf, builderBaseDir).numDataNodes(totalSize)
            .build()) {
      DistributedFileSystem fs = cluster.getFileSystem();
      fs.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());
      fs.enableErasureCodingPolicy(testECPolicy.getName());
      fs.mkdirs(new Path("/ecDir"));
      fs.setErasureCodingPolicy(new Path("/ecDir"), testECPolicy.getName());
      DFSTestUtil.createFile(fs, new Path("/ecDir/ecFile"),
          fs.getDefaultBlockSize() * dataBlocks, (short) 1, 1024);
      ExtendedBlock blk = DFSTestUtil
          .getAllBlocks(fs, new Path("/ecDir/ecFile")).get(0).getBlock();
      String id = "blk_" + Long.toString(blk.getBlockId());
      BlockInfo bInfo = cluster.getNameNode().getNamesystem().getBlockManager()
          .getStoredBlock(blk.getLocalBlock());
      DatanodeStorageInfo[] dnStorageInfo = cluster.getNameNode()
          .getNamesystem().getBlockManager().getStorages(bInfo);
      bInfo.removeStorage(dnStorageInfo[1]);
      ByteArrayOutputStream bStream = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(bStream, true);
      assertEquals(0, ToolRunner.run(new DFSck(conf, out), new String[] {
          new Path("/ecDir/ecFile").toString(), "-blockId", id }));
      assertFalse(out.toString().contains("null"));
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

  @Test(expected=IllegalArgumentException.class)
  public void testAddStorageWithReplicatedBlock() {
    DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo(
        "storageID", "127.0.0.1");
    BlockInfo replica = new BlockInfoContiguous(new Block(1000L), (short) 3);
    info.addStorage(storage, replica);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testAddStorageWithDifferentBlockGroup() {
    DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo(
        "storageID", "127.0.0.1");
    BlockInfo diffGroup = new BlockInfoStriped(new Block(BASE_ID + 100),
        testECPolicy);
    info.addStorage(storage, diffGroup);
  }
}
