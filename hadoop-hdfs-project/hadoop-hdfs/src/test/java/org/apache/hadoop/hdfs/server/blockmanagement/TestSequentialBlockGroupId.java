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

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BLOCK_GROUP_INDEX_MASK;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.MAX_BLOCKS_IN_GROUP;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.stubbing.Answer;

/**
 * Tests the sequential blockGroup ID generation mechanism and blockGroup ID
 * collision handling.
 */
public class TestSequentialBlockGroupId {
  private static final Log LOG = LogFactory
      .getLog("TestSequentialBlockGroupId");

  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final short REPLICATION = 1;
  private final long SEED = 0;
  private final int dataBlocks = ecPolicy.getNumDataUnits();
  private final int parityBlocks = ecPolicy.getNumParityUnits();
  private final int cellSize = ecPolicy.getCellSize();

  private final int stripesPerBlock = 2;
  private final int blockSize = cellSize * stripesPerBlock;
  private final int numDNs = dataBlocks + parityBlocks + 2;
  private final int blockGrpCount = 4;
  private final int fileLen = blockSize * dataBlocks * blockGrpCount;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private SequentialBlockGroupIdGenerator blockGrpIdGenerator;
  private Path ecDir = new Path("/ecDir");

  @Before
  public void setup() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
    blockGrpIdGenerator = cluster.getNamesystem().getBlockManager()
        .getBlockIdManager().getBlockGroupIdGenerator();
    fs.mkdirs(ecDir);
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/ecDir",
        StripedFileTestUtil.getDefaultECPolicy().getName());
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Test that blockGroup IDs are generating unique value.
   */
  @Test(timeout = 60000)
  public void testBlockGroupIdGeneration() throws IOException {
    long blockGroupIdInitialValue = blockGrpIdGenerator.getCurrentValue();

    // Create a file that is 4 blocks long.
    Path path = new Path(ecDir, "testBlockGrpIdGeneration.dat");
    DFSTestUtil.createFile(fs, path, cellSize, fileLen, blockSize, REPLICATION,
        SEED);
    List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(fs, path);
    assertThat("Wrong BlockGrps", blocks.size(), is(blockGrpCount));

    // initialising the block group generator for verifying the block id
    blockGrpIdGenerator.setCurrentValue(blockGroupIdInitialValue);
    // Ensure that the block IDs are generating unique value.
    for (int i = 0; i < blocks.size(); ++i) {
      blockGrpIdGenerator
          .skipTo((blockGrpIdGenerator.getCurrentValue() & ~BLOCK_GROUP_INDEX_MASK)
              + MAX_BLOCKS_IN_GROUP);
      long nextBlockExpectedId = blockGrpIdGenerator.getCurrentValue();
      long nextBlockGrpId = blocks.get(i).getBlock().getBlockId();
      LOG.info("BlockGrp" + i + " id is " + nextBlockGrpId);
      assertThat("BlockGrpId mismatches!", nextBlockGrpId,
          is(nextBlockExpectedId));
    }

    // verify that the blockGroupId resets on #clear call.
    cluster.getNamesystem().getBlockManager().clear();
    assertThat("BlockGrpId mismatches!", blockGrpIdGenerator.getCurrentValue(),
        is(Long.MIN_VALUE));
  }

  /**
   * Test that collisions in the blockGroup ID space are handled gracefully.
   */
  @Test(timeout = 60000)
  public void testTriggerBlockGroupIdCollision() throws IOException {
    long blockGroupIdInitialValue = blockGrpIdGenerator.getCurrentValue();

    // Create a file with a few blocks to rev up the global block ID
    // counter.
    Path path1 = new Path(ecDir, "testBlockGrpIdCollisionDetection_file1.dat");
    DFSTestUtil.createFile(fs, path1, cellSize, fileLen, blockSize,
        REPLICATION, SEED);
    List<LocatedBlock> blocks1 = DFSTestUtil.getAllBlocks(fs, path1);
    assertThat("Wrong BlockGrps", blocks1.size(), is(blockGrpCount));

    // Rewind the block ID counter in the name system object. This will result
    // in block ID collisions when we try to allocate new blocks.
    blockGrpIdGenerator.setCurrentValue(blockGroupIdInitialValue);

    // Trigger collisions by creating a new file.
    Path path2 = new Path(ecDir, "testBlockGrpIdCollisionDetection_file2.dat");
    DFSTestUtil.createFile(fs, path2, cellSize, fileLen, blockSize,
        REPLICATION, SEED);
    List<LocatedBlock> blocks2 = DFSTestUtil.getAllBlocks(fs, path2);
    assertThat("Wrong BlockGrps", blocks2.size(), is(blockGrpCount));

    // Make sure that file1 and file2 block IDs are different
    for (LocatedBlock locBlock1 : blocks1) {
      long blockId1 = locBlock1.getBlock().getBlockId();
      for (LocatedBlock locBlock2 : blocks2) {
        long blockId2 = locBlock2.getBlock().getBlockId();
        assertThat("BlockGrpId mismatches!", blockId1, is(not(blockId2)));
      }
    }
  }

  /**
   * Test that collisions in the blockGroup ID when the id is occupied by legacy
   * block.
   */
  @Test(timeout = 60000)
  public void testTriggerBlockGroupIdCollisionWithLegacyBlockId()
      throws Exception {
    long blockGroupIdInitialValue = blockGrpIdGenerator.getCurrentValue();
    blockGrpIdGenerator
        .skipTo((blockGrpIdGenerator.getCurrentValue() & ~BLOCK_GROUP_INDEX_MASK)
            + MAX_BLOCKS_IN_GROUP);
    final long curBlockGroupIdValue = blockGrpIdGenerator.getCurrentValue();

    // Creates contiguous block with negative blockId so that it would trigger
    // collision during blockGroup Id generation
    FSNamesystem fsn = cluster.getNamesystem();
    // Replace SequentialBlockIdGenerator with a spy
    SequentialBlockIdGenerator blockIdGenerator = spy(fsn.getBlockManager()
        .getBlockIdManager().getBlockIdGenerator());
    Whitebox.setInternalState(fsn.getBlockManager().getBlockIdManager(),
        "blockIdGenerator", blockIdGenerator);
    SequentialBlockIdGenerator spySequentialBlockIdGenerator = new SequentialBlockIdGenerator(
        null) {
      @Override
      public long nextValue() {
        return curBlockGroupIdValue;
      }
    };
    final Answer<Object> delegator = new GenericTestUtils.DelegateAnswer(
        spySequentialBlockIdGenerator);
    doAnswer(delegator).when(blockIdGenerator).nextValue();

    Path path1 = new Path("/testCollisionWithLegacyBlock_file1.dat");
    DFSTestUtil.createFile(fs, path1, 1024, REPLICATION, SEED);

    List<LocatedBlock> contiguousBlocks = DFSTestUtil.getAllBlocks(fs, path1);
    assertThat(contiguousBlocks.size(), is(1));
    Assert.assertEquals("Unexpected BlockId!", curBlockGroupIdValue,
        contiguousBlocks.get(0).getBlock().getBlockId());

    // Reset back to the initial value to trigger collision
    blockGrpIdGenerator.setCurrentValue(blockGroupIdInitialValue);
    // Trigger collisions by creating a new file.
    Path path2 = new Path(ecDir, "testCollisionWithLegacyBlock_file2.dat");
    DFSTestUtil.createFile(fs, path2, cellSize, fileLen, blockSize,
        REPLICATION, SEED);
    List<LocatedBlock> blocks2 = DFSTestUtil.getAllBlocks(fs, path2);
    assertThat("Wrong BlockGrps", blocks2.size(), is(blockGrpCount));

    // Make sure that file1 and file2 block IDs are different
    for (LocatedBlock locBlock1 : contiguousBlocks) {
      long blockId1 = locBlock1.getBlock().getBlockId();
      for (LocatedBlock locBlock2 : blocks2) {
        long blockId2 = locBlock2.getBlock().getBlockId();
        assertThat("BlockGrpId mismatches!", blockId1, is(not(blockId2)));
      }
    }
  }
}
