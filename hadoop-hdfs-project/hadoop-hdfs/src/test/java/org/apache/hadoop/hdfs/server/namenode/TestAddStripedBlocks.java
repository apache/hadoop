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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStripedUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestAddStripedBlocks {
  private final short GROUP_SIZE = HdfsConstants.NUM_DATA_BLOCKS +
      HdfsConstants.NUM_PARITY_BLOCKS;

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;

  @Before
  public void setup() throws IOException {
    cluster = new MiniDFSCluster.Builder(new HdfsConfiguration())
        .numDataNodes(GROUP_SIZE).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfs.getClient().createErasureCodingZone("/", null);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAddStripedBlock() throws Exception {
    final Path file = new Path("/file1");
    // create an empty file
    FSDataOutputStream out = null;
    try {
      out = dfs.create(file, (short) 1);

      FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
      INodeFile fileNode = fsdir.getINode4Write(file.toString()).asFile();
      LocatedBlock newBlock = cluster.getNamesystem().getAdditionalBlock(
          file.toString(), fileNode.getId(), dfs.getClient().getClientName(),
          null, null, null);
      assertEquals(GROUP_SIZE, newBlock.getLocations().length);
      assertEquals(GROUP_SIZE, newBlock.getStorageIDs().length);

      BlockInfo[] blocks = fileNode.getBlocks();
      assertEquals(1, blocks.length);
      Assert.assertTrue(blocks[0].isStriped());

      checkStripedBlockUC((BlockInfoStriped) fileNode.getLastBlock(), true);
    } finally {
      IOUtils.cleanup(null, out);
    }

    // restart NameNode to check editlog
    cluster.restartNameNode(true);
    FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
    INodeFile fileNode = fsdir.getINode4Write(file.toString()).asFile();
    BlockInfo[] blocks = fileNode.getBlocks();
    assertEquals(1, blocks.length);
    Assert.assertTrue(blocks[0].isStriped());
    checkStripedBlockUC((BlockInfoStriped) fileNode.getLastBlock(), false);

    // save namespace, restart namenode, and check
    dfs = cluster.getFileSystem();
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    dfs.saveNamespace();
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNode(true);
    fsdir = cluster.getNamesystem().getFSDirectory();
    fileNode = fsdir.getINode4Write(file.toString()).asFile();
    blocks = fileNode.getBlocks();
    assertEquals(1, blocks.length);
    Assert.assertTrue(blocks[0].isStriped());
    checkStripedBlockUC((BlockInfoStriped) fileNode.getLastBlock(), false);
  }

  private void checkStripedBlockUC(BlockInfoStriped block,
      boolean checkReplica) {
    assertEquals(0, block.numNodes());
    Assert.assertFalse(block.isComplete());
    Assert.assertEquals(HdfsConstants.NUM_DATA_BLOCKS, block.getDataBlockNum());
    Assert.assertEquals(HdfsConstants.NUM_PARITY_BLOCKS,
        block.getParityBlockNum());
    Assert.assertEquals(0,
        block.getBlockId() & HdfsConstants.BLOCK_GROUP_INDEX_MASK);

    final BlockInfoStripedUnderConstruction blockUC =
        (BlockInfoStripedUnderConstruction) block;
    Assert.assertEquals(HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION,
        blockUC.getBlockUCState());
    if (checkReplica) {
      Assert.assertEquals(GROUP_SIZE, blockUC.getNumExpectedLocations());
      DatanodeStorageInfo[] storages = blockUC.getExpectedStorageLocations();
      for (DataNode dn : cluster.getDataNodes()) {
        Assert.assertTrue(includeDataNode(dn.getDatanodeId(), storages));
      }
    }
  }

  private boolean includeDataNode(DatanodeID dn, DatanodeStorageInfo[] storages) {
    for (DatanodeStorageInfo storage : storages) {
      if (storage.getDatanodeDescriptor().equals(dn)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testGetLocatedStripedBlocks() throws Exception {
    final Path file = new Path("/file1");
    // create an empty file
    FSDataOutputStream out = null;
    try {
      out = dfs.create(file, (short) 1);

      FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
      INodeFile fileNode = fsdir.getINode4Write(file.toString()).asFile();
      cluster.getNamesystem().getAdditionalBlock(file.toString(),
          fileNode.getId(), dfs.getClient().getClientName(), null, null, null);
      BlockInfoStripedUnderConstruction lastBlk =
          (BlockInfoStripedUnderConstruction) fileNode.getLastBlock();
      DatanodeInfo[] expectedDNs = DatanodeStorageInfo
          .toDatanodeInfos(lastBlk.getExpectedStorageLocations());
      int[] indices = lastBlk.getBlockIndices();

      LocatedBlocks blks = dfs.getClient().getLocatedBlocks(file.toString(), 0L);
      Assert.assertEquals(1, blks.locatedBlockCount());
      LocatedBlock lblk = blks.get(0);

      Assert.assertTrue(lblk instanceof LocatedStripedBlock);
      DatanodeInfo[] datanodes = lblk.getLocations();
      int[] blockIndices = ((LocatedStripedBlock) lblk).getBlockIndices();
      Assert.assertEquals(GROUP_SIZE, datanodes.length);
      Assert.assertEquals(GROUP_SIZE, blockIndices.length);
      Assert.assertArrayEquals(indices, blockIndices);
      Assert.assertArrayEquals(expectedDNs, datanodes);
    } finally {
      IOUtils.cleanup(null, out);
    }
  }

  /**
   * Test BlockInfoStripedUnderConstruction#addReplicaIfNotPresent in different
   * scenarios.
   */
  @Test
  public void testAddUCReplica() throws Exception {
    final Path file = new Path("/file1");
    final List<String> storageIDs = new ArrayList<>();
    // create an empty file
    FSDataOutputStream out = null;
    try {
      out = dfs.create(file, (short) 1);

      // 1. create the UC striped block
      FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
      INodeFile fileNode = fsdir.getINode4Write(file.toString()).asFile();
      cluster.getNamesystem().getAdditionalBlock(file.toString(),
          fileNode.getId(), dfs.getClient().getClientName(), null, null, null);
      BlockInfo lastBlock = fileNode.getLastBlock();
      BlockInfoStripedUnderConstruction ucBlock =
          (BlockInfoStripedUnderConstruction) lastBlock;

      DatanodeStorageInfo[] locs = ucBlock.getExpectedStorageLocations();
      int[] indices = ucBlock.getBlockIndices();
      Assert.assertEquals(GROUP_SIZE, locs.length);
      Assert.assertEquals(GROUP_SIZE, indices.length);

      // 2. mimic incremental block reports and make sure the uc-replica list in
      // the BlockStripedUC is correct
      int i = 0;
      for (DataNode dn : cluster.getDataNodes()) {
        final Block block = new Block(lastBlock.getBlockId() + i++,
            0, lastBlock.getGenerationStamp());
        DatanodeStorage storage = new DatanodeStorage(UUID.randomUUID().toString());
        storageIDs.add(storage.getStorageID());
        StorageReceivedDeletedBlocks[] reports = DFSTestUtil
            .makeReportForReceivedBlock(block, BlockStatus.RECEIVING_BLOCK,
                storage);
        for (StorageReceivedDeletedBlocks report : reports) {
          cluster.getNamesystem().processIncrementalBlockReport(
              dn.getDatanodeId(), report);
        }
      }

      // make sure lastBlock is correct and the storages have been updated
      locs = ucBlock.getExpectedStorageLocations();
      indices = ucBlock.getBlockIndices();
      Assert.assertEquals(GROUP_SIZE, locs.length);
      Assert.assertEquals(GROUP_SIZE, indices.length);
      for (DatanodeStorageInfo newstorage : locs) {
        Assert.assertTrue(storageIDs.contains(newstorage.getStorageID()));
      }
    } finally {
      IOUtils.cleanup(null, out);
    }

    // 3. restart the namenode. mimic the full block reports and check the
    // uc-replica list again
    cluster.restartNameNode(true);
    final String bpId = cluster.getNamesystem().getBlockPoolId();
    INodeFile fileNode = cluster.getNamesystem().getFSDirectory()
        .getINode4Write(file.toString()).asFile();
    BlockInfo lastBlock = fileNode.getLastBlock();
    int i = GROUP_SIZE - 1;
    for (DataNode dn : cluster.getDataNodes()) {
      String storageID = storageIDs.get(i);
      final Block block = new Block(lastBlock.getBlockId() + i--,
          lastBlock.getGenerationStamp(), 0);
      DatanodeStorage storage = new DatanodeStorage(storageID);
      List<ReplicaBeingWritten> blocks = new ArrayList<>();
      ReplicaBeingWritten replica = new ReplicaBeingWritten(block, null, null,
          null);
      blocks.add(replica);
      BlockListAsLongs bll = BlockListAsLongs.encode(blocks);
      StorageBlockReport[] reports = {new StorageBlockReport(storage,
          bll)};
      cluster.getNameNodeRpc().blockReport(dn.getDNRegistrationForBP(bpId),
          bpId, reports, null);
    }

    BlockInfoStripedUnderConstruction ucBlock =
        (BlockInfoStripedUnderConstruction) lastBlock;
    DatanodeStorageInfo[] locs = ucBlock.getExpectedStorageLocations();
    int[] indices = ucBlock.getBlockIndices();
    Assert.assertEquals(GROUP_SIZE, locs.length);
    Assert.assertEquals(GROUP_SIZE, indices.length);
    for (i = 0; i < GROUP_SIZE; i++) {
      Assert.assertEquals(storageIDs.get(i),
          locs[GROUP_SIZE - 1 - i].getStorageID());
      Assert.assertEquals(GROUP_SIZE - i - 1, indices[i]);
    }
  }
}
