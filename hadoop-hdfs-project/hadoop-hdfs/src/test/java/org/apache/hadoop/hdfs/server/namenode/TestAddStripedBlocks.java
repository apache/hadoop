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
import org.apache.hadoop.hdfs.DFSStripedOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.junit.Assert.assertEquals;

public class TestAddStripedBlocks {
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final short dataBlocks = (short) ecPolicy.getNumDataUnits();
  private final short parityBlocks = (short) ecPolicy.getNumParityUnits();
  private final int cellSize = ecPolicy.getCellSize();
  private final short groupSize = (short) (ecPolicy.getNumDataUnits() +
      ecPolicy.getNumParityUnits());

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(groupSize).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfs.enableErasureCodingPolicy(ecPolicy.getName());
    dfs.getClient().setErasureCodingPolicy("/", ecPolicy.getName());
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Check if the scheduled block size on each DN storage is correctly updated
   */
  @Test
  public void testBlockScheduledUpdate() throws Exception {
    final FSNamesystem fsn = cluster.getNamesystem();
    final Path foo = new Path("/foo");
    try (FSDataOutputStream out = dfs.create(foo, true)) {
      DFSStripedOutputStream sout = (DFSStripedOutputStream) out.getWrappedStream();
      writeAndFlushStripedOutputStream(sout, DFS_BYTES_PER_CHECKSUM_DEFAULT);

      // make sure the scheduled block size has been updated for each DN storage
      // in NN
      final List<DatanodeDescriptor> dnList = new ArrayList<>();
      fsn.getBlockManager().getDatanodeManager().fetchDatanodes(dnList, null, false);
      for (DatanodeDescriptor dn : dnList) {
        Assert.assertEquals(1, dn.getBlocksScheduled());
      }
    }

    // we have completed the file, force the DN to flush IBR
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.triggerBlockReport(dn);
    }

    // check the scheduled block size again
    final List<DatanodeDescriptor> dnList = new ArrayList<>();
    fsn.getBlockManager().getDatanodeManager().fetchDatanodes(dnList, null, false);
    for (DatanodeDescriptor dn : dnList) {
      Assert.assertEquals(0, dn.getBlocksScheduled());
    }
  }

  /**
   * Make sure the IDs of striped blocks do not conflict
   */
  @Test
  public void testAllocateBlockId() throws Exception {
    Path testPath = new Path("/testfile");
    // create a file while allocates a new block
    DFSTestUtil.writeFile(dfs, testPath, "hello, world!");
    LocatedBlocks lb = dfs.getClient().getLocatedBlocks(testPath.toString(), 0);
    final long firstId = lb.get(0).getBlock().getBlockId();
    // delete the file
    dfs.delete(testPath, true);

    // allocate a new block, and make sure the new block's id does not conflict
    // with the previous one
    DFSTestUtil.writeFile(dfs, testPath, "hello again");
    lb = dfs.getClient().getLocatedBlocks(testPath.toString(), 0);
    final long secondId = lb.get(0).getBlock().getBlockId();
    Assert.assertEquals(firstId + HdfsServerConstants.MAX_BLOCKS_IN_GROUP, secondId);
  }

  private static void writeAndFlushStripedOutputStream(
      DFSStripedOutputStream out, int chunkSize) throws IOException {
    // FSOutputSummer.BUFFER_NUM_CHUNKS == 9
    byte[] toWrite = new byte[chunkSize * 9 + 1];
    out.write(toWrite);
    DFSTestUtil.flushInternal(out);
  }

  @Test (timeout=60000)
  public void testAddStripedBlock() throws Exception {
    final Path file = new Path("/file1");
    // create an empty file
    FSDataOutputStream out = null;
    try {
      out = dfs.create(file, (short) 1);
      writeAndFlushStripedOutputStream(
          (DFSStripedOutputStream) out.getWrappedStream(),
          DFS_BYTES_PER_CHECKSUM_DEFAULT);

      FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
      INodeFile fileNode = fsdir.getINode4Write(file.toString()).asFile();

      BlockInfo[] blocks = fileNode.getBlocks();
      assertEquals(1, blocks.length);
      Assert.assertTrue(blocks[0].isStriped());

      checkStripedBlockUC((BlockInfoStriped) fileNode.getLastBlock(), true);

      // restart NameNode to check editlog
      cluster.restartNameNode(true);
      fsdir = cluster.getNamesystem().getFSDirectory();
      fileNode = fsdir.getINode4Write(file.toString()).asFile();
      blocks = fileNode.getBlocks();
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
    } finally {
      IOUtils.cleanup(null, out);
    }
  }

  private void checkStripedBlockUC(BlockInfoStriped block,
      boolean checkReplica) {
    assertEquals(0, block.numNodes());
    Assert.assertFalse(block.isComplete());
    Assert.assertEquals(dataBlocks, block.getDataBlockNum());
    Assert.assertEquals(parityBlocks, block.getParityBlockNum());
    Assert.assertEquals(0,
        block.getBlockId() & HdfsServerConstants.BLOCK_GROUP_INDEX_MASK);

    Assert.assertEquals(HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION,
        block.getBlockUCState());
    if (checkReplica) {
      Assert.assertEquals(groupSize,
          block.getUnderConstructionFeature().getNumExpectedLocations());
      DatanodeStorageInfo[] storages = block.getUnderConstructionFeature()
          .getExpectedStorageLocations();
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
      writeAndFlushStripedOutputStream(
          (DFSStripedOutputStream) out.getWrappedStream(),
          DFS_BYTES_PER_CHECKSUM_DEFAULT);

      FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
      INodeFile fileNode = fsdir.getINode4Write(file.toString()).asFile();
      BlockInfoStriped lastBlk = (BlockInfoStriped) fileNode.getLastBlock();
      DatanodeInfo[] expectedDNs = DatanodeStorageInfo.toDatanodeInfos(
          lastBlk.getUnderConstructionFeature().getExpectedStorageLocations());
      byte[] indices = lastBlk.getUnderConstructionFeature().getBlockIndices();

      LocatedBlocks blks = dfs.getClient().getLocatedBlocks(file.toString(), 0L);
      Assert.assertEquals(1, blks.locatedBlockCount());
      LocatedBlock lblk = blks.get(0);

      Assert.assertTrue(lblk instanceof LocatedStripedBlock);
      DatanodeInfo[] datanodes = lblk.getLocations();
      byte[] blockIndices = ((LocatedStripedBlock) lblk).getBlockIndices();
      Assert.assertEquals(groupSize, datanodes.length);
      Assert.assertEquals(groupSize, blockIndices.length);
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
          fileNode.getId(), dfs.getClient().getClientName(), null, null, null, null);
      BlockInfo lastBlock = fileNode.getLastBlock();

      DatanodeStorageInfo[] locs = lastBlock.getUnderConstructionFeature()
          .getExpectedStorageLocations();
      byte[] indices = lastBlock.getUnderConstructionFeature().getBlockIndices();
      Assert.assertEquals(groupSize, locs.length);
      Assert.assertEquals(groupSize, indices.length);

      // 2. mimic incremental block reports and make sure the uc-replica list in
      // the BlockInfoUCStriped is correct
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
      locs = lastBlock.getUnderConstructionFeature().getExpectedStorageLocations();
      indices = lastBlock.getUnderConstructionFeature().getBlockIndices();
      Assert.assertEquals(groupSize, locs.length);
      Assert.assertEquals(groupSize, indices.length);
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
    int i = groupSize - 1;
    for (DataNode dn : cluster.getDataNodes()) {
      String storageID = storageIDs.get(i);
      final Block block = new Block(lastBlock.getBlockId() + i--, 0,
          lastBlock.getGenerationStamp());
      DatanodeStorage storage = new DatanodeStorage(storageID);
      List<ReplicaBeingWritten> blocks = new ArrayList<>();
      ReplicaBeingWritten replica = new ReplicaBeingWritten(block, null, null,
          null);
      blocks.add(replica);
      BlockListAsLongs bll = BlockListAsLongs.encode(blocks);
      StorageBlockReport[] reports = {new StorageBlockReport(storage,
          bll)};
      cluster.getNameNodeRpc().blockReport(dn.getDNRegistrationForBP(bpId),
          bpId, reports,
          new BlockReportContext(1, 0, System.nanoTime(), 0, true));
    }

    DatanodeStorageInfo[] locs = lastBlock.getUnderConstructionFeature()
        .getExpectedStorageLocations();
    byte[] indices = lastBlock.getUnderConstructionFeature().getBlockIndices();
    Assert.assertEquals(groupSize, locs.length);
    Assert.assertEquals(groupSize, indices.length);
    for (i = 0; i < groupSize; i++) {
      Assert.assertEquals(storageIDs.get(i),
          locs[groupSize - 1 - i].getStorageID());
      Assert.assertEquals(groupSize - i - 1, indices[i]);
    }
  }

  @Test
  public void testCheckStripedReplicaCorrupt() throws Exception {
    final int numBlocks = 4;
    final int numStripes = 4;
    final Path filePath = new Path("/corrupt");
    final FSNamesystem ns = cluster.getNameNode().getNamesystem();
    final BlockManager bm = ns.getBlockManager();
    DFSTestUtil.createStripedFile(cluster, filePath, null,
        numBlocks, numStripes, false);

    INodeFile fileNode = ns.getFSDirectory().getINode(filePath.toString()).
        asFile();
    Assert.assertTrue(fileNode.isStriped());
    BlockInfo stored = fileNode.getBlocks()[0];
    BlockManagerTestUtil.updateState(ns.getBlockManager());
    Assert.assertEquals(0, ns.getCorruptReplicaBlocks());

    // Now send a block report with correct size
    DatanodeStorage storage = new DatanodeStorage(UUID.randomUUID().toString());
    final Block reported = new Block(stored);
    reported.setNumBytes(numStripes * cellSize);
    StorageReceivedDeletedBlocks[] reports = DFSTestUtil
        .makeReportForReceivedBlock(reported,
            ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, storage);
    ns.processIncrementalBlockReport(
        cluster.getDataNodes().get(0).getDatanodeId(), reports[0]);
    BlockManagerTestUtil.updateState(ns.getBlockManager());
    Assert.assertEquals(0, ns.getCorruptReplicaBlocks());

    // Now send a block report with wrong size
    reported.setBlockId(stored.getBlockId() + 1);
    reported.setNumBytes(numStripes * cellSize - 1);
    reports = DFSTestUtil.makeReportForReceivedBlock(reported,
            ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, storage);
    ns.processIncrementalBlockReport(
        cluster.getDataNodes().get(1).getDatanodeId(), reports[0]);
    BlockManagerTestUtil.updateState(ns.getBlockManager());
    Assert.assertEquals(1, ns.getCorruptReplicaBlocks());

    // Now send a parity block report with correct size
    reported.setBlockId(stored.getBlockId() + dataBlocks);
    reported.setNumBytes(numStripes * cellSize);
    reports = DFSTestUtil.makeReportForReceivedBlock(reported,
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, storage);
    ns.processIncrementalBlockReport(
        cluster.getDataNodes().get(2).getDatanodeId(), reports[0]);
    BlockManagerTestUtil.updateState(ns.getBlockManager());
    Assert.assertEquals(1, ns.getCorruptReplicaBlocks());

    // Now send a parity block report with wrong size
    reported.setBlockId(stored.getBlockId() + dataBlocks);
    reported.setNumBytes(numStripes * cellSize + 1);
    reports = DFSTestUtil.makeReportForReceivedBlock(reported,
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, storage);
    ns.processIncrementalBlockReport(
        cluster.getDataNodes().get(3).getDatanodeId(), reports[0]);
    BlockManagerTestUtil.updateState(ns.getBlockManager());
    // the total number of corrupted block info is still 1
    Assert.assertEquals(1, ns.getCorruptECBlockGroups());
    Assert.assertEquals(1, ns.getCorruptReplicaBlocks());
    Assert.assertEquals(0, ns.getCorruptReplicatedBlocks());
    // 2 internal blocks corrupted
    Assert.assertEquals(2, bm.getCorruptReplicas(stored).size());

    // Now change the size of stored block, and test verifying the last
    // block size
    stored.setNumBytes(stored.getNumBytes() + 10);
    reported.setBlockId(stored.getBlockId() + dataBlocks + 2);
    reported.setNumBytes(numStripes * cellSize);
    reports = DFSTestUtil.makeReportForReceivedBlock(reported,
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, storage);
    ns.processIncrementalBlockReport(
        cluster.getDataNodes().get(4).getDatanodeId(), reports[0]);
    BlockManagerTestUtil.updateState(ns.getBlockManager());
    Assert.assertEquals(1, ns.getCorruptReplicaBlocks());
    Assert.assertEquals(3, bm.getCorruptReplicas(stored).size());

    // Now send a parity block report with correct size based on adjusted
    // size of stored block
    /** Now stored block has {@link numStripes} full stripes + a cell + 10 */
    stored.setNumBytes(stored.getNumBytes() + cellSize);
    reported.setBlockId(stored.getBlockId());
    reported.setNumBytes((numStripes + 1) * cellSize);
    reports = DFSTestUtil.makeReportForReceivedBlock(reported,
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, storage);
    ns.processIncrementalBlockReport(
        cluster.getDataNodes().get(0).getDatanodeId(), reports[0]);
    BlockManagerTestUtil.updateState(ns.getBlockManager());
    Assert.assertEquals(1, ns.getCorruptReplicaBlocks());
    Assert.assertEquals(3, bm.getCorruptReplicas(stored).size());

    reported.setBlockId(stored.getBlockId() + 1);
    reported.setNumBytes(numStripes * cellSize + 10);
    reports = DFSTestUtil.makeReportForReceivedBlock(reported,
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, storage);
    ns.processIncrementalBlockReport(
        cluster.getDataNodes().get(0).getDatanodeId(), reports[0]);
    BlockManagerTestUtil.updateState(ns.getBlockManager());
    Assert.assertEquals(1, ns.getCorruptReplicaBlocks());
    Assert.assertEquals(3, bm.getCorruptReplicas(stored).size());

    reported.setBlockId(stored.getBlockId() + dataBlocks);
    reported.setNumBytes((numStripes + 1) * cellSize);
    reports = DFSTestUtil.makeReportForReceivedBlock(reported,
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, storage);
    ns.processIncrementalBlockReport(
        cluster.getDataNodes().get(2).getDatanodeId(), reports[0]);
    BlockManagerTestUtil.updateState(ns.getBlockManager());
    Assert.assertEquals(1, ns.getCorruptReplicaBlocks());
    Assert.assertEquals(3, bm.getCorruptReplicas(stored).size());
  }

}
