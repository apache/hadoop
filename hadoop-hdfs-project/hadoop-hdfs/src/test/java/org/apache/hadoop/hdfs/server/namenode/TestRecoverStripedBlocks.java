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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockECRecoveryInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.BLOCK_STRIPED_CHUNK_SIZE;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.EC_STORAGE_POLICY_NAME;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.NUM_DATA_BLOCKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestRecoverStripedBlocks {
  private final short GROUP_SIZE =
      NUM_DATA_BLOCKS + HdfsConstants.NUM_PARITY_BLOCKS;
  private MiniDFSCluster cluster;
  private final Path dirPath = new Path("/dir");
  private Path filePath = new Path(dirPath, "file");

  @Before
  public void setup() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    // Large value to make sure the pending replication request can stay in
    // DatanodeDescriptor.replicateBlocks before test timeout.
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 100);
    // Make sure BlockManager can pull all blocks from UnderReplicatedBlocks via
    // chooseUnderReplicatedBlocks at once.
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION, 5);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(GROUP_SIZE + 1)
        .build();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public static void createECFile(MiniDFSCluster cluster, Path file, Path dir,
      int numBlocks) throws Exception {
    DistributedFileSystem dfs = cluster.getFileSystem();
    dfs.mkdirs(dir);
    dfs.setStoragePolicy(dir, EC_STORAGE_POLICY_NAME);

    FSDataOutputStream out = null;
    try {
      out = dfs.create(file, (short) 1); // create an empty file

      FSNamesystem ns = cluster.getNamesystem();
      FSDirectory fsdir = ns.getFSDirectory();
      INodeFile fileNode = fsdir.getINode4Write(file.toString()).asFile();

      ExtendedBlock previous = null;
      for (int i = 0; i < numBlocks; i++) {
        Block newBlock = createBlock(cluster.getDataNodes(), ns,
            file.toString(), fileNode, dfs.getClient().getClientName(),
            previous);
        previous = new ExtendedBlock(ns.getBlockPoolId(), newBlock);
      }

      ns.completeFile(file.toString(), dfs.getClient().getClientName(),
          previous, fileNode.getId());
    } finally {
      IOUtils.cleanup(null, out);
    }
  }

  static Block createBlock(List<DataNode> dataNodes, FSNamesystem ns,
      String file, INodeFile fileNode, String clientName,
      ExtendedBlock previous) throws Exception {
    ns.getAdditionalBlock(file, fileNode.getId(), clientName, previous, null,
        null);

    final BlockInfo lastBlock = fileNode.getLastBlock();
    final int groupSize = fileNode.getBlockReplication();
    // 1. RECEIVING_BLOCK IBR
    int i = 0;
    for (DataNode dn : dataNodes) {
      if (i < groupSize) {
        final Block block = new Block(lastBlock.getBlockId() + i++, 0,
            lastBlock.getGenerationStamp());
        DatanodeStorage storage = new DatanodeStorage(UUID.randomUUID().toString());
        StorageReceivedDeletedBlocks[] reports = DFSTestUtil
            .makeReportForReceivedBlock(block,
                ReceivedDeletedBlockInfo.BlockStatus.RECEIVING_BLOCK, storage);
        for (StorageReceivedDeletedBlocks report : reports) {
          ns.processIncrementalBlockReport(dn.getDatanodeId(), report);
        }
      }
    }

    // 2. RECEIVED_BLOCK IBR
    i = 0;
    for (DataNode dn : dataNodes) {
      if (i < groupSize) {
        final Block block = new Block(lastBlock.getBlockId() + i++,
            BLOCK_STRIPED_CHUNK_SIZE, lastBlock.getGenerationStamp());
        DatanodeStorage storage = new DatanodeStorage(UUID.randomUUID().toString());
        StorageReceivedDeletedBlocks[] reports = DFSTestUtil
            .makeReportForReceivedBlock(block,
                ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, storage);
        for (StorageReceivedDeletedBlocks report : reports) {
          ns.processIncrementalBlockReport(dn.getDatanodeId(), report);
        }
      }
    }

    lastBlock.setNumBytes(BLOCK_STRIPED_CHUNK_SIZE * NUM_DATA_BLOCKS);
    return lastBlock;
  }

  @Test
  public void testMissingStripedBlock() throws Exception {
    final int numBlocks = 4;
    createECFile(cluster, filePath, dirPath, numBlocks);

    // make sure the file is complete in NN
    final INodeFile fileNode = cluster.getNamesystem().getFSDirectory()
        .getINode4Write(filePath.toString()).asFile();
    assertFalse(fileNode.isUnderConstruction());
    assertTrue(fileNode.isWithStripedBlocks());
    BlockInfo[] blocks = fileNode.getBlocks();
    assertEquals(numBlocks, blocks.length);
    for (BlockInfo blk : blocks) {
      assertTrue(blk.isStriped());
      assertTrue(blk.isComplete());
      assertEquals(BLOCK_STRIPED_CHUNK_SIZE * NUM_DATA_BLOCKS, blk.getNumBytes());
      final BlockInfoStriped sb = (BlockInfoStriped) blk;
      assertEquals(GROUP_SIZE, sb.numNodes());
    }

    final BlockManager bm = cluster.getNamesystem().getBlockManager();
    BlockInfo firstBlock = fileNode.getBlocks()[0];
    DatanodeStorageInfo[] storageInfos = bm.getStorages(firstBlock);

    DatanodeDescriptor secondDn = storageInfos[1].getDatanodeDescriptor();
    assertEquals(numBlocks, secondDn.numBlocks());

    bm.getDatanodeManager().removeDatanode(secondDn);

    BlockManagerTestUtil.getComputedDatanodeWork(bm);

    // all the recovery work will be scheduled on the last DN
    DataNode lastDn = cluster.getDataNodes().get(GROUP_SIZE);
    DatanodeDescriptor last =
          bm.getDatanodeManager().getDatanode(lastDn.getDatanodeId());
    assertEquals("Counting the number of outstanding EC tasks", numBlocks,
        last.getNumberOfBlocksToBeErasureCoded());
    List<BlockECRecoveryInfo> recovery = last.getErasureCodeCommand(numBlocks);
    for (BlockECRecoveryInfo info : recovery) {
      assertEquals(1, info.targets.length);
      assertEquals(last, info.targets[0].getDatanodeDescriptor());
      assertEquals(GROUP_SIZE - 1, info.sources.length);
      assertEquals(GROUP_SIZE - 1, info.liveBlockIndices.length);
    }
  }
}
