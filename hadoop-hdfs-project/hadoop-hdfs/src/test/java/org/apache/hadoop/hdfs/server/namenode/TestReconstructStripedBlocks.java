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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand.BlockECReconstructionInfo;

import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.List;

import static org.apache.hadoop.hdfs.StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.NUM_DATA_BLOCKS;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.NUM_PARITY_BLOCKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestReconstructStripedBlocks {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestReconstructStripedBlocks.class);
  private static final int cellSize = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private final short GROUP_SIZE =
      (short) (NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS);

  private MiniDFSCluster cluster;
  private final Path dirPath = new Path("/dir");
  private Path filePath = new Path(dirPath, "file");
  private int maxReplicationStreams =
      DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT;

  private void initConf(Configuration conf) {
    // Large value to make sure the pending replication request can stay in
    // DatanodeDescriptor.replicateBlocks before test timeout.
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 100);
    // Make sure BlockManager can pull all blocks from UnderReplicatedBlocks via
    // chooseUnderReplicatedBlocks at once.
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION, 5);
  }

  @Test
  public void testMissingStripedBlock() throws Exception {
    doTestMissingStripedBlock(1, 0);
  }

  @Test
  public void testMissingStripedBlockWithBusyNode() throws Exception {
    for (int i = 1; i <= NUM_PARITY_BLOCKS; i++) {
      doTestMissingStripedBlock(i, 1);
    }
  }

  /**
   * Start GROUP_SIZE + 1 datanodes.
   * Inject striped blocks to first GROUP_SIZE datanodes.
   * Then make numOfBusy datanodes busy, make numOfMissed datanodes missed.
   * Then trigger BlockManager to compute reconstruction works. (so all
   * reconstruction work will be scheduled to the last datanode)
   * Finally, verify the reconstruction work of the last datanode.
   */
  private void doTestMissingStripedBlock(int numOfMissed, int numOfBusy)
      throws Exception {
    Configuration conf = new HdfsConfiguration();
    initConf(conf);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(GROUP_SIZE + 1)
        .build();

    try {
      cluster.waitActive();
      final int numBlocks = 4;
      DFSTestUtil.createStripedFile(cluster, filePath,
          dirPath, numBlocks, 1, true);
      // all blocks will be located at first GROUP_SIZE DNs, the last DN is
      // empty because of the util function createStripedFile

      // make sure the file is complete in NN
      final INodeFile fileNode = cluster.getNamesystem().getFSDirectory()
          .getINode4Write(filePath.toString()).asFile();
      assertFalse(fileNode.isUnderConstruction());
      assertTrue(fileNode.isStriped());
      BlockInfo[] blocks = fileNode.getBlocks();
      assertEquals(numBlocks, blocks.length);
      for (BlockInfo blk : blocks) {
        assertTrue(blk.isStriped());
        assertTrue(blk.isComplete());
        assertEquals(BLOCK_STRIPED_CELL_SIZE * NUM_DATA_BLOCKS,
            blk.getNumBytes());
        final BlockInfoStriped sb = (BlockInfoStriped) blk;
        assertEquals(GROUP_SIZE, sb.numNodes());
      }

      final BlockManager bm = cluster.getNamesystem().getBlockManager();
      BlockInfo firstBlock = fileNode.getBlocks()[0];
      DatanodeStorageInfo[] storageInfos = bm.getStorages(firstBlock);

      // make numOfBusy nodes busy
      int i = 0;
      for (; i < numOfBusy; i++) {
        DatanodeDescriptor busyNode = storageInfos[i].getDatanodeDescriptor();
        for (int j = 0; j < maxReplicationStreams + 1; j++) {
          BlockManagerTestUtil.addBlockToBeReplicated(busyNode, new Block(j),
              new DatanodeStorageInfo[]{storageInfos[0]});
        }
      }

      // make numOfMissed internal blocks missed
      for (; i < numOfBusy + numOfMissed; i++) {
        DatanodeDescriptor missedNode = storageInfos[i].getDatanodeDescriptor();
        assertEquals(numBlocks, missedNode.numBlocks());
        bm.getDatanodeManager().removeDatanode(missedNode);
      }

      BlockManagerTestUtil.getComputedDatanodeWork(bm);

      // all the reconstruction work will be scheduled on the last DN
      DataNode lastDn = cluster.getDataNodes().get(GROUP_SIZE);
      DatanodeDescriptor last =
          bm.getDatanodeManager().getDatanode(lastDn.getDatanodeId());
      assertEquals("Counting the number of outstanding EC tasks", numBlocks,
          last.getNumberOfBlocksToBeErasureCoded());
      List<BlockECReconstructionInfo> reconstruction =
          last.getErasureCodeCommand(numBlocks);
      for (BlockECReconstructionInfo info : reconstruction) {
        assertEquals(1, info.getTargetDnInfos().length);
        assertEquals(last, info.getTargetDnInfos()[0]);
        assertEquals(info.getSourceDnInfos().length,
            info.getLiveBlockIndices().length);
        if (GROUP_SIZE - numOfMissed == NUM_DATA_BLOCKS) {
          // It's a QUEUE_HIGHEST_PRIORITY block, so the busy DNs will be chosen
          // to make sure we have NUM_DATA_BLOCKS DNs to do reconstruction
          // work.
          assertEquals(NUM_DATA_BLOCKS, info.getSourceDnInfos().length);
        } else {
          // The block has no highest priority, so we don't use the busy DNs as
          // sources
          assertEquals(GROUP_SIZE - numOfMissed - numOfBusy,
              info.getSourceDnInfos().length);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void test2RecoveryTasksForSameBlockGroup() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, StripedFileTestUtil.blockSize);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(GROUP_SIZE + 2)
        .build();
    try {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      BlockManager bm = cluster.getNamesystem().getBlockManager();
      fs.getClient().setErasureCodingPolicy("/", null);
      int fileLen = NUM_DATA_BLOCKS * StripedFileTestUtil.blockSize;
      Path p = new Path("/test2RecoveryTasksForSameBlockGroup");
      final byte[] data = new byte[fileLen];
      DFSTestUtil.writeFile(fs, p, data);

      LocatedStripedBlock lb = (LocatedStripedBlock)fs.getClient()
          .getLocatedBlocks(p.toString(), 0).get(0);
      LocatedBlock[] lbs = StripedBlockUtil.parseStripedBlockGroup(lb,
          cellSize, NUM_DATA_BLOCKS, NUM_PARITY_BLOCKS);

      assertEquals(0, getNumberOfBlocksToBeErasureCoded(cluster));
      assertEquals(0, bm.getPendingReconstructionBlocksCount());

      // missing 1 block, so 1 task should be scheduled
      DatanodeInfo dn0 = lbs[0].getLocations()[0];
      cluster.stopDataNode(dn0.getName());
      cluster.setDataNodeDead(dn0);
      BlockManagerTestUtil.getComputedDatanodeWork(bm);
      assertEquals(1, getNumberOfBlocksToBeErasureCoded(cluster));
      assertEquals(1, bm.getPendingReconstructionBlocksCount());

      // missing another block, but no new task should be scheduled because
      // previous task isn't finished.
      DatanodeInfo dn1 = lbs[1].getLocations()[0];
      cluster.stopDataNode(dn1.getName());
      cluster.setDataNodeDead(dn1);
      BlockManagerTestUtil.getComputedDatanodeWork(bm);
      assertEquals(1, getNumberOfBlocksToBeErasureCoded(cluster));
      assertEquals(1, bm.getPendingReconstructionBlocksCount());
    } finally {
      cluster.shutdown();
    }
  }

  private static int getNumberOfBlocksToBeErasureCoded(MiniDFSCluster cluster)
      throws Exception {
    DatanodeManager dm =
        cluster.getNamesystem().getBlockManager().getDatanodeManager();
    int count = 0;
    for( DataNode dn : cluster.getDataNodes()){
      DatanodeDescriptor dd = dm.getDatanode(dn.getDatanodeId());
      count += dd.getNumberOfBlocksToBeErasureCoded();
    }
    return count;
  }

  /**
   * make sure the NN can detect the scenario where there are enough number of
   * internal blocks (>=9 by default) but there is still missing data/parity
   * block.
   */
  @Test
  public void testCountLiveReplicas() throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(GROUP_SIZE + 2)
        .build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();

    try {
      fs.mkdirs(dirPath);
      fs.setErasureCodingPolicy(dirPath, null);
      DFSTestUtil.createFile(fs, filePath,
          BLOCK_STRIPED_CELL_SIZE * NUM_DATA_BLOCKS * 2, (short) 1, 0L);

      // stop a dn
      LocatedBlocks blks = fs.getClient().getLocatedBlocks(filePath.toString(), 0);
      LocatedStripedBlock block = (LocatedStripedBlock) blks.getLastLocatedBlock();
      DatanodeInfo dnToStop = block.getLocations()[0];
      MiniDFSCluster.DataNodeProperties dnProp =
          cluster.stopDataNode(dnToStop.getXferAddr());
      cluster.setDataNodeDead(dnToStop);

      // wait for reconstruction to happen
      DFSTestUtil.waitForReplication(fs, filePath, GROUP_SIZE, 15 * 1000);

      // bring the dn back: 10 internal blocks now
      cluster.restartDataNode(dnProp);
      cluster.waitActive();

      // stop another dn: 9 internal blocks, but only cover 8 real one
      dnToStop = block.getLocations()[1];
      cluster.stopDataNode(dnToStop.getXferAddr());
      cluster.setDataNodeDead(dnToStop);

      // currently namenode is able to track the missing block. but restart NN
      cluster.restartNameNode(true);

      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.triggerBlockReport(dn);
      }

      FSNamesystem fsn = cluster.getNamesystem();
      BlockManager bm = fsn.getBlockManager();

      Thread.sleep(3000); // wait 3 running cycles of replication monitor
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.triggerHeartbeat(dn);
      }

      // check if NN can detect the missing internal block and finish the
      // reconstruction
      boolean reconstructed = false;
      for (int i = 0; i < 5; i++) {
        NumberReplicas num = null;
        fsn.readLock();
        try {
          BlockInfo blockInfo = cluster.getNamesystem().getFSDirectory()
              .getINode4Write(filePath.toString()).asFile().getLastBlock();
          num = bm.countNodes(blockInfo);
        } finally {
          fsn.readUnlock();
        }
        if (num.liveReplicas() >= GROUP_SIZE) {
          reconstructed = true;
          break;
        } else {
          Thread.sleep(1000);
        }
      }
      Assert.assertTrue(reconstructed);

      blks = fs.getClient().getLocatedBlocks(filePath.toString(), 0);
      block = (LocatedStripedBlock) blks.getLastLocatedBlock();
      BitSet bitSet = new BitSet(GROUP_SIZE);
      for (byte index : block.getBlockIndices()) {
        bitSet.set(index);
      }
      for (int i = 0; i < GROUP_SIZE; i++) {
        Assert.assertTrue(bitSet.get(i));
      }
    } finally {
      cluster.shutdown();
    }
  }
}
