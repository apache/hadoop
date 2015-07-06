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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.BlockECRecoveryCommand.BlockECRecoveryInfo;
import org.junit.Test;
import java.util.List;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.NUM_DATA_BLOCKS;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.NUM_PARITY_BLOCKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestRecoverStripedBlocks {
  private final short GROUP_SIZE =
      NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS;
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
  public void testMissingStripedBlockWithBusyNode1() throws Exception {
    doTestMissingStripedBlock(2, 1);
  }

  @Test
  public void testMissingStripedBlockWithBusyNode2() throws Exception {
    doTestMissingStripedBlock(3, 1);
  }

  /**
   * Start GROUP_SIZE + 1 datanodes.
   * Inject striped blocks to first GROUP_SIZE datanodes.
   * Then make numOfBusy datanodes busy, make numOfMissed datanodes missed.
   * Then trigger BlockManager to compute recovery works. (so all recovery work
   * will be scheduled to the last datanode)
   * Finally, verify the recovery work of the last datanode.
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

      // all the recovery work will be scheduled on the last DN
      DataNode lastDn = cluster.getDataNodes().get(GROUP_SIZE);
      DatanodeDescriptor last =
          bm.getDatanodeManager().getDatanode(lastDn.getDatanodeId());
      assertEquals("Counting the number of outstanding EC tasks", numBlocks,
          last.getNumberOfBlocksToBeErasureCoded());
      List<BlockECRecoveryInfo> recovery =
          last.getErasureCodeCommand(numBlocks);
      for (BlockECRecoveryInfo info : recovery) {
        assertEquals(1, info.getTargetDnInfos().length);
        assertEquals(last, info.getTargetDnInfos()[0]);
        assertEquals(info.getSourceDnInfos().length,
            info.getLiveBlockIndices().length);
        if (GROUP_SIZE - numOfMissed == NUM_DATA_BLOCKS) {
          // It's a QUEUE_HIGHEST_PRIORITY block, so the busy DNs will be chosen
          // to make sure we have NUM_DATA_BLOCKS DNs to do recovery work.
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
}
