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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAddOverReplicatedStripedBlocks {

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private final Path dirPath = new Path("/striped");
  private Path filePath = new Path(dirPath, "file");
  private final short DATA_BLK_NUM = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private final short PARITY_BLK_NUM = StripedFileTestUtil.NUM_PARITY_BLOCKS;
  private final short GROUP_SIZE = (short) (DATA_BLK_NUM + PARITY_BLK_NUM);
  private final int CELLSIZE = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private final int NUM_STRIPE_PER_BLOCK = 4;
  private final int BLOCK_SIZE = NUM_STRIPE_PER_BLOCK * CELLSIZE;
  private final int numDNs = GROUP_SIZE + 3;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    // disable block recovery
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    SimulatedFSDataset.setFactory(conf);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    fs.mkdirs(dirPath);
    fs.getClient().setErasureCodingPolicy(dirPath.toString(), null);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testProcessOverReplicatedStripedBlock() throws Exception {
    // create a file which has exact one block group to the first GROUP_SIZE DNs
    long fileLen = DATA_BLK_NUM * BLOCK_SIZE;
    DFSTestUtil.createStripedFile(cluster, filePath, null, 1,
        NUM_STRIPE_PER_BLOCK, false);
    LocatedBlocks lbs = cluster.getNameNodeRpc().getBlockLocations(
        filePath.toString(), 0, fileLen);
    LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(0));
    long gs = bg.getBlock().getGenerationStamp();
    String bpid = bg.getBlock().getBlockPoolId();
    long groupId = bg.getBlock().getBlockId();
    Block blk = new Block(groupId, BLOCK_SIZE, gs);
    for (int i = 0; i < GROUP_SIZE; i++) {
      blk.setBlockId(groupId + i);
      cluster.injectBlocks(i, Arrays.asList(blk), bpid);
    }
    cluster.triggerBlockReports();

    // let a internal block be over replicated with 2 redundant blocks.
    blk.setBlockId(groupId);
    cluster.injectBlocks(numDNs - 3, Arrays.asList(blk), bpid);
    cluster.injectBlocks(numDNs - 2, Arrays.asList(blk), bpid);
    // let a internal block be over replicated with 1 redundant block.
    blk.setBlockId(groupId + DATA_BLK_NUM);
    cluster.injectBlocks(numDNs - 1, Arrays.asList(blk), bpid);

    // update blocksMap
    cluster.triggerBlockReports();
    // add to invalidates
    cluster.triggerHeartbeats();
    // datanode delete block
    cluster.triggerHeartbeats();
    // update blocksMap
    cluster.triggerBlockReports();

    // verify that all internal blocks exists
    lbs = cluster.getNameNodeRpc().getBlockLocations(
        filePath.toString(), 0, fileLen);
    StripedFileTestUtil.verifyLocatedStripedBlocks(lbs, GROUP_SIZE);
  }

  @Test
  public void testProcessOverReplicatedSBSmallerThanFullBlocks()
      throws Exception {
    // Create a EC file which doesn't fill full internal blocks.
    int fileLen = CELLSIZE * (DATA_BLK_NUM - 1);
    byte[] content = new byte[fileLen];
    DFSTestUtil.writeFile(fs, filePath, new String(content));
    LocatedBlocks lbs = cluster.getNameNodeRpc().getBlockLocations(
        filePath.toString(), 0, fileLen);
    LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(0));
    long gs = bg.getBlock().getGenerationStamp();
    String bpid = bg.getBlock().getBlockPoolId();
    long groupId = bg.getBlock().getBlockId();
    Block blk = new Block(groupId, BLOCK_SIZE, gs);
    cluster.triggerBlockReports();
    List<DatanodeInfo> infos = Arrays.asList(bg.getLocations());

    // let a internal block be over replicated with (numDNs - GROUP_SIZE + 1)
    // redundant blocks. Therefor number of internal blocks is over GROUP_SIZE.
    blk.setBlockId(groupId);
    List<DataNode> dataNodeList = cluster.getDataNodes();
    for (int i = 0; i < numDNs; i++) {
      if (!infos.contains(dataNodeList.get(i).getDatanodeId())) {
        cluster.injectBlocks(i, Arrays.asList(blk), bpid);
        System.out.println("XXX: inject block into datanode " + i);
      }
    }

    // update blocksMap
    cluster.triggerBlockReports();
    // add to invalidates
    cluster.triggerHeartbeats();
    // datanode delete block
    cluster.triggerHeartbeats();
    // update blocksMap
    cluster.triggerBlockReports();

    // verify that all internal blocks exists
    lbs = cluster.getNameNodeRpc().getBlockLocations(
        filePath.toString(), 0, fileLen);
    StripedFileTestUtil.verifyLocatedStripedBlocks(lbs, GROUP_SIZE - 1);
  }

  @Test
  public void testProcessOverReplicatedAndCorruptStripedBlock()
      throws Exception {
    long fileLen = DATA_BLK_NUM * BLOCK_SIZE;
    DFSTestUtil.createStripedFile(cluster, filePath, null, 1,
        NUM_STRIPE_PER_BLOCK, false);
    LocatedBlocks lbs = cluster.getNameNodeRpc().getBlockLocations(
        filePath.toString(), 0, fileLen);
    LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(0));
    long gs = bg.getBlock().getGenerationStamp();
    String bpid = bg.getBlock().getBlockPoolId();
    long groupId = bg.getBlock().getBlockId();
    Block blk = new Block(groupId, BLOCK_SIZE, gs);
    BlockInfoStriped blockInfo = new BlockInfoStriped(blk,
        ErasureCodingPolicyManager.getSystemDefaultPolicy());
    for (int i = 0; i < GROUP_SIZE; i++) {
      blk.setBlockId(groupId + i);
      cluster.injectBlocks(i, Arrays.asList(blk), bpid);
    }
    cluster.triggerBlockReports();

    // let a internal block be corrupt
    BlockManager bm = cluster.getNamesystem().getBlockManager();
    List<DatanodeInfo> infos = Arrays.asList(bg.getLocations());
    List<String> storages = Arrays.asList(bg.getStorageIDs());
    cluster.getNamesystem().writeLock();
    try {
      bm.findAndMarkBlockAsCorrupt(lbs.getLastLocatedBlock().getBlock(),
          infos.get(0), storages.get(0), "TEST");
    } finally {
      cluster.getNamesystem().writeUnlock();
    }
    assertEquals(1, bm.countNodes(bm.getStoredBlock(blockInfo))
        .corruptReplicas());

    // let a internal block be over replicated with 2 redundant block.
    blk.setBlockId(groupId + 2);
    cluster.injectBlocks(numDNs - 3, Arrays.asList(blk), bpid);
    cluster.injectBlocks(numDNs - 2, Arrays.asList(blk), bpid);

    // update blocksMap
    cluster.triggerBlockReports();

    // verify that all internal blocks exists except b0
    // the redundant internal blocks will not be deleted before the corrupted
    // block gets reconstructed. but since we set
    // DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY to 0, the reconstruction will
    // not happen
    lbs = cluster.getNameNodeRpc().getBlockLocations(filePath.toString(), 0,
        fileLen);
    bg = (LocatedStripedBlock) (lbs.get(0));
    assertEquals(GROUP_SIZE + 1, bg.getBlockIndices().length);
    assertEquals(GROUP_SIZE + 1, bg.getLocations().length);
    BitSet set = new BitSet(GROUP_SIZE);
    for (byte index : bg.getBlockIndices()) {
      set.set(index);
    }
    Assert.assertFalse(set.get(0));
    for (int i = 1; i < GROUP_SIZE; i++) {
      assertTrue(set.get(i));
    }
  }

  @Test
  public void testProcessOverReplicatedAndMissingStripedBlock()
      throws Exception {
    long fileLen = CELLSIZE * DATA_BLK_NUM;
    DFSTestUtil.createStripedFile(cluster, filePath, null, 1,
        NUM_STRIPE_PER_BLOCK, false);
    LocatedBlocks lbs = cluster.getNameNodeRpc().getBlockLocations(
        filePath.toString(), 0, fileLen);
    LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(0));
    long gs = bg.getBlock().getGenerationStamp();
    String bpid = bg.getBlock().getBlockPoolId();
    long groupId = bg.getBlock().getBlockId();
    Block blk = new Block(groupId, BLOCK_SIZE, gs);
    // only inject GROUP_SIZE - 1 blocks, so there is one block missing
    for (int i = 0; i < GROUP_SIZE - 1; i++) {
      blk.setBlockId(groupId + i);
      cluster.injectBlocks(i, Arrays.asList(blk), bpid);
    }
    cluster.triggerBlockReports();

    // let a internal block be over replicated with 2 redundant blocks.
    // Therefor number of internal blocks is over GROUP_SIZE. (5 data blocks +
    // 3 parity blocks  + 2 redundant blocks > GROUP_SIZE)
    blk.setBlockId(groupId + 2);
    cluster.injectBlocks(numDNs - 3, Arrays.asList(blk), bpid);
    cluster.injectBlocks(numDNs - 2, Arrays.asList(blk), bpid);

    // update blocksMap
    cluster.triggerBlockReports();
    Thread.sleep(2000);
    // add to invalidates
    cluster.triggerHeartbeats();
    // datanode delete block
    cluster.triggerHeartbeats();
    // update blocksMap
    cluster.triggerBlockReports();

    // Since one block is missing, then over-replicated blocks will not be
    // deleted until reconstruction happens
    lbs = cluster.getNameNodeRpc().getBlockLocations(filePath.toString(), 0,
        fileLen);
    bg = (LocatedStripedBlock) (lbs.get(0));
    assertEquals(GROUP_SIZE + 1, bg.getBlockIndices().length);
    assertEquals(GROUP_SIZE + 1, bg.getLocations().length);
    BitSet set = new BitSet(GROUP_SIZE);
    for (byte index : bg.getBlockIndices()) {
      set.set(index);
    }
    Assert.assertFalse(set.get(GROUP_SIZE - 1));
    for (int i = 0; i < GROUP_SIZE - 1; i++) {
      assertTrue(set.get(i));
    }
  }

}
