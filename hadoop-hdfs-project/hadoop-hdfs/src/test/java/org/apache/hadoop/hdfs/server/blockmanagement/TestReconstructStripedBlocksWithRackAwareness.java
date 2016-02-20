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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdfs.StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.NUM_DATA_BLOCKS;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.NUM_PARITY_BLOCKS;

public class TestReconstructStripedBlocksWithRackAwareness {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestReconstructStripedBlocksWithRackAwareness.class);

  static {
    GenericTestUtils.setLogLevel(BlockPlacementPolicy.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(BlockManager.blockLog, Level.ALL);
  }

  private static final String[] hosts = new String[]{"host1", "host2", "host3",
      "host4", "host5", "host6", "host7", "host8", "host9", "host10"};
  private static final String[] racks = new String[]{"/r1", "/r1", "/r2", "/r2",
      "/r3", "/r3", "/r4", "/r4", "/r5", "/r6"};
  private static final List<String> singleNodeRacks = Arrays.asList("host9", "host10");
  private static final short blockNum = (short) (NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS);

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private FSNamesystem fsn;
  private BlockManager bm;

  @Before
  public void setup() throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);

    cluster = new MiniDFSCluster.Builder(conf).racks(racks).hosts(hosts)
        .numDataNodes(hosts.length).build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    bm = fsn.getBlockManager();

    fs = cluster.getFileSystem();
    fs.setErasureCodingPolicy(new Path("/"), null);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * When there are all the internal blocks available but they are not placed on
   * enough racks, NameNode should avoid normal decoding reconstruction but copy
   * an internal block to a new rack.
   *
   * In this test, we first need to create a scenario that a striped block has
   * all the internal blocks but distributed in <6 racks. Then we check if the
   * replication monitor can correctly schedule the reconstruction work for it.
   *
   * For the 9 internal blocks + 5 racks setup, the test does the following:
   * 1. create a 6 rack cluster with 10 datanodes, where there are 2 racks only
   * containing 1 datanodes each
   * 2. for a striped block with 9 internal blocks, there must be one internal
   * block locating in a single-node rack. find this node and stop it
   * 3. namenode will trigger reconstruction for the block and since the cluster
   * has only 5 racks remaining, after the reconstruction we have 9 internal
   * blocks distributed in 5 racks.
   * 4. we bring the datanode back, now the cluster has 6 racks again
   * 5. let the datanode call reportBadBlock, this will make the namenode to
   * check if the striped block is placed in >= 6 racks, and the namenode will
   * put the block into the under-replicated queue
   * 6. now we can check if the replication monitor works as expected
   */
  @Test
  public void testReconstructForNotEnoughRacks() throws Exception {
    final Path file = new Path("/foo");
    DFSTestUtil.createFile(fs, file,
        BLOCK_STRIPED_CELL_SIZE * NUM_DATA_BLOCKS * 2, (short) 1, 0L);
    Assert.assertEquals(0, bm.numOfUnderReplicatedBlocks());

    final INodeFile fileNode = fsn.getFSDirectory()
        .getINode4Write(file.toString()).asFile();
    BlockInfoStriped blockInfo = (BlockInfoStriped) fileNode.getLastBlock();

    // find the internal block located in the single node rack
    Block internalBlock = null;
    String hostToStop = null;
    for (DatanodeStorageInfo storage : blockInfo.storages) {
      if (singleNodeRacks.contains(storage.getDatanodeDescriptor().getHostName())) {
        hostToStop = storage.getDatanodeDescriptor().getHostName();
        internalBlock = blockInfo.getBlockOnStorage(storage);
      }
    }
    Assert.assertNotNull(internalBlock);
    Assert.assertNotNull(hostToStop);

    // delete the block on the chosen datanode
    cluster.corruptBlockOnDataNodesByDeletingBlockFile(
        new ExtendedBlock(bm.getBlockPoolId(), internalBlock));

    // stop the chosen datanode
    MiniDFSCluster.DataNodeProperties dnProp = null;
    for (int i = 0; i < cluster.getDataNodes().size(); i++) {
      DataNode dn = cluster.getDataNodes().get(i);
      if (dn.getDatanodeId().getHostName().equals(hostToStop)) {
        dnProp = cluster.stopDataNode(i);
        cluster.setDataNodeDead(dn.getDatanodeId());
        LOG.info("stop datanode " + dn.getDatanodeId().getHostName());
      }
    }
    NetworkTopology topology = bm.getDatanodeManager().getNetworkTopology();
    Assert.assertEquals(5, topology.getNumOfRacks());

    // make sure the reconstruction work can finish
    // now we have 9 internal blocks in 5 racks
    DFSTestUtil.waitForReplication(fs, file, blockNum, 15 * 1000);

    // we now should have 9 internal blocks distributed in 5 racks
    Set<String> rackSet = new HashSet<>();
    for (DatanodeStorageInfo storage : blockInfo.storages) {
      rackSet.add(storage.getDatanodeDescriptor().getNetworkLocation());
    }
    Assert.assertEquals(5, rackSet.size());

    // restart the stopped datanode
    cluster.restartDataNode(dnProp);
    cluster.waitActive();

    // make sure we have 6 racks again
    topology = bm.getDatanodeManager().getNetworkTopology();
    Assert.assertEquals(hosts.length, topology.getNumOfLeaves());
    Assert.assertEquals(6, topology.getNumOfRacks());

    // pause all the heartbeats
    DataNode badDn = null;
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
      if (dn.getDatanodeId().getHostName().equals(hostToStop)) {
        badDn = dn;
      }
    }
    assert badDn != null;
    // let the DN report the bad block, so that the namenode will put the block
    // into under-replicated queue. note that the block still has 9 internal
    // blocks but in 5 racks
    badDn.reportBadBlocks(new ExtendedBlock(bm.getBlockPoolId(), internalBlock));

    // check if replication monitor correctly schedule the replication work
    boolean scheduled = false;
    for (int i = 0; i < 5; i++) { // retry 5 times
      for (DatanodeStorageInfo storage : blockInfo.storages) {
        if (storage != null) {
          DatanodeDescriptor dn = storage.getDatanodeDescriptor();
          Assert.assertEquals(0, dn.getNumberOfBlocksToBeErasureCoded());
          if (dn.getNumberOfBlocksToBeReplicated() == 1) {
            scheduled = true;
          }
        }
      }
      if (scheduled) {
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertTrue(scheduled);
  }
}
