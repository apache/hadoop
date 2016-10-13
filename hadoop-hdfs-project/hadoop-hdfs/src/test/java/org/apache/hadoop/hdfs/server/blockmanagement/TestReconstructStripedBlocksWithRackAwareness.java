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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
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
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.ALL);
  }

  private static final String[] hosts =
      getHosts(NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS + 1);
  private static final String[] racks =
      getRacks(NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS + 1, NUM_DATA_BLOCKS);

  private static String[] getHosts(int numHosts) {
    String[] hosts = new String[numHosts];
    for (int i = 0; i < hosts.length; i++) {
      hosts[i] = "host" + (i + 1);
    }
    return hosts;
  }

  private static String[] getRacks(int numHosts, int numRacks) {
    String[] racks = new String[numHosts];
    int numHostEachRack = numHosts / numRacks;
    int residue = numHosts % numRacks;
    int j = 0;
    for (int i = 1; i <= numRacks; i++) {
      int limit = i <= residue ? numHostEachRack + 1 : numHostEachRack;
      for (int k = 0; k < limit; k++) {
        racks[j++] = "/r" + i;
      }
    }
    assert j == numHosts;
    return racks;
  }

  private MiniDFSCluster cluster;
  private static final HdfsConfiguration conf = new HdfsConfiguration();
  private DistributedFileSystem fs;

  @BeforeClass
  public static void setup() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 1);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private MiniDFSCluster.DataNodeProperties stopDataNode(String hostname)
      throws IOException {
    MiniDFSCluster.DataNodeProperties dnProp = null;
    for (int i = 0; i < cluster.getDataNodes().size(); i++) {
      DataNode dn = cluster.getDataNodes().get(i);
      if (dn.getDatanodeId().getHostName().equals(hostname)) {
        dnProp = cluster.stopDataNode(i);
        cluster.setDataNodeDead(dn.getDatanodeId());
        LOG.info("stop datanode " + dn.getDatanodeId().getHostName());
      }
    }
    return dnProp;
  }

  private DataNode getDataNode(String host) {
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getDatanodeId().getHostName().equals(host)) {
        return dn;
      }
    }
    return null;
  }

  /**
   * When there are all the internal blocks available but they are not placed on
   * enough racks, NameNode should avoid normal decoding reconstruction but copy
   * an internal block to a new rack.
   *
   * In this test, we first need to create a scenario that a striped block has
   * all the internal blocks but distributed in <6 racks. Then we check if the
   * replication monitor can correctly schedule the reconstruction work for it.
   */
  @Test
  public void testReconstructForNotEnoughRacks() throws Exception {
    LOG.info("cluster hosts: {}, racks: {}", Arrays.asList(hosts),
        Arrays.asList(racks));

    cluster = new MiniDFSCluster.Builder(conf).racks(racks).hosts(hosts)
        .numDataNodes(hosts.length).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    fs.setErasureCodingPolicy(new Path("/"), null);
    FSNamesystem fsn = cluster.getNamesystem();
    BlockManager bm = fsn.getBlockManager();

    MiniDFSCluster.DataNodeProperties lastHost = stopDataNode(
        hosts[hosts.length - 1]);
    final Path file = new Path("/foo");
    // the file's block is in 9 dn but 5 racks
    DFSTestUtil.createFile(fs, file,
        BLOCK_STRIPED_CELL_SIZE * NUM_DATA_BLOCKS * 2, (short) 1, 0L);
    Assert.assertEquals(0, bm.numOfUnderReplicatedBlocks());

    final INodeFile fileNode = fsn.getFSDirectory()
        .getINode4Write(file.toString()).asFile();
    BlockInfoStriped blockInfo = (BlockInfoStriped) fileNode.getLastBlock();

    // we now should have 9 internal blocks distributed in 5 racks
    Set<String> rackSet = new HashSet<>();
    for (DatanodeStorageInfo storage : blockInfo.storages) {
      rackSet.add(storage.getDatanodeDescriptor().getNetworkLocation());
    }
    Assert.assertEquals(NUM_DATA_BLOCKS - 1, rackSet.size());

    // restart the stopped datanode
    cluster.restartDataNode(lastHost);
    cluster.waitActive();

    // make sure we have 6 racks again
    NetworkTopology topology = bm.getDatanodeManager().getNetworkTopology();
    Assert.assertEquals(hosts.length, topology.getNumOfLeaves());
    Assert.assertEquals(NUM_DATA_BLOCKS, topology.getNumOfRacks());

    // pause all the heartbeats
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
    }

    fsn.writeLock();
    try {
      bm.processMisReplicatedBlocks();
    } finally {
      fsn.writeUnlock();
    }

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

  @Test
  public void testChooseExcessReplicasToDelete() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).racks(racks).hosts(hosts)
        .numDataNodes(hosts.length).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    fs.setErasureCodingPolicy(new Path("/"), null);

    MiniDFSCluster.DataNodeProperties lastHost = stopDataNode(
        hosts[hosts.length - 1]);

    final Path file = new Path("/foo");
    DFSTestUtil.createFile(fs, file,
        BLOCK_STRIPED_CELL_SIZE * NUM_DATA_BLOCKS * 2, (short) 1, 0L);

    // stop host1
    MiniDFSCluster.DataNodeProperties host1 = stopDataNode("host1");
    // bring last host back
    cluster.restartDataNode(lastHost);
    cluster.waitActive();

    // wait for reconstruction to finish
    final short blockNum = (short) (NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS);
    DFSTestUtil.waitForReplication(fs, file, blockNum, 15 * 1000);

    // restart host1
    cluster.restartDataNode(host1);
    cluster.waitActive();
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getDatanodeId().getHostName().equals("host1")) {
        DataNodeTestUtils.triggerBlockReport(dn);
        break;
      }
    }

    // make sure the excess replica is detected, and we delete host1's replica
    // so that we have 6 racks
    DFSTestUtil.waitForReplication(fs, file, blockNum, 15 * 1000);
    LocatedBlocks blks = fs.getClient().getLocatedBlocks(file.toString(), 0);
    LocatedStripedBlock block = (LocatedStripedBlock) blks.getLastLocatedBlock();
    for (DatanodeInfo dn : block.getLocations()) {
      Assert.assertFalse(dn.getHostName().equals("host1"));
    }
  }

  /**
   * In case we have 10 internal blocks on 5 racks, where 9 of blocks are live
   * and 1 decommissioning, make sure the reconstruction happens correctly.
   */
  @Test
  public void testReconstructionWithDecommission() throws Exception {
    final String[] racks = getRacks(NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS + 2,
        NUM_DATA_BLOCKS);
    final String[] hosts = getHosts(NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS + 2);
    // we now have 11 hosts on 6 racks with distribution: 2-2-2-2-2-1
    cluster = new MiniDFSCluster.Builder(conf).racks(racks).hosts(hosts)
        .numDataNodes(hosts.length).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    fs.setErasureCodingPolicy(new Path("/"), null);

    final BlockManager bm = cluster.getNamesystem().getBlockManager();
    final DatanodeManager dm = bm.getDatanodeManager();

    // stop h9 and h10 and create a file with 6+3 internal blocks
    MiniDFSCluster.DataNodeProperties h9 = stopDataNode(hosts[hosts.length - 3]);
    MiniDFSCluster.DataNodeProperties h10 = stopDataNode(hosts[hosts.length - 2]);
    final Path file = new Path("/foo");
    DFSTestUtil.createFile(fs, file,
        BLOCK_STRIPED_CELL_SIZE * NUM_DATA_BLOCKS * 2, (short) 1, 0L);
    final BlockInfo blockInfo = cluster.getNamesystem().getFSDirectory()
        .getINode(file.toString()).asFile().getLastBlock();

    // bring h9 back
    cluster.restartDataNode(h9);
    cluster.waitActive();

    // stop h11 so that the reconstruction happens
    MiniDFSCluster.DataNodeProperties h11 = stopDataNode(hosts[hosts.length - 1]);
    boolean recovered = bm.countNodes(blockInfo).liveReplicas() >=
        NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS;
    for (int i = 0; i < 10 & !recovered; i++) {
      Thread.sleep(1000);
      recovered = bm.countNodes(blockInfo).liveReplicas() >=
          NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS;
    }
    Assert.assertTrue(recovered);

    // mark h9 as decommissioning
    DataNode datanode9 = getDataNode(hosts[hosts.length - 3]);
    Assert.assertNotNull(datanode9);
    final DatanodeDescriptor dn9 = dm.getDatanode(datanode9.getDatanodeId());
    dn9.startDecommission();

    // restart h10 and h11
    cluster.restartDataNode(h10);
    cluster.restartDataNode(h11);
    cluster.waitActive();
    DataNodeTestUtils.triggerBlockReport(getDataNode(hosts[hosts.length - 1]));

    // start decommissioning h9
    boolean satisfied = bm.isPlacementPolicySatisfied(blockInfo);
    Assert.assertFalse(satisfied);
    final DecommissionManager decomManager =
        (DecommissionManager) Whitebox.getInternalState(dm, "decomManager");
    cluster.getNamesystem().writeLock();
    try {
      dn9.stopDecommission();
      decomManager.startDecommission(dn9);
    } finally {
      cluster.getNamesystem().writeUnlock();
    }

    // make sure the decommission finishes and the block in on 6 racks
    boolean decommissioned = dn9.isDecommissioned();
    for (int i = 0; i < 10 && !decommissioned; i++) {
      Thread.sleep(1000);
      decommissioned = dn9.isDecommissioned();
    }
    Assert.assertTrue(decommissioned);
    Assert.assertTrue(bm.isPlacementPolicySatisfied(blockInfo));
  }
}
