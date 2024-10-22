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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
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
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestReconstructStripedBlocks {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestReconstructStripedBlocks.class);
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final int cellSize = ecPolicy.getCellSize();
  private final short dataBlocks = (short) ecPolicy.getNumDataUnits();
  private final short parityBlocks = (short) ecPolicy.getNumParityUnits();
  private final short groupSize = (short) (dataBlocks + parityBlocks);
  private final int blockSize = 4 * cellSize;


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
    for (int i = 1; i <= parityBlocks; i++) {
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
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(groupSize + 1)
        .build();
    try {
      cluster.waitActive();
      cluster.getFileSystem().enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());
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
        assertEquals(cellSize * dataBlocks,
            blk.getNumBytes());
        final BlockInfoStriped sb = (BlockInfoStriped) blk;
        assertEquals(groupSize, sb.numNodes());
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
      BlockManagerTestUtil.updateState(bm);
      DFSTestUtil.verifyClientStats(conf, cluster);

      BlockManagerTestUtil.getComputedDatanodeWork(bm);

      // all the reconstruction work will be scheduled on the last DN
      DataNode lastDn = cluster.getDataNodes().get(groupSize);
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
        if (groupSize - numOfMissed == dataBlocks) {
          // It's a QUEUE_HIGHEST_PRIORITY block, so the busy DNs will be chosen
          // to make sure we have NUM_DATA_BLOCKS DNs to do reconstruction
          // work.
          assertEquals(dataBlocks, info.getSourceDnInfos().length);
        } else {
          // The block has no highest priority, so we don't use the busy DNs as
          // sources
          assertEquals(groupSize - numOfMissed - numOfBusy,
              info.getSourceDnInfos().length);
        }
      }
      BlockManagerTestUtil.updateState(bm);
      DFSTestUtil.verifyClientStats(conf, cluster);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void test2RecoveryTasksForSameBlockGroup() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(groupSize + 2)
        .build();
    try {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      BlockManager bm = cluster.getNamesystem().getBlockManager();
      fs.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());
      fs.getClient().setErasureCodingPolicy("/",
          StripedFileTestUtil.getDefaultECPolicy().getName());
      int fileLen = dataBlocks * blockSize;
      Path p = new Path("/test2RecoveryTasksForSameBlockGroup");
      final byte[] data = new byte[fileLen];
      DFSTestUtil.writeFile(fs, p, data);
      DFSTestUtil.waitForReplication(fs, p, groupSize, 5000);
      BlockManagerTestUtil.updateState(bm);
      DFSTestUtil.verifyClientStats(conf, cluster);

      LocatedStripedBlock lb = (LocatedStripedBlock)fs.getClient()
          .getLocatedBlocks(p.toString(), 0).get(0);
      LocatedBlock[] lbs = StripedBlockUtil.parseStripedBlockGroup(lb,
          cellSize, dataBlocks, parityBlocks);

      BlockManagerTestUtil.getComputedDatanodeWork(bm);
      BlockManagerTestUtil.updateState(bm);
      assertEquals(0, getNumberOfBlocksToBeErasureCoded(cluster));
      assertEquals(0, bm.getPendingReconstructionBlocksCount());
      DFSTestUtil.verifyClientStats(conf, cluster);

      // missing 1 block, so 1 task should be scheduled
      DatanodeInfo dn0 = lbs[0].getLocations()[0];
      cluster.stopDataNode(dn0.getName());
      cluster.setDataNodeDead(dn0);
      BlockManagerTestUtil.getComputedDatanodeWork(bm);
      BlockManagerTestUtil.updateState(bm);
      assertEquals(1, getNumberOfBlocksToBeErasureCoded(cluster));
      assertEquals(1, bm.getPendingReconstructionBlocksCount());
      DFSTestUtil.verifyClientStats(conf, cluster);

      // missing another block, but no new task should be scheduled because
      // previous task isn't finished.
      DatanodeInfo dn1 = lbs[1].getLocations()[0];
      cluster.stopDataNode(dn1.getName());
      cluster.setDataNodeDead(dn1);
      BlockManagerTestUtil.getComputedDatanodeWork(bm);
      BlockManagerTestUtil.updateState(bm);
      assertEquals(1, getNumberOfBlocksToBeErasureCoded(cluster));
      assertEquals(1, bm.getPendingReconstructionBlocksCount());
      DFSTestUtil.verifyClientStats(conf, cluster);
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
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(groupSize + 2)
        .build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
    try {
      fs.mkdirs(dirPath);
      fs.setErasureCodingPolicy(dirPath,
          StripedFileTestUtil.getDefaultECPolicy().getName());
      DFSTestUtil.createFile(fs, filePath,
          cellSize * dataBlocks * 2, (short) 1, 0L);

      // stop a dn
      LocatedBlocks blks = fs.getClient().getLocatedBlocks(filePath.toString(), 0);
      LocatedStripedBlock block = (LocatedStripedBlock) blks.getLastLocatedBlock();
      DatanodeInfo dnToStop = block.getLocations()[0];
      MiniDFSCluster.DataNodeProperties dnProp =
          cluster.stopDataNode(dnToStop.getXferAddr());
      cluster.setDataNodeDead(dnToStop);

      // wait for reconstruction to happen
      DFSTestUtil.waitForReplication(fs, filePath, groupSize, 15 * 1000);

      // bring the dn back: 10 internal blocks now
      cluster.restartDataNode(dnProp);
      cluster.waitActive();
      DFSTestUtil.verifyClientStats(conf, cluster);

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

      Thread.sleep(3000); // wait 3 running cycles of redundancy monitor
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.triggerHeartbeat(dn);
      }

      // check if NN can detect the missing internal block and finish the
      // reconstruction
      StripedFileTestUtil.waitForReconstructionFinished(filePath, fs,
          groupSize);
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
        if (num.liveReplicas() >= groupSize) {
          reconstructed = true;
          break;
        } else {
          Thread.sleep(1000);
        }
      }
      Assert.assertTrue(reconstructed);

      blks = fs.getClient().getLocatedBlocks(filePath.toString(), 0);
      block = (LocatedStripedBlock) blks.getLastLocatedBlock();
      BitSet bitSet = new BitSet(groupSize);
      for (byte index : block.getBlockIndices()) {
        bitSet.set(index);
      }
      for (int i = 0; i < groupSize; i++) {
        Assert.assertTrue(bitSet.get(i));
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=120000) // 2 min timeout
  public void testReconstructionWork() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1000);
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION,
        5);

    ErasureCodingPolicy policy =  SystemErasureCodingPolicies.getByID(
        SystemErasureCodingPolicies.XOR_2_1_POLICY_ID);
    Path ecDir = new Path("/ec");
    Path ecFilePath = new Path(ecDir, "ec-file");
    int blockGroups = 2;
    int totalDataNodes = policy.getNumDataUnits() +
        policy.getNumParityUnits() + 1;

    MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        totalDataNodes).build();
    try {
      // create an EC file with 2 block groups
      final DistributedFileSystem fs = dfsCluster.getFileSystem();
      fs.enableErasureCodingPolicy(policy.getName());
      fs.mkdirs(ecDir);
      fs.setErasureCodingPolicy(ecDir, policy.getName());
      DFSTestUtil.createStripedFile(dfsCluster, ecFilePath, ecDir,
          blockGroups, 2, false, policy);

      final BlockManager bm = dfsCluster.getNamesystem().getBlockManager();
      LocatedBlocks lbs = fs.getClient().getNamenode().getBlockLocations(
          ecFilePath.toString(), 0, blockGroups);
      assert lbs.get(0) instanceof LocatedStripedBlock;
      LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(0));

      Iterator<DatanodeStorageInfo> storageInfos =
          bm.getStorages(bg.getBlock().getLocalBlock()).iterator();
      DatanodeDescriptor firstDn = storageInfos.next().getDatanodeDescriptor();

      BlockManagerTestUtil.updateState(bm);
      DFSTestUtil.verifyClientStats(conf, dfsCluster);

      // Remove one of the DataUnit nodes
      bm.getDatanodeManager().removeDatanode(firstDn);

      // Verify low redundancy count matching EC block groups count
      BlockManagerTestUtil.updateState(bm);
      assertEquals(blockGroups, bm.getLowRedundancyECBlockGroups());
      DFSTestUtil.verifyClientStats(conf, dfsCluster);


      // Trigger block group reconstruction
      BlockManagerTestUtil.getComputedDatanodeWork(bm);
      BlockManagerTestUtil.updateState(bm);

      // Verify pending reconstruction count
      assertEquals(blockGroups, getNumberOfBlocksToBeErasureCoded(dfsCluster));
      assertEquals(0, bm.getLowRedundancyECBlockGroups());
      DFSTestUtil.verifyClientStats(conf, dfsCluster);
    } finally {
      dfsCluster.shutdown();
    }
  }
  private byte[] writeStripedFile(DistributedFileSystem fs, Path ecFile,
                                  int writeBytes) throws Exception {
    byte[] bytes = StripedFileTestUtil.generateBytes(writeBytes);
    DFSTestUtil.writeFile(fs, ecFile, new String(bytes));
    StripedFileTestUtil.waitBlockGroupsReported(fs, ecFile.toString());

    return bytes;
  }
  @Test
  public void testReconstrutionWithBusyBlock1() throws Exception {
    //When the index of busy block is smaller than the missing block
    //[0(busy),1(busy),3,4,5,6,7,8]
    int busyNodeIndex1 = 0;
    int busyNodeIndex2 = 1;
    int deadNodeIndex = 2;
    final Path ecDir = new Path(GenericTestUtils.getRandomizedTempPath());
    final Path ecFile = new Path(ecDir, "testReconstrutionWithBusyBlock1");
    int writeBytes = cellSize * dataBlocks;
    HdfsConfiguration conf = new HdfsConfiguration();
    initConf(conf);
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, false);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
            2000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
            1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY,
            4);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
            1);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(groupSize + 5)
            .build();
    cluster.waitActive();
    DistributedFileSystem dfs = cluster.getFileSystem(0);
    dfs.enableErasureCodingPolicy(
            StripedFileTestUtil.getDefaultECPolicy().getName());
    dfs.mkdirs(ecDir);
    dfs.setErasureCodingPolicy(ecDir,
            StripedFileTestUtil.getDefaultECPolicy().getName());
    byte[] originBytesArray = writeStripedFile(dfs, ecFile, writeBytes);
    List<LocatedBlock> lbs = ((HdfsDataInputStream) dfs.open(ecFile))
            .getAllBlocks();
    LocatedStripedBlock lsb = (LocatedStripedBlock) lbs.get(0);
    DatanodeInfo[] dnList = lsb.getLocations();
    BlockManager bm = cluster.getNamesystem().getBlockManager();
    BlockInfoStriped blockInfo =
            (BlockInfoStriped) bm.getStoredBlock(
                    new Block(lsb.getBlock().getBlockId()));

    //1.Make nodes busy
    DatanodeDescriptor busyNode = bm.getDatanodeManager()
            .getDatanode(dnList[busyNodeIndex1].getDatanodeUuid());
    for (int j = 0; j < maxReplicationStreams; j++) {
      busyNode.incrementPendingReplicationWithoutTargets();
    }
    DatanodeDescriptor busyNode2 = bm.getDatanodeManager()
            .getDatanode(dnList[busyNodeIndex2].getDatanodeUuid());
    for (int j = 0; j < maxReplicationStreams; j++) {
      busyNode2.incrementPendingReplicationWithoutTargets();
    }

    //2.Make a node missing
    DataNode dn = cluster.getDataNode(dnList[deadNodeIndex].getIpcPort());
    cluster.stopDataNode(dnList[deadNodeIndex].getXferAddr());
    cluster.setDataNodeDead(dn.getDatanodeId());

    //3.Whether there is excess replicas or not during the recovery?
    assertEquals(8, bm.countNodes(blockInfo).liveReplicas());

    GenericTestUtils.waitFor(
        () -> {
          return bm.countNodes(blockInfo).liveReplicas() == 9||
              bm.countNodes(blockInfo).excessReplicas() >= 1||
              bm.countNodes(blockInfo).redundantInternalBlocks() >= 1;
        },
        10, 100000);

    assertEquals(0, bm.countNodes(blockInfo).excessReplicas());
    assertEquals(9, bm.countNodes(blockInfo).liveReplicas());
  }

  @Test
  public void testReconstructionWithStorageTypeNotEnough() throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);

    // Nine disk node eleven archive node.
    int numDn = groupSize * 2 + 2;
    StorageType[][] storageTypes = new StorageType[numDn][];
    Arrays.fill(storageTypes, 0, groupSize,
        new StorageType[]{StorageType.DISK, StorageType.DISK});
    Arrays.fill(storageTypes, groupSize, numDn,
        new StorageType[]{StorageType.ARCHIVE, StorageType.ARCHIVE});

    // Nine disk racks and one archive rack.
    String[] racks = {
        "/rack1", "/rack2", "/rack3", "/rack4", "/rack5", "/rack6", "/rack7", "/rack8",
        "/rack9", "/rack0", "/rack0", "/rack0", "/rack0", "/rack0", "/rack0", "/rack0",
        "/rack0", "/rack0", "/rack0", "/rack0"};

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDn)
        .storageTypes(storageTypes)
        .racks(racks)
        .build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());

    try {
      fs.mkdirs(dirPath);
      fs.setStoragePolicy(dirPath, "COLD");
      fs.setErasureCodingPolicy(dirPath,
          StripedFileTestUtil.getDefaultECPolicy().getName());
      DFSTestUtil.createFile(fs, filePath,
          cellSize * dataBlocks * 2, (short) 1, 0L);

      // Stop one dn.
      LocatedBlocks blks = fs.getClient().getLocatedBlocks(filePath.toString(), 0);
      LocatedStripedBlock block = (LocatedStripedBlock) blks.getLastLocatedBlock();
      DatanodeInfo dnToStop = block.getLocations()[0];
      cluster.stopDataNode(dnToStop.getXferAddr());
      cluster.setDataNodeDead(dnToStop);

      // Wait for reconstruction to happen.
      StripedFileTestUtil.waitForReconstructionFinished(filePath, fs, groupSize);
      blks = fs.getClient().getLocatedBlocks(filePath.toString(), 0);
      block = (LocatedStripedBlock) blks.getLastLocatedBlock();
      BitSet bitSet = new BitSet(groupSize);
      for (byte index : block.getBlockIndices()) {
        bitSet.set(index);
      }
      for (int i = 0; i < groupSize; i++) {
        Assert.assertTrue(bitSet.get(i));
      }
    } finally {
      cluster.shutdown();
    }
  }
  @Test
  public void testDeleteOverReplicatedStripedBlock() throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
            false);
    StorageType[][] st = new StorageType[groupSize + 2][1];
    for (int i = 0; i < st.length-1; i++){
      st[i] = new StorageType[]{StorageType.SSD};
    }
    st[st.length -1] = new StorageType[]{StorageType.DISK};

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(groupSize + 2)
            .storagesPerDatanode(1)
            .storageTypes(st)
            .build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(
            StripedFileTestUtil.getDefaultECPolicy().getName());
    try {
      fs.mkdirs(dirPath);
      fs.setErasureCodingPolicy(dirPath,
              StripedFileTestUtil.getDefaultECPolicy().getName());
      fs.setStoragePolicy(dirPath, HdfsConstants.ALLSSD_STORAGE_POLICY_NAME);
      DFSTestUtil.createFile(fs, filePath,
              cellSize * dataBlocks * 2, (short) 1, 0L);
      // Stop a dn
      LocatedBlocks blks = fs.getClient().getLocatedBlocks(filePath.toString(), 0);
      LocatedStripedBlock block = (LocatedStripedBlock) blks.getLastLocatedBlock();
      DatanodeInfo dnToStop = block.getLocations()[0];

      MiniDFSCluster.DataNodeProperties dnProp =
              cluster.stopDataNode(dnToStop.getXferAddr());
      cluster.setDataNodeDead(dnToStop);

      // Wait for reconstruction to happen
      DFSTestUtil.waitForReplication(fs, filePath, groupSize, 15 * 1000);

      DatanodeInfo dnToStop2 = block.getLocations()[1];
      cluster.stopDataNode(dnToStop2.getXferAddr());
      cluster.setDataNodeDead(dnToStop2);
      DFSTestUtil.waitForReplication(fs, filePath, groupSize, 15 * 1000);

      // Bring the dn back: 10 internal blocks now
      cluster.restartDataNode(dnProp);
      DFSTestUtil.verifyClientStats(conf, cluster);

      // Currently namenode is able to track the missing block. And restart NN
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.triggerBlockReport(dn);
      }
      BlockManager bm = cluster.getNamesystem().getBlockManager();
      Thread.sleep(3000);
      GenericTestUtils.waitFor(() ->
          bm.getPendingDeletionBlocksCount() == 0, 10, 2000);
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.triggerHeartbeat(dn);
      }
      boolean isDeletedRedundantBlock = true;
      blks = fs.getClient().getLocatedBlocks(filePath.toString(), 0);
      block = (LocatedStripedBlock) blks.getLastLocatedBlock();
      BitSet bitSet = new BitSet(groupSize);
      for (byte index : block.getBlockIndices()) {
        if(bitSet.get(index)){
          isDeletedRedundantBlock = false;
        }
        bitSet.set(index);
      }
      assertTrue(isDeletedRedundantBlock);
    } finally {
      cluster.shutdown();
    }
  }
}
