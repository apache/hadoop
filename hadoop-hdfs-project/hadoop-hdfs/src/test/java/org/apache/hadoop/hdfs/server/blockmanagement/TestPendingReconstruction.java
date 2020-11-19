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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import java.util.function.Supplier;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.log4j.Level;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/**
 * This class tests the internals of PendingReconstructionBlocks.java, as well
 * as how PendingReconstructionBlocks acts in BlockManager
 */
public class TestPendingReconstruction {
  final static int TIMEOUT = 3;     // 3 seconds
  private static final int DFS_REPLICATION_INTERVAL = 1;
  // Number of datanodes in the cluster
  private static final int DATANODE_COUNT = 5;

  private BlockInfo genBlockInfo(long id, long length, long gs) {
    return new BlockInfoContiguous(new Block(id, length, gs),
        (short) DATANODE_COUNT);
  }

  @Test
  public void testPendingReconstruction() {
    NameNode.initMetrics(new Configuration(),
        HdfsServerConstants.NamenodeRole.NAMENODE);
    PendingReconstructionBlocks pendingReconstructions;
    pendingReconstructions = new PendingReconstructionBlocks(TIMEOUT * 1000);
    pendingReconstructions.start();
    //
    // Add 10 blocks to pendingReconstruction.
    //
    DatanodeStorageInfo[] storages = DFSTestUtil.createDatanodeStorageInfos(10);
    for (int i = 0; i < storages.length; i++) {
      BlockInfo block = genBlockInfo(i, i, 0);
      DatanodeStorageInfo[] targets = new DatanodeStorageInfo[i];
      System.arraycopy(storages, 0, targets, 0, i);
      pendingReconstructions.increment(block, targets);
    }
    assertEquals("Size of pendingReconstruction ",
                 10, pendingReconstructions.size());


    //
    // remove one item
    //
    BlockInfo blk = genBlockInfo(8, 8, 0);
    pendingReconstructions.decrement(blk, storages[7]); // removes one replica
    assertEquals("pendingReconstructions.getNumReplicas ",
                 7, pendingReconstructions.getNumReplicas(blk));

    //
    // insert the same item twice should be counted as once
    //
    pendingReconstructions.increment(blk, storages[0]);
    assertEquals("pendingReconstructions.getNumReplicas ",
        7, pendingReconstructions.getNumReplicas(blk));

    for (int i = 0; i < 7; i++) {
      // removes all replicas
      pendingReconstructions.decrement(blk, storages[i]);
    }
    assertTrue(pendingReconstructions.size() == 9);
    pendingReconstructions.increment(blk,
        DFSTestUtil.createDatanodeStorageInfos(8));
    assertTrue(pendingReconstructions.size() == 10);

    //
    // verify that the number of replicas returned
    // are sane.
    //
    for (int i = 0; i < 10; i++) {
      BlockInfo block = genBlockInfo(i, i, 0);
      int numReplicas = pendingReconstructions.getNumReplicas(block);
      assertTrue(numReplicas == i);
    }

    //
    // verify that nothing has timed out so far
    //
    assertNull(pendingReconstructions.getTimedOutBlocks());
    assertEquals(0L, pendingReconstructions.getNumTimedOuts());

    //
    // Wait for one second and then insert some more items.
    //
    try {
      Thread.sleep(1000);
    } catch (Exception ignored) {
    }

    for (int i = 10; i < 15; i++) {
      BlockInfo block = genBlockInfo(i, i, 0);
      pendingReconstructions.increment(block,
          DFSTestUtil.createDatanodeStorageInfos(i));
    }
    assertEquals(15, pendingReconstructions.size());
    assertEquals(0L, pendingReconstructions.getNumTimedOuts());

    //
    // Wait for everything to timeout.
    //
    int loop = 0;
    while (pendingReconstructions.size() > 0) {
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
      }
      loop++;
    }
    System.out.println("Had to wait for " + loop +
                       " seconds for the lot to timeout");

    //
    // Verify that everything has timed out.
    //
    assertEquals("Size of pendingReconstructions ", 0, pendingReconstructions.size());
    assertEquals(15L, pendingReconstructions.getNumTimedOuts());
    Block[] timedOut = pendingReconstructions.getTimedOutBlocks();
    assertNotNull(timedOut);
    assertEquals(15, timedOut.length);
    // Verify the number is not reset
    assertEquals(15L, pendingReconstructions.getNumTimedOuts());
    for (Block block : timedOut) {
      assertTrue(block.getBlockId() < 15);
    }
    pendingReconstructions.stop();
  }

/* Test that processpendingReconstructions will use the most recent
 * blockinfo from the blocksmap by placing a larger genstamp into
 * the blocksmap.
 */
  @Test
  public void testProcessPendingReconstructions() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(
        DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY, TIMEOUT);
    MiniDFSCluster cluster = null;
    Block block;
    BlockInfo blockInfo;
    try {
      cluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_COUNT).build();
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNamesystem();
      BlockManager blkManager = fsn.getBlockManager();

      PendingReconstructionBlocks pendingReconstruction =
          blkManager.pendingReconstruction;
      LowRedundancyBlocks neededReconstruction = blkManager.neededReconstruction;
      BlocksMap blocksMap = blkManager.blocksMap;

      //
      // Add 1 block to pendingReconstructions with GenerationStamp = 0.
      //

      block = new Block(1, 1, 0);
      blockInfo = new BlockInfoContiguous(block, (short) 3);

      pendingReconstruction.increment(blockInfo,
          DFSTestUtil.createDatanodeStorageInfos(1));
      BlockCollection bc = Mockito.mock(BlockCollection.class);
      // Place into blocksmap with GenerationStamp = 1
      blockInfo.setGenerationStamp(1);
      blocksMap.addBlockCollection(blockInfo, bc);
      //Save it for later.
      BlockInfo storedBlock = blockInfo;

      assertEquals("Size of pendingReconstructions ", 1,
          pendingReconstruction.size());

      // Add a second block to pendingReconstructions that has no
      // corresponding entry in blocksmap
      block = new Block(2, 2, 0);
      blockInfo = new BlockInfoContiguous(block, (short) 3);
      pendingReconstruction.increment(blockInfo,
          DFSTestUtil.createDatanodeStorageInfos(1));

      // verify 2 blocks in pendingReconstructions
      assertEquals("Size of pendingReconstructions ", 2,
          pendingReconstruction.size());

      //
      // Wait for everything to timeout.
      //
      while (pendingReconstruction.size() > 0) {
        try {
          Thread.sleep(100);
        } catch (Exception e) {
        }
      }

      //
      // Verify that block moves to neededReconstruction
      //
      while (neededReconstruction.size() == 0) {
        try {
          Thread.sleep(100);
        } catch (Exception e) {
        }
      }

      // Verify that the generation stamp we will try to replicate
      // is now 1
      for (Block b: neededReconstruction) {
        assertEquals("Generation stamp is 1 ", 1,
            b.getGenerationStamp());
      }

      // Verify size of neededReconstruction is exactly 1.
      assertEquals("size of neededReconstruction is 1 ", 1,
          neededReconstruction.size());

      // Verify HDFS-11960
      // Stop the replication/redundancy monitor
      BlockManagerTestUtil.stopRedundancyThread(blkManager);
      pendingReconstruction.clear();
      // Pick a real node
      DatanodeDescriptor desc[] = { blkManager.getDatanodeManager().
          getDatanodes().iterator().next() };

      // Add a stored block to the pendingReconstruction.
      pendingReconstruction.increment(blockInfo,
          DFSTestUtil.createDatanodeStorageInfos(1));
      assertEquals("Size of pendingReconstructions ", 1,
          pendingReconstruction.size());

      // A received IBR processing calls addBlock(). If the gen stamp in the
      // report is not the same, it should stay in pending.
      fsn.writeLock();
      try {
        // Use a wrong gen stamp.
        blkManager.addBlock(desc[0].getStorageInfos()[0],
            new Block(1, 1, 0), null);
      } finally {
        fsn.writeUnlock();
      }

      // The block should still be pending
      assertEquals("Size of pendingReconstructions ", 1,
          pendingReconstruction.size());

      // A block report with the correct gen stamp should remove the record
      // from the pending queue.
      fsn.writeLock();
      try {
        blkManager.addBlock(desc[0].getStorageInfos()[0],
            new Block(1, 1, 1), null);
      } finally {
        fsn.writeUnlock();
      }

      GenericTestUtils.waitFor(() -> pendingReconstruction.size() == 0, 500,
          10000);
      // The pending queue should be empty.
      assertEquals("Size of pendingReconstructions ", 0,
          pendingReconstruction.size());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test if DatanodeProtocol#blockReceivedAndDeleted can correctly update the
   * pending reconstruction. Also make sure the blockReceivedAndDeleted call is
   * idempotent to the pending reconstruction.
   */
  @Test
  public void testBlockReceived() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
          DATANODE_COUNT).build();
      cluster.waitActive();

      DistributedFileSystem hdfs = cluster.getFileSystem();
      FSNamesystem fsn = cluster.getNamesystem();
      BlockManager blkManager = fsn.getBlockManager();

      final String file = "/tmp.txt";
      final Path filePath = new Path(file);
      short replFactor = 1;
      DFSTestUtil.createFile(hdfs, filePath, 1024L, replFactor, 0);

      // temporarily stop the heartbeat
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      for (int i = 0; i < DATANODE_COUNT; i++) {
        DataNodeTestUtils.setHeartbeatsDisabledForTests(datanodes.get(i), true);
      }

      hdfs.setReplication(filePath, (short) DATANODE_COUNT);
      BlockManagerTestUtil.computeAllPendingWork(blkManager);

      assertEquals(1, blkManager.pendingReconstruction.size());
      INodeFile fileNode = fsn.getFSDirectory().getINode4Write(file).asFile();
      BlockInfo[] blocks = fileNode.getBlocks();
      assertEquals(DATANODE_COUNT - 1,
          blkManager.pendingReconstruction.getNumReplicas(blocks[0]));

      LocatedBlock locatedBlock = hdfs.getClient().getLocatedBlocks(file, 0)
          .get(0);
      DatanodeInfo existingDn = (locatedBlock.getLocations())[0];
      int reportDnNum = 0;
      String poolId = cluster.getNamesystem().getBlockPoolId();
      // let two datanodes (other than the one that already has the data) to
      // report to NN
      for (int i = 0; i < DATANODE_COUNT && reportDnNum < 2; i++) {
        if (!datanodes.get(i).getDatanodeId().equals(existingDn)) {
          DatanodeRegistration dnR = datanodes.get(i).getDNRegistrationForBP(
              poolId);
          StorageReceivedDeletedBlocks[] report = {
              new StorageReceivedDeletedBlocks(
                  new DatanodeStorage("Fake-storage-ID-Ignored"),
              new ReceivedDeletedBlockInfo[] { new ReceivedDeletedBlockInfo(
                  blocks[0], BlockStatus.RECEIVED_BLOCK, "") }) };
          cluster.getNameNodeRpc().blockReceivedAndDeleted(dnR, poolId, report);
          reportDnNum++;
        }
      }
      // IBRs are async, make sure the NN processes all of them.
      cluster.getNamesystem().getBlockManager().flushBlockOps();
      assertEquals(DATANODE_COUNT - 3,
          blkManager.pendingReconstruction.getNumReplicas(blocks[0]));

      // let the same datanodes report again
      for (int i = 0; i < DATANODE_COUNT && reportDnNum < 2; i++) {
        if (!datanodes.get(i).getDatanodeId().equals(existingDn)) {
          DatanodeRegistration dnR = datanodes.get(i).getDNRegistrationForBP(
              poolId);
          StorageReceivedDeletedBlocks[] report =
            { new StorageReceivedDeletedBlocks(
                new DatanodeStorage("Fake-storage-ID-Ignored"),
                new ReceivedDeletedBlockInfo[] {
                  new ReceivedDeletedBlockInfo(
                      blocks[0], BlockStatus.RECEIVED_BLOCK, "")}) };
          cluster.getNameNodeRpc().blockReceivedAndDeleted(dnR, poolId, report);
          reportDnNum++;
        }
      }

      cluster.getNamesystem().getBlockManager().flushBlockOps();
      assertEquals(DATANODE_COUNT - 3,
          blkManager.pendingReconstruction.getNumReplicas(blocks[0]));

      // re-enable heartbeat for the datanode that has data
      for (int i = 0; i < DATANODE_COUNT; i++) {
        DataNodeTestUtils
            .setHeartbeatsDisabledForTests(datanodes.get(i), false);
        DataNodeTestUtils.triggerHeartbeat(datanodes.get(i));
      }

      Thread.sleep(5000);
      assertEquals(0, blkManager.pendingReconstruction.size());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test if BlockManager can correctly remove corresponding pending records
   * when a file is deleted
   *
   * @throws Exception
   */
  @Test
  public void testPendingAndInvalidate() throws Exception {
    final Configuration CONF = new HdfsConfiguration();
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    CONF.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFS_REPLICATION_INTERVAL);
    CONF.setInt(DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        DFS_REPLICATION_INTERVAL);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(
        DATANODE_COUNT).build();
    cluster.waitActive();

    FSNamesystem namesystem = cluster.getNamesystem();
    BlockManager bm = namesystem.getBlockManager();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {
      // 1. create a file
      Path filePath = new Path("/tmp.txt");
      DFSTestUtil.createFile(fs, filePath, 1024, (short) 3, 0L);
      DFSTestUtil.waitForReplication(cluster.getFileSystem(), filePath,
          (short) 3, 10000);

      // 2. disable the heartbeats
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
      }

      // 3. mark a couple of blocks as corrupt
      LocatedBlock block = NameNodeAdapter.getBlockLocations(
          cluster.getNameNode(), filePath.toString(), 0, 1).get(0);
      cluster.getNamesystem().writeLock();
      try {
        bm.findAndMarkBlockAsCorrupt(block.getBlock(), block.getLocations()[0],
            "STORAGE_ID", "TEST");
        bm.findAndMarkBlockAsCorrupt(block.getBlock(), block.getLocations()[1],
            "STORAGE_ID", "TEST");
        BlockManagerTestUtil.computeAllPendingWork(bm);
        BlockManagerTestUtil.updateState(bm);
        assertEquals(bm.getPendingReconstructionBlocksCount(), 1L);
        BlockInfo storedBlock = bm.getStoredBlock(block.getBlock().getLocalBlock());
        assertEquals(bm.pendingReconstruction.getNumReplicas(storedBlock), 2);
      } finally {
        cluster.getNamesystem().writeUnlock();
      }

      // 4. delete the file
      fs.delete(filePath, true);
      // retry at most 10 times, each time sleep for 1s. Note that 10s is much
      // less than the default pending record timeout (5~10min)
      int retries = 10;
      long pendingNum = bm.getPendingReconstructionBlocksCount();
      while (pendingNum != 0 && retries-- > 0) {
        Thread.sleep(1000);  // let NN do the deletion
        BlockManagerTestUtil.updateState(bm);
        pendingNum = bm.getPendingReconstructionBlocksCount();
      }
      assertEquals(pendingNum, 0L);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test the metric counters of the re-replication process.
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test (timeout = 300000)
  public void testReplicationCounter() throws IOException,
      InterruptedException, TimeoutException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setInt(DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    conf.setInt(DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY, 1);
    MiniDFSCluster tmpCluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        DATANODE_COUNT).build();
    tmpCluster.waitActive();
    FSNamesystem fsn = tmpCluster.getNamesystem(0);
    fsn.writeLock();

    try {
      BlockManager bm = fsn.getBlockManager();
      BlocksMap blocksMap = bm.blocksMap;

      // create three blockInfo below, blockInfo0 will success, blockInfo1 will
      // time out, blockInfo2 will fail the replication.
      BlockCollection bc0 = Mockito.mock(BlockCollection.class);
      BlockInfo blockInfo0 = new BlockInfoContiguous((short) 3);
      blockInfo0.setBlockId(0);

      BlockCollection bc1 = Mockito.mock(BlockCollection.class);
      BlockInfo blockInfo1 = new BlockInfoContiguous((short) 3);
      blockInfo1.setBlockId(1);

      BlockCollection bc2 = Mockito.mock(BlockCollection.class);
      BlockInfo blockInfo2 = new BlockInfoContiguous((short) 3);
      blockInfo2.setBlockId(2);

      blocksMap.addBlockCollection(blockInfo0, bc0);
      blocksMap.addBlockCollection(blockInfo1, bc1);
      blocksMap.addBlockCollection(blockInfo2, bc2);

      PendingReconstructionBlocks pending = bm.pendingReconstruction;

      MetricsRecordBuilder rb = getMetrics("NameNodeActivity");
      assertCounter("SuccessfulReReplications", 0L, rb);
      assertCounter("NumTimesReReplicationNotScheduled", 0L, rb);
      assertCounter("TimeoutReReplications", 0L, rb);

      // add block0 and block1 to pending queue.
      pending.increment(blockInfo0);
      pending.increment(blockInfo1);

      // call addBlock on block0 will make it successfully replicated.
      // not calling addBlock on block1 will make it timeout later.
      DatanodeStorageInfo[] storageInfos =
          DFSTestUtil.createDatanodeStorageInfos(1);
      bm.addBlock(storageInfos[0], blockInfo0, null);

      // call schedule replication on blockInfo2 will fail the re-replication.
      // because there is no source data to replicate from.
      bm.scheduleReconstruction(blockInfo2, 0);

      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          MetricsRecordBuilder rb = getMetrics("NameNodeActivity");
          return getLongCounter("SuccessfulReReplications", rb) == 1 &&
              getLongCounter("NumTimesReReplicationNotScheduled", rb) == 1 &&
              getLongCounter("TimeoutReReplications", rb) == 1;
        }
      }, 100, 60000);
    } finally {
      tmpCluster.shutdown();
      fsn.writeUnlock();
    }
  }

  @Test
  public void testPendingReConstructionBlocksForSameDN() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
    DFSTestUtil.setNameNodeLogLevel(Level.DEBUG);
    LogCapturer logs = GenericTestUtils.LogCapturer
        .captureLogs(LoggerFactory.getLogger("BlockStateChange"));
    BlockManager bm = cluster.getNamesystem().getBlockManager();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      // 1. create a file
      Path filePath = new Path("/tmp.txt");
      DFSTestUtil.createFile(dfs, filePath, 1024, (short) 1, 0L);

      // 2. disable the IBR
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.pauseIBR(dn);
      }
      DatanodeManager datanodeManager =
          cluster.getNamesystem().getBlockManager().getDatanodeManager();
      ArrayList<DatanodeDescriptor> dnList =
          new ArrayList<DatanodeDescriptor>();
      datanodeManager.fetchDatanodes(dnList, dnList, false);

      LocatedBlock block = NameNodeAdapter
          .getBlockLocations(cluster.getNameNode(), filePath.toString(), 0, 1)
          .get(0);

      // 3. set replication as 3
      dfs.setReplication(filePath, (short) 3);

      // 4 compute replication work twice to make sure the same DN is not adding
      // twice
      BlockManagerTestUtil.computeAllPendingWork(bm);
      BlockManagerTestUtil.computeAllPendingWork(bm);
      BlockManagerTestUtil.updateState(bm);

      // 5 capture the logs and verify the reconstruction work for block for
      // same DN
      String blockName =
          "to replicate " + block.getBlock().getLocalBlock().toString();
      assertEquals(1, StringUtils.countMatches(logs.getOutput(), blockName));
    } finally {
      cluster.shutdown();
    }
  }
}
