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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

/**
 * This class tests the behavior of moving block replica to the given storage
 * type to fulfill the storage policy requirement.
 */
public class TestStoragePolicySatisfyWorker {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestStoragePolicySatisfyWorker.class);
  private static final int DEFAULT_BLOCK_SIZE = 100;
  private MiniDFSCluster cluster = null;
  private final Configuration conf = new HdfsConfiguration();

  private static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
    conf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.INTERNAL.toString());
    // Reduced refresh cycle to update latest datanodes.
    conf.setLong(DFSConfigKeys.DFS_SPS_DATANODE_CACHE_REFRESH_INTERVAL_MS,
        1000);
  }

  @Before
  public void setUp() throws IOException {
    initConf(conf);
  }

  @After
  public void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Tests to verify that the block replica is moving to ARCHIVE storage type to
   * fulfill the storage policy requirement.
   */
  @Test(timeout = 120000)
  public void testMoveSingleBlockToAnotherDatanode() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4)
        .storageTypes(
            new StorageType[][] {{StorageType.DISK, StorageType.ARCHIVE},
                {StorageType.DISK, StorageType.ARCHIVE},
                {StorageType.ARCHIVE, StorageType.ARCHIVE},
                {StorageType.ARCHIVE, StorageType.ARCHIVE}})
        .build();
    cluster.waitActive();
    final DistributedFileSystem dfs = cluster.getFileSystem();
    final String file = "/testMoveSingleBlockToAnotherDatanode";
    // write to DISK
    final FSDataOutputStream out = dfs.create(new Path(file), (short) 2);
    out.writeChars("testMoveSingleBlockToAnotherDatanode");
    out.close();

    // verify before movement
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
    StorageType[] storageTypes = lb.getStorageTypes();
    for (StorageType storageType : storageTypes) {
      Assert.assertTrue(StorageType.DISK == storageType);
    }
    // move to ARCHIVE
    dfs.setStoragePolicy(new Path(file), "COLD");

    dfs.satisfyStoragePolicy(new Path(file));

    cluster.triggerHeartbeats();

    // Wait till NameNode notified about the block location details
    waitForLocatedBlockWithArchiveStorageType(dfs, file, 2, 30000);
  }

  /**
   * Test to verify that satisfy worker can't move blocks. If specified target
   * datanode doesn't have enough space to accommodate the moving block.
   */
  @Test(timeout = 120000)
  public void testMoveWithNoSpaceAvailable() throws Exception {
    final long capacity = 150;
    final String rack0 = "/rack0";
    final String rack1 = "/rack1";
    long[] capacities = new long[] {capacity, capacity, capacity / 2};
    String[] hosts = {"host0", "host1", "host2"};
    String[] racks = {rack0, rack1, rack0};
    int numOfDatanodes = capacities.length;

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numOfDatanodes)
        .hosts(hosts).racks(racks).simulatedCapacities(capacities)
        .storageTypes(
            new StorageType[][] {{StorageType.DISK, StorageType.ARCHIVE},
                {StorageType.DISK, StorageType.ARCHIVE},
                {StorageType.ARCHIVE, StorageType.ARCHIVE}})
        .build();

    cluster.waitActive();
    InetSocketAddress[] favoredNodes = new InetSocketAddress[3];
    for (int i = 0; i < favoredNodes.length; i++) {
      // DFSClient will attempt reverse lookup. In case it resolves
      // "127.0.0.1" to "localhost", we manually specify the hostname.
      favoredNodes[i] = cluster.getDataNodes().get(i).getXferAddress();
    }
    final DistributedFileSystem dfs = cluster.getFileSystem();
    final String file = "/testMoveWithNoSpaceAvailable";
    DFSTestUtil.createFile(dfs, new Path(file), false, 1024, 100,
        DEFAULT_BLOCK_SIZE, (short) 2, 0, false, favoredNodes);

    // verify before movement
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
    StorageType[] storageTypes = lb.getStorageTypes();
    for (StorageType storageType : storageTypes) {
      Assert.assertTrue(StorageType.DISK == storageType);
    }

    // move to ARCHIVE
    dfs.setStoragePolicy(new Path(file), "COLD");

    lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
    DataNode src = cluster.getDataNodes().get(2);
    DatanodeInfo targetDnInfo = DFSTestUtil
        .getLocalDatanodeInfo(src.getXferPort());

    StoragePolicySatisfyWorker worker = new StoragePolicySatisfyWorker(conf,
        src);
    try {
      worker.start();
      List<BlockMovingInfo> blockMovingInfos = new ArrayList<>();
      BlockMovingInfo blockMovingInfo = prepareBlockMovingInfo(
          lb.getBlock().getLocalBlock(), lb.getLocations()[0], targetDnInfo,
          lb.getStorageTypes()[0], StorageType.ARCHIVE);
      blockMovingInfos.add(blockMovingInfo);
      worker.processBlockMovingTasks(cluster.getNamesystem().getBlockPoolId(),
          blockMovingInfos);

      waitForBlockMovementCompletion(worker, 1, 30000);
    } finally {
      worker.stop();
    }
  }

  /**
   * Tests that drop SPS work method clears all the queues.
   *
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testDropSPSWork() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(20).build();

    cluster.waitActive();
    final DistributedFileSystem dfs = cluster.getFileSystem();
    final String file = "/testDropSPSWork";
    DFSTestUtil.createFile(dfs, new Path(file), false, 1024, 50 * 100,
        DEFAULT_BLOCK_SIZE, (short) 2, 0, false, null);

    // move to ARCHIVE
    dfs.setStoragePolicy(new Path(file), "COLD");

    DataNode src = cluster.getDataNodes().get(2);
    DatanodeInfo targetDnInfo =
        DFSTestUtil.getLocalDatanodeInfo(src.getXferPort());

    StoragePolicySatisfyWorker worker =
        new StoragePolicySatisfyWorker(conf, src);
    worker.start();
    try {
      List<BlockMovingInfo> blockMovingInfos = new ArrayList<>();
      List<LocatedBlock> locatedBlocks =
          dfs.getClient().getLocatedBlocks(file, 0).getLocatedBlocks();
      for (LocatedBlock locatedBlock : locatedBlocks) {
        BlockMovingInfo blockMovingInfo =
            prepareBlockMovingInfo(locatedBlock.getBlock().getLocalBlock(),
                locatedBlock.getLocations()[0], targetDnInfo,
                locatedBlock.getStorageTypes()[0], StorageType.ARCHIVE);
        blockMovingInfos.add(blockMovingInfo);
      }
      worker.processBlockMovingTasks(cluster.getNamesystem().getBlockPoolId(),
          blockMovingInfos);
      // Wait till results queue build up
      waitForBlockMovementResult(worker, 30000);
      worker.dropSPSWork();
      assertTrue(worker.getBlocksMovementsStatusHandler()
          .getMoveAttemptFinishedBlocks().size() == 0);
    } finally {
      worker.stop();
    }
  }

  private void waitForBlockMovementResult(
      final StoragePolicySatisfyWorker worker, int timeout) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        List<Block> completedBlocks = worker.getBlocksMovementsStatusHandler()
            .getMoveAttemptFinishedBlocks();
        return completedBlocks.size() > 0;
      }
    }, 100, timeout);
  }

  private void waitForBlockMovementCompletion(
      final StoragePolicySatisfyWorker worker,
      int expectedFinishedItemsCount, int timeout) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        List<Block> completedBlocks = worker.getBlocksMovementsStatusHandler()
            .getMoveAttemptFinishedBlocks();
        int finishedCount = completedBlocks.size();
        LOG.info("Block movement completed count={}, expected={} and actual={}",
            completedBlocks.size(), expectedFinishedItemsCount, finishedCount);
        return expectedFinishedItemsCount == finishedCount;
      }
    }, 100, timeout);
  }

  private void waitForLocatedBlockWithArchiveStorageType(
      final DistributedFileSystem dfs, final String file,
      int expectedArchiveCount, int timeout) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LocatedBlock lb = null;
        try {
          lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
        } catch (IOException e) {
          LOG.error("Exception while getting located blocks", e);
          return false;
        }

        int archiveCount = 0;
        for (StorageType storageType : lb.getStorageTypes()) {
          if (StorageType.ARCHIVE == storageType) {
            archiveCount++;
          }
        }
        LOG.info("Archive replica count, expected={} and actual={}",
            expectedArchiveCount, archiveCount);
        return expectedArchiveCount == archiveCount;
      }
    }, 100, timeout);
  }

  private BlockMovingInfo prepareBlockMovingInfo(Block block,
      DatanodeInfo src, DatanodeInfo destin, StorageType storageType,
      StorageType targetStorageType) {
    return new BlockMovingInfo(block, src, destin, storageType,
        targetStorageType);
  }
}
