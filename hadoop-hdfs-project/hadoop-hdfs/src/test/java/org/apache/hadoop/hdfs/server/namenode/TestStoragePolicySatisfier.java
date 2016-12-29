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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

/**
 * Tests that StoragePolicySatisfier daemon is able to check the blocks to be
 * moved and finding its suggested target locations to move.
 */
public class TestStoragePolicySatisfier {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestStoragePolicySatisfier.class);
  private final Configuration config = new HdfsConfiguration();
  private StorageType[][] allDiskTypes =
      new StorageType[][]{{StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK}};
  private MiniDFSCluster hdfsCluster = null;
  final private int numOfDatanodes = 3;
  final private int storagesPerDatanode = 2;
  final private long capacity = 2 * 256 * 1024 * 1024;
  final private String file = "/testMoveWhenStoragePolicyNotSatisfying";
  private DistributedFileSystem dfs = null;
  private static final int DEFAULT_BLOCK_SIZE = 1024;

  private void shutdownCluster() {
    if (hdfsCluster != null) {
      hdfsCluster.shutdown();
    }
  }

  private void createCluster() throws IOException {
    config.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    hdfsCluster = startCluster(config, allDiskTypes, numOfDatanodes,
        storagesPerDatanode, capacity);
    dfs = hdfsCluster.getFileSystem();
    writeContent(file);
  }

  @Test(timeout = 300000)
  public void testWhenStoragePolicySetToCOLD()
      throws Exception {

    try {
      createCluster();
      // Change policy to COLD
      dfs.setStoragePolicy(new Path(file), "COLD");
      FSNamesystem namesystem = hdfsCluster.getNamesystem();
      INode inode = namesystem.getFSDirectory().getINode(file);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE}};
      startAdditionalDNs(config, 3, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());

      hdfsCluster.triggerHeartbeats();
      // Wait till namenode notified about the block location details
      waitExpectedStorageType(file, StorageType.ARCHIVE, 3, 30000);
    } finally {
      shutdownCluster();
    }
  }

  @Test(timeout = 300000)
  public void testWhenStoragePolicySetToALLSSD()
      throws Exception {
    try {
      createCluster();
      // Change policy to ALL_SSD
      dfs.setStoragePolicy(new Path(file), "ALL_SSD");
      FSNamesystem namesystem = hdfsCluster.getNamesystem();
      INode inode = namesystem.getFSDirectory().getINode(file);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK},
              {StorageType.SSD, StorageType.DISK},
              {StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 3, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);
      namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier Identified that block to move to SSD
      // areas
      waitExpectedStorageType(file, StorageType.SSD, 3, 30000);
    } finally {
      shutdownCluster();
    }
  }

  @Test(timeout = 300000)
  public void testWhenStoragePolicySetToONESSD()
      throws Exception {
    try {
      createCluster();
      // Change policy to ONE_SSD
      dfs.setStoragePolicy(new Path(file), "ONE_SSD");
      FSNamesystem namesystem = hdfsCluster.getNamesystem();
      INode inode = namesystem.getFSDirectory().getINode(file);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);
      namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier Identified that block to move to SSD
      // areas
      waitExpectedStorageType(file, StorageType.SSD, 1, 30000);
      waitExpectedStorageType(file, StorageType.DISK, 2, 30000);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests to verify that the block storage movement results will be propagated
   * to Namenode via datanode heartbeat.
   */
  @Test(timeout = 300000)
  public void testPerTrackIdBlocksStorageMovementResults() throws Exception {
    try {
      createCluster();
      // Change policy to ONE_SSD
      dfs.setStoragePolicy(new Path(file), "ONE_SSD");
      FSNamesystem namesystem = hdfsCluster.getNamesystem();
      INode inode = namesystem.getFSDirectory().getINode(file);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);
      namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());
      hdfsCluster.triggerHeartbeats();

      // Wait till the block is moved to SSD areas
      waitExpectedStorageType(file, StorageType.SSD, 1, 30000);
      waitExpectedStorageType(file, StorageType.DISK, 2, 30000);

      waitForBlocksMovementResult(1, 30000);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests to verify that multiple files are giving to satisfy storage policy
   * and should work well altogether.
   */
  @Test(timeout = 300000)
  public void testMultipleFilesForSatisfyStoragePolicy() throws Exception {
    try {
      createCluster();
      List<String> files = new ArrayList<>();
      files.add(file);

      // Creates 4 more files. Send all of them for satisfying the storage
      // policy together.
      for (int i = 0; i < 4; i++) {
        String file1 = "/testMoveWhenStoragePolicyNotSatisfying_" + i;
        files.add(file1);
        writeContent(file1);
      }
      FSNamesystem namesystem = hdfsCluster.getNamesystem();
      List<Long> blockCollectionIds = new ArrayList<>();
      // Change policy to ONE_SSD
      for (String fileName : files) {
        dfs.setStoragePolicy(new Path(fileName), "ONE_SSD");
        INode inode = namesystem.getFSDirectory().getINode(fileName);
        blockCollectionIds.add(inode.getId());
      }

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);
      for (long inodeId : blockCollectionIds) {
        namesystem.getBlockManager().satisfyStoragePolicy(inodeId);
      }
      hdfsCluster.triggerHeartbeats();

      for (String fileName : files) {
        // Wait till the block is moved to SSD areas
        waitExpectedStorageType(fileName, StorageType.SSD, 1, 30000);
        waitExpectedStorageType(fileName, StorageType.DISK, 2, 30000);
      }

      waitForBlocksMovementResult(blockCollectionIds.size(), 30000);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests to verify hdfsAdmin.satisfyStoragePolicy works well for file.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testSatisfyFileWithHdfsAdmin() throws Exception {
    try {
      createCluster();
      HdfsAdmin hdfsAdmin =
          new HdfsAdmin(FileSystem.getDefaultUri(config), config);
      // Change policy to COLD
      dfs.setStoragePolicy(new Path(file), "COLD");

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE}};
      startAdditionalDNs(config, 3, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      hdfsAdmin.satisfyStoragePolicy(new Path(file));

      hdfsCluster.triggerHeartbeats();
      // Wait till namenode notified about the block location details
      waitExpectedStorageType(file, StorageType.ARCHIVE, 3, 30000);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests to verify hdfsAdmin.satisfyStoragePolicy works well for dir.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testSatisfyDirWithHdfsAdmin() throws Exception {
    try {
      createCluster();
      HdfsAdmin hdfsAdmin =
          new HdfsAdmin(FileSystem.getDefaultUri(config), config);
      final String subDir = "/subDir";
      final String subFile1 = subDir + "/subFile1";
      final String subDir2 = subDir + "/subDir2";
      final String subFile2 = subDir2 + "/subFile2";
      dfs.mkdirs(new Path(subDir));
      writeContent(subFile1);
      dfs.mkdirs(new Path(subDir2));
      writeContent(subFile2);

      // Change policy to COLD
      dfs.setStoragePolicy(new Path(subDir), "ONE_SSD");

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK}};
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      hdfsAdmin.satisfyStoragePolicy(new Path(subDir));

      hdfsCluster.triggerHeartbeats();

      // take effect for the file in the directory.
      waitExpectedStorageType(subFile1, StorageType.SSD, 1, 30000);
      waitExpectedStorageType(subFile1, StorageType.DISK, 2, 30000);

      // take no effect for the sub-dir's file in the directory.
      waitExpectedStorageType(subFile2, StorageType.DEFAULT, 3, 30000);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests to verify hdfsAdmin.satisfyStoragePolicy exceptions.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testSatisfyWithExceptions() throws Exception {
    try {
      createCluster();
      final String nonExistingFile = "/noneExistingFile";
      hdfsCluster.getConfiguration(0).
          setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY, false);
      hdfsCluster.restartNameNodes();
      hdfsCluster.waitActive();
      HdfsAdmin hdfsAdmin =
          new HdfsAdmin(FileSystem.getDefaultUri(config), config);

      try {
        hdfsAdmin.satisfyStoragePolicy(new Path(file));
        Assert.fail(String.format(
            "Should failed to satisfy storage policy "
                + "for %s since %s is set to false.",
            file, DFS_STORAGE_POLICY_ENABLED_KEY));
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().contains(String.format(
            "Failed to satisfy storage policy since %s is set to false.",
            DFS_STORAGE_POLICY_ENABLED_KEY)));
      }

      hdfsCluster.getConfiguration(0).
          setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY, true);
      hdfsCluster.restartNameNodes();
      hdfsCluster.waitActive();
      hdfsAdmin = new HdfsAdmin(FileSystem.getDefaultUri(config), config);
      try {
        hdfsAdmin.satisfyStoragePolicy(new Path(nonExistingFile));
        Assert.fail("Should throw FileNotFoundException for " +
            nonExistingFile);
      } catch (FileNotFoundException e) {

      }
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests to verify that for the given path, some of the blocks or block src
   * locations(src nodes) under the given path will be scheduled for block
   * movement.
   *
   * For example, there are two block for a file:
   *
   * File1 => blk_1[locations=A(DISK),B(DISK),C(DISK)],
   * blk_2[locations=A(DISK),B(DISK),C(DISK)]. Now, set storage policy to COLD.
   * Only one datanode is available with storage type ARCHIVE, say D.
   *
   * SPS will schedule block movement to the coordinator node with the details,
   * blk_1[move A(DISK) -> D(ARCHIVE)], blk_2[move A(DISK) -> D(ARCHIVE)].
   */
  @Test(timeout = 300000)
  public void testWhenOnlyFewTargetDatanodeAreAvailableToSatisfyStoragePolicy()
      throws Exception {
    try {
      createCluster();
      // Change policy to COLD
      dfs.setStoragePolicy(new Path(file), "COLD");
      FSNamesystem namesystem = hdfsCluster.getNamesystem();
      INode inode = namesystem.getFSDirectory().getINode(file);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.ARCHIVE, StorageType.ARCHIVE}};

      // Adding ARCHIVE based datanodes.
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier identified that block to move to
      // ARCHIVE area.
      waitExpectedStorageType(file, StorageType.ARCHIVE, 1, 30000);
      waitExpectedStorageType(file, StorageType.DISK, 2, 30000);

      waitForBlocksMovementResult(1, 30000);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests to verify that for the given path, no blocks or block src
   * locations(src nodes) under the given path will be scheduled for block
   * movement as there are no available datanode with required storage type.
   *
   * For example, there are two block for a file:
   *
   * File1 => blk_1[locations=A(DISK),B(DISK),C(DISK)],
   * blk_2[locations=A(DISK),B(DISK),C(DISK)]. Now, set storage policy to COLD.
   * No datanode is available with storage type ARCHIVE.
   *
   * SPS won't schedule any block movement for this path.
   */
  @Test(timeout = 300000)
  public void testWhenNoTargetDatanodeToSatisfyStoragePolicy()
      throws Exception {
    try {
      createCluster();
      // Change policy to COLD
      dfs.setStoragePolicy(new Path(file), "COLD");
      FSNamesystem namesystem = hdfsCluster.getNamesystem();
      INode inode = namesystem.getFSDirectory().getINode(file);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.DISK, StorageType.DISK}};
      // Adding DISK based datanodes
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());
      hdfsCluster.triggerHeartbeats();

      // No block movement will be scheduled as there is no target node available
      // with the required storage type.
      waitForAttemptedItems(1, 30000);
      waitExpectedStorageType(file, StorageType.DISK, 3, 30000);
      // Since there is no target node the item will get timed out and then
      // re-attempted.
      waitForAttemptedItems(1, 30000);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests to verify that SPS should not start when a Mover instance
   * is running.
   */
  @Test(timeout = 300000)
  public void testWhenMoverIsAlreadyRunningBeforeStoragePolicySatisfier()
      throws IOException {
    try {
      createCluster();
      // Simulate Mover by creating MOVER_ID file
      DFSTestUtil.createFile(hdfsCluster.getFileSystem(),
          HdfsServerConstants.MOVER_ID_PATH, 0, (short) 1, 0);
      hdfsCluster.restartNameNode(true);
      boolean running = hdfsCluster.getFileSystem()
          .getClient().isStoragePolicySatisfierRunning();
      Assert.assertFalse("SPS should not start "
          + "when a Mover instance is running", running);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Test to verify that satisfy worker can't move blocks. If the given block is
   * pinned it shouldn't be considered for retries.
   */
  @Test(timeout = 120000)
  public void testMoveWithBlockPinning() throws Exception {
    config.setBoolean(DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED, true);
    hdfsCluster = new MiniDFSCluster.Builder(config).numDataNodes(3)
        .storageTypes(
            new StorageType[][] {{StorageType.DISK, StorageType.DISK},
                {StorageType.DISK, StorageType.DISK},
                {StorageType.DISK, StorageType.DISK}})
        .build();

    hdfsCluster.waitActive();
    dfs = hdfsCluster.getFileSystem();

    // create a file with replication factor 3 and mark 2 pinned block
    // locations.
    final String file1 = createFileAndSimulateFavoredNodes(2);

    // Change policy to COLD
    dfs.setStoragePolicy(new Path(file1), "COLD");
    FSNamesystem namesystem = hdfsCluster.getNamesystem();
    INode inode = namesystem.getFSDirectory().getINode(file1);

    StorageType[][] newtypes =
        new StorageType[][]{{StorageType.ARCHIVE, StorageType.ARCHIVE},
            {StorageType.ARCHIVE, StorageType.ARCHIVE},
            {StorageType.ARCHIVE, StorageType.ARCHIVE}};
    // Adding DISK based datanodes
    startAdditionalDNs(config, 3, numOfDatanodes, newtypes,
        storagesPerDatanode, capacity, hdfsCluster);

    namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());
    hdfsCluster.triggerHeartbeats();

    // No block movement will be scheduled as there is no target node available
    // with the required storage type.
    waitForAttemptedItems(1, 30000);
    waitForBlocksMovementResult(1, 30000);
    waitExpectedStorageType(file1, StorageType.ARCHIVE, 1, 30000);
    waitExpectedStorageType(file1, StorageType.DISK, 2, 30000);
  }

  /**
   * Tests to verify that for the given path, only few of the blocks or block
   * src locations(src nodes) under the given path will be scheduled for block
   * movement.
   *
   * For example, there are two block for a file:
   *
   * File1 => two blocks and default storage policy(HOT).
   * blk_1[locations=A(DISK),B(DISK),C(DISK),D(DISK),E(DISK)],
   * blk_2[locations=A(DISK),B(DISK),C(DISK),D(DISK),E(DISK)].
   *
   * Now, set storage policy to COLD.
   * Only two Dns are available with expected storage type ARCHIVE, say A, E.
   *
   * SPS will schedule block movement to the coordinator node with the details,
   * blk_1[move A(DISK) -> A(ARCHIVE), move E(DISK) -> E(ARCHIVE)],
   * blk_2[move A(DISK) -> A(ARCHIVE), move E(DISK) -> E(ARCHIVE)].
   */
  @Test(timeout = 300000)
  public void testWhenOnlyFewSourceNodesHaveMatchingTargetNodes()
      throws Exception {
    try {
      int numOfDns = 5;
      config.setLong("dfs.block.size", 1024);
      allDiskTypes =
          new StorageType[][]{{StorageType.DISK, StorageType.ARCHIVE},
              {StorageType.DISK, StorageType.DISK},
              {StorageType.DISK, StorageType.DISK},
              {StorageType.DISK, StorageType.DISK},
              {StorageType.DISK, StorageType.ARCHIVE}};
      hdfsCluster = startCluster(config, allDiskTypes, numOfDns,
          storagesPerDatanode, capacity);
      dfs = hdfsCluster.getFileSystem();
      writeContent(file, (short) 5);

      // Change policy to COLD
      dfs.setStoragePolicy(new Path(file), "COLD");
      FSNamesystem namesystem = hdfsCluster.getNamesystem();
      INode inode = namesystem.getFSDirectory().getINode(file);

      namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier identified that block to move to
      // ARCHIVE area.
      waitExpectedStorageType(file, StorageType.ARCHIVE, 2, 30000);
      waitExpectedStorageType(file, StorageType.DISK, 3, 30000);

      waitForBlocksMovementResult(1, 30000);
    } finally {
      shutdownCluster();
    }
  }

  private String createFileAndSimulateFavoredNodes(int favoredNodesCount)
      throws IOException {
    ArrayList<DataNode> dns = hdfsCluster.getDataNodes();
    final String file1 = "/testMoveWithBlockPinning";
    // replication factor 3
    InetSocketAddress[] favoredNodes = new InetSocketAddress[favoredNodesCount];
    for (int i = 0; i < favoredNodesCount; i++) {
      favoredNodes[i] = dns.get(i).getXferAddress();
    }
    DFSTestUtil.createFile(dfs, new Path(file1), false, 1024, 100,
        DEFAULT_BLOCK_SIZE, (short) 3, 0, false, favoredNodes);

    LocatedBlocks locatedBlocks = dfs.getClient().getLocatedBlocks(file1, 0);
    Assert.assertEquals("Wrong block count", 1,
        locatedBlocks.locatedBlockCount());

    // verify storage type before movement
    LocatedBlock lb = locatedBlocks.get(0);
    StorageType[] storageTypes = lb.getStorageTypes();
    for (StorageType storageType : storageTypes) {
      Assert.assertTrue(StorageType.DISK == storageType);
    }

    // Mock FsDatasetSpi#getPinning to show that the block is pinned.
    DatanodeInfo[] locations = lb.getLocations();
    Assert.assertEquals(3, locations.length);
    Assert.assertTrue(favoredNodesCount < locations.length);
    for(DatanodeInfo dnInfo: locations){
      LOG.info("Simulate block pinning in datanode {}",
          locations[favoredNodesCount]);
      DataNode dn = hdfsCluster.getDataNode(dnInfo.getIpcPort());
      DataNodeTestUtils.mockDatanodeBlkPinning(dn, true);
      favoredNodesCount--;
      if (favoredNodesCount <= 0) {
        break; // marked favoredNodesCount number of pinned block location
      }
    }
    return file1;
  }

  private void waitForAttemptedItems(long expectedBlkMovAttemptedCount,
      int timeout) throws TimeoutException, InterruptedException {
    BlockManager blockManager = hdfsCluster.getNamesystem().getBlockManager();
    final StoragePolicySatisfier sps = blockManager.getStoragePolicySatisfier();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LOG.info("expectedAttemptedItemsCount={} actualAttemptedItemsCount={}",
            expectedBlkMovAttemptedCount,
            sps.getAttemptedItemsMonitor().getAttemptedItemsCount());
        return sps.getAttemptedItemsMonitor()
            .getAttemptedItemsCount() == expectedBlkMovAttemptedCount;
      }
    }, 100, timeout);
  }

  private void waitForBlocksMovementResult(long expectedBlkMovResultsCount,
      int timeout) throws TimeoutException, InterruptedException {
    BlockManager blockManager = hdfsCluster.getNamesystem().getBlockManager();
    final StoragePolicySatisfier sps = blockManager.getStoragePolicySatisfier();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LOG.info("expectedResultsCount={} actualResultsCount={}",
            expectedBlkMovResultsCount,
            sps.getAttemptedItemsMonitor().resultsCount());
        return sps.getAttemptedItemsMonitor()
            .resultsCount() == expectedBlkMovResultsCount;
      }
    }, 100, timeout);
  }

  private void writeContent(final String fileName) throws IOException {
    writeContent(fileName, (short) 3);
  }

  private void writeContent(final String fileName, short replicatonFactor)
      throws IOException {
    // write to DISK
    final FSDataOutputStream out = dfs.create(new Path(fileName),
        replicatonFactor);
    for (int i = 0; i < 1000; i++) {
      out.writeChars("t");
    }
    out.close();
  }

  private void startAdditionalDNs(final Configuration conf,
      int newNodesRequired, int existingNodesNum, StorageType[][] newTypes,
      int storagesPerDatanode, long capacity, final MiniDFSCluster cluster)
          throws IOException {
    long[][] capacities;
    existingNodesNum += newNodesRequired;
    capacities = new long[newNodesRequired][storagesPerDatanode];
    for (int i = 0; i < newNodesRequired; i++) {
      for (int j = 0; j < storagesPerDatanode; j++) {
        capacities[i][j] = capacity;
      }
    }

    cluster.startDataNodes(conf, newNodesRequired, newTypes, true, null, null,
        null, capacities, null, false, false, false, null);
    cluster.triggerHeartbeats();
  }

  private MiniDFSCluster startCluster(final Configuration conf,
      StorageType[][] storageTypes, int numberOfDatanodes, int storagesPerDn,
      long nodeCapacity) throws IOException {
    long[][] capacities = new long[numberOfDatanodes][storagesPerDn];
    for (int i = 0; i < numberOfDatanodes; i++) {
      for (int j = 0; j < storagesPerDn; j++) {
        capacities[i][j] = nodeCapacity;
      }
    }
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numberOfDatanodes).storagesPerDatanode(storagesPerDn)
        .storageTypes(storageTypes).storageCapacities(capacities).build();
    cluster.waitActive();
    return cluster;
  }

  // Check whether the Block movement has been successfully completed to satisfy
  // the storage policy for the given file.
  private void waitExpectedStorageType(final String fileName,
      final StorageType expectedStorageType, int expectedStorageCount,
      int timeout) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LocatedBlock lb = null;
        try {
          lb = dfs.getClient().getLocatedBlocks(fileName, 0).get(0);
        } catch (IOException e) {
          LOG.error("Exception while getting located blocks", e);
          return false;
        }
        int actualStorageCount = 0;
        for (StorageType storageType : lb.getStorageTypes()) {
          if (expectedStorageType == storageType) {
            actualStorageCount++;
          }
        }
        LOG.info(
            expectedStorageType + " replica count, expected={} and actual={}",
            expectedStorageType, actualStorageCount);
        return expectedStorageCount == actualStorageCount;
      }
    }, 100, timeout);
  }
}
