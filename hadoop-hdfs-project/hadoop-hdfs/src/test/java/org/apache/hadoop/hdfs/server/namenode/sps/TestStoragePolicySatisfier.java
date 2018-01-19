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
package org.apache.hadoop.hdfs.server.namenode.sps;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_KEY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfyPathStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSTreeTraverser;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import com.google.common.base.Supplier;

/**
 * Tests that StoragePolicySatisfier daemon is able to check the blocks to be
 * moved and finding its suggested target locations to move.
 */
public class TestStoragePolicySatisfier {

  {
    GenericTestUtils.setLogLevel(
        getLogger(FSTreeTraverser.class), Level.DEBUG);
  }

  private static final String ONE_SSD = "ONE_SSD";
  private static final String COLD = "COLD";
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

  @After
  public void shutdownCluster() {
    if (hdfsCluster != null) {
      hdfsCluster.shutdown();
    }
  }

  private void createCluster() throws IOException {
    config.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
        true);
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
      doTestWhenStoragePolicySetToCOLD();
    } finally {
      shutdownCluster();
    }
  }

  private void doTestWhenStoragePolicySetToCOLD() throws Exception {
    // Change policy to COLD
    dfs.setStoragePolicy(new Path(file), COLD);

    StorageType[][] newtypes =
        new StorageType[][]{{StorageType.ARCHIVE, StorageType.ARCHIVE},
            {StorageType.ARCHIVE, StorageType.ARCHIVE},
            {StorageType.ARCHIVE, StorageType.ARCHIVE}};
    startAdditionalDNs(config, 3, numOfDatanodes, newtypes,
        storagesPerDatanode, capacity, hdfsCluster);

    hdfsCluster.triggerHeartbeats();
    dfs.satisfyStoragePolicy(new Path(file));
    // Wait till namenode notified about the block location details
    DFSTestUtil.waitExpectedStorageType(file, StorageType.ARCHIVE, 3, 35000,
        dfs);
  }

  @Test(timeout = 300000)
  public void testWhenStoragePolicySetToALLSSD()
      throws Exception {
    try {
      createCluster();
      // Change policy to ALL_SSD
      dfs.setStoragePolicy(new Path(file), "ALL_SSD");

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK},
              {StorageType.SSD, StorageType.DISK},
              {StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 3, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);
      dfs.satisfyStoragePolicy(new Path(file));
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier Identified that block to move to SSD
      // areas
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.SSD, 3, 30000, dfs);
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
      dfs.setStoragePolicy(new Path(file), ONE_SSD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);
      dfs.satisfyStoragePolicy(new Path(file));
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier Identified that block to move to SSD
      // areas
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.SSD, 1, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.DISK, 2, 30000, dfs);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests to verify that the block storage movement report will be propagated
   * to Namenode via datanode heartbeat.
   */
  @Test(timeout = 300000)
  public void testBlksStorageMovementAttemptFinishedReport() throws Exception {
    try {
      createCluster();
      // Change policy to ONE_SSD
      dfs.setStoragePolicy(new Path(file), ONE_SSD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);
      dfs.satisfyStoragePolicy(new Path(file));
      hdfsCluster.triggerHeartbeats();

      // Wait till the block is moved to SSD areas
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.SSD, 1, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.DISK, 2, 30000, dfs);

      waitForBlocksMovementAttemptReport(1, 30000);
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
      // Change policy to ONE_SSD
      for (String fileName : files) {
        dfs.setStoragePolicy(new Path(fileName), ONE_SSD);
        dfs.satisfyStoragePolicy(new Path(fileName));
      }

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);
      hdfsCluster.triggerHeartbeats();

      for (String fileName : files) {
        // Wait till the block is moved to SSD areas
        DFSTestUtil.waitExpectedStorageType(
            fileName, StorageType.SSD, 1, 30000, dfs);
        DFSTestUtil.waitExpectedStorageType(
            fileName, StorageType.DISK, 2, 30000, dfs);
      }

      waitForBlocksMovementAttemptReport(files.size(), 30000);
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
      dfs.setStoragePolicy(new Path(file), COLD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.DISK, StorageType.ARCHIVE},
              {StorageType.DISK, StorageType.ARCHIVE},
              {StorageType.DISK, StorageType.ARCHIVE}};
      startAdditionalDNs(config, 3, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      hdfsAdmin.satisfyStoragePolicy(new Path(file));

      hdfsCluster.triggerHeartbeats();
      // Wait till namenode notified about the block location details
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.ARCHIVE, 3, 30000, dfs);
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
      dfs.setStoragePolicy(new Path(subDir), ONE_SSD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK}};
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      hdfsAdmin.satisfyStoragePolicy(new Path(subDir));

      hdfsCluster.triggerHeartbeats();

      // take effect for the file in the directory.
      DFSTestUtil.waitExpectedStorageType(
          subFile1, StorageType.SSD, 1, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(
          subFile1, StorageType.DISK, 2, 30000, dfs);

      // take no effect for the sub-dir's file in the directory.
      DFSTestUtil.waitExpectedStorageType(
          subFile2, StorageType.SSD, 1, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(
          subFile2, StorageType.DISK, 2, 30000, dfs);
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

      try {
        hdfsAdmin.satisfyStoragePolicy(new Path(file));
        hdfsAdmin.satisfyStoragePolicy(new Path(file));
        Assert.fail(String.format(
            "Should failed to satisfy storage policy "
            + "for %s ,since it has been "
            + "added to satisfy movement queue.", file));
      } catch (IOException e) {
        GenericTestUtils.assertExceptionContains(
            String.format("Cannot request to call satisfy storage policy "
                + "on path %s, as this file/dir was already called for "
                + "satisfying storage policy.", file), e);
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
      dfs.setStoragePolicy(new Path(file), COLD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.ARCHIVE, StorageType.ARCHIVE}};

      // Adding ARCHIVE based datanodes.
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      dfs.satisfyStoragePolicy(new Path(file));
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier identified that block to move to
      // ARCHIVE area.
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.ARCHIVE, 1, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.DISK, 2, 30000, dfs);

      waitForBlocksMovementAttemptReport(1, 30000);
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
      dfs.setStoragePolicy(new Path(file), COLD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.DISK, StorageType.DISK}};
      // Adding DISK based datanodes
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      dfs.satisfyStoragePolicy(new Path(file));
      hdfsCluster.triggerHeartbeats();

      // No block movement will be scheduled as there is no target node
      // available with the required storage type.
      waitForAttemptedItems(1, 30000);
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.DISK, 3, 30000, dfs);
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
      throws Exception {
    boolean running;
    FSDataOutputStream out = null;
    try {
      createCluster();
      // Stop SPS
      hdfsCluster.getNameNode().reconfigureProperty(
          DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY, "false");
      running = hdfsCluster.getFileSystem()
          .getClient().isStoragePolicySatisfierRunning();
      Assert.assertFalse("SPS should stopped as configured.", running);

      // Simulate the case by creating MOVER_ID file
      out = hdfsCluster.getFileSystem().create(
          HdfsServerConstants.MOVER_ID_PATH);

      // Restart SPS
      hdfsCluster.getNameNode().reconfigureProperty(
          DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY, "true");

      running = hdfsCluster.getFileSystem()
          .getClient().isStoragePolicySatisfierRunning();
      Assert.assertFalse("SPS should not be able to run as file "
          + HdfsServerConstants.MOVER_ID_PATH + " is being hold.", running);

      // Simulate Mover exists
      out.close();
      out = null;
      hdfsCluster.getFileSystem().delete(
          HdfsServerConstants.MOVER_ID_PATH, true);

      // Restart SPS again
      hdfsCluster.getNameNode().reconfigureProperty(
          DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY, "true");
      running = hdfsCluster.getFileSystem()
          .getClient().isStoragePolicySatisfierRunning();
      Assert.assertTrue("SPS should be running as "
          + "Mover already exited", running);

      // Check functionality after SPS restart
      doTestWhenStoragePolicySetToCOLD();
    } catch (ReconfigurationException e) {
      throw new IOException("Exception when reconfigure "
          + DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY, e);
    } finally {
      if (out != null) {
        out.close();
      }
      hdfsCluster.shutdown();
    }
  }

  /**
   * Tests to verify that SPS should be able to start when the Mover ID file
   * is not being hold by a Mover. This can be the case when Mover exits
   * ungracefully without deleting the ID file from HDFS.
   */
  @Test(timeout = 300000)
  public void testWhenMoverExitsWithoutDeleteMoverIDFile()
      throws IOException {
    try {
      createCluster();
      // Simulate the case by creating MOVER_ID file
      DFSTestUtil.createFile(hdfsCluster.getFileSystem(),
          HdfsServerConstants.MOVER_ID_PATH, 0, (short) 1, 0);
      hdfsCluster.restartNameNode(true);
      boolean running = hdfsCluster.getFileSystem()
          .getClient().isStoragePolicySatisfierRunning();
      Assert.assertTrue("SPS should be running as "
          + "no Mover really running", running);
    } finally {
      if (hdfsCluster != null) {
        hdfsCluster.shutdown();
      }
    }
  }

  /**
   * Test to verify that satisfy worker can't move blocks. If the given block is
   * pinned it shouldn't be considered for retries.
   */
  @Test(timeout = 120000)
  public void testMoveWithBlockPinning() throws Exception {
    try{
      config.setBoolean(DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED, true);
      config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
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
      dfs.setStoragePolicy(new Path(file1), COLD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE}};
      // Adding DISK based datanodes
      startAdditionalDNs(config, 3, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      dfs.satisfyStoragePolicy(new Path(file1));
      hdfsCluster.triggerHeartbeats();

      // No block movement will be scheduled as there is no target node
      // available with the required storage type.
      waitForAttemptedItems(1, 30000);
      waitForBlocksMovementAttemptReport(1, 30000);
      DFSTestUtil.waitExpectedStorageType(
          file1, StorageType.ARCHIVE, 1, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(
          file1, StorageType.DISK, 2, 30000, dfs);
    } finally {
      if (hdfsCluster != null) {
        hdfsCluster.shutdown();
      }
    }
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
      config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
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
      dfs.setStoragePolicy(new Path(file), COLD);

      dfs.satisfyStoragePolicy(new Path(file));
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier identified that block to move to
      // ARCHIVE area.
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.ARCHIVE, 2, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.DISK, 3, 30000, dfs);

      waitForBlocksMovementAttemptReport(1, 30000);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests that moving block storage with in the same datanode. Let's say we
   * have DN1[DISK,ARCHIVE], DN2[DISK, SSD], DN3[DISK,RAM_DISK] when
   * storagepolicy set to ONE_SSD and request satisfyStoragePolicy, then block
   * should move to DN2[SSD] successfully.
   */
  @Test(timeout = 300000)
  public void testBlockMoveInSameDatanodeWithONESSD() throws Exception {
    StorageType[][] diskTypes =
        new StorageType[][]{{StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.SSD},
            {StorageType.DISK, StorageType.RAM_DISK}};
    config.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
        true);
    try {
      hdfsCluster = startCluster(config, diskTypes, numOfDatanodes,
          storagesPerDatanode, capacity);
      dfs = hdfsCluster.getFileSystem();
      writeContent(file);

      // Change policy to ONE_SSD
      dfs.setStoragePolicy(new Path(file), ONE_SSD);

      dfs.satisfyStoragePolicy(new Path(file));
      hdfsCluster.triggerHeartbeats();
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.SSD, 1, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.DISK, 2, 30000, dfs);

    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests that moving block storage with in the same datanode and remote node.
   * Let's say we have DN1[DISK,ARCHIVE], DN2[ARCHIVE, SSD], DN3[DISK,DISK],
   * DN4[DISK,DISK] when storagepolicy set to WARM and request
   * satisfyStoragePolicy, then block should move to DN1[ARCHIVE] and
   * DN2[ARCHIVE] successfully.
   */
  @Test(timeout = 300000)
  public void testBlockMoveInSameAndRemoteDatanodesWithWARM() throws Exception {
    StorageType[][] diskTypes =
        new StorageType[][]{{StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.ARCHIVE, StorageType.SSD},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK}};

    config.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
        true);
    try {
      hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
          storagesPerDatanode, capacity);
      dfs = hdfsCluster.getFileSystem();
      writeContent(file);

      // Change policy to WARM
      dfs.setStoragePolicy(new Path(file), "WARM");
      dfs.satisfyStoragePolicy(new Path(file));
      hdfsCluster.triggerHeartbeats();

      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.DISK, 1, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(
          file, StorageType.ARCHIVE, 2, 30000, dfs);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * If replica with expected storage type already exist in source DN then that
   * DN should be skipped.
   */
  @Test(timeout = 300000)
  public void testSPSWhenReplicaWithExpectedStorageAlreadyAvailableInSource()
      throws Exception {
    StorageType[][] diskTypes = new StorageType[][] {
        {StorageType.DISK, StorageType.ARCHIVE},
        {StorageType.DISK, StorageType.ARCHIVE},
        {StorageType.DISK, StorageType.ARCHIVE}};

    try {
      config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
          storagesPerDatanode, capacity);
      dfs = hdfsCluster.getFileSystem();
      // 1. Write two replica on disk
      DFSTestUtil.createFile(dfs, new Path(file), DEFAULT_BLOCK_SIZE,
          (short) 2, 0);
      // 2. Change policy to COLD, so third replica will be written to ARCHIVE.
      dfs.setStoragePolicy(new Path(file), "COLD");

      // 3.Change replication factor to 3.
      dfs.setReplication(new Path(file), (short) 3);

      DFSTestUtil
          .waitExpectedStorageType(file, StorageType.DISK, 2, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(file, StorageType.ARCHIVE, 1, 30000,
          dfs);

      // 4. Change policy to HOT, so we can move the all block to DISK.
      dfs.setStoragePolicy(new Path(file), "HOT");

      // 4. Satisfy the policy.
      dfs.satisfyStoragePolicy(new Path(file));

      // 5. Block should move successfully .
      DFSTestUtil
          .waitExpectedStorageType(file, StorageType.DISK, 3, 30000, dfs);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests that movements should not be assigned when there is no space in
   * target DN.
   */
  @Test(timeout = 300000)
  public void testChooseInSameDatanodeWithONESSDShouldNotChooseIfNoSpace()
      throws Exception {
    StorageType[][] diskTypes =
        new StorageType[][]{{StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.SSD},
            {StorageType.DISK, StorageType.DISK}};
    config.setLong("dfs.block.size", 2 * DEFAULT_BLOCK_SIZE);
    config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
        true);
    long dnCapacity = 1024 * DEFAULT_BLOCK_SIZE + (2 * DEFAULT_BLOCK_SIZE - 1);
    try {
      hdfsCluster = startCluster(config, diskTypes, numOfDatanodes,
          storagesPerDatanode, dnCapacity);
      dfs = hdfsCluster.getFileSystem();
      writeContent(file);

      // Change policy to ONE_SSD
      dfs.setStoragePolicy(new Path(file), ONE_SSD);
      Path filePath = new Path("/testChooseInSameDatanode");
      final FSDataOutputStream out =
          dfs.create(filePath, false, 100, (short) 1, 2 * DEFAULT_BLOCK_SIZE);
      try {
        dfs.setStoragePolicy(filePath, ONE_SSD);
        // Try to fill up SSD part by writing content
        long remaining = dfs.getStatus().getRemaining() / (3 * 2);
        for (int i = 0; i < remaining; i++) {
          out.write(i);
        }
      } finally {
        out.close();
      }
      hdfsCluster.triggerHeartbeats();
      ArrayList<DataNode> dataNodes = hdfsCluster.getDataNodes();
      // Temporarily disable heart beats, so that we can assert whether any
      // items schedules for DNs even though DN's does not have space to write.
      // Disabling heart beats can keep scheduled items on DatanodeDescriptor
      // itself.
      for (DataNode dataNode : dataNodes) {
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dataNode, true);
      }
      dfs.satisfyStoragePolicy(new Path(file));

      // Wait for items to be processed
      waitForAttemptedItems(1, 30000);

      // Make sure no items assigned for movements
      Set<DatanodeDescriptor> dns = hdfsCluster.getNamesystem()
          .getBlockManager().getDatanodeManager().getDatanodes();
      for (DatanodeDescriptor dd : dns) {
        assertNull(dd.getBlocksToMoveStorages(1));
      }

      // Enable heart beats now
      for (DataNode dataNode : dataNodes) {
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dataNode, false);
      }
      hdfsCluster.triggerHeartbeats();

      DFSTestUtil.waitExpectedStorageType(file, StorageType.DISK, 3, 30000,
          dfs);
      DFSTestUtil.waitExpectedStorageType(file, StorageType.SSD, 0, 30000, dfs);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests that Xattrs should be cleaned if satisfy storage policy called on EC
   * file with unsuitable storage policy set.
   *
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testSPSShouldNotLeakXattrIfSatisfyStoragePolicyCallOnECFiles()
      throws Exception {
    StorageType[][] diskTypes =
        new StorageType[][]{{StorageType.SSD, StorageType.DISK},
            {StorageType.SSD, StorageType.DISK},
            {StorageType.SSD, StorageType.DISK},
            {StorageType.SSD, StorageType.DISK},
            {StorageType.SSD, StorageType.DISK},
            {StorageType.DISK, StorageType.SSD},
            {StorageType.DISK, StorageType.SSD},
            {StorageType.DISK, StorageType.SSD},
            {StorageType.DISK, StorageType.SSD},
            {StorageType.DISK, StorageType.SSD}};

    int defaultStripedBlockSize =
        StripedFileTestUtil.getDefaultECPolicy().getCellSize() * 4;
    config.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, defaultStripedBlockSize);
    config.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    config.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1L);
    config.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
        true);
    try {
      hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
          storagesPerDatanode, capacity);
      dfs = hdfsCluster.getFileSystem();
      dfs.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());

      // set "/foo" directory with ONE_SSD storage policy.
      ClientProtocol client = NameNodeProxies.createProxy(config,
          hdfsCluster.getFileSystem(0).getUri(), ClientProtocol.class)
          .getProxy();
      String fooDir = "/foo";
      client.mkdirs(fooDir, new FsPermission((short) 777), true);
      // set an EC policy on "/foo" directory
      client.setErasureCodingPolicy(fooDir,
          StripedFileTestUtil.getDefaultECPolicy().getName());

      // write file to fooDir
      final String testFile = "/foo/bar";
      long fileLen = 20 * defaultStripedBlockSize;
      DFSTestUtil.createFile(dfs, new Path(testFile), fileLen, (short) 3, 0);

      // ONESSD is unsuitable storage policy on EC files
      client.setStoragePolicy(fooDir, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
      dfs.satisfyStoragePolicy(new Path(testFile));

      // Thread.sleep(9000); // To make sure SPS triggered
      // verify storage types and locations
      LocatedBlocks locatedBlocks =
          client.getBlockLocations(testFile, 0, fileLen);
      for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
        for (StorageType type : lb.getStorageTypes()) {
          Assert.assertEquals(StorageType.DISK, type);
        }
      }

      // Make sure satisfy xattr has been removed.
      DFSTestUtil.waitForXattrRemoved(testFile, XATTR_SATISFY_STORAGE_POLICY,
          hdfsCluster.getNamesystem(), 30000);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Test SPS with empty file.
   * 1. Create one empty file.
   * 2. Call satisfyStoragePolicy for empty file.
   * 3. SPS should skip this file and xattr should not be added for empty file.
   */
  @Test(timeout = 300000)
  public void testSPSWhenFileLengthIsZero() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      Path filePath = new Path("/zeroSizeFile");
      DFSTestUtil.createFile(fs, filePath, 0, (short) 1, 0);
      FSEditLog editlog = cluster.getNameNode().getNamesystem().getEditLog();
      long lastWrittenTxId = editlog.getLastWrittenTxId();
      fs.satisfyStoragePolicy(filePath);
      Assert.assertEquals("Xattr should not be added for the file",
          lastWrittenTxId, editlog.getLastWrittenTxId());
      INode inode = cluster.getNameNode().getNamesystem().getFSDirectory()
          .getINode(filePath.toString());
      Assert.assertTrue("XAttrFeature should be null for file",
          inode.getXAttrFeature() == null);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test SPS for low redundant file blocks.
   * 1. Create cluster with 3 datanode.
   * 1. Create one file with 3 replica.
   * 2. Set policy and call satisfyStoragePolicy for file.
   * 3. Stop NameNode and Datanodes.
   * 4. Start NameNode with 2 datanode and wait for block movement.
   * 5. Start third datanode.
   * 6. Third Datanode replica also should be moved in proper
   * sorage based on policy.
   */
  @Test(timeout = 300000)
  public void testSPSWhenFileHasLowRedundancyBlocks() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      conf.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
          "3000");
      StorageType[][] newtypes = new StorageType[][] {
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK}};
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
          .storageTypes(newtypes).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      Path filePath = new Path("/zeroSizeFile");
      DFSTestUtil.createFile(fs, filePath, 1024, (short) 3, 0);
      fs.setStoragePolicy(filePath, "COLD");
      List<DataNodeProperties> list = new ArrayList<>();
      list.add(cluster.stopDataNode(0));
      list.add(cluster.stopDataNode(0));
      list.add(cluster.stopDataNode(0));
      cluster.restartNameNodes();
      cluster.restartDataNode(list.get(0), false);
      cluster.restartDataNode(list.get(1), false);
      cluster.waitActive();
      fs.satisfyStoragePolicy(filePath);
      DFSTestUtil.waitExpectedStorageType(filePath.toString(),
          StorageType.ARCHIVE, 2, 30000, cluster.getFileSystem());
      cluster.restartDataNode(list.get(2), false);
      DFSTestUtil.waitExpectedStorageType(filePath.toString(),
          StorageType.ARCHIVE, 3, 30000, cluster.getFileSystem());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test SPS for extra redundant file blocks.
   * 1. Create cluster with 5 datanode.
   * 2. Create one file with 5 replica.
   * 3. Set file replication to 3.
   * 4. Set policy and call satisfyStoragePolicy for file.
   * 5. Block should be moved successfully.
   */
  @Test(timeout = 300000)
  public void testSPSWhenFileHasExcessRedundancyBlocks() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      conf.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
          "3000");
      StorageType[][] newtypes = new StorageType[][] {
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK}};
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5)
          .storageTypes(newtypes).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      Path filePath = new Path("/zeroSizeFile");
      DFSTestUtil.createFile(fs, filePath, 1024, (short) 5, 0);
      fs.setReplication(filePath, (short) 3);
      LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(
          LogFactory.getLog(BlockStorageMovementAttemptedItems.class));
      fs.setStoragePolicy(filePath, "COLD");
      fs.satisfyStoragePolicy(filePath);
      DFSTestUtil.waitExpectedStorageType(filePath.toString(),
          StorageType.ARCHIVE, 3, 30000, cluster.getFileSystem());
      assertFalse("Log output does not contain expected log message: ",
          logs.getOutput().contains("some of the blocks are low redundant"));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test SPS for empty directory, xAttr should be removed.
   */
  @Test(timeout = 300000)
  public void testSPSForEmptyDirectory() throws IOException, TimeoutException,
      InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      Path emptyDir = new Path("/emptyDir");
      fs.mkdirs(emptyDir);
      fs.satisfyStoragePolicy(emptyDir);
      // Make sure satisfy xattr has been removed.
      DFSTestUtil.waitForXattrRemoved("/emptyDir",
          XATTR_SATISFY_STORAGE_POLICY, cluster.getNamesystem(), 30000);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test SPS for not exist directory.
   */
  @Test(timeout = 300000)
  public void testSPSForNonExistDirectory() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      Path emptyDir = new Path("/emptyDir");
      try {
        fs.satisfyStoragePolicy(emptyDir);
        fail("FileNotFoundException should throw");
      } catch (FileNotFoundException e) {
        // nothing to do
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test SPS for directory tree which doesn't have files.
   */
  @Test(timeout = 300000)
  public void testSPSWithDirectoryTreeWithoutFile() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      // Create directories
      /*
       *                   root
       *                    |
       *           A--------C--------D
       *                    |
       *               G----H----I
       *                    |
       *                    O
       */
      DistributedFileSystem fs = cluster.getFileSystem();
      fs.mkdirs(new Path("/root/C/H/O"));
      fs.mkdirs(new Path("/root/A"));
      fs.mkdirs(new Path("/root/D"));
      fs.mkdirs(new Path("/root/C/G"));
      fs.mkdirs(new Path("/root/C/I"));
      fs.satisfyStoragePolicy(new Path("/root"));
      // Make sure satisfy xattr has been removed.
      DFSTestUtil.waitForXattrRemoved("/root",
          XATTR_SATISFY_STORAGE_POLICY, cluster.getNamesystem(), 30000);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test SPS for directory which has multilevel directories.
   */
  @Test(timeout = 300000)
  public void testMultipleLevelDirectoryForSatisfyStoragePolicy()
      throws Exception {
    try {
      StorageType[][] diskTypes = new StorageType[][] {
          {StorageType.DISK, StorageType.ARCHIVE},
          {StorageType.ARCHIVE, StorageType.SSD},
          {StorageType.DISK, StorageType.DISK}};
      config.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
      config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
          storagesPerDatanode, capacity);
      dfs = hdfsCluster.getFileSystem();
      createDirectoryTree(dfs);

      List<String> files = getDFSListOfTree();
      dfs.setStoragePolicy(new Path("/root"), COLD);
      dfs.satisfyStoragePolicy(new Path("/root"));
      for (String fileName : files) {
        // Wait till the block is moved to ARCHIVE
        DFSTestUtil.waitExpectedStorageType(fileName, StorageType.ARCHIVE, 2,
            30000, dfs);
      }
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Test SPS for batch processing.
   */
  @Test(timeout = 3000000)
  public void testBatchProcessingForSPSDirectory() throws Exception {
    try {
      StorageType[][] diskTypes = new StorageType[][] {
          {StorageType.DISK, StorageType.ARCHIVE},
          {StorageType.ARCHIVE, StorageType.SSD},
          {StorageType.DISK, StorageType.DISK}};
      config.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
      config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      // Set queue max capacity
      config.setInt(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_KEY,
          5);
      hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
          storagesPerDatanode, capacity);
      dfs = hdfsCluster.getFileSystem();
      createDirectoryTree(dfs);
      List<String> files = getDFSListOfTree();
      LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(LogFactory
          .getLog(FSTreeTraverser.class));

      dfs.setStoragePolicy(new Path("/root"), COLD);
      dfs.satisfyStoragePolicy(new Path("/root"));
      for (String fileName : files) {
        // Wait till the block is moved to ARCHIVE
        DFSTestUtil.waitExpectedStorageType(fileName, StorageType.ARCHIVE, 2,
            30000, dfs);
      }
      waitForBlocksMovementAttemptReport(files.size(), 30000);
      String expectedLogMessage = "StorageMovementNeeded queue remaining"
          + " capacity is zero";
      assertTrue("Log output does not contain expected log message: "
          + expectedLogMessage, logs.getOutput().contains(expectedLogMessage));
    } finally {
      shutdownCluster();
    }
  }


  /**
   *  Test traverse when parent got deleted.
   *  1. Delete /root when traversing Q
   *  2. U, R, S should not be in queued.
   */
  @Test(timeout = 300000)
  public void testTraverseWhenParentDeleted() throws Exception {
    StorageType[][] diskTypes = new StorageType[][] {
        {StorageType.DISK, StorageType.ARCHIVE},
        {StorageType.ARCHIVE, StorageType.SSD},
        {StorageType.DISK, StorageType.DISK}};
    config.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    config.setInt(DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_KEY, 10);
    hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
        storagesPerDatanode, capacity);
    dfs = hdfsCluster.getFileSystem();
    createDirectoryTree(dfs);

    List<String> expectedTraverseOrder = getDFSListOfTree();

    //Remove files which will not be traverse when parent is deleted
    expectedTraverseOrder.remove("/root/D/L/R");
    expectedTraverseOrder.remove("/root/D/L/S");
    expectedTraverseOrder.remove("/root/D/L/Q/U");
    FSDirectory fsDir = hdfsCluster.getNamesystem().getFSDirectory();

    //Queue limit can control the traverse logic to wait for some free
    //entry in queue. After 10 files, traverse control will be on U.
    StoragePolicySatisfier sps = new StoragePolicySatisfier(config);
    Context ctxt = new IntraSPSNameNodeContext(hdfsCluster.getNamesystem(),
        hdfsCluster.getNamesystem().getBlockManager(), sps) {
      @Override
      public boolean isInSafeMode() {
        return false;
      }

      @Override
      public boolean isRunning() {
        return true;
      }
    };

    FileIdCollector fileIDCollector =
        new IntraSPSNameNodeFileIdCollector(fsDir, sps);
    sps.init(ctxt, fileIDCollector, null);
    sps.getStorageMovementQueue().activate();

    INode rootINode = fsDir.getINode("/root");
    hdfsCluster.getNamesystem().getBlockManager()
        .addSPSPathId(rootINode.getId());

    //Wait for thread to reach U.
    Thread.sleep(1000);

    dfs.delete(new Path("/root/D/L"), true);

    // Remove 10 element and make queue free, So other traversing will start.
    for (int i = 0; i < 10; i++) {
      String path = expectedTraverseOrder.remove(0);
      long trackId = sps.getStorageMovementQueue().get().getFileId();
      INode inode = fsDir.getInode(trackId);
      assertTrue("Failed to traverse tree, expected " + path + " but got "
          + inode.getFullPathName(), path.equals(inode.getFullPathName()));
    }
    //Wait to finish tree traverse
    Thread.sleep(5000);

    // Check other element traversed in order and R,S should not be added in
    // queue which we already removed from expected list
    for (String path : expectedTraverseOrder) {
      long trackId = sps.getStorageMovementQueue().get().getFileId();
      INode inode = fsDir.getInode(trackId);
      assertTrue("Failed to traverse tree, expected " + path + " but got "
          + inode.getFullPathName(), path.equals(inode.getFullPathName()));
    }
    dfs.delete(new Path("/root"), true);
  }

  /**
   *  Test traverse when root parent got deleted.
   *  1. Delete L when traversing Q
   *  2. E, M, U, R, S should not be in queued.
   */
  @Test(timeout = 300000)
  public void testTraverseWhenRootParentDeleted() throws Exception {
    StorageType[][] diskTypes = new StorageType[][] {
        {StorageType.DISK, StorageType.ARCHIVE},
        {StorageType.ARCHIVE, StorageType.SSD},
        {StorageType.DISK, StorageType.DISK}};
    config.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    config.setInt(DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_KEY, 10);
    hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
        storagesPerDatanode, capacity);
    dfs = hdfsCluster.getFileSystem();
    createDirectoryTree(dfs);

    List<String> expectedTraverseOrder = getDFSListOfTree();

    // Remove files which will not be traverse when parent is deleted
    expectedTraverseOrder.remove("/root/D/L/R");
    expectedTraverseOrder.remove("/root/D/L/S");
    expectedTraverseOrder.remove("/root/D/L/Q/U");
    expectedTraverseOrder.remove("/root/D/M");
    expectedTraverseOrder.remove("/root/E");
    FSDirectory fsDir = hdfsCluster.getNamesystem().getFSDirectory();

    // Queue limit can control the traverse logic to wait for some free
    // entry in queue. After 10 files, traverse control will be on U.
    // StoragePolicySatisfier sps = new StoragePolicySatisfier(config);
    StoragePolicySatisfier sps = new StoragePolicySatisfier(config);
    Context ctxt = new IntraSPSNameNodeContext(hdfsCluster.getNamesystem(),
        hdfsCluster.getNamesystem().getBlockManager(), sps) {
      @Override
      public boolean isInSafeMode() {
        return false;
      }

      @Override
      public boolean isRunning() {
        return true;
      }
    };

    FileIdCollector fileIDCollector =
        new IntraSPSNameNodeFileIdCollector(fsDir, sps);
    sps.init(ctxt, fileIDCollector, null);
    sps.getStorageMovementQueue().activate();

    INode rootINode = fsDir.getINode("/root");
    hdfsCluster.getNamesystem().getBlockManager()
        .addSPSPathId(rootINode.getId());

    // Wait for thread to reach U.
    Thread.sleep(1000);

    dfs.delete(new Path("/root/D/L"), true);

    // Remove 10 element and make queue free, So other traversing will start.
    for (int i = 0; i < 10; i++) {
      String path = expectedTraverseOrder.remove(0);
      long trackId = sps.getStorageMovementQueue().get().getFileId();
      INode inode = fsDir.getInode(trackId);
      assertTrue("Failed to traverse tree, expected " + path + " but got "
          + inode.getFullPathName(), path.equals(inode.getFullPathName()));
    }
    // Wait to finish tree traverse
    Thread.sleep(5000);

    // Check other element traversed in order and E, M, U, R, S should not be
    // added in queue which we already removed from expected list
    for (String path : expectedTraverseOrder) {
      long trackId = sps.getStorageMovementQueue().get().getFileId();
      INode inode = fsDir.getInode(trackId);
      assertTrue("Failed to traverse tree, expected " + path + " but got "
          + inode.getFullPathName(), path.equals(inode.getFullPathName()));
    }
    dfs.delete(new Path("/root"), true);
  }

  /**
   * Test storage move blocks while under replication block tasks exists in the
   * system. So, both will share the max transfer streams.
   *
   * 1. Create cluster with 3 datanode.
   * 2. Create 20 files with 2 replica.
   * 3. Start 2 more DNs with DISK & SSD types
   * 4. SetReplication factor for the 1st 10 files to 4 to trigger replica task
   * 5. Set policy to SSD to the 2nd set of files from 11-20
   * 6. Call SPS for 11-20 files to trigger move block tasks to new DNs
   * 7. Wait for the under replica and SPS tasks completion
   */
  @Test(timeout = 300000)
  public void testMoveBlocksWithUnderReplicatedBlocks() throws Exception {
    try {
      config.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 3);
      config.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
      config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
          "3000");
      config.setBoolean(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_LOW_MAX_STREAMS_PREFERENCE_KEY,
          false);

      StorageType[][] storagetypes = new StorageType[][] {
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK}};
      hdfsCluster = new MiniDFSCluster.Builder(config).numDataNodes(2)
          .storageTypes(storagetypes).build();
      hdfsCluster.waitActive();
      dfs = hdfsCluster.getFileSystem();

      // Below files will be used for pending replication block tasks.
      for (int i=1; i<=20; i++){
        Path filePath = new Path("/file" + i);
        DFSTestUtil.createFile(dfs, filePath, DEFAULT_BLOCK_SIZE * 5, (short) 2,
            0);
      }

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.DISK, StorageType.SSD},
              {StorageType.DISK, StorageType.SSD}};
      startAdditionalDNs(config, 2, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      // increase replication factor to 4 for the first 10 files and thus
      // initiate replica tasks
      for (int i=1; i<=10; i++){
        Path filePath = new Path("/file" + i);
        dfs.setReplication(filePath, (short) 4);
      }

      // invoke SPS for 11-20 files
      for (int i = 11; i <= 20; i++) {
        Path filePath = new Path("/file" + i);
        dfs.setStoragePolicy(filePath, "ALL_SSD");
        dfs.satisfyStoragePolicy(filePath);
      }

      for (int i = 1; i <= 10; i++) {
        Path filePath = new Path("/file" + i);
        DFSTestUtil.waitExpectedStorageType(filePath.toString(),
            StorageType.DISK, 4, 30000, hdfsCluster.getFileSystem());
      }
      for (int i = 11; i <= 20; i++) {
        Path filePath = new Path("/file" + i);
        DFSTestUtil.waitExpectedStorageType(filePath.toString(),
            StorageType.SSD, 2, 30000, hdfsCluster.getFileSystem());
      }
    } finally {
      shutdownCluster();
    }
  }

  @Test(timeout = 300000)
  public void testStoragePolicySatisfyPathStatus() throws Exception {
    try {
      config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
          "3000");
      config.setBoolean(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_LOW_MAX_STREAMS_PREFERENCE_KEY,
          false);

      StorageType[][] storagetypes = new StorageType[][] {
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK}};
      hdfsCluster = new MiniDFSCluster.Builder(config).numDataNodes(2)
          .storageTypes(storagetypes).build();
      hdfsCluster.waitActive();
      // BlockStorageMovementNeeded.setStatusClearanceElapsedTimeMs(200000);
      dfs = hdfsCluster.getFileSystem();
      Path filePath = new Path("/file");
      DFSTestUtil.createFile(dfs, filePath, 1024, (short) 2,
            0);
      dfs.setStoragePolicy(filePath, "COLD");
      dfs.satisfyStoragePolicy(filePath);
      Thread.sleep(3000);
      StoragePolicySatisfyPathStatus status = dfs.getClient()
          .checkStoragePolicySatisfyPathStatus(filePath.toString());
      Assert.assertTrue(
          "Status should be IN_PROGRESS/SUCCESS, but status is " + status,
          StoragePolicySatisfyPathStatus.IN_PROGRESS.equals(status)
              || StoragePolicySatisfyPathStatus.SUCCESS.equals(status));
      DFSTestUtil.waitExpectedStorageType(filePath.toString(),
          StorageType.ARCHIVE, 2, 30000, dfs);

      // wait till status is SUCCESS
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            StoragePolicySatisfyPathStatus status = dfs.getClient()
                .checkStoragePolicySatisfyPathStatus(filePath.toString());
            return StoragePolicySatisfyPathStatus.SUCCESS.equals(status);
          } catch (IOException e) {
            Assert.fail("Fail to get path status for sps");
          }
          return false;
        }
      }, 100, 60000);
      BlockStorageMovementNeeded.setStatusClearanceElapsedTimeMs(1000);
      // wait till status is NOT_AVAILABLE
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            StoragePolicySatisfyPathStatus status = dfs.getClient()
                .checkStoragePolicySatisfyPathStatus(filePath.toString());
            return StoragePolicySatisfyPathStatus.NOT_AVAILABLE.equals(status);
          } catch (IOException e) {
            Assert.fail("Fail to get path status for sps");
          }
          return false;
        }
      }, 100, 60000);
    } finally {
      shutdownCluster();
    }
  }

  @Test(timeout = 300000)
  public void testMaxRetryForFailedBlock() throws Exception {
    try {
      config.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ENABLED_KEY,
          true);
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
          "1000");
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY,
          "1000");
      StorageType[][] storagetypes = new StorageType[][] {
          {StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK}};
      hdfsCluster = new MiniDFSCluster.Builder(config).numDataNodes(2)
          .storageTypes(storagetypes).build();
      hdfsCluster.waitActive();
      dfs = hdfsCluster.getFileSystem();

      Path filePath = new Path("/retryFile");
      DFSTestUtil.createFile(dfs, filePath, DEFAULT_BLOCK_SIZE, (short) 2,
          0);

      dfs.setStoragePolicy(filePath, "COLD");
      dfs.satisfyStoragePolicy(filePath);
      Thread.sleep(3000
          * DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_MAX_RETRY_ATTEMPTS_DEFAULT);
      DFSTestUtil.waitExpectedStorageType(filePath.toString(),
          StorageType.DISK, 2, 60000, hdfsCluster.getFileSystem());
      // Path status should be FAILURE
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            StoragePolicySatisfyPathStatus status = dfs.getClient()
                .checkStoragePolicySatisfyPathStatus(filePath.toString());
            return StoragePolicySatisfyPathStatus.FAILURE.equals(status);
          } catch (IOException e) {
            Assert.fail("Fail to get path status for sps");
          }
          return false;
        }
      }, 100, 90000);
    } finally {
      shutdownCluster();
    }
  }

  private static void createDirectoryTree(DistributedFileSystem dfs)
      throws Exception {
    // tree structure
    /*
     *                           root
     *                             |
     *           A--------B--------C--------D--------E
     *                    |                 |
     *          F----G----H----I       J----K----L----M
     *               |                           |
     *          N----O----P                 Q----R----S
     *                    |                 |
     *                    T                 U
     */
    // create root Node and child
    dfs.mkdirs(new Path("/root"));
    DFSTestUtil.createFile(dfs, new Path("/root/A"), 1024, (short) 3, 0);
    dfs.mkdirs(new Path("/root/B"));
    DFSTestUtil.createFile(dfs, new Path("/root/C"), 1024, (short) 3, 0);
    dfs.mkdirs(new Path("/root/D"));
    DFSTestUtil.createFile(dfs, new Path("/root/E"), 1024, (short) 3, 0);

    // Create /root/B child
    DFSTestUtil.createFile(dfs, new Path("/root/B/F"), 1024, (short) 3, 0);
    dfs.mkdirs(new Path("/root/B/G"));
    DFSTestUtil.createFile(dfs, new Path("/root/B/H"), 1024, (short) 3, 0);
    DFSTestUtil.createFile(dfs, new Path("/root/B/I"), 1024, (short) 3, 0);

    // Create /root/D child
    DFSTestUtil.createFile(dfs, new Path("/root/D/J"), 1024, (short) 3, 0);
    DFSTestUtil.createFile(dfs, new Path("/root/D/K"), 1024, (short) 3, 0);
    dfs.mkdirs(new Path("/root/D/L"));
    DFSTestUtil.createFile(dfs, new Path("/root/D/M"), 1024, (short) 3, 0);

    // Create /root/B/G child
    DFSTestUtil.createFile(dfs, new Path("/root/B/G/N"), 1024, (short) 3, 0);
    DFSTestUtil.createFile(dfs, new Path("/root/B/G/O"), 1024, (short) 3, 0);
    dfs.mkdirs(new Path("/root/B/G/P"));

    // Create /root/D/L child
    dfs.mkdirs(new Path("/root/D/L/Q"));
    DFSTestUtil.createFile(dfs, new Path("/root/D/L/R"), 1024, (short) 3, 0);
    DFSTestUtil.createFile(dfs, new Path("/root/D/L/S"), 1024, (short) 3, 0);

    // Create /root/B/G/P child
    DFSTestUtil.createFile(dfs, new Path("/root/B/G/P/T"), 1024, (short) 3, 0);

    // Create /root/D/L/Q child
    DFSTestUtil.createFile(dfs, new Path("/root/D/L/Q/U"), 1024, (short) 3, 0);
  }

  private List<String> getDFSListOfTree() {
    List<String> dfsList = new ArrayList<>();
    dfsList.add("/root/A");
    dfsList.add("/root/B/F");
    dfsList.add("/root/B/G/N");
    dfsList.add("/root/B/G/O");
    dfsList.add("/root/B/G/P/T");
    dfsList.add("/root/B/H");
    dfsList.add("/root/B/I");
    dfsList.add("/root/C");
    dfsList.add("/root/D/J");
    dfsList.add("/root/D/K");
    dfsList.add("/root/D/L/Q/U");
    dfsList.add("/root/D/L/R");
    dfsList.add("/root/D/L/S");
    dfsList.add("/root/D/M");
    dfsList.add("/root/E");
    return dfsList;
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
      InternalDataNodeTestUtils.mockDatanodeBlkPinning(dn, true);
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
            ((BlockStorageMovementAttemptedItems) (sps
                .getAttemptedItemsMonitor())).getAttemptedItemsCount());
        return ((BlockStorageMovementAttemptedItems) (sps
            .getAttemptedItemsMonitor()))
            .getAttemptedItemsCount() == expectedBlkMovAttemptedCount;
      }
    }, 100, timeout);
  }

  private void waitForBlocksMovementAttemptReport(
      long expectedMovementFinishedBlocksCount, int timeout)
          throws TimeoutException, InterruptedException {
    BlockManager blockManager = hdfsCluster.getNamesystem().getBlockManager();
    final StoragePolicySatisfier sps = blockManager.getStoragePolicySatisfier();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LOG.info("MovementFinishedBlocks: expectedCount={} actualCount={}",
            expectedMovementFinishedBlocksCount,
            ((BlockStorageMovementAttemptedItems) (sps
                .getAttemptedItemsMonitor())).getMovementFinishedBlocksCount());
        return ((BlockStorageMovementAttemptedItems) (sps
            .getAttemptedItemsMonitor()))
                .getMovementFinishedBlocksCount()
            >= expectedMovementFinishedBlocksCount;
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
    for (int i = 0; i < 1024; i++) {
      out.write(i);
    }
    out.close();
  }

  private void startAdditionalDNs(final Configuration conf,
      int newNodesRequired, int existingNodesNum, StorageType[][] newTypes,
      int storagesPerDn, long nodeCapacity, final MiniDFSCluster cluster)
          throws IOException {
    long[][] capacities;
    existingNodesNum += newNodesRequired;
    capacities = new long[newNodesRequired][storagesPerDn];
    for (int i = 0; i < newNodesRequired; i++) {
      for (int j = 0; j < storagesPerDn; j++) {
        capacities[i][j] = nodeCapacity;
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
}
