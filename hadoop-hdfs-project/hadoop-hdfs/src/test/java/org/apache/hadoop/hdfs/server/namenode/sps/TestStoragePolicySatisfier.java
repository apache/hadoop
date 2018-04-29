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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
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
import org.junit.Before;
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
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestStoragePolicySatisfier.class);
  private Configuration config = null;
  private StorageType[][] allDiskTypes =
      new StorageType[][]{{StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK}};
  private MiniDFSCluster hdfsCluster = null;
  private DistributedFileSystem dfs = null;
  public static final int NUM_OF_DATANODES = 3;
  public static final int STORAGES_PER_DATANODE = 2;
  public static final long CAPACITY = 2 * 256 * 1024 * 1024;
  public static final String FILE = "/testMoveToSatisfyStoragePolicy";
  public static final int DEFAULT_BLOCK_SIZE = 1024;
  private ExternalBlockMovementListener blkMoveListener =
      new ExternalBlockMovementListener();

  /**
   * Sets hdfs cluster.
   */
  public void setCluster(MiniDFSCluster cluster) {
    this.hdfsCluster = cluster;
  }

  /**
   * @return conf.
   */
  public Configuration getConf() {
    return this.config;
  }

  /**
   * @return hdfs cluster.
   */
  public MiniDFSCluster getCluster() {
    return hdfsCluster;
  }

  /**
   * Gets distributed file system.
   *
   * @throws IOException
   */
  public DistributedFileSystem getFS() throws IOException {
    this.dfs = hdfsCluster.getFileSystem();
    return this.dfs;
  }

  @After
  public void shutdownCluster() {
    if (hdfsCluster != null) {
      hdfsCluster.shutdown();
    }
  }

  public void createCluster() throws IOException {
    config.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    hdfsCluster = startCluster(config, allDiskTypes, NUM_OF_DATANODES,
        STORAGES_PER_DATANODE, CAPACITY);
    getFS();
    writeContent(FILE);
  }

  @Before
  public void setUp() {
    config = new HdfsConfiguration();
    config.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.INTERNAL.toString());
    // Most of the tests are restarting DNs and NN. So, reduced refresh cycle to
    // update latest datanodes.
    config.setLong(DFSConfigKeys.DFS_SPS_DATANODE_CACHE_REFRESH_INTERVAL_MS,
        3000);
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
    dfs.setStoragePolicy(new Path(FILE), COLD);

    StorageType[][] newtypes =
        new StorageType[][]{{StorageType.ARCHIVE, StorageType.ARCHIVE},
            {StorageType.ARCHIVE, StorageType.ARCHIVE},
            {StorageType.ARCHIVE, StorageType.ARCHIVE}};
    startAdditionalDNs(config, 3, NUM_OF_DATANODES, newtypes,
        STORAGES_PER_DATANODE, CAPACITY, hdfsCluster);

    hdfsCluster.triggerHeartbeats();
    dfs.satisfyStoragePolicy(new Path(FILE));
    // Wait till namenode notified about the block location details
    DFSTestUtil.waitExpectedStorageType(FILE, StorageType.ARCHIVE, 3, 35000,
        dfs);
  }

  @Test(timeout = 300000)
  public void testWhenStoragePolicySetToALLSSD()
      throws Exception {
    try {
      createCluster();
      // Change policy to ALL_SSD
      dfs.setStoragePolicy(new Path(FILE), "ALL_SSD");

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK},
              {StorageType.SSD, StorageType.DISK},
              {StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 3, NUM_OF_DATANODES, newtypes,
          STORAGES_PER_DATANODE, CAPACITY, hdfsCluster);
      dfs.satisfyStoragePolicy(new Path(FILE));
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier Identified that block to move to SSD
      // areas
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.SSD, 3, 30000, dfs);
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
      dfs.setStoragePolicy(new Path(FILE), ONE_SSD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 1, NUM_OF_DATANODES, newtypes,
          STORAGES_PER_DATANODE, CAPACITY, hdfsCluster);
      dfs.satisfyStoragePolicy(new Path(FILE));
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier Identified that block to move to SSD
      // areas
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.SSD, 1, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.DISK, 2, 30000,
          dfs);
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
      dfs.setStoragePolicy(new Path(FILE), ONE_SSD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 1, NUM_OF_DATANODES, newtypes,
          STORAGES_PER_DATANODE, CAPACITY, hdfsCluster);
      dfs.satisfyStoragePolicy(new Path(FILE));
      hdfsCluster.triggerHeartbeats();

      // Wait till the block is moved to SSD areas
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.SSD, 1, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.DISK, 2, 30000,
          dfs);

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
      files.add(FILE);

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
      startAdditionalDNs(config, 1, NUM_OF_DATANODES, newtypes,
          STORAGES_PER_DATANODE, CAPACITY, hdfsCluster);
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
      dfs.setStoragePolicy(new Path(FILE), COLD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.DISK, StorageType.ARCHIVE},
              {StorageType.DISK, StorageType.ARCHIVE},
              {StorageType.DISK, StorageType.ARCHIVE}};
      startAdditionalDNs(config, 3, NUM_OF_DATANODES, newtypes,
          STORAGES_PER_DATANODE, CAPACITY, hdfsCluster);

      hdfsAdmin.satisfyStoragePolicy(new Path(FILE));

      hdfsCluster.triggerHeartbeats();
      // Wait till namenode notified about the block location details
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.ARCHIVE, 3, 30000,
          dfs);
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
      startAdditionalDNs(config, 1, NUM_OF_DATANODES, newtypes,
          STORAGES_PER_DATANODE, CAPACITY, hdfsCluster);

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
      restartNamenode();
      HdfsAdmin hdfsAdmin =
          new HdfsAdmin(FileSystem.getDefaultUri(config), config);

      try {
        hdfsAdmin.satisfyStoragePolicy(new Path(FILE));
        Assert.fail(String.format(
            "Should failed to satisfy storage policy "
                + "for %s since %s is set to false.",
            FILE, DFS_STORAGE_POLICY_ENABLED_KEY));
      } catch (IOException e) {
        GenericTestUtils.assertExceptionContains(String.format(
            "Failed to satisfy storage policy since %s is set to false.",
            DFS_STORAGE_POLICY_ENABLED_KEY), e);
      }

      hdfsCluster.getConfiguration(0).
          setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY, true);
      restartNamenode();

      hdfsAdmin = new HdfsAdmin(FileSystem.getDefaultUri(config), config);
      try {
        hdfsAdmin.satisfyStoragePolicy(new Path(nonExistingFile));
        Assert.fail("Should throw FileNotFoundException for " +
            nonExistingFile);
      } catch (FileNotFoundException e) {

      }

      try {
        hdfsAdmin.satisfyStoragePolicy(new Path(FILE));
        hdfsAdmin.satisfyStoragePolicy(new Path(FILE));
      } catch (Exception e) {
        Assert.fail(String.format("Allow to invoke mutlipe times "
            + "#satisfyStoragePolicy() api for a path %s , internally just "
            + "skipping addtion to satisfy movement queue.", FILE));
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
      dfs.setStoragePolicy(new Path(FILE), COLD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.ARCHIVE, StorageType.ARCHIVE}};

      // Adding ARCHIVE based datanodes.
      startAdditionalDNs(config, 1, NUM_OF_DATANODES, newtypes,
          STORAGES_PER_DATANODE, CAPACITY, hdfsCluster);

      dfs.satisfyStoragePolicy(new Path(FILE));
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier identified that block to move to
      // ARCHIVE area.
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.ARCHIVE, 1, 30000,
          dfs);
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.DISK, 2, 30000,
          dfs);

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
      dfs.setStoragePolicy(new Path(FILE), COLD);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.DISK, StorageType.DISK}};
      // Adding DISK based datanodes
      startAdditionalDNs(config, 1, NUM_OF_DATANODES, newtypes,
          STORAGES_PER_DATANODE, CAPACITY, hdfsCluster);

      dfs.satisfyStoragePolicy(new Path(FILE));
      hdfsCluster.triggerHeartbeats();

      // No block movement will be scheduled as there is no target node
      // available with the required storage type.
      waitForAttemptedItems(1, 30000);
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.DISK, 3, 30000,
          dfs);
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
          DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
          StoragePolicySatisfierMode.NONE.toString());
      running = hdfsCluster.getFileSystem()
          .getClient().isInternalSatisfierRunning();
      Assert.assertFalse("SPS should stopped as configured.", running);

      // Simulate the case by creating MOVER_ID file
      out = hdfsCluster.getFileSystem().create(
          HdfsServerConstants.MOVER_ID_PATH);

      // Restart SPS
      hdfsCluster.getNameNode().reconfigureProperty(
          DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
          StoragePolicySatisfierMode.INTERNAL.toString());

      running = hdfsCluster.getFileSystem()
          .getClient().isInternalSatisfierRunning();
      Assert.assertFalse("SPS should not be able to run as file "
          + HdfsServerConstants.MOVER_ID_PATH + " is being hold.", running);

      // Simulate Mover exists
      out.close();
      out = null;
      hdfsCluster.getFileSystem().delete(
          HdfsServerConstants.MOVER_ID_PATH, true);

      // Restart SPS again
      hdfsCluster.getNameNode().reconfigureProperty(
          DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
          StoragePolicySatisfierMode.INTERNAL.toString());
      running = hdfsCluster.getFileSystem()
          .getClient().isInternalSatisfierRunning();
      Assert.assertTrue("SPS should be running as "
          + "Mover already exited", running);

      // Check functionality after SPS restart
      doTestWhenStoragePolicySetToCOLD();
    } catch (ReconfigurationException e) {
      throw new IOException("Exception when reconfigure "
          + DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY, e);
    } finally {
      if (out != null) {
        out.close();
      }
      shutdownCluster();
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
      restartNamenode();
      boolean running = hdfsCluster.getFileSystem()
          .getClient().isInternalSatisfierRunning();
      Assert.assertTrue("SPS should be running as "
          + "no Mover really running", running);
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
    try{
      config.setBoolean(DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED, true);
      hdfsCluster = startCluster(config, allDiskTypes, 3, 2, CAPACITY);

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
      startAdditionalDNs(config, 3, NUM_OF_DATANODES, newtypes,
          STORAGES_PER_DATANODE, CAPACITY, hdfsCluster);

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
      shutdownCluster();
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
      allDiskTypes =
          new StorageType[][]{{StorageType.DISK, StorageType.ARCHIVE},
              {StorageType.DISK, StorageType.DISK},
              {StorageType.DISK, StorageType.DISK},
              {StorageType.DISK, StorageType.DISK},
              {StorageType.DISK, StorageType.ARCHIVE}};
      hdfsCluster = startCluster(config, allDiskTypes, numOfDns,
          STORAGES_PER_DATANODE, CAPACITY);
      dfs = hdfsCluster.getFileSystem();
      writeContent(FILE, (short) 5);

      // Change policy to COLD
      dfs.setStoragePolicy(new Path(FILE), COLD);

      dfs.satisfyStoragePolicy(new Path(FILE));
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier identified that block to move to
      // ARCHIVE area.
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.ARCHIVE, 2, 30000,
          dfs);
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.DISK, 3, 30000,
          dfs);

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
    try {
      hdfsCluster = startCluster(config, diskTypes, NUM_OF_DATANODES,
          STORAGES_PER_DATANODE, CAPACITY);
      dfs = hdfsCluster.getFileSystem();
      writeContent(FILE);

      // Change policy to ONE_SSD
      dfs.setStoragePolicy(new Path(FILE), ONE_SSD);

      dfs.satisfyStoragePolicy(new Path(FILE));
      hdfsCluster.triggerHeartbeats();
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.SSD, 1, 30000, dfs);
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.DISK, 2, 30000,
          dfs);

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
    try {
      hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
          STORAGES_PER_DATANODE, CAPACITY);
      dfs = hdfsCluster.getFileSystem();
      writeContent(FILE);

      // Change policy to WARM
      dfs.setStoragePolicy(new Path(FILE), "WARM");
      dfs.satisfyStoragePolicy(new Path(FILE));
      hdfsCluster.triggerHeartbeats();

      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.DISK, 1, 30000,
          dfs);
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.ARCHIVE, 2, 30000,
          dfs);
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
      hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
          STORAGES_PER_DATANODE, CAPACITY);
      dfs = hdfsCluster.getFileSystem();
      // 1. Write two replica on disk
      DFSTestUtil.createFile(dfs, new Path(FILE), DEFAULT_BLOCK_SIZE,
          (short) 2, 0);
      // 2. Change policy to COLD, so third replica will be written to ARCHIVE.
      dfs.setStoragePolicy(new Path(FILE), "COLD");

      // 3.Change replication factor to 3.
      dfs.setReplication(new Path(FILE), (short) 3);

      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.DISK, 2, 30000,
          dfs);
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.ARCHIVE, 1, 30000,
          dfs);

      // 4. Change policy to HOT, so we can move the all block to DISK.
      dfs.setStoragePolicy(new Path(FILE), "HOT");

      // 4. Satisfy the policy.
      dfs.satisfyStoragePolicy(new Path(FILE));

      // 5. Block should move successfully .
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.DISK, 3, 30000,
          dfs);
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
    long dnCapacity = 1024 * DEFAULT_BLOCK_SIZE + (2 * DEFAULT_BLOCK_SIZE - 1);
    try {
      hdfsCluster = startCluster(config, diskTypes, NUM_OF_DATANODES,
          STORAGES_PER_DATANODE, dnCapacity);
      dfs = hdfsCluster.getFileSystem();
      writeContent(FILE);

      // Change policy to ONE_SSD
      dfs.setStoragePolicy(new Path(FILE), ONE_SSD);
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
      dfs.satisfyStoragePolicy(new Path(FILE));

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

      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.DISK, 3, 30000,
          dfs);
      DFSTestUtil.waitExpectedStorageType(FILE, StorageType.SSD, 0, 30000, dfs);
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
    try {
      hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
          STORAGES_PER_DATANODE, CAPACITY);
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
    try {
      hdfsCluster = startCluster(config, allDiskTypes, NUM_OF_DATANODES,
          STORAGES_PER_DATANODE, CAPACITY);
      hdfsCluster.waitActive();
      DistributedFileSystem fs = hdfsCluster.getFileSystem();
      Path filePath = new Path("/zeroSizeFile");
      DFSTestUtil.createFile(fs, filePath, 0, (short) 1, 0);
      FSEditLog editlog = hdfsCluster.getNameNode().getNamesystem()
          .getEditLog();
      long lastWrittenTxId = editlog.getLastWrittenTxId();
      fs.satisfyStoragePolicy(filePath);
      Assert.assertEquals("Xattr should not be added for the file",
          lastWrittenTxId, editlog.getLastWrittenTxId());
      INode inode = hdfsCluster.getNameNode().getNamesystem().getFSDirectory()
          .getINode(filePath.toString());
      Assert.assertTrue("XAttrFeature should be null for file",
          inode.getXAttrFeature() == null);
    } finally {
      shutdownCluster();
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
    try {
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
          "3000");
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY,
          "5000");
      StorageType[][] newtypes = new StorageType[][] {
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK}};
      hdfsCluster = startCluster(config, newtypes, 3, 2, CAPACITY);
      hdfsCluster.waitActive();
      DistributedFileSystem fs = hdfsCluster.getFileSystem();
      Path filePath = new Path("/zeroSizeFile");
      DFSTestUtil.createFile(fs, filePath, 1024, (short) 3, 0);
      fs.setStoragePolicy(filePath, "COLD");
      List<DataNodeProperties> list = new ArrayList<>();
      list.add(hdfsCluster.stopDataNode(0));
      list.add(hdfsCluster.stopDataNode(0));
      list.add(hdfsCluster.stopDataNode(0));
      restartNamenode();
      hdfsCluster.restartDataNode(list.get(0), false);
      hdfsCluster.restartDataNode(list.get(1), false);
      hdfsCluster.waitActive();
      fs.satisfyStoragePolicy(filePath);
      DFSTestUtil.waitExpectedStorageType(filePath.toString(),
          StorageType.ARCHIVE, 2, 30000, hdfsCluster.getFileSystem());
      hdfsCluster.restartDataNode(list.get(2), false);
      DFSTestUtil.waitExpectedStorageType(filePath.toString(),
          StorageType.ARCHIVE, 3, 30000, hdfsCluster.getFileSystem());
    } finally {
      shutdownCluster();
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
    try {
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
          "3000");
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY,
          "5000");
      StorageType[][] newtypes = new StorageType[][] {
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK}};
      hdfsCluster = startCluster(config, newtypes, 5, 2, CAPACITY);
      hdfsCluster.waitActive();
      DistributedFileSystem fs = hdfsCluster.getFileSystem();
      Path filePath = new Path("/zeroSizeFile");
      DFSTestUtil.createFile(fs, filePath, 1024, (short) 5, 0);
      fs.setReplication(filePath, (short) 3);
      LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(
          LogFactory.getLog(BlockStorageMovementAttemptedItems.class));
      fs.setStoragePolicy(filePath, "COLD");
      fs.satisfyStoragePolicy(filePath);
      DFSTestUtil.waitExpectedStorageType(filePath.toString(),
          StorageType.ARCHIVE, 3, 60000, hdfsCluster.getFileSystem());
      assertFalse("Log output does not contain expected log message: ",
          logs.getOutput().contains("some of the blocks are low redundant"));
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Test SPS for empty directory, xAttr should be removed.
   */
  @Test(timeout = 300000)
  public void testSPSForEmptyDirectory() throws IOException, TimeoutException,
      InterruptedException {
    try {
      hdfsCluster = startCluster(config, allDiskTypes, NUM_OF_DATANODES,
          STORAGES_PER_DATANODE, CAPACITY);
      hdfsCluster.waitActive();
      DistributedFileSystem fs = hdfsCluster.getFileSystem();
      Path emptyDir = new Path("/emptyDir");
      fs.mkdirs(emptyDir);
      fs.satisfyStoragePolicy(emptyDir);
      // Make sure satisfy xattr has been removed.
      DFSTestUtil.waitForXattrRemoved("/emptyDir",
          XATTR_SATISFY_STORAGE_POLICY, hdfsCluster.getNamesystem(), 30000);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Test SPS for not exist directory.
   */
  @Test(timeout = 300000)
  public void testSPSForNonExistDirectory() throws Exception {
    try {
      hdfsCluster = startCluster(config, allDiskTypes, NUM_OF_DATANODES,
          STORAGES_PER_DATANODE, CAPACITY);
      hdfsCluster.waitActive();
      DistributedFileSystem fs = hdfsCluster.getFileSystem();
      Path emptyDir = new Path("/emptyDir");
      try {
        fs.satisfyStoragePolicy(emptyDir);
        fail("FileNotFoundException should throw");
      } catch (FileNotFoundException e) {
        // nothing to do
      }
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Test SPS for directory tree which doesn't have files.
   */
  @Test(timeout = 300000)
  public void testSPSWithDirectoryTreeWithoutFile() throws Exception {
    try {
      hdfsCluster = startCluster(config, allDiskTypes, NUM_OF_DATANODES,
          STORAGES_PER_DATANODE, CAPACITY);
      hdfsCluster.waitActive();
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
      DistributedFileSystem fs = hdfsCluster.getFileSystem();
      fs.mkdirs(new Path("/root/C/H/O"));
      fs.mkdirs(new Path("/root/A"));
      fs.mkdirs(new Path("/root/D"));
      fs.mkdirs(new Path("/root/C/G"));
      fs.mkdirs(new Path("/root/C/I"));
      fs.satisfyStoragePolicy(new Path("/root"));
      // Make sure satisfy xattr has been removed.
      DFSTestUtil.waitForXattrRemoved("/root",
          XATTR_SATISFY_STORAGE_POLICY, hdfsCluster.getNamesystem(), 30000);
    } finally {
      shutdownCluster();
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
      hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
          STORAGES_PER_DATANODE, CAPACITY);
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
      // Set queue max capacity
      config.setInt(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_KEY,
          5);
      hdfsCluster = startCluster(config, diskTypes, diskTypes.length,
          STORAGES_PER_DATANODE, CAPACITY);
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
        STORAGES_PER_DATANODE, CAPACITY);
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
    StoragePolicySatisfier<Long> sps = new StoragePolicySatisfier<Long>(config);
    Context<Long> ctxt = new IntraSPSNameNodeContext(
        hdfsCluster.getNamesystem(),
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

    FileCollector<Long> fileIDCollector = createFileIdCollector(sps, ctxt);
    sps.init(ctxt, fileIDCollector, null, null);
    sps.getStorageMovementQueue().activate();

    INode rootINode = fsDir.getINode("/root");
    hdfsCluster.getNamesystem().getBlockManager().getSPSManager()
        .addPathId(rootINode.getId());

    //Wait for thread to reach U.
    Thread.sleep(1000);
    dfs.delete(new Path("/root/D/L"), true);


    assertTraversal(expectedTraverseOrder, fsDir, sps);
    dfs.delete(new Path("/root"), true);
  }

  public FileCollector<Long> createFileIdCollector(
      StoragePolicySatisfier<Long> sps, Context<Long> ctxt) {
    FileCollector<Long> fileIDCollector = new IntraSPSNameNodeFileIdCollector(
        hdfsCluster.getNamesystem().getFSDirectory(), sps);
    return fileIDCollector;
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
        STORAGES_PER_DATANODE, CAPACITY);
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
    StoragePolicySatisfier<Long> sps = new StoragePolicySatisfier<Long>(config);
    Context<Long> ctxt = new IntraSPSNameNodeContext(
        hdfsCluster.getNamesystem(),
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
    FileCollector<Long> fileIDCollector = createFileIdCollector(sps, ctxt);
    sps.init(ctxt, fileIDCollector, null, null);
    sps.getStorageMovementQueue().activate();

    INode rootINode = fsDir.getINode("/root");
    hdfsCluster.getNamesystem().getBlockManager().getSPSManager()
        .addPathId(rootINode.getId());

    // Wait for thread to reach U.
    Thread.sleep(1000);

    dfs.delete(new Path("/root/D/L"), true);

    assertTraversal(expectedTraverseOrder, fsDir, sps);
    dfs.delete(new Path("/root"), true);
  }

  private void assertTraversal(List<String> expectedTraverseOrder,
      FSDirectory fsDir, StoragePolicySatisfier<Long> sps)
          throws InterruptedException {
    // Remove 10 element and make queue free, So other traversing will start.
    for (int i = 0; i < 10; i++) {
      String path = expectedTraverseOrder.remove(0);
      ItemInfo<Long> itemInfo = sps.getStorageMovementQueue().get();
      if (itemInfo == null) {
        continue;
      }
      Long trackId = itemInfo.getFile();
      INode inode = fsDir.getInode(trackId);
      assertTrue("Failed to traverse tree, expected " + path + " but got "
          + inode.getFullPathName(), path.equals(inode.getFullPathName()));
    }
    // Wait to finish tree traverse
    Thread.sleep(5000);

    // Check other element traversed in order and E, M, U, R, S should not be
    // added in queue which we already removed from expected list
    for (String path : expectedTraverseOrder) {
      ItemInfo<Long> itemInfo = sps.getStorageMovementQueue().get();
      if (itemInfo == null) {
        continue;
      }
      Long trackId = itemInfo.getFile();
      INode inode = fsDir.getInode(trackId);
      assertTrue("Failed to traverse tree, expected " + path + " but got "
          + inode.getFullPathName(), path.equals(inode.getFullPathName()));
    }
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
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
          "3000");
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY,
          "5000");
      config.setBoolean(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_LOW_MAX_STREAMS_PREFERENCE_KEY,
          false);

      StorageType[][] storagetypes = new StorageType[][] {
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK}};

      hdfsCluster = startCluster(config, storagetypes, 2, 2, CAPACITY);
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
      startAdditionalDNs(config, 2, NUM_OF_DATANODES, newtypes,
          STORAGES_PER_DATANODE, CAPACITY, hdfsCluster);

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
            StorageType.DISK, 4, 60000, hdfsCluster.getFileSystem());
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
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
          "3000");
      config.setBoolean(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_LOW_MAX_STREAMS_PREFERENCE_KEY,
          false);

      StorageType[][] storagetypes = new StorageType[][] {
          {StorageType.ARCHIVE, StorageType.DISK},
          {StorageType.ARCHIVE, StorageType.DISK}};
      hdfsCluster = startCluster(config, storagetypes, 2, 2, CAPACITY);
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
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
          "1000");
      config.set(DFSConfigKeys
          .DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY,
          "1000");
      StorageType[][] storagetypes = new StorageType[][] {
          {StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK}};
      hdfsCluster = startCluster(config, storagetypes, 2, 2, CAPACITY);
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

  public void waitForAttemptedItems(long expectedBlkMovAttemptedCount,
      int timeout) throws TimeoutException, InterruptedException {
    BlockManager blockManager = hdfsCluster.getNamesystem().getBlockManager();
    final StoragePolicySatisfier<Long> sps =
        (StoragePolicySatisfier<Long>) blockManager.getSPSManager()
        .getInternalSPSService();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LOG.info("expectedAttemptedItemsCount={} actualAttemptedItemsCount={}",
            expectedBlkMovAttemptedCount,
            ((BlockStorageMovementAttemptedItems<Long>) (sps
                .getAttemptedItemsMonitor())).getAttemptedItemsCount());
        return ((BlockStorageMovementAttemptedItems<Long>) (sps
            .getAttemptedItemsMonitor()))
            .getAttemptedItemsCount() == expectedBlkMovAttemptedCount;
      }
    }, 100, timeout);
  }

  public void waitForBlocksMovementAttemptReport(
      long expectedMovementFinishedBlocksCount, int timeout)
          throws TimeoutException, InterruptedException {
    Assert.assertNotNull("Didn't set external block move listener",
        blkMoveListener);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        int actualCount = blkMoveListener.getActualBlockMovements().size();
        LOG.info("MovementFinishedBlocks: expectedCount={} actualCount={}",
            expectedMovementFinishedBlocksCount,
            actualCount);
        return actualCount
            >= expectedMovementFinishedBlocksCount;
      }
    }, 100, timeout);
  }

  public void writeContent(final String fileName) throws IOException {
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

  public MiniDFSCluster startCluster(final Configuration conf,
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

    // Sets external listener for assertion.
    blkMoveListener.clear();
    BlockManager blockManager = cluster.getNamesystem().getBlockManager();
    final StoragePolicySatisfier<Long> sps =
        (StoragePolicySatisfier<Long>) blockManager
        .getSPSManager().getInternalSPSService();
    sps.setBlockMovementListener(blkMoveListener);
    return cluster;
  }

  public void restartNamenode() throws IOException {
    hdfsCluster.restartNameNodes();
    hdfsCluster.waitActive();
    BlockManager blockManager = hdfsCluster.getNamesystem().getBlockManager();
    StoragePolicySatisfyManager spsMgr = blockManager.getSPSManager();
    if (spsMgr != null && spsMgr.isInternalSatisfierRunning()) {
      // Sets external listener for assertion.
      blkMoveListener.clear();
      final StoragePolicySatisfier<Long> sps =
          (StoragePolicySatisfier<Long>) spsMgr.getInternalSPSService();
      sps.setBlockMovementListener(blkMoveListener);
    }
  }

  /**
   * Implementation of listener callback, where it collects all the sps move
   * attempted blocks for assertion.
   */
  public static final class ExternalBlockMovementListener
      implements BlockMovementListener {

    private List<Block> actualBlockMovements = new ArrayList<>();

    @Override
    public void notifyMovementTriedBlocks(Block[] moveAttemptFinishedBlks) {
      for (Block block : moveAttemptFinishedBlks) {
        actualBlockMovements.add(block);
      }
      LOG.info("Movement attempted blocks:{}", actualBlockMovements);
    }

    public List<Block> getActualBlockMovements() {
      return actualBlockMovements;
    }

    public void clear() {
      actualBlockMovements.clear();
    }
  }
}
