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

import java.util.function.Supplier;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi.FsVolumeReferences;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;


public class TestNameNodePrunesMissingStorages {
  static final Logger LOG =
      LoggerFactory.getLogger(TestNameNodePrunesMissingStorages.class);


  private static void runTest(final String testCaseName,
                              final boolean createFiles,
                              final int numInitialStorages,
                              final int expectedStoragesAfterTest) throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster
          .Builder(conf)
          .numDataNodes(1)
          .storagesPerDatanode(numInitialStorages)
          .build();
      cluster.waitActive();

      final DataNode dn0 = cluster.getDataNodes().get(0);

      // Ensure NN knows about the storage.
      final DatanodeID dnId = dn0.getDatanodeId();
      final DatanodeDescriptor dnDescriptor =
          cluster.getNamesystem().getBlockManager().getDatanodeManager().getDatanode(dnId);
      assertThat(dnDescriptor.getStorageInfos().length, is(numInitialStorages));

      final String bpid = cluster.getNamesystem().getBlockPoolId();
      final DatanodeRegistration dnReg = dn0.getDNRegistrationForBP(bpid);
      DataNodeTestUtils.triggerBlockReport(dn0);

      if (createFiles) {
        final Path path = new Path("/", testCaseName);
        DFSTestUtil.createFile(
            cluster.getFileSystem(), path, 1024, (short) 1, 0x1BAD5EED);
        DataNodeTestUtils.triggerBlockReport(dn0);
      }

      // Generate a fake StorageReport that is missing one storage.
      final StorageReport reports[] =
          dn0.getFSDataset().getStorageReports(bpid);
      final StorageReport prunedReports[] = new StorageReport[numInitialStorages - 1];
      System.arraycopy(reports, 0, prunedReports, 0, prunedReports.length);

      // Stop the DataNode and send fake heartbeat with missing storage.
      cluster.stopDataNode(0);
      cluster.getNameNodeRpc().sendHeartbeat(dnReg, prunedReports, 0L, 0L, 0, 0,
          0, null, true, SlowPeerReports.EMPTY_REPORT,
          SlowDiskReports.EMPTY_REPORT);

      // Check that the missing storage was pruned.
      assertThat(dnDescriptor.getStorageInfos().length, is(expectedStoragesAfterTest));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test that the NameNode prunes empty storage volumes that are no longer
   * reported by the DataNode.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testUnusedStorageIsPruned() throws IOException {
    // Run the test with 1 storage, after the text expect 0 storages.
    runTest(GenericTestUtils.getMethodName(), false, 1, 0);
  }

  /**
   * Verify that the NameNode does not prune storages with blocks
   * simply as a result of a heartbeat being sent missing that storage.
   *
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testStorageWithBlocksIsNotPruned() throws IOException {
    // Run the test with 1 storage, after the text still expect 1 storage.
    runTest(GenericTestUtils.getMethodName(), true, 1, 1);
  }

  /**
   * Regression test for HDFS-7960.<p/>
   *
   * Shutting down a datanode, removing a storage directory, and restarting
   * the DataNode should not produce zombie storages.
   */
  @Test(timeout=300000)
  public void testRemovingStorageDoesNotProduceZombies() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        1000);
    final int NUM_STORAGES_PER_DN = 2;
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf).numDataNodes(3)
        .storagesPerDatanode(NUM_STORAGES_PER_DN)
        .build();
    try {
      cluster.waitActive();
      for (DataNode dn : cluster.getDataNodes()) {
        assertEquals(NUM_STORAGES_PER_DN,
          cluster.getNamesystem().getBlockManager().
              getDatanodeManager().getDatanode(dn.getDatanodeId()).
              getStorageInfos().length);
      }
      // Create a file which will end up on all 3 datanodes.
      final Path TEST_PATH = new Path("/foo1");
      DistributedFileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH, 1024, (short) 3, 0xcafecafe);
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.triggerBlockReport(dn);
      }
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, new Path("/foo1"));
      cluster.getNamesystem().writeLock();
      final String storageIdToRemove;
      String datanodeUuid;
      // Find the first storage which this block is in.
      try {
        BlockInfo storedBlock =
            cluster.getNamesystem().getBlockManager().
                getStoredBlock(block.getLocalBlock());
        Iterator<DatanodeStorageInfo> storageInfoIter =
            cluster.getNamesystem().getBlockManager().
                blocksMap.getStorages(storedBlock).iterator();
        assertTrue(storageInfoIter.hasNext());
        DatanodeStorageInfo info = storageInfoIter.next();
        storageIdToRemove = info.getStorageID();
        datanodeUuid = info.getDatanodeDescriptor().getDatanodeUuid();
      } finally {
        cluster.getNamesystem().writeUnlock();
      }
      // Find the DataNode which holds that first storage.
      final DataNode datanodeToRemoveStorageFrom;
      int datanodeToRemoveStorageFromIdx = 0;
      while (true) {
        if (datanodeToRemoveStorageFromIdx >= cluster.getDataNodes().size()) {
          Assert.fail("failed to find datanode with uuid " + datanodeUuid);
          datanodeToRemoveStorageFrom = null;
          break;
        }
        DataNode dn = cluster.getDataNodes().
            get(datanodeToRemoveStorageFromIdx);
        if (dn.getDatanodeUuid().equals(datanodeUuid)) {
          datanodeToRemoveStorageFrom = dn;
          break;
        }
        datanodeToRemoveStorageFromIdx++;
      }
      // Find the volume within the datanode which holds that first storage.
      StorageLocation volumeLocationToRemove = null;
      try (FsVolumeReferences volumes =
          datanodeToRemoveStorageFrom.getFSDataset().getFsVolumeReferences()) {
        assertEquals(NUM_STORAGES_PER_DN, volumes.size());
        for (FsVolumeSpi volume : volumes) {
          if (volume.getStorageID().equals(storageIdToRemove)) {
            volumeLocationToRemove = volume.getStorageLocation();
          }
        }
      };
      // Shut down the datanode and remove the volume.
      // Replace the volume directory with a regular file, which will
      // cause a volume failure.  (If we merely removed the directory,
      // it would be re-initialized with a new storage ID.)
      assertNotNull(volumeLocationToRemove);
      datanodeToRemoveStorageFrom.shutdown();
      FileUtil.fullyDelete(new File(volumeLocationToRemove.getUri()));
      FileOutputStream fos = new FileOutputStream(
          new File(volumeLocationToRemove.getUri()));
      try {
        fos.write(1);
      } finally {
        fos.close();
      }
      cluster.restartDataNode(datanodeToRemoveStorageFromIdx);
      // Wait for the NameNode to remove the storage.
      LOG.info("waiting for the datanode to remove " + storageIdToRemove);
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          final DatanodeDescriptor dnDescriptor =
              cluster.getNamesystem().getBlockManager().getDatanodeManager().
                  getDatanode(datanodeToRemoveStorageFrom.getDatanodeUuid());
          assertNotNull(dnDescriptor);
          DatanodeStorageInfo[] infos = dnDescriptor.getStorageInfos();
          for (DatanodeStorageInfo info : infos) {
            if (info.getStorageID().equals(storageIdToRemove)) {
              LOG.info("Still found storage " + storageIdToRemove + " on " +
                  info + ".");
              return false;
            }
          }
          assertEquals(NUM_STORAGES_PER_DN - 1, infos.length);
          return true;
        }
      }, 1000, 30000);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private static void rewriteVersionFile(File versionFile,
                            String newStorageId) throws IOException {
    BufferedReader in = new BufferedReader(new FileReader(versionFile));
    File newVersionFile =
        new File(versionFile.getParent(), UUID.randomUUID().toString());
    Writer out = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(newVersionFile), "UTF-8"));
    final String STORAGE_ID = "storageID=";
    boolean success = false;
    try {
      String line;
      while ((line = in.readLine()) != null) {
        if (line.startsWith(STORAGE_ID)) {
          out.write(STORAGE_ID + newStorageId + "\n");
        } else {
          out.write(line + "\n");
        }
      }
      in.close();
      in = null;
      out.close();
      out = null;
      // Delete old version file
      success = versionFile.delete();
      success &= newVersionFile.renameTo(versionFile);
    } finally {
      if (in != null) {
        in.close();
      }
      if (out != null) {
        out.close();
      }
      if (!success) {
        versionFile.delete();
      }
    }
  }

  @Test(timeout=300000)
  public void testRenamingStorageIds() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 0);
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf).numDataNodes(1)
        .storagesPerDatanode(1)
        .build();
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.ALL);
    try {
      cluster.waitActive();
      final Path TEST_PATH = new Path("/foo1");
      DistributedFileSystem fs = cluster.getFileSystem();
      // Create a file and leave it open
      DFSTestUtil.createFile(fs, TEST_PATH, 1, (short)1, 0xdeadbeef);
      // Find the volume within the datanode which holds that first storage.
      DataNode dn = cluster.getDataNodes().get(0);
      FsVolumeReferences volumeRefs =
          dn.getFSDataset().getFsVolumeReferences();
      final String newStorageId = DatanodeStorage.generateUuid();
      try {
        File currentDir = new File(
            new File(volumeRefs.get(0).getStorageLocation().getUri()),
            "current");
        File versionFile = new File(currentDir, "VERSION");
        rewriteVersionFile(versionFile, newStorageId);
      } finally {
        volumeRefs.close();
      }
      final ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, TEST_PATH);
      cluster.restartDataNodes();
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          cluster.getNamesystem().writeLock();
          try {
            Iterator<DatanodeStorageInfo> storageInfoIter =
                cluster.getNamesystem().getBlockManager().
                    getStorages(block.getLocalBlock()).iterator();
            if (!storageInfoIter.hasNext()) {
              LOG.info("Expected to find a storage for " +
                      block.getBlockName() + ", but nothing was found.  " +
                      "Continuing to wait.");
              return false;
            }
            DatanodeStorageInfo info = storageInfoIter.next();
            if (!newStorageId.equals(info.getStorageID())) {
              LOG.info("Expected " + block.getBlockName() + " to " +
                  "be in storage id " + newStorageId + ", but it " +
                  "was in " + info.getStorageID() + ".  Continuing " +
                  "to wait.");
              return false;
            }
            LOG.info("Successfully found " + block.getBlockName() + " in " +
                "be in storage id " + newStorageId);
          } finally {
            cluster.getNamesystem().writeUnlock();
          }
          return true;
        }
      }, 20, 100000);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=300000)
  public void testNameNodePrunesUnreportedStorages() throws Exception {
    Configuration conf = new HdfsConfiguration();
    // Create a cluster with one datanode with two storages
    MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf).numDataNodes(1)
        .storagesPerDatanode(2)
        .build();
    try {
      cluster.waitActive();
      // Create two files to ensure each storage has a block
      DFSTestUtil.createFile(cluster.getFileSystem(), new Path("file1"),
          102400, 102400, 102400, (short)1,
          0x1BAD5EE);
      DFSTestUtil.createFile(cluster.getFileSystem(), new Path("file2"),
          102400, 102400, 102400, (short)1,
          0x1BAD5EED);
      // Get the datanode storages and data directories
      DataNode dn = cluster.getDataNodes().get(0);
      BlockManager bm =
          cluster.getNameNode().getNamesystem().getBlockManager();
      DatanodeDescriptor dnDescriptor = bm.getDatanodeManager().
          getDatanode(cluster.getDataNodes().get(0).getDatanodeUuid());
      DatanodeStorageInfo[] dnStoragesInfosBeforeRestart =
          dnDescriptor.getStorageInfos();
      Collection<String> oldDirs =  new ArrayList<String>(dn.getConf().
          getTrimmedStringCollection(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY));
      // Keep the first data directory and remove the second.
      String newDirs = oldDirs.iterator().next();
      conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, newDirs);
      // Restart the datanode with the new conf
      cluster.stopDataNode(0);
      cluster.startDataNodes(conf, 1, false, null, null);
      dn = cluster.getDataNodes().get(0);
      cluster.waitActive();
      // Assert that the dnDescriptor has both the storages after restart
      assertArrayEquals(dnStoragesInfosBeforeRestart,
          dnDescriptor.getStorageInfos());
      // Assert that the removed storage is marked as FAILED
      // when DN heartbeats to the NN
      int numFailedStoragesWithBlocks = 0;
      DatanodeStorageInfo failedStorageInfo = null;
      for (DatanodeStorageInfo dnStorageInfo: dnDescriptor.getStorageInfos()) {
        if (dnStorageInfo.areBlocksOnFailedStorage()) {
          numFailedStoragesWithBlocks++;
          failedStorageInfo = dnStorageInfo;
        }
      }
      assertEquals(1, numFailedStoragesWithBlocks);
      // Heartbeat manager removes the blocks associated with this failed
      // storage
      bm.getDatanodeManager().getHeartbeatManager().heartbeatCheck();
      assertTrue(!failedStorageInfo.areBlocksOnFailedStorage());
      // pruneStorageMap removes the unreported storage
      cluster.triggerHeartbeats();
      // Assert that the unreported storage is pruned
      assertEquals(DataNode.getStorageLocations(dn.getConf()).size(),
          dnDescriptor.getStorageInfos().length);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
