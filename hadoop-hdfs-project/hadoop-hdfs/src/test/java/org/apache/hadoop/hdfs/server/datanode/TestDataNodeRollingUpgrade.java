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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.Builder;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.TestRollingUpgrade;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

/**
 * Ensure that the DataNode correctly handles rolling upgrade
 * finalize and rollback.
 */
public class TestDataNodeRollingUpgrade {
  private static final Log LOG = LogFactory.getLog(TestDataNodeRollingUpgrade.class);

  private static final short REPL_FACTOR = 1;
  private static final int BLOCK_SIZE = 1024 * 1024;
  private static final long FILE_SIZE = BLOCK_SIZE;
  private static final long SEED = 0x1BADF00DL;

  Configuration conf;
  MiniDFSCluster cluster = null;
  DistributedFileSystem fs = null;
  DataNode dn0 = null;
  NameNode nn = null;
  String blockPoolId = null;

  private void startCluster() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt("dfs.blocksize", 1024*1024);
    cluster = new Builder(conf).numDataNodes(REPL_FACTOR).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    nn = cluster.getNameNode(0);
    assertNotNull(nn);
    dn0 = cluster.getDataNodes().get(0);
    assertNotNull(dn0);
    blockPoolId = cluster.getNameNode(0).getNamesystem().getBlockPoolId();
  }

  private void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    fs = null;
    nn = null;
    dn0 = null;
    blockPoolId = null;
  }

  private void triggerHeartBeats() throws Exception {
    // Sleep briefly so that DN learns of the rolling upgrade
    // state and other states from heartbeats.
    cluster.triggerHeartbeats();
    Thread.sleep(5000);
  }

  /** Test assumes that the file has a single block */
  private File getBlockForFile(Path path, boolean exists) throws IOException {
    LocatedBlocks blocks = nn.getRpcServer().getBlockLocations(path.toString(),
        0, Long.MAX_VALUE);
    assertEquals("The test helper functions assume that each file has a single block",
                 1, blocks.getLocatedBlocks().size());
    ExtendedBlock block = blocks.getLocatedBlocks().get(0).getBlock();
    BlockLocalPathInfo bInfo = dn0.getFSDataset().getBlockLocalPathInfo(block);
    File blockFile = new File(bInfo.getBlockPath());
    assertEquals(exists, blockFile.exists());
    return blockFile;
  }

  private File getTrashFileForBlock(File blockFile, boolean exists) {
    File trashFile = new File(
        dn0.getStorage().getTrashDirectoryForBlockFile(blockPoolId, blockFile));
    assertEquals(exists, trashFile.exists());
    return trashFile;
  }

  /**
   * Ensures that the blocks belonging to the deleted file are in trash
   */
  private void deleteAndEnsureInTrash(Path pathToDelete,
      File blockFile, File trashFile) throws Exception {
    assertTrue(blockFile.exists());
    assertFalse(trashFile.exists());

    // Now delete the file and ensure the corresponding block in trash
    LOG.info("Deleting file " + pathToDelete + " during rolling upgrade");
    fs.delete(pathToDelete, false);
    assert(!fs.exists(pathToDelete));
    triggerHeartBeats();
    assertTrue(trashFile.exists());
    assertFalse(blockFile.exists());
  }

  private boolean isTrashRootPresent() {
    // Trash is disabled; trash root does not exist
    BlockPoolSliceStorage bps = dn0.getStorage().getBPStorage(blockPoolId);
    return bps.trashEnabled();
  }

  /**
   * Ensures that the blocks from trash are restored
   */
  private void ensureTrashRestored(File blockFile, File trashFile)
      throws Exception {
    assertTrue(blockFile.exists());
    assertFalse(trashFile.exists());
    assertFalse(isTrashRootPresent());
  }

  private boolean isBlockFileInPrevious(File blockFile) {
    Pattern blockFilePattern = Pattern.compile(String.format(
      "^(.*%1$scurrent%1$s.*%1$s)(current)(%1$s.*)$",
      Pattern.quote(File.separator)));
    Matcher matcher = blockFilePattern.matcher(blockFile.toString());
    String previousFileName = matcher.replaceFirst("$1" + "previous" + "$3");
    return ((new File(previousFileName)).exists());
  }

  private void startRollingUpgrade() throws Exception {
    LOG.info("Starting rolling upgrade");
    fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    final DFSAdmin dfsadmin = new DFSAdmin(conf);
    TestRollingUpgrade.runCmd(dfsadmin, true, "-rollingUpgrade", "prepare");
    triggerHeartBeats();

    // Ensure datanode rolling upgrade is started
    assertTrue(dn0.getFSDataset().trashEnabled(blockPoolId));
  }

  private void finalizeRollingUpgrade() throws Exception {
    LOG.info("Finalizing rolling upgrade");
    final DFSAdmin dfsadmin = new DFSAdmin(conf);
    TestRollingUpgrade.runCmd(dfsadmin, true, "-rollingUpgrade", "finalize");
    triggerHeartBeats();

    // Ensure datanode rolling upgrade is started
    assertFalse(dn0.getFSDataset().trashEnabled(blockPoolId));
    BlockPoolSliceStorage bps = dn0.getStorage().getBPStorage(blockPoolId);
    assertFalse(bps.trashEnabled());
  }

  private void rollbackRollingUpgrade() throws Exception {
    // Shutdown datanodes and namenodes
    // Restart the namenode with rolling upgrade rollback
    LOG.info("Starting rollback of the rolling upgrade");
    MiniDFSCluster.DataNodeProperties dnprop = cluster.stopDataNode(0);
    dnprop.setDnArgs("-rollback");
    cluster.shutdownNameNodes();
    cluster.restartNameNode("-rollingupgrade", "rollback");
    cluster.restartDataNode(dnprop);
    cluster.waitActive();
    nn = cluster.getNameNode(0);
    dn0 = cluster.getDataNodes().get(0);
    triggerHeartBeats();
    LOG.info("The cluster is active after rollback");
  }

  @Test (timeout=600000)
  public void testDatanodeRollingUpgradeWithFinalize() throws Exception {
    try {
      startCluster();
      rollingUpgradeAndFinalize();
      // Do it again
      rollingUpgradeAndFinalize();
    } finally {
      shutdownCluster();
    }
  }

  @Test(timeout = 600000)
  public void testDatanodeRUwithRegularUpgrade() throws Exception {
    try {
      startCluster();
      rollingUpgradeAndFinalize();
      DataNodeProperties dn = cluster.stopDataNode(0);
      cluster.restartNameNode(0, true, "-upgrade");
      cluster.restartDataNode(dn, true);
      cluster.waitActive();
      fs = cluster.getFileSystem(0);
      Path testFile3 = new Path("/" + GenericTestUtils.getMethodName()
          + ".03.dat");
      DFSTestUtil.createFile(fs, testFile3, FILE_SIZE, REPL_FACTOR, SEED);
      cluster.getFileSystem().finalizeUpgrade();
    } finally {
      shutdownCluster();
    }
  }

  private void rollingUpgradeAndFinalize() throws IOException, Exception {
    // Create files in DFS.
    Path testFile1 = new Path("/" + GenericTestUtils.getMethodName() + ".01.dat");
    Path testFile2 = new Path("/" + GenericTestUtils.getMethodName() + ".02.dat");
    DFSTestUtil.createFile(fs, testFile1, FILE_SIZE, REPL_FACTOR, SEED);
    DFSTestUtil.createFile(fs, testFile2, FILE_SIZE, REPL_FACTOR, SEED);

    startRollingUpgrade();
    File blockFile = getBlockForFile(testFile2, true);
    File trashFile = getTrashFileForBlock(blockFile, false);
    cluster.triggerBlockReports();
    deleteAndEnsureInTrash(testFile2, blockFile, trashFile);
    finalizeRollingUpgrade();

    // Ensure that delete file testFile2 stays deleted after finalize
    assertFalse(isTrashRootPresent());
    assert(!fs.exists(testFile2));
    assert(fs.exists(testFile1));
  }

  @Test (timeout=600000)
  public void testDatanodeRollingUpgradeWithRollback() throws Exception {
    try {
      startCluster();

      // Create files in DFS.
      Path testFile1 = new Path("/" + GenericTestUtils.getMethodName() + ".01.dat");
      DFSTestUtil.createFile(fs, testFile1, FILE_SIZE, REPL_FACTOR, SEED);
      String fileContents1 = DFSTestUtil.readFile(fs, testFile1);

      startRollingUpgrade();

      File blockFile = getBlockForFile(testFile1, true);
      File trashFile = getTrashFileForBlock(blockFile, false);
      deleteAndEnsureInTrash(testFile1, blockFile, trashFile);

      // Now perform a rollback to restore DFS to the pre-rollback state.
      rollbackRollingUpgrade();

      // Ensure that block was restored from trash
      ensureTrashRestored(blockFile, trashFile);

      // Ensure that files exist and restored file contents are the same.
      assert(fs.exists(testFile1));
      String fileContents2 = DFSTestUtil.readFile(fs, testFile1);
      assertThat(fileContents1, is(fileContents2));
    } finally {
      shutdownCluster();
    }
  }
  
  @Test (timeout=600000)
  // Test DatanodeXceiver has correct peer-dataxceiver pairs for sending OOB message
  public void testDatanodePeersXceiver() throws Exception {
    try {
      startCluster();

      // Create files in DFS.
      String testFile1 = "/" + GenericTestUtils.getMethodName() + ".01.dat";
      String testFile2 = "/" + GenericTestUtils.getMethodName() + ".02.dat";
      String testFile3 = "/" + GenericTestUtils.getMethodName() + ".03.dat";

      DFSClient client1 = new DFSClient(NameNode.getAddress(conf), conf);
      DFSClient client2 = new DFSClient(NameNode.getAddress(conf), conf);
      DFSClient client3 = new DFSClient(NameNode.getAddress(conf), conf);

      DFSOutputStream s1 = (DFSOutputStream) client1.create(testFile1, true);
      DFSOutputStream s2 = (DFSOutputStream) client2.create(testFile2, true);
      DFSOutputStream s3 = (DFSOutputStream) client3.create(testFile3, true);

      byte[] toWrite = new byte[1024*1024*8];
      Random rb = new Random(1111);
      rb.nextBytes(toWrite);
      s1.write(toWrite, 0, 1024*1024*8);
      s1.flush();
      s2.write(toWrite, 0, 1024*1024*8);
      s2.flush();
      s3.write(toWrite, 0, 1024*1024*8);
      s3.flush();       

      assertTrue(dn0.getXferServer().getNumPeersXceiver() == dn0.getXferServer()
          .getNumPeersXceiver());
      s1.close();
      s2.close();
      s3.close();
      assertTrue(dn0.getXferServer().getNumPeersXceiver() == dn0.getXferServer()
          .getNumPeersXceiver());
      client1.close();
      client2.close();
      client3.close();      
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Support for layout version change with rolling upgrade was
   * added by HDFS-6800 and HDFS-6981.
   */
  @Test(timeout=300000)
  public void testWithLayoutChangeAndFinalize() throws Exception {
    final long seed = 0x600DF00D;
    try {
      startCluster();

      Path[] paths = new Path[3];
      File[] blockFiles = new File[3];

      // Create two files in DFS.
      for (int i = 0; i < 2; ++i) {
        paths[i] = new Path("/" + GenericTestUtils.getMethodName() + "." + i + ".dat");
        DFSTestUtil.createFile(fs, paths[i], BLOCK_SIZE, (short) 2, seed);
      }

      startRollingUpgrade();

      // Delete the first file. The DN will save its block files in trash.
      blockFiles[0] = getBlockForFile(paths[0], true);
      File trashFile0 = getTrashFileForBlock(blockFiles[0], false);
      deleteAndEnsureInTrash(paths[0], blockFiles[0], trashFile0);

      // Restart the DN with a new layout version to trigger layout upgrade.
      LOG.info("Shutting down the Datanode");
      MiniDFSCluster.DataNodeProperties dnprop = cluster.stopDataNode(0);
      DFSTestUtil.addDataNodeLayoutVersion(
          DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION - 1,
          "Test Layout for TestDataNodeRollingUpgrade");
      LOG.info("Restarting the DataNode");
      cluster.restartDataNode(dnprop, true);
      cluster.waitActive();

      dn0 = cluster.getDataNodes().get(0);
      LOG.info("The DN has been restarted");
      assertFalse(trashFile0.exists());
      assertFalse(dn0.getStorage().getBPStorage(blockPoolId).isTrashAllowed(blockFiles[0]));

      // Ensure that the block file for the first file was moved from 'trash' to 'previous'.
      assertTrue(isBlockFileInPrevious(blockFiles[0]));
      assertFalse(isTrashRootPresent());

      // Delete the second file. Ensure that its block file is in previous.
      blockFiles[1] = getBlockForFile(paths[1], true);
      fs.delete(paths[1], false);
      assertTrue(isBlockFileInPrevious(blockFiles[1]));
      assertFalse(isTrashRootPresent());

      // Finalize and ensure that neither block file exists in trash or previous.
      finalizeRollingUpgrade();
      assertFalse(isTrashRootPresent());
      assertFalse(isBlockFileInPrevious(blockFiles[0]));
      assertFalse(isBlockFileInPrevious(blockFiles[1]));
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Support for layout version change with rolling upgrade was
   * added by HDFS-6800 and HDFS-6981.
   */
  @Test(timeout=300000)
  public void testWithLayoutChangeAndRollback() throws Exception {
    final long seed = 0x600DF00D;
    try {
      startCluster();

      Path[] paths = new Path[3];
      File[] blockFiles = new File[3];

      // Create two files in DFS.
      for (int i = 0; i < 2; ++i) {
        paths[i] = new Path("/" + GenericTestUtils.getMethodName() + "." + i + ".dat");
        DFSTestUtil.createFile(fs, paths[i], BLOCK_SIZE, (short) 1, seed);
      }

      startRollingUpgrade();

      // Delete the first file. The DN will save its block files in trash.
      blockFiles[0] = getBlockForFile(paths[0], true);
      File trashFile0 = getTrashFileForBlock(blockFiles[0], false);
      deleteAndEnsureInTrash(paths[0], blockFiles[0], trashFile0);

      // Restart the DN with a new layout version to trigger layout upgrade.
      LOG.info("Shutting down the Datanode");
      MiniDFSCluster.DataNodeProperties dnprop = cluster.stopDataNode(0);
      DFSTestUtil.addDataNodeLayoutVersion(
          DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION - 1,
          "Test Layout for TestDataNodeRollingUpgrade");
      LOG.info("Restarting the DataNode");
      cluster.restartDataNode(dnprop, true);
      cluster.waitActive();

      dn0 = cluster.getDataNodes().get(0);
      LOG.info("The DN has been restarted");
      assertFalse(trashFile0.exists());
      assertFalse(dn0.getStorage().getBPStorage(blockPoolId).isTrashAllowed(blockFiles[0]));

      // Ensure that the block file for the first file was moved from 'trash' to 'previous'.
      assertTrue(isBlockFileInPrevious(blockFiles[0]));
      assertFalse(isTrashRootPresent());

      // Delete the second file. Ensure that its block file is in previous.
      blockFiles[1] = getBlockForFile(paths[1], true);
      fs.delete(paths[1], false);
      assertTrue(isBlockFileInPrevious(blockFiles[1]));
      assertFalse(isTrashRootPresent());

      // Create and delete a third file. Its block file should not be
      // in either trash or previous after deletion.
      paths[2] = new Path("/" + GenericTestUtils.getMethodName() + ".2.dat");
      DFSTestUtil.createFile(fs, paths[2], BLOCK_SIZE, (short) 1, seed);
      blockFiles[2] = getBlockForFile(paths[2], true);
      fs.delete(paths[2], false);
      assertFalse(isBlockFileInPrevious(blockFiles[2]));
      assertFalse(isTrashRootPresent());

      // Rollback and ensure that the first two file contents were restored.
      rollbackRollingUpgrade();
      for (int i = 0; i < 2; ++i) {
        byte[] actual = DFSTestUtil.readFileBuffer(fs, paths[i]);
        byte[] calculated = DFSTestUtil.calculateFileContentsFromSeed(seed, BLOCK_SIZE);
        assertArrayEquals(actual, calculated);
      }

      // And none of the block files must be in previous or trash.
      assertFalse(isTrashRootPresent());
      for (int i = 0; i < 3; ++i) {
        assertFalse(isBlockFileInPrevious(blockFiles[i]));
      }
    } finally {
      shutdownCluster();
    }
  }
}
