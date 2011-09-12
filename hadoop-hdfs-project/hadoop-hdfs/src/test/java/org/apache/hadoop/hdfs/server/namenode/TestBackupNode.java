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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import junit.framework.TestCase;

public class TestBackupNode extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestBackupNode.class);

  
  static {
    ((Log4JLogger)Checkpointer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)BackupImage.LOG).getLogger().setLevel(Level.ALL);
  }
  
  static final String BASE_DIR = MiniDFSCluster.getBaseDirectory();

  protected void setUp() throws Exception {
    super.setUp();
    File baseDir = new File(BASE_DIR);
    if(baseDir.exists())
      if(!(FileUtil.fullyDelete(baseDir)))
        throw new IOException("Cannot remove directory: " + baseDir);
    File dirC = new File(getBackupNodeDir(StartupOption.CHECKPOINT, 1));
    dirC.mkdirs();
    File dirB = new File(getBackupNodeDir(StartupOption.BACKUP, 1));
    dirB.mkdirs();
    dirB = new File(getBackupNodeDir(StartupOption.BACKUP, 2));
    dirB.mkdirs();
  }

  static String getBackupNodeDir(StartupOption t, int idx) {
    return BASE_DIR + "name" + t.getName() + idx + "/";
  }

  BackupNode startBackupNode(Configuration conf,
                             StartupOption startupOpt,
                             int idx) throws IOException {
    Configuration c = new HdfsConfiguration(conf);
    String dirs = getBackupNodeDir(startupOpt, idx);
    c.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, dirs);
    c.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        "${" + DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY + "}");
    c.set(DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY,
        "127.0.0.1:0");

    return (BackupNode)NameNode.createNameNode(new String[]{startupOpt.getName()}, c);
  }

  void waitCheckpointDone(
      MiniDFSCluster cluster, BackupNode backup, long txid) {
    long thisCheckpointTxId;
    do {
      try {
        LOG.info("Waiting checkpoint to complete... " +
            "checkpoint txid should increase above " + txid);
        Thread.sleep(1000);
      } catch (Exception e) {}
      thisCheckpointTxId = backup.getFSImage().getStorage()
        .getMostRecentCheckpointTxId();

    } while (thisCheckpointTxId < txid);
    
    // Check that the checkpoint got uploaded to NN successfully
    FSImageTestUtil.assertNNHasCheckpoints(cluster,
        Collections.singletonList((int)thisCheckpointTxId));
  }

  public void testCheckpointNode() throws Exception {
    testCheckpoint(StartupOption.CHECKPOINT);
  }
  
  /**
   * Ensure that the backupnode will tail edits from the NN
   * and keep in sync, even while the NN rolls, checkpoints
   * occur, etc.
   */
  public void testBackupNodeTailsEdits() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    BackupNode backup = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(0).build();
      fileSys = cluster.getFileSystem();
      backup = startBackupNode(conf, StartupOption.BACKUP, 1);
      
      BackupImage bnImage = backup.getBNImage();
      testBNInSync(cluster, backup, 1);
      
      // Force a roll -- BN should roll with NN.
      NameNode nn = cluster.getNameNode();
      nn.rollEditLog();
      assertEquals(bnImage.getEditLog().getCurSegmentTxId(),
          nn.getFSImage().getEditLog().getCurSegmentTxId());
      
      // BN should stay in sync after roll
      testBNInSync(cluster, backup, 2);
      
      long nnImageBefore =
        nn.getFSImage().getStorage().getMostRecentCheckpointTxId();
      // BN checkpoint
      backup.doCheckpoint();
      
      // NN should have received a new image
      long nnImageAfter =
        nn.getFSImage().getStorage().getMostRecentCheckpointTxId();
      
      assertTrue("nn should have received new checkpoint. before: " +
          nnImageBefore + " after: " + nnImageAfter,
          nnImageAfter > nnImageBefore);

      // BN should stay in sync after checkpoint
      testBNInSync(cluster, backup, 3);

      // Stop BN
      StorageDirectory sd = bnImage.getStorage().getStorageDir(0);
      backup.stop();
      backup = null;
      
      // When shutting down the BN, it shouldn't finalize logs that are
      // still open on the NN
      EditLogFile editsLog = FSImageTestUtil.findLatestEditsLog(sd);
      assertEquals(editsLog.getFirstTxId(),
          nn.getFSImage().getEditLog().getCurSegmentTxId());
      assertTrue("Should not have finalized " + editsLog,
          editsLog.isInProgress());
      
      // do some edits
      assertTrue(fileSys.mkdirs(new Path("/edit-while-bn-down")));
      
      // start a new backup node
      backup = startBackupNode(conf, StartupOption.BACKUP, 1);

      testBNInSync(cluster, backup, 4);
      assertNotNull(backup.getNamesystem().getFileInfo("/edit-while-bn-down", false));
    } finally {
      LOG.info("Shutting down...");
      if (backup != null) backup.stop();
      if (fileSys != null) fileSys.close();
      if (cluster != null) cluster.shutdown();
    }
    
    assertStorageDirsMatch(cluster.getNameNode(), backup);
  }

  private void testBNInSync(MiniDFSCluster cluster, final BackupNode backup,
      int testIdx) throws Exception {
    
    final NameNode nn = cluster.getNameNode();
    final FileSystem fs = cluster.getFileSystem();

    // Do a bunch of namespace operations, make sure they're replicated
    // to the BN.
    for (int i = 0; i < 10; i++) {
      final String src = "/test_" + testIdx + "_" + i;
      LOG.info("Creating " + src + " on NN");
      Path p = new Path(src);
      assertTrue(fs.mkdirs(p));
      
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          LOG.info("Checking for " + src + " on BN");
          try {
            boolean hasFile = backup.getNamesystem().getFileInfo(src, false) != null;
            boolean txnIdMatch = backup.getTransactionID() == nn.getTransactionID();
            return hasFile && txnIdMatch;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }, 30, 10000);
    }
    
    assertStorageDirsMatch(nn, backup);
  }

  private void assertStorageDirsMatch(final NameNode nn, final BackupNode backup)
      throws Exception {
    // Check that the stored files in the name dirs are identical
    List<File> dirs = Lists.newArrayList(
        FSImageTestUtil.getCurrentDirs(nn.getFSImage().getStorage(),
            null));
    dirs.addAll(FSImageTestUtil.getCurrentDirs(backup.getFSImage().getStorage(),
        null));
    FSImageTestUtil.assertParallelFilesAreIdentical(dirs, ImmutableSet.of("VERSION"));
  }
  
  public void testBackupNode() throws Exception {
    testCheckpoint(StartupOption.BACKUP);
  }  

  void testCheckpoint(StartupOption op) throws Exception {
    Path file1 = new Path("checkpoint.dat");
    Path file2 = new Path("checkpoint2.dat");

    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY, "0");
    conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1); // disable block scanner
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 1);
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    BackupNode backup = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(0).build();
      fileSys = cluster.getFileSystem();
      //
      // verify that 'format' really blew away all pre-existing files
      //
      assertTrue(!fileSys.exists(file1));
      assertTrue(!fileSys.exists(file2));

      //
      // Create file1
      //
      assertTrue(fileSys.mkdirs(file1));

      //
      // Take a checkpoint
      //
      long txid = cluster.getNameNode().getTransactionID();
      backup = startBackupNode(conf, op, 1);
      waitCheckpointDone(cluster, backup, txid);
    } catch(IOException e) {
      LOG.error("Error in TestBackupNode:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if(backup != null) backup.stop();
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
    File nnCurDir = new File(BASE_DIR, "name1/current/");
    File bnCurDir = new File(getBackupNodeDir(op, 1), "/current/");

    FSImageTestUtil.assertParallelFilesAreIdentical(
        ImmutableList.of(bnCurDir, nnCurDir),
        ImmutableSet.<String>of("VERSION"));
    
    try {
      //
      // Restart cluster and verify that file1 still exist.
      //
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
                                                .format(false).build();
      fileSys = cluster.getFileSystem();
      // check that file1 still exists
      assertTrue(fileSys.exists(file1));
      fileSys.delete(file1, true);

      // create new file file2
      fileSys.mkdirs(file2);

      //
      // Take a checkpoint
      //
      backup = startBackupNode(conf, op, 1);
      long txid = cluster.getNameNode().getTransactionID();
      waitCheckpointDone(cluster, backup, txid);

      for (int i = 0; i < 10; i++) {
        fileSys.mkdirs(new Path("file_" + i));
      }

      txid = cluster.getNameNode().getTransactionID();
      backup.doCheckpoint();
      waitCheckpointDone(cluster, backup, txid);

      txid = cluster.getNameNode().getTransactionID();
      backup.doCheckpoint();
      waitCheckpointDone(cluster, backup, txid);

    } catch(IOException e) {
      LOG.error("Error in TestBackupNode:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if(backup != null) backup.stop();
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
    FSImageTestUtil.assertParallelFilesAreIdentical(
        ImmutableList.of(bnCurDir, nnCurDir),
        ImmutableSet.<String>of("VERSION"));

    try {
      //
      // Restart cluster and verify that file2 exists and
      // file1 does not exist.
      //
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).format(false).build();
      fileSys = cluster.getFileSystem();

      assertTrue(!fileSys.exists(file1));

      // verify that file2 exists
      assertTrue(fileSys.exists(file2));
    } catch(IOException e) {
      LOG.error("Error in TestBackupNode:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
