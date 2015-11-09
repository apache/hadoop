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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.directory.api.ldap.aci.UserClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class TestBackupNode {
  public static final Log LOG = LogFactory.getLog(TestBackupNode.class);

  
  static {
    GenericTestUtils.setLogLevel(Checkpointer.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(BackupImage.LOG, Level.ALL);
  }
  
  static final String BASE_DIR = MiniDFSCluster.getBaseDirectory();
  
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;

  @Before
  public void setUp() throws Exception {
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
    c.set(DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY,
            "127.0.0.1:0");

    BackupNode bn = (BackupNode)NameNode.createNameNode(
        new String[]{startupOpt.getName()}, c);
    assertTrue(bn.getRole() + " must be in SafeMode.", bn.isInSafeMode());
    assertTrue(bn.getRole() + " must be in StandbyState",
               bn.getNamesystem().getHAState()
                 .equalsIgnoreCase(HAServiceState.STANDBY.name()));
    return bn;
  }

  void waitCheckpointDone(MiniDFSCluster cluster, long txid) {
    long thisCheckpointTxId;
    do {
      try {
        LOG.info("Waiting checkpoint to complete... " +
            "checkpoint txid should increase above " + txid);
        Thread.sleep(1000);
      } catch (Exception e) {}
      // The checkpoint is not done until the nn has received it from the bn
      thisCheckpointTxId = cluster.getNameNode().getFSImage().getStorage()
        .getMostRecentCheckpointTxId();
    } while (thisCheckpointTxId < txid);
    // Check that the checkpoint got uploaded to NN successfully
    FSImageTestUtil.assertNNHasCheckpoints(cluster,
        Collections.singletonList((int)thisCheckpointTxId));
  }


  /**
   *  Regression test for HDFS-9249.
   *  This test configures the primary name node with SIMPLE authentication,
   *  and configures the backup node with Kerberose authentication with
   *  invalid keytab settings.
   *
   *  This configuration causes the backup node to throw a NPE trying to abort
   *  the edit log.
   *  */
  @Test
    public void startBackupNodeWithIncorrectAuthentication() throws IOException {
    Configuration c = new HdfsConfiguration();
    StartupOption startupOpt = StartupOption.CHECKPOINT;
    String dirs = getBackupNodeDir(startupOpt, 1);
    c.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://127.0.0.1:1234");
    c.set(DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, "localhost:0");
    c.set(DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY, "0");
    c.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY,
        -1); // disable block scanner
    c.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 1);
    c.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, dirs);
    c.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        "${" + DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY + "}");
    c.set(DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY,
        "127.0.0.1:0");
    c.set(DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY,
        "127.0.0.1:0");

    NameNode nn;
    try {
      Configuration nnconf = new HdfsConfiguration(c);
      DFSTestUtil.formatNameNode(nnconf);
      nn = NameNode.createNameNode(new String[] {}, nnconf);
    } catch (IOException e) {
      LOG.info("IOException is thrown creating name node");
      throw e;
    }

    c.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    c.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, "");

    BackupNode bn = null;
    try {
      bn = (BackupNode)NameNode.createNameNode(
          new String[] {startupOpt.getName()}, c);
      assertTrue("Namesystem in BackupNode should be null",
          bn.getNamesystem() == null);
      fail("Incorrect authentication setting should throw IOException");
    } catch (IOException e) {
      LOG.info("IOException thrown as expected", e);
    } finally {
      if (nn != null) {
        nn.stop();
      }
      if (bn != null) {
        bn.stop();
      }
      SecurityUtil.setAuthenticationMethod(
          UserGroupInformation.AuthenticationMethod.SIMPLE, c);
      // reset security authentication
      UserGroupInformation.setConfiguration(c);
    }
  }

  @Test
  public void testCheckpointNode() throws Exception {
    testCheckpoint(StartupOption.CHECKPOINT);
  }
  
  /**
   * Ensure that the backupnode will tail edits from the NN
   * and keep in sync, even while the NN rolls, checkpoints
   * occur, etc.
   */
  @Test
  public void testBackupNodeTailsEdits() throws Exception {
    Configuration conf = new HdfsConfiguration();
    HAUtil.setAllowStandbyReads(conf, true);
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    BackupNode backup = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(0).build();
      fileSys = cluster.getFileSystem();
      backup = startBackupNode(conf, StartupOption.BACKUP, 1);
      
      BackupImage bnImage = (BackupImage) backup.getFSImage();
      testBNInSync(cluster, backup, 1);
      
      // Force a roll -- BN should roll with NN.
      NameNode nn = cluster.getNameNode();
      NamenodeProtocols nnRpc = nn.getRpcServer();
      nnRpc.rollEditLog();
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
      
      // Trigger an unclean shutdown of the backup node. Backup node will not
      // unregister from the active when this is done simulating a node crash.
      backup.stop(false);
           
      // do some edits on the active. This should go through without failing.
      // This will verify that active is still up and can add entries to
      // master editlog.
      assertTrue(fileSys.mkdirs(new Path("/edit-while-bn-down-2")));
      
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
            boolean txnIdMatch =
              backup.getRpcServer().getTransactionID() ==
              nn.getRpcServer().getTransactionID();
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
  
  @Test
  public void testBackupNode() throws Exception {
    testCheckpoint(StartupOption.BACKUP);
  }  

  void testCheckpoint(StartupOption op) throws Exception {
    Path file1 = new Path("/checkpoint.dat");
    Path file2 = new Path("/checkpoint2.dat");
    Path file3 = new Path("/backup.dat");

    Configuration conf = new HdfsConfiguration();
    HAUtil.setAllowStandbyReads(conf, true);
    short replication = (short)conf.getInt("dfs.replication", 3);
    int numDatanodes = Math.max(3, replication);
    conf.set(DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, "localhost:0");
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
      long txid = cluster.getNameNodeRpc().getTransactionID();
      backup = startBackupNode(conf, op, 1);
      waitCheckpointDone(cluster, txid);
    } catch(IOException e) {
      LOG.error("Error in TestBackupNode:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if(backup != null) backup.stop();
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
    File nnCurDir = new File(MiniDFSCluster.getNameNodeDirectory(BASE_DIR, 0, 0)[0], "current/");
    File bnCurDir = new File(getBackupNodeDir(op, 1), "/current/");

    FSImageTestUtil.assertParallelFilesAreIdentical(
        ImmutableList.of(bnCurDir, nnCurDir),
        ImmutableSet.<String>of("VERSION"));
    
    try {
      //
      // Restart cluster and verify that file1 still exist.
      //
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
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
      long txid = cluster.getNameNodeRpc().getTransactionID();
      backup = startBackupNode(conf, op, 1);
      waitCheckpointDone(cluster, txid);

      for (int i = 0; i < 10; i++) {
        fileSys.mkdirs(new Path("file_" + i));
      }

      txid = cluster.getNameNodeRpc().getTransactionID();
      backup.doCheckpoint();
      waitCheckpointDone(cluster, txid);

      txid = cluster.getNameNodeRpc().getTransactionID();
      backup.doCheckpoint();
      waitCheckpointDone(cluster, txid);

      // Try BackupNode operations
      InetSocketAddress add = backup.getNameNodeAddress();
      // Write to BN
      FileSystem bnFS = FileSystem.get(new Path("hdfs://"
          + NetUtils.getHostPortString(add)).toUri(), conf);
      boolean canWrite = true;
      try {
        DFSTestUtil.createFile(bnFS, file3, fileSize, fileSize, blockSize,
            replication, seed);
      } catch (IOException eio) {
        LOG.info("Write to " + backup.getRole() + " failed as expected: ", eio);
        canWrite = false;
      }
      assertFalse("Write to BackupNode must be prohibited.", canWrite);

      // Reads are allowed for BackupNode, but not for CheckpointNode
      boolean canRead = true;
      try {
        bnFS.exists(file2);
      } catch (IOException eio) {
        LOG.info("Read from " + backup.getRole() + " failed: ", eio);
        canRead = false;
      }
      assertEquals("Reads to BackupNode are allowed, but not CheckpointNode.",
          canRead, backup.isRole(NamenodeRole.BACKUP));

      DFSTestUtil.createFile(fileSys, file3, fileSize, fileSize, blockSize,
          replication, seed);
      
      TestCheckpoint.checkFile(fileSys, file3, replication);
      // should also be on BN right away
      assertTrue("file3 does not exist on BackupNode",
          op != StartupOption.BACKUP ||
          backup.getNamesystem().getFileInfo(
              file3.toUri().getPath(), false) != null);

    } catch(IOException e) {
      LOG.error("Error in TestBackupNode:", e);
      throw new AssertionError(e);
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
      LOG.error("Error in TestBackupNode: ", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /**
   * Verify that a file can be read both from NameNode and BackupNode.
   */
  @Test
  public void testCanReadData() throws IOException {
    Path file1 = new Path("/fileToRead.dat");
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    BackupNode backup = null;
    try {
      // Start NameNode and BackupNode
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(0).format(true).build();
      fileSys = cluster.getFileSystem();
      long txid = cluster.getNameNodeRpc().getTransactionID();
      backup = startBackupNode(conf, StartupOption.BACKUP, 1);
      waitCheckpointDone(cluster, txid);

      // Setup dual NameNode configuration for DataNodes
      String rpcAddrKeyPreffix =
          DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".bnCluster";
      String nnAddr = cluster.getNameNode().getNameNodeAddressHostPortString();
          conf.get(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
      String bnAddr = backup.getNameNodeAddressHostPortString();
      conf.set(DFSConfigKeys.DFS_NAMESERVICES, "bnCluster");
      conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, "bnCluster");
      conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".bnCluster",
          "nnActive, nnBackup");
      conf.set(rpcAddrKeyPreffix + ".nnActive", nnAddr);
      conf.set(rpcAddrKeyPreffix + ".nnBackup", bnAddr);
      cluster.startDataNodes(conf, 3, true, StartupOption.REGULAR, null);

      DFSTestUtil.createFile(
          fileSys, file1, fileSize, fileSize, blockSize, (short)3, seed);

      // Read the same file from file systems pointing to NN and BN
      FileSystem bnFS = FileSystem.get(
          new Path("hdfs://" + bnAddr).toUri(), conf);
      String nnData = DFSTestUtil.readFile(fileSys, file1);
      String bnData = DFSTestUtil.readFile(bnFS, file1);
      assertEquals("Data read from BackupNode and NameNode is not the same.",
          nnData, bnData);
    } catch(IOException e) {
      LOG.error("Error in TestBackupNode: ", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if(fileSys != null) fileSys.close();
      if(backup != null) backup.stop();
      if(cluster != null) cluster.shutdown();
    }
  }
}
