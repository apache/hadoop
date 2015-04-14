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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster.Builder;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.util.BestEffortLongFile;
import org.apache.hadoop.hdfs.util.PersistentLongFile;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import org.mockito.internal.util.reflection.Whitebox;

/**
 * Tests for upgrading with HA enabled.
 */
public class TestDFSUpgradeWithHA {

  private static final Log LOG = LogFactory.getLog(TestDFSUpgradeWithHA.class);
  
  private Configuration conf;
  
  @Before
  public void createConfiguration() {
    conf = new HdfsConfiguration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
  }

  private static void assertCTimesEqual(MiniDFSCluster cluster) {
    long nn1CTime = cluster.getNamesystem(0).getFSImage().getStorage().getCTime();
    long nn2CTime = cluster.getNamesystem(1).getFSImage().getStorage().getCTime();
    assertEquals(nn1CTime, nn2CTime);
  }

  private static void checkClusterPreviousDirExistence(MiniDFSCluster cluster,
      boolean shouldExist) {
    for (int i = 0; i < 2; i++) {
      checkNnPreviousDirExistence(cluster, i, shouldExist);
    }
  }

  private static void checkNnPreviousDirExistence(MiniDFSCluster cluster,
      int index, boolean shouldExist) {
    Collection<URI> nameDirs = cluster.getNameDirs(index);
    for (URI nnDir : nameDirs) {
      checkPreviousDirExistence(new File(nnDir), shouldExist);
    }
  }

  private static void checkJnPreviousDirExistence(MiniQJMHACluster jnCluster,
      boolean shouldExist) throws IOException {
    for (int i = 0; i < 3; i++) {
      checkPreviousDirExistence(
          jnCluster.getJournalCluster().getJournalDir(i, "ns1"), shouldExist);
    }
    if (shouldExist) {
      assertEpochFilesCopied(jnCluster);
    }
  }

  private static void assertEpochFilesCopied(MiniQJMHACluster jnCluster)
      throws IOException {
    for (int i = 0; i < 3; i++) {
      File journalDir = jnCluster.getJournalCluster().getJournalDir(i, "ns1");
      File currDir = new File(journalDir, "current");
      File prevDir = new File(journalDir, "previous");
      for (String fileName : new String[]{ Journal.LAST_PROMISED_FILENAME,
          Journal.LAST_WRITER_EPOCH }) {
        File prevFile = new File(prevDir, fileName);
        // Possible the prev file doesn't exist, e.g. if there has never been a
        // writer before the upgrade.
        if (prevFile.exists()) {
          PersistentLongFile prevLongFile = new PersistentLongFile(prevFile, -10);
          PersistentLongFile currLongFile = new PersistentLongFile(new File(currDir,
              fileName), -11);
          assertTrue("Value in " + fileName + " has decreased on upgrade in "
              + journalDir, prevLongFile.get() <= currLongFile.get());
        }
      }
    }
  }

  private static void checkPreviousDirExistence(File rootDir,
      boolean shouldExist) {
    File previousDir = new File(rootDir, "previous");
    if (shouldExist) {
      assertTrue(previousDir + " does not exist", previousDir.exists());
    } else {
      assertFalse(previousDir + " does exist", previousDir.exists());
    }
  }
  
  private void runFinalizeCommand(MiniDFSCluster cluster)
      throws IOException {
    HATestUtil.setFailoverConfigurations(cluster, conf);
    new DFSAdmin(conf).finalizeUpgrade();
  }
  
  /**
   * Ensure that an admin cannot finalize an HA upgrade without at least one NN
   * being active.
   */
  @Test
  public void testCannotFinalizeIfNoActive() throws IOException,
      URISyntaxException {
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleHATopology())
          .numDataNodes(0)
          .build();

      File sharedDir = new File(cluster.getSharedEditsDir(0, 1));
      
      // No upgrade is in progress at the moment.
      checkClusterPreviousDirExistence(cluster, false);
      assertCTimesEqual(cluster);
      checkPreviousDirExistence(sharedDir, false);
      
      // Transition NN0 to active and do some FS ops.
      cluster.transitionToActive(0);
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      assertTrue(fs.mkdirs(new Path("/foo1")));
      
      // Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
      // flag.
      cluster.shutdownNameNode(1);
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.UPGRADE);
      cluster.restartNameNode(0, false);
      
      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, false);
      checkPreviousDirExistence(sharedDir, true);
      
      // NN0 should come up in the active state when given the -upgrade option,
      // so no need to transition it to active.
      assertTrue(fs.mkdirs(new Path("/foo2")));
      
      // Restart NN0 without the -upgrade flag, to make sure that works.
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.REGULAR);
      cluster.restartNameNode(0, false);
      
      // Make sure we can still do FS ops after upgrading.
      cluster.transitionToActive(0);
      assertTrue(fs.mkdirs(new Path("/foo3")));
      
      // Now bootstrap the standby with the upgraded info.
      int rc = BootstrapStandby.run(
          new String[]{"-force"},
          cluster.getConfiguration(1));
      assertEquals(0, rc);
      
      // Now restart NN1 and make sure that we can do ops against that as well.
      cluster.restartNameNode(1);
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      assertTrue(fs.mkdirs(new Path("/foo4")));
      
      assertCTimesEqual(cluster);
      
      // Now there's no active NN.
      cluster.transitionToStandby(1);

      try {
        runFinalizeCommand(cluster);
        fail("Should not have been able to finalize upgrade with no NN active");
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains(
            "Cannot finalize with no NameNode active", ioe);
      }
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Make sure that an HA NN with NFS-based HA can successfully start and
   * upgrade.
   */
  @Test
  public void testNfsUpgrade() throws IOException, URISyntaxException {
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleHATopology())
          .numDataNodes(0)
          .build();
      
      File sharedDir = new File(cluster.getSharedEditsDir(0, 1));
      
      // No upgrade is in progress at the moment.
      checkClusterPreviousDirExistence(cluster, false);
      assertCTimesEqual(cluster);
      checkPreviousDirExistence(sharedDir, false);
      
      // Transition NN0 to active and do some FS ops.
      cluster.transitionToActive(0);
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      assertTrue(fs.mkdirs(new Path("/foo1")));
      
      // Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
      // flag.
      cluster.shutdownNameNode(1);
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.UPGRADE);
      cluster.restartNameNode(0, false);
      
      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, false);
      checkPreviousDirExistence(sharedDir, true);
      
      // NN0 should come up in the active state when given the -upgrade option,
      // so no need to transition it to active.
      assertTrue(fs.mkdirs(new Path("/foo2")));
      
      // Restart NN0 without the -upgrade flag, to make sure that works.
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.REGULAR);
      cluster.restartNameNode(0, false);
      
      // Make sure we can still do FS ops after upgrading.
      cluster.transitionToActive(0);
      assertTrue(fs.mkdirs(new Path("/foo3")));
      
      // Now bootstrap the standby with the upgraded info.
      int rc = BootstrapStandby.run(
          new String[]{"-force"},
          cluster.getConfiguration(1));
      assertEquals(0, rc);
      
      // Now restart NN1 and make sure that we can do ops against that as well.
      cluster.restartNameNode(1);
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      assertTrue(fs.mkdirs(new Path("/foo4")));
      
      assertCTimesEqual(cluster);
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private long getCommittedTxnIdValue(MiniQJMHACluster qjCluster)
      throws IOException {
    Journal journal1 = qjCluster.getJournalCluster().getJournalNode(0)
        .getOrCreateJournal(MiniQJMHACluster.NAMESERVICE);
    BestEffortLongFile committedTxnId = (BestEffortLongFile) Whitebox
        .getInternalState(journal1, "committedTxnId");
    return committedTxnId != null ? committedTxnId.get() :
        HdfsConstants.INVALID_TXID;
  }

  /**
   * Make sure that an HA NN can successfully upgrade when configured using
   * JournalNodes.
   */
  @Test
  public void testUpgradeWithJournalNodes() throws IOException,
      URISyntaxException {
    MiniQJMHACluster qjCluster = null;
    FileSystem fs = null;
    try {
      Builder builder = new MiniQJMHACluster.Builder(conf);
      builder.getDfsBuilder()
          .numDataNodes(0);
      qjCluster = builder.build();

      MiniDFSCluster cluster = qjCluster.getDfsCluster();
      
      // No upgrade is in progress at the moment.
      checkJnPreviousDirExistence(qjCluster, false);
      checkClusterPreviousDirExistence(cluster, false);
      assertCTimesEqual(cluster);
      
      // Transition NN0 to active and do some FS ops.
      cluster.transitionToActive(0);
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      assertTrue(fs.mkdirs(new Path("/foo1")));

      // get the value of the committedTxnId in journal nodes
      final long cidBeforeUpgrade = getCommittedTxnIdValue(qjCluster);

      // Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
      // flag.
      cluster.shutdownNameNode(1);
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.UPGRADE);
      cluster.restartNameNode(0, false);
      
      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, false);
      checkJnPreviousDirExistence(qjCluster, true);

      assertTrue(cidBeforeUpgrade <= getCommittedTxnIdValue(qjCluster));
      
      // NN0 should come up in the active state when given the -upgrade option,
      // so no need to transition it to active.
      assertTrue(fs.mkdirs(new Path("/foo2")));
      
      // Restart NN0 without the -upgrade flag, to make sure that works.
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.REGULAR);
      cluster.restartNameNode(0, false);
      
      // Make sure we can still do FS ops after upgrading.
      cluster.transitionToActive(0);
      assertTrue(fs.mkdirs(new Path("/foo3")));

      assertTrue(getCommittedTxnIdValue(qjCluster) > cidBeforeUpgrade);
      
      // Now bootstrap the standby with the upgraded info.
      int rc = BootstrapStandby.run(
          new String[]{"-force"},
          cluster.getConfiguration(1));
      assertEquals(0, rc);
      
      // Now restart NN1 and make sure that we can do ops against that as well.
      cluster.restartNameNode(1);
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      assertTrue(fs.mkdirs(new Path("/foo4")));
      
      assertCTimesEqual(cluster);
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (qjCluster != null) {
        qjCluster.shutdown();
      }
    }
  }

  @Test
  public void testFinalizeWithJournalNodes() throws IOException,
      URISyntaxException {
    MiniQJMHACluster qjCluster = null;
    FileSystem fs = null;
    try {
      Builder builder = new MiniQJMHACluster.Builder(conf);
      builder.getDfsBuilder()
          .numDataNodes(0);
      qjCluster = builder.build();

      MiniDFSCluster cluster = qjCluster.getDfsCluster();
      
      // No upgrade is in progress at the moment.
      checkJnPreviousDirExistence(qjCluster, false);
      checkClusterPreviousDirExistence(cluster, false);
      assertCTimesEqual(cluster);
      
      // Transition NN0 to active and do some FS ops.
      cluster.transitionToActive(0);
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      assertTrue(fs.mkdirs(new Path("/foo1")));

      final long cidBeforeUpgrade = getCommittedTxnIdValue(qjCluster);
      
      // Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
      // flag.
      cluster.shutdownNameNode(1);
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.UPGRADE);
      cluster.restartNameNode(0, false);
      assertTrue(cidBeforeUpgrade <= getCommittedTxnIdValue(qjCluster));
      
      assertTrue(fs.mkdirs(new Path("/foo2")));

      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, false);
      checkJnPreviousDirExistence(qjCluster, true);
      
      // Now bootstrap the standby with the upgraded info.
      int rc = BootstrapStandby.run(
          new String[]{"-force"},
          cluster.getConfiguration(1));
      assertEquals(0, rc);
      
      cluster.restartNameNode(1);

      final long cidDuringUpgrade = getCommittedTxnIdValue(qjCluster);
      assertTrue(cidDuringUpgrade > cidBeforeUpgrade);

      runFinalizeCommand(cluster);

      assertEquals(cidDuringUpgrade, getCommittedTxnIdValue(qjCluster));
      checkClusterPreviousDirExistence(cluster, false);
      checkJnPreviousDirExistence(qjCluster, false);
      assertCTimesEqual(cluster);
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (qjCluster != null) {
        qjCluster.shutdown();
      }
    }
  }
  
  /**
   * Make sure that even if the NN which initiated the upgrade is in the standby
   * state that we're allowed to finalize.
   */
  @Test
  public void testFinalizeFromSecondNameNodeWithJournalNodes()
      throws IOException, URISyntaxException {
    MiniQJMHACluster qjCluster = null;
    FileSystem fs = null;
    try {
      Builder builder = new MiniQJMHACluster.Builder(conf);
      builder.getDfsBuilder()
          .numDataNodes(0);
      qjCluster = builder.build();

      MiniDFSCluster cluster = qjCluster.getDfsCluster();
      
      // No upgrade is in progress at the moment.
      checkJnPreviousDirExistence(qjCluster, false);
      checkClusterPreviousDirExistence(cluster, false);
      assertCTimesEqual(cluster);
      
      // Transition NN0 to active and do some FS ops.
      cluster.transitionToActive(0);
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      assertTrue(fs.mkdirs(new Path("/foo1")));
      
      // Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
      // flag.
      cluster.shutdownNameNode(1);
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.UPGRADE);
      cluster.restartNameNode(0, false);
      
      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, false);
      checkJnPreviousDirExistence(qjCluster, true);
      
      // Now bootstrap the standby with the upgraded info.
      int rc = BootstrapStandby.run(
          new String[]{"-force"},
          cluster.getConfiguration(1));
      assertEquals(0, rc);
      
      cluster.restartNameNode(1);
      
      // Make the second NN (not the one that initiated the upgrade) active when
      // the finalize command is run.
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      
      runFinalizeCommand(cluster);
      
      checkClusterPreviousDirExistence(cluster, false);
      checkJnPreviousDirExistence(qjCluster, false);
      assertCTimesEqual(cluster);
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (qjCluster != null) {
        qjCluster.shutdown();
      }
    }
  }

  /**
   * Make sure that an HA NN will start if a previous upgrade was in progress.
   */
  @Test
  public void testStartingWithUpgradeInProgressSucceeds() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleHATopology())
          .numDataNodes(0)
          .build();

      // Simulate an upgrade having started.
      for (int i = 0; i < 2; i++) {
        for (URI uri : cluster.getNameDirs(i)) {
          File prevTmp = new File(new File(uri), Storage.STORAGE_TMP_PREVIOUS);
          LOG.info("creating previous tmp dir: " + prevTmp);
          assertTrue(prevTmp.mkdirs());
        }
      }

      cluster.restartNameNodes();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test rollback with NFS shared dir.
   */
  @Test
  public void testRollbackWithNfs() throws Exception {
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleHATopology())
          .numDataNodes(0)
          .build();

      File sharedDir = new File(cluster.getSharedEditsDir(0, 1));
      
      // No upgrade is in progress at the moment.
      checkClusterPreviousDirExistence(cluster, false);
      assertCTimesEqual(cluster);
      checkPreviousDirExistence(sharedDir, false);
      
      // Transition NN0 to active and do some FS ops.
      cluster.transitionToActive(0);
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      assertTrue(fs.mkdirs(new Path("/foo1")));
      
      // Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
      // flag.
      cluster.shutdownNameNode(1);
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.UPGRADE);
      cluster.restartNameNode(0, false);
      
      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, false);
      checkPreviousDirExistence(sharedDir, true);
      
      // NN0 should come up in the active state when given the -upgrade option,
      // so no need to transition it to active.
      assertTrue(fs.mkdirs(new Path("/foo2")));
      
      // Now bootstrap the standby with the upgraded info.
      int rc = BootstrapStandby.run(
          new String[]{"-force"},
          cluster.getConfiguration(1));
      assertEquals(0, rc);
      
      cluster.restartNameNode(1);
      
      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, true);
      checkPreviousDirExistence(sharedDir, true);
      assertCTimesEqual(cluster);
      
      // Now shut down the cluster and do the rollback.
      Collection<URI> nn1NameDirs = cluster.getNameDirs(0);
      cluster.shutdown();

      conf.setStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, Joiner.on(",").join(nn1NameDirs));
      NameNode.doRollback(conf, false);

      // The rollback operation should have rolled back the first NN's local
      // dirs, and the shared dir, but not the other NN's dirs. Those have to be
      // done by bootstrapping the standby.
      checkNnPreviousDirExistence(cluster, 0, false);
      checkPreviousDirExistence(sharedDir, false);
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testRollbackWithJournalNodes() throws IOException,
      URISyntaxException {
    MiniQJMHACluster qjCluster = null;
    FileSystem fs = null;
    try {
      Builder builder = new MiniQJMHACluster.Builder(conf);
      builder.getDfsBuilder()
          .numDataNodes(0);
      qjCluster = builder.build();

      MiniDFSCluster cluster = qjCluster.getDfsCluster();
      
      // No upgrade is in progress at the moment.
      checkClusterPreviousDirExistence(cluster, false);
      assertCTimesEqual(cluster);
      checkJnPreviousDirExistence(qjCluster, false);
      
      // Transition NN0 to active and do some FS ops.
      cluster.transitionToActive(0);
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      assertTrue(fs.mkdirs(new Path("/foo1")));

      final long cidBeforeUpgrade = getCommittedTxnIdValue(qjCluster);

      // Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
      // flag.
      cluster.shutdownNameNode(1);
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.UPGRADE);
      cluster.restartNameNode(0, false);
      
      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, false);
      checkJnPreviousDirExistence(qjCluster, true);
      
      // NN0 should come up in the active state when given the -upgrade option,
      // so no need to transition it to active.
      assertTrue(fs.mkdirs(new Path("/foo2")));

      final long cidDuringUpgrade = getCommittedTxnIdValue(qjCluster);
      assertTrue(cidDuringUpgrade > cidBeforeUpgrade);

      // Now bootstrap the standby with the upgraded info.
      int rc = BootstrapStandby.run(
          new String[]{"-force"},
          cluster.getConfiguration(1));
      assertEquals(0, rc);
      
      cluster.restartNameNode(1);
      
      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, true);
      checkJnPreviousDirExistence(qjCluster, true);
      assertCTimesEqual(cluster);
      
      // Shut down the NNs, but deliberately leave the JNs up and running.
      Collection<URI> nn1NameDirs = cluster.getNameDirs(0);
      cluster.shutdown();

      conf.setStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, Joiner.on(",").join(nn1NameDirs));
      NameNode.doRollback(conf, false);

      final long cidAfterRollback = getCommittedTxnIdValue(qjCluster);
      assertTrue(cidBeforeUpgrade < cidAfterRollback);
      // make sure the committedTxnId has been reset correctly after rollback
      assertTrue(cidDuringUpgrade > cidAfterRollback);

      // The rollback operation should have rolled back the first NN's local
      // dirs, and the shared dir, but not the other NN's dirs. Those have to be
      // done by bootstrapping the standby.
      checkNnPreviousDirExistence(cluster, 0, false);
      checkJnPreviousDirExistence(qjCluster, false);
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (qjCluster != null) {
        qjCluster.shutdown();
      }
    }
  }
  
  /**
   * Make sure that starting a second NN with the -upgrade flag fails if the
   * other NN has already done that.
   */
  @Test
  public void testCannotUpgradeSecondNameNode() throws IOException,
      URISyntaxException {
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(0)
      .build();
  
      File sharedDir = new File(cluster.getSharedEditsDir(0, 1));
      
      // No upgrade is in progress at the moment.
      checkClusterPreviousDirExistence(cluster, false);
      assertCTimesEqual(cluster);
      checkPreviousDirExistence(sharedDir, false);
      
      // Transition NN0 to active and do some FS ops.
      cluster.transitionToActive(0);
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      assertTrue(fs.mkdirs(new Path("/foo1")));
      
      // Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
      // flag.
      cluster.shutdownNameNode(1);
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.UPGRADE);
      cluster.restartNameNode(0, false);
      
      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, false);
      checkPreviousDirExistence(sharedDir, true);
      
      // NN0 should come up in the active state when given the -upgrade option,
      // so no need to transition it to active.
      assertTrue(fs.mkdirs(new Path("/foo2")));
      
      // Restart NN0 without the -upgrade flag, to make sure that works.
      cluster.getNameNodeInfos()[0].setStartOpt(StartupOption.REGULAR);
      cluster.restartNameNode(0, false);
      
      // Make sure we can still do FS ops after upgrading.
      cluster.transitionToActive(0);
      assertTrue(fs.mkdirs(new Path("/foo3")));
      
      // Make sure that starting the second NN with the -upgrade flag fails.
      cluster.getNameNodeInfos()[1].setStartOpt(StartupOption.UPGRADE);
      try {
        cluster.restartNameNode(1, false);
        fail("Should not have been able to start second NN with -upgrade");
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains(
            "It looks like the shared log is already being upgraded", ioe);
      }
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
