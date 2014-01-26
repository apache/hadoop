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
package org.apache.hadoop.contrib.bkjournal;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;

import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;

import org.apache.hadoop.ipc.RemoteException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ExitUtil.ExitException;

import org.apache.bookkeeper.proto.BookieServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Integration test to ensure that the BookKeeper JournalManager
 * works for HDFS Namenode HA
 */
public class TestBookKeeperAsHASharedDir {
  static final Log LOG = LogFactory.getLog(TestBookKeeperAsHASharedDir.class);

  private static BKJMUtil bkutil;
  static int numBookies = 3;

  private static final String TEST_FILE_DATA = "HA BookKeeperJournalManager";

  @BeforeClass
  public static void setupBookkeeper() throws Exception {
    bkutil = new BKJMUtil(numBookies);
    bkutil.start();
  }
  
  @Before
  public void clearExitStatus() {
    ExitUtil.resetFirstExitException();
  }

  @AfterClass
  public static void teardownBookkeeper() throws Exception {
    bkutil.teardown();
  }

  /**
   * Test simple HA failover usecase with BK
   */
  @Test
  public void testFailoverWithBK() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
               BKJMUtil.createJournalURI("/hotfailover").toString());
      BKJMUtil.addJournalManagerDefinition(conf);

      cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0)
        .manageNameDfsSharedDirs(false)
        .build();
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);

      cluster.waitActive();
      cluster.transitionToActive(0);

      Path p = new Path("/testBKJMfailover");

      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);

      fs.mkdirs(p);
      cluster.shutdownNameNode(0);

      cluster.transitionToActive(1);

      assertTrue(fs.exists(p));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test HA failover, where BK, as the shared storage, fails.
   * Once it becomes available again, a standby can come up.
   * Verify that any write happening after the BK fail is not
   * available on the standby.
   */
  @Test
  public void testFailoverWithFailingBKCluster() throws Exception {
    int ensembleSize = numBookies + 1;
    BookieServer newBookie = bkutil.newBookie();
    assertEquals("New bookie didn't start",
                 ensembleSize, bkutil.checkBookiesUp(ensembleSize, 10));

    BookieServer replacementBookie = null;

    MiniDFSCluster cluster = null;

    try {
      Configuration conf = new Configuration();
      conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
               BKJMUtil.createJournalURI("/hotfailoverWithFail").toString());
      conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_ENSEMBLE_SIZE,
                  ensembleSize);
      conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_QUORUM_SIZE,
                  ensembleSize);
      BKJMUtil.addJournalManagerDefinition(conf);

      cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0)
        .manageNameDfsSharedDirs(false)
        .checkExitOnShutdown(false)
        .build();
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);

      cluster.waitActive();
      cluster.transitionToActive(0);

      Path p1 = new Path("/testBKJMFailingBKCluster1");
      Path p2 = new Path("/testBKJMFailingBKCluster2");

      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);

      fs.mkdirs(p1);
      newBookie.shutdown(); // will take down shared storage
      assertEquals("New bookie didn't stop",
                   numBookies, bkutil.checkBookiesUp(numBookies, 10));

      try {
        fs.mkdirs(p2);
        fail("mkdirs should result in the NN exiting");
      } catch (RemoteException re) {
        assertTrue(re.getClassName().contains("ExitException"));
      }
      cluster.shutdownNameNode(0);

      try {
        cluster.transitionToActive(1);
        fail("Shouldn't have been able to transition with bookies down");
      } catch (ExitException ee) {
        assertTrue("Should shutdown due to required journal failure",
            ee.getMessage().contains(
                "starting log segment 3 failed for required journal"));
      }

      replacementBookie = bkutil.newBookie();
      assertEquals("Replacement bookie didn't start",
                   ensembleSize, bkutil.checkBookiesUp(ensembleSize, 10));
      cluster.transitionToActive(1); // should work fine now

      assertTrue(fs.exists(p1));
      assertFalse(fs.exists(p2));
    } finally {
      newBookie.shutdown();
      if (replacementBookie != null) {
        replacementBookie.shutdown();
      }

      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test that two namenodes can't continue as primary
   */
  @Test
  public void testMultiplePrimariesStarted() throws Exception {
    Path p1 = new Path("/testBKJMMultiplePrimary");

    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
               BKJMUtil.createJournalURI("/hotfailoverMultiple").toString());
      BKJMUtil.addJournalManagerDefinition(conf);

      cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0)
        .manageNameDfsSharedDirs(false)
        .checkExitOnShutdown(false)
        .build();
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      cluster.waitActive();
      cluster.transitionToActive(0);

      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      fs.mkdirs(p1);
      nn1.getRpcServer().rollEditLog();
      cluster.transitionToActive(1);
      fs = cluster.getFileSystem(0); // get the older active server.

      try {
        fs.delete(p1, true);
        fail("Log update on older active should cause it to exit");
      } catch (RemoteException re) {
        assertTrue(re.getClassName().contains("ExitException"));
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Use NameNode INTIALIZESHAREDEDITS to initialize the shared edits. i.e. copy
   * the edits log segments to new bkjm shared edits.
   * 
   * @throws Exception
   */
  @Test
  public void testInitializeBKSharedEdits() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      HAUtil.setAllowStandbyReads(conf, true);
      conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);

      MiniDFSNNTopology topology = MiniDFSNNTopology.simpleHATopology();
      cluster = new MiniDFSCluster.Builder(conf).nnTopology(topology)
          .numDataNodes(0).build();
      cluster.waitActive();
      // Shutdown and clear the current filebased shared dir.
      cluster.shutdownNameNodes();
      File shareddir = new File(cluster.getSharedEditsDir(0, 1));
      assertTrue("Initial Shared edits dir not fully deleted",
          FileUtil.fullyDelete(shareddir));

      // Check namenodes should not start without shared dir.
      assertCanNotStartNamenode(cluster, 0);
      assertCanNotStartNamenode(cluster, 1);

      // Configure bkjm as new shared edits dir in both namenodes
      Configuration nn1Conf = cluster.getConfiguration(0);
      Configuration nn2Conf = cluster.getConfiguration(1);
      nn1Conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY, BKJMUtil
          .createJournalURI("/initializeSharedEdits").toString());
      nn2Conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY, BKJMUtil
          .createJournalURI("/initializeSharedEdits").toString());
      BKJMUtil.addJournalManagerDefinition(nn1Conf);
      BKJMUtil.addJournalManagerDefinition(nn2Conf);

      // Initialize the BKJM shared edits.
      assertFalse(NameNode.initializeSharedEdits(nn1Conf));

      // NameNode should be able to start and should be in sync with BKJM as
      // shared dir
      assertCanStartHANameNodes(cluster, conf, "/testBKJMInitialize");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void assertCanNotStartNamenode(MiniDFSCluster cluster, int nnIndex) {
    try {
      cluster.restartNameNode(nnIndex, false);
      fail("Should not have been able to start NN" + (nnIndex)
          + " without shared dir");
    } catch (IOException ioe) {
      LOG.info("Got expected exception", ioe);
      GenericTestUtils.assertExceptionContains(
          "storage directory does not exist or is not accessible", ioe);
    }
  }

  private void assertCanStartHANameNodes(MiniDFSCluster cluster,
      Configuration conf, String path) throws ServiceFailedException,
      IOException, URISyntaxException, InterruptedException {
    // Now should be able to start both NNs. Pass "false" here so that we don't
    // try to waitActive on all NNs, since the second NN doesn't exist yet.
    cluster.restartNameNode(0, false);
    cluster.restartNameNode(1, true);

    // Make sure HA is working.
    cluster
        .getNameNode(0)
        .getRpcServer()
        .transitionToActive(
            new StateChangeRequestInfo(RequestSource.REQUEST_BY_USER));
    FileSystem fs = null;
    try {
      Path newPath = new Path(path);
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      assertTrue(fs.mkdirs(newPath));
      HATestUtil.waitForStandbyToCatchUp(cluster.getNameNode(0),
          cluster.getNameNode(1));
      assertTrue(NameNodeAdapter.getFileInfo(cluster.getNameNode(1),
          newPath.toString(), false).isDir());
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
  }

  /**
   * NameNode should load the edits correctly if the applicable edits are
   * present in the BKJM.
   */
  @Test
  public void testNameNodeMultipleSwitchesUsingBKJM() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY, BKJMUtil
          .createJournalURI("/correctEditLogSelection").toString());
      BKJMUtil.addJournalManagerDefinition(conf);

      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(0)
          .manageNameDfsSharedDirs(false).build();
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      cluster.waitActive();
      cluster.transitionToActive(0);
      nn1.getRpcServer().rollEditLog(); // Roll Edits from current Active.
      // Transition to standby current active gracefully.
      cluster.transitionToStandby(0);
      // Make the other Active and Roll edits multiple times
      cluster.transitionToActive(1);
      nn2.getRpcServer().rollEditLog();
      nn2.getRpcServer().rollEditLog();
      // Now One more failover. So NN1 should be able to failover successfully.
      cluster.transitionToStandby(1);
      cluster.transitionToActive(0);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
