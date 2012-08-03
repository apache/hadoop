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
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.DFSTestUtil;

import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogTestUtil;

import org.apache.hadoop.ipc.RemoteException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.util.ExitUtil.ExitException;

import org.apache.bookkeeper.proto.BookieServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

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
}
