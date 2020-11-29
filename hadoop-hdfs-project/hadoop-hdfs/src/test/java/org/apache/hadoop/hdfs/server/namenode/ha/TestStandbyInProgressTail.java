/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.qjournal.server.JournalTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;
import static org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter.getFileInfo;
import static org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager.QJM_RPC_MAX_TXNS_KEY;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import java.util.function.Supplier;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
 * Test cases for in progress tailing edit logs by
 * the standby node.
 */
public class TestStandbyInProgressTail {
  private static final Logger LOG =
          LoggerFactory.getLogger(TestStandbyInProgressTail.class);
  private Configuration conf;
  private MiniQJMHACluster qjmhaCluster;
  private MiniDFSCluster cluster;
  private NameNode nn0;
  private NameNode nn1;

  @Before
  public void startUp() throws IOException {
    conf = new Configuration();
    // Set period of tail edits to a large value (20 mins) for test purposes
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 20 * 60);
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY,
        500);
    // Set very samll limit of transactions per a journal rpc call
    conf.setInt(QJM_RPC_MAX_TXNS_KEY, 3);
    HAUtil.setAllowStandbyReads(conf, true);
    qjmhaCluster = new MiniQJMHACluster.Builder(conf).build();
    cluster = qjmhaCluster.getDfsCluster();

    // Get NameNode from cluster to future manual control
    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);
  }

  @After
  public void tearDown() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }

  @Test
  public void testDefault() throws Exception {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
    conf = new Configuration();
    // Set period of tail edits to a large value (20 mins) for test purposes
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 20 * 60);
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, false);
    HAUtil.setAllowStandbyReads(conf, true);
    qjmhaCluster = new MiniQJMHACluster.Builder(conf).build();
    cluster = qjmhaCluster.getDfsCluster();

    try {
      // During HA startup, both nodes should be in
      // standby and we shouldn't have any edits files
      // in any edits directory!
      List<URI> allDirs = Lists.newArrayList();
      allDirs.addAll(cluster.getNameDirs(0));
      allDirs.addAll(cluster.getNameDirs(1));
      assertNoEditFiles(allDirs);

      // Set the first NN to active, make sure it creates edits
      // in its own dirs and the shared dir. The standby
      // should still have no edits!
      cluster.transitionToActive(0);

      assertEditFiles(cluster.getNameDirs(0),
              NNStorage.getInProgressEditsFileName(1));
      assertNoEditFiles(cluster.getNameDirs(1));

      cluster.getNameNode(0).getRpcServer().mkdirs("/test",
              FsPermission.createImmutable((short) 0755), true);

      cluster.getNameNode(1).getNamesystem().getEditLogTailer().doTailEdits();

      // StandbyNameNode should not finish tailing in-progress logs
      assertNull(getFileInfo(cluster.getNameNode(1),
              "/test", true, false, false));

      // Restarting the standby should not finalize any edits files
      // in the shared directory when it starts up!
      cluster.restartNameNode(1);

      assertEditFiles(cluster.getNameDirs(0),
              NNStorage.getInProgressEditsFileName(1));
      assertNoEditFiles(cluster.getNameDirs(1));

      // Additionally it should not have applied any in-progress logs
      // at start-up -- otherwise, it would have read half-way into
      // the current log segment, and on the next roll, it would have to
      // either replay starting in the middle of the segment (not allowed)
      // or double-replay the edits (incorrect).
      assertNull(getFileInfo(cluster.getNameNode(1),
              "/test", true, false, false));

      cluster.getNameNode(0).getRpcServer().mkdirs("/test2",
              FsPermission.createImmutable((short) 0755), true);

      // If we restart NN0, it'll come back as standby, and we can
      // transition NN1 to active and make sure it reads edits correctly.
      cluster.restartNameNode(0);
      cluster.transitionToActive(1);

      // NN1 should have both the edits that came before its restart,
      // and the edits that came after its restart.
      assertNotNull(getFileInfo(cluster.getNameNode(1),
              "/test", true, false, false));
      assertNotNull(getFileInfo(cluster.getNameNode(1),
              "/test2", true, false, false));
    } finally {
      if (qjmhaCluster != null) {
        qjmhaCluster.shutdown();
      }
    }
  }

  @Test
  public void testSetup() throws Exception {
    // During HA startup, both nodes should be in
    // standby and we shouldn't have any edits files
    // in any edits directory!
    List<URI> allDirs = Lists.newArrayList();
    allDirs.addAll(cluster.getNameDirs(0));
    allDirs.addAll(cluster.getNameDirs(1));
    assertNoEditFiles(allDirs);

    // Set the first NN to active, make sure it creates edits
    // in its own dirs and the shared dir. The standby
    // should still have no edits!
    cluster.transitionToActive(0);

    assertEditFiles(cluster.getNameDirs(0),
            NNStorage.getInProgressEditsFileName(1));
    assertNoEditFiles(cluster.getNameDirs(1));

    cluster.getNameNode(0).getRpcServer().mkdirs("/test",
            FsPermission.createImmutable((short) 0755), true);

    waitForFileInfo(nn1, "/test");

    // Restarting the standby should not finalize any edits files
    // in the shared directory when it starts up!
    cluster.restartNameNode(1);

    assertEditFiles(cluster.getNameDirs(0),
            NNStorage.getInProgressEditsFileName(1));
    assertNoEditFiles(cluster.getNameDirs(1));

    // Because we're using in-progress tailer, this should not be null
    assertNotNull(getFileInfo(cluster.getNameNode(1),
            "/test", true, false, false));

    cluster.getNameNode(0).getRpcServer().mkdirs("/test2",
            FsPermission.createImmutable((short) 0755), true);

    // If we restart NN0, it'll come back as standby, and we can
    // transition NN1 to active and make sure it reads edits correctly.
    cluster.restartNameNode(0);
    cluster.transitionToActive(1);

    // NN1 should have both the edits that came before its restart,
    // and the edits that came after its restart.
    assertNotNull(getFileInfo(cluster.getNameNode(1),
            "/test", true, false, false));
    assertNotNull(getFileInfo(cluster.getNameNode(1),
            "/test2", true, false, false));
  }

  @Test
  public void testHalfStartInProgressTail() throws Exception {
    // Set the first NN to active, make sure it creates edits
    // in its own dirs and the shared dir. The standby
    // should still have no edits!
    cluster.transitionToActive(0);

    assertEditFiles(cluster.getNameDirs(0),
            NNStorage.getInProgressEditsFileName(1));
    assertNoEditFiles(cluster.getNameDirs(1));

    cluster.getNameNode(0).getRpcServer().mkdirs("/test",
            FsPermission.createImmutable((short) 0755), true);

    // StandbyNameNode should tail the in-progress edit
    waitForFileInfo(nn1, "/test");

    // Create a new edit and finalized it
    cluster.getNameNode(0).getRpcServer().mkdirs("/test2",
            FsPermission.createImmutable((short) 0755), true);
    nn0.getRpcServer().rollEditLog();

    // StandbyNameNode shouldn't tail the edit since we do not call the method
    waitForFileInfo(nn1, "/test2");

    // Create a new in-progress edit and let SBNN do the tail
    cluster.getNameNode(0).getRpcServer().mkdirs("/test3",
            FsPermission.createImmutable((short) 0755), true);

    // StandbyNameNode should tail the finalized edit and the new in-progress
    waitForFileInfo(nn1, "/test", "/test2", "/test3");
  }

  @Test
  public void testInitStartInProgressTail() throws Exception {
    // Set the first NN to active, make sure it creates edits
    // in its own dirs and the shared dir. The standby
    // should still have no edits!
    cluster.transitionToActive(0);

    assertEditFiles(cluster.getNameDirs(0),
            NNStorage.getInProgressEditsFileName(1));
    assertNoEditFiles(cluster.getNameDirs(1));

    cluster.getNameNode(0).getRpcServer().mkdirs("/test",
            FsPermission.createImmutable((short) 0755), true);
    cluster.getNameNode(0).getRpcServer().mkdirs("/test2",
            FsPermission.createImmutable((short) 0755), true);
    nn0.getRpcServer().rollEditLog();

    cluster.getNameNode(0).getRpcServer().mkdirs("/test3",
            FsPermission.createImmutable((short) 0755), true);

    assertNull(getFileInfo(nn1, "/test", true, false, false));
    assertNull(getFileInfo(nn1, "/test2", true, false, false));
    assertNull(getFileInfo(nn1, "/test3", true, false, false));

    // StandbyNameNode should tail the finalized edit and the new in-progress
    waitForFileInfo(nn1, "/test", "/test2", "/test3");
  }

  @Test
  public void testNewStartInProgressTail() throws Exception {
    cluster.transitionToActive(0);

    assertEditFiles(cluster.getNameDirs(0),
            NNStorage.getInProgressEditsFileName(1));
    assertNoEditFiles(cluster.getNameDirs(1));

    cluster.getNameNode(0).getRpcServer().mkdirs("/test",
            FsPermission.createImmutable((short) 0755), true);
    cluster.getNameNode(0).getRpcServer().mkdirs("/test2",
            FsPermission.createImmutable((short) 0755), true);
    waitForFileInfo(nn1, "/test", "/test2");
    nn0.getRpcServer().rollEditLog();

    cluster.getNameNode(0).getRpcServer().mkdirs("/test3",
            FsPermission.createImmutable((short) 0755), true);

    // StandbyNameNode should tail the finalized edit and the new in-progress
    waitForFileInfo(nn1, "/test", "/test2", "/test3");
  }

  /**
   * Test that Standby Node tails multiple segments while catching up
   * during the transition to Active.
   */
  @Test
  public void testUndertailingWhileFailover() throws Exception {
    cluster.transitionToActive(0);
    cluster.waitActive(0);

    String p = "/testFailoverWhileTailingWithoutCache/";
    mkdirs(nn0, p + 0, p + 1, p + 2, p + 3, p + 4);
    nn0.getRpcServer().rollEditLog(); // create segment 1

    mkdirs(nn0, p + 5, p + 6, p + 7, p + 8, p + 9);
    nn0.getRpcServer().rollEditLog(); // create segment 2

    mkdirs(nn0, p + 10, p + 11, p + 12, p + 13, p + 14);
    nn0.getRpcServer().rollEditLog(); // create segment 3

    cluster.transitionToStandby(0);
    cluster.transitionToActive(1);
    cluster.waitActive(1);
    waitForFileInfo(nn1, p + 0, p + 1, p + 14);
  }

  @Test
  public void testNonUniformConfig() throws Exception {
    // Test case where some NNs (in this case the active NN) in the cluster
    // do not have in-progress tailing enabled.
    Configuration newConf = cluster.getNameNode(0).getConf();
    newConf.setBoolean(
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY,
        false);
    cluster.restartNameNode(0);
    cluster.transitionToActive(0);

    cluster.getNameNode(0).getRpcServer().mkdirs("/test",
        FsPermission.createImmutable((short) 0755), true);
    cluster.getNameNode(0).getRpcServer().rollEdits();

    waitForFileInfo(nn1, "/test");
  }

  @Test
  public void testEditsServedViaCache() throws Exception {
    cluster.transitionToActive(0);
    cluster.waitActive(0);

    mkdirs(nn0, "/test", "/test2");
    nn0.getRpcServer().rollEditLog();
    for (int idx = 0; idx < qjmhaCluster.getJournalCluster().getNumNodes();
        idx++) {
      File[] startingEditFile = qjmhaCluster.getJournalCluster()
          .getCurrentDir(idx, DFSUtil.getNamenodeNameServiceId(conf))
          .listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
              return name.matches("edits_0+1-[0-9]+");
            }
          });
      assertNotNull(startingEditFile);
      assertEquals(1, startingEditFile.length);
      // Delete this edit file to ensure that edits can't be served via the
      // streaming mechanism - RPC/cache-based only
      startingEditFile[0].delete();
    }
    // Ensure edits were not tailed before the edit files were deleted;
    // quick spot check of a single dir
    assertNull(getFileInfo(nn1, "/tmp0", false, false, false));

    waitForFileInfo(nn1, "/test", "/test2");
  }

  @Test
  public void testCorruptJournalCache() throws Exception {
    cluster.transitionToActive(0);
    cluster.waitActive(0);

    // Shut down one JN so there is only a quorum remaining to make it easier
    // to manage the remaining two
    qjmhaCluster.getJournalCluster().getJournalNode(0).stopAndJoin(0);

    mkdirs(nn0, "/test", "/test2");
    JournalTestUtil.corruptJournaledEditsCache(1,
        qjmhaCluster.getJournalCluster().getJournalNode(1)
            .getJournal(DFSUtil.getNamenodeNameServiceId(conf)));

    nn0.getRpcServer().rollEditLog();

    waitForFileInfo(nn1, "/test", "/test2");

    mkdirs(nn0, "/test3", "/test4");
    JournalTestUtil.corruptJournaledEditsCache(3,
        qjmhaCluster.getJournalCluster().getJournalNode(2)
            .getJournal(DFSUtil.getNamenodeNameServiceId(conf)));

    waitForFileInfo(nn1, "/test3", "/test4");
  }

  @Test
  public void testTailWithoutCache() throws Exception {
    qjmhaCluster.shutdown();
    // Effectively disable the cache by setting its size too small to be used
    conf.setInt(DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_KEY, 1);
    qjmhaCluster = new MiniQJMHACluster.Builder(conf).build();
    cluster = qjmhaCluster.getDfsCluster();
    cluster.transitionToActive(0);
    cluster.waitActive(0);
    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);

    mkdirs(nn0, "/test", "/test2");
    nn0.getRpcServer().rollEditLog();

    mkdirs(nn0, "/test3", "/test4");

    // Skip the last directory; the JournalNodes' idea of the committed
    // txn ID may not have been updated to include it yet
    waitForFileInfo(nn1, "/test", "/test2", "/test3");
  }

  /**
   * Check that no edits files are present in the given storage dirs.
   */
  private static void assertNoEditFiles(Iterable<URI> dirs) throws IOException {
    assertEditFiles(dirs);
  }

  /**
   * Check that the given list of edits files are present in the given storage
   * dirs.
   */
  private static void assertEditFiles(Iterable<URI> dirs, String... files)
          throws IOException {
    for (URI u : dirs) {
      File editDirRoot = new File(u.getPath());
      File editDir = new File(editDirRoot, "current");
      GenericTestUtils.assertExists(editDir);
      if (files.length == 0) {
        LOG.info("Checking no edit files exist in " + editDir);
      } else {
        LOG.info("Checking for following edit files in " + editDir
                + ": " + Joiner.on(",").join(files));
      }

      GenericTestUtils.assertGlobEquals(editDir, "edits_.*", files);
    }
  }

  /**
   * Create the given directories on the provided NameNode.
   */
  private static void mkdirs(NameNode nameNode, String... dirNames)
      throws Exception {
    for (String dirName : dirNames) {
      nameNode.getRpcServer().mkdirs(dirName,
          FsPermission.createImmutable((short) 0755), true);
    }
  }

  /**
   * Wait up to 1 second until the given NameNode is aware of the existing of
   * all of the provided fileNames.
   */
  private static void waitForFileInfo(NameNode standbyNN, String... fileNames)
      throws Exception {
    List<String> remainingFiles = Lists.newArrayList(fileNames);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          standbyNN.getNamesystem().getEditLogTailer().doTailEdits();
          for (Iterator<String> it = remainingFiles.iterator(); it.hasNext();) {
            if (getFileInfo(standbyNN, it.next(), true, false, false) == null) {
              return false;
            } else {
              it.remove();
            }
          }
          return true;
        } catch (IOException|InterruptedException e) {
          throw new AssertionError("Exception while waiting: " + e);
        }
      }
    }, 10, 1000);
  }

}
