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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import static org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter.getFileInfo;

import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Test cases for the handling of edit logs during failover
 * and startup of the standby node.
 */
public class TestEditLogsDuringFailover {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestEditLogsDuringFailover.class);
  private static final int NUM_DIRS_IN_LOG = 5;

  static {
    // No need to fsync for the purposes of tests. This makes
    // the tests run much faster.
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
  }
  
  @Test
  public void testStartup() throws Exception {
    Configuration conf = new Configuration();
    HAUtil.setAllowStandbyReads(conf, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(0)
      .build();
    try {
      // During HA startup, both nodes should be in
      // standby and we shouldn't have any edits files
      // in any edits directory!
      List<URI> allDirs = Lists.newArrayList();
      allDirs.addAll(cluster.getNameDirs(0));
      allDirs.addAll(cluster.getNameDirs(1));
      allDirs.add(cluster.getSharedEditsDir(0, 1));
      assertNoEditFiles(allDirs);
      
      // Set the first NN to active, make sure it creates edits
      // in its own dirs and the shared dir. The standby
      // should still have no edits!
      cluster.transitionToActive(0);
      
      assertEditFiles(cluster.getNameDirs(0),
          NNStorage.getInProgressEditsFileName(1));
      assertEditFiles(
          Collections.singletonList(cluster.getSharedEditsDir(0, 1)),
          NNStorage.getInProgressEditsFileName(1));
      assertNoEditFiles(cluster.getNameDirs(1));
      
      cluster.getNameNode(0).getRpcServer().mkdirs("/test",
          FsPermission.createImmutable((short)0755), true);

      // Restarting the standby should not finalize any edits files
      // in the shared directory when it starts up!
      cluster.restartNameNode(1);
      
      assertEditFiles(cluster.getNameDirs(0),
          NNStorage.getInProgressEditsFileName(1));
      assertEditFiles(
          Collections.singletonList(cluster.getSharedEditsDir(0, 1)),
          NNStorage.getInProgressEditsFileName(1));
      assertNoEditFiles(cluster.getNameDirs(1));
      
      // Additionally it should not have applied any in-progress logs
      // at start-up -- otherwise, it would have read half-way into
      // the current log segment, and on the next roll, it would have to
      // either replay starting in the middle of the segment (not allowed)
      // or double-replay the edits (incorrect).
      assertNull(getFileInfo(cluster.getNameNode(1), "/test",
          true, false, false));
      
      cluster.getNameNode(0).getRpcServer().mkdirs("/test2",
          FsPermission.createImmutable((short)0755), true);

      // If we restart NN0, it'll come back as standby, and we can
      // transition NN1 to active and make sure it reads edits correctly at this point.
      cluster.restartNameNode(0);
      cluster.transitionToActive(1);

      // NN1 should have both the edits that came before its restart, and the edits that
      // came after its restart.
      assertNotNull(getFileInfo(cluster.getNameNode(1), "/test",
          true, false, false));
      assertNotNull(getFileInfo(cluster.getNameNode(1), "/test2",
          true, false, false));
    } finally {
      cluster.shutdown();
    }
  }
  
  private void testFailoverFinalizesAndReadsInProgress(
      boolean partialTxAtEnd) throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(0)
      .build();
    try {
      // Create a fake in-progress edit-log in the shared directory
      URI sharedUri = cluster.getSharedEditsDir(0, 1);
      File sharedDir = new File(sharedUri.getPath(), "current");
      FSNamesystem fsn = cluster.getNamesystem(0);
      FSImageTestUtil.createAbortedLogWithMkdirs(sharedDir, NUM_DIRS_IN_LOG, 1,
          fsn.getFSDirectory().getLastInodeId() + 1);
      
      assertEditFiles(Collections.singletonList(sharedUri),
          NNStorage.getInProgressEditsFileName(1));
      if (partialTxAtEnd) {
        FileOutputStream outs = null;
        try {
          File editLogFile =
              new File(sharedDir, NNStorage.getInProgressEditsFileName(1));
          outs = new FileOutputStream(editLogFile, true);
          outs.write(new byte[] { 0x18, 0x00, 0x00, 0x00 } );
          LOG.error("editLogFile = " + editLogFile);
        } finally {
          IOUtils.cleanupWithLogger(LOG, outs);
        }
     }

      // Transition one of the NNs to active
      cluster.transitionToActive(0);
      
      // In the transition to active, it should have read the log -- and
      // hence see one of the dirs we made in the fake log.
      String testPath = "/dir" + NUM_DIRS_IN_LOG;
      assertNotNull(cluster.getNameNode(0).getRpcServer()
          .getFileInfo(testPath));
      
      // It also should have finalized that log in the shared directory and started
      // writing to a new one at the next txid.
      assertEditFiles(Collections.singletonList(sharedUri),
          NNStorage.getFinalizedEditsFileName(1, NUM_DIRS_IN_LOG + 1),
          NNStorage.getInProgressEditsFileName(NUM_DIRS_IN_LOG + 2));
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testFailoverFinalizesAndReadsInProgressSimple()
      throws Exception {
    testFailoverFinalizesAndReadsInProgress(false);
  }

  @Test
  public void testFailoverFinalizesAndReadsInProgressWithPartialTxAtEnd()
      throws Exception {
    testFailoverFinalizesAndReadsInProgress(true);
  }

  /**
   * Check that no edits files are present in the given storage dirs.
   */
  private void assertNoEditFiles(Iterable<URI> dirs) throws IOException {
    assertEditFiles(dirs, new String[]{});
  }
  
  /**
   * Check that the given list of edits files are present in the given storage
   * dirs.
   */
  private void assertEditFiles(Iterable<URI> dirs, String ... files)
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
}
