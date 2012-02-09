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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Joiner;

public class TestFailureOfSharedDir {
  
  private static final Log LOG = LogFactory.getLog(TestFailureOfSharedDir.class);

  /**
   * Test that the shared edits dir is automatically added to the list of edits
   * dirs that are marked required.
   */
  @Test
  public void testSharedDirIsAutomaticallyMarkedRequired()
      throws URISyntaxException {
    URI foo = new URI("file:/foo");
    URI bar = new URI("file:/bar");
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, Joiner.on(",").join(foo, bar));
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY, foo.toString());
    assertFalse(FSNamesystem.getRequiredNamespaceEditsDirs(conf).contains(
        bar));
    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY, bar.toString());
    Collection<URI> requiredEditsDirs = FSNamesystem
        .getRequiredNamespaceEditsDirs(conf); 
    assertTrue(Joiner.on(",").join(requiredEditsDirs) + " does not contain " + bar,
        requiredEditsDirs.contains(bar));
  }

  /**
   * Multiple shared edits directories is an invalid configuration.
   */
  @Test
  public void testMultipleSharedDirsFails() throws Exception {
    Configuration conf = new Configuration();
    URI sharedA = new URI("file:///shared-A");
    URI sharedB = new URI("file:///shared-B");
    URI localA = new URI("file:///local-A");

    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
        Joiner.on(",").join(sharedA,sharedB));
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        localA.toString());

    try {
      FSNamesystem.getNamespaceEditsDirs(conf);
      fail("Allowed multiple shared edits directories");
    } catch (IOException ioe) {
      assertEquals("Multiple shared edits directories are not yet supported",
          ioe.getMessage());
    }
  }
  
  /**
   * Make sure that the shared edits dirs are listed before non-shared dirs
   * when the configuration is parsed. This ensures that the shared journals
   * are synced before the local ones.
   */
  @Test
  public void testSharedDirsComeFirstInEditsList() throws Exception {
    Configuration conf = new Configuration();
    URI sharedA = new URI("file:///shared-A");
    URI localA = new URI("file:///local-A");
    URI localB = new URI("file:///local-B");
    URI localC = new URI("file:///local-C");
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
        sharedA.toString());
    // List them in reverse order, to make sure they show up in
    // the order listed, regardless of lexical sort order.
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        Joiner.on(",").join(localC, localB, localA));
    List<URI> dirs = FSNamesystem.getNamespaceEditsDirs(conf);
    assertEquals(
        "Shared dirs should come first, then local dirs, in the order " +
        "they were listed in the configuration.",
        Joiner.on(",").join(sharedA, localC, localB, localA),
        Joiner.on(",").join(dirs));
  }
  
  /**
   * Test that marking the shared edits dir as being "required" causes the NN to
   * fail if that dir can't be accessed.
   */
  @Test
  public void testFailureOfSharedDir() throws Exception {
    Configuration conf = new Configuration();
    
    // The shared edits dir will automatically be marked required.
    MiniDFSCluster cluster = null;
    File sharedEditsDir = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0)
        .build();
      
      cluster.waitActive();
      cluster.transitionToActive(0);
      
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      
      assertTrue(fs.mkdirs(new Path("/test1")));
      
      // Blow away the shared edits dir.
      Runtime mockRuntime = Mockito.mock(Runtime.class);
      URI sharedEditsUri = cluster.getSharedEditsDir(0, 1);
      sharedEditsDir = new File(sharedEditsUri);
      assertEquals(0, FileUtil.chmod(sharedEditsDir.getAbsolutePath(), "-w",
          true));

      NameNode nn0 = cluster.getNameNode(0);
      nn0.getNamesystem().getFSImage().getEditLog().getJournalSet()
          .setRuntimeForTesting(mockRuntime);
      try {
        // Make sure that subsequent operations on the NN fail.
        nn0.getRpcServer().rollEditLog();
        fail("Succeeded in rolling edit log despite shared dir being deleted");
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains(
            "Unable to start log segment 4: too few journals successfully started",
            ioe);
        // By current policy the NN should exit upon this error.
        // exit() should be called once, but since it is mocked, exit gets
        // called once during FSEditsLog.endCurrentLogSegment() and then after
        // that during FSEditsLog.startLogSegment(). So the check is atLeast(1)
        Mockito.verify(mockRuntime, Mockito.atLeastOnce()).exit(
            Mockito.anyInt());
        LOG.info("Got expected exception", ioe);
      }
      
      // Check that none of the edits dirs rolled, since the shared edits
      // dir didn't roll. Regression test for HDFS-2874.
      for (URI editsUri : cluster.getNameEditsDirs(0)) {
        if (editsUri.equals(sharedEditsUri)) {
          continue;
        }
        File editsDir = new File(editsUri.getPath());
        File curDir = new File(editsDir, "current");
        GenericTestUtils.assertGlobEquals(curDir,
            "edits_.*",
            NNStorage.getInProgressEditsFileName(1));
      }
    } finally {
      if (sharedEditsDir != null) {
        // without this test cleanup will fail
        FileUtil.chmod(sharedEditsDir.getAbsolutePath(), "+w", true);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
