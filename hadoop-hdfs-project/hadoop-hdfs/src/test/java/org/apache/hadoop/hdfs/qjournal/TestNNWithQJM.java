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
package org.apache.hadoop.hdfs.qjournal;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNNWithQJM {
  Configuration conf = new HdfsConfiguration();
  private MiniJournalCluster mjc;
  private Path TEST_PATH = new Path("/test-dir");
  private Path TEST_PATH_2 = new Path("/test-dir");

  @Before
  public void resetSystemExit() {
    ExitUtil.resetFirstExitException();
  }
  
  @Before
  public void startJNs() throws Exception {
    mjc = new MiniJournalCluster.Builder(conf).build();
  }
  
  @After
  public void stopJNs() throws Exception {
    if (mjc != null) {
      mjc.shutdown();
    }
  }
  
  @Test
  public void testLogAndRestart() throws IOException {
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        MiniDFSCluster.getBaseDirectory() + "/TestNNWithQJM/image");
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        mjc.getQuorumJournalURI("myjournal").toString());
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .manageNameDfsDirs(false)
      .build();
    try {
      cluster.getFileSystem().mkdirs(TEST_PATH);
      
      // Restart the NN and make sure the edit was persisted
      // and loaded again
      cluster.restartNameNode();
      
      assertTrue(cluster.getFileSystem().exists(TEST_PATH));
      cluster.getFileSystem().mkdirs(TEST_PATH_2);
      
      // Restart the NN again and make sure both edits are persisted.
      cluster.restartNameNode();
      assertTrue(cluster.getFileSystem().exists(TEST_PATH));
      assertTrue(cluster.getFileSystem().exists(TEST_PATH_2));
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testNewNamenodeTakesOverWriter() throws Exception {
    File nn1Dir = new File(
        MiniDFSCluster.getBaseDirectory() + "/TestNNWithQJM/image-nn1");
    File nn2Dir = new File(
        MiniDFSCluster.getBaseDirectory() + "/TestNNWithQJM/image-nn2");
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        nn1Dir.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        mjc.getQuorumJournalURI("myjournal").toString());
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .manageNameDfsDirs(false)
      .checkExitOnShutdown(false)
      .build();

    try {
      cluster.getFileSystem().mkdirs(TEST_PATH);
      
      // Start a second NN pointed to the same quorum.
      // We need to copy the image dir from the first NN -- or else
      // the new NN will just be rejected because of Namespace mismatch.
      FileUtil.fullyDelete(nn2Dir);
      FileUtil.copy(nn1Dir, FileSystem.getLocal(conf).getRaw(),
          new Path(nn2Dir.getAbsolutePath()), false, conf);
      
      Configuration conf2 = new Configuration();
      conf2.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
          nn2Dir.getAbsolutePath());
      conf2.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
          mjc.getQuorumJournalURI("myjournal").toString());
      MiniDFSCluster cluster2 = new MiniDFSCluster.Builder(conf2)
        .numDataNodes(0)
        .format(false)
        .manageNameDfsDirs(false)
        .build();
      
      // Check that the new cluster sees the edits made on the old cluster
      try {
        assertTrue(cluster2.getFileSystem().exists(TEST_PATH));
      } finally {
        cluster2.shutdown();
      }
      
      // Check that, if we try to write to the old NN
      // that it aborts.
      try {
        cluster.getFileSystem().mkdirs(new Path("/x"));
        fail("Did not abort trying to write to a fenced NN");
      } catch (RemoteException re) {
        GenericTestUtils.assertExceptionContains(
            "Could not sync enough journals to persistent storage", re);
      }
    } finally {
      //cluster.shutdown();
    }
  }

  @Test
  public void testMismatchedNNIsRejected() throws Exception {
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        MiniDFSCluster.getBaseDirectory() + "/TestNNWithQJM/image");
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        mjc.getQuorumJournalURI("myjournal").toString());
    
    // Start a NN, so the storage is formatted -- both on-disk
    // and QJM.
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .manageNameDfsDirs(false)
      .build();
    cluster.shutdown();
    
    // Reformat just the on-disk portion
    Configuration onDiskOnly = new Configuration(conf);
    onDiskOnly.unset(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY);
    NameNode.format(onDiskOnly);

    // Start the NN - should fail because the JNs are still formatted
    // with the old namespace ID.
    try {
      cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0)
        .manageNameDfsDirs(false)
        .format(false)
        .build();
      fail("New NN with different namespace should have been rejected");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Unable to start log segment 1: too few journals", ioe);
    }
  }
  
  @Test
  public void testWebPageHasQjmInfo() throws Exception {
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        MiniDFSCluster.getBaseDirectory() + "/TestNNWithQJM/image");
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        mjc.getQuorumJournalURI("myjournal").toString());
    // Speed up the test
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .manageNameDfsDirs(false)
      .build();
    try {
      URL url = new URL("http://localhost:"
          + NameNode.getHttpAddress(cluster.getConfiguration(0)).getPort()
          + "/dfshealth.jsp");
      
      cluster.getFileSystem().mkdirs(TEST_PATH);
      
      String contents = DFSTestUtil.urlGet(url); 
      assertTrue(contents.contains("QJM to ["));
      assertTrue(contents.contains("Written txid 2"));

      // Stop one JN, do another txn, and make sure it shows as behind
      // stuck behind the others.
      mjc.getJournalNode(0).stopAndJoin(0);
      
      cluster.getFileSystem().delete(TEST_PATH, true);
      
      contents = DFSTestUtil.urlGet(url); 
      System.out.println(contents);
      assertTrue(Pattern.compile("1 txns/\\d+ms behind").matcher(contents)
          .find());

      // Restart NN while JN0 is still down.
      cluster.restartNameNode();

      contents = DFSTestUtil.urlGet(url); 
      System.out.println(contents);
      assertTrue(Pattern.compile("never written").matcher(contents)
          .find());
      

    } finally {
      cluster.shutdown();
    }

  }
}
