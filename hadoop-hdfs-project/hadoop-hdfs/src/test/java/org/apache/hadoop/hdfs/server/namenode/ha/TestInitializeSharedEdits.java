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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestInitializeSharedEdits {

  private static final Log LOG = LogFactory.getLog(TestInitializeSharedEdits.class);
  
  private static final Path TEST_PATH = new Path("/test");
  private Configuration conf;
  private MiniDFSCluster cluster;
  
  @Before
  public void setupCluster() throws IOException {
    conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    HAUtil.setAllowStandbyReads(conf, true);
    
    MiniDFSNNTopology topology = MiniDFSNNTopology.simpleHATopology();
    
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(topology)
      .numDataNodes(0)
      .build();
    cluster.waitActive();

    shutdownClusterAndRemoveSharedEditsDir();
  }
  
  @After
  public void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  private void shutdownClusterAndRemoveSharedEditsDir() throws IOException {
    cluster.shutdownNameNode(0);
    cluster.shutdownNameNode(1);
    File sharedEditsDir = new File(cluster.getSharedEditsDir(0, 1));
    assertTrue(FileUtil.fullyDelete(sharedEditsDir));
  }
  
  private void assertCannotStartNameNodes() {
    // Make sure we can't currently start either NN.
    try {
      cluster.restartNameNode(0, false);
      fail("Should not have been able to start NN1 without shared dir");
    } catch (IOException ioe) {
      LOG.info("Got expected exception", ioe);
      GenericTestUtils.assertExceptionContains(
          "storage directory does not exist or is not accessible", ioe);
    }
    try {
      cluster.restartNameNode(1, false);
      fail("Should not have been able to start NN2 without shared dir");
    } catch (IOException ioe) {
      LOG.info("Got expected exception", ioe);
      GenericTestUtils.assertExceptionContains(
          "storage directory does not exist or is not accessible", ioe);
    }
  }
  
  private void assertCanStartHaNameNodes(String pathSuffix)
      throws ServiceFailedException, IOException, URISyntaxException,
      InterruptedException {
    // Now should be able to start both NNs. Pass "false" here so that we don't
    // try to waitActive on all NNs, since the second NN doesn't exist yet.
    cluster.restartNameNode(0, false);
    cluster.restartNameNode(1, true);
    
    // Make sure HA is working.
    cluster.getNameNode(0).getRpcServer().transitionToActive(
        new StateChangeRequestInfo(RequestSource.REQUEST_BY_USER));
    FileSystem fs = null;
    try {
      Path newPath = new Path(TEST_PATH, pathSuffix);
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
  
  @Test
  public void testInitializeSharedEdits() throws Exception {
    assertCannotStartNameNodes();
    
    // Initialize the shared edits dir.
    assertFalse(NameNode.initializeSharedEdits(cluster.getConfiguration(0)));
    
    assertCanStartHaNameNodes("1");
    
    // Now that we've done a metadata operation, make sure that deleting and
    // re-initializing the shared edits dir will let the standby still start.
    
    shutdownClusterAndRemoveSharedEditsDir();
    
    assertCannotStartNameNodes();
    
    // Re-initialize the shared edits dir.
    assertFalse(NameNode.initializeSharedEdits(cluster.getConfiguration(0)));
    
    // Should *still* be able to start both NNs
    assertCanStartHaNameNodes("2");
  }
  
  @Test
  public void testFailWhenNoSharedEditsSpecified() throws Exception {
    Configuration confNoShared = new Configuration(conf);
    confNoShared.unset(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
    assertFalse(NameNode.initializeSharedEdits(confNoShared, true));
  }
  
  @Test
  public void testDontOverWriteExistingDir() throws IOException {
    assertFalse(NameNode.initializeSharedEdits(conf, false));
    assertTrue(NameNode.initializeSharedEdits(conf, false));
  }
  
  @Test
  public void testInitializeSharedEditsConfiguresGenericConfKeys() throws IOException {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1");
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
        "ns1"), "nn1,nn2");
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,
        "ns1", "nn1"), "localhost:1234");
    assertNull(conf.get(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY));
    NameNode.initializeSharedEdits(conf);
    assertNotNull(conf.get(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY));
  }
}
