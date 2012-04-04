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

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestInitializeSharedEdits {

  private static final Log LOG = LogFactory.getLog(TestInitializeSharedEdits.class);
  
  private static final Path TEST_PATH = new Path("/test");
  private Configuration conf;
  private MiniDFSCluster cluster;
  
  @Before
  public void setupCluster() throws IOException {
    conf = new Configuration();

    MiniDFSNNTopology topology = MiniDFSNNTopology.simpleHATopology();
    
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(topology)
      .numDataNodes(0)
      .build();
    cluster.waitActive();
  
    cluster.shutdownNameNode(0);
    cluster.shutdownNameNode(1);
    File sharedEditsDir = new File(cluster.getSharedEditsDir(0, 1));
    assertTrue(FileUtil.fullyDelete(sharedEditsDir));
  }
  
  @After
  public void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testInitializeSharedEdits() throws Exception {
    // Make sure we can't currently start either NN.
    try {
      cluster.restartNameNode(0, false);
      fail("Should not have been able to start NN1 without shared dir");
    } catch (IOException ioe) {
      LOG.info("Got expected exception", ioe);
      GenericTestUtils.assertExceptionContains(
          "Cannot start an HA namenode with name dirs that need recovery", ioe);
    }
    try {
      cluster.restartNameNode(1, false);
      fail("Should not have been able to start NN2 without shared dir");
    } catch (IOException ioe) {
      LOG.info("Got expected exception", ioe);
      GenericTestUtils.assertExceptionContains(
          "Cannot start an HA namenode with name dirs that need recovery", ioe);
    }
    
    // Initialize the shared edits dir.
    assertFalse(NameNode.initializeSharedEdits(conf));
    
    // Now should be able to start both NNs. Pass "false" here so that we don't
    // try to waitActive on all NNs, since the second NN doesn't exist yet.
    cluster.restartNameNode(0, false);
    cluster.restartNameNode(1, true);
    
    // Make sure HA is working.
    cluster.transitionToActive(0);
    FileSystem fs = null;
    try {
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      assertTrue(fs.mkdirs(TEST_PATH));
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      assertTrue(fs.isDirectory(TEST_PATH));
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
  }
  
  @Test
  public void testDontOverWriteExistingDir() {
    assertFalse(NameNode.initializeSharedEdits(conf, false));
    assertTrue(NameNode.initializeSharedEdits(conf, false));
  }
}
