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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

public class TestFailureOfSharedDir {
  
  private static final Log LOG = LogFactory.getLog(TestFailureOfSharedDir.class);

  /**
   * Test that marking the shared edits dir as being "required" causes the NN to
   * fail if that dir can't be accessed.
   */
  @Test
  public void testFailureOfSharedDir() throws Exception {
    Configuration conf = new Configuration();
    URI sharedEditsUri = MiniDFSCluster.formatSharedEditsDir(
        new File(MiniDFSCluster.getBaseDirectory()), 0, 1);
    // Mark the shared edits dir required.
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY,
        sharedEditsUri.toString());
    
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0)
        .build();
      
      assertEquals(sharedEditsUri, cluster.getSharedEditsDir(0, 1));
      
      cluster.waitActive();
      cluster.transitionToActive(0);
      
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      
      assertTrue(fs.mkdirs(new Path("/test1")));
      
      // Blow away the shared edits dir.
      FileUtil.fullyDelete(new File(sharedEditsUri));
      
      NameNode nn0 = cluster.getNameNode(0);
      try {
        // Make sure that subsequent operations on the NN fail.
        nn0.getRpcServer().rollEditLog();
        fail("Succeeded in rolling edit log despite shared dir being deleted");
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains(
            "Unable to start log segment 4: too few journals successfully started",
            ioe);
        LOG.info("Got expected exception", ioe);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
