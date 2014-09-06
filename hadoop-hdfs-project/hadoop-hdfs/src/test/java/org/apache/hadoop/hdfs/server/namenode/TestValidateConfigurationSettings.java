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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.util.Random;

/**
 * This class tests the validation of the configuration object when passed 
 * to the NameNode
 */
public class TestValidateConfigurationSettings {

  @After
  public void cleanUp() {
    FileUtil.fullyDeleteContents(new File(MiniDFSCluster.getBaseDirectory()));
  }

  /**
   * Tests setting the rpc port to the same as the web port to test that 
   * an exception
   * is thrown when trying to re-use the same port
   */
  @Test(expected = BindException.class, timeout = 300000)
  public void testThatMatchingRPCandHttpPortsThrowException() 
      throws IOException {

    NameNode nameNode = null;
    try {
      Configuration conf = new HdfsConfiguration();
      File nameDir = new File(MiniDFSCluster.getBaseDirectory(), "name");
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
          nameDir.getAbsolutePath());

      Random rand = new Random();
      final int port = 30000 + rand.nextInt(30000);

      // set both of these to the same port. It should fail.
      FileSystem.setDefaultUri(conf, "hdfs://localhost:" + port);
      conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "127.0.0.1:" + port);
      DFSTestUtil.formatNameNode(conf);
      nameNode = new NameNode(conf);
    } finally {
      if (nameNode != null) {
        nameNode.stop();
      }
    }
  }

  /**
   * Tests setting the rpc port to a different as the web port that an 
   * exception is NOT thrown 
   */
  @Test(timeout = 300000)
  public void testThatDifferentRPCandHttpPortsAreOK() 
      throws IOException {

    Configuration conf = new HdfsConfiguration();
    File nameDir = new File(MiniDFSCluster.getBaseDirectory(), "name");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        nameDir.getAbsolutePath());

    Random rand = new Random();

    // A few retries in case the ports we choose are in use.
    for (int i = 0; i < 5; ++i) {
      final int port1 = 30000 + rand.nextInt(10000);
      final int port2 = port1 + 1 + rand.nextInt(10000);

      FileSystem.setDefaultUri(conf, "hdfs://localhost:" + port1);
      conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "127.0.0.1:" + port2);
      DFSTestUtil.formatNameNode(conf);
      NameNode nameNode = null;

      try {
        nameNode = new NameNode(conf); // should be OK!
        break;
      } catch(BindException be) {
        continue;     // Port in use? Try another.
      } finally {
        if (nameNode != null) {
          nameNode.stop();
        }
      }
    }
  }

  /**
   * HDFS-3013: NameNode format command doesn't pick up
   * dfs.namenode.name.dir.NameServiceId configuration.
   */
  @Test(timeout = 300000)
  public void testGenericKeysForNameNodeFormat()
      throws IOException {
    Configuration conf = new HdfsConfiguration();

    // Set ephemeral ports 
    conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,
        "127.0.0.1:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY,
        "127.0.0.1:0");
    
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1");
    
    // Set a nameservice-specific configuration for name dir
    File dir = new File(MiniDFSCluster.getBaseDirectory(),
        "testGenericKeysForNameNodeFormat");
    if (dir.exists()) {
      FileUtil.fullyDelete(dir);
    }
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY + ".ns1",
        dir.getAbsolutePath());
    
    // Format and verify the right dir is formatted.
    DFSTestUtil.formatNameNode(conf);
    GenericTestUtils.assertExists(dir);

    // Ensure that the same dir is picked up by the running NN
    NameNode nameNode = new NameNode(conf);
    nameNode.stop();
  }
}
