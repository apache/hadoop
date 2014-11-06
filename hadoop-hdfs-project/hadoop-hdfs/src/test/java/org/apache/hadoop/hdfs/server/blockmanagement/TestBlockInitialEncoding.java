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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.EC_STORAGE_POLICY_NAME;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.EC_STORAGE_POLICY_ID;
import static org.junit.Assert.assertEquals;

public class TestBlockInitialEncoding {
  private final int NUM_OF_DATANODES = 3;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private static final int BLOCK_SIZE = 1024;
  private HdfsAdmin dfsAdmin;
  private FSNamesystem namesystem;

  @Before
  public void setupCluster() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(NUM_OF_DATANODES).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    dfsAdmin = new HdfsAdmin(cluster.getURI(), conf);
    namesystem = cluster.getNamesystem();
  }

  @After
  public void shutdownCluster() throws IOException {
    cluster.shutdown();
  }

  @Test
  public void testBlockInitialEncoding()
      throws IOException, InterruptedException {
    final Path testDir = new Path("/test");
    fs.mkdir(testDir, FsPermission.getDirDefault());
    dfsAdmin.setStoragePolicy(testDir, EC_STORAGE_POLICY_NAME);
    final Path ECFilePath = new Path("/test/foo.ec");
    DFSTestUtil.createFile(fs, ECFilePath, 4 * BLOCK_SIZE, (short) 3, 0);
    INode inode = namesystem.getFSDirectory().getINode(ECFilePath.toString());
    assertEquals(EC_STORAGE_POLICY_ID, inode.getStoragePolicyID());
  }

}