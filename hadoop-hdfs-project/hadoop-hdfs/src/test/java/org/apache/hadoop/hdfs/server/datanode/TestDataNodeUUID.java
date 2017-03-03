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

package org.apache.hadoop.hdfs.server.datanode;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestDataNodeUUID {

  /**
   * This test makes sure that we have a valid
   * Node ID after the checkNodeUUID is done.
   */
  @Test
  public void testDatanodeUuid() throws Exception {

    final InetSocketAddress NN_ADDR = new InetSocketAddress(
      "localhost", 5020);
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "0.0.0.0:0");
    FileSystem.setDefaultUri(conf,
      "hdfs://" + NN_ADDR.getHostName() + ":" + NN_ADDR.getPort());
    ArrayList<StorageLocation> locations = new ArrayList<>();

    DataNode dn = new DataNode(conf, locations, null, null);

    //Assert that Node iD is null
    String nullString = null;
    assertEquals(dn.getDatanodeUuid(), nullString);

    // CheckDataNodeUUID will create an UUID if UUID is null
    dn.checkDatanodeUuid();

    // Make sure that we have a valid DataNodeUUID at that point of time.
    assertNotEquals(dn.getDatanodeUuid(), nullString);
  }

  @Test(timeout = 10000)
  public void testUUIDRegeneration() throws Exception {
    File baseDir = GenericTestUtils.getTestDir();
    File disk1 = new File(baseDir, "disk1");
    File disk2 = new File(baseDir, "disk2");

    // Ensure the configured disks do not pre-exist
    FileUtils.deleteDirectory(disk1);
    FileUtils.deleteDirectory(disk2);

    MiniDFSCluster cluster = null;
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
            disk1.toURI().toString(),
            disk2.toURI().toString());
    try {
      cluster = new MiniDFSCluster.Builder(conf)
              .numDataNodes(1)
              .manageDataDfsDirs(false)
              .build();
      cluster.waitActive();

      // Grab the new-cluster UUID as the original one to test against
      String originalUUID = cluster.getDataNodes().get(0).getDatanodeUuid();
      // Stop and simulate a DN wipe or unmount-but-root-path condition
      // on the second disk
      MiniDFSCluster.DataNodeProperties dn = cluster.stopDataNode(0);
      FileUtils.deleteDirectory(disk2);
      assertTrue("Failed to recreate the data directory: " + disk2,
              disk2.mkdirs());

      // Restart and check if the UUID changed
      assertTrue("DataNode failed to start up: " + dn,
              cluster.restartDataNode(dn));
      // We need to wait until the DN has completed registration
      while (!cluster.getDataNodes().get(0).isDatanodeFullyStarted()) {
        Thread.sleep(50);
      }
      assertEquals(
              "DN generated a new UUID despite disk1 having it intact",
              originalUUID, cluster.getDataNodes().get(0).getDatanodeUuid());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
