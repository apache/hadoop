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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.junit.Test;

/**
 * Tests deleteBlockPool functionality.
 */
public class TestDeleteBlockPool {
  
  @Test
  public void testDeleteBlockPool() throws Exception {
    // Start cluster with a 2 NN and 2 DN
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      conf.set(DFSConfigKeys.DFS_NAMESERVICES,
          "namesServerId1,namesServerId2");
      cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology
            (conf.get(DFSConfigKeys.DFS_NAMESERVICES)))
        .numDataNodes(2).build();

      cluster.waitActive();

      FileSystem fs1 = cluster.getFileSystem(0);
      FileSystem fs2 = cluster.getFileSystem(1);

      DFSTestUtil.createFile(fs1, new Path("/alpha"), 1024, (short) 2, 54);
      DFSTestUtil.createFile(fs2, new Path("/beta"), 1024, (short) 2, 54);

      DataNode dn1 = cluster.getDataNodes().get(0);
      DataNode dn2 = cluster.getDataNodes().get(1);

      String bpid1 = cluster.getNamesystem(0).getBlockPoolId();
      String bpid2 = cluster.getNamesystem(1).getBlockPoolId();

      // Although namenode is shutdown, the bp offerservice is still running
      try {
        dn1.deleteBlockPool(bpid1, true);
        fail("Must not delete a running block pool");
      } catch (IOException expected) {
      }

      Configuration nn1Conf = cluster.getConfiguration(1);
      nn1Conf.set(DFSConfigKeys.DFS_NAMESERVICES, "namesServerId2");
      dn1.refreshNamenodes(nn1Conf);
      assertEquals(1, dn1.getAllBpOs().size());

      try {
        dn1.deleteBlockPool(bpid1, false);
        fail("Must not delete if any block files exist unless "
            + "force is true");
      } catch (IOException expected) {
      }

      cluster.getFsDatasetTestUtils(0).verifyBlockPoolExists(bpid1);

      dn1.deleteBlockPool(bpid1, true);

      cluster.getFsDatasetTestUtils(0).verifyBlockPoolMissing(bpid1);
     
      fs1.delete(new Path("/alpha"), true);
      
      // Wait till all blocks are deleted from the dn2 for bpid1.
      while (cluster.getFsDatasetTestUtils(1).getStoredReplicas(bpid1)
          .hasNext()) {
        try {
          Thread.sleep(3000);
        } catch (Exception ignored) {
        }
      }
      cluster.shutdownNameNode(0);
      
      // Although namenode is shutdown, the bp offerservice is still running 
      // on dn2
      try {
        dn2.deleteBlockPool(bpid1, true);
        fail("Must not delete a running block pool");
      } catch (IOException expected) {
      }
      
      dn2.refreshNamenodes(nn1Conf);
      assertEquals(1, dn2.getAllBpOs().size());

      cluster.getFsDatasetTestUtils(1).verifyBlockPoolExists(bpid1);
      
      // Now deleteBlockPool must succeed with force as false, because no 
      // blocks exist for bpid1 and bpOfferService is also stopped for bpid1.
      dn2.deleteBlockPool(bpid1, false);

      cluster.getFsDatasetTestUtils(1).verifyBlockPoolMissing(bpid1);
      
      //bpid2 must not be impacted
      cluster.getFsDatasetTestUtils(0).verifyBlockPoolExists(bpid2);
      cluster.getFsDatasetTestUtils(1).verifyBlockPoolExists(bpid2);
      //make sure second block pool is running all fine
      Path gammaFile = new Path("/gamma");
      DFSTestUtil.createFile(fs2, gammaFile, 1024, (short) 1, 55);
      fs2.setReplication(gammaFile, (short)2);
      DFSTestUtil.waitReplication(fs2, gammaFile, (short) 2);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testDfsAdminDeleteBlockPool() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      conf.set(DFSConfigKeys.DFS_NAMESERVICES,
          "namesServerId1,namesServerId2");
      cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(
            conf.get(DFSConfigKeys.DFS_NAMESERVICES)))
        .numDataNodes(1).build();

      cluster.waitActive();

      FileSystem fs1 = cluster.getFileSystem(0);
      FileSystem fs2 = cluster.getFileSystem(1);

      DFSTestUtil.createFile(fs1, new Path("/alpha"), 1024, (short) 1, 54);
      DFSTestUtil.createFile(fs2, new Path("/beta"), 1024, (short) 1, 54);

      DataNode dn1 = cluster.getDataNodes().get(0);

      String bpid1 = cluster.getNamesystem(0).getBlockPoolId();
      String bpid2 = cluster.getNamesystem(1).getBlockPoolId();
      
      Configuration nn1Conf = cluster.getConfiguration(0);
      nn1Conf.set(DFSConfigKeys.DFS_NAMESERVICES, "namesServerId1");
      dn1.refreshNamenodes(nn1Conf);
      assertEquals(1, dn1.getAllBpOs().size());
      
      DFSAdmin admin = new DFSAdmin(nn1Conf);
      String dn1Address = dn1.getDatanodeId().getIpAddr() + ":" + dn1.getIpcPort();
      String[] args = { "-deleteBlockPool", dn1Address, bpid2 };
      
      int ret = admin.run(args);
      assertFalse(0 == ret);

      cluster.getFsDatasetTestUtils(0).verifyBlockPoolExists(bpid2);
      
      String[] forceArgs = { "-deleteBlockPool", dn1Address, bpid2, "force" };
      ret = admin.run(forceArgs);
      assertEquals(0, ret);

      cluster.getFsDatasetTestUtils(0).verifyBlockPoolMissing(bpid2);
      
      //bpid1 remains good
      cluster.getFsDatasetTestUtils(0).verifyBlockPoolExists(bpid1);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
