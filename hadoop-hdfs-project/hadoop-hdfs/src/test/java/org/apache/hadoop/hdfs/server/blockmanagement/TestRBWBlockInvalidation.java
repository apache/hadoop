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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test when RBW block is removed. Invalidation of the corrupted block happens
 * and then the under replicated block gets replicated to the datanode.
 */
public class TestRBWBlockInvalidation {
  private static NumberReplicas countReplicas(final FSNamesystem namesystem,
      ExtendedBlock block) {
    return namesystem.getBlockManager().countNodes(block.getLocalBlock());
  }

  /**
   * Test when a block's replica is removed from RBW folder in one of the
   * datanode, namenode should ask to invalidate that corrupted block and
   * schedule replication for one more replica for that under replicated block.
   */
  @Test
  public void testBlockInvalidationWhenRBWReplicaMissedInDN()
      throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 300);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
        .build();
    FSDataOutputStream out = null;
    try {
      final FSNamesystem namesystem = cluster.getNamesystem();
      FileSystem fs = cluster.getFileSystem();
      Path testPath = new Path(MiniDFSCluster.getBaseDirectory(), "foo1");
      out = fs.create(testPath, (short) 3);
      out.writeBytes("HDFS-3157: " + testPath);
      out.hsync();
      String bpid = namesystem.getBlockPoolId();
      ExtendedBlock blk = DFSTestUtil.getFirstBlock(fs, testPath);
      Block block = blk.getLocalBlock();
      // Deleting partial block and its meta information from the RBW folder
      // of first datanode.
      DataNode dn = cluster.getDataNodes().get(0);
      File blockFile = DataNodeTestUtils.getBlockFile(dn, bpid, block);
      File metaFile = DataNodeTestUtils.getMetaFile(dn, bpid, block);
      assertTrue("Could not delete the block file from the RBW folder",
          blockFile.delete());
      assertTrue("Could not delete the block meta file from the RBW folder",
          metaFile.delete());
      out.close();
      assertEquals("The corrupt replica could not be invalidated", 0,
          countReplicas(namesystem, blk).corruptReplicas());
      /*
       * Sleep for 3 seconds, for under replicated block to get replicated. As
       * one second will be taken by ReplicationMonitor and one more second for
       * invalidated block to get deleted from the datanode.
       */
      Thread.sleep(3000);
      blk = DFSTestUtil.getFirstBlock(fs, testPath);
      assertEquals("There should be three live replicas", 3,
          countReplicas(namesystem, blk).liveReplicas());
    } finally {
      if (out != null) {
        out.close();
      }
      cluster.shutdown();
    }
  }
}
