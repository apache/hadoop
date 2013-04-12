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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
  @Test(timeout=60000)
  public void testBlockInvalidationWhenRBWReplicaMissedInDN()
      throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 2);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 300);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .build();
    FSDataOutputStream out = null;
    try {
      final FSNamesystem namesystem = cluster.getNamesystem();
      FileSystem fs = cluster.getFileSystem();
      Path testPath = new Path("/tmp/TestRBWBlockInvalidation", "foo1");
      out = fs.create(testPath, (short) 2);
      out.writeBytes("HDFS-3157: " + testPath);
      out.hsync();
      cluster.startDataNodes(conf, 1, true, null, null, null);
      String bpid = namesystem.getBlockPoolId();
      ExtendedBlock blk = DFSTestUtil.getFirstBlock(fs, testPath);
      Block block = blk.getLocalBlock();
      DataNode dn = cluster.getDataNodes().get(0);

      // Delete partial block and its meta information from the RBW folder
      // of first datanode.
      File blockFile = DataNodeTestUtils.getBlockFile(dn, bpid, block);
      File metaFile = DataNodeTestUtils.getMetaFile(dn, bpid, block);
      assertTrue("Could not delete the block file from the RBW folder",
          blockFile.delete());
      assertTrue("Could not delete the block meta file from the RBW folder",
          metaFile.delete());

      out.close();

      // Check datanode has reported the corrupt block.
      boolean isCorruptReported = false;
      while (!isCorruptReported) {
        if (countReplicas(namesystem, blk).corruptReplicas() > 0) {
          isCorruptReported = true;
        }
        Thread.sleep(100);
      }
      assertEquals("There should be 1 replica in the corruptReplicasMap", 1,
          countReplicas(namesystem, blk).corruptReplicas());

      // Check the block has got replicated to another datanode.
      blk = DFSTestUtil.getFirstBlock(fs, testPath);
      boolean isReplicated = false;
      while (!isReplicated) {
        if (countReplicas(namesystem, blk).liveReplicas() > 1) {
          isReplicated = true;
        }
        Thread.sleep(100);
      }
      assertEquals("There should be two live replicas", 2, countReplicas(
          namesystem, blk).liveReplicas());

      // sleep for 1 second, so that by this time datanode reports the corrupt
      // block after a live replica of block got replicated.
      Thread.sleep(1000);

      // Check that there is no corrupt block in the corruptReplicasMap.
      assertEquals("There should not be any replica in the corruptReplicasMap",
          0, countReplicas(namesystem, blk).corruptReplicas());
    } finally {
      if (out != null) {
        out.close();
      }
      cluster.shutdown();
    }
  }
}
