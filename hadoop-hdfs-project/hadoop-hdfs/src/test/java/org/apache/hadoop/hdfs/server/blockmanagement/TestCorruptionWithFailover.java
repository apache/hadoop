/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CORRUPT_BLOCK_DELETE_IMMEDIATELY_ENABLED;

/**
 * Tests corruption of replicas in case of failover.
 */
public class TestCorruptionWithFailover {

  @Test
  public void testCorruptReplicaAfterFailover() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_CORRUPT_BLOCK_DELETE_IMMEDIATELY_ENABLED,
        false);
    // Enable data to be written, to less replicas in case of pipeline failure.
    conf.setInt(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.
        MIN_REPLICATION, 2);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(3)
        .build()) {
      cluster.transitionToActive(0);
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem(0);
      FSDataOutputStream out = dfs.create(new Path("/dir/file"));
      // Write some data and flush.
      for (int i = 0; i < 1024 * 1024; i++) {
        out.write(i);
      }
      out.hsync();
      // Stop one datanode, so as to trigger update pipeline.
      MiniDFSCluster.DataNodeProperties dn = cluster.stopDataNode(0);
      // Write some more data and close the file.
      for (int i = 0; i < 1024 * 1024; i++) {
        out.write(i);
      }
      out.close();
      BlockManager bm0 = cluster.getNamesystem(0).getBlockManager();
      BlockManager bm1 = cluster.getNamesystem(1).getBlockManager();
      // Mark datanodes as stale, as are marked if a namenode went through a
      // failover, to prevent replica deletion.
      bm0.getDatanodeManager().markAllDatanodesStale();
      bm1.getDatanodeManager().markAllDatanodesStale();
      // Restart the datanode
      cluster.restartDataNode(dn);
      // The replica from the datanode will be having lesser genstamp, so
      // would be marked as CORRUPT.
      GenericTestUtils.waitFor(() -> bm0.getCorruptBlocks() == 1, 100, 10000);

      // Perform failover to other namenode
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      cluster.waitActive(1);
      // The corrupt count should be same as first namenode.
      GenericTestUtils.waitFor(() -> bm1.getCorruptBlocks() == 1, 100, 10000);
    }
  }
}

