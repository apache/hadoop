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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * Test Datanode Replica Trash with enable and disable.
 */
public class TestDatanodeReplicaTrash {
  private final static Logger LOG = LoggerFactory.getLogger(
      TestDatanodeReplicaTrash.class);
  private final Configuration conf = new Configuration();
  private static final Random RANDOM = new Random();
  private static final String FILE_NAME = "/tmp.txt";
  private static final int DEFAULT_BLOCK_SIZE = 512;

  @Test
  public void testDeleteWithReplicaTrashEnable() throws Exception {

    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_ENABLE_REPLICA_TRASH_KEY,
        true);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 2);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).storagesPerDatanode(1).build();
    try {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      final ClientProtocol client = cluster.getNameNode().getRpcServer();
      final Path f = new Path(FILE_NAME);
      int len = 1024;
      DFSTestUtil.createFile(dfs, f, len, (short) 1, RANDOM.nextLong());

      LocatedBlocks blockLocations = client.getBlockLocations(f.toString(),
          0, 1024);
      String bpId =  blockLocations.getLocatedBlocks().get(0).getBlock()
          .getBlockPoolId();

      Collection<String> locations = conf.getTrimmedStringCollection(
          DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);

      String loc;
      File replicaTrashDir = null;

      for (String location : locations) {
        loc = location.replace("[DISK]file:", "");
        replicaTrashDir = new File(loc + File.separator + Storage
            .STORAGE_DIR_CURRENT + File.separator + bpId + File
            .separator + DataStorage.STORAGE_DIR_REPLICA_TRASH);
      }

      //Before Delete replica-trash dir should be empty
      Assert.assertTrue(replicaTrashDir.list().length == 0);

      dfs.delete(f, true);
      LOG.info("File is being deleted");


      List<DataNode> datanodes = cluster.getDataNodes();
      for (DataNode datanode : datanodes) {
        DataNodeTestUtils.triggerHeartbeat(datanode);
      }

      final File replicaTrash = replicaTrashDir;
      //After delete, replica-trash dir should not be empty
      LambdaTestUtils.await(30000, 1000,
          () -> {
            return replicaTrash.list().length > 0;
          });
    } finally {
      cluster.shutdown();
    }

  }

  @Test
  public void testDeleteWithReplicaTrashDisable() throws Exception {

    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_ENABLE_REPLICA_TRASH_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 2);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).storagesPerDatanode(1).build();
    try {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      ClientProtocol client = cluster.getNameNode().getRpcServer();
      DataNode dn = cluster.getDataNodes().get(0);
      Path f = new Path(FILE_NAME);
      int len = 100;
      DFSTestUtil.createFile(dfs, f, len, (short) 1, RANDOM.nextLong());

      LocatedBlocks blockLocations = client.getBlockLocations(f.toString(),
          0, 100);
      String bpId =  blockLocations.getLocatedBlocks().get(0).getBlock()
          .getBlockPoolId();

      Collection<String> locations = conf.getTrimmedStringCollection(
          DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);

      String loc;
      File replicaTrashDir = null;


      for (String location : locations) {
        loc = location.replace("[DISK]file:", "");
        replicaTrashDir = new File(loc + File.separator + Storage
            .STORAGE_DIR_CURRENT + File.separator + bpId + File
            .separator + DataStorage.STORAGE_DIR_REPLICA_TRASH);
      }

      dfs.delete(f, true);
      LOG.info("File is being deleted");

      List<DataNode> datanodes = cluster.getDataNodes();
      for (DataNode datanode : datanodes) {
        DataNodeTestUtils.triggerHeartbeat(datanode);
      }

      //replica-trash folder should not be created, as replica trash is not
      // enabled
      Assert.assertTrue(!replicaTrashDir.exists());
    } finally {
      cluster.shutdown();
    }

  }

}
