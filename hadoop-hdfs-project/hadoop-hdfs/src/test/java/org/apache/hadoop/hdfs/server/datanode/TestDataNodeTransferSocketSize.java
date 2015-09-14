/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

public class TestDataNodeTransferSocketSize {

  @Test
  public void testSpecifiedDataSocketSize() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(
      DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, 4 * 1024);
    SimulatedFSDataset.setFactory(conf);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      List<DataNode> datanodes = cluster.getDataNodes();
      DataNode datanode = datanodes.get(0);
      assertEquals("Receive buffer size should be 4K",
        4 * 1024, datanode.getXferServer().getPeerServer().getReceiveBufferSize());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testAutoTuningDataSocketSize() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(
      DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, 0);
    SimulatedFSDataset.setFactory(conf);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      List<DataNode> datanodes = cluster.getDataNodes();
      DataNode datanode = datanodes.get(0);
      assertTrue(
        "Receive buffer size should be a default value (determined by kernel)",
        datanode.getXferServer().getPeerServer().getReceiveBufferSize() > 0);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
