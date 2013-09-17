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

/*
 * Test the MiniDFSCluster functionality that allows "dfs.datanode.address",
 * "dfs.datanode.http.address", and "dfs.datanode.ipc.address" to be
 * configurable. The MiniDFSCluster.startDataNodes() API now has a parameter
 * that will check these properties if told to do so.
 */
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.Test;

public class TestNameNodeRpcServer {

  @Test
  public void testNamenodeRpcBindAny() throws IOException {
    Configuration conf = new HdfsConfiguration();

    // The name node in MiniDFSCluster only binds to 127.0.0.1.
    // We can set the bind address to 0.0.0.0 to make it listen
    // to all interfaces.
    conf.set(DFS_NAMENODE_RPC_BIND_HOST_KEY, "0.0.0.0");
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      assertEquals("0.0.0.0", ((NameNodeRpcServer)cluster.getNameNodeRpc())
          .getClientRpcServer().getListenerAddress().getHostName());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      // Reset the config
      conf.unset(DFS_NAMENODE_RPC_BIND_HOST_KEY);
    }
  }
}

