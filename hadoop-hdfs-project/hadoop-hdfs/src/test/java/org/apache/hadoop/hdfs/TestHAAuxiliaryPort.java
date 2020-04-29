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
package org.apache.hadoop.hdfs;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.junit.Test;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMESERVICES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Test NN auxiliary port with HA.
 */
public class TestHAAuxiliaryPort {
  @Test
  public void testHAAuxiliaryPort() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY, "0,0");
    conf.set(DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY + ".ha-nn-uri-0.nn1",
        "9000,9001");
    conf.set(DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY + ".ha-nn-uri-0.nn2",
        "9000,9001");
    conf.set(DFS_NAMESERVICES, "ha-nn-uri-0");
    conf.set(DFS_HA_NAMENODES_KEY_PREFIX + ".ha-nn-uri-0", "nn1,nn2");
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    MiniDFSNNTopology topology = new MiniDFSNNTopology()
        .addNameservice(new MiniDFSNNTopology.NSConf("ha-nn-uri-0")
            .addNN(new MiniDFSNNTopology.NNConf("nn1"))
            .addNN(new MiniDFSNNTopology.NNConf("nn2")));

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(topology)
        .numDataNodes(0)
        .build();
    cluster.transitionToActive(0);
    cluster.waitActive();

    NameNode nn0 = cluster.getNameNode(0);
    NameNode nn1 = cluster.getNameNode(1);

    // all the addresses below are valid nn0 addresses
    NameNodeRpcServer rpcServer0 = (NameNodeRpcServer)nn0.getRpcServer();
    InetSocketAddress server0RpcAddress = rpcServer0.getRpcAddress();
    Set<InetSocketAddress> auxAddrServer0 =
        rpcServer0.getAuxiliaryRpcAddresses();
    assertEquals(2, auxAddrServer0.size());

    // all the addresses below are valid nn1 addresses
    NameNodeRpcServer rpcServer1 = (NameNodeRpcServer)nn1.getRpcServer();
    InetSocketAddress server1RpcAddress = rpcServer1.getRpcAddress();
    Set<InetSocketAddress> auxAddrServer1 =
        rpcServer1.getAuxiliaryRpcAddresses();
    assertEquals(2, auxAddrServer1.size());

    // mkdir on nn0 uri 0
    URI nn0URI = new URI("hdfs://localhost:" +
        server0RpcAddress.getPort());
    try (DFSClient client0 = new DFSClient(nn0URI, conf)){
      client0.mkdirs("/test", null, true);
      // should be available on other ports also
      for (InetSocketAddress auxAddr : auxAddrServer0) {
        nn0URI = new URI("hdfs://localhost:" + auxAddr.getPort());
        try (DFSClient clientTmp = new DFSClient(nn0URI, conf)) {
          assertTrue(clientTmp.exists("/test"));
        }
      }
    }

    // now perform a failover
    cluster.shutdownNameNode(0);
    cluster.transitionToActive(1);

    // then try to read the file from the nn1
    URI nn1URI = new URI("hdfs://localhost:" +
        server1RpcAddress.getPort());
    try (DFSClient client1 = new DFSClient(nn1URI, conf)) {
      assertTrue(client1.exists("/test"));
      // should be available on other ports also
      for (InetSocketAddress auxAddr : auxAddrServer1) {
        nn1URI = new URI("hdfs://localhost:" + auxAddr.getPort());
        try (DFSClient clientTmp = new DFSClient(nn1URI, conf)) {
          assertTrue(client1.exists("/test"));
        }
      }
    }
  }
}
