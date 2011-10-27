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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDFSClientFailover {
  
  private static final Path TEST_FILE = new Path("/tmp/failover-test-file");
  private static final int FILE_LENGTH_TO_VERIFY = 100;
  
  private Configuration conf = new Configuration();
  private MiniDFSCluster cluster;
  
  @Before
  public void setUpCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numNameNodes(2).build();
    cluster.waitActive();
  }
  
  @After
  public void tearDownCluster() throws IOException {
    cluster.shutdown();
  }
  
  // TODO(HA): This test should probably be made to fail if a client fails over
  // to talk to an NN with a different block pool id. Once failover between
  // active/standy in a single block pool is implemented, this test should be
  // changed to exercise that.
  @Test
  public void testDfsClientFailover() throws IOException, URISyntaxException {
    final String logicalNameNodeId = "ha-nn-uri";
    InetSocketAddress nnAddr1 = cluster.getNameNode(0).getNameNodeAddress();
    InetSocketAddress nnAddr2 = cluster.getNameNode(1).getNameNodeAddress();
    String nameServiceId1 = DFSUtil.getNameServiceIdFromAddress(conf, nnAddr1,
        DFS_NAMENODE_RPC_ADDRESS_KEY);
    String nameServiceId2 = DFSUtil.getNameServiceIdFromAddress(conf, nnAddr2,
        DFS_NAMENODE_RPC_ADDRESS_KEY);
    
    String nameNodeId1 = "nn1";
    String nameNodeId2 = "nn2";
    
    ClientProtocol nn1 = DFSUtil.createNamenode(nnAddr1, conf);
    ClientProtocol nn2 = DFSUtil.createNamenode(nnAddr2, conf);
    
    DFSClient dfsClient1 = new DFSClient(null, nn1, conf, null);
    DFSClient dfsClient2 = new DFSClient(null, nn2, conf, null);
    
    OutputStream out1 = dfsClient1.create(TEST_FILE.toString(), false);
    OutputStream out2 = dfsClient2.create(TEST_FILE.toString(), false);
    AppendTestUtil.write(out1, 0, FILE_LENGTH_TO_VERIFY);
    AppendTestUtil.write(out2, 0, FILE_LENGTH_TO_VERIFY);
    out1.close();
    out2.close();
    
    String address1 = "hdfs://" + nnAddr1.getHostName() + ":" + nnAddr1.getPort();
    String address2 = "hdfs://" + nnAddr2.getHostName() + ":" + nnAddr2.getPort();
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
        nameServiceId1, nameNodeId1), address1);
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
        nameServiceId2, nameNodeId2), address2);
    
    conf.set(DFS_HA_NAMENODES_KEY, nameNodeId1 + "," + nameNodeId2);
    conf.set(DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + logicalNameNodeId,
        ConfiguredFailoverProxyProvider.class.getName());
    
    FileSystem fs = FileSystem.get(new URI("hdfs://" + logicalNameNodeId), conf);
    
    AppendTestUtil.check(fs, TEST_FILE, FILE_LENGTH_TO_VERIFY);
    cluster.getNameNode(0).stop();
    AppendTestUtil.check(fs, TEST_FILE, FILE_LENGTH_TO_VERIFY);
    
    fs.close();
  }

}