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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestNameNodeJspHelper {

  private static final int DATA_NODES_AMOUNT = 2;
  
  private static MiniDFSCluster cluster;
  private static Configuration conf;
  
  @BeforeClass
  public static void setUp() throws Exception {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(DATA_NODES_AMOUNT).build();
    cluster.waitClusterUp();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster != null)
      cluster.shutdown();
  }

  @Test
  public void testDelegationToken() throws IOException, InterruptedException {
    NamenodeProtocols nn = cluster.getNameNodeRpc();
    HttpServletRequest request = mock(HttpServletRequest.class);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("auser");
    String tokenString = NamenodeJspHelper.getDelegationToken(nn, request,
        conf, ugi);
    // tokenString returned must be null because security is disabled
    Assert.assertEquals(null, tokenString);
  }

  @Test(timeout = 15000)
  public void testGetRandomDatanode() {
    NameNode nameNode = cluster.getNameNode();
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (DataNode dataNode : cluster.getDataNodes()) {
      builder.add(dataNode.getDisplayName());
    }
    ImmutableSet<String> set = builder.build();

    for (int i = 0; i < 10; i++) {
      DatanodeDescriptor dnDescriptor = NamenodeJspHelper
          .getRandomDatanode(nameNode);
      assertTrue("testGetRandomDatanode error",
          set.contains(dnDescriptor.toString()));
    }
  }
}
