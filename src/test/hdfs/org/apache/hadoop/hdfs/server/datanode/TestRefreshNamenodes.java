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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.DataNode.BPOfferService;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Before;
import org.junit.Test;


public class TestRefreshNamenodes {
  
  private String localhost = "127.0.0.1";
  private int nnPort1 = 2221;
  private int nnPort2 = 2222;
  private int nnPort3 = 2223;
  private int nnPort4 = 2224;
  private final String nnURL1 = "hdfs://" + localhost + ":" + nnPort1;
  private final String nnURL2 = "hdfs://" + localhost + ":" + nnPort2;
  private final String nnURL3 = "hdfs://" + localhost + ":" + nnPort3;
  private final String nnURL4 = "hdfs://" + localhost + ":" + nnPort4;
  private NameNode nn1 = null;
  private NameNode nn2 = null;
  private NameNode nn3 = null;
  private NameNode nn4 = null;
  private TestDataNodeMultipleRegistrations tdnmr = null;
  
  @Before
  public void setUp() throws Exception {
    tdnmr = new TestDataNodeMultipleRegistrations();
    tdnmr.setUp();
  }
  
  private void startNamenodes() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "127.0.0.1:0");

    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:50071");
    FileSystem.setDefaultUri(conf, nnURL1);
    nn1 = tdnmr.startNameNode(conf, nnPort1);

    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:50072");
    FileSystem.setDefaultUri(conf, nnURL2);
    nn2 = tdnmr.startNameNode(conf, nnPort2);
   
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:50073");
    FileSystem.setDefaultUri(conf, nnURL3);
    nn3 = tdnmr.startNameNode(conf, nnPort3);
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:50074");
    FileSystem.setDefaultUri(conf, nnURL4);
    nn4 = tdnmr.startNameNode(conf, nnPort4);
  }

  @Test
  public void testRefreshNamenodes() throws IOException {
    Configuration conf = new Configuration();
    
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMENODES, nnURL1 +","+ nnURL2);   
    startNamenodes();
    
    DataNode dn = tdnmr.startDataNode(conf);
    tdnmr.waitDataNodeUp(dn);

    assertEquals(2, dn.getAllBpOs().length);
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMENODES, nnURL1 + "," + nnURL3
        + "," + nnURL4);
    dn.refreshNamenodes(conf);
    tdnmr.waitDataNodeUp(dn);
    BPOfferService[] bpoList = dn.getAllBpOs();
    assertEquals(3, bpoList.length);

    InetSocketAddress nn_addr_1 = bpoList[0].nnAddr;
    InetSocketAddress nn_addr_2 = bpoList[1].nnAddr;
    InetSocketAddress nn_addr_3 = bpoList[2].nnAddr;
    
    assertTrue(nn_addr_1.equals(nn1.getNameNodeAddress()));
    assertTrue(nn_addr_2.equals(nn3.getNameNodeAddress()));
    assertTrue(nn_addr_3.equals(nn4.getNameNodeAddress()));

    dn.shutdown();
    tdnmr.shutdownNN(nn1);
    tdnmr.shutdownNN(nn2);
    tdnmr.shutdownNN(nn3);
    tdnmr.shutdownNN(nn4);
  }
}
