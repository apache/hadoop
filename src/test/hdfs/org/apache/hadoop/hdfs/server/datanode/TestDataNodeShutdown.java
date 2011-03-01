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


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataNodeShutdown {
  
  private String localhost = "127.0.0.1";
  private int nnPort1 = 2231;
  private int nnPort2 = 2232;
  private final String nnURL1 = "hdfs://" + localhost + ":" + nnPort1;
  private final String nnURL2 = "hdfs://" + localhost + ":" + nnPort2;
  private NameNode nn1 = null;
  private NameNode nn2 = null;
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
  }

  @Test
  public void testDataNodeShutdown() throws IOException {
    Configuration conf = new Configuration();
    
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMENODES, nnURL1 +","+ nnURL2);
    startNamenodes();
    
    DataNode dn = tdnmr.startDataNode(conf);
    tdnmr.waitDataNodeUp(dn);

    //shutdown datanode
    dn.shutdown();
    
    Assert.assertEquals(0, dn.getAllBpOs().length);
    
    //Restart datanode
    dn = tdnmr.startDataNode(conf);
    tdnmr.waitDataNodeUp(dn);
    Assert.assertEquals(2, dn.getAllBpOs().length);
    
    dn.shutdown();
  }
  
  @After
  public void tearDown() throws Exception {
    tdnmr.shutdownNN(nn1);
    tdnmr.shutdownNN(nn2);
  }

}
