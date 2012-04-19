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

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;

public class TestGetImageServlet {
  
  @Test
  public void testIsValidRequestorWithHa() throws IOException {
    Configuration conf = new HdfsConfiguration();
    
    // Set up generic HA configs.
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMESERVICES, "ns1");
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
        "ns1"), "nn1,nn2");
    
    // Set up NN1 HA configs.
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,
        "ns1", "nn1"), "host1:1234");
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
        "ns1", "nn1"), "hdfs/_HOST@TEST-REALM.COM");
    
    // Set up NN2 HA configs.
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,
        "ns1", "nn2"), "host2:1234");
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
        "ns1", "nn2"), "hdfs/_HOST@TEST-REALM.COM");
    
    // Initialize this conf object as though we're running on NN1.
    NameNode.initializeGenericKeys(conf, "ns1", "nn1");
    
    // Make sure that NN2 is considered a valid fsimage/edits requestor.
    assertTrue(GetImageServlet.isValidRequestor("hdfs/host2@TEST-REALM.COM",
        conf));
  }
}
