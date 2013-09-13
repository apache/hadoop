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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.Test;

/**
 * This test makes sure that when
 * {@link DFSConfigKeys#DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY} is set,
 * DFSClient instances can still be created within NN/DN (e.g., the fs instance
 * used by the trash emptier thread in NN)
 */
public class TestLossyRetryInvocationHandler {
  
  @Test
  public void testStartNNWithTrashEmptier() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new HdfsConfiguration();
    
    // enable both trash emptier and dropping response
    conf.setLong("fs.trash.interval", 360);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY, 2);
    
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(0)
          .build();
      cluster.waitActive();
      cluster.transitionToActive(0);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
}
