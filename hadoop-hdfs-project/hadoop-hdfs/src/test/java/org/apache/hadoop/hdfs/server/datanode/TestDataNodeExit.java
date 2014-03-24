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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** 
 * Tests if DataNode process exits if all Block Pool services exit. 
 */
public class TestDataNodeExit {
  private static final long WAIT_TIME_IN_MILLIS = 10;
  Configuration conf;
  MiniDFSCluster cluster = null;
  
  @Before
  public void setUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 100);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 100);
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(3))
      .build();
    for (int i = 0; i < 3; i++) {
      cluster.waitActive(i);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null)
      cluster.shutdown();
  }
  
  private void stopBPServiceThreads(int numStopThreads, DataNode dn)
      throws Exception {
    BPOfferService[] bpoList = dn.getAllBpOs();
    int expected = dn.getBpOsCount() - numStopThreads;
    int index = numStopThreads - 1;
    while (index >= 0) {
      bpoList[index--].stop();
    }
    int iterations = 3000; // Total 30 seconds MAX wait time
    while(dn.getBpOsCount() != expected && iterations > 0) {
      Thread.sleep(WAIT_TIME_IN_MILLIS);
      iterations--;
    }
    assertEquals("Mismatch in number of BPServices running", expected,
        dn.getBpOsCount());
  }

  /**
   * Test BPService Thread Exit
   */
  @Test
  public void testBPServiceExit() throws Exception {
    DataNode dn = cluster.getDataNodes().get(0);
    stopBPServiceThreads(1, dn);
    assertTrue("DataNode should not exit", dn.isDatanodeUp());
    stopBPServiceThreads(2, dn);
    assertFalse("DataNode should exit", dn.isDatanodeUp());
  }
}
