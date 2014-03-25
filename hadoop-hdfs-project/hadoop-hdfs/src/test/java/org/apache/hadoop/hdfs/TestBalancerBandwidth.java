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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Test;

/**
 * This test ensures that the balancer bandwidth is dynamically adjusted
 * correctly.
 */
public class TestBalancerBandwidth {
  final static private Configuration conf = new Configuration();
  final static private int NUM_OF_DATANODES = 2;
  final static private int DEFAULT_BANDWIDTH = 1024*1024;
  public static final Log LOG = LogFactory.getLog(TestBalancerBandwidth.class);

  @Test
  public void testBalancerBandwidth() throws Exception {
    /* Set bandwidthPerSec to a low value of 1M bps. */
    conf.setLong(
        DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY,
        DEFAULT_BANDWIDTH);

    /* Create and start cluster */
    MiniDFSCluster cluster = 
      new MiniDFSCluster.Builder(conf).numDataNodes(NUM_OF_DATANODES).build();
    try {
      cluster.waitActive();

      DistributedFileSystem fs = cluster.getFileSystem();

      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      // Ensure value from the configuration is reflected in the datanodes.
      assertEquals(DEFAULT_BANDWIDTH, (long) datanodes.get(0).getBalancerBandwidth());
      assertEquals(DEFAULT_BANDWIDTH, (long) datanodes.get(1).getBalancerBandwidth());

      // Dynamically change balancer bandwidth and ensure the updated value
      // is reflected on the datanodes.
      long newBandwidth = 12 * DEFAULT_BANDWIDTH; // 12M bps
      fs.setBalancerBandwidth(newBandwidth);

      // Give it a few seconds to propogate new the value to the datanodes.
      try {
        Thread.sleep(5000);
      } catch (Exception e) {}

      assertEquals(newBandwidth, (long) datanodes.get(0).getBalancerBandwidth());
      assertEquals(newBandwidth, (long) datanodes.get(1).getBalancerBandwidth());

      // Dynamically change balancer bandwidth to 0. Balancer bandwidth on the
      // datanodes should remain as it was.
      fs.setBalancerBandwidth(0);

      // Give it a few seconds to propogate new the value to the datanodes.
      try {
        Thread.sleep(5000);
      } catch (Exception e) {}

      assertEquals(newBandwidth, (long) datanodes.get(0).getBalancerBandwidth());
      assertEquals(newBandwidth, (long) datanodes.get(1).getBalancerBandwidth());
    }finally {
      cluster.shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
    new TestBalancerBandwidth().testBalancerBandwidth();
  }
}
