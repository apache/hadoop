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

import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Test;

/**
 * This test ensures the all types of data node report work correctly.
 */
public class TestDatanodeReport {
  final static private Configuration conf = new HdfsConfiguration();
  final static private int NUM_OF_DATANODES = 4;
    
  /**
   * This test attempts to different types of datanode report.
   */
  @Test
  public void testDatanodeReport() throws Exception {
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500); // 0.5s
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    MiniDFSCluster cluster = 
      new MiniDFSCluster.Builder(conf).numDataNodes(NUM_OF_DATANODES).build();
    try {
      //wait until the cluster is up
      cluster.waitActive();

      InetSocketAddress addr = new InetSocketAddress("localhost",
          cluster.getNameNodePort());
      DFSClient client = new DFSClient(addr, conf);

      assertEquals(client.datanodeReport(DatanodeReportType.ALL).length,
                   NUM_OF_DATANODES);
      assertEquals(client.datanodeReport(DatanodeReportType.LIVE).length,
                   NUM_OF_DATANODES);
      assertEquals(client.datanodeReport(DatanodeReportType.DEAD).length, 0);

      // bring down one datanode
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      datanodes.remove(datanodes.size()-1).shutdown();

      DatanodeInfo[] nodeInfo = client.datanodeReport(DatanodeReportType.DEAD);
      while (nodeInfo.length != 1) {
        try {
          Thread.sleep(500);
        } catch (Exception e) {
        }
        nodeInfo = client.datanodeReport(DatanodeReportType.DEAD);
      }

      assertEquals(client.datanodeReport(DatanodeReportType.LIVE).length,
                   NUM_OF_DATANODES-1);
      assertEquals(client.datanodeReport(DatanodeReportType.ALL).length,
                   NUM_OF_DATANODES);

      Thread.sleep(5000);
      assertGauge("ExpiredHeartbeats", 1, getMetrics("FSNamesystem"));
    }finally {
      cluster.shutdown();
    }
  }
 
  public static void main(String[] args) throws Exception {
    new TestDatanodeReport().testDatanodeReport();
  }
  
}


