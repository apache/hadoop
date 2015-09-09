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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
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
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final PrintStream outStream = new PrintStream(outContent);

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
      ClientDatanodeProtocol dn1Proxy = DFSUtilClient
          .createClientDatanodeProtocolProxy(datanodes.get(0).getDatanodeId(),
              conf, 60000, false);
      ClientDatanodeProtocol dn2Proxy = DFSUtilClient
          .createClientDatanodeProtocolProxy(datanodes.get(1).getDatanodeId(),
              conf, 60000, false);
      DFSAdmin admin = new DFSAdmin(conf);
      String dn1Address = datanodes.get(0).ipcServer.getListenerAddress()
          .getHostName() + ":" + datanodes.get(0).getIpcPort();
      String dn2Address = datanodes.get(1).ipcServer.getListenerAddress()
          .getHostName() + ":" + datanodes.get(1).getIpcPort();

      // verifies the dfsadmin command execution
      String[] args = new String[] { "-getBalancerBandwidth", dn1Address };
      runGetBalancerBandwidthCmd(admin, args, dn1Proxy, DEFAULT_BANDWIDTH);
      args = new String[] { "-getBalancerBandwidth", dn2Address };
      runGetBalancerBandwidthCmd(admin, args, dn2Proxy, DEFAULT_BANDWIDTH);

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
      // verifies the dfsadmin command execution
      args = new String[] { "-getBalancerBandwidth", dn1Address };
      runGetBalancerBandwidthCmd(admin, args, dn1Proxy, newBandwidth);
      args = new String[] { "-getBalancerBandwidth", dn2Address };
      runGetBalancerBandwidthCmd(admin, args, dn2Proxy, newBandwidth);

      // Dynamically change balancer bandwidth to 0. Balancer bandwidth on the
      // datanodes should remain as it was.
      fs.setBalancerBandwidth(0);

      // Give it a few seconds to propogate new the value to the datanodes.
      try {
        Thread.sleep(5000);
      } catch (Exception e) {}

      assertEquals(newBandwidth, (long) datanodes.get(0).getBalancerBandwidth());
      assertEquals(newBandwidth, (long) datanodes.get(1).getBalancerBandwidth());
      // verifies the dfsadmin command execution
      args = new String[] { "-getBalancerBandwidth", dn1Address };
      runGetBalancerBandwidthCmd(admin, args, dn1Proxy, newBandwidth);
      args = new String[] { "-getBalancerBandwidth", dn2Address };
      runGetBalancerBandwidthCmd(admin, args, dn2Proxy, newBandwidth);
    } finally {
      cluster.shutdown();
    }
  }

  private void runGetBalancerBandwidthCmd(DFSAdmin admin, String[] args,
      ClientDatanodeProtocol proxy, long expectedBandwidth) throws Exception {
    PrintStream initialStdOut = System.out;
    outContent.reset();
    try {
      System.setOut(outStream);
      int exitCode = admin.run(args);
      assertEquals("DFSAdmin should return 0", 0, exitCode);
      String bandwidthOutMsg = "Balancer bandwidth is " + expectedBandwidth
          + " bytes per second.";
      String strOut = new String(outContent.toByteArray(), UTF8);
      assertTrue("Wrong balancer bandwidth!", strOut.contains(bandwidthOutMsg));
    } finally {
      System.setOut(initialStdOut);
    }
  }

  public static void main(String[] args) throws Exception {
    new TestBalancerBandwidth().testBalancerBandwidth();
  }
}
