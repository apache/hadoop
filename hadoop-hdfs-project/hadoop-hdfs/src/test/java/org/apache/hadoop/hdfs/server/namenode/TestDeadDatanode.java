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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.junit.After;
import org.junit.Test;

/**
 * Test to ensure requests from dead datnodes are rejected by namenode with
 * appropriate exceptions/failure response
 */
public class TestDeadDatanode {
  private static final Log LOG = LogFactory.getLog(TestDeadDatanode.class);
  private MiniDFSCluster cluster;

  @After
  public void cleanup() {
    cluster.shutdown();
  }

  /**
   * wait for datanode to reach alive or dead state for waitTime given in
   * milliseconds.
   */
  private void waitForDatanodeState(String nodeID, boolean alive, int waitTime)
      throws TimeoutException, InterruptedException {
    long stopTime = System.currentTimeMillis() + waitTime;
    FSNamesystem namesystem = cluster.getNamesystem();
    String state = alive ? "alive" : "dead";
    while (System.currentTimeMillis() < stopTime) {
      final DatanodeDescriptor dd = BlockManagerTestUtil.getDatanode(
          namesystem, nodeID);
      if (dd.isAlive == alive) {
        LOG.info("datanode " + nodeID + " is " + state);
        return;
      }
      LOG.info("Waiting for datanode " + nodeID + " to become " + state);
      Thread.sleep(1000);
    }
    throw new TimeoutException("Timedout waiting for datanode reach state "
        + state);
  }

  /**
   * Test to ensure namenode rejects request from dead datanode
   * - Start a cluster
   * - Shutdown the datanode and wait for it to be marked dead at the namenode
   * - Send datanode requests to Namenode and make sure it is rejected 
   *   appropriately.
   */
  @Test
  public void testDeadDatanode() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();

    String poolId = cluster.getNamesystem().getBlockPoolId();
    // wait for datanode to be marked live
    DataNode dn = cluster.getDataNodes().get(0);
    DatanodeRegistration reg = 
      DataNodeTestUtils.getDNRegistrationForBP(cluster.getDataNodes().get(0), poolId);
      
    waitForDatanodeState(reg.getStorageID(), true, 20000);

    // Shutdown and wait for datanode to be marked dead
    dn.shutdown();
    waitForDatanodeState(reg.getStorageID(), false, 20000);

    DatanodeProtocol dnp = cluster.getNameNodeRpc();
    
    ReceivedDeletedBlockInfo[] blocks = { new ReceivedDeletedBlockInfo(
        new Block(0), "") };
    
    // Ensure blockReceived call from dead datanode is rejected with IOException
    try {
      dnp.blockReceivedAndDeleted(reg, poolId, blocks);
      Assert.fail("Expected IOException is not thrown");
    } catch (IOException ex) {
      // Expected
    }

    // Ensure blockReport from dead datanode is rejected with IOException
    long[] blockReport = new long[] { 0L, 0L, 0L };
    try {
      dnp.blockReport(reg, poolId, blockReport);
      Assert.fail("Expected IOException is not thrown");
    } catch (IOException ex) {
      // Expected
    }

    // Ensure heartbeat from dead datanode is rejected with a command
    // that asks datanode to register again
    DatanodeCommand[] cmd = dnp.sendHeartbeat(reg, 0, 0, 0, 0, 0, 0, 0);
    Assert.assertEquals(1, cmd.length);
    Assert.assertEquals(cmd[0].getAction(), DatanodeCommand.REGISTER
        .getAction());
  }
}
