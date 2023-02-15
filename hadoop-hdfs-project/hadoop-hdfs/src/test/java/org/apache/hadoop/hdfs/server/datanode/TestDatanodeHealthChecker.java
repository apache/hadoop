/*
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

import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;

import static org.junit.Assert.assertTrue;

/**
 * Tests to validate the datanode behaviour when it is either not healthy or does not
 * stay connected to active namenode for long time.
 */
public class TestDatanodeHealthChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestDatanodeHealthChecker.class);

  @Test
  public void testDatanodeShutdownWhenActiveNNDown() throws Exception {
    Configuration conf = new HdfsConfiguration();
    // if datanode cannot receive heartbeat response from active namenode in 3s,
    // datanodehealthchecker would terminate it.
    conf.setLong(DFSConfigKeys.DFS_DATANODE_HEALTH_ACTIVENNCONNECT_TIMEOUT, 3000);
    conf.setLong(DataNodeHealthChecker.DFS_DATANODE_HEATHCHECK_RUN_INTERVAL, 1000);

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
        .build()) {

      cluster.waitActive();

      NameNode nameNode = cluster.getNameNode();
      List<DataNode> dataNodes = cluster.getDataNodes();

      cluster.waitDatanodeConnectedToActive(dataNodes.get(0), 2000);
      cluster.waitDatanodeConnectedToActive(dataNodes.get(1), 2000);
      cluster.waitDatanodeConnectedToActive(dataNodes.get(2), 2000);

      LOGGER.info("The cluster is up. Namenode state: {}, Num of datanodes: {}",
          nameNode.getState(), dataNodes.size());

      Thread.sleep(5000);

      assertTrue(dataNodes.get(0).shouldRun());
      assertTrue(dataNodes.get(1).shouldRun());
      assertTrue(dataNodes.get(2).shouldRun());

      LOGGER.info("Shutting down namenode");

      nameNode.stop();
      nameNode.join();

      // in some time, all datanodes should be shutdown
      GenericTestUtils.waitFor(() -> !dataNodes.get(0).shouldRun(), 100, 7000,
          "Datanode should not be running after loosing connection to active namenode. "
              + "Waited 7000 ms for datanode to be shutdown");
      GenericTestUtils.waitFor(() -> !dataNodes.get(1).shouldRun(), 100, 7000,
          "Datanode should not be running after loosing connection to active namenode. "
              + "Waited 7000 ms for datanode to be shutdown");
      GenericTestUtils.waitFor(() -> !dataNodes.get(2).shouldRun(), 100, 7000,
          "Datanode should not be running after loosing connection to active namenode. "
              + "Waited 7000 ms for datanode to be shutdown");
    }
  }

  @Test
  public void testDatanodeShutdownWithMultipleNNs() throws Exception {
    Configuration conf = new HdfsConfiguration();
    // if datanode cannot receive heartbeat response from active namenode in 3s,
    // datanodehealthchecker would terminate it.
    conf.setLong(DFSConfigKeys.DFS_DATANODE_HEALTH_ACTIVENNCONNECT_TIMEOUT, 3000);
    conf.setLong(DataNodeHealthChecker.DFS_DATANODE_HEATHCHECK_RUN_INTERVAL, 1000);
    conf.setLong(DataNodeHealthChecker.DFS_DATANODE_HEATHCHECK_RUN_INIT_DELAY, 7000);

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology(2)).numDataNodes(2)
        .build()) {
      cluster.waitActive();
      cluster.transitionToActive(0);
      cluster.transitionToStandby(1);

      NameNode activeNamenode = cluster.getNameNode(0);
      NameNode standbyNamenode = cluster.getNameNode(1);
      List<DataNode> dataNodes = cluster.getDataNodes();

      cluster.waitDatanodeConnectedToActive(dataNodes.get(0), 5000);
      cluster.waitDatanodeConnectedToActive(dataNodes.get(1), 5000);

      LOGGER.info("The cluster is up. Namenode state: {}, Num of datanodes: {}",
          activeNamenode.getState(), dataNodes.size());

      Thread.sleep(5000);

      assertTrue(dataNodes.get(0).shouldRun());
      assertTrue(dataNodes.get(1).shouldRun());

      LOGGER.info("Shutting down namenode");

      activeNamenode.stop();
      activeNamenode.join();
      standbyNamenode.stop();
      standbyNamenode.join();

      // in some time, all datanodes should be shutdown
      GenericTestUtils.waitFor(() -> !dataNodes.get(0).shouldRun(), 100, 7000,
          "Datanode should not be running after loosing connection to active namenode. "
              + "Waited 7000 ms for datanode to be shutdown");
      GenericTestUtils.waitFor(() -> !dataNodes.get(1).shouldRun(), 100, 7000,
          "Datanode should not be running after loosing connection to active namenode. "
              + "Waited 7000 ms for datanode to be shutdown");
    }
  }

  @Test
  public void testDatanodeHealthyWhenActiveNNIsDown() throws Exception {
    Configuration conf = new HdfsConfiguration();

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
        .build()) {

      cluster.waitActive();

      NameNode nameNode = cluster.getNameNode();
      List<DataNode> dataNodes = cluster.getDataNodes();

      cluster.waitDatanodeConnectedToActive(dataNodes.get(0), 2000);
      cluster.waitDatanodeConnectedToActive(dataNodes.get(1), 2000);
      cluster.waitDatanodeConnectedToActive(dataNodes.get(2), 2000);

      LOGGER.info("The cluster is up. Namenode state: {}, Num of datanodes: {}",
          nameNode.getState(), dataNodes.size());

      assertTrue(dataNodes.get(0).shouldRun());
      assertTrue(dataNodes.get(1).shouldRun());
      assertTrue(dataNodes.get(2).shouldRun());

      nameNode.stop();
      nameNode.join();

      Thread.sleep(5000);

      // health of the datanodes is not affected after loosing connection to active namenode
      assertTrue(dataNodes.get(0).shouldRun());
      assertTrue(dataNodes.get(1).shouldRun());
      assertTrue(dataNodes.get(2).shouldRun());
    }
  }

}