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

package org.apache.hadoop.yarn.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster.CustomNodeManager;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.junit.Before;
import org.junit.Test;

public class TestMiniYarnClusterNodeUtilization {
  // Mini YARN cluster setup
  private static final int NUM_RM = 1;
  private static final int NUM_NM = 1;

  // Values for the first round
  private static final int CONTAINER_PMEM_1 = 1024;
  private static final int CONTAINER_VMEM_1 = 2048;
  private static final float CONTAINER_CPU_1 = 11.0f;

  private static final int NODE_PMEM_1 = 10240;
  private static final int NODE_VMEM_1 = 20480;
  private static final float NODE_CPU_1 = 51.0f;

  // Values for the second round
  private static final int CONTAINER_PMEM_2 = 2048;
  private static final int CONTAINER_VMEM_2 = 4096;
  private static final float CONTAINER_CPU_2 = 22.0f;

  private static final int NODE_PMEM_2 = 20480;
  private static final int NODE_VMEM_2 = 40960;
  private static final float NODE_CPU_2 = 61.0f;

  private MiniYARNCluster cluster;
  private CustomNodeManager nm;

  private Configuration conf;

  private NodeStatus nodeStatus;

  @Before
  public void setup() {
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:0");
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);
    String name = TestMiniYarnClusterNodeUtilization.class.getName();
    cluster = new MiniYARNCluster(name, NUM_RM, NUM_NM, 1, 1);
    cluster.init(conf);
    cluster.start();
    assertFalse("RM never turned active", -1 == cluster.getActiveRMIndex());

    nm = (CustomNodeManager)cluster.getNodeManager(0);
    nodeStatus = createNodeStatus(nm.getNMContext().getNodeId(), 0,
        CONTAINER_PMEM_1, CONTAINER_VMEM_1, CONTAINER_CPU_1,
        NODE_PMEM_1, NODE_VMEM_1, NODE_CPU_1);
    nm.setNodeStatus(nodeStatus);
  }

  /**
   * Simulates a NM heartbeat using the simulated NodeStatus fixture. Verify
   * both the RMNode and SchedulerNode have been updated with the new
   * utilization.
   */
  @Test(timeout=60000)
  public void testUpdateNodeUtilization()
      throws InterruptedException, IOException, YarnException {
    assertTrue("NMs fail to connect to the RM",
        cluster.waitForNodeManagersToConnect(10000));

    // Give the heartbeat time to propagate to the RM
    verifySimulatedUtilization();

    // Alter utilization
    nodeStatus = createNodeStatus(nm.getNMContext().getNodeId(), 0,
        CONTAINER_PMEM_2, CONTAINER_VMEM_2, CONTAINER_CPU_2,
        NODE_PMEM_2, NODE_VMEM_2, NODE_CPU_2);
    nm.setNodeStatus(nodeStatus);

    // Give the heartbeat time to propagate to the RM
    verifySimulatedUtilization();
  }

  /**
   * Trigger the NM to send a heartbeat using the simulated NodeStatus fixture.
   * Verify both the RMNode and SchedulerNode have been updated with the new
   * utilization.
   */
  @Test(timeout=60000)
  public void testMockNodeStatusHeartbeat()
      throws InterruptedException, YarnException {
    assertTrue("NMs fail to connect to the RM",
        cluster.waitForNodeManagersToConnect(10000));

    NodeStatusUpdater updater = nm.getNodeStatusUpdater();
    updater.sendOutofBandHeartBeat();

    // Give the heartbeat time to propagate to the RM
    verifySimulatedUtilization();

    // Alter utilization
    nodeStatus = createNodeStatus(nm.getNMContext().getNodeId(), 0,
        CONTAINER_PMEM_2, CONTAINER_VMEM_2, CONTAINER_CPU_2,
        NODE_PMEM_2, NODE_VMEM_2, NODE_CPU_2);
    nm.setNodeStatus(nodeStatus);
    updater.sendOutofBandHeartBeat();

    verifySimulatedUtilization();
  }

  /**
   * Create a NodeStatus test vector.
   * @param nodeId Node identifier.
   * @param responseId Response identifier.
   * @param containerPMem Virtual memory of the container.
   * @param containerVMem Physical memory of the container.
   * @param containerCPU CPU percentage of the container.
   * @param nodePMem Physical memory of the node.
   * @param nodeVMem Virtual memory of the node.
   * @param nodeCPU CPU percentage of the node.
   */
  private NodeStatus createNodeStatus(
      NodeId nodeId,
      int responseId,
      int containerPMem,
      int containerVMem,
      float containerCPU,
      int nodePMem,
      int nodeVMem,
      float nodeCPU) {

    // Fake node status with fake utilization
    ResourceUtilization containersUtilization =
        ResourceUtilization.newInstance(containerPMem, containerVMem,
            containerCPU);
    ResourceUtilization nodeUtilization =
        ResourceUtilization.newInstance(nodePMem, nodeVMem, nodeCPU);
    NodeStatus status = NodeStatus.newInstance(
        nodeId,
        responseId,
        new ArrayList<ContainerStatus>(),
        null,
        NodeHealthStatus.newInstance(true, null, 0),
        containersUtilization,
        nodeUtilization,
        null);

    return status;
  }

  /**
   * Verify both the RMNode and SchedulerNode have been updated with the test
   * fixture utilization data.
   */
  private void verifySimulatedUtilization() throws InterruptedException {
    ResourceManager rm = cluster.getResourceManager(0);
    RMContext rmContext = rm.getRMContext();

    ResourceUtilization containersUtilization =
        nodeStatus.getContainersUtilization();
    ResourceUtilization nodeUtilization =
        nodeStatus.getNodeUtilization();

    // Give the heartbeat time to propagate to the RM (max 10 seconds)
    // We check if the nodeUtilization is up to date
    for (int i=0; i<100; i++) {
      for (RMNode ni : rmContext.getRMNodes().values()) {
        if (ni.getNodeUtilization() != null) {
            if (ni.getNodeUtilization().equals(nodeUtilization)) {
              break;
            }
        }
      }
      Thread.sleep(100);
    }

    // Verify the data is readable from the RM and scheduler nodes
    for (RMNode ni : rmContext.getRMNodes().values()) {
      ResourceUtilization cu = ni.getAggregatedContainersUtilization();
      assertEquals("Containers Utillization not propagated to RMNode",
          containersUtilization, cu);

      ResourceUtilization nu = ni.getNodeUtilization();
      assertEquals("Node Utillization not propagated to RMNode",
          nodeUtilization, nu);

      SchedulerNode scheduler =
          rmContext.getScheduler().getSchedulerNode(ni.getNodeID());
      cu = scheduler.getAggregatedContainersUtilization();
      assertEquals("Containers Utillization not propagated to SchedulerNode",
          containersUtilization, cu);

      nu = scheduler.getNodeUtilization();
      assertEquals("Node Utillization not propagated to SchedulerNode",
          nodeUtilization, nu);
    }
  }
}
