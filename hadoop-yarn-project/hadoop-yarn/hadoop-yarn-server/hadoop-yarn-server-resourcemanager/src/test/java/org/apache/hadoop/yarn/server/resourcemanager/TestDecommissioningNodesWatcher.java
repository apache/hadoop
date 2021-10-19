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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.DecommissioningNodesWatcher.DecommissioningNodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * This class tests DecommissioningNodesWatcher.
 */
public class TestDecommissioningNodesWatcher {
  private MockRM rm;

  @Test
  public void testDecommissioningNodesWatcher() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODE_GRACEFUL_DECOMMISSION_TIMEOUT, "40");

    rm = new MockRM(conf);
    rm.start();

    DecommissioningNodesWatcher watcher =
        new DecommissioningNodesWatcher(rm.getRMContext());

    MockNM nm1 = rm.registerNode("host1:1234", 10240);
    RMNodeImpl node1 =
        (RMNodeImpl) rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    NodeId id1 = nm1.getNodeId();

    rm.waitForState(id1, NodeState.RUNNING);

    RMApp app = MockRMAppSubmitter.submitWithMemory(2000, rm);
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm1);

    NodeStatus nodeStatus = createNodeStatus(id1, app, 3);
    node1.handle(new RMNodeStatusEvent(nm1.getNodeId(), nodeStatus));

    // Setup nm1 as DECOMMISSIONING for DecommissioningNodesWatcher.
    rm.sendNodeGracefulDecommission(nm1,
        YarnConfiguration.DEFAULT_RM_NODE_GRACEFUL_DECOMMISSION_TIMEOUT);
    rm.waitForState(id1, NodeState.DECOMMISSIONING);

    // Update status with decreasing number of running containers until 0.
    nodeStatus = createNodeStatus(id1, app, 3);
    node1.handle(new RMNodeStatusEvent(nm1.getNodeId(), nodeStatus));
    watcher.update(node1, nodeStatus);

    nodeStatus = createNodeStatus(id1, app, 2);
    node1.handle(new RMNodeStatusEvent(nm1.getNodeId(), nodeStatus));
    watcher.update(node1, nodeStatus);
    Assert.assertFalse(watcher.checkReadyToBeDecommissioned(id1));

    nodeStatus = createNodeStatus(id1, app, 1);
    node1.handle(new RMNodeStatusEvent(nm1.getNodeId(), nodeStatus));
    watcher.update(node1, nodeStatus);
    Assert.assertEquals(DecommissioningNodeStatus.WAIT_CONTAINER,
        watcher.checkDecommissioningStatus(id1));

    nodeStatus = createNodeStatus(id1, app, 0);
    watcher.update(node1, nodeStatus);
    node1.handle(new RMNodeStatusEvent(nm1.getNodeId(), nodeStatus));
    Assert.assertEquals(DecommissioningNodeStatus.WAIT_APP,
        watcher.checkDecommissioningStatus(id1));

    // Set app to be FINISHED and verified DecommissioningNodeStatus is READY.
    MockRM.finishAMAndVerifyAppState(app, rm, nm1, am);
    rm.waitForState(app.getApplicationId(), RMAppState.FINISHED);
    watcher.update(node1, nodeStatus);
    Assert.assertEquals(DecommissioningNodeStatus.READY,
        watcher.checkDecommissioningStatus(id1));
  }

  @Test
  public void testDecommissioningNodesWatcherWithPreviousRunningApps()
      throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODE_GRACEFUL_DECOMMISSION_TIMEOUT, "40");

    rm = new MockRM(conf);
    rm.start();

    DecommissioningNodesWatcher watcher =
        new DecommissioningNodesWatcher(rm.getRMContext());

    MockNM nm1 = rm.registerNode("host1:1234", 10240);
    RMNodeImpl node1 =
        (RMNodeImpl) rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    NodeId id1 = nm1.getNodeId();

    rm.waitForState(id1, NodeState.RUNNING);

    RMApp app = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(2000, rm).build());
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm1);

    NodeStatus nodeStatus = createNodeStatus(id1, app, 3);
    node1.handle(new RMNodeStatusEvent(nm1.getNodeId(), nodeStatus));

    Assert.assertEquals(1, node1.getRunningApps().size());

    // update node with 0 running containers
    nodeStatus = createNodeStatus(id1, app, 0);
    node1.handle(new RMNodeStatusEvent(nm1.getNodeId(), nodeStatus));

    Assert.assertEquals(1, node1.getRunningApps().size());

    // Setup nm1 as DECOMMISSIONING for DecommissioningNodesWatcher. Right now
    // there is no container running on the node.
    rm.sendNodeGracefulDecommission(nm1,
        YarnConfiguration.DEFAULT_RM_NODE_GRACEFUL_DECOMMISSION_TIMEOUT);
    rm.waitForState(id1, NodeState.DECOMMISSIONING);

    // we should still get WAIT_APP as container for a running app previously
    // ran on this node.
    watcher.update(node1, nodeStatus);
    Assert.assertFalse(watcher.checkReadyToBeDecommissioned(id1));
    Assert.assertEquals(DecommissioningNodeStatus.WAIT_APP,
        watcher.checkDecommissioningStatus(id1));

    // Set app to be FINISHED and verified DecommissioningNodeStatus is READY.
    MockRM.finishAMAndVerifyAppState(app, rm, nm1, am);
    rm.waitForState(app.getApplicationId(), RMAppState.FINISHED);
    Assert.assertEquals(0, node1.getRunningApps().size());
    watcher.update(node1, nodeStatus);
    Assert.assertEquals(DecommissioningNodeStatus.READY,
        watcher.checkDecommissioningStatus(id1));
  }

  @After
  public void tearDown() {
    if (rm != null) {
      rm.stop();
    }
  }

  private NodeStatus createNodeStatus(
      NodeId nodeId, RMApp app, int numRunningContainers) {
    return NodeStatus.newInstance(
        nodeId, 0, getContainerStatuses(app, numRunningContainers),
        Collections.emptyList(),
        NodeHealthStatus.newInstance(
            true,  "", System.currentTimeMillis() - 1000),
        null, null, null);
  }

  // Get mocked ContainerStatus for bunch of containers,
  // where numRunningContainers are RUNNING.
  private List<ContainerStatus> getContainerStatuses(
      RMApp app, int numRunningContainers) {
    // Total 3 containers
    final int total = 3;
    numRunningContainers = Math.min(total, numRunningContainers);
    List<ContainerStatus> output = new ArrayList<ContainerStatus>();
    for (int i = 0; i < total; i++) {
      ContainerState cstate = (i >= numRunningContainers)?
          ContainerState.COMPLETE : ContainerState.RUNNING;
      output.add(ContainerStatus.newInstance(
          ContainerId.newContainerId(
              ApplicationAttemptId.newInstance(app.getApplicationId(), 0), i),
          cstate, "", 0));
    }
    return output;
  }
}

