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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * Unit tests for NodeQueueLoadMonitor.
 */
public class TestNodeQueueLoadMonitor {

  static class FakeNodeId extends NodeId {
    final String host;
    final int port;

    public FakeNodeId(String host, int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public String getHost() {
      return host;
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    protected void setHost(String host) {}
    @Override
    protected void setPort(int port) {}
    @Override
    protected void build() {}

    @Override
    public String toString() {
      return host + ":" + port;
    }
  }

  @Test
  public void testWaitTimeSort() {
    NodeQueueLoadMonitor selector = new NodeQueueLoadMonitor(
        NodeQueueLoadMonitor.LoadComparator.QUEUE_WAIT_TIME);
    selector.updateNode(createRMNode("h1", 1, 15, 10));
    selector.updateNode(createRMNode("h2", 2, 5, 10));
    selector.updateNode(createRMNode("h3", 3, 10, 10));
    selector.computeTask.run();
    List<NodeId> nodeIds = selector.selectNodes();
    System.out.println("1-> " + nodeIds);
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h3:3", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());

    // Now update node3
    selector.updateNode(createRMNode("h3", 3, 2, 10));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    System.out.println("2-> "+ nodeIds);
    Assert.assertEquals("h3:3", nodeIds.get(0).toString());
    Assert.assertEquals("h2:2", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());

    // Now send update with -1 wait time
    selector.updateNode(createRMNode("h4", 4, -1, 10));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    System.out.println("3-> "+ nodeIds);
    // No change
    Assert.assertEquals("h3:3", nodeIds.get(0).toString());
    Assert.assertEquals("h2:2", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());
  }

  @Test
  public void testQueueLengthSort() {
    NodeQueueLoadMonitor selector = new NodeQueueLoadMonitor(
        NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH);
    selector.updateNode(createRMNode("h1", 1, -1, 15));
    selector.updateNode(createRMNode("h2", 2, -1, 5));
    selector.updateNode(createRMNode("h3", 3, -1, 10));
    selector.computeTask.run();
    List<NodeId> nodeIds = selector.selectNodes();
    System.out.println("1-> " + nodeIds);
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h3:3", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());

    // Now update node3
    selector.updateNode(createRMNode("h3", 3, -1, 2));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    System.out.println("2-> "+ nodeIds);
    Assert.assertEquals("h3:3", nodeIds.get(0).toString());
    Assert.assertEquals("h2:2", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());

    // Now send update with -1 wait time but valid length
    selector.updateNode(createRMNode("h4", 4, -1, 20));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    System.out.println("3-> "+ nodeIds);
    // No change
    Assert.assertEquals("h3:3", nodeIds.get(0).toString());
    Assert.assertEquals("h2:2", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());
    Assert.assertEquals("h4:4", nodeIds.get(3).toString());
  }

  @Test
  public void testContainerQueuingLimit() {
    NodeQueueLoadMonitor selector = new NodeQueueLoadMonitor(
        NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH);
    selector.updateNode(createRMNode("h1", 1, -1, 15));
    selector.updateNode(createRMNode("h2", 2, -1, 5));
    selector.updateNode(createRMNode("h3", 3, -1, 10));

    // Test Mean Calculation
    selector.initThresholdCalculator(0, 6, 100);
    QueueLimitCalculator calculator = selector.getThresholdCalculator();
    ContainerQueuingLimit containerQueuingLimit = calculator
        .createContainerQueuingLimit();
    Assert.assertEquals(6, containerQueuingLimit.getMaxQueueLength());
    Assert.assertEquals(-1, containerQueuingLimit.getMaxQueueWaitTimeInMs());
    selector.computeTask.run();
    containerQueuingLimit = calculator.createContainerQueuingLimit();
    Assert.assertEquals(10, containerQueuingLimit.getMaxQueueLength());
    Assert.assertEquals(-1, containerQueuingLimit.getMaxQueueWaitTimeInMs());

    // Test Limits do not exceed specified max
    selector.updateNode(createRMNode("h1", 1, -1, 110));
    selector.updateNode(createRMNode("h2", 2, -1, 120));
    selector.updateNode(createRMNode("h3", 3, -1, 130));
    selector.updateNode(createRMNode("h4", 4, -1, 140));
    selector.updateNode(createRMNode("h5", 5, -1, 150));
    selector.updateNode(createRMNode("h6", 6, -1, 160));
    selector.computeTask.run();
    containerQueuingLimit = calculator.createContainerQueuingLimit();
    Assert.assertEquals(100, containerQueuingLimit.getMaxQueueLength());

    // Test Limits do not go below specified min
    selector.updateNode(createRMNode("h1", 1, -1, 1));
    selector.updateNode(createRMNode("h2", 2, -1, 2));
    selector.updateNode(createRMNode("h3", 3, -1, 3));
    selector.updateNode(createRMNode("h4", 4, -1, 4));
    selector.updateNode(createRMNode("h5", 5, -1, 5));
    selector.updateNode(createRMNode("h6", 6, -1, 6));
    selector.computeTask.run();
    containerQueuingLimit = calculator.createContainerQueuingLimit();
    Assert.assertEquals(6, containerQueuingLimit.getMaxQueueLength());

  }

  private RMNode createRMNode(String host, int port,
      int waitTime, int queueLength) {
    RMNode node1 = Mockito.mock(RMNode.class);
    NodeId nID1 = new FakeNodeId(host, port);
    Mockito.when(node1.getNodeID()).thenReturn(nID1);
    OpportunisticContainersStatus status1 =
        Mockito.mock(OpportunisticContainersStatus.class);
    Mockito.when(status1.getEstimatedQueueWaitTime())
        .thenReturn(waitTime);
    Mockito.when(status1.getWaitQueueLength())
        .thenReturn(queueLength);
    Mockito.when(node1.getOpportunisticContainersStatus()).thenReturn(status1);
    return node1;
  }
}
