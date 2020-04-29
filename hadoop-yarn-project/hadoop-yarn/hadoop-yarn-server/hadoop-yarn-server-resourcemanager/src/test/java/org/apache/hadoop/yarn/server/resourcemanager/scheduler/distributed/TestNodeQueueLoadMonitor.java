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
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Unit tests for NodeQueueLoadMonitor.
 */
public class TestNodeQueueLoadMonitor {

  private final static int DEFAULT_MAX_QUEUE_LENGTH = 200;

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

    // Now update node 2 to DECOMMISSIONING state
    selector
        .updateNode(createRMNode("h2", 2, 1, 10, NodeState.DECOMMISSIONING));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    Assert.assertEquals(2, nodeIds.size());
    Assert.assertEquals("h3:3", nodeIds.get(0).toString());
    Assert.assertEquals("h1:1", nodeIds.get(1).toString());

    // Now update node 2 back to RUNNING state
    selector.updateNode(createRMNode("h2", 2, 1, 10, NodeState.RUNNING));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h3:3", nodeIds.get(1).toString());
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

    // Now update h3 and fill its queue.
    selector.updateNode(createRMNode("h3", 3, -1,
        DEFAULT_MAX_QUEUE_LENGTH));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    System.out.println("4-> "+ nodeIds);
    Assert.assertEquals(3, nodeIds.size());
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h1:1", nodeIds.get(1).toString());
    Assert.assertEquals("h4:4", nodeIds.get(2).toString());

    // Now update h2 to Decommissioning state
    selector.updateNode(createRMNode("h2", 2, -1,
        5, NodeState.DECOMMISSIONING));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    Assert.assertEquals(2, nodeIds.size());
    Assert.assertEquals("h1:1", nodeIds.get(0).toString());
    Assert.assertEquals("h4:4", nodeIds.get(1).toString());

    // Now update h2 back to Running state
    selector.updateNode(createRMNode("h2", 2, -1,
        5, NodeState.RUNNING));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    Assert.assertEquals(3, nodeIds.size());
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h1:1", nodeIds.get(1).toString());
    Assert.assertEquals("h4:4", nodeIds.get(2).toString());
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

  /**
   * Tests selection of local node from NodeQueueLoadMonitor. This test covers
   * selection of node based on queue limit and blacklisted nodes.
   */
  @Test
  public void testSelectLocalNode() {
    NodeQueueLoadMonitor selector = new NodeQueueLoadMonitor(
        NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH);

    RMNode h1 = createRMNode("h1", 1, -1, 2, 5);
    RMNode h2 = createRMNode("h2", 2, -1, 5, 5);
    RMNode h3 = createRMNode("h3", 3, -1, 4, 5);

    selector.addNode(null, h1);
    selector.addNode(null, h2);
    selector.addNode(null, h3);

    selector.updateNode(h1);
    selector.updateNode(h2);
    selector.updateNode(h3);

    // basic test for selecting node which has queue length less
    // than queue capacity.
    Set<String> blacklist = new HashSet<>();
    RMNode node = selector.selectLocalNode("h1", blacklist);
    Assert.assertEquals("h1", node.getHostName());

    // if node has been added to blacklist
    blacklist.add("h1");
    node = selector.selectLocalNode("h1", blacklist);
    Assert.assertNull(node);

    node = selector.selectLocalNode("h2", blacklist);
    Assert.assertNull(node);

    node = selector.selectLocalNode("h3", blacklist);
    Assert.assertEquals("h3", node.getHostName());
  }

  /**
   * Tests selection of rack local node from NodeQueueLoadMonitor. This test
   * covers selection of node based on queue limit and blacklisted nodes.
   */
  @Test
  public void testSelectRackLocalNode() {
    NodeQueueLoadMonitor selector = new NodeQueueLoadMonitor(
        NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH);

    RMNode h1 = createRMNode("h1", 1, "rack1", -1, 2, 5);
    RMNode h2 = createRMNode("h2", 2, "rack2", -1, 5, 5);
    RMNode h3 = createRMNode("h3", 3, "rack2", -1, 4, 5);

    selector.addNode(null, h1);
    selector.addNode(null, h2);
    selector.addNode(null, h3);

    selector.updateNode(h1);
    selector.updateNode(h2);
    selector.updateNode(h3);

    // basic test for selecting node which has queue length less
    // than queue capacity.
    Set<String> blacklist = new HashSet<>();
    RMNode node = selector.selectRackLocalNode("rack1", blacklist);
    Assert.assertEquals("h1", node.getHostName());

    // if node has been added to blacklist
    blacklist.add("h1");
    node = selector.selectRackLocalNode("rack1", blacklist);
    Assert.assertNull(node);

    node = selector.selectRackLocalNode("rack2", blacklist);
    Assert.assertEquals("h3", node.getHostName());

    blacklist.add("h3");
    node = selector.selectRackLocalNode("rack2", blacklist);
    Assert.assertNull(node);
  }

  /**
   * Tests selection of any node from NodeQueueLoadMonitor. This test
   * covers selection of node based on queue limit and blacklisted nodes.
   */
  @Test
  public void testSelectAnyNode() {
    NodeQueueLoadMonitor selector = new NodeQueueLoadMonitor(
        NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH);

    RMNode h1 = createRMNode("h1", 1, "rack1", -1, 2, 5);
    RMNode h2 = createRMNode("h2", 2, "rack2", -1, 5, 5);
    RMNode h3 = createRMNode("h3", 3, "rack2", -1, 4, 10);

    selector.addNode(null, h1);
    selector.addNode(null, h2);
    selector.addNode(null, h3);

    selector.updateNode(h1);
    selector.updateNode(h2);
    selector.updateNode(h3);

    selector.computeTask.run();

    Assert.assertEquals(2, selector.getSortedNodes().size());

    // basic test for selecting node which has queue length
    // less than queue capacity.
    Set<String> blacklist = new HashSet<>();
    RMNode node = selector.selectAnyNode(blacklist);
    Assert.assertTrue(node.getHostName().equals("h1") ||
        node.getHostName().equals("h3"));

    // if node has been added to blacklist
    blacklist.add("h1");
    node = selector.selectAnyNode(blacklist);
    Assert.assertEquals("h3", node.getHostName());

    blacklist.add("h3");
    node = selector.selectAnyNode(blacklist);
    Assert.assertNull(node);
  }

  private RMNode createRMNode(String host, int port,
      int waitTime, int queueLength) {
    return createRMNode(host, port, waitTime, queueLength,
        DEFAULT_MAX_QUEUE_LENGTH);
  }

  private RMNode createRMNode(String host, int port,
      int waitTime, int queueLength, NodeState state) {
    return createRMNode(host, port, "default", waitTime, queueLength,
        DEFAULT_MAX_QUEUE_LENGTH, state);
  }

  private RMNode createRMNode(String host, int port,
      int waitTime, int queueLength, int queueCapacity) {
    return createRMNode(host, port, "default", waitTime, queueLength,
        queueCapacity, NodeState.RUNNING);
  }

  private RMNode createRMNode(String host, int port, String rack,
      int waitTime, int queueLength, int queueCapacity) {
    return createRMNode(host, port, rack, waitTime, queueLength, queueCapacity,
        NodeState.RUNNING);
  }

  private RMNode createRMNode(String host, int port, String rack,
      int waitTime, int queueLength, int queueCapacity, NodeState state) {
    RMNode node1 = Mockito.mock(RMNode.class);
    NodeId nID1 = new FakeNodeId(host, port);
    Mockito.when(node1.getHostName()).thenReturn(host);
    Mockito.when(node1.getRackName()).thenReturn(rack);
    Mockito.when(node1.getNodeID()).thenReturn(nID1);
    Mockito.when(node1.getState()).thenReturn(state);
    OpportunisticContainersStatus status1 =
        Mockito.mock(OpportunisticContainersStatus.class);
    Mockito.when(status1.getEstimatedQueueWaitTime())
        .thenReturn(waitTime);
    Mockito.when(status1.getWaitQueueLength())
        .thenReturn(queueLength);
    Mockito.when(status1.getOpportQueueCapacity())
        .thenReturn(queueCapacity);
    Mockito.when(node1.getOpportunisticContainersStatus()).thenReturn(status1);
    return node1;
  }
}
