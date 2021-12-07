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

import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for NodeQueueLoadMonitor.
 */
public class TestNodeQueueLoadMonitor {

  // Extra resource type to test that all resource dimensions are considered
  private static final String NETWORK_RESOURCE = "network";
  private final static int DEFAULT_MAX_QUEUE_LENGTH = 200;

  // Note: The following variables are private static resources
  // re-initialized on each test because resource dimensions considered
  // are initialized in a static method.
  // Declaring them as static final will "lock-in" resource dimensions and
  // disallow specification of a new resource dimension ("network") in tests.
  private static Resource defaultResourceRequested;
  private static Resource defaultCapacity;

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

  private static Resource newResourceInstance(long memory, int vCores) {
    return newResourceInstance(memory, vCores, 0L);
  }

  private static Resource newResourceInstance(
      final long memory, final int vCores, final long network) {
    return Resource.newInstance(memory, vCores,
        ImmutableMap.of(NETWORK_RESOURCE, network));
  }

  private static long getNetworkResourceValue(final Resource resource) {
    return resource.getResourceValue(NETWORK_RESOURCE);
  }

  public static void addNewTypesToResources(String... resourceTypes) {
    // Initialize resource map
    Map<String, ResourceInformation> riMap = new HashMap<>();

    // Initialize mandatory resources
    riMap.put(ResourceInformation.MEMORY_URI, ResourceInformation.MEMORY_MB);
    riMap.put(ResourceInformation.VCORES_URI, ResourceInformation.VCORES);

    for (String newResource : resourceTypes) {
      riMap.put(newResource, ResourceInformation
          .newInstance(newResource, "", 0, ResourceTypes.COUNTABLE, 0,
              Integer.MAX_VALUE));
    }

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);
  }

  @BeforeClass
  public static void classSetUp() {
    addNewTypesToResources(NETWORK_RESOURCE);
    defaultResourceRequested = newResourceInstance(128, 1, 1);
    defaultCapacity = newResourceInstance(1024, 8, 1000);
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
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h3:3", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());

    // Now update node3
    selector.updateNode(createRMNode("h3", 3, 2, 10));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    Assert.assertEquals("h3:3", nodeIds.get(0).toString());
    Assert.assertEquals("h2:2", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());

    // Now send update with -1 wait time
    selector.updateNode(createRMNode("h4", 4, -1, 10));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
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
  public void testQueueLengthThenResourcesSort() {
    NodeQueueLoadMonitor selector = new NodeQueueLoadMonitor(
        NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH_THEN_RESOURCES);

    // Node and queue sizes were selected such that we can determine the
    // order of these nodes in the selectNodes call deterministically
    // h2 -> h1 -> h3 -> h4
    selector.updateNode(createRMNode(
        "h1", 1, -1, 0,
        Resources.multiply(defaultResourceRequested, 3), defaultCapacity));
    selector.updateNode(createRMNode(
        "h2", 2, -1, 0,
        Resources.multiply(defaultResourceRequested, 2), defaultCapacity));
    selector.updateNode(createRMNode(
        "h3", 3, -1, 5,
        Resources.multiply(defaultResourceRequested, 3), defaultCapacity));
    selector.updateNode(createRMNode(
        "h4", 4, -1, 10,
        Resources.multiply(defaultResourceRequested, 2), defaultCapacity));
    selector.computeTask.run();
    List<NodeId> nodeIds = selector.selectNodes();
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h1:1", nodeIds.get(1).toString());
    Assert.assertEquals("h3:3", nodeIds.get(2).toString());
    Assert.assertEquals("h4:4", nodeIds.get(3).toString());

    // Now update node3
    // node3 should now rank after node4 since it has the same queue length
    // but less resources available
    selector.updateNode(createRMNode(
        "h3", 3, -1, 10,
        Resources.multiply(defaultResourceRequested, 3), defaultCapacity));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h1:1", nodeIds.get(1).toString());
    Assert.assertEquals("h4:4", nodeIds.get(2).toString());
    Assert.assertEquals("h3:3", nodeIds.get(3).toString());

    // Now update h3 and fill its queue -- it should no longer be available
    selector.updateNode(createRMNode("h3", 3, -1,
        DEFAULT_MAX_QUEUE_LENGTH));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    // h3 is queued up, so we should only have 3 nodes left
    Assert.assertEquals(3, nodeIds.size());
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h1:1", nodeIds.get(1).toString());
    Assert.assertEquals("h4:4", nodeIds.get(2).toString());

    // Now update h2 to Decommissioning state
    selector.updateNode(createRMNode("h2", 2, -1,
        5, NodeState.DECOMMISSIONING));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    // h2 is decommissioned, and h3 is full, so we should only have 2 nodes
    Assert.assertEquals(2, nodeIds.size());
    Assert.assertEquals("h1:1", nodeIds.get(0).toString());
    Assert.assertEquals("h4:4", nodeIds.get(1).toString());

    // Now update h2 back to Running state
    selector.updateNode(createRMNode(
        "h2", 2, -1, 0,
        Resources.multiply(defaultResourceRequested, 2), defaultCapacity));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    Assert.assertEquals(3, nodeIds.size());
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h1:1", nodeIds.get(1).toString());
    Assert.assertEquals("h4:4", nodeIds.get(2).toString());

    // Now update h2 to have a zero queue capacity.
    // Make sure that here it is still in the pool.
    selector.updateNode(createRMNode(
        "h2", 2, -1, 0, 0,
        Resources.multiply(defaultResourceRequested, 2),
        defaultCapacity));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    Assert.assertEquals(3, nodeIds.size());
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h1:1", nodeIds.get(1).toString());
    Assert.assertEquals("h4:4", nodeIds.get(2).toString());

    // Now update h2 to have a positive queue length but a zero queue capacity.
    // Make sure that here it is no longer in the pool.
    // Need to first remove the node, because node capacity is not updated.
    selector.removeNode(createRMNode(
        "h2", 2, -1, 0, 0,
        Resources.multiply(defaultResourceRequested, 2),
        defaultCapacity));
    selector.updateNode(createRMNode(
        "h2", 2, -1, 1, 0,
        Resources.multiply(defaultResourceRequested, 2),
        defaultCapacity));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    Assert.assertEquals(2, nodeIds.size());
    Assert.assertEquals("h1:1", nodeIds.get(0).toString());
    Assert.assertEquals("h4:4", nodeIds.get(1).toString());
  }

  /**
   * Tests that when using QUEUE_LENGTH_THEN_RESOURCES decrements the amount
   * of resources on the internal {@link ClusterNode} representation.
   */
  @Test
  public void testQueueLengthThenResourcesDecrementsAvailable() {
    NodeQueueLoadMonitor selector = new NodeQueueLoadMonitor(
        NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH_THEN_RESOURCES);
    RMNode node = createRMNode("h1", 1, -1, 0);
    selector.addNode(null, node);
    selector.updateNode(node);
    selector.updateSortedNodes();

    ClusterNode clusterNode = selector.getClusterNodes().get(node.getNodeID());
    Assert.assertEquals(Resources.none(),
        clusterNode.getAllocatedResource());

    // Has enough resources
    RMNode selectedNode = selector.selectAnyNode(
        Collections.emptySet(), defaultResourceRequested);
    Assert.assertNotNull(selectedNode);
    Assert.assertEquals(node.getNodeID(), selectedNode.getNodeID());

    clusterNode = selector.getClusterNodes().get(node.getNodeID());
    Assert.assertEquals(defaultResourceRequested,
        clusterNode.getAllocatedResource());

    // Does not have enough resources, but can queue
    selectedNode = selector.selectAnyNode(
        Collections.emptySet(), defaultCapacity);
    Assert.assertNotNull(selectedNode);
    Assert.assertEquals(node.getNodeID(), selectedNode.getNodeID());

    clusterNode = selector.getClusterNodes().get(node.getNodeID());
    Assert.assertEquals(1, clusterNode.getQueueLength());

    // Does not have enough resources and cannot queue
    selectedNode = selector.selectAnyNode(
        Collections.emptySet(),
        Resources.add(defaultResourceRequested, defaultCapacity));
    Assert.assertNull(selectedNode);
  }

  @Test
  public void testQueueLengthThenResourcesCapabilityChange() {
    NodeQueueLoadMonitor selector = new NodeQueueLoadMonitor(
        NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH_THEN_RESOURCES);

    // Node sizes were selected such that we can determine the
    // order of these nodes in the selectNodes call deterministically
    // h1 -> h2 -> h3 -> h4
    selector.updateNode(createRMNode(
        "h1", 1, -1, 0,
        Resources.multiply(defaultResourceRequested, 1), defaultCapacity));
    selector.updateNode(createRMNode(
        "h2", 2, -1, 0,
        Resources.multiply(defaultResourceRequested, 2), defaultCapacity));
    selector.updateNode(createRMNode(
        "h3", 3, -1, 0,
        Resources.multiply(defaultResourceRequested, 3), defaultCapacity));
    selector.updateNode(createRMNode(
        "h4", 4, -1, 0,
        Resources.multiply(defaultResourceRequested, 4), defaultCapacity));
    selector.computeTask.run();
    List<NodeId> nodeIds = selector.selectNodes();
    Assert.assertEquals("h1:1", nodeIds.get(0).toString());
    Assert.assertEquals("h2:2", nodeIds.get(1).toString());
    Assert.assertEquals("h3:3", nodeIds.get(2).toString());
    Assert.assertEquals("h4:4", nodeIds.get(3).toString());

    // Now update node1 to have only defaultResourceRequested available
    // by changing its capability to 2x defaultResourceReqeusted
    // node1 should now rank last
    selector.updateNode(createRMNode(
        "h1", 1, -1, 0,
        Resources.multiply(defaultResourceRequested, 1),
        Resources.multiply(defaultResourceRequested, 2)));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h3:3", nodeIds.get(1).toString());
    Assert.assertEquals("h4:4", nodeIds.get(2).toString());
    Assert.assertEquals("h1:1", nodeIds.get(3).toString());

    // Now update node2 to have no resources available
    // by changing its capability to 1x defaultResourceReqeusted
    // node2 should now rank last
    selector.updateNode(createRMNode(
        "h2", 2, -1, 0,
        Resources.multiply(defaultResourceRequested, 1),
        Resources.multiply(defaultResourceRequested, 1)));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    Assert.assertEquals("h3:3", nodeIds.get(0).toString());
    Assert.assertEquals("h4:4", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());
    Assert.assertEquals("h2:2", nodeIds.get(3).toString());
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
    RMNode node = selector.selectLocalNode(
        "h1", blacklist, defaultResourceRequested);
    Assert.assertEquals("h1", node.getHostName());

    // if node has been added to blacklist
    blacklist.add("h1");
    node = selector.selectLocalNode(
        "h1", blacklist, defaultResourceRequested);
    Assert.assertNull(node);

    node = selector.selectLocalNode(
        "h2", blacklist, defaultResourceRequested);
    Assert.assertNull(node);

    node = selector.selectLocalNode(
        "h3", blacklist, defaultResourceRequested);
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
    RMNode node = selector.selectRackLocalNode(
        "rack1", blacklist, defaultResourceRequested);
    Assert.assertEquals("h1", node.getHostName());

    // if node has been added to blacklist
    blacklist.add("h1");
    node = selector.selectRackLocalNode(
        "rack1", blacklist, defaultResourceRequested);
    Assert.assertNull(node);

    node = selector.selectRackLocalNode(
        "rack2", blacklist, defaultResourceRequested);
    Assert.assertEquals("h3", node.getHostName());

    blacklist.add("h3");
    node = selector.selectRackLocalNode(
        "rack2", blacklist, defaultResourceRequested);
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
    RMNode node = selector.selectAnyNode(blacklist, defaultResourceRequested);
    Assert.assertTrue(node.getHostName().equals("h1") ||
        node.getHostName().equals("h3"));

    // if node has been added to blacklist
    blacklist.add("h1");
    node = selector.selectAnyNode(blacklist, defaultResourceRequested);
    Assert.assertEquals("h3", node.getHostName());

    blacklist.add("h3");
    node = selector.selectAnyNode(blacklist, defaultResourceRequested);
    Assert.assertNull(node);
  }

  @Test
  public void testQueueLengthThenResourcesComparator() {
    NodeQueueLoadMonitor.LoadComparator comparator =
        NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH_THEN_RESOURCES;

    NodeId n1 = new FakeNodeId("n1", 5000);
    NodeId n2 = new FakeNodeId("n2", 5000);

    // Case 1: larger available cores should be ranked first
    {
      ClusterNode.Properties cn1Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(5, 5))
              .setCapability(newResourceInstance(10, 10, 1000))
              .setQueueLength(10);
      ClusterNode cn1 = new ClusterNode(n1);
      cn1.setProperties(cn1Props);

      ClusterNode.Properties cn2Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(6, 6))
              .setCapability(newResourceInstance(10, 10, 1000))
              .setQueueLength(10);
      ClusterNode cn2 = new ClusterNode(n2);
      cn2.setProperties(cn2Props);

      comparator.setClusterResource(
          Resources.add(cn1.getCapability(), cn2.getCapability()));
      Assert.assertTrue(comparator.compare(cn1, cn2) < 0);
    }

    // Case 2: Shorter queue should be ranked first before comparing resources
    {
      ClusterNode.Properties cn1Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(5, 5))
              .setCapability(newResourceInstance(10, 10, 1000))
              .setQueueLength(5);
      ClusterNode cn1 = new ClusterNode(n1);
      cn1.setProperties(cn1Props);

      ClusterNode.Properties cn2Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(3, 3))
              .setCapability(newResourceInstance(10, 10, 1000))
              .setQueueLength(10);
      ClusterNode cn2 = new ClusterNode(n2);
      cn2.setProperties(cn2Props);

      comparator.setClusterResource(
          Resources.add(cn1.getCapability(), cn2.getCapability()));
      Assert.assertTrue(comparator.compare(cn1, cn2) < 0);
    }

    // Case 3: No capability vs with capability,
    // with capability should come first
    {
      ClusterNode.Properties cn1Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(Resources.none())
              .setCapability(newResourceInstance(1, 1, 1000))
              .setQueueLength(5);
      ClusterNode cn1 = new ClusterNode(n1);
      cn1.setProperties(cn1Props);

      ClusterNode.Properties cn2Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(Resources.none())
              .setCapability(Resources.none())
              .setQueueLength(5);
      ClusterNode cn2 = new ClusterNode(n2);
      cn2.setProperties(cn2Props);

      comparator.setClusterResource(
          Resources.add(cn1.getCapability(), cn2.getCapability()));
      Assert.assertTrue(comparator.compare(cn1, cn2) < 0);
    }

    // Case 4: Compare same values
    {
      ClusterNode.Properties cn1Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(5, 5))
              .setCapability(newResourceInstance(10, 10, 1000))
              .setQueueLength(10);
      ClusterNode cn1 = new ClusterNode(n1);
      cn1.setProperties(cn1Props);

      ClusterNode.Properties cn2Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(5, 5))
              .setCapability(newResourceInstance(10, 10, 1000))
              .setQueueLength(10);
      ClusterNode cn2 = new ClusterNode(n2);
      cn2.setProperties(cn2Props);

      comparator.setClusterResource(
          Resources.add(cn1.getCapability(), cn2.getCapability()));
      Assert.assertEquals(0, comparator.compare(cn1, cn2));
    }

    // Case 5: If ratio is the same, compare raw values
    // by VCores first, then memory
    {
      ClusterNode.Properties cn1Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(6, 5))
              .setCapability(newResourceInstance(10, 10, 1000))
              .setQueueLength(10);
      ClusterNode cn1 = new ClusterNode(n1);
      cn1.setProperties(cn1Props);

      ClusterNode.Properties cn2Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(5, 6))
              .setCapability(newResourceInstance(10, 10, 1000))
              .setQueueLength(10);
      ClusterNode cn2 = new ClusterNode(n2);
      cn2.setProperties(cn2Props);

      comparator.setClusterResource(
          Resources.add(cn1.getCapability(), cn2.getCapability()));
      // Both are 60% allocated, but CN1 has 5 avail VCores, CN2 only has 4
      Assert.assertTrue(comparator.compare(cn1, cn2) < 0);
    }

    // Case 6: by VCores absolute value
    {
      ClusterNode.Properties cn1Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(5, 5))
              .setCapability(newResourceInstance(10, 10, 1000))
              .setQueueLength(10);
      ClusterNode cn1 = new ClusterNode(n1);
      cn1.setProperties(cn1Props);

      ClusterNode.Properties cn2Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(5, 6))
              .setCapability(newResourceInstance(10, 12, 1000))
              .setQueueLength(10);
      ClusterNode cn2 = new ClusterNode(n2);
      cn2.setProperties(cn2Props);

      comparator.setClusterResource(
          Resources.add(cn1.getCapability(), cn2.getCapability()));
      Assert.assertTrue(comparator.compare(cn2, cn1) < 0);
    }

    // Case 7: by memory absolute value
    {
      ClusterNode.Properties cn1Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(5, 5))
              .setCapability(newResourceInstance(10, 10, 1000))
              .setQueueLength(10);
      ClusterNode cn1 = new ClusterNode(n1);
      cn1.setProperties(cn1Props);

      ClusterNode.Properties cn2Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(6, 5))
              .setCapability(newResourceInstance(12, 10, 1000))
              .setQueueLength(10);
      ClusterNode cn2 = new ClusterNode(n2);
      cn2.setProperties(cn2Props);

      comparator.setClusterResource(
          Resources.add(cn1.getCapability(), cn2.getCapability()));
      Assert.assertTrue(comparator.compare(cn2, cn1) < 0);
    }

    // Case 8: Memory should be more constraining in the overall cluster,
    // so rank the node with less allocated memory first
    {
      ClusterNode.Properties cn1Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(5, 11))
              .setCapability(newResourceInstance(10, 100, 1000))
              .setQueueLength(10);
      ClusterNode cn1 = new ClusterNode(n1);
      cn1.setProperties(cn1Props);

      ClusterNode.Properties cn2Props =
          ClusterNode.Properties.newInstance()
              .setAllocatedResource(newResourceInstance(6, 10))
              .setCapability(newResourceInstance(10, 100, 1000))
              .setQueueLength(10);
      ClusterNode cn2 = new ClusterNode(n2);
      cn2.setProperties(cn2Props);

      comparator.setClusterResource(
          Resources.add(cn1.getCapability(), cn2.getCapability()));
      Assert.assertTrue(comparator.compare(cn1, cn2) < 0);
    }
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
    return createRMNode(host, port, rack, waitTime, queueLength, queueCapacity,
        state, Resources.none(), defaultCapacity);
  }

  private RMNode createRMNode(
      String host, int port, int waitTime, int queueLength,
      Resource allocatedResource, Resource nodeResource) {
    return createRMNode(host, port, waitTime, queueLength,
        DEFAULT_MAX_QUEUE_LENGTH, allocatedResource, nodeResource);
  }

  private RMNode createRMNode(
      String host, int port, int waitTime, int queueLength, int queueCapacity,
      Resource allocatedResource, Resource nodeResource) {
    return createRMNode(host, port, "default", waitTime, queueLength,
        queueCapacity, NodeState.RUNNING, allocatedResource, nodeResource);
  }

  @SuppressWarnings("parameternumber")
  private RMNode createRMNode(String host, int port, String rack,
      int waitTime, int queueLength, int queueCapacity, NodeState state,
      Resource allocatedResource, Resource nodeResource) {
    RMNode node1 = Mockito.mock(RMNode.class);
    NodeId nID1 = new FakeNodeId(host, port);
    Mockito.when(node1.getHostName()).thenReturn(host);
    Mockito.when(node1.getRackName()).thenReturn(rack);
    Mockito.when(node1.getNode()).thenReturn(new NodeBase("/" + host));
    Mockito.when(node1.getNodeID()).thenReturn(nID1);
    Mockito.when(node1.getState()).thenReturn(state);
    Mockito.when(node1.getTotalCapability()).thenReturn(nodeResource);
    Mockito.when(node1.getNodeUtilization()).thenReturn(
        ResourceUtilization.newInstance(0, 0, 0));
    Mockito.when(node1.getAllocatedContainerResource()).thenReturn(
        allocatedResource);
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
