/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.policy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.mockito.Mockito.when;

public class TestMultiComparatorPolicy {
  private final int GB = 1024;

  @Test
  public void testSetConf() {
    MultiComparatorPolicy policy = new MultiComparatorPolicy();
    /*
     * use default comparators for null, empty, or invalid conf
     */
    // null conf
    policy.setConf(null);
    Assert.assertSame("use default comparators for null conf",
        policy.comparators, MultiComparatorPolicy.DEFAULT_COMPARATORS);
    // empty conf
    Configuration conf = new Configuration();
    policy.setConf(conf);
    Assert.assertSame("use default comparators for empty conf",
        policy.comparators, MultiComparatorPolicy.DEFAULT_COMPARATORS);
    // conf with current-name of policy but no configured comparators
    String policyName = "policy1";
    conf.set(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_CURRENT_NAME,
        policyName);
    policy.setConf(conf);
    Assert.assertSame("use default comparators for empty conf",
        policy.comparators, MultiComparatorPolicy.DEFAULT_COMPARATORS);
    // conf with current-name of policy and empty comparators conf
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        ",,,");
    policy.setConf(conf);
    Assert.assertSame("use default comparators for empty conf",
        policy.comparators, MultiComparatorPolicy.DEFAULT_COMPARATORS);
    // conf with current-name of policy and comparators conf with invalid comparator-key
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        "INVALID");
    policy.setConf(conf);
    Assert.assertSame("use default comparators for empty conf",
        policy.comparators, MultiComparatorPolicy.DEFAULT_COMPARATORS);
    // conf with current-name of policy and comparators conf with invalid order-direction
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        "NODE_ID:INVALID");
    policy.setConf(conf);
    Assert.assertSame("use default comparators for empty conf",
        policy.comparators, MultiComparatorPolicy.DEFAULT_COMPARATORS);
    /*
     * use configured comparators for valid comparators conf
     */
    // conf with current-name of policy and 1 valid comparator
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        "NODE_ID:ASC");
    policy.setConf(conf);
    Assert.assertEquals("configured 1 comparator", policy.getComparatorKeys(),
        Collections.singletonList(ComparatorKey.NODE_ID));
    Assert.assertEquals("configured 1 comparator", policy.getOrderDirections(),
        Collections.singletonList(OrderDirection.ASC));
    // conf with current-name of policy and 2 valid comparators
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        "NODE_ID:ASC,ALLOCATED_RESOURCE:DESC");
    policy.setConf(conf);
    Assert.assertEquals("configured 2 comparators", policy.getComparatorKeys(),
        Arrays.asList(ComparatorKey.NODE_ID, ComparatorKey.ALLOCATED_RESOURCE));
    Assert.assertEquals("configured 2 comparators", policy.getOrderDirections(),
        Arrays.asList(OrderDirection.ASC, OrderDirection.DESC));
  }

  @Test
  public void testNodeSortingWithDifferentComparators() {
    // init policy & conf
    MultiComparatorPolicy<SchedulerNode> policy =
        new MultiComparatorPolicy<>();
    String policyName = "policy1", partitionName = "partition1";
    Configuration conf = new Configuration();
    conf.set(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_CURRENT_NAME,
        policyName);

    // Create nodes: node1 ~ node6
    // dominant allocated ratios:
    //    node1: 60%, node2: 50%, node3: 40%, node4: 40%, node5: 50%, node6: 30%
    SchedulerNode node1 = createMockNode("node1", Resource.newInstance(GB, 6),
        Resource.newInstance(10 * GB, 10));
    SchedulerNode node2 =
        createMockNode("node2", Resource.newInstance(2 * GB, 5),
            Resource.newInstance(10 * GB, 10));
    SchedulerNode node3 =
        createMockNode("node3", Resource.newInstance(3 * GB, 4),
            Resource.newInstance(10 * GB, 10));
    SchedulerNode node4 =
        createMockNode("node4", Resource.newInstance(4 * GB, 3),
            Resource.newInstance(10 * GB, 10));
    SchedulerNode node5 =
        createMockNode("node5", Resource.newInstance(5 * GB, 2),
            Resource.newInstance(10 * GB, 10));
    SchedulerNode node6 =
        createMockNode("node6", Resource.newInstance(6 * GB, 1),
            Resource.newInstance(20 * GB, 20));
    List<List<SchedulerNode>> nodesCases =
        Arrays.asList(Arrays.asList(node1, node2, node3, node4, node5, node6),
            Arrays.asList(node6, node5, node4, node3, node2, node1),
            Arrays.asList(node5, node1, node6, node3, node4, node2));
    /*
     * expected sorted nodes in ascending order
     */
    List<SchedulerNode> expectedNodesByID =
        Arrays.asList(node1, node2, node3, node4, node5, node6);
    List<SchedulerNode> expectedNodesByAllocatedMemory =
        Arrays.asList(node1, node2, node3, node4, node5, node6);
    List<SchedulerNode> expectedNodesByAllocatedVCores =
        Arrays.asList(node6, node5, node4, node3, node2, node1);
    List<SchedulerNode> expectedNodesByUnallocatedMemory =
        Arrays.asList(node5, node4, node3, node2, node1, node6);
    List<SchedulerNode> expectedNodesByUnallocatedVCores =
        Arrays.asList(node1, node2, node3, node4, node5, node6);
    // expected nodes depend on the second comparator - NODE_ID:ASC
    List<SchedulerNode> expectedNodesByTotalResource =
        Arrays.asList(node1, node2, node3, node4, node5, node6);
    List<SchedulerNode> expectedNodesByDominantResourceRatio =
        Arrays.asList(node6, node3, node4, node2, node5, node1);

    // test cases
    TestCase[] testCases = new TestCase[] {
        // NODE_ID
        new TestCase("NODE_ID", nodesCases, expectedNodesByID),
        new TestCase("NODE_ID:ASC", nodesCases, expectedNodesByID),
        new TestCase("NODE_ID:DESC", nodesCases, reverse(expectedNodesByID)),
        // ALLOCATED_MEMORY
        new TestCase("ALLOCATED_MEMORY:ASC", nodesCases,
            expectedNodesByAllocatedMemory),
        new TestCase("ALLOCATED_MEMORY:DESC", nodesCases,
            reverse(expectedNodesByAllocatedMemory)),
        // ALLOCATED_VCORES
        new TestCase("ALLOCATED_VCORES:ASC", nodesCases,
            expectedNodesByAllocatedVCores),
        new TestCase("ALLOCATED_VCORES:DESC", nodesCases,
            reverse(expectedNodesByAllocatedVCores)),
        // ALLOCATED_RESOURCE
        new TestCase("ALLOCATED_RESOURCE:ASC", nodesCases,
            expectedNodesByAllocatedMemory),
        new TestCase("ALLOCATED_RESOURCE:DESC", nodesCases,
            reverse(expectedNodesByAllocatedMemory)),
        // UNALLOCATED_MEMORY
        new TestCase("UNALLOCATED_MEMORY:ASC", nodesCases,
            expectedNodesByUnallocatedMemory),
        new TestCase("UNALLOCATED_MEMORY:DESC", nodesCases,
            reverse(expectedNodesByUnallocatedMemory)),
        // UNALLOCATED_VCORES
        new TestCase("UNALLOCATED_VCORES:ASC", nodesCases,
            expectedNodesByUnallocatedVCores),
        new TestCase("UNALLOCATED_VCORES:DESC", nodesCases,
            reverse(expectedNodesByUnallocatedVCores)),
        // UNALLOCATED_RESOURCE
        new TestCase("UNALLOCATED_RESOURCE:ASC", nodesCases,
            expectedNodesByUnallocatedMemory),
        new TestCase("UNALLOCATED_RESOURCE:DESC", nodesCases,
            reverse(expectedNodesByUnallocatedMemory)),
        // TOTAL_MEMORY
        new TestCase("TOTAL_MEMORY:ASC,NODE_ID:ASC", nodesCases,
            expectedNodesByTotalResource),
        new TestCase("TOTAL_MEMORY:DESC,NODE_ID:DESC", nodesCases,
            reverse(expectedNodesByTotalResource)),
        // TOTAL_VCORES
        new TestCase("TOTAL_VCORES:ASC,NODE_ID:ASC", nodesCases,
            expectedNodesByTotalResource),
        new TestCase("TOTAL_VCORES:DESC,NODE_ID:DESC", nodesCases,
            reverse(expectedNodesByTotalResource)),
        // TOTAL_RESOURCE
        new TestCase("TOTAL_RESOURCE:ASC,NODE_ID:ASC", nodesCases,
            expectedNodesByTotalResource),
        new TestCase("TOTAL_RESOURCE:DESC,NODE_ID:DESC", nodesCases,
            reverse(expectedNodesByTotalResource)),
        // DOMINANT_ALLOCATED_RATIO + NODE_ID
        new TestCase("DOMINANT_ALLOCATED_RATIO:ASC,NODE_ID:ASC", nodesCases,
            expectedNodesByDominantResourceRatio),
        new TestCase("DOMINANT_ALLOCATED_RATIO:DESC,NODE_ID:DESC", nodesCases,
            reverse(expectedNodesByDominantResourceRatio)) };

    for (TestCase testCase : testCases) {
      conf.set(
          CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
              + policyName + DOT
              + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
          testCase.comparatorsConf);
      policy.setConf(conf);

      for (List<SchedulerNode> nodes : testCase.nodes) {
        policy.addAndRefreshNodesSet(nodes, partitionName);
        List<SchedulerNode> sortedNodes =
            new ArrayList<>(policy.getNodesPerPartition(partitionName));
        assertNodes("Case: comparatorsConf=" + testCase.comparatorsConf,
            testCase.expectedNodes, sortedNodes);
      }
    }
  }

  @Test
  public void testNodeSortingWithMultiplePartitions() {
    // init policy & conf
    MultiComparatorPolicy<SchedulerNode> policy =
        new MultiComparatorPolicy<>();
    String policyName = "policy1", partition1Name = "partition1",
        partition2Name = "partition2";
    Configuration conf = new Configuration();
    conf.set(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_CURRENT_NAME,
        policyName);
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        "NODE_ID:ASC");
    policy.setConf(conf);

    // Create nodes: node1 ~ node5
    SchedulerNode node1 = createMockNode("node1", Resource.newInstance(GB, 5),
        Resource.newInstance(5 * GB, 5));
    SchedulerNode node2 =
        createMockNode("node2", Resource.newInstance(2 * GB, 4),
            Resource.newInstance(5 * GB, 5));
    SchedulerNode node3 =
        createMockNode("node3", Resource.newInstance(3 * GB, 3),
            Resource.newInstance(5 * GB, 5));
    SchedulerNode node4 =
        createMockNode("node4", Resource.newInstance(4 * GB, 2),
            Resource.newInstance(5 * GB, 5));
    // add and refresh nodes for partitions
    // partition1: node1, node2
    // partition2: node3, node4
    policy.addAndRefreshNodesSet(Arrays.asList(node1, node2), partition1Name);
    policy.addAndRefreshNodesSet(Arrays.asList(node4, node3), partition2Name);

    // verify sorted nodes for partition1
    List<SchedulerNode> partition1SortedNodes =
        new ArrayList<>(policy.getNodesPerPartition(partition1Name));
    assertNodes("Case: partition=" + partition1Name,
        Arrays.asList(node1, node2), partition1SortedNodes);

    // verify sorted nodes for partition2
    List<SchedulerNode> partition2SortedNodes =
        new ArrayList<>(policy.getNodesPerPartition(partition2Name));
    assertNodes("Case: partition=" + partition2Name,
        Arrays.asList(node3, node4), partition2SortedNodes);
  }

  private SchedulerNode createMockNode(String nodeId,
      Resource allocatedResource, Resource totalResource) {
    SchedulerNode node = Mockito.mock(SchedulerNode.class);
    when(node.getNodeID()).thenReturn(NodeId.newInstance(nodeId, 0));
    when(node.getAllocatedResource()).thenReturn(allocatedResource);
    when(node.getTotalResource()).thenReturn(totalResource);
    when(node.getUnallocatedResource()).thenReturn(
        Resources.subtract(totalResource, allocatedResource));
    return node;
  }

  private void assertNodes(String message,
      List<SchedulerNode> expectedSortedNodes,
      List<SchedulerNode> actualNodes) {
    Assert.assertEquals(message, expectedSortedNodes.size(),
        actualNodes.size());
    List<NodeId> nodeIds = actualNodes.stream().map(SchedulerNode::getNodeID)
        .collect(Collectors.toList());
    List<NodeId> expectedIds =
        expectedSortedNodes.stream().map(SchedulerNode::getNodeID)
            .collect(Collectors.toList());
    Assert.assertEquals(message, expectedIds, nodeIds);
  }

  private List<SchedulerNode> reverse(List<SchedulerNode> nodes) {
    List<SchedulerNode> reversedNodes = new ArrayList<>(nodes);
    Collections.reverse(reversedNodes);
    return reversedNodes;
  }

  private static class TestCase {
    String comparatorsConf;
    List<List<SchedulerNode>> nodes;
    List<SchedulerNode> expectedNodes;

    TestCase(String comparatorsConf, List<List<SchedulerNode>> nodes,
        List<SchedulerNode> expectedNodes) {
      this.comparatorsConf = comparatorsConf;
      this.nodes = nodes;
      this.expectedNodes = expectedNodes;
    }
  }
}
