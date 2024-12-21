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
import org.apache.hadoop.metrics2.MetricsJsonBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.mockito.Mockito.when;

public class TestMultiComparatorPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMultiComparatorPolicy.class);
  public static final int GB = 1024;

  @Test
  public void testSetConf() {
    MultiComparatorPolicy policy = new MultiComparatorPolicy();
    /*
     * use default comparators for null, empty, or invalid conf
     */
    // null conf
    policy.setConf(null);
    Assert.assertSame("use default comparators for null conf",
        policy.getComparators(), MultiComparatorPolicy.DEFAULT_COMPARATORS);
    // empty conf
    Configuration conf = new Configuration();
    policy.setConf(conf);
    Assert.assertSame("use default comparators for empty conf",
        policy.getComparators(), MultiComparatorPolicy.DEFAULT_COMPARATORS);
    // conf with current-name of policy but no configured comparators
    String policyName = "policy1";
    conf.set(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_CURRENT_NAME,
        policyName);
    policy.setConf(conf);
    Assert.assertSame("use default comparators for empty conf",
        policy.getComparators(), MultiComparatorPolicy.DEFAULT_COMPARATORS);
    // conf with current-name of policy and empty comparators conf
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        ",,,");
    policy.setConf(conf);
    Assert.assertSame("use default comparators for empty conf",
        policy.getComparators(), MultiComparatorPolicy.DEFAULT_COMPARATORS);
    // conf with current-name of policy and comparators conf with invalid comparator-key
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        "INVALID");
    policy.setConf(conf);
    Assert.assertSame("use default comparators for empty conf",
        policy.getComparators(), MultiComparatorPolicy.DEFAULT_COMPARATORS);
    // conf with current-name of policy and comparators conf with invalid order-direction
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        "NODE_ID:INVALID");
    policy.setConf(conf);
    Assert.assertSame("use default comparators for empty conf",
        policy.getComparators(), MultiComparatorPolicy.DEFAULT_COMPARATORS);
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
        // get nodes from iterator
        sortedNodes.clear();
        Iterator<SchedulerNode>
            it = policy.getPreferredNodeIterator(null, partitionName);
        while (it.hasNext()) {
          sortedNodes.add(it.next());
        }
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

  @Test
  public void testGetNodeIteratorInMultiThreads()
      throws ExecutionException, InterruptedException {
    MultiComparatorPolicy<SchedulerNode> policy =
        new MultiComparatorPolicy<>();
    String policyName = "policy1", partitionName = "partition1";
    Configuration conf = new Configuration();
    conf.set(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_CURRENT_NAME,
        policyName);
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        "DOMINANT_ALLOCATED_RATIO:ASC,NODE_ID:ASC");
    conf.setFloat(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.PREFER_RATIO_CONF_KEY,
        0.2f);
    conf.setFloat(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.IGNORE_RATIO_CONF_KEY,
        0.25f);
    policy.setConf(conf);

    // mock nodes
    // node1 ~ node1999: total=<5GB, 5>, used=<GB, 1>,  dominant ratio: 0.2
    // node2000 ~ node3999: total=<5GB, 5>, used=<2GB, 1>,  dominant ratio: 0.4
    // node4000 ~ node5999: total=<20GB, 20>, used=<GB, 3>,  dominant ratio: 0.15
    List<SchedulerNode> nodes = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      SchedulerNode node = createMockNode("node" + i,
          Resource.newInstance(GB, 1),
          Resource.newInstance(5 * GB, 5));
      nodes.add(node);
    }
    for (int i = 2000; i < 4000; i++) {
      SchedulerNode node = createMockNode("node" + i,
          Resource.newInstance(2*GB, 1),
          Resource.newInstance(5 * GB, 5));
      nodes.add(node);
    }
    for (int i = 4000; i < 6000; i++) {
      SchedulerNode node = createMockNode("node" + i,
          Resource.newInstance(GB, 3),
          Resource.newInstance(20 * GB, 20));
      nodes.add(node);
    }
    /*
     * add and refresh nodes
     */
    policy.addAndRefreshNodesSet(nodes, partitionName);
    /*
     * call getPreferredNodeIterator in multi-threads
     */
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    checkConcurrentGet(executorService, policy, partitionName,
        2000, 4000, 5999);
    // print metrics
    Map<String, MutableMetric> metrics = new LinkedHashMap<>();
    metrics.put("refreshDelay", PolicyMetrics.getMetrics().getRefreshDelay());
    metrics.put("getDelay", PolicyMetrics.getMetrics().getGetDelay());
    printMetrics(metrics);

    /*
     * add preferred nodes and then refresh nodes
     * node6000 ~ node7999: total=<10GB, 10>, used=<GB, 1>,  dominant ratio: 0.1
     */
    for (int i = 6000; i < 8000; i++) {
      SchedulerNode node = createMockNode("node" + i,
          Resource.newInstance(GB, 1),
          Resource.newInstance(10 * GB, 10));
      nodes.add(node);
    }
    policy.addAndRefreshNodesSet(nodes, partitionName);
    // check thread local caches are updated
    checkConcurrentGet(executorService, policy, partitionName,
        2000, 6000, 7999);
    printMetrics(metrics);
    executorService.shutdown();

    /*
     * check single iterator: should be reinitialized after it has not next element.
     * for each round, ranges should be: [6000, 7999], [4000, 5999], [0, 1999]
     * ignored range: [2000, 3999]
     */
    executorService = Executors.newFixedThreadPool(1);
    for (int i = 0; i < 3; i++) {
      checkConcurrentGet(executorService, policy, partitionName,
          2000, 6000, 7999);
      checkConcurrentGet(executorService, policy, partitionName,
          2000, 4000, 5999);
      checkConcurrentGet(executorService, policy, partitionName,
          2000, 0, 1999);
    }
    executorService.shutdown();
  }

  @Test
  public void testGetNodeIteratorWithMultiPartitionsInMultiThreads()
      throws ExecutionException, InterruptedException {
    PolicyMetrics.reset();
    MultiComparatorPolicy<SchedulerNode> policy =
        Mockito.spy(new MultiComparatorPolicy<>());
    String policyName = "policy1", partitionName1 = "partition1",
        partitionName2 = "partition2";
    Configuration conf = new Configuration();
    conf.set(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_CURRENT_NAME,
        policyName);
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        "DOMINANT_ALLOCATED_RATIO:ASC,NODE_ID:ASC");
    conf.setFloat(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.PREFER_RATIO_CONF_KEY,
        0.2f);
    conf.setFloat(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + policyName + DOT + MultiComparatorPolicy.IGNORE_RATIO_CONF_KEY,
        0.25f);
    policy.setConf(conf);

    // mock nodes for partition1
    // node1 ~ node1999: total=<5GB, 5>, used=<GB, 1>,  dominant ratio: 0.2
    // node2000 ~ node3999: total=<5GB, 5>, used=<2GB, 1>,  dominant ratio: 0.4
    List<SchedulerNode> nodesForP1 = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      SchedulerNode node = createMockNode("node" + i,
          Resource.newInstance(GB, 1),
          Resource.newInstance(5 * GB, 5));
      nodesForP1.add(node);
    }
    for (int i = 2000; i < 4000; i++) {
      SchedulerNode node = createMockNode("node" + i,
          Resource.newInstance(2*GB, 1),
          Resource.newInstance(5 * GB, 5));
      nodesForP1.add(node);
    }
    // mock nodes for partition2
    // node4000 ~ node4099: total=<10GB, 10>, used=<5GB, 1>,  dominant ratio: 0.5
    // node4100 ~ node4199: total=<10GB, 10>, used=<GB, 3>,  dominant ratio: 0.3
    List<SchedulerNode> nodesForP2 = new ArrayList<>();
    for (int i = 4000; i < 4100; i++) {
      SchedulerNode node = createMockNode("node" + i,
          Resource.newInstance(5 * GB, 1),
          Resource.newInstance(10 * GB, 10));
      nodesForP2.add(node);
    }
    for (int i = 4100; i < 4200; i++) {
      SchedulerNode node = createMockNode("node" + i,
          Resource.newInstance(GB, 3), Resource.newInstance(10 * GB, 10));
      nodesForP2.add(node);
    }

    /*
     * add and refresh nodes
     */
    policy.addAndRefreshNodesSet(nodesForP1, partitionName1);
    policy.addAndRefreshNodesSet(nodesForP2, partitionName2);

    // partition test cases
    List<PartitionTestCase> cases =
        Arrays.asList(new PartitionTestCase(partitionName1, 2000, 0, 1999),
            new PartitionTestCase(partitionName2, 100, 4100, 4199));

    /*
     * call getPreferredNodeIterator in multi-threads
     */
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    checkConcurrentGetForPartitions(executorService, policy, cases);
    // print metrics
    Map<String, MutableMetric> metrics = new LinkedHashMap<>();
    metrics.put("iteratorRefreshed",
        PolicyMetrics.getMetrics().iteratorCacheRefreshed);
    metrics.put("refreshDelay", PolicyMetrics.getMetrics().refreshDelay);
    metrics.put("getDelay", PolicyMetrics.getMetrics().getDelay);
    printMetrics(metrics);
    // check iterator refreshed num must be in range [2, 20]
    long refreshedNum = PolicyMetrics.getMetrics().iteratorCacheRefreshed.value();
    Assert.assertTrue(refreshedNum >= 2 && refreshedNum <= 20);

    executorService.shutdown();
  }

  private void checkConcurrentGet(ExecutorService executorService,
      MultiComparatorPolicy<SchedulerNode> policy, String partitionName,
      int getNum, int expectedMinNodeID, int expectedMaxNodeID)
      throws ExecutionException, InterruptedException {
    List<Future<String>> futures = new ArrayList<>();
    for (int i = 0; i < getNum; i++) {
      futures.add(executorService.submit(() -> {
        Iterator<SchedulerNode> it =
            policy.getPreferredNodeIterator(null, partitionName);
        // return flag: <thread_name>_<node_host>
        return Thread.currentThread().getName() + "_" + it.next().getNodeID()
            .getHost();
      }));
    }
    List<String> flags = new ArrayList<>();
    Set<String> flagSet = new HashSet<>();
    int maxNodeID = Integer.MIN_VALUE, minNodeID = Integer.MAX_VALUE;
    for (Future<String> future : futures) {
      String flag = future.get();
      flags.add(flag);
      flagSet.add(flag);
      int nodeID = Integer.parseInt(flag.split("node")[1]);
      if (nodeID > maxNodeID) {
        maxNodeID = nodeID;
      }
      if (nodeID < minNodeID) {
        minNodeID = nodeID;
      }
    }
    LOG.info("Check flags: totalNum=" + flags.size() + ", deduplicatedNum="
        + flagSet.size() + ", minNodeID=" + minNodeID + "," + " maxNodeID="
        + maxNodeID);
    // check chosen nodeID are in range [expectedMinNodeID, expectedMaxNodeID]
    Assert.assertTrue(
        minNodeID >= expectedMinNodeID && maxNodeID <= expectedMaxNodeID);
    // check there are no duplicated flags(no duplicated node in the same thread)
    Assert.assertEquals(flags.size(), flagSet.size());
  }

  private static class PartitionTestCase {
    private String partitionName;
    private int submitNum;
    private int expectedMinNodeID;
    private int expectedMaxNodeID;
    PartitionTestCase(String partitionName, int submitNum, int expectedMinNodeID,
        int expectedMaxNodeID) {
      this.partitionName = partitionName;
      this.submitNum = submitNum;
      this.expectedMinNodeID = expectedMinNodeID;
      this.expectedMaxNodeID = expectedMaxNodeID;
    }
    public int getSubmitNum() {
      return submitNum;
    }
  }

  private void checkConcurrentGetForPartitions(ExecutorService executorService,
      MultiComparatorPolicy<SchedulerNode> policy,
      List<PartitionTestCase> partitionTestCases)
      throws ExecutionException, InterruptedException {
    int maxSubmitNum = partitionTestCases.stream()
        .mapToInt(PartitionTestCase::getSubmitNum).max().getAsInt();
    List<Future<String>> futures = new ArrayList<>();
    for (int i = 0; i < maxSubmitNum; i++) {
      for (PartitionTestCase partitionTestCase: partitionTestCases) {
        String partitionName = partitionTestCase.partitionName;
        if (i < partitionTestCase.getSubmitNum()) {
          futures.add(executorService.submit(() -> {
            Iterator<SchedulerNode> it =
                policy.getPreferredNodeIterator(null, partitionName);
            // return flag: <thread_name>_<partition>_<node_host>
            return Thread.currentThread().getName() + "_" + partitionName +
                "_" + it.next().getNodeID().getHost();
          }));
        }
      }
    }
    Map<String, Set<String>> nodeFlags = new HashMap<>();
    for (Future<String> future : futures) {
      String flag = future.get();
      String partitionName = flag.split("_")[1];
      Set<String> nodeFlagsForPartition =
          nodeFlags.computeIfAbsent(partitionName, k -> new HashSet<>());
      nodeFlagsForPartition.add(flag);
    }
    for (PartitionTestCase partitionTestCase : partitionTestCases) {
      Set<String> nodeIDsForPartition =
          nodeFlags.get(partitionTestCase.partitionName);
      List<String> flags = new ArrayList<>();
      Set<String> flagSet = new HashSet<>();
      int maxNodeID = Integer.MIN_VALUE, minNodeID = Integer.MAX_VALUE;
      for (String nodeFlag : nodeIDsForPartition) {
        flags.add(nodeFlag);
        flagSet.add(nodeFlag);
        int nodeID = Integer.parseInt(nodeFlag.split("node")[1]);
        if (nodeID > maxNodeID) {
          maxNodeID = nodeID;
        }
        if (nodeID < minNodeID) {
          minNodeID = nodeID;
        }
      }
      LOG.info("Check flags: partition=" + partitionTestCase.partitionName
          + ", totalNum=" + flags.size() + ", deduplicatedNum="
          + flagSet.size() + ", minNodeID="
          + minNodeID + "," + " maxNodeID=" + maxNodeID);
      // check chosen nodeID are in range [expectedMinNodeID, expectedMaxNodeID]
      Assert.assertTrue(minNodeID >= partitionTestCase.expectedMinNodeID
          && maxNodeID <= partitionTestCase.expectedMaxNodeID);
      // check there are no duplicated flags(no duplicated node in the same thread)
      Assert.assertEquals(flags.size(), flagSet.size());
    }
  }

  private void printMetrics(Map<String, MutableMetric> metrics) {
    for (Map.Entry<String, MutableMetric> entry : metrics.entrySet()) {
      MetricsRecordBuilder builder = new MetricsJsonBuilder(null);
      entry.getValue().snapshot(builder, true);
      LOG.info("Print " + entry.getKey() + " metric: " + builder);
    }
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
    private String comparatorsConf;
    private List<List<SchedulerNode>> nodes;
    private List<SchedulerNode> expectedNodes;

    TestCase(String comparatorsConf, List<List<SchedulerNode>> nodes,
        List<SchedulerNode> expectedNodes) {
      this.comparatorsConf = comparatorsConf;
      this.nodes = nodes;
      this.expectedNodes = expectedNodes;
    }
  }
}
