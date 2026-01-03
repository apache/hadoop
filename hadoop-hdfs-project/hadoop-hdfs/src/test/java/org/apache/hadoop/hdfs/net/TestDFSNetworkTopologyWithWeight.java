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
package org.apache.hadoop.hdfs.net;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Similar to {@link TestDFSNetworkTopology}, but with weight mapping.
 */
@Timeout(30)
public class TestDFSNetworkTopologyWithWeight {

  private static final int CHOOSE_TIMES = 1000000;
  private static final double PROBABILITY_DELTA = 0.01f;

  private DFSNetworkTopologyWithWeight cluster;
  private Map<String, NodeStatistics> nodeMap;

  /**
   * Helper class to store the information and statistics of a datanode.
   */
  private static final class NodeStatistics {
    private final DatanodeDescriptor dn;
    private final String rack;
    private final int weight;
    private final StorageType storageType;

    private long chosenCount;
    private boolean isExcluded;

    private NodeStatistics(DatanodeDescriptor dn, String rack, int weight,
        StorageType storageType) {
      this.dn = dn;
      this.rack = rack;
      this.weight = weight;
      this.storageType = storageType;
    }

    public int getWeight() {
      return weight;
    }

    void incrementChosenCount() {
      chosenCount++;
    }

    void resetChosenCount() {
      chosenCount = 0;
    }

    public boolean notExcluded() {
      return !isExcluded;
    }

    public void setExcluded(boolean val) {
      this.isExcluded = val;
    }
  }

  @BeforeEach
  public void setup() {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithWeight.class.getName());
    conf.set(DFSConfigKeys.DFS_NET_TOPOLOGY_NODE_WEIGHT_MAPPING_IMPL_KEY,
        StaticDataNodeWeightMapping.class.getName());
    cluster = (DFSNetworkTopologyWithWeight) DFSNetworkTopology.getInstance(conf);
    cluster.init(conf);

    final String[] racks = {
        "/l1/d1/r1", "/l1/d1/r1", "/l1/d1/r2", "/l1/d1/r2", "/l1/d1/r2",

        "/l1/d2/r3", "/l1/d2/r3", "/l1/d2/r3", "/l1/d2/r3",

        "/l2/d3/r1", "/l2/d3/r2", "/l2/d3/r3", "/l2/d3/r4", "/l2/d3/r5",

        "/l2/d4/r1", "/l2/d4/r1", "/l2/d4/r1", "/l2/d4/r1", "/l2/d4/r1",
        "/l2/d4/r1", "/l2/d4/r1", "/l2/d4/r2",

        "/l3/d5/r1", "/l3/d5/r1",

        "/l4/d6/r1"};

    final String[] hosts = {
        "host1", "host2", "host3", "host4", "host5",

        "host6", "host7", "host8", "host9",

        "host10", "host11", "host12", "host13", "host14",

        "host15", "host16", "host17", "host18", "host19",
        "host20", "host21", "host22",

        "host23", "host24",

        "host25"};

    final StorageType[] types = {
        StorageType.SSD, StorageType.DISK, StorageType.SSD,
        StorageType.DISK, StorageType.DISK,

        StorageType.DISK, StorageType.SSD, StorageType.SSD, StorageType.SSD,

        StorageType.DISK, StorageType.DISK, StorageType.DISK,
        StorageType.SSD, StorageType.SSD,

        StorageType.DISK, StorageType.DISK, StorageType.SSD,
        StorageType.SSD, StorageType.SSD, StorageType.SSD,
        StorageType.SSD, StorageType.SSD,

        StorageType.SSD, StorageType.DISK,

        StorageType.SSD};

    final int[] weights = {
        3, 2, 1, 5, 1,

        3, 7, 3, 3,

        1, 3, 2, 6, 1,

        3, 7, 3, 3, 9, 3, 7, 4,

        5, 10,

        3};

    final DatanodeStorageInfo[] storages =
        DFSTestUtil.createDatanodeStorageInfos(racks.length, racks, hosts, types);
    DatanodeDescriptor[] dds = DFSTestUtil.toDatanodeDescriptor(storages);
    nodeMap = new HashMap<>();
    for (int i = 0; i < dds.length; i++) {
      StaticDataNodeWeightMapping.setNodeWeight(dds[i].getHostName(), weights[i]);
      nodeMap.put(dds[i].getHostName(),
          new NodeStatistics(dds[i], racks[i], weights[i], storages[i].getStorageType()));
      cluster.add(dds[i]);
    }
  }

  @AfterAll
  public static void cleanup() {
    StaticDataNodeWeightMapping.resetMap();
  }

  @Test
  public void testChooseRandomProbability() {
    String rootScope = "";

    for (int i = 0; i < CHOOSE_TIMES; i++) {
      Node n = cluster.chooseRandomWithStorageType(rootScope, null, null,
          StorageType.DISK);
      assertChosenNode(n, rootScope, null, StorageType.DISK);

      n = cluster.chooseRandomWithStorageType(rootScope, null, null,
          StorageType.SSD);
      assertChosenNode(n, rootScope, null, StorageType.SSD);
    }

    verifyProbability(filterNodeStatistics(rootScope, StorageType.DISK));
    verifyProbability(filterNodeStatistics(rootScope, StorageType.SSD));
    resetStatistics();

    // test with scope
    String l1Scope = "/l2";

    for (int i = 0; i < CHOOSE_TIMES; i++) {
      Node n = cluster.chooseRandomWithStorageType(l1Scope, null, null,
          StorageType.DISK);
      assertChosenNode(n, l1Scope, null, StorageType.DISK);

      n = cluster.chooseRandomWithStorageType(l1Scope, null, null,
          StorageType.SSD);
      assertChosenNode(n, l1Scope, null, StorageType.SSD);
    }

    verifyProbability(filterNodeStatistics(l1Scope, StorageType.DISK));
    verifyProbability(filterNodeStatistics(l1Scope, StorageType.SSD));
    resetStatistics();
  }

  @Test
  public void testChooseRandomProbabilityWithExcluded() {
    String rootScope = "/";

    String hostToExclude = "host25";
    HashSet<Node> excludedNodes = new HashSet<>();
    excludedNodes.add(nodeMap.get(hostToExclude).dn);

    List<NodeStatistics> ssdUnderRoot = filterNodeStatistics(rootScope, StorageType.SSD);

    for (int i = 0; i < CHOOSE_TIMES; i++) {
      Node n = cluster.chooseRandomWithStorageType(rootScope, null, null,
          StorageType.SSD);
      assertChosenNode(n, rootScope, null, StorageType.SSD);
    }
    verifyProbability(ssdUnderRoot);
    resetStatistics();

    // test exclude nodes
    for (int i = 0; i < CHOOSE_TIMES; i++) {
      Node n = cluster.chooseRandomWithStorageType(rootScope, null, excludedNodes,
          StorageType.SSD);
      assertChosenNode(n, rootScope, null, StorageType.SSD);
      assertNotEquals(hostToExclude, ((DatanodeDescriptor) n).getHostName());
    }
    excludeNode(hostToExclude);
    verifyProbability(ssdUnderRoot);
    resetStatistics();

    // test exclude scope
    String l2Scope = "/l2";
    for (int i = 0; i < CHOOSE_TIMES; i++) {
      Node n = cluster.chooseRandomWithStorageType(rootScope, l2Scope, null,
          StorageType.SSD);
      assertChosenNode(n, rootScope, l2Scope, StorageType.SSD);
    }

    excludeNodes(l2Scope, StorageType.SSD);
    verifyProbability(ssdUnderRoot);
    resetStatistics();
  }

  @Test
  public void testGetSubtreeStorageCount() {
    Node l2 = cluster.getNode("/l2");
    Node l2d3 = cluster.getNode("/l2/d3");
    Node l2d3r1 = cluster.getNode("/l2/d3/r1");
    Node l2d3r3 = cluster.getNode("/l2/d3/r3");

    assertInstanceOf(DFSTopologyNodeImplWithWeight.class, l2);
    assertInstanceOf(DFSTopologyNodeImplWithWeight.class, l2d3);
    assertInstanceOf(DFSTopologyNodeImplWithWeight.class, l2d3r1);
    assertInstanceOf(DFSTopologyNodeImplWithWeight.class, l2d3r3);

    DFSTopologyNodeImpl innerl2 = (DFSTopologyNodeImpl) l2;
    DFSTopologyNodeImpl innerl2d3 = (DFSTopologyNodeImpl) l2d3;
    DFSTopologyNodeImpl innerl2d3r1 = (DFSTopologyNodeImpl) l2d3r1;
    DFSTopologyNodeImpl innerl2d3r3 = (DFSTopologyNodeImpl) l2d3r3;

    assertEquals(computeTotalWeight("/l2", StorageType.DISK),
        innerl2.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(computeTotalWeight("/l2/d3", StorageType.DISK),
        innerl2d3.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(computeTotalWeight("/l2/d3/r1", StorageType.DISK),
        innerl2d3r1.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(computeTotalWeight("/l2/d3/r3", StorageType.DISK),
        innerl2d3r3.getSubtreeStorageCount(StorageType.DISK));

    DatanodeStorageInfo storageInfo =
        DFSTestUtil.createDatanodeStorageInfo("StorageID",
            "1.2.3.4", "/l2/d3/r1", "newhost");
    DatanodeDescriptor newNode = storageInfo.getDatanodeDescriptor();
    int newNodeWeight = 3;
    StaticDataNodeWeightMapping.setNodeWeight(newNode.getHostName(), newNodeWeight);
    cluster.add(newNode);

    assertEquals(computeTotalWeight("/l2", StorageType.DISK, newNodeWeight),
        innerl2.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(computeTotalWeight("/l2/d3", StorageType.DISK, newNodeWeight),
        innerl2d3.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(computeTotalWeight("/l2/d3/r1", StorageType.DISK, newNodeWeight),
        innerl2d3r1.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(computeTotalWeight("/l2/d3/r3", StorageType.DISK),
        innerl2d3r3.getSubtreeStorageCount(StorageType.DISK));

    cluster.remove(newNode);

    assertEquals(computeTotalWeight("/l2", StorageType.DISK),
        innerl2.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(computeTotalWeight("/l2/d3", StorageType.DISK),
        innerl2d3.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(computeTotalWeight("/l2/d3/r1", StorageType.DISK),
        innerl2d3r1.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(computeTotalWeight("/l2/d3/r3", StorageType.DISK),
        innerl2d3r3.getSubtreeStorageCount(StorageType.DISK));
  }

  private int computeTotalWeight(String scope, StorageType storageType) {
    return computeTotalWeight(scope, storageType, 0);
  }

  private int computeTotalWeight(String scope, StorageType storageType, int base) {
    return computeTotalWeight(filterNodeStatistics(scope, storageType), base);
  }

  private int computeTotalWeight(List<NodeStatistics> nodes, int base) {
    return nodes.stream()
        .filter(NodeStatistics::notExcluded)
        .map(NodeStatistics::getWeight)
        .reduce(base, Integer::sum);
  }

  private List<NodeStatistics> filterNodeStatistics(String scope, StorageType storageType) {
    return nodeMap.values().stream()
        .filter(dn -> dn.rack.startsWith(scope))
        .filter(dn -> dn.storageType == storageType)
        .collect(Collectors.toList());
  }

  private void incrementChosenCount(Node node) {
    getNodeStatistics(node).incrementChosenCount();
  }

  private void verifyProbability(List<NodeStatistics> dnInfos) {
    int totalWeight = computeTotalWeight(dnInfos, 0);
    assertTrue(totalWeight > 0);

    for (NodeStatistics dnInfo : dnInfos) {
      if (dnInfo.notExcluded()) {
        double realProbability = (double) dnInfo.chosenCount / CHOOSE_TIMES;
        double expectedProbability = (double) dnInfo.weight / totalWeight;
        assertEquals(expectedProbability, realProbability, PROBABILITY_DELTA,
            "Node " + dnInfo.dn.getHostName() + " has wrong probability.");
      } else {
        assertEquals(0, dnInfo.chosenCount);
      }
    }
  }

  private void resetStatistics() {
    for (NodeStatistics dnInfo : nodeMap.values()) {
      dnInfo.resetChosenCount();
      dnInfo.setExcluded(false);
    }
  }

  private void excludeNode(String hostName) {
    nodeMap.get(hostName).setExcluded(true);
  }

  private void excludeNodes(String scope, StorageType storageType) {
    for (NodeStatistics nodeStatistics : filterNodeStatistics(scope, storageType)) {
      excludeNode(nodeStatistics.dn.getHostName());
    }
  }

  private void assertChosenNode(Node node, String scope, String excludeScope,
      StorageType storageType) {
    assertInstanceOf(DatanodeDescriptor.class, node);
    NodeStatistics nodeStatistics = getNodeStatistics(node);
    if (scope != null) {
      assertTrue(isChildScope(nodeStatistics.rack, scope));
    }
    if (excludeScope != null) {
      assertFalse(isChildScope(nodeStatistics.rack, excludeScope));
    }
    assertEquals(nodeStatistics.storageType, storageType);
    incrementChosenCount(node);
  }

  private NodeStatistics getNodeStatistics(Node node) {
    return nodeMap.get(((DatanodeDescriptor) node).getHostName());
  }

  private static boolean isChildScope(final String parentScope,
      final String childScope) {
    String pScope = parentScope.endsWith(NodeBase.PATH_SEPARATOR_STR) ?
        parentScope :  parentScope + NodeBase.PATH_SEPARATOR_STR;
    String cScope = childScope.endsWith(NodeBase.PATH_SEPARATOR_STR) ?
        childScope :  childScope + NodeBase.PATH_SEPARATOR_STR;
    return pScope.startsWith(cScope);
  }

}
