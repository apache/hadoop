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
package org.apache.hadoop.net;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestClusterTopology extends Assert {

  public static class NodeElement implements Node {
    private String location;
    private String name;
    private Node parent;
    private int level;

    public NodeElement(String name) {
      this.name = name;
    }

    @Override
    public String getNetworkLocation() {
      return location;
    }

    @Override
    public void setNetworkLocation(String location) {
      this.location = location;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Node getParent() {
      return parent;
    }

    @Override
    public void setParent(Node parent) {
      this.parent = parent;
    }

    @Override
    public int getLevel() {
      return level;
    }

    @Override
    public void setLevel(int i) {
      this.level = i;
    }

  }

  /**
   * Test the count of nodes with exclude list
   */
  @Test
  public void testCountNumNodes() throws Exception {
    // create the topology
    NetworkTopology cluster = NetworkTopology.getInstance(new Configuration());
    NodeElement node1 = getNewNode("node1", "/d1/r1");
    cluster.add(node1);
    NodeElement node2 = getNewNode("node2", "/d1/r2");
    cluster.add(node2);
    NodeElement node3 = getNewNode("node3", "/d1/r3");
    cluster.add(node3);
    NodeElement node4 = getNewNode("node4", "/d1/r4");
    cluster.add(node4);
    // create exclude list
    List<Node> excludedNodes = new ArrayList<Node>();

    assertEquals("4 nodes should be available", 4,
        cluster.countNumOfAvailableNodes(NodeBase.ROOT, excludedNodes));
    NodeElement deadNode = getNewNode("node5", "/d1/r2");
    excludedNodes.add(deadNode);
    assertEquals("4 nodes should be available with extra excluded Node", 4,
        cluster.countNumOfAvailableNodes(NodeBase.ROOT, excludedNodes));
    // add one existing node to exclude list
    excludedNodes.add(node4);
    assertEquals("excluded nodes with ROOT scope should be considered", 3,
        cluster.countNumOfAvailableNodes(NodeBase.ROOT, excludedNodes));
    assertEquals("excluded nodes without ~ scope should be considered", 2,
        cluster.countNumOfAvailableNodes("~" + deadNode.getNetworkLocation(),
            excludedNodes));
    assertEquals("excluded nodes with rack scope should be considered", 1,
        cluster.countNumOfAvailableNodes(deadNode.getNetworkLocation(),
            excludedNodes));
    // adding the node in excluded scope to excluded list
    excludedNodes.add(node2);
    assertEquals("excluded nodes with ~ scope should be considered", 2,
        cluster.countNumOfAvailableNodes("~" + deadNode.getNetworkLocation(),
            excludedNodes));
    // getting count with non-exist scope.
    assertEquals("No nodes should be considered for non-exist scope", 0,
        cluster.countNumOfAvailableNodes("/non-exist", excludedNodes));
    // remove a node from the cluster
    cluster.remove(node1);
    assertEquals("1 node should be available", 1,
        cluster.countNumOfAvailableNodes(NodeBase.ROOT, excludedNodes));
  }

  /**
   * Test how well we pick random nodes.
   */
  @Test
  public void testChooseRandom() {
    // create the topology
    NetworkTopology cluster = NetworkTopology.getInstance(new Configuration());
    NodeElement node1 = getNewNode("node1", "/d1/r1");
    cluster.add(node1);
    NodeElement node2 = getNewNode("node2", "/d1/r2");
    cluster.add(node2);
    NodeElement node3 = getNewNode("node3", "/d1/r3");
    cluster.add(node3);
    NodeElement node4 = getNewNode("node4", "/d1/r3");
    cluster.add(node4);

    // Number of test runs
    int numTestRuns = 3;
    int chiSquareTestRejectedCounter = 0;

    // Number of iterations to do the test
    int numIterations = 100;

    for (int testRun = 0; testRun < numTestRuns; ++testRun) {

      // Pick random nodes
      HashMap<String, Integer> histogram = new HashMap<String, Integer>();
      for (int i = 0; i < numIterations; i++) {
        String randomNode = cluster.chooseRandom(NodeBase.ROOT).getName();
        if (!histogram.containsKey(randomNode)) {
          histogram.put(randomNode, 0);
        }
        histogram.put(randomNode, histogram.get(randomNode) + 1);
      }
      assertEquals("Random is not selecting all nodes", 4, histogram.size());

      // Check with 99% confidence alpha=0.01 as confidence = 100 * (1 - alpha)
      ChiSquareTest chiSquareTest = new ChiSquareTest();
      double[] expected = new double[histogram.size()];
      long[] observed = new long[histogram.size()];
      int j = 0;
      for (Integer occurrence : histogram.values()) {
        expected[j] = 1.0 * numIterations / histogram.size();
        observed[j] = occurrence;
        j++;
      }
      boolean chiSquareTestRejected =
            chiSquareTest.chiSquareTest(expected, observed, 0.01);

      if (chiSquareTestRejected) {
        ++chiSquareTestRejectedCounter;
      }
    }

    // Check that they have the proper distribution
    assertFalse("Random not choosing nodes with proper distribution",
            chiSquareTestRejectedCounter==3);

    // Pick random nodes excluding the 2 nodes in /d1/r3
    HashMap<String, Integer> histogram = new HashMap<String, Integer>();
    for (int i=0; i<numIterations; i++) {
      String randomNode = cluster.chooseRandom("~/d1/r3").getName();
      if (!histogram.containsKey(randomNode)) {
        histogram.put(randomNode, 0);
      }
      histogram.put(randomNode, histogram.get(randomNode) + 1);
    }
    assertEquals("Random is not selecting the nodes it should",
        2, histogram.size());

    Node val = cluster.chooseRandom("/d1", "/d", Collections.emptyList());
    assertNotNull(val);

  }

  @Test
  public void testChooseRandomExcluded() {
    // create the topology
    //                        a1
    //                b1------|--------b2
    //                 |                |
    //          c1-----|-----c2         c3
    //         /  \          |          |
    //        /    \         |          |
    //     node1    node2   node3      node4

    NetworkTopology cluster = NetworkTopology.getInstance(new Configuration());
    NodeElement node1 = getNewNode("node1", "/a1/b1/c1");
    cluster.add(node1);
    NodeElement node2 = getNewNode("node2", "/a1/b1/c1");
    cluster.add(node2);
    NodeElement node3 = getNewNode("node3", "/a1/b1/c2");
    cluster.add(node3);
    NodeElement node4 = getNewNode("node4", "/a1/b2/c3");
    cluster.add(node4);

    Node node = cluster.chooseRandom("/a1/b1", "/a1/b1/c1", null);
    assertSame("node3", node.getName());

    node = cluster.chooseRandom("/a1/b1", "/a1/b1/c1", Arrays.asList(node1));
    assertSame("node3", node.getName());

    node = cluster.chooseRandom("/a1/b1", "/a1/b1/c1", Arrays.asList(node3));
    assertNull(node);

    node = cluster.chooseRandom("/a1/b1", "/a1/b1/c1", Arrays.asList(node4));
    assertSame("node3", node.getName());
  }

  @Test
  public void testNodeBaseNormalizeRemoveLeadingSlash() {
    assertEquals("/d1", NodeBase.normalize("/d1///"));
    assertEquals("/d1", NodeBase.normalize("/d1/"));
    assertEquals("/d1", NodeBase.normalize("/d1"));
    assertEquals("", NodeBase.normalize("///"));
    assertEquals("", NodeBase.normalize("/"));
  }

  private NodeElement getNewNode(String name, String rackLocation) {
    NodeElement node = new NodeElement(name);
    node.setNetworkLocation(rackLocation);
    return node;
  }

  private NodeElement getNewNode(NetworkTopology cluster,
                                 String name, String rackLocation) {
    NodeElement node = getNewNode(name, rackLocation);
    cluster.add(node);
    return node;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWeights() {
    // create the topology
    NetworkTopology cluster = NetworkTopology.getInstance(new Configuration());
    NodeElement node1 = getNewNode(cluster, "node1", "/r1");
    NodeElement node2 = getNewNode(cluster, "node2", "/r1");
    NodeElement node3 = getNewNode(cluster, "node3", "/r2");
    for (Pair<Integer, NodeElement> test: new Pair[]{Pair.of(0, node1),
        Pair.of(2, node2), Pair.of(4, node3)}) {
      int expect = test.getLeft();
      assertEquals(test.toString(), expect, cluster.getWeight(node1, test.getRight()));
      assertEquals(test.toString(), expect,
          cluster.getWeightUsingNetworkLocation(node1, test.getRight()));
    }
    // Reset so that we can have 2 levels
    cluster = NetworkTopology.getInstance(new Configuration());
    NodeElement node5 = getNewNode(cluster, "node5", "/pod1/r1");
    NodeElement node6 = getNewNode(cluster, "node6", "/pod1/r1");
    NodeElement node7 = getNewNode(cluster, "node7", "/pod1/r2");
    NodeElement node8 = getNewNode(cluster, "node8", "/pod2/r3");
    for (Pair<Integer, NodeElement> test: new Pair[]{Pair.of(0, node5),
        Pair.of(2, node6), Pair.of(4, node7), Pair.of(6, node8)}) {
      int expect = test.getLeft();
      assertEquals(test.toString(), expect, cluster.getWeight(node5, test.getRight()));
      assertEquals(test.toString(), expect,
          cluster.getWeightUsingNetworkLocation(node5, test.getRight()));
    }
  }
}
