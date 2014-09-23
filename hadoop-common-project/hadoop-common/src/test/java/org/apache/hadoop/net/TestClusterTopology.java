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
import java.util.List;

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
    NetworkTopology cluster = new NetworkTopology();
    cluster.add(getNewNode("node1", "/d1/r1"));
    NodeElement node2 = getNewNode("node2", "/d1/r2");
    cluster.add(node2);
    cluster.add(getNewNode("node3", "/d1/r3"));
    NodeElement node3 = getNewNode("node4", "/d1/r4");
    cluster.add(node3);
    // create exclude list
    List<Node> excludedNodes = new ArrayList<Node>();

    assertEquals("4 nodes should be available", 4,
        cluster.countNumOfAvailableNodes(NodeBase.ROOT, excludedNodes));
    NodeElement deadNode = getNewNode("node5", "/d1/r2");
    excludedNodes.add(deadNode);
    assertEquals("4 nodes should be available with extra excluded Node", 4,
        cluster.countNumOfAvailableNodes(NodeBase.ROOT, excludedNodes));
    // add one existing node to exclude list
    excludedNodes.add(node3);
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
  }

  private NodeElement getNewNode(String name, String rackLocation) {
    NodeElement node = new NodeElement(name);
    node.setNetworkLocation(rackLocation);
    return node;
  }
}
