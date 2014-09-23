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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TestNetworkTopologyWithNodeGroup {
  private final static NetworkTopologyWithNodeGroup cluster = new 
      NetworkTopologyWithNodeGroup();

  private final static NodeBase dataNodes[] = new NodeBase[] {
      new NodeBase("h1", "/d1/r1/s1"),
      new NodeBase("h2", "/d1/r1/s1"),
      new NodeBase("h3", "/d1/r1/s2"),
      new NodeBase("h4", "/d1/r2/s3"),
      new NodeBase("h5", "/d1/r2/s3"),
      new NodeBase("h6", "/d1/r2/s4"),
      new NodeBase("h7", "/d2/r3/s5"),
      new NodeBase("h8", "/d2/r3/s6")
  };

  private final static NodeBase computeNode = new NodeBase("/d1/r1/s1/h9");
  
  private final static NodeBase rackOnlyNode = new NodeBase("h10", "/r2");

  static {
    for(int i=0; i<dataNodes.length; i++) {
      cluster.add(dataNodes[i]);
    }
  }

  @Test
  public void testNumOfChildren() throws Exception {
    assertEquals(dataNodes.length, cluster.getNumOfLeaves());
  }

  @Test
  public void testNumOfRacks() throws Exception {
    assertEquals(3, cluster.getNumOfRacks());
  }

  @Test
  public void testRacks() throws Exception {
    assertEquals(3, cluster.getNumOfRacks());
    assertTrue(cluster.isOnSameRack(dataNodes[0], dataNodes[1]));
    assertTrue(cluster.isOnSameRack(dataNodes[1], dataNodes[2]));
    assertFalse(cluster.isOnSameRack(dataNodes[2], dataNodes[3]));
    assertTrue(cluster.isOnSameRack(dataNodes[3], dataNodes[4]));
    assertTrue(cluster.isOnSameRack(dataNodes[4], dataNodes[5]));
    assertFalse(cluster.isOnSameRack(dataNodes[5], dataNodes[6]));
    assertTrue(cluster.isOnSameRack(dataNodes[6], dataNodes[7]));
  }

  @Test
  public void testNodeGroups() throws Exception {
    assertEquals(3, cluster.getNumOfRacks());
    assertTrue(cluster.isOnSameNodeGroup(dataNodes[0], dataNodes[1]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[1], dataNodes[2]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[2], dataNodes[3]));
    assertTrue(cluster.isOnSameNodeGroup(dataNodes[3], dataNodes[4]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[4], dataNodes[5]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[5], dataNodes[6]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[6], dataNodes[7]));
  }

  @Test
  public void testGetDistance() throws Exception {
    assertEquals(0, cluster.getDistance(dataNodes[0], dataNodes[0]));
    assertEquals(2, cluster.getDistance(dataNodes[0], dataNodes[1]));
    assertEquals(4, cluster.getDistance(dataNodes[0], dataNodes[2]));
    assertEquals(6, cluster.getDistance(dataNodes[0], dataNodes[3]));
    assertEquals(8, cluster.getDistance(dataNodes[0], dataNodes[6]));
  }

  @Test
  public void testSortByDistance() throws Exception {
    NodeBase[] testNodes = new NodeBase[4];

    // array contains both local node, local node group & local rack node
    testNodes[0] = dataNodes[1];
    testNodes[1] = dataNodes[2];
    testNodes[2] = dataNodes[3];
    testNodes[3] = dataNodes[0];
    cluster.sortByDistance(dataNodes[0], testNodes, testNodes.length);
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[1]);
    assertTrue(testNodes[2] == dataNodes[2]);
    assertTrue(testNodes[3] == dataNodes[3]);

    // array contains local node & local node group
    testNodes[0] = dataNodes[3];
    testNodes[1] = dataNodes[4];
    testNodes[2] = dataNodes[1];
    testNodes[3] = dataNodes[0];
    cluster.sortByDistance(dataNodes[0], testNodes, testNodes.length);
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[1]);

    // array contains local node & rack node
    testNodes[0] = dataNodes[5];
    testNodes[1] = dataNodes[3];
    testNodes[2] = dataNodes[2];
    testNodes[3] = dataNodes[0];
    cluster.sortByDistance(dataNodes[0], testNodes, testNodes.length);
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[2]);

    // array contains local-nodegroup node (not a data node also) & rack node
    testNodes[0] = dataNodes[6];
    testNodes[1] = dataNodes[7];
    testNodes[2] = dataNodes[2];
    testNodes[3] = dataNodes[0];
    cluster.sortByDistance(computeNode, testNodes, testNodes.length);
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[2]);
  }

  /**
   * This picks a large number of nodes at random in order to ensure coverage
   * 
   * @param numNodes the number of nodes
   * @param excludedScope the excluded scope
   * @return the frequency that nodes were chosen
   */
  private Map<Node, Integer> pickNodesAtRandom(int numNodes,
      String excludedScope) {
    Map<Node, Integer> frequency = new HashMap<Node, Integer>();
    for (NodeBase dnd : dataNodes) {
      frequency.put(dnd, 0);
    }

    for (int j = 0; j < numNodes; j++) {
      Node random = cluster.chooseRandom(excludedScope);
      frequency.put(random, frequency.get(random) + 1);
    }
    return frequency;
  }

  /**
   * This test checks that chooseRandom works for an excluded node.
   */
  /**
   * Test replica placement policy in case last node is invalid.
   * We create 6 nodes but the last node is in fault topology (with rack info),
   * so cannot be added to cluster. We should test proper exception is thrown in 
   * adding node but shouldn't affect the cluster.
   */
  @Test
  public void testChooseRandomExcludedNode() {
    String scope = "~" + NodeBase.getPath(dataNodes[0]);
    Map<Node, Integer> frequency = pickNodesAtRandom(100, scope);

    for (Node key : dataNodes) {
      // all nodes except the first should be more than zero
      assertTrue(frequency.get(key) > 0 || key == dataNodes[0]);
    }
  }
  
  /**
   * This test checks that adding a node with invalid topology will be failed 
   * with an exception to show topology is invalid.
   */
  @Test
  public void testAddNodeWithInvalidTopology() {
    // The last node is a node with invalid topology
    try {
      cluster.add(rackOnlyNode);
      fail("Exception should be thrown, so we should not have reached here.");
    } catch (Exception e) {
      if (!(e instanceof IllegalArgumentException)) {
        fail("Expecting IllegalArgumentException, but caught:" + e);
      }
      assertTrue(e.getMessage().contains("illegal network location"));
    }
  }

}
