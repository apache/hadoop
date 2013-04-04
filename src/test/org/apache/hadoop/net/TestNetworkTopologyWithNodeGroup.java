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

import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;

public class TestNetworkTopologyWithNodeGroup extends TestCase {
  private final static NetworkTopologyWithNodeGroup cluster = new 
      NetworkTopologyWithNodeGroup();

  private final static DatanodeDescriptor dataNodes[] = new DatanodeDescriptor[] {
      new DatanodeDescriptor(new DatanodeID("h1:5020"), "/d1/r1/n1"),
      new DatanodeDescriptor(new DatanodeID("h2:5020"), "/d1/r1/n1"),
      new DatanodeDescriptor(new DatanodeID("h3:5020"), "/d1/r1/n2"),
      new DatanodeDescriptor(new DatanodeID("h4:5020"), "/d1/r2/n3"),
      new DatanodeDescriptor(new DatanodeID("h5:5020"), "/d1/r2/n3"),
      new DatanodeDescriptor(new DatanodeID("h6:5020"), "/d1/r2/n4"),
      new DatanodeDescriptor(new DatanodeID("h7:5020"), "/d2/r3/n5"),
      new DatanodeDescriptor(new DatanodeID("h8:5020"), "/d2/r3/n6")
  };

  private final static NodeBase computeNode = new NodeBase("/d1/r1/n1/h9");
  
  private final static NodeBase rackOnlyNode = new NodeBase("h10", "/r1");

  static {
    for(int i=0; i<dataNodes.length; i++) {
      cluster.add(dataNodes[i]);
    }
  }

  public void testNumOfChildren() throws Exception {
    assertEquals(cluster.getNumOfLeaves(), dataNodes.length);
  }

  public void testNumOfRacks() throws Exception {
    assertEquals(cluster.getNumOfRacks(), 3);
  }

  public void testRacks() throws Exception {
    assertEquals(cluster.getNumOfRacks(), 3);
    assertTrue(cluster.isOnSameRack(dataNodes[0], dataNodes[1]));
    assertTrue(cluster.isOnSameRack(dataNodes[1], dataNodes[2]));
    assertFalse(cluster.isOnSameRack(dataNodes[2], dataNodes[3]));
    assertTrue(cluster.isOnSameRack(dataNodes[3], dataNodes[4]));
    assertTrue(cluster.isOnSameRack(dataNodes[4], dataNodes[5]));
    assertFalse(cluster.isOnSameRack(dataNodes[5], dataNodes[6]));
    assertTrue(cluster.isOnSameRack(dataNodes[6], dataNodes[7]));
  }

  public void testNodeGroups() throws Exception {
    assertEquals(cluster.getNumOfRacks(), 3);
    assertTrue(cluster.isOnSameNodeGroup(dataNodes[0], dataNodes[1]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[1], dataNodes[2]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[2], dataNodes[3]));
    assertTrue(cluster.isOnSameNodeGroup(dataNodes[3], dataNodes[4]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[4], dataNodes[5]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[5], dataNodes[6]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[6], dataNodes[7]));
  }

  public void testGetDistance() throws Exception {
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[0]), 0);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[1]), 2);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[2]), 4);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[3]), 6);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[6]), 8);
  }

  public void testPseudoSortByDistance() throws Exception {
    DatanodeDescriptor[] testNodes = new DatanodeDescriptor[4];

    // array contains both local node, local node group & local rack node
    testNodes[0] = dataNodes[1];
    testNodes[1] = dataNodes[2];
    testNodes[2] = dataNodes[3];
    testNodes[3] = dataNodes[0];
    cluster.pseudoSortByDistance(dataNodes[0], testNodes );
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[1]);
    assertTrue(testNodes[2] == dataNodes[2]);
    assertTrue(testNodes[3] == dataNodes[3]);

    // array contains local node & local node group
    testNodes[0] = dataNodes[3];
    testNodes[1] = dataNodes[4];
    testNodes[2] = dataNodes[1];
    testNodes[3] = dataNodes[0];
    cluster.pseudoSortByDistance(dataNodes[0], testNodes );
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[1]);

    // array contains local node & rack node
    testNodes[0] = dataNodes[5];
    testNodes[1] = dataNodes[3];
    testNodes[2] = dataNodes[2];
    testNodes[3] = dataNodes[0];
    cluster.pseudoSortByDistance(dataNodes[0], testNodes );
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[2]);

    // array contains local-nodegroup node (not a data node also) & rack node
    testNodes[0] = dataNodes[6];
    testNodes[1] = dataNodes[7];
    testNodes[2] = dataNodes[2];
    testNodes[3] = dataNodes[0];
    cluster.pseudoSortByDistance(computeNode, testNodes );
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
    for (DatanodeDescriptor dnd : dataNodes) {
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
