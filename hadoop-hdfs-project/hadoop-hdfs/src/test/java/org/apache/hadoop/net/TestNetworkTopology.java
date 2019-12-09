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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class TestNetworkTopology {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestNetworkTopology.class);
  private final static NetworkTopology cluster =
      NetworkTopology.getInstance(new Configuration());
  private DatanodeDescriptor dataNodes[];

  @Rule
  public Timeout testTimeout = new Timeout(30000);

  @Before
  public void setupDatanodes() {
    dataNodes = new DatanodeDescriptor[] {
        DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r2"),
        DFSTestUtil.getDatanodeDescriptor("4.4.4.4", "/d1/r2"),
        DFSTestUtil.getDatanodeDescriptor("5.5.5.5", "/d1/r2"),
        DFSTestUtil.getDatanodeDescriptor("6.6.6.6", "/d2/r3"),
        DFSTestUtil.getDatanodeDescriptor("7.7.7.7", "/d2/r3"),
        DFSTestUtil.getDatanodeDescriptor("8.8.8.8", "/d2/r3"),
        DFSTestUtil.getDatanodeDescriptor("9.9.9.9", "/d3/r1"),
        DFSTestUtil.getDatanodeDescriptor("10.10.10.10", "/d3/r1"),
        DFSTestUtil.getDatanodeDescriptor("11.11.11.11", "/d3/r1"),
        DFSTestUtil.getDatanodeDescriptor("12.12.12.12", "/d3/r2"),
        DFSTestUtil.getDatanodeDescriptor("13.13.13.13", "/d3/r2"),
        DFSTestUtil.getDatanodeDescriptor("14.14.14.14", "/d4/r1"),
        DFSTestUtil.getDatanodeDescriptor("15.15.15.15", "/d4/r1"),
        DFSTestUtil.getDatanodeDescriptor("16.16.16.16", "/d4/r1"),
        DFSTestUtil.getDatanodeDescriptor("17.17.17.17", "/d4/r1"),
        DFSTestUtil.getDatanodeDescriptor("18.18.18.18", "/d4/r1"),
        DFSTestUtil.getDatanodeDescriptor("19.19.19.19", "/d4/r1"),
        DFSTestUtil.getDatanodeDescriptor("20.20.20.20", "/d4/r1"),        
    };
    for (int i = 0; i < dataNodes.length; i++) {
      cluster.add(dataNodes[i]);
    }
    dataNodes[9].setDecommissioned();
    dataNodes[10].setDecommissioned();
    GenericTestUtils.setLogLevel(NetworkTopology.LOG, Level.TRACE);
  }
  
  @Test
  public void testContains() throws Exception {
    DatanodeDescriptor nodeNotInMap = 
      DFSTestUtil.getDatanodeDescriptor("8.8.8.8", "/d2/r4");
    for (int i=0; i < dataNodes.length; i++) {
      assertTrue(cluster.contains(dataNodes[i]));
    }
    assertFalse(cluster.contains(nodeNotInMap));
  }
  
  @Test
  public void testNumOfChildren() throws Exception {
    assertEquals(cluster.getNumOfLeaves(), dataNodes.length);
  }

  @Test
  public void testCreateInvalidTopology() throws Exception {
    NetworkTopology invalCluster =
        NetworkTopology.getInstance(new Configuration());
    DatanodeDescriptor invalDataNodes[] = new DatanodeDescriptor[] {
        DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1")
    };
    invalCluster.add(invalDataNodes[0]);
    invalCluster.add(invalDataNodes[1]);
    try {
      invalCluster.add(invalDataNodes[2]);
      fail("expected InvalidTopologyException");
    } catch (NetworkTopology.InvalidTopologyException e) {
      assertTrue(e.getMessage().startsWith("Failed to add "));
      assertTrue(e.getMessage().contains(
          "You cannot have a rack and a non-rack node at the same " +
          "level of the network topology."));
    }
  }

  @Test
  public void testRacks() throws Exception {
    assertEquals(cluster.getNumOfRacks(), 6);
    assertTrue(cluster.isOnSameRack(dataNodes[0], dataNodes[1]));
    assertFalse(cluster.isOnSameRack(dataNodes[1], dataNodes[2]));
    assertTrue(cluster.isOnSameRack(dataNodes[2], dataNodes[3]));
    assertTrue(cluster.isOnSameRack(dataNodes[3], dataNodes[4]));
    assertFalse(cluster.isOnSameRack(dataNodes[4], dataNodes[5]));
    assertTrue(cluster.isOnSameRack(dataNodes[5], dataNodes[6]));
  }
  
  @Test
  public void testGetDistance() throws Exception {
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[0]), 0);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[1]), 2);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[3]), 4);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[6]), 6);
    // verify the distance is zero as long as two nodes have the same path.
    // They don't need to refer to the same object.
    NodeBase node1 = new NodeBase(dataNodes[0].getHostName(),
        dataNodes[0].getNetworkLocation());
    NodeBase node2 = new NodeBase(dataNodes[0].getHostName(),
        dataNodes[0].getNetworkLocation());
    assertEquals(0, cluster.getDistance(node1, node2));
    // verify the distance can be computed by path.
    // They don't need to refer to the same object or parents.
    NodeBase node3 = new NodeBase(dataNodes[3].getHostName(),
        dataNodes[3].getNetworkLocation());
    NodeBase node4 = new NodeBase(dataNodes[6].getHostName(),
        dataNodes[6].getNetworkLocation());
    assertEquals(0, NetworkTopology.getDistanceByPath(node1, node2));
    assertEquals(4, NetworkTopology.getDistanceByPath(node2, node3));
    assertEquals(6, NetworkTopology.getDistanceByPath(node2, node4));
  }

  @Test
  public void testSortByDistance() throws Exception {
    DatanodeDescriptor[] testNodes = new DatanodeDescriptor[3];
    
    // array contains both local node & local rack node
    testNodes[0] = dataNodes[1];
    testNodes[1] = dataNodes[2];
    testNodes[2] = dataNodes[0];
    cluster.setRandomSeed(0xDEADBEEF);
    cluster.sortByDistance(dataNodes[0], testNodes, testNodes.length);
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[1]);
    assertTrue(testNodes[2] == dataNodes[2]);

    // array contains both local node & local rack node & decommissioned node
    DatanodeDescriptor[] dtestNodes = new DatanodeDescriptor[5];
    dtestNodes[0] = dataNodes[8];
    dtestNodes[1] = dataNodes[12];
    dtestNodes[2] = dataNodes[11];
    dtestNodes[3] = dataNodes[9];
    dtestNodes[4] = dataNodes[10];
    cluster.setRandomSeed(0xDEADBEEF);
    cluster.sortByDistance(dataNodes[8], dtestNodes, dtestNodes.length - 2);
    assertTrue(dtestNodes[0] == dataNodes[8]);
    assertTrue(dtestNodes[1] == dataNodes[11]);
    assertTrue(dtestNodes[2] == dataNodes[12]);
    assertTrue(dtestNodes[3] == dataNodes[9]);
    assertTrue(dtestNodes[4] == dataNodes[10]);

    // array contains local node
    testNodes[0] = dataNodes[1];
    testNodes[1] = dataNodes[3];
    testNodes[2] = dataNodes[0];
    cluster.setRandomSeed(0xDEADBEEF);
    cluster.sortByDistance(dataNodes[0], testNodes, testNodes.length);
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[1]);
    assertTrue(testNodes[2] == dataNodes[3]);

    // array contains local rack node
    testNodes[0] = dataNodes[5];
    testNodes[1] = dataNodes[3];
    testNodes[2] = dataNodes[1];
    cluster.setRandomSeed(0xDEADBEEF);
    cluster.sortByDistance(dataNodes[0], testNodes, testNodes.length);
    assertTrue(testNodes[0] == dataNodes[1]);
    assertTrue(testNodes[1] == dataNodes[3]);
    assertTrue(testNodes[2] == dataNodes[5]);

    // array contains local rack node which happens to be in position 0
    testNodes[0] = dataNodes[1];
    testNodes[1] = dataNodes[5];
    testNodes[2] = dataNodes[3];
    cluster.setRandomSeed(0xDEADBEEF);
    cluster.sortByDistance(dataNodes[0], testNodes, testNodes.length);
    assertTrue(testNodes[0] == dataNodes[1]);
    assertTrue(testNodes[1] == dataNodes[3]);
    assertTrue(testNodes[2] == dataNodes[5]);

    // Same as previous, but with a different random seed to test randomization
    testNodes[0] = dataNodes[1];
    testNodes[1] = dataNodes[5];
    testNodes[2] = dataNodes[3];
    cluster.setRandomSeed(0xDEAD);
    cluster.sortByDistance(dataNodes[0], testNodes, testNodes.length);
    assertTrue(testNodes[0] == dataNodes[1]);
    assertTrue(testNodes[1] == dataNodes[3]);
    assertTrue(testNodes[2] == dataNodes[5]);

    // Array of just rack-local nodes
    // Expect a random first node
    DatanodeDescriptor first = null;
    boolean foundRandom = false;
    for (int i=5; i<=7; i++) {
      testNodes[0] = dataNodes[5];
      testNodes[1] = dataNodes[6];
      testNodes[2] = dataNodes[7];
      cluster.sortByDistance(dataNodes[i], testNodes, testNodes.length);
      if (first == null) {
        first = testNodes[0];
      } else {
        if (first != testNodes[0]) {
          foundRandom = true;
          break;
        }
      }
    }
    assertTrue("Expected to find a different first location", foundRandom);

    // Array of just remote nodes
    // Expect random first node
    first = null;
    for (int i = 1; i <= 4; i++) {
      testNodes[0] = dataNodes[13];
      testNodes[1] = dataNodes[14];
      testNodes[2] = dataNodes[15];
      cluster.sortByDistance(dataNodes[i], testNodes, testNodes.length);
      if (first == null) {
        first = testNodes[0];
      } else {
        if (first != testNodes[0]) {
          foundRandom = true;
          break;
        }
      }
    }
    assertTrue("Expected to find a different first location", foundRandom);

    //Reader is not a datanode, but is in one of the datanode's rack.
    testNodes[0] = dataNodes[0];
    testNodes[1] = dataNodes[5];
    testNodes[2] = dataNodes[8];
    Node rackClient = new NodeBase("/d3/r1/25.25.25");
    cluster.setRandomSeed(0xDEADBEEF);
    cluster.sortByDistance(rackClient, testNodes, testNodes.length);
    assertTrue(testNodes[0] == dataNodes[8]);
    assertTrue(testNodes[1] == dataNodes[5]);
    assertTrue(testNodes[2] == dataNodes[0]);

    //Reader is not a datanode , but is in one of the datanode's data center.
    testNodes[0] = dataNodes[8];
    testNodes[1] = dataNodes[5];
    testNodes[2] = dataNodes[0];
    Node dcClient = new NodeBase("/d1/r2/25.25.25");
    cluster.setRandomSeed(0xDEADBEEF);
    cluster.sortByDistance(dcClient, testNodes, testNodes.length);
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[5]);
    assertTrue(testNodes[2] == dataNodes[8]);

  }
  
  @Test
  public void testRemove() throws Exception {
    for(int i=0; i<dataNodes.length; i++) {
      cluster.remove(dataNodes[i]);
    }
    for(int i=0; i<dataNodes.length; i++) {
      assertFalse(cluster.contains(dataNodes[i]));
    }
    assertEquals(0, cluster.getNumOfLeaves());
    assertEquals(0, cluster.clusterMap.getChildren().size());
    for(int i=0; i<dataNodes.length; i++) {
      cluster.add(dataNodes[i]);
    }
  }
  
  /**
   * This picks a large number of nodes at random in order to ensure coverage
   * 
   * @param numNodes the number of nodes
   * @param excludedScope the excluded scope
   * @return the frequency that nodes were chosen
   */
  private Map<Node, Integer> pickNodesAtRandom(int numNodes,
      String excludedScope, Collection<Node> excludedNodes) {
    Map<Node, Integer> frequency = new HashMap<Node, Integer>();
    for (DatanodeDescriptor dnd : dataNodes) {
      frequency.put(dnd, 0);
    }

    for (int j = 0; j < numNodes; j++) {
      Node random = cluster.chooseRandom(excludedScope, excludedNodes);
      if (random != null) {
        frequency.put(random, frequency.get(random) + 1);
      }
    }
    LOG.info("Result:" + frequency);
    return frequency;
  }

  /**
   * This test checks that chooseRandom works for an excluded node.
   */
  @Test
  public void testChooseRandomExcludedNode() {
    String scope = "~" + NodeBase.getPath(dataNodes[0]);
    Map<Node, Integer> frequency = pickNodesAtRandom(100, scope, null);

    for (Node key : dataNodes) {
      // all nodes except the first should be more than zero
      assertTrue(frequency.get(key) > 0 || key == dataNodes[0]);
    }
  }

  /**
   * This test checks that chooseRandom works for an excluded rack.
   */
  @Test
  public void testChooseRandomExcludedRack() {
    Map<Node, Integer> frequency = pickNodesAtRandom(100, "~" + "/d2", null);
    // all the nodes on the second rack should be zero
    for (int j = 0; j < dataNodes.length; j++) {
      int freq = frequency.get(dataNodes[j]);
      if (dataNodes[j].getNetworkLocation().startsWith("/d2")) {
        assertEquals(0, freq);
      } else {
        assertTrue(freq > 0);
      }
    }
  }

  /**
   * This test checks that chooseRandom works for a list of excluded nodes.
   */
  @Test
  public void testChooseRandomExcludedNodeList() {
    String scope = "~" + NodeBase.getPath(dataNodes[0]);
    Set<Node> excludedNodes = new HashSet<>();
    excludedNodes.add(dataNodes[3]);
    excludedNodes.add(dataNodes[5]);
    excludedNodes.add(dataNodes[7]);
    excludedNodes.add(dataNodes[9]);
    excludedNodes.add(dataNodes[13]);
    excludedNodes.add(dataNodes[18]);
    Map<Node, Integer> frequency = pickNodesAtRandom(100, scope, excludedNodes);

    assertEquals("dn[3] should be excluded", 0,
        frequency.get(dataNodes[3]).intValue());
    assertEquals("dn[5] should be exclude18d", 0,
        frequency.get(dataNodes[5]).intValue());
    assertEquals("dn[7] should be excluded", 0,
        frequency.get(dataNodes[7]).intValue());
    assertEquals("dn[9] should be excluded", 0,
        frequency.get(dataNodes[9]).intValue());
    assertEquals("dn[13] should be excluded", 0,
        frequency.get(dataNodes[13]).intValue());
    assertEquals("dn[18] should be excluded", 0,
        frequency.get(dataNodes[18]).intValue());
    for (Node key : dataNodes) {
      if (excludedNodes.contains(key)) {
        continue;
      }
      // all nodes except the first should be more than zero
      assertTrue(frequency.get(key) > 0 || key == dataNodes[0]);
    }
  }

  /**
   * This test checks that chooseRandom works when all nodes are excluded.
   */
  @Test
  public void testChooseRandomExcludeAllNodes() {
    String scope = "~" + NodeBase.getPath(dataNodes[0]);
    Set<Node> excludedNodes = new HashSet<>();
    for (int i = 0; i < dataNodes.length; i++) {
      excludedNodes.add(dataNodes[i]);
    }
    Map<Node, Integer> frequency = pickNodesAtRandom(100, scope, excludedNodes);
    for (Node key : dataNodes) {
      // all nodes except the first should be more than zero
      assertTrue(frequency.get(key) == 0);
    }
  }

  @Test(timeout=180000)
  public void testInvalidNetworkTopologiesNotCachedInHdfs() throws Exception {
    // start a cluster
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      // bad rack topology
      String racks[] = { "/a/b", "/c" };
      String hosts[] = { "foo1.example.com", "foo2.example.com" };
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).
          racks(racks).hosts(hosts).build();
      cluster.waitActive();
      
      NamenodeProtocols nn = cluster.getNameNodeRpc();
      Assert.assertNotNull(nn);
      
      // Wait for one DataNode to register.
      // The other DataNode will not be able to register up because of the rack mismatch.
      DatanodeInfo[] info;
      while (true) {
        info = nn.getDatanodeReport(DatanodeReportType.LIVE);
        Assert.assertFalse(info.length == 2);
        if (info.length == 1) {
          break;
        }
        Thread.sleep(1000);
      }
      // Set the network topology of the other node to the match the network
      // topology of the node that came up.
      int validIdx = info[0].getHostName().equals(hosts[0]) ? 0 : 1;
      int invalidIdx = validIdx == 1 ? 0 : 1;
      StaticMapping.addNodeToRack(hosts[invalidIdx], racks[validIdx]);
      LOG.info("datanode " + validIdx + " came up with network location " + 
        info[0].getNetworkLocation());

      // Restart the DN with the invalid topology and wait for it to register.
      cluster.restartDataNode(invalidIdx);
      Thread.sleep(5000);
      while (true) {
        info = nn.getDatanodeReport(DatanodeReportType.LIVE);
        if (info.length == 2) {
          break;
        }
        if (info.length == 0) {
          LOG.info("got no valid DNs");
        } else if (info.length == 1) {
          LOG.info("got one valid DN: " + info[0].getHostName() +
              " (at " + info[0].getNetworkLocation() + ")");
        }
        Thread.sleep(1000);
      }
      Assert.assertEquals(info[0].getNetworkLocation(),
                          info[1].getNetworkLocation());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Tests chooseRandom with include scope, excluding a few nodes.
   */
  @Test
  public void testChooseRandomInclude1() {
    final String scope = "/d1";
    final Set<Node> excludedNodes = new HashSet<>();
    final Random r = new Random();
    for (int i = 0; i < 4; ++i) {
      final int index = r.nextInt(5);
      excludedNodes.add(dataNodes[index]);
    }
    Map<Node, Integer> frequency = pickNodesAtRandom(100, scope, excludedNodes);

    verifyResults(5, excludedNodes, frequency);
  }

  /**
   * Tests chooseRandom with include scope at rack, excluding a node.
   */
  @Test
  public void testChooseRandomInclude2() {
    String scope = dataNodes[0].getNetworkLocation();
    Set<Node> excludedNodes = new HashSet<>();
    final Random r = new Random();
    int index = r.nextInt(1);
    excludedNodes.add(dataNodes[index]);
    final int count = 100;
    Map<Node, Integer> frequency =
        pickNodesAtRandom(count, scope, excludedNodes);

    verifyResults(1, excludedNodes, frequency);
  }

  private void verifyResults(int upperbound, Set<Node> excludedNodes,
      Map<Node, Integer> frequency) {
    LOG.info("Excluded nodes are: {}", excludedNodes);
    for (int i = 0; i < upperbound; ++i) {
      final Node n = dataNodes[i];
      LOG.info("Verifying node {}", n);
      if (excludedNodes.contains(n)) {
        assertEquals(n + " should not have been chosen.", 0,
            (int) frequency.get(n));
      } else {
        assertTrue(n + " should have been chosen", frequency.get(n) > 0);
      }
    }
  }

  /**
   * Tests chooseRandom with include scope, no exlucde nodes.
   */
  @Test
  public void testChooseRandomInclude3() {
    String scope = "/d1";
    Map<Node, Integer> frequency = pickNodesAtRandom(200, scope, null);
    LOG.info("No node is excluded.");
    for (int i = 0; i < 5; ++i) {
      // all nodes should be more than zero
      assertTrue(dataNodes[i] + " should have been chosen.",
          frequency.get(dataNodes[i]) > 0);
    }
  }
}
