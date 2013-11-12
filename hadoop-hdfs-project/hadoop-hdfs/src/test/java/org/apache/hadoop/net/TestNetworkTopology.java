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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNetworkTopology {
  private static final Log LOG = LogFactory.getLog(TestNetworkTopology.class);
  private final static NetworkTopology cluster = new NetworkTopology();
  private DatanodeDescriptor dataNodes[];
  
  @Before
  public void setupDatanodes() {
    dataNodes = new DatanodeDescriptor[] {
        DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r2"),
        DFSTestUtil.getDatanodeDescriptor("4.4.4.4", "/d1/r2"),
        DFSTestUtil.getDatanodeDescriptor("5.5.5.5", "/d1/r2"),
        DFSTestUtil.getDatanodeDescriptor("6.6.6.6", "/d2/r3"),
        DFSTestUtil.getDatanodeDescriptor("7.7.7.7", "/d2/r3")
    };
    for (int i = 0; i < dataNodes.length; i++) {
      cluster.add(dataNodes[i]);
    }
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
    NetworkTopology invalCluster = new NetworkTopology();
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
    assertEquals(cluster.getNumOfRacks(), 3);
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
  }

  @Test
  public void testPseudoSortByDistance() throws Exception {
    DatanodeDescriptor[] testNodes = new DatanodeDescriptor[3];
    
    // array contains both local node & local rack node
    testNodes[0] = dataNodes[1];
    testNodes[1] = dataNodes[2];
    testNodes[2] = dataNodes[0];
    cluster.pseudoSortByDistance(dataNodes[0], testNodes );
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[1]);
    assertTrue(testNodes[2] == dataNodes[2]);

    // array contains local node
    testNodes[0] = dataNodes[1];
    testNodes[1] = dataNodes[3];
    testNodes[2] = dataNodes[0];
    cluster.pseudoSortByDistance(dataNodes[0], testNodes );
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[1]);
    assertTrue(testNodes[2] == dataNodes[3]);

    // array contains local rack node
    testNodes[0] = dataNodes[5];
    testNodes[1] = dataNodes[3];
    testNodes[2] = dataNodes[1];
    cluster.pseudoSortByDistance(dataNodes[0], testNodes );
    assertTrue(testNodes[0] == dataNodes[1]);
    assertTrue(testNodes[1] == dataNodes[3]);
    assertTrue(testNodes[2] == dataNodes[5]);
    
    // array contains local rack node which happens to be in position 0
    testNodes[0] = dataNodes[1];
    testNodes[1] = dataNodes[5];
    testNodes[2] = dataNodes[3];
    cluster.pseudoSortByDistance(dataNodes[0], testNodes );
    // peudoSortByDistance does not take the "data center" layer into consideration 
    // and it doesn't sort by getDistance, so 1, 5, 3 is also valid here
    assertTrue(testNodes[0] == dataNodes[1]);
    assertTrue(testNodes[1] == dataNodes[5]);
    assertTrue(testNodes[2] == dataNodes[3]);
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
   * This test checks that chooseRandom works for an excluded rack.
   */
  @Test
  public void testChooseRandomExcludedRack() {
    Map<Node, Integer> frequency = pickNodesAtRandom(100, "~" + "/d2");
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

}
