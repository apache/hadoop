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
package org.apache.hadoop.hdds.scm.net;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT;
import static org.apache.hadoop.hdds.scm.net.NetConstants.PATH_SEPARATOR_STR;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.REGION_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.DATACENTER_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.NODEGROUP_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;

import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

/** Test the network topology functions. */
@RunWith(Parameterized.class)
public class TestNetworkTopologyImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestNetworkTopologyImpl.class);
  private NetworkTopology cluster;
  private Node[] dataNodes;
  private Random random = new Random();

  public TestNetworkTopologyImpl(NodeSchema[] schemas, Node[] nodeArray) {
    NodeSchemaManager.getInstance().init(schemas, true);
    cluster = new NetworkTopologyImpl(NodeSchemaManager.getInstance());
    dataNodes = nodeArray;
    for (int i = 0; i < dataNodes.length; i++) {
      cluster.add(dataNodes[i]);
    }
  }

  @Rule
  public Timeout testTimeout = new Timeout(3000000);

  @Parameters
  public static Collection<Object[]> setupDatanodes() {
    Object[][] topologies = new Object[][]{
        {new NodeSchema[] {ROOT_SCHEMA, LEAF_SCHEMA},
            new Node[]{
                createDatanode("1.1.1.1", "/"),
                createDatanode("2.2.2.2", "/"),
                createDatanode("3.3.3.3", "/"),
                createDatanode("4.4.4.4", "/"),
                createDatanode("5.5.5.5", "/"),
                createDatanode("6.6.6.6", "/"),
                createDatanode("7.7.7.7", "/"),
                createDatanode("8.8.8.8", "/"),
            }},
        {new NodeSchema[] {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA},
            new Node[]{
                createDatanode("1.1.1.1", "/r1"),
                createDatanode("2.2.2.2", "/r1"),
                createDatanode("3.3.3.3", "/r2"),
                createDatanode("4.4.4.4", "/r2"),
                createDatanode("5.5.5.5", "/r2"),
                createDatanode("6.6.6.6", "/r3"),
                createDatanode("7.7.7.7", "/r3"),
                createDatanode("8.8.8.8", "/r3"),
            }},
        {new NodeSchema[]
            {ROOT_SCHEMA, DATACENTER_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA},
            new Node[]{
                createDatanode("1.1.1.1", "/d1/r1"),
                createDatanode("2.2.2.2", "/d1/r1"),
                createDatanode("3.3.3.3", "/d1/r2"),
                createDatanode("4.4.4.4", "/d1/r2"),
                createDatanode("5.5.5.5", "/d1/r2"),
                createDatanode("6.6.6.6", "/d2/r3"),
                createDatanode("7.7.7.7", "/d2/r3"),
                createDatanode("8.8.8.8", "/d2/r3"),
            }},
        {new NodeSchema[] {ROOT_SCHEMA, DATACENTER_SCHEMA, RACK_SCHEMA,
            NODEGROUP_SCHEMA, LEAF_SCHEMA},
            new Node[]{
                createDatanode("1.1.1.1", "/d1/r1/ng1"),
                createDatanode("2.2.2.2", "/d1/r1/ng1"),
                createDatanode("3.3.3.3", "/d1/r2/ng2"),
                createDatanode("4.4.4.4", "/d1/r2/ng2"),
                createDatanode("5.5.5.5", "/d1/r2/ng3"),
                createDatanode("6.6.6.6", "/d2/r3/ng3"),
                createDatanode("7.7.7.7", "/d2/r3/ng3"),
                createDatanode("8.8.8.8", "/d2/r3/ng3"),
                createDatanode("9.9.9.9", "/d3/r1/ng1"),
                createDatanode("10.10.10.10", "/d3/r1/ng1"),
                createDatanode("11.11.11.11", "/d3/r1/ng1"),
                createDatanode("12.12.12.12", "/d3/r2/ng2"),
                createDatanode("13.13.13.13", "/d3/r2/ng2"),
                createDatanode("14.14.14.14", "/d4/r1/ng1"),
                createDatanode("15.15.15.15", "/d4/r1/ng1"),
                createDatanode("16.16.16.16", "/d4/r1/ng1"),
                createDatanode("17.17.17.17", "/d4/r1/ng2"),
                createDatanode("18.18.18.18", "/d4/r1/ng2"),
                createDatanode("19.19.19.19", "/d4/r1/ng3"),
                createDatanode("20.20.20.20", "/d4/r1/ng3"),
            }},
        {new NodeSchema[] {ROOT_SCHEMA, REGION_SCHEMA, DATACENTER_SCHEMA,
            RACK_SCHEMA, NODEGROUP_SCHEMA, LEAF_SCHEMA},
            new Node[]{
                createDatanode("1.1.1.1", "/d1/rg1/r1/ng1"),
                createDatanode("2.2.2.2", "/d1/rg1/r1/ng1"),
                createDatanode("3.3.3.3", "/d1/rg1/r1/ng2"),
                createDatanode("4.4.4.4", "/d1/rg1/r1/ng1"),
                createDatanode("5.5.5.5", "/d1/rg1/r1/ng1"),
                createDatanode("6.6.6.6", "/d1/rg1/r1/ng2"),
                createDatanode("7.7.7.7", "/d1/rg1/r1/ng2"),
                createDatanode("8.8.8.8", "/d1/rg1/r1/ng2"),
                createDatanode("9.9.9.9", "/d1/rg1/r1/ng2"),
                createDatanode("10.10.10.10", "/d1/rg1/r1/ng2"),
                createDatanode("11.11.11.11", "/d1/rg1/r2/ng1"),
                createDatanode("12.12.12.12", "/d1/rg1/r2/ng1"),
                createDatanode("13.13.13.13", "/d1/rg1/r2/ng1"),
                createDatanode("14.14.14.14", "/d1/rg1/r2/ng1"),
                createDatanode("15.15.15.15", "/d1/rg1/r2/ng1"),
                createDatanode("16.16.16.16", "/d1/rg1/r2/ng2"),
                createDatanode("17.17.17.17", "/d1/rg1/r2/ng2"),
                createDatanode("18.18.18.18", "/d1/rg1/r2/ng2"),
                createDatanode("19.19.19.19", "/d1/rg1/r2/ng2"),
                createDatanode("20.20.20.20", "/d1/rg1/r2/ng2"),
                createDatanode("21.21.21.21", "/d2/rg1/r2/ng1"),
                createDatanode("22.22.22.22", "/d2/rg1/r2/ng1"),
                createDatanode("23.23.23.23", "/d2/rg2/r2/ng1"),
                createDatanode("24.24.24.24", "/d2/rg2/r2/ng1"),
                createDatanode("25.25.25.25", "/d2/rg2/r2/ng1"),
            }}
    };
    return Arrays.asList(topologies);
  }

  @Test
  public void testContains() {
    Node nodeNotInMap = createDatanode("8.8.8.8", "/d2/r4");
    for (int i=0; i < dataNodes.length; i++) {
      assertTrue(cluster.contains(dataNodes[i]));
    }
    assertFalse(cluster.contains(nodeNotInMap));
  }

  @Test
  public void testNumOfChildren() {
    assertEquals(dataNodes.length, cluster.getNumOfLeafNode(null));
    assertEquals(0, cluster.getNumOfLeafNode("/switch1/node1"));
  }

  @Test
  public void testGetNode() {
    assertEquals(cluster.getNode(""), cluster.getNode(null));
    assertEquals(cluster.getNode(""), cluster.getNode("/"));
    assertEquals(null, cluster.getNode("/switch1/node1"));
    assertEquals(null, cluster.getNode("/switch1"));
  }

  @Test
  public void testCreateInvalidTopology() {
    List<NodeSchema> schemas = new ArrayList<NodeSchema>();
    schemas.add(ROOT_SCHEMA);
    schemas.add(RACK_SCHEMA);
    schemas.add(LEAF_SCHEMA);
    NodeSchemaManager.getInstance().init(schemas.toArray(new NodeSchema[0]),
        true);
    NetworkTopology newCluster = new NetworkTopologyImpl(
        NodeSchemaManager.getInstance());
    Node[] invalidDataNodes = new Node[] {
        createDatanode("1.1.1.1", "/r1"),
        createDatanode("2.2.2.2", "/r2"),
        createDatanode("3.3.3.3", "/d1/r2")
    };
    newCluster.add(invalidDataNodes[0]);
    newCluster.add(invalidDataNodes[1]);
    try {
      newCluster.add(invalidDataNodes[2]);
      fail("expected InvalidTopologyException");
    } catch (NetworkTopology.InvalidTopologyException e) {
      assertTrue(e.getMessage().contains("Failed to add"));
      assertTrue(e.getMessage().contains("Its path depth is not " +
          newCluster.getMaxLevel()));
    }
  }

  @Test
  public void testInitWithConfigFile() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Configuration conf = new Configuration();
    try {
      String filePath = classLoader.getResource(
          "./networkTopologyTestFiles/good.xml").getPath();
      conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE, filePath);
      NetworkTopology newCluster = new NetworkTopologyImpl(conf);
      LOG.info("network topology max level = " + newCluster.getMaxLevel());
    } catch (Throwable e) {
      fail("should succeed");
    }
  }

  @Test
  public void testAncestor() {
    assumeTrue(cluster.getMaxLevel() > 2);
    int maxLevel = cluster.getMaxLevel();
    assertTrue(cluster.isSameParent(dataNodes[0], dataNodes[1]));
    while(maxLevel > 1) {
      assertTrue(cluster.isSameAncestor(dataNodes[0], dataNodes[1],
          maxLevel - 1));
      maxLevel--;
    }
    assertFalse(cluster.isSameParent(dataNodes[1], dataNodes[2]));
    assertFalse(cluster.isSameParent(null, dataNodes[2]));
    assertFalse(cluster.isSameParent(dataNodes[1], null));
    assertFalse(cluster.isSameParent(null, null));

    assertFalse(cluster.isSameAncestor(dataNodes[1], dataNodes[2], 0));
    assertFalse(cluster.isSameAncestor(dataNodes[1], null, 1));
    assertFalse(cluster.isSameAncestor(null, dataNodes[2], 1));
    assertFalse(cluster.isSameAncestor(null, null, 1));

    maxLevel = cluster.getMaxLevel();
    assertTrue(cluster.isSameAncestor(
        dataNodes[random.nextInt(cluster.getNumOfLeafNode(null))],
        dataNodes[random.nextInt(cluster.getNumOfLeafNode(null))],
        maxLevel - 1));
  }

  @Test
  public void testAddRemove() {
    for(int i = 0; i < dataNodes.length; i++) {
      cluster.remove(dataNodes[i]);
    }
    for(int i = 0; i < dataNodes.length; i++) {
      assertFalse(cluster.contains(dataNodes[i]));
    }
    // no leaf nodes
    assertEquals(0, cluster.getNumOfLeafNode(null));
    // no inner nodes
    assertEquals(0, cluster.getNumOfNodes(2));
    for(int i = 0; i < dataNodes.length; i++) {
      cluster.add(dataNodes[i]);
    }
    // Inner nodes are created automatically
    assertTrue(cluster.getNumOfNodes(2) > 0);

    try {
      cluster.add(cluster.chooseRandom(null).getParent());
      fail("Inner node can not be added manually");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith(
          "Not allowed to add an inner node"));
    }

    try {
      cluster.remove(cluster.chooseRandom(null).getParent());
      fail("Inner node can not be removed manually");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith(
          "Not allowed to remove an inner node"));
    }
  }

  @Test
  public void testGetNodesWithLevel() {
    int maxLevel = cluster.getMaxLevel();
    try {
      assertEquals(1, cluster.getNumOfNodes(0));
      fail("level 0 is not supported");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().startsWith("Invalid level"));
    }

    try {
      assertEquals(1, cluster.getNumOfNodes(0));
      fail("level 0 is not supported");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().startsWith("Invalid level"));
    }

    try {
      assertEquals(1, cluster.getNumOfNodes(maxLevel + 1));
      fail("level out of scope");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().startsWith("Invalid level"));
    }

    try {
      assertEquals(1, cluster.getNumOfNodes(maxLevel + 1));
      fail("level out of scope");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().startsWith("Invalid level"));
    }
    // root node
    assertEquals(1, cluster.getNumOfNodes(1));
    assertEquals(1, cluster.getNumOfNodes(1));
    // leaf nodes
    assertEquals(dataNodes.length, cluster.getNumOfNodes(maxLevel));
    assertEquals(dataNodes.length, cluster.getNumOfNodes(maxLevel));
  }

  @Test
  public void testChooseRandomSimple() {
    String path =
        dataNodes[random.nextInt(dataNodes.length)].getNetworkFullPath();
    assertEquals(path, cluster.chooseRandom(path).getNetworkFullPath());
    path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR));
    // test chooseRandom(String scope)
    while (!path.equals(ROOT)) {
      assertTrue(cluster.chooseRandom(path).getNetworkLocation()
          .startsWith(path));
      Node node = cluster.chooseRandom("~" + path);
      assertTrue(!node.getNetworkLocation()
          .startsWith(path));
      path = path.substring(0,
          path.lastIndexOf(PATH_SEPARATOR_STR));
    }
    assertNotNull(cluster.chooseRandom(null));
    assertNotNull(cluster.chooseRandom(""));
    assertNotNull(cluster.chooseRandom("/"));
    assertNull(cluster.chooseRandom("~"));
    assertNull(cluster.chooseRandom("~/"));

    // test chooseRandom(String scope, String excludedScope)
    path = dataNodes[random.nextInt(dataNodes.length)].getNetworkFullPath();
    assertNull(cluster.chooseRandom(path, path));
    assertNotNull(cluster.chooseRandom(null, path));
    assertNotNull(cluster.chooseRandom("", path));

    // test chooseRandom(String scope, Collection<Node> excludedNodes)
    assertNull(cluster.chooseRandom("", Arrays.asList(dataNodes)));
    assertNull(cluster.chooseRandom("/", Arrays.asList(dataNodes)));
    assertNull(cluster.chooseRandom("~", Arrays.asList(dataNodes)));
    assertNull(cluster.chooseRandom("~/", Arrays.asList(dataNodes)));
    assertNull(cluster.chooseRandom(null, Arrays.asList(dataNodes)));
  }

  /**
   * Following test checks that chooseRandom works for an excluded scope.
   */
  @Test
  public void testChooseRandomExcludedScope() {
    int[] excludedNodeIndexs = {0, dataNodes.length - 1,
        random.nextInt(dataNodes.length), random.nextInt(dataNodes.length)};
    String scope;
    Map<Node, Integer> frequency;
    for (int i : excludedNodeIndexs) {
      String path = dataNodes[i].getNetworkFullPath();
      while (!path.equals(ROOT)) {
        scope = "~" + path;
        frequency = pickNodesAtRandom(100, scope, null, 0);
        for (Node key : dataNodes) {
          if (key.getNetworkFullPath().startsWith(path)) {
            assertTrue(frequency.get(key) == 0);
          }
        }
        path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR));
      }
    }

    // null excludedScope, every node should be chosen
    frequency = pickNodes(100, null, null, null, 0);
    for (Node key : dataNodes) {
      assertTrue(frequency.get(key) != 0);
    }

    // "" excludedScope,  no node will ever be chosen
    frequency = pickNodes(100, "", null, null, 0);
    for (Node key : dataNodes) {
      assertTrue(frequency.get(key) == 0);
    }

    // "~" scope, no node will ever be chosen
    scope = "~";
    frequency = pickNodesAtRandom(100, scope, null, 0);
    for (Node key : dataNodes) {
      assertTrue(frequency.get(key) == 0);
    }
    // out network topology excluded scope, every node should be chosen
    scope = "/city1";
    frequency = pickNodes(cluster.getNumOfLeafNode(null), scope, null, null, 0);
    for (Node key : dataNodes) {
      assertTrue(frequency.get(key) != 0);
    }
  }

  /**
   * Following test checks that chooseRandom works for an excluded nodes.
   */
  @Test
  public void testChooseRandomExcludedNode() {
    Node[][] excludedNodeLists = {
        {},
        {dataNodes[0]},
        {dataNodes[dataNodes.length - 1]},
        {dataNodes[random.nextInt(dataNodes.length)]},
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)]
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        }};
    int leafNum = cluster.getNumOfLeafNode(null);
    Map<Node, Integer> frequency;
    for(Node[] list : excludedNodeLists) {
      List<Node> excludedList = Arrays.asList(list);
      int ancestorGen = 0;
      while(ancestorGen < cluster.getMaxLevel()) {
        frequency = pickNodesAtRandom(leafNum, null, excludedList, ancestorGen);
        List<Node> ancestorList = NetUtils.getAncestorList(cluster,
            excludedList, ancestorGen);
        for (Node key : dataNodes) {
          if (excludedList.contains(key) ||
              (ancestorList.size() > 0 &&
                  ancestorList.stream()
                      .map(a -> (InnerNode) a)
                      .filter(a -> a.isAncestor(key))
                      .collect(Collectors.toList()).size() > 0)) {
            assertTrue(frequency.get(key) == 0);
          }
        }
        ancestorGen++;
      }
    }
    // all nodes excluded, no node will be picked
    List<Node> excludedList = Arrays.asList(dataNodes);
    int ancestorGen = 0;
    while(ancestorGen < cluster.getMaxLevel()) {
      frequency = pickNodesAtRandom(leafNum, null, excludedList, ancestorGen);
      for (Node key : dataNodes) {
        assertTrue(frequency.get(key) == 0);
      }
      ancestorGen++;
    }
    // out scope excluded nodes, each node will be picked
    excludedList = Arrays.asList(createDatanode("1.1.1.1.", "/city1/rack1"));
    ancestorGen = 0;
    while(ancestorGen < cluster.getMaxLevel()) {
      frequency = pickNodes(leafNum, null, excludedList, null, ancestorGen);
      for (Node key : dataNodes) {
        assertTrue(frequency.get(key) != 0);
      }
      ancestorGen++;
    }
  }

  /**
   * Following test checks that chooseRandom works for excluded nodes and scope.
   */
  @Test
  public void testChooseRandomExcludedNodeAndScope() {
    int[] excludedNodeIndexs = {0, dataNodes.length - 1,
        random.nextInt(dataNodes.length), random.nextInt(dataNodes.length)};
    Node[][] excludedNodeLists = {
        {},
        {dataNodes[0]},
        {dataNodes[dataNodes.length - 1]},
        {dataNodes[random.nextInt(dataNodes.length)]},
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)]
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        }};
    int leafNum = cluster.getNumOfLeafNode(null);
    Map<Node, Integer> frequency;
    String scope;
    for (int i : excludedNodeIndexs) {
      String path = dataNodes[i].getNetworkFullPath();
      while (!path.equals(ROOT)) {
        scope = "~" + path;
        int ancestorGen = 0;
        while(ancestorGen < cluster.getMaxLevel()) {
          for (Node[] list : excludedNodeLists) {
            List<Node> excludedList = Arrays.asList(list);
            frequency =
                pickNodesAtRandom(leafNum, scope, excludedList, ancestorGen);
            List<Node> ancestorList = NetUtils.getAncestorList(cluster,
                excludedList, ancestorGen);
            for (Node key : dataNodes) {
              if (excludedList.contains(key) ||
                  key.getNetworkFullPath().startsWith(path) ||
                  (ancestorList.size() > 0 &&
                      ancestorList.stream()
                          .map(a -> (InnerNode) a)
                          .filter(a -> a.isAncestor(key))
                          .collect(Collectors.toList()).size() > 0)) {
                assertTrue(frequency.get(key) == 0);
              }
            }
          }
          ancestorGen++;
        }
        path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR));
      }
    }
    // all nodes excluded, no node will be picked
    List<Node> excludedList = Arrays.asList(dataNodes);
    for (int i : excludedNodeIndexs) {
      String path = dataNodes[i].getNetworkFullPath();
      while (!path.equals(ROOT)) {
        scope = "~" + path;
        int ancestorGen = 0;
        while (ancestorGen < cluster.getMaxLevel()) {
          frequency =
              pickNodesAtRandom(leafNum, scope, excludedList, ancestorGen);
          for (Node key : dataNodes) {
            assertTrue(frequency.get(key) == 0);
          }
          ancestorGen++;
        }
        path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR));
      }
    }

    // no node excluded and no excluded scope, each node will be picked
    int ancestorGen = 0;
    while (ancestorGen < cluster.getMaxLevel()) {
      frequency = pickNodes(leafNum, null, null, null, ancestorGen);
      for (Node key : dataNodes) {
        assertTrue(frequency.get(key) != 0);
      }
      ancestorGen++;
    }
  }

  /**
   * Following test checks that chooseRandom works for excluded nodes, scope
   * and ancestor generation.
   */
  @Test
  public void testChooseRandomWithAffinityNode() {
    int[] excludedNodeIndexs = {0, dataNodes.length - 1,
        random.nextInt(dataNodes.length), random.nextInt(dataNodes.length)};
    Node[][] excludedNodeLists = {
        {},
        {dataNodes[0]},
        {dataNodes[dataNodes.length - 1]},
        {dataNodes[random.nextInt(dataNodes.length)]},
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)]
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        }};
    int[] affinityNodeIndexs = {0, dataNodes.length - 1,
        random.nextInt(dataNodes.length), random.nextInt(dataNodes.length)};
    int leafNum = cluster.getNumOfLeafNode(null);
    Map<Node, Integer> frequency;
    String scope;
    for (int k : affinityNodeIndexs) {
      for (int i : excludedNodeIndexs) {
        String path = dataNodes[i].getNetworkFullPath();
        while (!path.equals(ROOT)) {
          int ancestorGen = cluster.getMaxLevel() - 1;
          while (ancestorGen > 0) {
            for (Node[] list : excludedNodeLists) {
              List<Node> excludedList = Arrays.asList(list);
              frequency = pickNodes(leafNum, path, excludedList, dataNodes[k],
                  ancestorGen);
              Node affinityAncestor = dataNodes[k].getAncestor(ancestorGen);
              for (Node key : dataNodes) {
                if (affinityAncestor != null) {
                  if (frequency.get(key) > 0) {
                    assertTrue(affinityAncestor.isAncestor(key));
                  } else if (!affinityAncestor.isAncestor(key)) {
                    continue;
                  } else if (excludedList != null &&
                      excludedList.contains(key)) {
                    continue;
                  } else if (path != null &&
                      key.getNetworkFullPath().startsWith(path)) {
                    continue;
                  } else {
                    fail("Node is not picked when sequentially going " +
                        "through ancestor node's leaf nodes. node:" +
                        key.getNetworkFullPath() + ", ancestor node:" +
                        affinityAncestor.getNetworkFullPath() +
                        ", excludedScope: " + path + ", " + "excludedList:" +
                        (excludedList == null ? "" : excludedList.toString()));
                  }
                }
              }
            }
            ancestorGen--;
          }
          path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR));
        }
      }
    }

    // all nodes excluded, no node will be picked
    List<Node> excludedList = Arrays.asList(dataNodes);
    for (int k : affinityNodeIndexs) {
      for (int i : excludedNodeIndexs) {
        String path = dataNodes[i].getNetworkFullPath();
        while (!path.equals(ROOT)) {
          scope = "~" + path;
          int ancestorGen = 0;
          while (ancestorGen < cluster.getMaxLevel()) {
            frequency = pickNodesAtRandom(leafNum, scope, excludedList,
                dataNodes[k], ancestorGen);
            for (Node key : dataNodes) {
              assertTrue(frequency.get(key) == 0);
            }
            ancestorGen++;
          }
          path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR));
        }
      }
    }
    // no node excluded and no excluded scope, each node will be picked
    int ancestorGen = cluster.getMaxLevel() - 1;
    for (int k : affinityNodeIndexs) {
      while (ancestorGen > 0) {
        frequency =
            pickNodes(leafNum, null, null, dataNodes[k], ancestorGen);
        Node affinityAncestor = dataNodes[k].getAncestor(ancestorGen);
        for (Node key : dataNodes) {
          if (frequency.get(key) > 0) {
            if (affinityAncestor != null) {
              assertTrue(affinityAncestor.isAncestor(key));
            }
          }
        }
        ancestorGen--;
      }
    }
    // check invalid ancestor generation
    try {
      cluster.chooseRandom(null, null, null, dataNodes[0],
          cluster.getMaxLevel());
      fail("ancestor generation exceeds max level, should fail");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("ancestorGen " +
          cluster.getMaxLevel() +
          " exceeds this network topology acceptable level"));
    }
  }

  @Test
  public void testCost() {
    // network topology with default cost
    List<NodeSchema> schemas = new ArrayList<>();
    schemas.add(ROOT_SCHEMA);
    schemas.add(RACK_SCHEMA);
    schemas.add(NODEGROUP_SCHEMA);
    schemas.add(LEAF_SCHEMA);

    NodeSchemaManager manager = NodeSchemaManager.getInstance();
    manager.init(schemas.toArray(new NodeSchema[0]), true);
    NetworkTopology newCluster =
        new NetworkTopologyImpl(manager);
    Node[] nodeList = new Node[] {
        createDatanode("1.1.1.1", "/r1/ng1"),
        createDatanode("2.2.2.2", "/r1/ng1"),
        createDatanode("3.3.3.3", "/r1/ng2"),
        createDatanode("4.4.4.4", "/r2/ng1"),
    };
    for (Node node: nodeList) {
      newCluster.add(node);
    }
    Node outScopeNode1 = createDatanode("5.5.5.5", "/r2/ng2");
    Node outScopeNode2 = createDatanode("6.6.6.6", "/r2/ng2");
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(nodeList[0], null));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(null, nodeList[0]));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(outScopeNode1, nodeList[0]));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(nodeList[0], outScopeNode1));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(outScopeNode1, outScopeNode2));

    assertEquals(0, newCluster.getDistanceCost(null, null));
    assertEquals(0, newCluster.getDistanceCost(nodeList[0], nodeList[0]));
    assertEquals(2, newCluster.getDistanceCost(nodeList[0], nodeList[1]));
    assertEquals(4, newCluster.getDistanceCost(nodeList[0], nodeList[2]));
    assertEquals(6, newCluster.getDistanceCost(nodeList[0], nodeList[3]));

    // network topology with customized cost
    schemas.clear();
    schemas.add(new NodeSchema.Builder()
        .setType(NodeSchema.LayerType.ROOT).setCost(5).build());
    schemas.add(new NodeSchema.Builder()
        .setType(NodeSchema.LayerType.INNER_NODE).setCost(3).build());
    schemas.add(new NodeSchema.Builder()
        .setType(NodeSchema.LayerType.INNER_NODE).setCost(1).build());
    schemas.add(new NodeSchema.Builder()
        .setType(NodeSchema.LayerType.LEAF_NODE).build());
    manager = NodeSchemaManager.getInstance();
    manager.init(schemas.toArray(new NodeSchema[0]), true);
    newCluster = new NetworkTopologyImpl(manager);
    for (Node node: nodeList) {
      newCluster.add(node);
    }
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(nodeList[0], null));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(null, nodeList[0]));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(outScopeNode1, nodeList[0]));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(nodeList[0], outScopeNode1));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(outScopeNode1, outScopeNode2));

    assertEquals(0, newCluster.getDistanceCost(null, null));
    assertEquals(0, newCluster.getDistanceCost(nodeList[0], nodeList[0]));
    assertEquals(2, newCluster.getDistanceCost(nodeList[0], nodeList[1]));
    assertEquals(8, newCluster.getDistanceCost(nodeList[0], nodeList[2]));
    assertEquals(18, newCluster.getDistanceCost(nodeList[0], nodeList[3]));
  }

  @Test
  public void testSortByDistanceCost() {
    Node[][] nodes = {
        {},
        {dataNodes[0]},
        {dataNodes[dataNodes.length - 1]},
        {dataNodes[random.nextInt(dataNodes.length)]},
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)]
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        }};
    Node[] readers = {null, dataNodes[0], dataNodes[dataNodes.length - 1],
        dataNodes[random.nextInt(dataNodes.length)],
        dataNodes[random.nextInt(dataNodes.length)],
        dataNodes[random.nextInt(dataNodes.length)]
    };
    for (Node reader : readers) {
      for (Node[] nodeList : nodes) {
        int length = nodeList.length;
        while (length > 0) {
          cluster.sortByDistanceCost(reader, nodeList, length);
          for (int i = 0; i < nodeList.length; i++) {
            if ((i + 1) < nodeList.length) {
              int cost1 = cluster.getDistanceCost(reader, nodeList[i]);
              int cost2 = cluster.getDistanceCost(reader, nodeList[i + 1]);
              assertTrue("reader:" + (reader != null ?
                  reader.getNetworkFullPath() : "null") +
                  ",node1:" + nodeList[i].getNetworkFullPath() +
                  ",node2:" + nodeList[i + 1].getNetworkFullPath() +
                  ",cost1:" + cost1 + ",cost2:" + cost2,
                  cost1 == Integer.MAX_VALUE || cost1 <= cost2);
            }
          }
          length--;
        }
      }
    }

    // sort all nodes
    Node[] nodeList = dataNodes.clone();
    for (Node reader : readers) {
      int length = nodeList.length;
      while (length >= 0) {
        cluster.sortByDistanceCost(reader, nodeList, length);
        for (int i = 0; i < nodeList.length; i++) {
          if ((i + 1) < nodeList.length) {
            int cost1 = cluster.getDistanceCost(reader, nodeList[i]);
            int cost2 = cluster.getDistanceCost(reader, nodeList[i + 1]);
            // node can be removed when called in testConcurrentAccess
            assertTrue("reader:" + (reader != null ?
                reader.getNetworkFullPath() : "null") +
                ",node1:" + nodeList[i].getNetworkFullPath() +
                ",node2:" + nodeList[i + 1].getNetworkFullPath() +
                ",cost1:" + cost1 + ",cost2:" + cost2,
                cost1 == Integer.MAX_VALUE || cost1 <= cost2);
          }
        }
        length--;
      }
    }
  }

  private static Node createDatanode(String name, String path) {
    return new NodeImpl(name, path, NetConstants.NODE_COST_DEFAULT);
  }

  /**
   * This picks a large number of nodes at random in order to ensure coverage.
   *
   * @param numNodes the number of nodes
   * @param excludedScope the excluded scope
   * @param excludedNodes the excluded node list
   * @param ancestorGen the chosen node cannot share the same ancestor at
   *                    this generation with excludedNodes
   * @return the frequency that nodes were chosen
   */
  private Map<Node, Integer> pickNodesAtRandom(int numNodes,
      String excludedScope, Collection<Node> excludedNodes, int ancestorGen) {
    Map<Node, Integer> frequency = new HashMap<Node, Integer>();
    for (Node dnd : dataNodes) {
      frequency.put(dnd, 0);
    }
    for (int j = 0; j < numNodes; j++) {
      Node node = cluster.chooseRandom(excludedScope, excludedNodes,
          ancestorGen);
      if (node != null) {
        frequency.put(node, frequency.get(node) + 1);
      }
    }
    LOG.info("Result:" + frequency);
    return frequency;
  }

  /**
   * This picks a large number of nodes at random in order to ensure coverage.
   *
   * @param numNodes the number of nodes
   * @param excludedScope the excluded scope
   * @param excludedNodes the excluded node list
   * @param affinityNode the chosen node should share the same ancestor at
   *                     generation "ancestorGen" with this node
   * @param ancestorGen  the chosen node cannot share the same ancestor at
   *                     this generation with excludedNodes
   * @return the frequency that nodes were chosen
   */
  private Map<Node, Integer> pickNodesAtRandom(int numNodes,
      String excludedScope, Collection<Node> excludedNodes, Node affinityNode,
      int ancestorGen) {
    Map<Node, Integer> frequency = new HashMap<Node, Integer>();
    for (Node dnd : dataNodes) {
      frequency.put(dnd, 0);
    }

    for (int j = 0; j < numNodes; j++) {
      Node node = cluster.chooseRandom("", excludedScope.substring(1),
          excludedNodes, affinityNode, ancestorGen);
      if (node != null) {
        frequency.put(node, frequency.get(node) + 1);
      }
    }
    LOG.info("Result:" + frequency);
    return frequency;
  }

  /**
   * This picks a large amount of nodes sequentially.
   *
   * @param numNodes the number of nodes
   * @param excludedScope the excluded scope, should not start with "~"
   * @param excludedNodes the excluded node list
   * @param affinityNode the chosen node should share the same ancestor at
   *                     generation "ancestorGen" with this node
   * @param ancestorGen  the chosen node cannot share the same ancestor at
   *                     this generation with excludedNodes
   * @return the frequency that nodes were chosen
   */
  private Map<Node, Integer> pickNodes(int numNodes, String excludedScope,
      Collection<Node> excludedNodes, Node affinityNode, int ancestorGen) {
    Map<Node, Integer> frequency = new HashMap<>();
    for (Node dnd : dataNodes) {
      frequency.put(dnd, 0);
    }
    excludedNodes = excludedNodes == null ? null :
        excludedNodes.stream().distinct().collect(Collectors.toList());
    for (int j = 0; j < numNodes; j++) {
      Node node = cluster.getNode(j, null, excludedScope, excludedNodes,
          affinityNode, ancestorGen);
      if (node != null) {
        frequency.put(node, frequency.get(node) + 1);
      }
    }

    LOG.info("Result:" + frequency);
    return frequency;
  }
}
