/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.when;

/**
 * Test for the scm container rack aware placement.
 */
public class TestSCMContainerPlacementRackAware {
  private NetworkTopology cluster;
  private Configuration conf;
  private NodeManager nodeManager;
  private List<DatanodeDetails> datanodes = new ArrayList<>();
  // policy with fallback capability
  private SCMContainerPlacementRackAware policy;
  // policy prohibit fallback
  private SCMContainerPlacementRackAware policyNoFallback;
  // node storage capacity
  private static final long STORAGE_CAPACITY = 100L;

  @Before
  public void setup() {
    //initialize network topology instance
    conf = new OzoneConfiguration();
    NodeSchema[] schemas = new NodeSchema[]
        {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    cluster = new NetworkTopologyImpl(NodeSchemaManager.getInstance());

    // build datanodes, and network topology
    String rack = "/rack";
    String hostname = "node";
    for (int i = 0; i < 15; i++) {
      // Totally 3 racks, each has 5 datanodes
      DatanodeDetails node = TestUtils.createDatanodeDetails(
          hostname + i, rack + (i / 5));
      datanodes.add(node);
      cluster.add(node);
    }

    // create mock node manager
    nodeManager = Mockito.mock(NodeManager.class);
    when(nodeManager.getNodes(NodeState.HEALTHY))
        .thenReturn(new ArrayList<>(datanodes));
    when(nodeManager.getNodeStat(anyObject()))
        .thenReturn(new SCMNodeMetric(STORAGE_CAPACITY, 0L, 100L));
    when(nodeManager.getNodeStat(datanodes.get(2)))
        .thenReturn(new SCMNodeMetric(STORAGE_CAPACITY, 90L, 10L));
    when(nodeManager.getNodeStat(datanodes.get(3)))
        .thenReturn(new SCMNodeMetric(STORAGE_CAPACITY, 80L, 20L));
    when(nodeManager.getNodeStat(datanodes.get(4)))
        .thenReturn(new SCMNodeMetric(STORAGE_CAPACITY, 70L, 30L));

    // create placement policy instances
    policy =
        new SCMContainerPlacementRackAware(nodeManager, conf, cluster, true);
    policyNoFallback =
        new SCMContainerPlacementRackAware(nodeManager, conf, cluster, false);
  }


  @Test
  public void chooseNodeWithNoExcludedNodes() throws SCMException {
    // test choose new datanodes for new pipeline cases
    // 1 replica
    int nodeNum = 1;
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(null, null, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());

    // 2 replicas
    nodeNum = 2;
    datanodeDetails = policy.chooseDatanodes(null, null, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));

    //  3 replicas
    nodeNum = 3;
    datanodeDetails = policy.chooseDatanodes(null, null, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    Assert.assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    Assert.assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));

    //  4 replicas
    nodeNum = 4;
    datanodeDetails = policy.chooseDatanodes(null, null, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    Assert.assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    Assert.assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
  }

  @Test
  public void chooseNodeWithExcludedNodes() throws SCMException {
    // test choose new datanodes for under replicated pipeline
    // 3 replicas, two existing datanodes on same rack
    int nodeNum = 1;
    List<DatanodeDetails> excludedNodes = new ArrayList<>();

    excludedNodes.add(datanodes.get(0));
    excludedNodes.add(datanodes.get(1));
    List<DatanodeDetails> datanodeDetails = policy.chooseDatanodes(
        excludedNodes, null, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        excludedNodes.get(0)));
    Assert.assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        excludedNodes.get(1)));

    // 3 replicas, two existing datanodes on different rack
    excludedNodes.clear();
    excludedNodes.add(datanodes.get(0));
    excludedNodes.add(datanodes.get(7));
    datanodeDetails = policy.chooseDatanodes(
        excludedNodes, null, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertTrue(cluster.isSameParent(
        datanodeDetails.get(0), excludedNodes.get(0)) ||
        cluster.isSameParent(datanodeDetails.get(0), excludedNodes.get(1)));

    // 3 replicas, one existing datanode
    nodeNum = 2;
    excludedNodes.clear();
    excludedNodes.add(datanodes.get(0));
    datanodeDetails = policy.chooseDatanodes(
        excludedNodes, null, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertTrue(cluster.isSameParent(
        datanodeDetails.get(0), excludedNodes.get(0)) ||
        cluster.isSameParent(datanodeDetails.get(0), excludedNodes.get(1)));
  }

  @Test
  public void testFallback() throws SCMException {

    // 5 replicas. there are only 3 racks. policy with fallback should
    // allocate the 5th datanode though it will break the rack rule(first
    // 2 replicas on same rack, others on different racks).
    int nodeNum = 5;
    List<DatanodeDetails>  datanodeDetails =
        policy.chooseDatanodes(null, null, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    Assert.assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    Assert.assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
  }


  @Test(expected = SCMException.class)
  public void testNoFallback() throws SCMException {
    // 5 replicas. there are only 3 racks. policy prohibit fallback should fail.
    int nodeNum = 5;
    policyNoFallback.chooseDatanodes(null, null, nodeNum, 15);
  }

  @Test
  public void chooseNodeWithFavoredNodes() throws SCMException {
    int nodeNum = 1;
    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    List<DatanodeDetails> favoredNodes = new ArrayList<>();

    // no excludedNodes, only favoredNodes
    favoredNodes.add(datanodes.get(0));
    List<DatanodeDetails> datanodeDetails = policy.chooseDatanodes(
        excludedNodes, favoredNodes, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertTrue(datanodeDetails.get(0).getNetworkFullPath()
        .equals(favoredNodes.get(0).getNetworkFullPath()));

    // no overlap between excludedNodes and favoredNodes, favoredNodes can been
    // chosen.
    excludedNodes.clear();
    favoredNodes.clear();
    excludedNodes.add(datanodes.get(0));
    favoredNodes.add(datanodes.get(2));
    datanodeDetails = policy.chooseDatanodes(
        excludedNodes, favoredNodes, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertTrue(datanodeDetails.get(0).getNetworkFullPath()
        .equals(favoredNodes.get(0).getNetworkFullPath()));

    // there is overlap between excludedNodes and favoredNodes, favoredNodes
    // should not be chosen.
    excludedNodes.clear();
    favoredNodes.clear();
    excludedNodes.add(datanodes.get(0));
    favoredNodes.add(datanodes.get(0));
    datanodeDetails = policy.chooseDatanodes(
        excludedNodes, favoredNodes, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertFalse(datanodeDetails.get(0).getNetworkFullPath()
        .equals(favoredNodes.get(0).getNetworkFullPath()));
  }

  @Test(expected = SCMException.class)
  public void testNoInfiniteLoop() throws SCMException {
    int nodeNum = 1;
    // request storage space larger than node capability
    policy.chooseDatanodes(null, null, nodeNum, STORAGE_CAPACITY + 15);
  }

  @Test
  public void testDatanodeWithDefaultNetworkLocation() throws SCMException {
    String hostname = "node";
    List<DatanodeDetails> dataList = new ArrayList<>();
    NetworkTopology clusterMap =
        new NetworkTopologyImpl(NodeSchemaManager.getInstance());
    for (int i = 0; i < 15; i++) {
      // Totally 3 racks, each has 5 datanodes
      DatanodeDetails node = TestUtils.createDatanodeDetails(
          hostname + i, null);
      dataList.add(node);
      clusterMap.add(node);
    }
    Assert.assertEquals(dataList.size(), StringUtils.countMatches(
        clusterMap.toString(), NetConstants.DEFAULT_RACK));

    // choose nodes to host 3 replica
    int nodeNum = 3;
    SCMContainerPlacementRackAware newPolicy =
        new SCMContainerPlacementRackAware(nodeManager, conf, clusterMap, true);
    List<DatanodeDetails> datanodeDetails =
        newPolicy.chooseDatanodes(null, null, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    Assert.assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    Assert.assertTrue(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
  }
}