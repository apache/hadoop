/*
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

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.*;
import org.apache.hadoop.hdds.scm.node.states.Node2PipelineMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Test for PipelinePlacementPolicy.
 */
public class TestPipelinePlacementPolicy {
  private MockNodeManager nodeManager;
  private PipelinePlacementPolicy placementPolicy;
  private static final int PIPELINE_PLACEMENT_MAX_NODES_COUNT = 10;

  @Before
  public void init() throws Exception {
    nodeManager = new MockNodeManager(true,
        PIPELINE_PLACEMENT_MAX_NODES_COUNT);
    placementPolicy =
        new PipelinePlacementPolicy(nodeManager, new OzoneConfiguration());
  }

  @Test
  public void testChooseNodeBasedOnNetworkTopology() {
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY);
    DatanodeDetails anchor = placementPolicy.chooseNode(healthyNodes);
    // anchor should be removed from healthyNodes after being chosen.
    Assert.assertFalse(healthyNodes.contains(anchor));

    List<DatanodeDetails> excludedNodes =
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT);
    DatanodeDetails nextNode = placementPolicy.chooseNodeFromNetworkTopology(
        nodeManager.getClusterNetworkTopologyMap(), anchor, excludedNodes);
    // excludedNodes should contain nextNode after being chosen.
    Assert.assertTrue(excludedNodes.contains(nextNode));
    // nextNode should not be the same as anchor.
    Assert.assertTrue(anchor.getUuid() != nextNode.getUuid());
  }

  @Test
  public void testChooseNodeBasedOnRackAwareness() {
    List<DatanodeDetails> healthyNodes = overWriteLocationInNodes(
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY));
    DatanodeDetails anchor = placementPolicy.chooseNode(healthyNodes);
    NetworkTopology topologyWithDifRacks =
        createNetworkTopologyOnDifRacks();
    DatanodeDetails nextNode = placementPolicy.chooseNodeBasedOnRackAwareness(
        healthyNodes, new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
        topologyWithDifRacks, anchor);
    Assert.assertFalse(topologyWithDifRacks.isSameParent(anchor, nextNode));
  }

  private final static Node[] NODES = new NodeImpl[] {
      new NodeImpl("h1", "/r1", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h2", "/r1", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h3", "/r1", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h4", "/r1", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h5", "/r2", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h6", "/r2", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h7", "/r2", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h8", "/r2", NetConstants.NODE_COST_DEFAULT),
  };


  private NetworkTopology createNetworkTopologyOnDifRacks() {
    NetworkTopology topology = new NetworkTopologyImpl(new Configuration());
    for (Node n : NODES) {
      topology.add(n);
    }
    return topology;
  }

  private List<DatanodeDetails> overWriteLocationInNodes(
      List<DatanodeDetails> datanodes) {
    List<DatanodeDetails> results = new ArrayList<>(datanodes.size());
    for (int i = 0; i < datanodes.size(); i++) {
      DatanodeDetails datanode = datanodes.get(i);
      DatanodeDetails result = DatanodeDetails.newBuilder()
          .setUuid(datanode.getUuidString())
          .setHostName(datanode.getHostName())
          .setIpAddress(datanode.getIpAddress())
          .addPort(datanode.getPort(DatanodeDetails.Port.Name.STANDALONE))
          .addPort(datanode.getPort(DatanodeDetails.Port.Name.RATIS))
          .addPort(datanode.getPort(DatanodeDetails.Port.Name.REST))
          .setNetworkLocation(NODES[i].getNetworkLocation()).build();
      results.add(result);
    }
    return results;
  }

  @Test
  public void testHeavyNodeShouldBeExcluded() throws SCMException{
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY);
    int nodesRequired = healthyNodes.size()/2;
    // only minority of healthy NODES are heavily engaged in pipelines.
    int minorityHeavy = healthyNodes.size()/2 - 1;
    List<DatanodeDetails> pickedNodes1 = placementPolicy.chooseDatanodes(
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
        nodesRequired, 0);
    // modify node to pipeline mapping.
    insertHeavyNodesIntoNodeManager(healthyNodes, minorityHeavy);
    // NODES should be sufficient.
    Assert.assertEquals(nodesRequired, pickedNodes1.size());
    // make sure pipeline placement policy won't select duplicated NODES.
    Assert.assertTrue(checkDuplicateNodesUUID(pickedNodes1));

    // majority of healthy NODES are heavily engaged in pipelines.
    int majorityHeavy = healthyNodes.size()/2 + 2;
    insertHeavyNodesIntoNodeManager(healthyNodes, majorityHeavy);
    boolean thrown = false;
    List<DatanodeDetails> pickedNodes2 = null;
    try {
      pickedNodes2 = placementPolicy.chooseDatanodes(
          new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
          new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
          nodesRequired, 0);
    } catch (SCMException e) {
      Assert.assertFalse(thrown);
      thrown = true;
    }
    // NODES should NOT be sufficient and exception should be thrown.
    Assert.assertNull(pickedNodes2);
    Assert.assertTrue(thrown);
  }

  private boolean checkDuplicateNodesUUID(List<DatanodeDetails> nodes) {
    HashSet<UUID> uuids = nodes.stream().
        map(DatanodeDetails::getUuid).
        collect(Collectors.toCollection(HashSet::new));
    return uuids.size() == nodes.size();
  }

  private Set<PipelineID> mockPipelineIDs(int count) {
    Set<PipelineID> pipelineIDs = new HashSet<>(count);
    for (int i = 0; i < count; i++) {
      pipelineIDs.add(PipelineID.randomId());
    }
    return pipelineIDs;
  }

  private void insertHeavyNodesIntoNodeManager(
      List<DatanodeDetails> nodes, int heavyNodeCount) throws SCMException{
    if (nodes == null) {
      throw new SCMException("",
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    int considerHeavyCount =
        ScmConfigKeys.OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT_DEFAULT + 1;

    Node2PipelineMap mockMap = new Node2PipelineMap();
    for (DatanodeDetails node : nodes) {
      // mock heavy node
      if (heavyNodeCount > 0) {
        mockMap.insertNewDatanode(
            node.getUuid(), mockPipelineIDs(considerHeavyCount));
        heavyNodeCount--;
      } else {
        mockMap.insertNewDatanode(node.getUuid(), mockPipelineIDs(1));
      }
    }
    nodeManager.setNode2PipelineMap(mockMap);
  }
}
