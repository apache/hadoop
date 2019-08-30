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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
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
  public void testChooseNextNodeBasedOnNetworkTopology() {
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY);
    DatanodeDetails anchor = placementPolicy.chooseNode(healthyNodes);
    // anchor should be removed from healthyNodes after being chosen.
    Assert.assertFalse(healthyNodes.contains(anchor));

    List<DatanodeDetails> excludedNodes =
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT);
    DatanodeDetails nextNode = placementPolicy.chooseNextNode(
        healthyNodes, excludedNodes, anchor);
    // excludedNodes should contain nextNode after being chosen.
    Assert.assertTrue(excludedNodes.contains(nextNode));
    // nextNode should not be the same as anchor.
    Assert.assertTrue(anchor.getUuid() != nextNode.getUuid());
  }

  @Test
  public void testHeavyNodeShouldBeExcluded() throws SCMException{
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY);
    int nodesRequired = healthyNodes.size()/2;
    // only minority of healthy nodes are heavily engaged in pipelines.
    int minorityHeavy = healthyNodes.size()/2 - 1;
    List<DatanodeDetails> pickedNodes1 = placementPolicy.chooseDatanodes(
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
        nodesRequired, 0);
    // modify node to pipeline mapping.
    insertHeavyNodesIntoNodeManager(healthyNodes, minorityHeavy);
    // nodes should be sufficient.
    Assert.assertEquals(nodesRequired, pickedNodes1.size());
    // make sure pipeline placement policy won't select duplicated nodes.
    Assert.assertTrue(checkDuplicateNodesUUID(pickedNodes1));

    // majority of healthy nodes are heavily engaged in pipelines.
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
    // nodes should NOT be sufficient and exception should be thrown.
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
