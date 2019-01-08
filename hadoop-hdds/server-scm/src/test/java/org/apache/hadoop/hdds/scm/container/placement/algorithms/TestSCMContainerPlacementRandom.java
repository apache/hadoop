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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;

import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Matchers.anyObject;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;

/**
 * Test for the random container placement.
 */
public class TestSCMContainerPlacementRandom {

  @Test
  public void chooseDatanodes() throws SCMException {
    //given
    Configuration conf = new OzoneConfiguration();

    List<DatanodeDetails> datanodes = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      datanodes.add(TestUtils.randomDatanodeDetails());
    }

    NodeManager mockNodeManager = Mockito.mock(NodeManager.class);
    when(mockNodeManager.getNodes(NodeState.HEALTHY))
        .thenReturn(new ArrayList<>(datanodes));

    when(mockNodeManager.getNodeStat(anyObject()))
        .thenReturn(new SCMNodeMetric(100L, 0L, 100L));
    when(mockNodeManager.getNodeStat(datanodes.get(2)))
        .thenReturn(new SCMNodeMetric(100L, 90L, 10L));

    SCMContainerPlacementRandom scmContainerPlacementRandom =
        new SCMContainerPlacementRandom(mockNodeManager, conf);

    List<DatanodeDetails> existingNodes = new ArrayList<>();
    existingNodes.add(datanodes.get(0));
    existingNodes.add(datanodes.get(1));

    for (int i = 0; i < 100; i++) {
      //when
      List<DatanodeDetails> datanodeDetails =
          scmContainerPlacementRandom.chooseDatanodes(existingNodes, 1, 15);

      //then
      Assert.assertEquals(1, datanodeDetails.size());
      DatanodeDetails datanode0Details = datanodeDetails.get(0);

      Assert.assertNotEquals(
          "Datanode 0 should not been selected: excluded by parameter",
          datanodes.get(0), datanode0Details);
      Assert.assertNotEquals(
          "Datanode 1 should not been selected: excluded by parameter",
          datanodes.get(1), datanode0Details);
      Assert.assertNotEquals(
          "Datanode 2 should not been selected: not enough space there",
          datanodes.get(2), datanode0Details);

    }
  }
}