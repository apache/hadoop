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
package org.apache.hadoop.ozone.container.placement;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .SCMContainerPlacementRandom;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState
    .HEALTHY;
import static org.junit.Assert.assertEquals;

/**
 * Asserts that allocation strategy works as expected.
 */
public class TestContainerPlacement {

  private DescriptiveStatistics computeStatistics(NodeManager nodeManager) {
    DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
    for (DatanodeDetails dd : nodeManager.getNodes(HEALTHY)) {
      float weightedValue =
          nodeManager.getNodeStat(dd).get().getScmUsed().get() / (float)
              nodeManager.getNodeStat(dd).get().getCapacity().get();
      descriptiveStatistics.addValue(weightedValue);
    }
    return descriptiveStatistics;
  }

  /**
   * This test simulates lots of Cluster I/O and updates the metadata in SCM.
   * We simulate adding and removing containers from the cluster. It asserts
   * that our placement algorithm has taken the capacity of nodes into
   * consideration by asserting that standard deviation of used space on these
   * has improved.
   */
  @Test
  public void testCapacityPlacementYieldsBetterDataDistribution() throws
      SCMException {
    final int opsCount = 200 * 1000;
    final int nodesRequired = 3;
    Random random = new Random();

    // The nature of init code in MockNodeManager yields similar clusters.
    MockNodeManager nodeManagerCapacity = new MockNodeManager(true, 100);
    MockNodeManager nodeManagerRandom = new MockNodeManager(true, 100);
    DescriptiveStatistics beforeCapacity =
        computeStatistics(nodeManagerCapacity);
    DescriptiveStatistics beforeRandom = computeStatistics(nodeManagerRandom);

    //Assert that our initial layout of clusters are similar.
    assertEquals(beforeCapacity.getStandardDeviation(), beforeRandom
        .getStandardDeviation(), 0.001);

    SCMContainerPlacementCapacity capacityPlacer = new
        SCMContainerPlacementCapacity(nodeManagerCapacity, new Configuration(),
        null, true);
    SCMContainerPlacementRandom randomPlacer = new
        SCMContainerPlacementRandom(nodeManagerRandom, new Configuration(),
        null, true);

    for (int x = 0; x < opsCount; x++) {
      long containerSize = random.nextInt(100) * OzoneConsts.GB;
      List<DatanodeDetails> nodesCapacity =
          capacityPlacer.chooseDatanodes(new ArrayList<>(), null, nodesRequired,
              containerSize);
      assertEquals(nodesRequired, nodesCapacity.size());

      List<DatanodeDetails> nodesRandom =
          randomPlacer.chooseDatanodes(nodesCapacity, null, nodesRequired,
              containerSize);

      // One fifth of all calls are delete
      if (x % 5 == 0) {
        deleteContainer(nodeManagerCapacity, nodesCapacity, containerSize);
        deleteContainer(nodeManagerRandom, nodesRandom, containerSize);
      } else {
        createContainer(nodeManagerCapacity, nodesCapacity, containerSize);
        createContainer(nodeManagerRandom, nodesRandom, containerSize);
      }
    }
    DescriptiveStatistics postCapacity = computeStatistics(nodeManagerCapacity);
    DescriptiveStatistics postRandom = computeStatistics(nodeManagerRandom);

    // This is a very bold claim, and needs large number of I/O operations.
    // The claim in this assertion is that we improved the data distribution
    // of this cluster in relation to the start state of the cluster.
    Assert.assertTrue(beforeCapacity.getStandardDeviation() >
        postCapacity.getStandardDeviation());

    // This asserts that Capacity placement yields a better placement
    // algorithm than random placement, since both cluster started at an
    // identical state.

    Assert.assertTrue(postRandom.getStandardDeviation() >
        postCapacity.getStandardDeviation());
  }

  private void deleteContainer(MockNodeManager nodeManager,
      List<DatanodeDetails> nodes, long containerSize) {
    for (DatanodeDetails dd : nodes) {
      nodeManager.delContainer(dd, containerSize);
    }
  }

  private void createContainer(MockNodeManager nodeManager,
      List<DatanodeDetails> nodes, long containerSize) {
    for (DatanodeDetails dd : nodes) {
      nodeManager.addContainer(dd, containerSize);
    }
  }
}
