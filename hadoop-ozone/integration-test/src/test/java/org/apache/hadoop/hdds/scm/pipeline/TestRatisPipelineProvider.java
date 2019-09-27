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

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT;

/**
 * Test for RatisPipelineProvider.
 */
public class TestRatisPipelineProvider {

  private NodeManager nodeManager;
  private PipelineProvider provider;
  private PipelineStateManager stateManager;

  @Before
  public void init() throws Exception {
    nodeManager = new MockNodeManager(true, 10);
    stateManager = new PipelineStateManager(new OzoneConfiguration());
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT, 1);
    provider = new MockRatisPipelineProvider(nodeManager,
        stateManager, conf);
  }

  private void createPipelineAndAssertions(
          HddsProtos.ReplicationFactor factor) throws IOException {
    Pipeline pipeline = provider.create(factor);
    stateManager.addPipeline(pipeline);
    nodeManager.addPipeline(pipeline);
    Assert.assertEquals(pipeline.getType(), HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipeline.getFactor(), factor);
    Assert.assertEquals(pipeline.getPipelineState(),
            Pipeline.PipelineState.OPEN);
    Assert.assertEquals(pipeline.getNodes().size(), factor.getNumber());
    Pipeline pipeline1 = provider.create(factor);
    stateManager.addPipeline(pipeline1);
    nodeManager.addPipeline(pipeline1);
    Assert.assertEquals(pipeline1.getType(), HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipeline1.getFactor(), factor);
    Assert.assertEquals(pipeline1.getPipelineState(),
            Pipeline.PipelineState.OPEN);
    Assert.assertEquals(pipeline1.getNodes().size(), factor.getNumber());
  }

  @Test
  public void testCreatePipelineWithFactor() throws IOException {
    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;
    Pipeline pipeline = provider.create(factor);
    stateManager.addPipeline(pipeline);
    nodeManager.addPipeline(pipeline);
    Assert.assertEquals(pipeline.getType(), HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipeline.getFactor(), factor);
    Assert.assertEquals(pipeline.getPipelineState(),
        Pipeline.PipelineState.OPEN);
    Assert.assertEquals(pipeline.getNodes().size(), factor.getNumber());

    factor = HddsProtos.ReplicationFactor.ONE;
    Pipeline pipeline1 = provider.create(factor);
    stateManager.addPipeline(pipeline1);
    nodeManager.addPipeline(pipeline1);
    Assert.assertEquals(pipeline1.getType(), HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipeline1.getFactor(), factor);
    Assert.assertEquals(pipeline1.getPipelineState(),
        Pipeline.PipelineState.OPEN);
    Assert.assertEquals(pipeline1.getNodes().size(), factor.getNumber());
  }

  @Test
  public void testCreatePipelineWithFactorThree() throws IOException {
    createPipelineAndAssertions(HddsProtos.ReplicationFactor.THREE);
  }

  @Test
  public void testCreatePipelineWithFactorOne() throws IOException {
    createPipelineAndAssertions(HddsProtos.ReplicationFactor.ONE);
  }

  private List<DatanodeDetails> createListOfNodes(int nodeCount) {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < nodeCount; i++) {
      nodes.add(TestUtils.randomDatanodeDetails());
    }
    return nodes;
  }

  @Test
  public void testCreatePipelineWithNodes() {
    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;
    Pipeline pipeline =
        provider.create(factor, createListOfNodes(factor.getNumber()));
    Assert.assertEquals(pipeline.getType(), HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipeline.getFactor(), factor);
    Assert.assertEquals(
        pipeline.getPipelineState(), Pipeline.PipelineState.OPEN);
    Assert.assertEquals(pipeline.getNodes().size(), factor.getNumber());

    factor = HddsProtos.ReplicationFactor.ONE;
    pipeline = provider.create(factor, createListOfNodes(factor.getNumber()));
    Assert.assertEquals(pipeline.getType(), HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipeline.getFactor(), factor);
    Assert.assertEquals(pipeline.getPipelineState(),
        Pipeline.PipelineState.OPEN);
    Assert.assertEquals(pipeline.getNodes().size(), factor.getNumber());
  }

  @Test
  public void testCreatePipelinesDnExclude() throws IOException {

    List<DatanodeDetails> allHealthyNodes =
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY);
    int totalHealthyNodesCount = allHealthyNodes.size();

    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;

    List<DatanodeDetails> closePipelineDns = new ArrayList<>();
    for (int i = 0; i < totalHealthyNodesCount/3; i++) {
      List<DatanodeDetails> pipelineDns = allHealthyNodes
          .subList(3 * i, 3 * (i + 1));

      Pipeline.PipelineState state;
      if (i % 2 == 0) {
        state = Pipeline.PipelineState.OPEN;
      } else {
        state = Pipeline.PipelineState.CLOSED;
        closePipelineDns.addAll(pipelineDns);
      }

      Pipeline openPipeline = Pipeline.newBuilder()
          .setType(HddsProtos.ReplicationType.RATIS)
          .setFactor(factor)
          .setNodes(pipelineDns)
          .setState(state)
          .setId(PipelineID.randomId())
          .build();


      stateManager.addPipeline(openPipeline);
      nodeManager.addPipeline(openPipeline);
    }

    Pipeline pipeline = provider.create(factor);
    Assert.assertEquals(pipeline.getType(), HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipeline.getFactor(), factor);
    Assert.assertEquals(pipeline.getPipelineState(),
        Pipeline.PipelineState.OPEN);
    Assert.assertEquals(pipeline.getNodes().size(), factor.getNumber());
    List<DatanodeDetails> pipelineNodes = pipeline.getNodes();

    // Since we have only 10 DNs, at least 1 pipeline node should have been
    // from the closed pipeline DN list.
    Assert.assertTrue(pipelineNodes.parallelStream().filter(
        closePipelineDns::contains).count() > 0);
  }
}