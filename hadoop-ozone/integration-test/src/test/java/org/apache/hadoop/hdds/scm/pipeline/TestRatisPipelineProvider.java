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

import org.apache.commons.collections.CollectionUtils;
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
    provider = new MockRatisPipelineProvider(nodeManager,
        stateManager, new OzoneConfiguration());
  }

  private void createPipelineAndAssertions(
          HddsProtos.ReplicationFactor factor) throws IOException {
    Pipeline pipeline = provider.create(factor);
    stateManager.addPipeline(pipeline);
    Assert.assertEquals(pipeline.getType(), HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipeline.getFactor(), factor);
    Assert.assertEquals(pipeline.getPipelineState(),
            Pipeline.PipelineState.OPEN);
    Assert.assertEquals(pipeline.getNodes().size(), factor.getNumber());
    Pipeline pipeline1 = provider.create(factor);
    stateManager.addPipeline(pipeline1);
    // New pipeline should not overlap with the previous created pipeline
    Assert.assertTrue(
        CollectionUtils.intersection(pipeline.getNodes(), pipeline1.getNodes())
            .isEmpty());
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
    Assert.assertEquals(pipeline.getType(), HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipeline.getFactor(), factor);
    Assert.assertEquals(pipeline.getPipelineState(),
        Pipeline.PipelineState.OPEN);
    Assert.assertEquals(pipeline.getNodes().size(), factor.getNumber());

    factor = HddsProtos.ReplicationFactor.ONE;
    Pipeline pipeline1 = provider.create(factor);
    stateManager.addPipeline(pipeline1);
    // New pipeline should overlap with the previous created pipeline,
    // and one datanode should overlap between the two types.
    Assert.assertEquals(
        CollectionUtils.intersection(pipeline.getNodes(),
            pipeline1.getNodes()).size(), 1);
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

    // We have 10 DNs in MockNodeManager.
    // Use up first 3 DNs for an open pipeline.
    List<DatanodeDetails> openPiplineDns = nodeManager.getAllNodes()
        .subList(0, 3);
    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;

    Pipeline openPipeline = Pipeline.newBuilder()
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(factor)
        .setNodes(openPiplineDns)
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .build();

    stateManager.addPipeline(openPipeline);

    // Use up next 3 DNs also for an open pipeline.
    List<DatanodeDetails> moreOpenPiplineDns = nodeManager.getAllNodes()
        .subList(3, 6);
    Pipeline anotherOpenPipeline = Pipeline.newBuilder()
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(factor)
        .setNodes(moreOpenPiplineDns)
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .build();
    stateManager.addPipeline(anotherOpenPipeline);

    // Use up next 3 DNs also for a closed pipeline.
    List<DatanodeDetails> closedPiplineDns = nodeManager.getAllNodes()
        .subList(6, 9);
    Pipeline anotherClosedPipeline = Pipeline.newBuilder()
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(factor)
        .setNodes(closedPiplineDns)
        .setState(Pipeline.PipelineState.CLOSED)
        .setId(PipelineID.randomId())
        .build();
    stateManager.addPipeline(anotherClosedPipeline);

    Pipeline pipeline = provider.create(factor);
    Assert.assertEquals(pipeline.getType(), HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipeline.getFactor(), factor);
    Assert.assertEquals(pipeline.getPipelineState(),
        Pipeline.PipelineState.OPEN);
    Assert.assertEquals(pipeline.getNodes().size(), factor.getNumber());
    List<DatanodeDetails> pipelineNodes = pipeline.getNodes();

    // Pipline nodes cannot be from open pipelines.
    Assert.assertTrue(
        pipelineNodes.parallelStream().filter(dn ->
        (openPiplineDns.contains(dn) || moreOpenPiplineDns.contains(dn)))
        .count() == 0);

    // Since we have only 10 DNs, at least 1 pipeline node should have been
    // from the closed pipeline DN list.
    Assert.assertTrue(pipelineNodes.parallelStream().filter(
        closedPiplineDns::contains).count() > 0);
  }
}