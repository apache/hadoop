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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test for PipelineStateManager.
 */
public class TestPipelineStateManager {

  private PipelineStateManager stateManager;

  @Before
  public void init() throws Exception {
    Configuration conf = new OzoneConfiguration();
    stateManager = new PipelineStateManager(conf);
  }

  private Pipeline createDummyPipeline(int numNodes) {
    return createDummyPipeline(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, numNodes);
  }

  private Pipeline createDummyPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, int numNodes) {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      nodes.add(TestUtils.randomDatanodeDetails());
    }
    return Pipeline.newBuilder()
        .setType(type)
        .setFactor(factor)
        .setNodes(nodes)
        .setState(Pipeline.PipelineState.ALLOCATED)
        .setId(PipelineID.randomId())
        .build();
  }

  @Test
  public void testAddAndGetPipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(0);
    try {
      stateManager.addPipeline(pipeline);
      Assert.fail("Pipeline should not have been added");
    } catch (IllegalArgumentException e) {
      // replication factor and number of nodes in the pipeline do not match
      Assert.assertTrue(e.getMessage().contains("do not match"));
    }

    // add a pipeline
    pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);

    try {
      stateManager.addPipeline(pipeline);
      Assert.fail("Pipeline should not have been added");
    } catch (IOException e) {
      // Can not add a pipeline twice
      Assert.assertTrue(e.getMessage().contains("Duplicate pipeline ID"));
    }

    // verify pipeline returned is same
    Pipeline pipeline1 = stateManager.getPipeline(pipeline.getId());
    Assert.assertTrue(pipeline == pipeline1);

    // clean up
    removePipeline(pipeline);
  }

  @Test
  public void testGetPipelines() throws IOException {
    // In start there should be no pipelines
    Assert.assertTrue(stateManager.getPipelines().isEmpty());

    Set<Pipeline> pipelines = new HashSet<>();
    Pipeline pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    stateManager.openPipeline(pipeline.getId());
    pipelines.add(pipeline);
    pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    stateManager.openPipeline(pipeline.getId());
    pipelines.add(pipeline);

    Set<Pipeline> pipelines1 = new HashSet<>(stateManager.getPipelines(
        HddsProtos.ReplicationType.RATIS));
    Assert.assertEquals(pipelines1.size(), pipelines.size());

    pipelines1 = new HashSet<>(stateManager.getPipelines());
    Assert.assertEquals(pipelines1.size(), pipelines.size());

    // clean up
    for (Pipeline pipeline1 : pipelines) {
      removePipeline(pipeline1);
    }
  }

  @Test
  public void testGetPipelinesByTypeAndFactor() throws IOException {
    Set<Pipeline> pipelines = new HashSet<>();
    for (HddsProtos.ReplicationType type : HddsProtos.ReplicationType
        .values()) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        for (int i = 0; i < 5; i++) {
          // 5 pipelines in allocated state for each type and factor
          Pipeline pipeline =
              createDummyPipeline(type, factor, factor.getNumber());
          stateManager.addPipeline(pipeline);
          pipelines.add(pipeline);

          // 5 pipelines in open state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber());
          stateManager.addPipeline(pipeline);
          stateManager.openPipeline(pipeline.getId());
          pipelines.add(pipeline);

          // 5 pipelines in closed state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber());
          stateManager.addPipeline(pipeline);
          stateManager.finalizePipeline(pipeline.getId());
          pipelines.add(pipeline);
        }
      }
    }

    for (HddsProtos.ReplicationType type : HddsProtos.ReplicationType
        .values()) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        // verify pipelines received
        List<Pipeline> pipelines1 =
            stateManager.getPipelines(type, factor);
        Assert.assertEquals(15, pipelines1.size());
        pipelines1.stream().forEach(p -> {
          Assert.assertEquals(type, p.getType());
          Assert.assertEquals(factor, p.getFactor());
        });
      }
    }

    //clean up
    for (Pipeline pipeline : pipelines) {
      removePipeline(pipeline);
    }
  }

  @Test
  public void testGetPipelinesByTypeAndState() throws IOException {
    Set<Pipeline> pipelines = new HashSet<>();
    for (HddsProtos.ReplicationType type : HddsProtos.ReplicationType
        .values()) {
      HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;
      for (int i = 0; i < 5; i++) {
        // 5 pipelines in allocated state for each type and factor
        Pipeline pipeline =
            createDummyPipeline(type, factor, factor.getNumber());
        stateManager.addPipeline(pipeline);
        pipelines.add(pipeline);

        // 5 pipelines in open state for each type and factor
        pipeline = createDummyPipeline(type, factor, factor.getNumber());
        stateManager.addPipeline(pipeline);
        stateManager.openPipeline(pipeline.getId());
        pipelines.add(pipeline);

        // 5 pipelines in closed state for each type and factor
        pipeline = createDummyPipeline(type, factor, factor.getNumber());
        stateManager.addPipeline(pipeline);
        stateManager.finalizePipeline(pipeline.getId());
        pipelines.add(pipeline);
      }
    }

    for (HddsProtos.ReplicationType type : HddsProtos.ReplicationType
        .values()) {
      // verify pipelines received
      List<Pipeline> pipelines1 = stateManager
          .getPipelines(type, Pipeline.PipelineState.OPEN);
      Assert.assertEquals(5, pipelines1.size());
      pipelines1.forEach(p -> {
        Assert.assertEquals(type, p.getType());
        Assert.assertEquals(Pipeline.PipelineState.OPEN, p.getPipelineState());
      });

      pipelines1 = stateManager
          .getPipelines(type, Pipeline.PipelineState.OPEN,
              Pipeline.PipelineState.CLOSED, Pipeline.PipelineState.ALLOCATED);
      Assert.assertEquals(15, pipelines1.size());
    }

    //clean up
    for (Pipeline pipeline : pipelines) {
      removePipeline(pipeline);
    }
  }

  @Test
  public void testGetPipelinesByTypeFactorAndState() throws IOException {
    Set<Pipeline> pipelines = new HashSet<>();
    for (HddsProtos.ReplicationType type : HddsProtos.ReplicationType
        .values()) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        for (int i = 0; i < 5; i++) {
          // 5 pipelines in allocated state for each type and factor
          Pipeline pipeline =
              createDummyPipeline(type, factor, factor.getNumber());
          stateManager.addPipeline(pipeline);
          pipelines.add(pipeline);

          // 5 pipelines in open state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber());
          stateManager.addPipeline(pipeline);
          stateManager.openPipeline(pipeline.getId());
          pipelines.add(pipeline);

          // 5 pipelines in closed state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber());
          stateManager.addPipeline(pipeline);
          stateManager.finalizePipeline(pipeline.getId());
          pipelines.add(pipeline);
        }
      }
    }

    for (HddsProtos.ReplicationType type : HddsProtos.ReplicationType
        .values()) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        for (Pipeline.PipelineState state : Pipeline.PipelineState.values()) {
          // verify pipelines received
          List<Pipeline> pipelines1 =
              stateManager.getPipelines(type, factor, state);
          Assert.assertEquals(5, pipelines1.size());
          pipelines1.forEach(p -> {
            Assert.assertEquals(type, p.getType());
            Assert.assertEquals(factor, p.getFactor());
            Assert.assertEquals(state, p.getPipelineState());
          });
        }
      }
    }

    //clean up
    for (Pipeline pipeline : pipelines) {
      removePipeline(pipeline);
    }
  }

  @Test
  public void testAddAndGetContainer() throws IOException {
    long containerID = 0;
    Pipeline pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    pipeline = stateManager.getPipeline(pipeline.getId());
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueof(++containerID));

    // move pipeline to open state
    stateManager.openPipeline(pipeline.getId());
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueof(++containerID));
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueof(++containerID));

    //verify the number of containers returned
    Set<ContainerID> containerIDs =
        stateManager.getContainers(pipeline.getId());
    Assert.assertEquals(containerIDs.size(), containerID);

    removePipeline(pipeline);
    try {
      stateManager.addContainerToPipeline(pipeline.getId(),
          ContainerID.valueof(++containerID));
      Assert.fail("Container should not have been added");
    } catch (IOException e) {
      // Can not add a container to removed pipeline
      Assert.assertTrue(e.getMessage().contains("not found"));
    }
  }

  @Test
  public void testRemovePipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    // close the pipeline
    stateManager.openPipeline(pipeline.getId());
    stateManager
        .addContainerToPipeline(pipeline.getId(), ContainerID.valueof(1));

    try {
      stateManager.removePipeline(pipeline.getId());
      Assert.fail("Pipeline should not have been removed");
    } catch (IOException e) {
      // can not remove a pipeline which already has containers
      Assert.assertTrue(e.getMessage().contains("not yet closed"));
    }

    // close the pipeline
    stateManager.finalizePipeline(pipeline.getId());
    // remove containers and then remove the pipeline
    removePipeline(pipeline);
  }

  @Test
  public void testRemoveContainer() throws IOException {
    long containerID = 1;
    Pipeline pipeline = createDummyPipeline(1);
    // create an open pipeline in stateMap
    stateManager.addPipeline(pipeline);
    stateManager.openPipeline(pipeline.getId());

    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueof(containerID));
    Assert.assertEquals(1, stateManager.getContainers(pipeline.getId()).size());
    stateManager.removeContainerFromPipeline(pipeline.getId(),
        ContainerID.valueof(containerID));
    Assert.assertEquals(0, stateManager.getContainers(pipeline.getId()).size());

    // add two containers in the pipeline
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueof(++containerID));
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueof(++containerID));
    Assert.assertEquals(2, stateManager.getContainers(pipeline.getId()).size());

    // move pipeline to closing state
    stateManager.finalizePipeline(pipeline.getId());

    stateManager.removeContainerFromPipeline(pipeline.getId(),
        ContainerID.valueof(containerID));
    stateManager.removeContainerFromPipeline(pipeline.getId(),
        ContainerID.valueof(--containerID));
    Assert.assertEquals(0, stateManager.getContainers(pipeline.getId()).size());

    // clean up
    stateManager.removePipeline(pipeline.getId());
  }

  @Test
  public void testFinalizePipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    // finalize on ALLOCATED pipeline
    stateManager.finalizePipeline(pipeline.getId());
    Assert.assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    removePipeline(pipeline);

    pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    stateManager.openPipeline(pipeline.getId());
    // finalize on OPEN pipeline
    stateManager.finalizePipeline(pipeline.getId());
    Assert.assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    removePipeline(pipeline);

    pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    stateManager.openPipeline(pipeline.getId());
    stateManager.finalizePipeline(pipeline.getId());
    // finalize should work on already closed pipeline
    stateManager.finalizePipeline(pipeline.getId());
    Assert.assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    removePipeline(pipeline);
  }

  @Test
  public void testOpenPipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    // open on ALLOCATED pipeline
    stateManager.openPipeline(pipeline.getId());
    Assert.assertEquals(Pipeline.PipelineState.OPEN,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());

    stateManager.openPipeline(pipeline.getId());
    // open should work on already open pipeline
    Assert.assertEquals(Pipeline.PipelineState.OPEN,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    removePipeline(pipeline);
  }

  @Test
  public void testQueryPipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.THREE, 3);
    // pipeline in allocated state should not be reported
    stateManager.addPipeline(pipeline);
    Assert.assertEquals(0, stateManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN)
        .size());

    // pipeline in open state should be reported
    stateManager.openPipeline(pipeline.getId());
    Assert.assertEquals(1, stateManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN)
        .size());

    Pipeline pipeline2 = createDummyPipeline(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.THREE, 3);
    pipeline2 = Pipeline.newBuilder(pipeline2)
        .setState(Pipeline.PipelineState.OPEN)
        .build();
    // pipeline in open state should be reported
    stateManager.addPipeline(pipeline2);
    Assert.assertEquals(2, stateManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN)
        .size());

    // pipeline in closed state should not be reported
    stateManager.finalizePipeline(pipeline2.getId());
    Assert.assertEquals(1, stateManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN)
        .size());

    // clean up
    removePipeline(pipeline);
    removePipeline(pipeline2);
  }

  private void removePipeline(Pipeline pipeline) throws IOException {
    stateManager.finalizePipeline(pipeline.getId());
    stateManager.removePipeline(pipeline.getId());
  }
}