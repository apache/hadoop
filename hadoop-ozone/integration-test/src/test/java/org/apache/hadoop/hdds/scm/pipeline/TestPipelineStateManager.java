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
    Pipeline pipeline1 = stateManager.getPipeline(pipeline.getID());
    Assert.assertTrue(pipeline == pipeline1);

    // clean up
    removePipeline(pipeline);
  }

  @Test
  public void testGetPipelines() throws IOException {
    Set<Pipeline> pipelines = new HashSet<>();
    Pipeline pipeline = createDummyPipeline(1);
    pipelines.add(pipeline);
    stateManager.addPipeline(pipeline);
    pipeline = createDummyPipeline(1);
    pipelines.add(pipeline);
    stateManager.addPipeline(pipeline);

    Set<Pipeline> pipelines1 = new HashSet<>(stateManager.getPipelinesByType(
        HddsProtos.ReplicationType.RATIS));
    Assert.assertEquals(pipelines, pipelines1);
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

          // 5 pipelines in allocated state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber());
          stateManager.addPipeline(pipeline);
          stateManager.openPipeline(pipeline.getID());
          pipelines.add(pipeline);

          // 5 pipelines in allocated state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber());
          stateManager.addPipeline(pipeline);
          stateManager.finalizePipeline(pipeline.getID());
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
            stateManager.getPipelinesByTypeAndFactor(type, factor);
        Assert.assertEquals(5, pipelines1.size());
        pipelines1.stream().forEach(p -> {
          Assert.assertEquals(p.getType(), type);
          Assert.assertEquals(p.getFactor(), factor);
        });
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
    pipeline = stateManager.getPipeline(pipeline.getID());

    try {
      stateManager.addContainerToPipeline(pipeline.getID(),
          ContainerID.valueof(++containerID));
      Assert.fail("Container should not have been added");
    } catch (IOException e) {
      // add container possible only in container with open state
      Assert.assertTrue(e.getMessage().contains("is not in open state"));
    }

    // move pipeline to open state
    stateManager.openPipeline(pipeline.getID());

    // add three containers
    stateManager.addContainerToPipeline(pipeline.getID(),
        ContainerID.valueof(containerID));
    stateManager.addContainerToPipeline(pipeline.getID(),
        ContainerID.valueof(++containerID));
    stateManager.addContainerToPipeline(pipeline.getID(),
        ContainerID.valueof(++containerID));

    //verify the number of containers returned
    Set<ContainerID> containerIDs =
        stateManager.getContainers(pipeline.getID());
    Assert.assertEquals(containerIDs.size(), containerID);

    removePipeline(pipeline);
    try {
      stateManager.addContainerToPipeline(pipeline.getID(),
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
    stateManager.openPipeline(pipeline.getID());
    stateManager
        .addContainerToPipeline(pipeline.getID(), ContainerID.valueof(1));

    try {
      stateManager.removePipeline(pipeline.getID());
      Assert.fail("Pipeline should not have been removed");
    } catch (IOException e) {
      // can not remove a pipeline which already has containers
      Assert.assertTrue(e.getMessage().contains("not yet closed"));
    }

    // close the pipeline
    stateManager.finalizePipeline(pipeline.getID());

    try {
      stateManager.removePipeline(pipeline.getID());
      Assert.fail("Pipeline should not have been removed");
    } catch (IOException e) {
      // can not remove a pipeline which already has containers
      Assert.assertTrue(e.getMessage().contains("not empty"));
    }

    // remove containers and then remove the pipeline
    removePipeline(pipeline);
  }

  @Test
  public void testRemoveContainer() throws IOException {
    long containerID = 1;
    Pipeline pipeline = createDummyPipeline(1);
    // create an open pipeline in stateMap
    stateManager.addPipeline(pipeline);
    stateManager.openPipeline(pipeline.getID());

    stateManager.addContainerToPipeline(pipeline.getID(),
        ContainerID.valueof(containerID));
    Assert.assertEquals(1, stateManager.getContainers(pipeline.getID()).size());
    stateManager.removeContainerFromPipeline(pipeline.getID(),
        ContainerID.valueof(containerID));
    Assert.assertEquals(0, stateManager.getContainers(pipeline.getID()).size());

    // add two containers in the pipeline
    stateManager.addContainerToPipeline(pipeline.getID(),
        ContainerID.valueof(++containerID));
    stateManager.addContainerToPipeline(pipeline.getID(),
        ContainerID.valueof(++containerID));
    Assert.assertEquals(2, stateManager.getContainers(pipeline.getID()).size());

    // move pipeline to closing state
    stateManager.finalizePipeline(pipeline.getID());

    stateManager.removeContainerFromPipeline(pipeline.getID(),
        ContainerID.valueof(containerID));
    stateManager.removeContainerFromPipeline(pipeline.getID(),
        ContainerID.valueof(--containerID));
    Assert.assertEquals(0, stateManager.getContainers(pipeline.getID()).size());

    // clean up
    stateManager.removePipeline(pipeline.getID());
  }

  @Test
  public void testFinalizePipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    // finalize on ALLOCATED pipeline
    stateManager.finalizePipeline(pipeline.getID());
    Assert.assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getID()).getPipelineState());
    // clean up
    removePipeline(pipeline);

    pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    stateManager.openPipeline(pipeline.getID());
    // finalize on OPEN pipeline
    stateManager.finalizePipeline(pipeline.getID());
    Assert.assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getID()).getPipelineState());
    // clean up
    removePipeline(pipeline);

    pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    stateManager.openPipeline(pipeline.getID());
    stateManager.finalizePipeline(pipeline.getID());
    // finalize should work on already closed pipeline
    stateManager.finalizePipeline(pipeline.getID());
    Assert.assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getID()).getPipelineState());
    // clean up
    removePipeline(pipeline);
  }

  @Test
  public void testOpenPipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    // open on ALLOCATED pipeline
    stateManager.openPipeline(pipeline.getID());
    Assert.assertEquals(Pipeline.PipelineState.OPEN,
        stateManager.getPipeline(pipeline.getID()).getPipelineState());

    stateManager.openPipeline(pipeline.getID());
    // open should work on already open pipeline
    Assert.assertEquals(Pipeline.PipelineState.OPEN,
        stateManager.getPipeline(pipeline.getID()).getPipelineState());
    // clean up
    removePipeline(pipeline);
  }

  private void removePipeline(Pipeline pipeline) throws IOException {
    stateManager.finalizePipeline(pipeline.getID());
    Set<ContainerID> containerIDs =
        stateManager.getContainers(pipeline.getID());
    for (ContainerID containerID : containerIDs) {
      stateManager.removeContainerFromPipeline(pipeline.getID(), containerID);
    }
    stateManager.removePipeline(pipeline.getID());
  }
}