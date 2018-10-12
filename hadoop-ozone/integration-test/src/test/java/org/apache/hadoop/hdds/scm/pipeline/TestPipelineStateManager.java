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
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      nodes.add(TestUtils.randomDatanodeDetails());
    }
    return Pipeline.newBuilder()
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setNodes(nodes)
        .setState(HddsProtos.LifeCycleState.ALLOCATED)
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
    stateManager.removePipeline(pipeline1.getID());
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

    Set<Pipeline> pipelines1 = new HashSet<>(stateManager.getPipelines(
        HddsProtos.ReplicationType.RATIS));
    Assert.assertEquals(pipelines, pipelines1);
  }

  @Test
  public void testAddAndGetContainer() throws IOException {
    long containerID = 0;
    Pipeline pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    pipeline = stateManager.getPipeline(pipeline.getID());

    try {
      stateManager
          .addContainerToPipeline(pipeline.getID(), ContainerID.valueof(++containerID));
      Assert.fail("Container should not have been added");
    } catch (IOException e) {
      // add container possible only in container with open state
      Assert.assertTrue(e.getMessage().contains("is not in open state"));
    }

    // move pipeline to open state
    updateEvents(pipeline.getID(), HddsProtos.LifeCycleEvent.CREATE,
        HddsProtos.LifeCycleEvent.CREATED);

    // add three containers
    stateManager
        .addContainerToPipeline(pipeline.getID(), ContainerID.valueof(containerID));
    stateManager
        .addContainerToPipeline(pipeline.getID(), ContainerID.valueof(++containerID));
    stateManager
        .addContainerToPipeline(pipeline.getID(), ContainerID.valueof(++containerID));

    //verify the number of containers returned
    Set<ContainerID> containerIDs =
        stateManager.getContainers(pipeline.getID());
    Assert.assertEquals(containerIDs.size(), containerID);

    removePipeline(pipeline);
    try {
      stateManager
          .addContainerToPipeline(pipeline.getID(), ContainerID.valueof(++containerID));
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
    updateEvents(pipeline.getID(), HddsProtos.LifeCycleEvent.CREATE,
        HddsProtos.LifeCycleEvent.CREATED);
    stateManager
        .addContainerToPipeline(pipeline.getID(), ContainerID.valueof(1));

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
    updateEvents(pipeline.getID(), HddsProtos.LifeCycleEvent.CREATE,
        HddsProtos.LifeCycleEvent.CREATED);

    stateManager
        .addContainerToPipeline(pipeline.getID(), ContainerID.valueof(containerID));
    stateManager
        .removeContainerFromPipeline(pipeline.getID(), ContainerID.valueof(containerID));
    // removeContainerFromPipeline in open pipeline does not lead to removal of pipeline
    Assert.assertNotNull(stateManager.getPipeline(pipeline.getID()));

    // add two containers in the pipeline
    stateManager
        .addContainerToPipeline(pipeline.getID(), ContainerID.valueof(++containerID));
    stateManager
        .addContainerToPipeline(pipeline.getID(), ContainerID.valueof(++containerID));

    // move pipeline to closing state
    updateEvents(pipeline.getID(), HddsProtos.LifeCycleEvent.FINALIZE);

    stateManager
        .removeContainerFromPipeline(pipeline.getID(), ContainerID.valueof(containerID));
    // removal of second last container in closing or closed pipeline should
    // not lead to removal of pipeline
    Assert.assertNotNull(stateManager.getPipeline(pipeline.getID()));
    stateManager
        .removeContainerFromPipeline(pipeline.getID(), ContainerID.valueof(--containerID));
    // removal of last container in closing or closed pipeline should lead to
    // removal of pipeline
    try {
      stateManager.getPipeline(pipeline.getID());
      Assert.fail("getPipeline should have failed.");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains(" not found"));
    }
  }

  @Test
  public void testUpdatePipelineState() throws IOException {
    Pipeline pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    updateEvents(pipeline.getID(), HddsProtos.LifeCycleEvent.CREATE,
        HddsProtos.LifeCycleEvent.CREATED, HddsProtos.LifeCycleEvent.FINALIZE,
        HddsProtos.LifeCycleEvent.CLOSE);

    pipeline = createDummyPipeline(1);
    stateManager.addPipeline(pipeline);
    updateEvents(pipeline.getID(), HddsProtos.LifeCycleEvent.CREATE,
        HddsProtos.LifeCycleEvent.TIMEOUT);
  }

  private void updateEvents(PipelineID pipelineID,
      HddsProtos.LifeCycleEvent... events) throws IOException {
    for (HddsProtos.LifeCycleEvent event : events) {
      stateManager.updatePipelineState(pipelineID, event);
    }
  }

  private void removePipeline(Pipeline pipeline) throws IOException {
    Set<ContainerID> containerIDs =
        stateManager.getContainers(pipeline.getID());
    for (ContainerID containerID : containerIDs) {
      stateManager.removeContainerFromPipeline(pipeline.getID(), containerID);
    }
    stateManager.removePipeline(pipeline.getID());
  }
}