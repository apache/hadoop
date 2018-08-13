/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerMapping;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.states.ContainerStateMap;
import org.apache.hadoop.hdds.scm.pipelines.PipelineSelector;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .ReplicationType.RATIS;

public class TestPipelineClose {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;
  private static ContainerWithPipeline ratisContainer1;
  private static ContainerWithPipeline ratisContainer2;
  private static ContainerStateMap stateMap;
  private static ContainerMapping mapping;
  private static PipelineSelector pipelineSelector;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(6).build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    mapping = (ContainerMapping)scm.getScmContainerManager();
    stateMap = mapping.getStateManager().getContainerStateMap();
    ratisContainer1 = mapping.allocateContainer(RATIS, THREE, "testOwner");
    ratisContainer2 = mapping.allocateContainer(RATIS, THREE, "testOwner");
    pipelineSelector = mapping.getPipelineSelector();
    // At this stage, there should be 2 pipeline one with 1 open container each.
    // Try closing the both the pipelines, one with a closed container and
    // the other with an open container.
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }


  @Test
  public void testPipelineCloseWithClosedContainer() throws IOException {
    NavigableSet<ContainerID> set = stateMap.getOpenContainerIDsByPipeline(
        ratisContainer1.getPipeline().getId());

    long cId = ratisContainer1.getContainerInfo().getContainerID();
    Assert.assertEquals(1, set.size());
    Assert.assertEquals(cId, set.first().getId());

    // Now close the container and it should not show up while fetching
    // containers by pipeline
    mapping
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.CREATE);
    mapping
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.CREATED);
    mapping
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.FINALIZE);
    mapping
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.CLOSE);

    NavigableSet<ContainerID> setClosed = stateMap.getOpenContainerIDsByPipeline(
        ratisContainer1.getPipeline().getId());
    Assert.assertEquals(0, setClosed.size());

    pipelineSelector.finalizePipeline(ratisContainer1.getPipeline());
    Pipeline pipeline1 = pipelineSelector
        .getPipeline(ratisContainer1.getPipeline().getId(),
            ratisContainer1.getContainerInfo().getReplicationType());
    Assert.assertNull(pipeline1);
    Assert.assertEquals(ratisContainer1.getPipeline().getLifeCycleState(),
        HddsProtos.LifeCycleState.CLOSED);
    for (DatanodeDetails dn : ratisContainer1.getPipeline().getMachines()) {
      // Assert that the pipeline has been removed from Node2PipelineMap as well
      Assert.assertEquals(pipelineSelector.getNode2PipelineMap()
          .getPipelines(dn.getUuid()).size(), 0);
    }
  }

  @Test
  public void testPipelineCloseWithOpenContainer() throws IOException,
      TimeoutException, InterruptedException {
    NavigableSet<ContainerID> setOpen = stateMap.getOpenContainerIDsByPipeline(
        ratisContainer2.getPipeline().getId());
    Assert.assertEquals(1, setOpen.size());

    long cId2 = ratisContainer2.getContainerInfo().getContainerID();
    mapping
        .updateContainerState(cId2, HddsProtos.LifeCycleEvent.CREATE);
    mapping
        .updateContainerState(cId2, HddsProtos.LifeCycleEvent.CREATED);
    pipelineSelector.finalizePipeline(ratisContainer2.getPipeline());
    Assert.assertEquals(ratisContainer2.getPipeline().getLifeCycleState(),
        HddsProtos.LifeCycleState.CLOSING);
    Pipeline pipeline2 = pipelineSelector
        .getPipeline(ratisContainer2.getPipeline().getId(),
            ratisContainer2.getContainerInfo().getReplicationType());
    Assert.assertEquals(pipeline2.getLifeCycleState(),
        HddsProtos.LifeCycleState.CLOSING);
  }
}