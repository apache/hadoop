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
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.container.states.ContainerStateMap;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipelines.PipelineSelector;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .ReplicationFactor.THREE;

public class TestNode2PipelineMap {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;
  private static ContainerWithPipeline ratisContainer;
  private static ContainerStateMap stateMap;
  private static ContainerMapping mapping;
  private static PipelineSelector pipelineSelector;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    mapping = (ContainerMapping)scm.getScmContainerManager();
    stateMap = mapping.getStateManager().getContainerStateMap();
    ratisContainer = mapping.allocateContainer(RATIS, THREE, "testOwner");
    pipelineSelector = mapping.getPipelineSelector();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }


  @Test
  public void testPipelineMap() throws IOException {

    Set<ContainerID> set = pipelineSelector.getOpenContainerIDsByPipeline(
        ratisContainer.getPipeline().getId());

    long cId = ratisContainer.getContainerInfo().getContainerID();
    Assert.assertEquals(1, set.size());
    set.forEach(containerID ->
            Assert.assertEquals(containerID, ContainerID.valueof(cId)));

    List<DatanodeDetails> dns = ratisContainer.getPipeline().getMachines();
    Assert.assertEquals(3, dns.size());

    // get pipeline details by dnid
    Set<PipelineID> pipelines = mapping.getPipelineSelector()
        .getPipelineByDnID(dns.get(0).getUuid());
    Assert.assertEquals(1, pipelines.size());
    pipelines.forEach(p -> Assert.assertEquals(p,
        ratisContainer.getPipeline().getId()));


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
    Set<ContainerID> set2 = pipelineSelector.getOpenContainerIDsByPipeline(
        ratisContainer.getPipeline().getId());
    Assert.assertEquals(0, set2.size());

    try {
      pipelineSelector.updatePipelineState(ratisContainer.getPipeline(),
          HddsProtos.LifeCycleEvent.CLOSE);
      Assert.fail("closing of pipeline without finalize should fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SCMException);
      Assert.assertEquals(((SCMException)e).getResult(),
          SCMException.ResultCodes.FAILED_TO_CHANGE_PIPELINE_STATE);
    }
  }
}
