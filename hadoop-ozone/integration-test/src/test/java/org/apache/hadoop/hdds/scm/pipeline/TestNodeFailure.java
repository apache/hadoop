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
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .ReplicationType.RATIS;

/**
 * Test Node failure detection and handling in Ratis.
 */
public class TestNodeFailure {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static Pipeline ratisPipelineOne;
  private static Pipeline ratisPipelineTwo;
  private static ContainerManager containerManager;
  private static PipelineManager pipelineManager;
  private static long timeForFailure;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OzoneConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_KEY,
        10, TimeUnit.SECONDS);
    conf.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT,
        10, TimeUnit.SECONDS);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(6)
        .setHbInterval(1000)
        .setHbProcessorInterval(1000)
        .build();
    cluster.waitForClusterToBeReady();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
    pipelineManager = scm.getPipelineManager();
    ratisPipelineOne = pipelineManager.getPipeline(
        containerManager.allocateContainer(
        RATIS, THREE, "testOwner").getPipelineID());
    ratisPipelineTwo = pipelineManager.getPipeline(
        containerManager.allocateContainer(
        RATIS, THREE, "testOwner").getPipelineID());
    // At this stage, there should be 2 pipeline one with 1 open container each.
    // Try closing the both the pipelines, one with a closed container and
    // the other with an open container.
    timeForFailure = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_KEY,
        OzoneConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_DEFAULT
            .getDuration(), TimeUnit.MILLISECONDS);
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

  @Ignore
  // Enable this after we implement teardown pipeline logic once a datanode
  // dies.
  @Test(timeout = 300_000L)
  public void testPipelineFail() throws InterruptedException, IOException,
      TimeoutException {
    Assert.assertEquals(ratisPipelineOne.getPipelineState(),
        Pipeline.PipelineState.OPEN);
    Pipeline pipelineToFail = ratisPipelineOne;
    DatanodeDetails dnToFail = pipelineToFail.getFirstNode();
    cluster.shutdownHddsDatanode(dnToFail);

    // wait for sufficient time for the callback to be triggered
    Thread.sleep(3 * timeForFailure);

    Assert.assertEquals(Pipeline.PipelineState.CLOSED,
        pipelineManager.getPipeline(ratisPipelineOne.getId())
            .getPipelineState());
    Assert.assertEquals(Pipeline.PipelineState.OPEN,
        pipelineManager.getPipeline(ratisPipelineTwo.getId())
            .getPipelineState());
    // Now restart the datanode and make sure that a new pipeline is created.
    cluster.setWaitForClusterToBeReadyTimeout(300000);
    cluster.restartHddsDatanode(dnToFail, true);
    Pipeline ratisPipelineThree = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            RATIS, THREE, "testOwner").getPipelineID());
    //Assert that new container is not created from the ratis 2 pipeline
    Assert.assertNotEquals(ratisPipelineThree.getId(),
        ratisPipelineTwo.getId());
  }
}