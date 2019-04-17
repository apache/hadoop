/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests for RatisPipelineUtils.
 */
public class TestRatisPipelineUtils {

  private static MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private static PipelineManager pipelineManager;

  public void init(int numDatanodes) throws Exception {
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        GenericTestUtils.getRandomizedTempPath());
    cluster = MiniOzoneCluster.newBuilder(conf)
            .setNumDatanodes(numDatanodes)
            .setHbInterval(1000)
            .setHbProcessorInterval(1000)
            .build();
    cluster.waitForClusterToBeReady();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    pipelineManager = scm.getPipelineManager();
  }

  @After
  public void cleanup() {
    cluster.shutdown();
  }

  @Test(timeout = 30000)
  public void testAutomaticPipelineCreationOnPipelineDestroy()
      throws Exception {
    init(6);
    // make sure two pipelines are created
    waitForPipelines(2);
    List<Pipeline> pipelines = pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN);
    for (Pipeline pipeline : pipelines) {
      pipelineManager.finalizeAndDestroyPipeline(pipeline, false);
    }
    // make sure two pipelines are created
    waitForPipelines(2);
  }

  @Test(timeout = 30000)
  public void testPipelineCreationOnNodeRestart() throws Exception {
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        5, TimeUnit.SECONDS);
    init(3);
    // make sure a pipelines is created
    waitForPipelines(1);
    List<HddsDatanodeService> dns = new ArrayList<>(cluster.getHddsDatanodes());

    List<Pipeline> pipelines =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);
    for (HddsDatanodeService dn : dns) {
      cluster.shutdownHddsDatanode(dn.getDatanodeDetails());
    }

    // try creating another pipeline now
    try {
      RatisPipelineUtils.createPipeline(pipelines.get(0), conf);
      Assert.fail("pipeline creation should fail after shutting down pipeline");
    } catch (IOException ioe) {
      // in case the pipeline creation fails, MultipleIOException is thrown
      Assert.assertTrue(ioe instanceof MultipleIOException);
    }

    // make sure pipelines is destroyed
    waitForPipelines(0);
    for (HddsDatanodeService dn : dns) {
      cluster.restartHddsDatanode(dn.getDatanodeDetails(), false);
    }

    // destroy the existing pipelines
    for (Pipeline pipeline : pipelines) {
      pipelineManager.finalizeAndDestroyPipeline(pipeline, false);
    }
    // make sure pipelines is created after node start
    pipelineManager.triggerPipelineCreation();
    waitForPipelines(1);
  }

  private void waitForPipelines(int numPipelines)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN)
        .size() == numPipelines, 100, 40000);
  }
}
