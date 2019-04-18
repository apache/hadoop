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

package org.apache.hadoop.hdds.scm.safemode;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This class tests HealthyPipelineSafeMode rule.
 */
public class TestHealthyPipelineSafeModeRule {

  @Test
  public void testHealthyPipelineSafeModeRuleWithNoPipelines()
      throws Exception {

    String storageDir = GenericTestUtils.getTempPath(
        TestHealthyPipelineSafeModeRule.class.getName() + UUID.randomUUID());
    try {
      EventQueue eventQueue = new EventQueue();
      List<ContainerInfo> containers = new ArrayList<>();
      containers.addAll(HddsTestUtils.getContainerInfo(1));

      OzoneConfiguration config = new OzoneConfiguration();
      MockNodeManager nodeManager = new MockNodeManager(true, 0);
      config.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir);
      // enable pipeline check
      config.setBoolean(
          HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK, true);


      SCMPipelineManager pipelineManager = new SCMPipelineManager(config,
          nodeManager, eventQueue);
      PipelineProvider mockRatisProvider =
          new MockRatisPipelineProvider(nodeManager,
              pipelineManager.getStateManager(), config);
      pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
          mockRatisProvider);
      SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(
          config, containers, pipelineManager, eventQueue);

      HealthyPipelineSafeModeRule healthyPipelineSafeModeRule =
          scmSafeModeManager.getHealthyPipelineSafeModeRule();

      // This should be immediately satisfied, as no pipelines are there yet.
      Assert.assertTrue(healthyPipelineSafeModeRule.validate());
    } finally {
      FileUtil.fullyDelete(new File(storageDir));
    }

  }


  @Test
  public void testHealthyPipelineSafeModeRuleWithPipelines() throws Exception {

    String storageDir = GenericTestUtils.getTempPath(
        TestHealthyPipelineSafeModeRule.class.getName() + UUID.randomUUID());

    try {
      EventQueue eventQueue = new EventQueue();
      List<ContainerInfo> containers = new ArrayList<>();
      containers.addAll(HddsTestUtils.getContainerInfo(1));

      OzoneConfiguration config = new OzoneConfiguration();

      // In Mock Node Manager, first 8 nodes are healthy, next 2 nodes are
      // stale and last one is dead, and this repeats. So for a 12 node, 9
      // healthy, 2 stale and one dead.
      MockNodeManager nodeManager = new MockNodeManager(true, 12);
      config.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir);
      // enable pipeline check
      config.setBoolean(
          HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK, true);


      SCMPipelineManager pipelineManager = new SCMPipelineManager(config,
          nodeManager, eventQueue);

      PipelineProvider mockRatisProvider =
          new MockRatisPipelineProvider(nodeManager,
              pipelineManager.getStateManager(), config);
      pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
          mockRatisProvider);

      // Create 3 pipelines
      Pipeline pipeline1 =
          pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);
      Pipeline pipeline2 =
          pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);
      Pipeline pipeline3 =
          pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);


      SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(
          config, containers, pipelineManager, eventQueue);

      HealthyPipelineSafeModeRule healthyPipelineSafeModeRule =
          scmSafeModeManager.getHealthyPipelineSafeModeRule();


      // No datanodes have sent pipelinereport from datanode
      Assert.assertFalse(healthyPipelineSafeModeRule.validate());

      // Fire pipeline report from all datanodes in first pipeline, as here we
      // have 3 pipelines, 10% is 0.3, when doing ceil it is 1. So, we should
      // validate should return true after fire pipeline event


      //Here testing with out pipelinereport handler, so not moving created
      // pipelines to allocated state, as pipelines changing to healthy is
      // handled by pipeline report handler. So, leaving pipeline's in pipeline
      // manager in open state for test case simplicity.

      firePipelineEvent(pipeline1, eventQueue);
      GenericTestUtils.waitFor(() -> healthyPipelineSafeModeRule.validate(),
          1000, 5000);
    } finally {
      FileUtil.fullyDelete(new File(storageDir));
    }

  }


  @Test
  public void testHealthyPipelineSafeModeRuleWithMixedPipelines()
      throws Exception {

    String storageDir = GenericTestUtils.getTempPath(
        TestHealthyPipelineSafeModeRule.class.getName() + UUID.randomUUID());

    try {
      EventQueue eventQueue = new EventQueue();
      List<ContainerInfo> containers = new ArrayList<>();
      containers.addAll(HddsTestUtils.getContainerInfo(1));

      OzoneConfiguration config = new OzoneConfiguration();

      // In Mock Node Manager, first 8 nodes are healthy, next 2 nodes are
      // stale and last one is dead, and this repeats. So for a 12 node, 9
      // healthy, 2 stale and one dead.
      MockNodeManager nodeManager = new MockNodeManager(true, 12);
      config.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir);
      // enable pipeline check
      config.setBoolean(
          HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK, true);


      SCMPipelineManager pipelineManager = new SCMPipelineManager(config,
          nodeManager, eventQueue);
      PipelineProvider mockRatisProvider =
          new MockRatisPipelineProvider(nodeManager,
              pipelineManager.getStateManager(), config);
      pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
          mockRatisProvider);

      // Create 3 pipelines
      Pipeline pipeline1 =
          pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE);
      Pipeline pipeline2 =
          pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);
      Pipeline pipeline3 =
          pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);


      SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(
          config, containers, pipelineManager, eventQueue);

      HealthyPipelineSafeModeRule healthyPipelineSafeModeRule =
          scmSafeModeManager.getHealthyPipelineSafeModeRule();


      // No datanodes have sent pipelinereport from datanode
      Assert.assertFalse(healthyPipelineSafeModeRule.validate());


      GenericTestUtils.LogCapturer logCapturer =
          GenericTestUtils.LogCapturer.captureLogs(LoggerFactory.getLogger(
              SCMSafeModeManager.class));

      // fire event with pipeline report with ratis type and factor 1
      // pipeline, validate() should return false
      firePipelineEvent(pipeline1, eventQueue);

      GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
          "reported count is 0"),
          1000, 5000);
      Assert.assertFalse(healthyPipelineSafeModeRule.validate());

      firePipelineEvent(pipeline2, eventQueue);
      firePipelineEvent(pipeline3, eventQueue);

      GenericTestUtils.waitFor(() -> healthyPipelineSafeModeRule.validate(),
          1000, 5000);

    } finally {
      FileUtil.fullyDelete(new File(storageDir));
    }

  }


  private void firePipelineEvent(Pipeline pipeline, EventQueue eventQueue) {
    PipelineReportsProto.Builder reportBuilder = PipelineReportsProto
        .newBuilder();

    reportBuilder.addPipelineReport(PipelineReport.newBuilder()
        .setPipelineID(pipeline.getId().getProtobuf()));

    // Here no need to fire event from 3 nodes, as already pipeline is in
    // open state, but doing it.
    eventQueue.fireEvent(SCMEvents.PROCESSED_PIPELINE_REPORT,
        new SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode(
            pipeline.getNodes().get(0), reportBuilder.build()));
  }

}
