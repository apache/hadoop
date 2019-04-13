/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.safemode;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This class tests OneReplicaPipelineSafeModeRule.
 */
public class TestOneReplicaPipelineSafeModeRule {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private OneReplicaPipelineSafeModeRule rule;
  private SCMPipelineManager pipelineManager;
  private EventQueue eventQueue;


  private void setup(int nodes, int pipelineFactorThreeCount,
      int pipelineFactorOneCount) throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK, true);
    ozoneConfiguration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        folder.newFolder().toString());

    List<ContainerInfo> containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(1));
    MockNodeManager mockNodeManager = new MockNodeManager(true, nodes);

    eventQueue = new EventQueue();
    pipelineManager =
        new SCMPipelineManager(ozoneConfiguration, mockNodeManager,
            eventQueue);

    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(mockNodeManager,
            pipelineManager.getStateManager(), ozoneConfiguration);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    createPipelines(pipelineFactorThreeCount,
        HddsProtos.ReplicationFactor.THREE);
    createPipelines(pipelineFactorOneCount,
        HddsProtos.ReplicationFactor.ONE);

    SCMSafeModeManager scmSafeModeManager =
        new SCMSafeModeManager(ozoneConfiguration, containers,
            pipelineManager, eventQueue);

    rule = scmSafeModeManager.getOneReplicaPipelineSafeModeRule();
  }

  @Test
  public void testOneReplicaPipelineRule() throws Exception {

    // As with 30 nodes, We can create 7 pipelines with replication factor 3.
    // (This is because in node manager for every 10 nodes, 7 nodes are
    // healthy, 2 are stale one is dead.)
    int nodes = 30;
    int pipelineFactorThreeCount = 7;
    int pipelineCountOne = 0;
    setup(nodes, pipelineFactorThreeCount, pipelineCountOne);

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(
            LoggerFactory.getLogger(SCMSafeModeManager.class));

    List<Pipeline> pipelines = pipelineManager.getPipelines();
    for (int i = 0; i < pipelineFactorThreeCount -1; i++) {
      firePipelineEvent(pipelines.get(i));
    }

    // As 90% of 7 with ceil is 7, if we send 6 pipeline reports, rule
    // validate should be still false.

    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "reported count is 6"), 1000, 5000);

    Assert.assertFalse(rule.validate());

    //Fire last pipeline event from datanode.
    firePipelineEvent(pipelines.get(pipelineFactorThreeCount - 1));

    GenericTestUtils.waitFor(() -> rule.validate(), 1000, 5000);

  }


  @Test
  public void testOneReplicaPipelineRuleMixedPipelines() throws Exception {

    // As with 30 nodes, We can create 7 pipelines with replication factor 3.
    // (This is because in node manager for every 10 nodes, 7 nodes are
    // healthy, 2 are stale one is dead.)
    int nodes = 30;
    int pipelineCountThree = 7;
    int pipelineCountOne = 21;

    setup(nodes, pipelineCountThree, pipelineCountOne);

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(
            LoggerFactory.getLogger(SCMSafeModeManager.class));

    List<Pipeline> pipelines =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE);
    for (int i = 0; i < pipelineCountOne; i++) {
      firePipelineEvent(pipelines.get(i));
    }

    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "reported count is 0"), 1000, 5000);

    // fired events for one node ratis pipeline, so we will be still false.
    Assert.assertFalse(rule.validate());

    pipelines =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);
    for (int i = 0; i < pipelineCountThree - 1; i++) {
      firePipelineEvent(pipelines.get(i));
    }

    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "reported count is 6"), 1000, 5000);

    //Fire last pipeline event from datanode.
    firePipelineEvent(pipelines.get(pipelineCountThree - 1));

    GenericTestUtils.waitFor(() -> rule.validate(), 1000, 5000);

  }



  private void createPipelines(int count,
      HddsProtos.ReplicationFactor factor) throws Exception {
    for (int i = 0; i < count; i++) {
      pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
          factor);
    }
  }

  private void firePipelineEvent(Pipeline pipeline) {
    PipelineReportsProto.Builder reportBuilder =
        PipelineReportsProto.newBuilder();

    reportBuilder.addPipelineReport(PipelineReport.newBuilder()
        .setPipelineID(pipeline.getId().getProtobuf()));

    if (pipeline.getFactor() == HddsProtos.ReplicationFactor.THREE) {
      eventQueue.fireEvent(SCMEvents.PROCESSED_PIPELINE_REPORT,
          new SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode(
              pipeline.getNodes().get(0), reportBuilder.build()));
      eventQueue.fireEvent(SCMEvents.PROCESSED_PIPELINE_REPORT,
          new SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode(
              pipeline.getNodes().get(1), reportBuilder.build()));
      eventQueue.fireEvent(SCMEvents.PROCESSED_PIPELINE_REPORT,
          new SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode(
              pipeline.getNodes().get(2), reportBuilder.build()));
    } else {
      eventQueue.fireEvent(SCMEvents.PROCESSED_PIPELINE_REPORT,
          new SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode(
              pipeline.getNodes().get(0), reportBuilder.build()));
    }
  }
}
