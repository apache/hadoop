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

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineActionsFromDatanode;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;

/**
 * Tests for Pipeline Closing.
 */
public class TestPipelineClose {

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private StorageContainerManager scm;
  private ContainerWithPipeline ratisContainer;
  private ContainerManager containerManager;
  private PipelineManager pipelineManager;

  private long pipelineDestroyTimeoutInMillis;
  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, 1000,
        TimeUnit.MILLISECONDS);
    pipelineDestroyTimeoutInMillis = 5000;
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT,
        pipelineDestroyTimeoutInMillis, TimeUnit.MILLISECONDS);
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
    pipelineManager = scm.getPipelineManager();
    ContainerInfo containerInfo = containerManager
        .allocateContainer(RATIS, THREE, "testOwner");
    ratisContainer = new ContainerWithPipeline(containerInfo,
        pipelineManager.getPipeline(containerInfo.getPipelineID()));
    pipelineManager = scm.getPipelineManager();
    // At this stage, there should be 2 pipeline one with 1 open container each.
    // Try closing the both the pipelines, one with a closed container and
    // the other with an open container.
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
  public void testPipelineCloseWithClosedContainer() throws IOException {
    Set<ContainerID> set = pipelineManager
        .getContainersInPipeline(ratisContainer.getPipeline().getId());

    ContainerID cId = ratisContainer.getContainerInfo().containerID();
    Assert.assertEquals(1, set.size());
    set.forEach(containerID -> Assert.assertEquals(containerID, cId));

    // Now close the container and it should not show up while fetching
    // containers by pipeline
    containerManager
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.CLOSE);

    Set<ContainerID> setClosed = pipelineManager
        .getContainersInPipeline(ratisContainer.getPipeline().getId());
    Assert.assertEquals(0, setClosed.size());

    pipelineManager
        .finalizeAndDestroyPipeline(ratisContainer.getPipeline(), false);
    for (DatanodeDetails dn : ratisContainer.getPipeline().getNodes()) {
      // Assert that the pipeline has been removed from Node2PipelineMap as well
      Assert.assertFalse(scm.getScmNodeManager().getPipelines(dn)
          .contains(ratisContainer.getPipeline().getId()));
    }
  }

  @Test
  public void testPipelineCloseWithOpenContainer()
      throws IOException, TimeoutException, InterruptedException {
    Set<ContainerID> setOpen = pipelineManager.getContainersInPipeline(
        ratisContainer.getPipeline().getId());
    Assert.assertEquals(1, setOpen.size());

    pipelineManager
        .finalizeAndDestroyPipeline(ratisContainer.getPipeline(), false);
    GenericTestUtils.waitFor(() -> {
      try {
        return containerManager
            .getContainer(ratisContainer.getContainerInfo().containerID())
            .getState() == HddsProtos.LifeCycleState.CLOSING;
      } catch (ContainerNotFoundException e) {
        return false;
      }
    }, 100, 10000);
  }

  @Test
  public void testPipelineCloseWithPipelineAction() throws Exception {
    List<DatanodeDetails> dns = ratisContainer.getPipeline().getNodes();
    PipelineActionsFromDatanode
        pipelineActionsFromDatanode = TestUtils
        .getPipelineActionFromDatanode(dns.get(0),
            ratisContainer.getPipeline().getId());
    // send closing action for pipeline
    PipelineActionHandler pipelineActionHandler =
        new PipelineActionHandler(pipelineManager, conf);
    pipelineActionHandler
        .onMessage(pipelineActionsFromDatanode, new EventQueue());
    Thread.sleep((int) (pipelineDestroyTimeoutInMillis * 1.2));
    OzoneContainer ozoneContainer =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer();
    List<PipelineReport> pipelineReports =
        ozoneContainer.getPipelineReport().getPipelineReportList();
    for (PipelineReport pipelineReport : pipelineReports) {
      // ensure the pipeline is not reported by any dn
      Assert.assertNotEquals(
          PipelineID.getFromProtobuf(pipelineReport.getPipelineID()),
          ratisContainer.getPipeline().getId());
    }

    try {
      pipelineManager.getPipeline(ratisContainer.getPipeline().getId());
      Assert.fail("Pipeline should not exist in SCM");
    } catch (PipelineNotFoundException e) {
    }
  }

  @Test
  public void testPipelineCloseWithLogFailure() throws IOException {

    EventQueue eventQ = (EventQueue) scm.getEventQueue();
    PipelineActionHandler pipelineActionTest =
        Mockito.mock(PipelineActionHandler.class);
    eventQ.addHandler(SCMEvents.PIPELINE_ACTIONS, pipelineActionTest);
    ArgumentCaptor<PipelineActionsFromDatanode> actionCaptor =
        ArgumentCaptor.forClass(PipelineActionsFromDatanode.class);

    ContainerInfo containerInfo = containerManager
        .allocateContainer(RATIS, THREE, "testOwner");
    ContainerWithPipeline containerWithPipeline =
        new ContainerWithPipeline(containerInfo,
            pipelineManager.getPipeline(containerInfo.getPipelineID()));
    Pipeline openPipeline = containerWithPipeline.getPipeline();
    RaftGroupId groupId = RaftGroupId.valueOf(openPipeline.getId().getId());

    try {
      pipelineManager.getPipeline(openPipeline.getId());
    } catch (PipelineNotFoundException e) {
      Assert.assertTrue("pipeline should exist", false);
    }

    DatanodeDetails datanodeDetails = openPipeline.getNodes().get(0);
    int index = cluster.getHddsDatanodeIndex(datanodeDetails);

    XceiverServerRatis xceiverRatis =
        (XceiverServerRatis) cluster.getHddsDatanodes().get(index)
        .getDatanodeStateMachine().getContainer().getWriteChannel();

    /**
     * Notify Datanode Ratis Server endpoint of a Ratis log failure.
     * This is expected to trigger an immediate pipeline actions report to SCM
     */
    xceiverRatis.handleNodeLogFailure(groupId, null);

    // verify SCM receives a pipeline action report "immediately"
    Mockito.verify(pipelineActionTest, Mockito.timeout(100))
        .onMessage(
            actionCaptor.capture(),
            Mockito.any(EventPublisher.class));

    PipelineActionsFromDatanode actionsFromDatanode =
        actionCaptor.getValue();

    // match the pipeline id
    verifyCloseForPipeline(openPipeline, actionsFromDatanode);
  }

  private boolean verifyCloseForPipeline(Pipeline pipeline,
      PipelineActionsFromDatanode report) {
    UUID uuidToFind = pipeline.getId().getId();

    boolean found = false;
    for (StorageContainerDatanodeProtocolProtos.PipelineAction action :
        report.getReport().getPipelineActionsList()) {
      if (action.getAction() ==
          StorageContainerDatanodeProtocolProtos.PipelineAction.Action.CLOSE) {
        PipelineID closedPipelineId = PipelineID.
              getFromProtobuf(action.getClosePipeline().getPipelineID());

        if (closedPipelineId.getId().equals(uuidToFind)) {
          found = true;
        }
      }
    }

    Assert.assertTrue("SCM did not receive a Close action for the Pipeline",
        found);
    return found;
  }
}
