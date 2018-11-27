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
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Set;

import static org.apache.hadoop.hdds.scm.container
    .TestContainerReportHelper.addContainerToContainerManager;
import static org.apache.hadoop.hdds.scm.container
    .TestContainerReportHelper.getContainer;
import static org.apache.hadoop.hdds.scm.container
    .TestContainerReportHelper.getReplicas;
import static org.apache.hadoop.hdds.scm.container
    .TestContainerReportHelper.mockUpdateContainerReplica;
import static org.apache.hadoop.hdds.scm.container
    .TestContainerReportHelper.mockUpdateContainerState;

/**
 * Test cases to verify the functionality of IncrementalContainerReportHandler.
 */
public class TestIncrementalContainerReportHandler {

  @Test
  public void testClosingToClosed() throws IOException {
    final ContainerManager containerManager = Mockito.mock(
        ContainerManager.class);
    final PipelineManager pipelineManager = Mockito.mock(PipelineManager.class);
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(
            pipelineManager, containerManager);
    final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
    final DatanodeDetails datanodeOne = TestUtils.randomDatanodeDetails();
    final DatanodeDetails datanodeTwo = TestUtils.randomDatanodeDetails();
    final DatanodeDetails datanodeThree = TestUtils.randomDatanodeDetails();
    final Set<ContainerReplica> containerReplicas = getReplicas(
        container.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne, datanodeTwo, datanodeThree);

    addContainerToContainerManager(
        containerManager, container, containerReplicas);
    mockUpdateContainerReplica(
        containerManager, container, containerReplicas);
    mockUpdateContainerState(containerManager, container,
        LifeCycleEvent.CLOSE, LifeCycleState.CLOSED);

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            datanodeOne.getUuidString());
    final EventPublisher publisher = Mockito.mock(EventPublisher.class);
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    Assert.assertEquals(
        LifeCycleState.CLOSED, container.getState());
  }

  @Test
  public void testClosingToQuasiClosed() throws IOException {
    final ContainerManager containerManager = Mockito.mock(
        ContainerManager.class);
    final PipelineManager pipelineManager = Mockito.mock(PipelineManager.class);
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(
            pipelineManager, containerManager);
    final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
    final DatanodeDetails datanodeOne = TestUtils.randomDatanodeDetails();
    final DatanodeDetails datanodeTwo = TestUtils.randomDatanodeDetails();
    final DatanodeDetails datanodeThree = TestUtils.randomDatanodeDetails();
    final Set<ContainerReplica> containerReplicas = getReplicas(
        container.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne, datanodeTwo, datanodeThree);

    addContainerToContainerManager(
        containerManager, container, containerReplicas);
    mockUpdateContainerReplica(
        containerManager, container, containerReplicas);
    mockUpdateContainerState(containerManager, container,
        LifeCycleEvent.QUASI_CLOSE, LifeCycleState.QUASI_CLOSED);

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.QUASI_CLOSED,
            datanodeOne.getUuidString());
    final EventPublisher publisher = Mockito.mock(EventPublisher.class);
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    Assert.assertEquals(
        LifeCycleState.QUASI_CLOSED, container.getState());
  }

  @Test
  public void testQuasiClosedToClosed() throws IOException {
    final ContainerManager containerManager = Mockito.mock(
        ContainerManager.class);
    final PipelineManager pipelineManager = Mockito.mock(PipelineManager.class);
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(
            pipelineManager, containerManager);
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final DatanodeDetails datanodeOne = TestUtils.randomDatanodeDetails();
    final DatanodeDetails datanodeTwo = TestUtils.randomDatanodeDetails();
    final DatanodeDetails datanodeThree = TestUtils.randomDatanodeDetails();
    final Set<ContainerReplica> containerReplicas = getReplicas(
        container.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne, datanodeTwo);
    containerReplicas.addAll(getReplicas(
        container.containerID(),
        ContainerReplicaProto.State.QUASI_CLOSED,
        datanodeThree));


    addContainerToContainerManager(
        containerManager, container, containerReplicas);
    mockUpdateContainerReplica(
        containerManager, container, containerReplicas);
    mockUpdateContainerState(containerManager, container,
        LifeCycleEvent.FORCE_CLOSE, LifeCycleState.CLOSED);

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.QUASI_CLOSED,
            datanodeOne.getUuidString(), 999999L);
    final EventPublisher publisher = Mockito.mock(EventPublisher.class);
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);

    // SCM should issue force close.
    Mockito.verify(publisher, Mockito.times(1))
        .fireEvent(Mockito.any(), Mockito.any());

    final IncrementalContainerReportProto containerReportTwo =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            datanodeOne.getUuidString(), 999999L);
    final IncrementalContainerReportFromDatanode icrTwoFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReportTwo);
    reportHandler.onMessage(icrTwoFromDatanode, publisher);
    Assert.assertEquals(
        LifeCycleState.CLOSED, container.getState());
  }

  private static IncrementalContainerReportProto
      getIncrementalContainerReportProto(
          final ContainerID containerId,
          final ContainerReplicaProto.State state,
          final String originNodeId) {
    return getIncrementalContainerReportProto(
        containerId, state, originNodeId, 100L);
  }

  private static IncrementalContainerReportProto
      getIncrementalContainerReportProto(
          final ContainerID containerId,
          final ContainerReplicaProto.State state,
          final String originNodeId, final long bcsid) {
    final IncrementalContainerReportProto.Builder crBuilder =
        IncrementalContainerReportProto.newBuilder();
    final ContainerReplicaProto replicaProto =
        ContainerReplicaProto.newBuilder()
            .setContainerID(containerId.getId())
            .setState(state)
            .setOriginNodeId(originNodeId)
            .setFinalhash("e16cc9d6024365750ed8dbd194ea46d2")
            .setSize(5368709120L)
            .setUsed(2000000000L)
            .setKeyCount(100000000L)
            .setReadCount(100000000L)
            .setWriteCount(100000000L)
            .setReadBytes(2000000000L)
            .setWriteBytes(2000000000L)
            .setBlockCommitSequenceId(bcsid)
            .setDeleteTransactionId(0)
            .build();
    return crBuilder.addReport(replicaProto).build();
  }
}