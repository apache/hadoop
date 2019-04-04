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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Set;

import static org.apache.hadoop.hdds.scm.TestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.TestUtils.getReplicas;

/**
 * Test cases to verify the functionality of IncrementalContainerReportHandler.
 */
public class TestIncrementalContainerReportHandler {

  private ContainerManager containerManager;
  private ContainerStateManager containerStateManager;
  private EventPublisher publisher;

  @Before
  public void setup() throws IOException {
    final Configuration conf = new OzoneConfiguration();
    this.containerManager = Mockito.mock(ContainerManager.class);
    this.containerStateManager = new ContainerStateManager(conf);
    this.publisher = Mockito.mock(EventPublisher.class);


    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer((ContainerID)invocation.getArguments()[0]));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas((ContainerID)invocation.getArguments()[0]));

    Mockito.doAnswer(invocation -> {
      containerStateManager
          .updateContainerState((ContainerID)invocation.getArguments()[0],
              (HddsProtos.LifeCycleEvent)invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerState(
        Mockito.any(ContainerID.class),
        Mockito.any(HddsProtos.LifeCycleEvent.class));

  }

  @After
  public void tearDown() throws IOException {
    containerStateManager.close();
  }


  @Test
  public void testClosingToClosed() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(containerManager);
    final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
    final DatanodeDetails datanodeOne = TestUtils.randomDatanodeDetails();
    final DatanodeDetails datanodeTwo = TestUtils.randomDatanodeDetails();
    final DatanodeDetails datanodeThree = TestUtils.randomDatanodeDetails();
    final Set<ContainerReplica> containerReplicas = getReplicas(
        container.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne, datanodeTwo, datanodeThree);

    containerStateManager.loadContainer(container);
    containerReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            container.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    Assert.assertEquals(LifeCycleState.CLOSED, container.getState());
  }

  @Test
  public void testClosingToQuasiClosed() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(containerManager);
    final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
    final DatanodeDetails datanodeOne = TestUtils.randomDatanodeDetails();
    final DatanodeDetails datanodeTwo = TestUtils.randomDatanodeDetails();
    final DatanodeDetails datanodeThree = TestUtils.randomDatanodeDetails();
    final Set<ContainerReplica> containerReplicas = getReplicas(
        container.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne, datanodeTwo, datanodeThree);

    containerStateManager.loadContainer(container);
    containerReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            container.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });


    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.QUASI_CLOSED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    Assert.assertEquals(LifeCycleState.QUASI_CLOSED, container.getState());
  }

  @Test
  public void testQuasiClosedToClosed() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(containerManager);
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

    containerStateManager.loadContainer(container);
    containerReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            container.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            datanodeThree.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icr, publisher);
    Assert.assertEquals(LifeCycleState.CLOSED, container.getState());
  }

  private static IncrementalContainerReportProto
      getIncrementalContainerReportProto(
          final ContainerID containerId,
          final ContainerReplicaProto.State state,
          final String originNodeId) {
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
            .setBlockCommitSequenceId(10000L)
            .setDeleteTransactionId(0)
            .build();
    return crBuilder.addReport(replicaProto).build();
  }
}