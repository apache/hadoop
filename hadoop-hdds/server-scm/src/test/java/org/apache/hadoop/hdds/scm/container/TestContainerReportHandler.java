/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo
    .Builder;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.container.replication
    .ReplicationActivityStatus;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationRequest;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipelines.PipelineSelector;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .ContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.anyLong;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the behaviour of the ContainerReportHandler.
 */
public class TestContainerReportHandler implements EventPublisher {

  private List<Object> publishedEvents = new ArrayList<>();
  private final NodeManager nodeManager = new MockNodeManager(true, 1);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerReportHandler.class);

  @Before
  public void resetEventCollector() {
    publishedEvents.clear();
  }

  @Test
  public void test() throws IOException {
    //GIVEN
    OzoneConfiguration conf = new OzoneConfiguration();
    ContainerManager containerManager = Mockito.mock(ContainerManager.class);
    PipelineSelector selector = Mockito.mock(PipelineSelector.class);

    when(containerManager.getContainer(anyLong()))
        .thenAnswer(
            (Answer<ContainerInfo>) invocation ->
                new Builder()
                    .setReplicationFactor(ReplicationFactor.THREE)
                    .setContainerID((Long) invocation.getArguments()[0])
                    .setState(LifeCycleState.CLOSED)
                    .build()
      );

    ContainerStateManager containerStateManager =
        new ContainerStateManager(conf, containerManager, selector);

    when(containerManager.getStateManager()).thenReturn(containerStateManager);

    ReplicationActivityStatus replicationActivityStatus =
        new ReplicationActivityStatus();

    ContainerReportHandler reportHandler =
        new ContainerReportHandler(containerManager, nodeManager,
            replicationActivityStatus);

    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails dn2 = TestUtils.randomDatanodeDetails();
    DatanodeDetails dn3 = TestUtils.randomDatanodeDetails();
    DatanodeDetails dn4 = TestUtils.randomDatanodeDetails();
    nodeManager.addDatanodeInContainerMap(dn1.getUuid(), new HashSet<>());
    nodeManager.addDatanodeInContainerMap(dn2.getUuid(), new HashSet<>());
    nodeManager.addDatanodeInContainerMap(dn3.getUuid(), new HashSet<>());
    nodeManager.addDatanodeInContainerMap(dn4.getUuid(), new HashSet<>());
    PipelineSelector pipelineSelector = Mockito.mock(PipelineSelector.class);

    Pipeline pipeline = new Pipeline("leader", LifeCycleState.CLOSED,
        ReplicationType.STAND_ALONE, ReplicationFactor.THREE,
        PipelineID.randomId());

    when(pipelineSelector.getReplicationPipeline(ReplicationType.STAND_ALONE,
        ReplicationFactor.THREE)).thenReturn(pipeline);

    ContainerInfo cont1 = containerStateManager
        .allocateContainer(pipelineSelector, ReplicationType.STAND_ALONE,
            ReplicationFactor.THREE, "root").getContainerInfo();
    ContainerInfo cont2 = containerStateManager
        .allocateContainer(pipelineSelector, ReplicationType.STAND_ALONE,
            ReplicationFactor.THREE, "root").getContainerInfo();
    // Open Container
    ContainerInfo cont3 = containerStateManager
        .allocateContainer(pipelineSelector, ReplicationType.STAND_ALONE,
            ReplicationFactor.THREE, "root").getContainerInfo();

    long c1 = cont1.getContainerID();
    long c2 = cont2.getContainerID();
    long c3 = cont3.getContainerID();

    // Close remaining containers
    TestUtils.closeContainer(containerStateManager, cont1);
    TestUtils.closeContainer(containerStateManager, cont2);

    //when

    //initial reports before replication is enabled. 2 containers w 3 replicas.
    reportHandler.onMessage(
        new ContainerReportFromDatanode(dn1,
            createContainerReport(new long[] {c1, c2, c3})), this);

    reportHandler.onMessage(
        new ContainerReportFromDatanode(dn2,
            createContainerReport(new long[] {c1, c2, c3})), this);

    reportHandler.onMessage(
        new ContainerReportFromDatanode(dn3,
            createContainerReport(new long[] {c1, c2})), this);

    reportHandler.onMessage(
        new ContainerReportFromDatanode(dn4,
            createContainerReport(new long[] {})), this);

    Assert.assertEquals(0, publishedEvents.size());

    replicationActivityStatus.enableReplication();

    //no problem here
    reportHandler.onMessage(
        new ContainerReportFromDatanode(dn1,
            createContainerReport(new long[] {c1, c2})), this);

    Assert.assertEquals(0, publishedEvents.size());

    //container is missing from d2
    reportHandler.onMessage(
        new ContainerReportFromDatanode(dn2,
            createContainerReport(new long[] {c1})), this);

    Assert.assertEquals(1, publishedEvents.size());
    ReplicationRequest replicationRequest =
        (ReplicationRequest) publishedEvents.get(0);

    Assert.assertEquals(c2, replicationRequest.getContainerId());
    Assert.assertEquals(3, replicationRequest.getExpecReplicationCount());
    Assert.assertEquals(2, replicationRequest.getReplicationCount());

    //container was replicated to dn4
    reportHandler.onMessage(
        new ContainerReportFromDatanode(dn4,
            createContainerReport(new long[] {c2})), this);

    //no more event, everything is perfect
    Assert.assertEquals(1, publishedEvents.size());

    //c2 was found at dn2 (it was missing before, magic)
    reportHandler.onMessage(
        new ContainerReportFromDatanode(dn2,
            createContainerReport(new long[] {c1, c2})), this);

    //c2 is over replicated (dn1,dn2,dn3,dn4)
    Assert.assertEquals(2, publishedEvents.size());

    replicationRequest =
        (ReplicationRequest) publishedEvents.get(1);

    Assert.assertEquals(c2, replicationRequest.getContainerId());
    Assert.assertEquals(3, replicationRequest.getExpecReplicationCount());
    Assert.assertEquals(4, replicationRequest.getReplicationCount());

  }

  private ContainerReportsProto createContainerReport(long[] containerIds) {

    ContainerReportsProto.Builder crBuilder =
        ContainerReportsProto.newBuilder();

    for (long containerId : containerIds) {
      org.apache.hadoop.hdds.protocol.proto
          .StorageContainerDatanodeProtocolProtos.ContainerInfo.Builder
          ciBuilder = org.apache.hadoop.hdds.protocol.proto
          .StorageContainerDatanodeProtocolProtos.ContainerInfo.newBuilder();
      ciBuilder.setFinalhash("e16cc9d6024365750ed8dbd194ea46d2")
          .setSize(5368709120L)
          .setUsed(2000000000L)
          .setKeyCount(100000000L)
          .setReadCount(100000000L)
          .setWriteCount(100000000L)
          .setReadBytes(2000000000L)
          .setWriteBytes(2000000000L)
          .setContainerID(containerId)
          .setDeleteTransactionId(0);

      crBuilder.addReports(ciBuilder.build());
    }

    return crBuilder.build();
  }

  @Override
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void fireEvent(
      EVENT_TYPE event, PAYLOAD payload) {
    LOG.info("Event is published: {}", payload);
    publishedEvents.add(payload);
  }
}