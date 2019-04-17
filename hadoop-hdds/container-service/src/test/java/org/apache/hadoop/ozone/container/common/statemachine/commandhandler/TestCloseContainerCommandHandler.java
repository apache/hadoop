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
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine
    .DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.TimeDuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Test cases to verify CloseContainerCommandHandler in datanode.
 */
public class TestCloseContainerCommandHandler {

  private final StateContext context = Mockito.mock(StateContext.class);
  private final Random random = new Random();
  private static File testDir;

  @Test
  public void testCloseContainerViaRatis()
      throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final OzoneContainer ozoneContainer =
        getOzoneContainer(conf, datanodeDetails);
    ozoneContainer.start(UUID.randomUUID().toString());
    try {
      final Container container =
          createContainer(conf, datanodeDetails, ozoneContainer);
      Mockito.verify(context.getParent(),
          Mockito.times(1)).triggerHeartbeat();
      final long containerId = container.getContainerData().getContainerID();
      final PipelineID pipelineId = PipelineID.valueOf(UUID.fromString(
          container.getContainerData().getOriginPipelineId()));

      // We have created a container via ratis.
      // Now close the container on ratis.
      final CloseContainerCommandHandler closeHandler =
          new CloseContainerCommandHandler();
      final CloseContainerCommand command = new CloseContainerCommand(
          containerId, pipelineId);

      closeHandler.handle(command, ozoneContainer, context, null);

      Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
          ozoneContainer.getContainerSet().getContainer(containerId)
              .getContainerState());

      Mockito.verify(context.getParent(),
          Mockito.times(3)).triggerHeartbeat();
    } finally {
      ozoneContainer.stop();
    }
  }

  @Test
  public void testCloseContainerViaStandalone()
      throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final OzoneContainer ozoneContainer =
        getOzoneContainer(conf, datanodeDetails);
    ozoneContainer.start(UUID.randomUUID().toString());
    try {
      final Container container =
          createContainer(conf, datanodeDetails, ozoneContainer);
      Mockito.verify(context.getParent(),
          Mockito.times(1)).triggerHeartbeat();
      final long containerId = container.getContainerData().getContainerID();
      // To quasi close specify a pipeline which doesn't exist in the datanode.
      final PipelineID pipelineId = PipelineID.randomId();

      // We have created a container via ratis. Now quasi close it.
      final CloseContainerCommandHandler closeHandler =
          new CloseContainerCommandHandler();
      final CloseContainerCommand command = new CloseContainerCommand(
          containerId, pipelineId);

      closeHandler.handle(command, ozoneContainer, context, null);

      Assert.assertEquals(ContainerProtos.ContainerDataProto.State.QUASI_CLOSED,
          ozoneContainer.getContainerSet().getContainer(containerId)
              .getContainerState());

      Mockito.verify(context.getParent(),
          Mockito.times(3)).triggerHeartbeat();
    } finally {
      ozoneContainer.stop();
    }
  }

  @Test
  public void testQuasiCloseToClose() throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final OzoneContainer ozoneContainer =
        getOzoneContainer(conf, datanodeDetails);
    ozoneContainer.start(UUID.randomUUID().toString());
    try {
      final Container container =
          createContainer(conf, datanodeDetails, ozoneContainer);
      Mockito.verify(context.getParent(),
          Mockito.times(1)).triggerHeartbeat();
      final long containerId = container.getContainerData().getContainerID();
      // A pipeline which doesn't exist in the datanode.
      final PipelineID pipelineId = PipelineID.randomId();

      // We have created a container via ratis. Now quasi close it.
      final CloseContainerCommandHandler closeHandler =
          new CloseContainerCommandHandler();
      final CloseContainerCommand command = new CloseContainerCommand(
          containerId, pipelineId);

      closeHandler.handle(command, ozoneContainer, context, null);

      Assert.assertEquals(ContainerProtos.ContainerDataProto.State.QUASI_CLOSED,
          ozoneContainer.getContainerSet().getContainer(containerId)
              .getContainerState());

      Mockito.verify(context.getParent(),
          Mockito.times(3)).triggerHeartbeat();

      // The container is quasi closed. Force close the container now.
      final CloseContainerCommand closeCommand = new CloseContainerCommand(
          containerId, pipelineId, true);

      closeHandler.handle(closeCommand, ozoneContainer, context, null);

      Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
          ozoneContainer.getContainerSet().getContainer(containerId)
              .getContainerState());

      Mockito.verify(context.getParent(),
          Mockito.times(4)).triggerHeartbeat();
    } finally {
      ozoneContainer.stop();
    }
  }

  @Test
  public void testForceCloseOpenContainer() throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final OzoneContainer ozoneContainer =
        getOzoneContainer(conf, datanodeDetails);
    ozoneContainer.start(UUID.randomUUID().toString());
    try {
      final Container container =
          createContainer(conf, datanodeDetails, ozoneContainer);
      Mockito.verify(context.getParent(),
          Mockito.times(1)).triggerHeartbeat();
      final long containerId = container.getContainerData().getContainerID();
      // A pipeline which doesn't exist in the datanode.
      final PipelineID pipelineId = PipelineID.randomId();

      final CloseContainerCommandHandler closeHandler =
          new CloseContainerCommandHandler();

      final CloseContainerCommand closeCommand = new CloseContainerCommand(
          containerId, pipelineId, true);

      closeHandler.handle(closeCommand, ozoneContainer, context, null);

      Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
          ozoneContainer.getContainerSet().getContainer(containerId)
              .getContainerState());

      Mockito.verify(context.getParent(),
          Mockito.times(3)).triggerHeartbeat();
    } finally {
      ozoneContainer.stop();
    }
  }

  @Test
  public void testQuasiCloseClosedContainer()
      throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final OzoneContainer ozoneContainer = getOzoneContainer(
        conf, datanodeDetails);
    ozoneContainer.start(UUID.randomUUID().toString());
    try {
      final Container container = createContainer(
          conf, datanodeDetails, ozoneContainer);
      Mockito.verify(context.getParent(),
          Mockito.times(1)).triggerHeartbeat();
      final long containerId = container.getContainerData().getContainerID();
      final PipelineID pipelineId = PipelineID.valueOf(UUID.fromString(
          container.getContainerData().getOriginPipelineId()));

      final CloseContainerCommandHandler closeHandler =
          new CloseContainerCommandHandler();
      final CloseContainerCommand closeCommand = new CloseContainerCommand(
          containerId, pipelineId);

      closeHandler.handle(closeCommand, ozoneContainer, context, null);

      Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
          ozoneContainer.getContainerSet().getContainer(containerId)
              .getContainerState());

      // The container is closed, now we send close command with
      // pipeline id which doesn't exist.
      // This should cause the datanode to trigger quasi close, since the
      // container is already closed, this should do nothing.
      // The command should not fail either.
      final PipelineID randomPipeline = PipelineID.randomId();
      final CloseContainerCommand quasiCloseCommand =
          new CloseContainerCommand(containerId, randomPipeline);
      closeHandler.handle(quasiCloseCommand, ozoneContainer, context, null);

      Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
          ozoneContainer.getContainerSet().getContainer(containerId)
              .getContainerState());
    } finally {
      ozoneContainer.stop();
    }
  }

  private OzoneContainer getOzoneContainer(final OzoneConfiguration conf,
      final DatanodeDetails datanodeDetails) throws IOException {
    testDir = GenericTestUtils.getTestDir(
        TestCloseContainerCommandHandler.class.getName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, testDir.getPath());
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, true);

    final DatanodeStateMachine datanodeStateMachine = Mockito.mock(
        DatanodeStateMachine.class);
    Mockito.when(datanodeStateMachine.getDatanodeDetails())
        .thenReturn(datanodeDetails);
    Mockito.when(context.getParent()).thenReturn(datanodeStateMachine);
    final OzoneContainer ozoneContainer = new  OzoneContainer(
        datanodeDetails, conf, context, null);
    return ozoneContainer;
  }

  private Container createContainer(final Configuration conf,
      final DatanodeDetails datanodeDetails,
      final OzoneContainer ozoneContainer) throws Exception  {
    final PipelineID pipelineID = PipelineID.randomId();
    final RaftGroupId raftGroupId = RaftGroupId.valueOf(pipelineID.getId());
    final RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(conf);
    final RaftPeer peer = RatisHelper.toRaftPeer(datanodeDetails);
    final RaftGroup group = RatisHelper.newRaftGroup(raftGroupId,
        Collections.singleton(datanodeDetails));
    final int maxOutstandingRequests = 100;
    final RaftClient client = RatisHelper
        .newRaftClient(SupportedRpcType.GRPC, peer, retryPolicy,
            maxOutstandingRequests,
            TimeDuration.valueOf(3, TimeUnit.SECONDS));
    Assert.assertTrue(client.groupAdd(group, peer.getId()).isSuccess());
    Thread.sleep(2000);
    final ContainerID containerId = ContainerID.valueof(
        random.nextLong() & Long.MAX_VALUE);
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setContainerID(containerId.getId());
    request.setCreateContainer(
        ContainerProtos.CreateContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(datanodeDetails.getUuidString());
    ozoneContainer.getWriteChannel().submitRequest(
        request.build(), pipelineID.getProtobuf());

    final Container container = ozoneContainer.getContainerSet().getContainer(
        containerId.getId());
    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.OPEN,
        container.getContainerState());
    return container;
  }

  /**
   * Creates a random DatanodeDetails.
   * @return DatanodeDetails
   */
  private static DatanodeDetails randomDatanodeDetails() {
    String ipAddress = "127.0.0.1";
    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID().toString())
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  @AfterClass
  public static void teardown() throws IOException {
    FileUtils.deleteDirectory(testDir);
  }
}