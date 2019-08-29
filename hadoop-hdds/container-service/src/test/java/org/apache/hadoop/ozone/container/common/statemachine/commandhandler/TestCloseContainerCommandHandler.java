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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine
    .DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

/**
 * Test cases to verify CloseContainerCommandHandler in datanode.
 */
public class TestCloseContainerCommandHandler {

  private final Random random = new Random();

  private File testDir;
  private OzoneConfiguration conf;
  private DatanodeDetails datanodeDetails;
  private OzoneContainer ozoneContainer;
  private StateContext context;

  @Before
  public void before() throws Exception {
    testDir = GenericTestUtils.getTestDir(
        TestCloseContainerCommandHandler.class.getName() + UUID.randomUUID());
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, testDir.getPath());
    conf.set(ScmConfigKeys.DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY, "1s");
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, true);
    datanodeDetails = randomDatanodeDetails();
    context = Mockito.mock(StateContext.class);
    ozoneContainer = getOzoneContainer(conf, datanodeDetails);
    ozoneContainer.start(UUID.randomUUID().toString());
  }

  @After
  public void after() throws IOException {
    ozoneContainer.stop();
    FileUtils.deleteDirectory(testDir);
  }

  @Test
  public void testCloseContainerViaStandalone()
      throws Exception {
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
  }

  @Test
  public void testQuasiCloseToClose() throws Exception {
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
  }

  @Test
  public void testForceCloseOpenContainer() throws Exception {
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
  }

  @Test
  public void testQuasiCloseClosedContainer()
      throws Exception {
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
  }

  @Test
  public void zzzCloseContainerViaRatis()
      throws Exception {
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
  }

  private OzoneContainer getOzoneContainer(final OzoneConfiguration conf,
      final DatanodeDetails datanodeDetails) throws IOException {
    final DatanodeStateMachine datanodeStateMachine = Mockito.mock(
        DatanodeStateMachine.class);
    Mockito.when(datanodeStateMachine.getDatanodeDetails())
        .thenReturn(datanodeDetails);
    Mockito.when(context.getParent()).thenReturn(datanodeStateMachine);
    return new  OzoneContainer(
        datanodeDetails, conf, context, null);
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
            TimeDuration.valueOf(5, TimeUnit.SECONDS));
    RaftPeerId peerId = peer.getId();
    Assert.assertTrue(client.groupAdd(group, peerId).isSuccess());
    long containerId = random.nextLong() & Long.MAX_VALUE;
    ContainerProtos.ContainerCommandRequestProto request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.CreateContainer)
        .setContainerID(containerId)
        .setCreateContainer(
            ContainerProtos.CreateContainerRequestProto.getDefaultInstance())
        .setDatanodeUuid(datanodeDetails.getUuidString())
        .build();
    LambdaTestUtils.await(
        10 * 1000,
        () -> RaftProtos.RaftPeerRole.LEADER.equals(
            client.getGroupInfo(raftGroupId, peerId).getRoleInfoProto().getRole()),
        () -> 100,
        (timeout, ex) -> ex != null ? ex : new TimeoutException("timeout"));

    ozoneContainer.getWriteChannel().submitRequest(request, pipelineID.getProtobuf());
    ContainerSet containerSet = ozoneContainer.getContainerSet();
    LambdaTestUtils.await(
        20 * 1000,
        () -> null != containerSet.getContainer(containerId),
        () -> 5000,
        (timeout, ex) -> ex != null ? ex : new TimeoutException("timeout"));
    final Container container = containerSet.getContainer(containerId);
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
}