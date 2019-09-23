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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine
    .DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static java.util.Collections.singletonMap;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases to verify CloseContainerCommandHandler in datanode.
 */
public class TestCloseContainerCommandHandler {

  private static final long CONTAINER_ID = 123L;

  private OzoneContainer ozoneContainer;
  private StateContext context;
  private XceiverServerSpi writeChannel;
  private Container container;
  private Handler containerHandler;
  private PipelineID pipelineID;
  private PipelineID nonExistentPipelineID = PipelineID.randomId();

  private CloseContainerCommandHandler subject =
      new CloseContainerCommandHandler();

  @Before
  public void before() throws Exception {
    context = mock(StateContext.class);
    DatanodeStateMachine dnStateMachine = mock(DatanodeStateMachine.class);
    when(dnStateMachine.getDatanodeDetails())
        .thenReturn(randomDatanodeDetails());
    when(context.getParent()).thenReturn(dnStateMachine);

    pipelineID = PipelineID.randomId();

    KeyValueContainerData data = new KeyValueContainerData(CONTAINER_ID, GB,
        pipelineID.getId().toString(), null);

    container = new KeyValueContainer(data, new OzoneConfiguration());
    ContainerSet containerSet = new ContainerSet();
    containerSet.addContainer(container);

    containerHandler = mock(Handler.class);
    ContainerController controller = new ContainerController(containerSet,
        singletonMap(ContainerProtos.ContainerType.KeyValueContainer,
            containerHandler));

    writeChannel = mock(XceiverServerSpi.class);
    ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(ozoneContainer.getWriteChannel()).thenReturn(writeChannel);
    when(writeChannel.isExist(pipelineID.getProtobuf())).thenReturn(true);
    when(writeChannel.isExist(nonExistentPipelineID.getProtobuf()))
        .thenReturn(false);
  }

  @Test
  public void closeContainerWithPipeline() throws Exception {
    // close a container that's associated with an existing pipeline
    subject.handle(closeWithKnownPipeline(), ozoneContainer, context, null);

    verify(containerHandler)
        .markContainerForClose(container);
    verify(writeChannel)
        .submitRequest(any(), eq(pipelineID.getProtobuf()));
    verify(containerHandler, never())
        .quasiCloseContainer(container);
  }

  @Test
  public void closeContainerWithoutPipeline() throws IOException {
    // close a container that's NOT associated with an open pipeline
    subject.handle(closeWithUnknownPipeline(), ozoneContainer, context, null);

    verify(containerHandler)
        .markContainerForClose(container);
    verify(writeChannel, never())
        .submitRequest(any(), any());
    // Container in CLOSING state is moved to UNHEALTHY if pipeline does not
    // exist. Container should not exist in CLOSING state without a pipeline.
    verify(containerHandler)
        .markContainerUnhealthy(container);
  }

  @Test
  public void forceCloseQuasiClosedContainer() throws Exception {
    // force-close a container that's already quasi closed
    container.getContainerData()
        .setState(ContainerProtos.ContainerDataProto.State.QUASI_CLOSED);

    subject.handle(forceCloseWithoutPipeline(), ozoneContainer, context, null);

    verify(writeChannel, never())
        .submitRequest(any(), any());
    verify(containerHandler)
        .closeContainer(container);
  }

  @Test
  public void forceCloseOpenContainer() throws Exception {
    // force-close a container that's NOT associated with an open pipeline
    subject.handle(forceCloseWithoutPipeline(), ozoneContainer, context, null);

    verify(writeChannel, never())
        .submitRequest(any(), any());
    // Container in CLOSING state is moved to UNHEALTHY if pipeline does not
    // exist. Container should not exist in CLOSING state without a pipeline.
    verify(containerHandler)
        .markContainerUnhealthy(container);
  }

  @Test
  public void forceCloseOpenContainerWithPipeline() throws Exception {
    // force-close a container that's associated with an existing pipeline
    subject.handle(forceCloseWithPipeline(), ozoneContainer, context, null);

    verify(containerHandler)
        .markContainerForClose(container);
    verify(writeChannel)
        .submitRequest(any(), any());
    verify(containerHandler, never())
        .quasiCloseContainer(container);
    verify(containerHandler, never())
        .closeContainer(container);
  }

  @Test
  public void closeAlreadyClosedContainer() throws Exception {
    container.getContainerData()
        .setState(ContainerProtos.ContainerDataProto.State.CLOSED);

    // Since the container is already closed, these commands should do nothing,
    // neither should they fail
    subject.handle(closeWithUnknownPipeline(), ozoneContainer, context, null);
    subject.handle(closeWithKnownPipeline(), ozoneContainer, context, null);

    verify(containerHandler, never())
        .markContainerForClose(container);
    verify(containerHandler, never())
        .quasiCloseContainer(container);
    verify(containerHandler, never())
        .closeContainer(container);
    verify(writeChannel, never())
        .submitRequest(any(), any());
  }

  private CloseContainerCommand closeWithKnownPipeline() {
    return new CloseContainerCommand(CONTAINER_ID, pipelineID);
  }

  private CloseContainerCommand closeWithUnknownPipeline() {
    return new CloseContainerCommand(CONTAINER_ID, nonExistentPipelineID);
  }

  private CloseContainerCommand forceCloseWithPipeline() {
    return new CloseContainerCommand(CONTAINER_ID, pipelineID, true);
  }

  private CloseContainerCommand forceCloseWithoutPipeline() {
    return new CloseContainerCommand(CONTAINER_ID, nonExistentPipelineID, true);
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