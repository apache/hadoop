/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_UNHEALTHY;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Test that KeyValueHandler fails certain operations when the
 * container is unhealthy.
 */
public class TestKeyValueHandlerWithUnhealthyContainer {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestKeyValueHandlerWithUnhealthyContainer.class);

  private final static String DATANODE_UUID = UUID.randomUUID().toString();
  private static final long DUMMY_CONTAINER_ID = 9999;

  @Test
  public void testRead() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleReadContainer(
            getDummyCommandRequestProto(ContainerProtos.Type.ReadContainer),
            container);
    assertThat(response.getResult(), is(CONTAINER_UNHEALTHY));
  }

  @Test
  public void testGetBlock() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleGetBlock(
            getDummyCommandRequestProto(ContainerProtos.Type.GetBlock),
            container);
    assertThat(response.getResult(), is(CONTAINER_UNHEALTHY));
  }

  @Test
  public void testGetCommittedBlockLength() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleGetCommittedBlockLength(
            getDummyCommandRequestProto(
                ContainerProtos.Type.GetCommittedBlockLength),
            container);
    assertThat(response.getResult(), is(CONTAINER_UNHEALTHY));
  }

  @Test
  public void testReadChunk() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleReadChunk(
            getDummyCommandRequestProto(
                ContainerProtos.Type.ReadChunk),
            container, null);
    assertThat(response.getResult(), is(CONTAINER_UNHEALTHY));
  }

  @Test
  public void testDeleteChunk() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleDeleteChunk(
            getDummyCommandRequestProto(
                ContainerProtos.Type.DeleteChunk),
            container);
    assertThat(response.getResult(), is(CONTAINER_UNHEALTHY));
  }

  @Test
  public void testGetSmallFile() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleGetSmallFile(
            getDummyCommandRequestProto(
                ContainerProtos.Type.GetSmallFile),
            container);
    assertThat(response.getResult(), is(CONTAINER_UNHEALTHY));
  }

  // -- Helper methods below.

  private KeyValueHandler getDummyHandler() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeDetails dnDetails = DatanodeDetails.newBuilder()
        .setUuid(DATANODE_UUID)
        .setHostName("dummyHost")
        .setIpAddress("1.2.3.4")
        .build();
    DatanodeStateMachine stateMachine = mock(DatanodeStateMachine.class);
    when(stateMachine.getDatanodeDetails()).thenReturn(dnDetails);

    StateContext context = new StateContext(
        conf, DatanodeStateMachine.DatanodeStates.RUNNING,
        stateMachine);

    return new KeyValueHandler(
        new OzoneConfiguration(),
        context,
        mock(ContainerSet.class),
        mock(VolumeSet.class),
        mock(ContainerMetrics.class));
  }

  private KeyValueContainer getMockUnhealthyContainer() {
    KeyValueContainerData containerData = mock(KeyValueContainerData.class);
    when(containerData.getState()).thenReturn(
        ContainerProtos.ContainerDataProto.State.UNHEALTHY);
    return new KeyValueContainer(containerData, new OzoneConfiguration());
  }

  /**
   * Construct fake protobuf messages for various types of requests.
   * This is tedious, however necessary to test. Protobuf classes are final
   * and cannot be mocked by Mockito.
   *
   * @param cmdType type of the container command.
   * @return
   */
  private ContainerCommandRequestProto getDummyCommandRequestProto(
      ContainerProtos.Type cmdType) {
    final ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(cmdType)
            .setContainerID(DUMMY_CONTAINER_ID)
            .setDatanodeUuid(DATANODE_UUID);

    final ContainerProtos.DatanodeBlockID fakeBlockId =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(DUMMY_CONTAINER_ID).setLocalID(1).build();

    final ContainerProtos.ChunkInfo fakeChunkInfo =
        ContainerProtos.ChunkInfo.newBuilder()
            .setChunkName("dummy")
            .setOffset(0)
            .setLen(100)
            .setChecksumData(ContainerProtos.ChecksumData.newBuilder()
                .setBytesPerChecksum(1)
                .setType(ContainerProtos.ChecksumType.CRC32)
                .build())
            .build();

    switch (cmdType) {
    case ReadContainer:
      builder.setReadContainer(
          ContainerProtos.ReadContainerRequestProto.newBuilder().build());
      break;
    case GetBlock:
      builder.setGetBlock(ContainerProtos.GetBlockRequestProto.newBuilder()
          .setBlockID(fakeBlockId).build());
      break;
    case GetCommittedBlockLength:
      builder.setGetCommittedBlockLength(
          ContainerProtos.GetCommittedBlockLengthRequestProto.newBuilder()
              .setBlockID(fakeBlockId).build());
    case ReadChunk:
      builder.setReadChunk(ContainerProtos.ReadChunkRequestProto.newBuilder()
          .setBlockID(fakeBlockId).setChunkData(fakeChunkInfo).build());
      break;
    case DeleteChunk:
      builder
          .setDeleteChunk(ContainerProtos.DeleteChunkRequestProto.newBuilder()
              .setBlockID(fakeBlockId).setChunkData(fakeChunkInfo).build());
      break;
    case GetSmallFile:
      builder
          .setGetSmallFile(ContainerProtos.GetSmallFileRequestProto.newBuilder()
              .setBlock(ContainerProtos.GetBlockRequestProto.newBuilder()
                  .setBlockID(fakeBlockId)
                  .build())
              .build());
      break;

    default:
      Assert.fail("Unhandled request type " + cmdType + " in unit test");
    }

    return builder.build();
  }
}
