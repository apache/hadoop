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

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.collect.Maps;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto
    .ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto
    .ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test-cases to verify the functionality of HddsDispatcher.
 */
public class TestHddsDispatcher {

  @Test
  public void testContainerCloseActionWhenFull() throws IOException {
    String testDir = GenericTestUtils.getTempPath(
        TestHddsDispatcher.class.getSimpleName());
    try {
      UUID scmId = UUID.randomUUID();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, testDir);
      DatanodeDetails dd = randomDatanodeDetails();
      ContainerSet containerSet = new ContainerSet();
      VolumeSet volumeSet = new VolumeSet(dd.getUuidString(), conf);
      DatanodeStateMachine stateMachine = Mockito.mock(
          DatanodeStateMachine.class);
      StateContext context = Mockito.mock(StateContext.class);
      Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(dd);
      Mockito.when(context.getParent()).thenReturn(stateMachine);
      KeyValueContainerData containerData = new KeyValueContainerData(1L,
          (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
          dd.getUuidString());
      Container container = new KeyValueContainer(containerData, conf);
      container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(),
          scmId.toString());
      containerSet.addContainer(container);
      ContainerMetrics metrics = ContainerMetrics.create(conf);
      Map<ContainerType, Handler> handlers = Maps.newHashMap();
      for (ContainerType containerType : ContainerType.values()) {
        handlers.put(containerType,
            Handler.getHandlerForContainerType(containerType, conf, context,
                containerSet, volumeSet, metrics));
      }
      HddsDispatcher hddsDispatcher = new HddsDispatcher(
          conf, containerSet, volumeSet, handlers, context, metrics);
      hddsDispatcher.setScmId(scmId.toString());
      ContainerCommandResponseProto responseOne = hddsDispatcher
          .dispatch(getWriteChunkRequest(dd.getUuidString(), 1L, 1L), null);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          responseOne.getResult());
      verify(context, times(0))
          .addContainerActionIfAbsent(Mockito.any(ContainerAction.class));
      containerData.setBytesUsed(Double.valueOf(
          StorageUnit.MB.toBytes(950)).longValue());
      ContainerCommandResponseProto responseTwo = hddsDispatcher
          .dispatch(getWriteChunkRequest(dd.getUuidString(), 1L, 2L), null);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          responseTwo.getResult());
      verify(context, times(1))
          .addContainerActionIfAbsent(Mockito.any(ContainerAction.class));

    } finally {
      FileUtils.deleteDirectory(new File(testDir));
    }

  }

  @Test
  public void testCreateContainerWithWriteChunk() throws IOException {
    String testDir =
        GenericTestUtils.getTempPath(TestHddsDispatcher.class.getSimpleName());
    try {
      UUID scmId = UUID.randomUUID();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, testDir);
      DatanodeDetails dd = randomDatanodeDetails();
      HddsDispatcher hddsDispatcher = createDispatcher(dd, scmId, conf);
      ContainerCommandRequestProto writeChunkRequest =
          getWriteChunkRequest(dd.getUuidString(), 1L, 1L);
      // send read chunk request and make sure container does not exist
      ContainerCommandResponseProto response =
          hddsDispatcher.dispatch(getReadChunkRequest(writeChunkRequest), null);
      Assert.assertEquals(response.getResult(),
          ContainerProtos.Result.CONTAINER_NOT_FOUND);
      // send write chunk request without sending create container
      response = hddsDispatcher.dispatch(writeChunkRequest, null);
      // container should be created as part of write chunk request
      Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      // send read chunk request to read the chunk written above
      response =
          hddsDispatcher.dispatch(getReadChunkRequest(writeChunkRequest), null);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      Assert.assertEquals(response.getReadChunk().getData(),
          writeChunkRequest.getWriteChunk().getData());
    } finally {
      FileUtils.deleteDirectory(new File(testDir));
    }
  }

  @Test
  public void testWriteChunkWithCreateContainerFailure() throws IOException {
    String testDir = GenericTestUtils.getTempPath(
        TestHddsDispatcher.class.getSimpleName());
    try {
      UUID scmId = UUID.randomUUID();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, testDir);
      DatanodeDetails dd = randomDatanodeDetails();
      HddsDispatcher hddsDispatcher = createDispatcher(dd, scmId, conf);
      ContainerCommandRequestProto writeChunkRequest = getWriteChunkRequest(
          dd.getUuidString(), 1L, 1L);

      HddsDispatcher mockDispatcher = Mockito.spy(hddsDispatcher);
      ContainerCommandResponseProto.Builder builder = ContainerUtils
          .getContainerCommandResponse(writeChunkRequest,
              ContainerProtos.Result.DISK_OUT_OF_SPACE, "");
      // Return DISK_OUT_OF_SPACE response when writing chunk
      // with container creation.
      Mockito.doReturn(builder.build()).when(mockDispatcher)
          .createContainer(writeChunkRequest);

      GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
          .captureLogs(HddsDispatcher.LOG);
      // send write chunk request without sending create container
      mockDispatcher.dispatch(writeChunkRequest, null);
      // verify the error log
      assertTrue(logCapturer.getOutput()
          .contains("ContainerID " + writeChunkRequest.getContainerID()
              + " creation failed : Result: DISK_OUT_OF_SPACE"));
    } finally {
      FileUtils.deleteDirectory(new File(testDir));
    }
  }

  /**
   * Creates HddsDispatcher instance with given infos.
   * @param dd datanode detail info.
   * @param scmId UUID of scm id.
   * @param conf configuration be used.
   * @return HddsDispatcher HddsDispatcher instance.
   * @throws IOException
   */
  private HddsDispatcher createDispatcher(DatanodeDetails dd, UUID scmId,
      OzoneConfiguration conf) throws IOException {
    ContainerSet containerSet = new ContainerSet();
    VolumeSet volumeSet = new VolumeSet(dd.getUuidString(), conf);
    DatanodeStateMachine stateMachine = Mockito.mock(
        DatanodeStateMachine.class);
    StateContext context = Mockito.mock(StateContext.class);
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(dd);
    Mockito.when(context.getParent()).thenReturn(stateMachine);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    Map<ContainerType, Handler> handlers = Maps.newHashMap();
    for (ContainerType containerType : ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(containerType, conf, context,
              containerSet, volumeSet, metrics));
    }

    HddsDispatcher hddsDispatcher = new HddsDispatcher(
        conf, containerSet, volumeSet, handlers, context, metrics);
    hddsDispatcher.setScmId(scmId.toString());
    return hddsDispatcher;
  }

  // This method has to be removed once we move scm/TestUtils.java
  // from server-scm project to container-service or to common project.
  private static DatanodeDetails randomDatanodeDetails() {
    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID().toString())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  private ContainerCommandRequestProto getWriteChunkRequest(
      String datanodeId, Long containerId, Long localId) {

    ByteString data = ByteString.copyFrom(
        UUID.randomUUID().toString().getBytes(UTF_8));
    ContainerProtos.ChunkInfo chunk = ContainerProtos.ChunkInfo
        .newBuilder()
        .setChunkName(
            DigestUtils.md5Hex("dummy-key") + "_stream_"
                + containerId + "_chunk_" + localId)
        .setOffset(0)
        .setLen(data.size())
        .setChecksumData(Checksum.getNoChecksumDataProto())
        .build();

    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setBlockID(new BlockID(containerId, localId)
            .getDatanodeBlockIDProtobuf())
        .setChunkData(chunk)
        .setData(data);

    return ContainerCommandRequestProto
        .newBuilder()
        .setContainerID(containerId)
        .setCmdType(ContainerProtos.Type.WriteChunk)
        .setDatanodeUuid(datanodeId)
        .setWriteChunk(writeChunkRequest)
        .build();
  }

  /**
   * Creates container read chunk request using input container write chunk
   * request.
   *
   * @param writeChunkRequest - Input container write chunk request
   * @return container read chunk request
   */
  private ContainerCommandRequestProto getReadChunkRequest(
      ContainerCommandRequestProto writeChunkRequest) {
    WriteChunkRequestProto writeChunk = writeChunkRequest.getWriteChunk();
    ContainerProtos.ReadChunkRequestProto.Builder readChunkRequest =
        ContainerProtos.ReadChunkRequestProto.newBuilder()
            .setBlockID(writeChunk.getBlockID())
            .setChunkData(writeChunk.getChunkData());
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.ReadChunk)
        .setContainerID(writeChunk.getBlockID().getContainerID())
        .setTraceID(writeChunkRequest.getTraceID())
        .setDatanodeUuid(writeChunkRequest.getDatanodeUuid())
        .setReadChunk(readChunkRequest)
        .build();
  }

}