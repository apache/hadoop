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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto
    .ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
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
      StateContext context = Mockito.mock(StateContext.class);
      KeyValueContainerData containerData = new KeyValueContainerData(1L,
          (long) StorageUnit.GB.toBytes(1));
      Container container = new KeyValueContainer(containerData, conf);
      container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(),
          scmId.toString());
      containerSet.addContainer(container);
      HddsDispatcher hddsDispatcher = new HddsDispatcher(
          conf, containerSet, volumeSet, context);
      hddsDispatcher.setScmId(scmId.toString());
      ContainerCommandResponseProto responseOne = hddsDispatcher.dispatch(
          getWriteChunkRequest(dd.getUuidString(), 1L, 1L));
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          responseOne.getResult());
      verify(context, times(0))
          .addContainerActionIfAbsent(Mockito.any(ContainerAction.class));
      containerData.setBytesUsed(Double.valueOf(
          StorageUnit.MB.toBytes(950)).longValue());
      ContainerCommandResponseProto responseTwo = hddsDispatcher.dispatch(
          getWriteChunkRequest(dd.getUuidString(), 1L, 2L));
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          responseTwo.getResult());
      verify(context, times(1))
          .addContainerActionIfAbsent(Mockito.any(ContainerAction.class));

    } finally {
      FileUtils.deleteDirectory(new File(testDir));
    }

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
        UUID.randomUUID().toString().getBytes());
    ContainerProtos.ChunkInfo chunk = ContainerProtos.ChunkInfo
        .newBuilder()
        .setChunkName(
            DigestUtils.md5Hex("dummy-key") + "_stream_"
                + containerId + "_chunk_" + localId)
        .setOffset(0)
        .setLen(data.size())
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
        .setTraceID(UUID.randomUUID().toString())
        .setDatanodeUuid(datanodeId)
        .setWriteChunk(writeChunkRequest)
        .build();
  }

}