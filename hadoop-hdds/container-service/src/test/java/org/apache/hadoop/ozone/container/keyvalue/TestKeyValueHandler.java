/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import org.mockito.Mockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.times;


import java.util.UUID;

/**
 * Unit tests for {@link KeyValueHandler}.
 */
public class TestKeyValueHandler {

  @Rule
  public TestRule timeout = new Timeout(300000);

  private Configuration conf;
  private HddsDispatcher dispatcher;
  private ContainerSet containerSet;
  private VolumeSet volumeSet;
  private KeyValueHandler handler;

  private final static String SCM_ID = UUID.randomUUID().toString();
  private final static String DATANODE_UUID = UUID.randomUUID().toString();
  private int containerID;

  private final String baseDir = MiniDFSCluster.getBaseDirectory();
  private final String volume = baseDir + "disk1";

  private void setup() throws Exception {
    this.conf = new Configuration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volume);

    this.containerSet = new ContainerSet();
    this.volumeSet = new VolumeSet(DATANODE_UUID, conf);

    this.dispatcher = new HddsDispatcher(conf, containerSet, volumeSet);
    this.handler = (KeyValueHandler) dispatcher.getHandler(
        ContainerProtos.ContainerType.KeyValueContainer);
    dispatcher.setScmId(UUID.randomUUID().toString());
  }

  @Test
  /**
   * Test that Handler handles different command types correctly.
   */
  public void testHandlerCommandHandling() throws Exception{
    // Create mock HddsDispatcher and KeyValueHandler.
    this.handler = Mockito.mock(KeyValueHandler.class);
    this.dispatcher = Mockito.mock(HddsDispatcher.class);
    Mockito.when(dispatcher.getHandler(any())).thenReturn(handler);
    Mockito.when(dispatcher.dispatch(any())).thenCallRealMethod();
    Mockito.when(dispatcher.getContainer(anyLong())).thenReturn(
        Mockito.mock(KeyValueContainer.class));
    Mockito.when(handler.handle(any(), any())).thenCallRealMethod();
    doCallRealMethod().when(dispatcher).setMetricsForTesting(any());
    dispatcher.setMetricsForTesting(Mockito.mock(ContainerMetrics.class));

    // Test Create Container Request handling
    ContainerCommandRequestProto createContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.CreateContainer);

    dispatcher.dispatch(createContainerRequest);
    Mockito.verify(handler, times(1)).handleCreateContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Read Container Request handling
    ContainerCommandRequestProto readContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ReadContainer);
    dispatcher.dispatch(readContainerRequest);
    Mockito.verify(handler, times(1)).handleReadContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Update Container Request handling
    ContainerCommandRequestProto updateContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.UpdateContainer);
    dispatcher.dispatch(updateContainerRequest);
    Mockito.verify(handler, times(1)).handleUpdateContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Delete Container Request handling
    ContainerCommandRequestProto deleteContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.DeleteContainer);
    dispatcher.dispatch(deleteContainerRequest);
    Mockito.verify(handler, times(1)).handleDeleteContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test List Container Request handling
    ContainerCommandRequestProto listContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListContainer);
    dispatcher.dispatch(listContainerRequest);
    Mockito.verify(handler, times(1)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Close Container Request handling
    ContainerCommandRequestProto closeContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.CloseContainer);
    dispatcher.dispatch(closeContainerRequest);
    Mockito.verify(handler, times(1)).handleCloseContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Put Key Request handling
    ContainerCommandRequestProto putKeyRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.PutKey);
    dispatcher.dispatch(putKeyRequest);
    Mockito.verify(handler, times(1)).handlePutKey(
        any(ContainerCommandRequestProto.class), any());

    // Test Get Key Request handling
    ContainerCommandRequestProto getKeyRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.GetKey);
    dispatcher.dispatch(getKeyRequest);
    Mockito.verify(handler, times(1)).handleGetKey(
        any(ContainerCommandRequestProto.class), any());

    // Test Delete Key Request handling
    ContainerCommandRequestProto deleteKeyRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.DeleteKey);
    dispatcher.dispatch(deleteKeyRequest);
    Mockito.verify(handler, times(1)).handleDeleteKey(
        any(ContainerCommandRequestProto.class), any());

    // Test List Key Request handling
    ContainerCommandRequestProto listKeyRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListKey);
    dispatcher.dispatch(listKeyRequest);
    Mockito.verify(handler, times(2)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Read Chunk Request handling
    ContainerCommandRequestProto readChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ReadChunk);
    dispatcher.dispatch(readChunkRequest);
    Mockito.verify(handler, times(1)).handleReadChunk(
        any(ContainerCommandRequestProto.class), any());

    // Test Delete Chunk Request handling
    ContainerCommandRequestProto deleteChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.DeleteChunk);
    dispatcher.dispatch(deleteChunkRequest);
    Mockito.verify(handler, times(1)).handleDeleteChunk(
        any(ContainerCommandRequestProto.class), any());

    // Test Write Chunk Request handling
    ContainerCommandRequestProto writeChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.WriteChunk);
    dispatcher.dispatch(writeChunkRequest);
    Mockito.verify(handler, times(1)).handleWriteChunk(
        any(ContainerCommandRequestProto.class), any());

    // Test List Chunk Request handling
    ContainerCommandRequestProto listChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListChunk);
    dispatcher.dispatch(listChunkRequest);
    Mockito.verify(handler, times(3)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Put Small File Request handling
    ContainerCommandRequestProto putSmallFileRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.PutSmallFile);
    dispatcher.dispatch(putSmallFileRequest);
    Mockito.verify(handler, times(1)).handlePutSmallFile(
        any(ContainerCommandRequestProto.class), any());

    // Test Get Small File Request handling
    ContainerCommandRequestProto getSmallFileRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.GetSmallFile);
    dispatcher.dispatch(getSmallFileRequest);
    Mockito.verify(handler, times(1)).handleGetSmallFile(
        any(ContainerCommandRequestProto.class), any());
  }

  private ContainerCommandRequestProto getDummyCommandRequestProto(
      ContainerProtos.Type cmdType) {
    ContainerCommandRequestProto request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(cmdType)
            .setDatanodeUuid(DATANODE_UUID)
            .build();

    return request;
  }

  @Test
  public void testCreateContainer() throws Exception {
    setup();

    long contId = ++containerID;
    ContainerProtos.CreateContainerRequestProto createReq =
        ContainerProtos.CreateContainerRequestProto.newBuilder()
            .setContainerID(contId)
            .build();

    ContainerCommandRequestProto request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CreateContainer)
            .setDatanodeUuid(DATANODE_UUID)
            .setCreateContainer(createReq)
            .build();

    dispatcher.dispatch(request);

    // Verify that new container is added to containerSet.
    Container container = containerSet.getContainer(contId);
    Assert.assertEquals(contId, container.getContainerData().getContainerID());
    Assert.assertEquals(ContainerProtos.ContainerLifeCycleState.OPEN,
        container.getContainerState());
  }
}
