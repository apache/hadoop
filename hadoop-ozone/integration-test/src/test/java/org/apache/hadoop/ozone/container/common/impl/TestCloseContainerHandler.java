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

package org.apache.hadoop.ozone.container.common.impl;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.LinkedList;
import static org.apache.hadoop.ozone.container.ContainerTestHelper
    .createSingleNodePipeline;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getChunk;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getData;
import static org.apache.hadoop.ozone.container.ContainerTestHelper
    .setDataChecksum;

/**
 * Simple tests to verify that closeContainer handler on Datanode.
 */
public class TestCloseContainerHandler {

  @Rule
  public TestRule timeout = new Timeout(300000);

  private static Configuration conf;
  private static HddsDispatcher dispatcher;
  private static ContainerSet containerSet;
  private static VolumeSet volumeSet;
  private static KeyValueHandler handler;
  private static OpenContainerBlockMap openContainerBlockMap;

  private final static String DATANODE_UUID = UUID.randomUUID().toString();

  private static final String BASE_DIR = MiniDFSCluster.getBaseDirectory();
  private static final String VOLUME_1 = BASE_DIR + "disk1";
  private static final String VOLUME_2 = BASE_DIR + "disk2";

  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration();
    String dataDirKey = VOLUME_1 + "," + VOLUME_2;
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDirKey);
    containerSet = new ContainerSet();
    DatanodeDetails datanodeDetails =
        DatanodeDetails.newBuilder().setUuid(DATANODE_UUID)
            .setHostName("localhost").setIpAddress("127.0.0.1").build();
    volumeSet = new VolumeSet(datanodeDetails.getUuidString(), conf);

    dispatcher = new HddsDispatcher(conf, containerSet, volumeSet, null);
    handler = (KeyValueHandler) dispatcher
        .getHandler(ContainerProtos.ContainerType.KeyValueContainer);
    openContainerBlockMap = handler.getOpenContainerBlockMap();
    dispatcher.setScmId(UUID.randomUUID().toString());
  }

  @AfterClass
  public static void shutdown() throws IOException {
    // Delete the hdds volume root dir
    List<HddsVolume> volumes = new ArrayList<>();
    volumes.addAll(volumeSet.getVolumesList());
    volumes.addAll(volumeSet.getFailedVolumesList());

    for (HddsVolume volume : volumes) {
      FileUtils.deleteDirectory(volume.getHddsRootDir());
    }
    volumeSet.shutdown();
  }

  private long createContainer() {
    long testContainerId = ContainerTestHelper.getTestContainerID();

    ContainerProtos.ContainerCommandRequestProto request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CreateContainer)
            .setContainerID(testContainerId)
            .setDatanodeUuid(DATANODE_UUID)
            .setCreateContainer(ContainerProtos.CreateContainerRequestProto
                .getDefaultInstance())
            .build();

    dispatcher.dispatch(request);
    return testContainerId;
  }

  private List<ChunkInfo> writeChunkBuilder(BlockID blockID, Pipeline pipeline,
      int chunkCount)
      throws IOException, NoSuchAlgorithmException {
    final int datalen = 1024;
    long testContainerID = blockID.getContainerID();
    List<ChunkInfo> chunkList = new LinkedList<>();
    for (int x = 0; x < chunkCount; x++) {
      ChunkInfo info = getChunk(blockID.getLocalID(), x, datalen * x, datalen);
      byte[] data = getData(datalen);
      setDataChecksum(info, data);
      ContainerProtos.WriteChunkRequestProto.Builder writeRequest =
          ContainerProtos.WriteChunkRequestProto.newBuilder();
      writeRequest.setBlockID(blockID.getDatanodeBlockIDProtobuf());
      writeRequest.setChunkData(info.getProtoBufMessage());
      writeRequest.setData(ByteString.copyFrom(data));
      writeRequest.setStage(ContainerProtos.Stage.COMBINED);
      ContainerProtos.ContainerCommandRequestProto.Builder request =
          ContainerProtos.ContainerCommandRequestProto.newBuilder();
      request.setCmdType(ContainerProtos.Type.WriteChunk);
      request.setContainerID(blockID.getContainerID());
      request.setWriteChunk(writeRequest);
      request.setTraceID(UUID.randomUUID().toString());
      request.setDatanodeUuid(pipeline.getLeader().getUuidString());
      dispatcher.dispatch(request.build());
      chunkList.add(info);
    }
    return chunkList;
  }

  @Test
  public void testPutKeyWithMultipleChunks()
      throws IOException, NoSuchAlgorithmException {
    long testContainerID = createContainer();
    Assert.assertNotNull(containerSet.getContainer(testContainerID));
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(testContainerID);
    Pipeline pipeline = createSingleNodePipeline();
    List<ChunkInfo> chunkList = writeChunkBuilder(blockID, pipeline, 3);
    // the block should exist in the map
    Assert.assertNotNull(
        openContainerBlockMap.getBlockDataMap(testContainerID)
            .get(blockID.getLocalID()));
    BlockData blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> chunkProtoList = new LinkedList<>();
    for (ChunkInfo i : chunkList) {
      chunkProtoList.add(i.getProtoBufMessage());
    }
    blockData.setChunks(chunkProtoList);
    ContainerProtos.PutBlockRequestProto.Builder putBlockRequestProto =
        ContainerProtos.PutBlockRequestProto.newBuilder();
    putBlockRequestProto.setBlockData(blockData.getProtoBufMessage());
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.PutBlock);
    request.setContainerID(blockID.getContainerID());
    request.setPutBlock(putBlockRequestProto);
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(pipeline.getLeader().getUuidString());
    dispatcher.dispatch(request.build());

    //the open block should be removed from Map
    Assert.assertNull(
        openContainerBlockMap.getBlockDataMap(testContainerID));
  }

  @Test
  public void testDeleteChunk() throws Exception {
    long testContainerID = createContainer();
    Assert.assertNotNull(containerSet.getContainer(testContainerID));
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(testContainerID);
    Pipeline pipeline = createSingleNodePipeline();
    List<ChunkInfo> chunkList = writeChunkBuilder(blockID, pipeline, 3);
    // the key should exist in the map
    Assert.assertNotNull(
        openContainerBlockMap.getBlockDataMap(testContainerID)
            .get(blockID.getLocalID()));
    Assert.assertTrue(
        openContainerBlockMap.getBlockDataMap(testContainerID)
            .get(blockID.getLocalID()).getChunks().size() == 3);
    ContainerProtos.DeleteChunkRequestProto.Builder deleteChunkProto =
        ContainerProtos.DeleteChunkRequestProto.newBuilder();
    deleteChunkProto.setBlockID(blockID.getDatanodeBlockIDProtobuf());
    deleteChunkProto.setChunkData(chunkList.get(0).getProtoBufMessage());
    ContainerProtos.WriteChunkRequestProto.Builder writeRequest =
        ContainerProtos.WriteChunkRequestProto.newBuilder();
    writeRequest.setBlockID(blockID.getDatanodeBlockIDProtobuf());
    writeRequest.setChunkData(chunkList.get(0).getProtoBufMessage());
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteChunk);
    request.setContainerID(blockID.getContainerID());
    request.setDeleteChunk(deleteChunkProto);
    request.setWriteChunk(writeRequest);
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(pipeline.getLeader().getUuidString());
    dispatcher.dispatch(request.build());
    Assert.assertTrue(
        openContainerBlockMap.getBlockDataMap(testContainerID)
            .get(blockID.getLocalID()).getChunks().size() == 2);

  }

  @Test
  public void testCloseContainer() throws Exception {
    long testContainerID = createContainer();
    Assert.assertNotNull(containerSet.getContainer(testContainerID));
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(testContainerID);
    Pipeline pipeline = createSingleNodePipeline();
    List<ChunkInfo> chunkList = writeChunkBuilder(blockID, pipeline, 3);

    Container container = containerSet.getContainer(testContainerID);
    BlockData blockData = openContainerBlockMap.
        getBlockDataMap(testContainerID).get(blockID.getLocalID());
    // the key should exist in the map
    Assert.assertNotNull(
        openContainerBlockMap.getBlockDataMap(testContainerID)
            .get(blockID.getLocalID()));
    Assert.assertTrue(
        blockData.getChunks().size() == chunkList.size());
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CloseContainer);
    request.setContainerID(blockID.getContainerID());
    request.setCloseContainer(
        ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(pipeline.getLeader().getUuidString());
    dispatcher.dispatch(request.build());
    Assert.assertNull(
        openContainerBlockMap.getBlockDataMap(testContainerID));
    // Make sure the key got committed
    Assert.assertNotNull(handler.getBlockManager()
        .getBlock(container, blockID));
  }
}