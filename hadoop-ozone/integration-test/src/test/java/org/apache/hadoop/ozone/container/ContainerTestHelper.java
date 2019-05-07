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

package org.apache.hadoop.ozone.container;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto.Builder;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.security.token.Token;

import com.google.common.base.Preconditions;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for container tests.
 */
public final class ContainerTestHelper {
  public static final Logger LOG = LoggerFactory.getLogger(
      ContainerTestHelper.class);
  private static Random r = new Random();

  public static final long CONTAINER_MAX_SIZE =
      (long) StorageUnit.GB.toBytes(1);

  /**
   * Never constructed.
   */
  private ContainerTestHelper() {
  }

  // TODO: mock multi-node pipeline
  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   * @throws IOException
   */
  public static Pipeline createSingleNodePipeline() throws
      IOException {
    return createPipeline(1);
  }

  public static String createLocalAddress() throws IOException {
    try(ServerSocket s = new ServerSocket(0)) {
      return "127.0.0.1:" + s.getLocalPort();
    }
  }
  public static DatanodeDetails createDatanodeDetails() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, port);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, port);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, port);
    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID().toString())
        .setIpAddress(socket.getInetAddress().getHostAddress())
        .setHostName(socket.getInetAddress().getHostName())
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort)
        .build();

    socket.close();
    return datanodeDetails;
  }

  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   * @throws IOException
   */
  public static Pipeline createPipeline(int numNodes)
      throws IOException {
    Preconditions.checkArgument(numNodes >= 1);
    final List<DatanodeDetails> ids = new ArrayList<>(numNodes);
    for(int i = 0; i < numNodes; i++) {
      ids.add(createDatanodeDetails());
    }
    return createPipeline(ids);
  }

  public static Pipeline createPipeline(
      Iterable<DatanodeDetails> ids) throws IOException {
    Objects.requireNonNull(ids, "ids == null");
    Preconditions.checkArgument(ids.iterator().hasNext());
    List<DatanodeDetails> dns = new ArrayList<>();
    ids.forEach(dns::add);
    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .setFactor(ReplicationFactor.ONE)
        .setNodes(dns)
        .build();
    return pipeline;
  }

  /**
   * Creates a ChunkInfo for testing.
   *
   * @param keyID - ID of the key
   * @param seqNo - Chunk number.
   * @return ChunkInfo
   * @throws IOException
   */
  public static ChunkInfo getChunk(long keyID, int seqNo, long offset,
      long len) throws IOException {

    ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", keyID,
        seqNo), offset, len);
    return info;
  }

  /**
   * Generates some data of the requested len.
   *
   * @param len - Number of bytes.
   * @return byte array with valid data.
   */
  public static byte[] getData(int len) {
    byte[] data = new byte[len];
    r.nextBytes(data);
    return data;
  }

  /**
   * Computes the hash and sets the value correctly.
   *
   * @param info - chunk info.
   * @param data - data array
   * @throws NoSuchAlgorithmException
   */
  public static void setDataChecksum(ChunkInfo info, byte[] data)
      throws OzoneChecksumException {
    Checksum checksum = new Checksum();
    info.setChecksumData(checksum.computeChecksum(data));
  }

  /**
   * Returns a writeChunk Request.
   *
   * @param pipeline - A set of machines where this container lives.
   * @param blockID - Block ID of the chunk.
   * @param datalen - Length of data.
   * @return ContainerCommandRequestProto
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  public static ContainerCommandRequestProto getWriteChunkRequest(
      Pipeline pipeline, BlockID blockID, int datalen) throws IOException {
    LOG.trace("writeChunk {} (blockID={}) to pipeline=",
        datalen, blockID, pipeline);
    ContainerProtos.WriteChunkRequestProto.Builder writeRequest =
        ContainerProtos.WriteChunkRequestProto
            .newBuilder();

    writeRequest.setBlockID(blockID.getDatanodeBlockIDProtobuf());

    byte[] data = getData(datalen);
    ChunkInfo info = getChunk(blockID.getLocalID(), 0, 0, datalen);
    setDataChecksum(info, data);

    writeRequest.setChunkData(info.getProtoBufMessage());
    writeRequest.setData(ByteString.copyFrom(data));

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.WriteChunk);
    request.setContainerID(blockID.getContainerID());
    request.setWriteChunk(writeRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());

    return request.build();
  }

  /**
   * Returns PutSmallFile Request that we can send to the container.
   *
   * @param pipeline - Pipeline
   * @param blockID - Block ID of the small file.
   * @param dataLen - Number of bytes in the data
   * @return ContainerCommandRequestProto
   */
  public static ContainerCommandRequestProto getWriteSmallFileRequest(
      Pipeline pipeline, BlockID blockID, int dataLen)
      throws Exception {
    ContainerProtos.PutSmallFileRequestProto.Builder smallFileRequest =
        ContainerProtos.PutSmallFileRequestProto.newBuilder();
    byte[] data = getData(dataLen);
    ChunkInfo info = getChunk(blockID.getLocalID(), 0, 0, dataLen);
    setDataChecksum(info, data);


    ContainerProtos.PutBlockRequestProto.Builder putRequest =
        ContainerProtos.PutBlockRequestProto.newBuilder();

    BlockData blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> newList = new LinkedList<>();
    newList.add(info.getProtoBufMessage());
    blockData.setChunks(newList);
    putRequest.setBlockData(blockData.getProtoBufMessage());

    smallFileRequest.setChunkInfo(info.getProtoBufMessage());
    smallFileRequest.setData(ByteString.copyFrom(data));
    smallFileRequest.setBlock(putRequest);

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.PutSmallFile);
    request.setContainerID(blockID.getContainerID());
    request.setPutSmallFile(smallFileRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request.build();
  }


  public static ContainerCommandRequestProto getReadSmallFileRequest(
      Pipeline pipeline, ContainerProtos.PutBlockRequestProto putKey)
      throws Exception {
    ContainerProtos.GetSmallFileRequestProto.Builder smallFileRequest =
        ContainerProtos.GetSmallFileRequestProto.newBuilder();
    ContainerCommandRequestProto getKey = getBlockRequest(pipeline, putKey);
    smallFileRequest.setBlock(getKey.getGetBlock());

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.GetSmallFile);
    request.setContainerID(getKey.getGetBlock().getBlockID().getContainerID());
    request.setGetSmallFile(smallFileRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request.build();
  }

  /**
   * Returns a read Request.
   *
   * @param pipeline pipeline.
   * @param request writeChunkRequest.
   * @return Request.
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  public static ContainerCommandRequestProto getReadChunkRequest(
      Pipeline pipeline, ContainerProtos.WriteChunkRequestProto request)
      throws IOException, NoSuchAlgorithmException {
    LOG.trace("readChunk blockID={} from pipeline={}",
        request.getBlockID(), pipeline);

    ContainerProtos.ReadChunkRequestProto.Builder readRequest =
        ContainerProtos.ReadChunkRequestProto.newBuilder();
    readRequest.setBlockID(request.getBlockID());
    readRequest.setChunkData(request.getChunkData());

    Builder newRequest =
        ContainerCommandRequestProto.newBuilder();
    newRequest.setCmdType(ContainerProtos.Type.ReadChunk);
    newRequest.setContainerID(readRequest.getBlockID().getContainerID());
    newRequest.setReadChunk(readRequest);
    newRequest.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return newRequest.build();
  }

  /**
   * Returns a delete Request.
   *
   * @param pipeline pipeline.
   * @param writeRequest - write request
   * @return request
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  public static ContainerCommandRequestProto getDeleteChunkRequest(
      Pipeline pipeline, ContainerProtos.WriteChunkRequestProto writeRequest)
      throws
      IOException, NoSuchAlgorithmException {
    LOG.trace("deleteChunk blockID={} from pipeline={}",
        writeRequest.getBlockID(), pipeline);

    ContainerProtos.DeleteChunkRequestProto.Builder deleteRequest =
        ContainerProtos.DeleteChunkRequestProto
            .newBuilder();

    deleteRequest.setChunkData(writeRequest.getChunkData());
    deleteRequest.setBlockID(writeRequest.getBlockID());

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteChunk);
    request.setContainerID(writeRequest.getBlockID().getContainerID());
    request.setDeleteChunk(deleteRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request.build();
  }

  /**
   * Returns a create container command for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCreateContainerRequest(
      long containerID, Pipeline pipeline) throws IOException {
    LOG.trace("addContainer: {}", containerID);
    return getContainerCommandRequestBuilder(containerID, pipeline).build();
  }

  /**
   * Returns a create container command with token. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCreateContainerRequest(
      long containerID, Pipeline pipeline, Token token) throws IOException {
    LOG.trace("addContainer: {}", containerID);
    return getContainerCommandRequestBuilder(containerID, pipeline)
        .setEncodedToken(token.encodeToUrlString())
        .build();
  }

  private static Builder getContainerCommandRequestBuilder(long containerID,
      Pipeline pipeline) throws IOException {
    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setContainerID(containerID);
    request.setCreateContainer(
        ContainerProtos.CreateContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());

    return request;
  }

  /**
   * Returns a create container command for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCreateContainerSecureRequest(
      long containerID, Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token) throws IOException {
    LOG.trace("addContainer: {}", containerID);

    Builder request = getContainerCommandRequestBuilder(containerID, pipeline);
    if(token != null){
      request.setEncodedToken(token.encodeToUrlString());
    }
    return request.build();
  }

  /**
   * Return an update container command for test purposes.
   * Creates a container data based on the given meta data,
   * and request to update an existing container with it.
   *
   * @param containerID
   * @param metaData
   * @return
   * @throws IOException
   */
  public static ContainerCommandRequestProto getUpdateContainerRequest(
      long containerID, Map<String, String> metaData) throws IOException {
    ContainerProtos.UpdateContainerRequestProto.Builder updateRequestBuilder =
        ContainerProtos.UpdateContainerRequestProto.newBuilder();
    String[] keys = metaData.keySet().toArray(new String[]{});
    for(int i=0; i<keys.length; i++) {
      KeyValue.Builder kvBuilder = KeyValue.newBuilder();
      kvBuilder.setKey(keys[i]);
      kvBuilder.setValue(metaData.get(keys[i]));
      updateRequestBuilder.addMetadata(kvBuilder.build());
    }
    Pipeline pipeline =
        ContainerTestHelper.createSingleNodePipeline();

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.UpdateContainer);
    request.setContainerID(containerID);
    request.setUpdateContainer(updateRequestBuilder.build());
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request.build();
  }
  /**
   * Returns a create container response for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandResponseProto
      getCreateContainerResponse(ContainerCommandRequestProto request) {

    ContainerCommandResponseProto.Builder response =
        ContainerCommandResponseProto.newBuilder();
    response.setCmdType(ContainerProtos.Type.CreateContainer);
    response.setTraceID(request.getTraceID());
    response.setCreateContainer(
        ContainerProtos.CreateContainerResponseProto.getDefaultInstance());
    response.setResult(ContainerProtos.Result.SUCCESS);
    return response.build();
  }

  /**
   * Returns the PutBlockRequest for test purpose.
   * @param pipeline - pipeline.
   * @param writeRequest - Write Chunk Request.
   * @return - Request
   */
  public static ContainerCommandRequestProto getPutBlockRequest(
      Pipeline pipeline, ContainerProtos.WriteChunkRequestProto writeRequest)
      throws IOException {
    LOG.trace("putBlock: {} to pipeline={}",
        writeRequest.getBlockID());

    ContainerProtos.PutBlockRequestProto.Builder putRequest =
        ContainerProtos.PutBlockRequestProto.newBuilder();

    BlockData blockData = new BlockData(
        BlockID.getFromProtobuf(writeRequest.getBlockID()));
    List<ContainerProtos.ChunkInfo> newList = new LinkedList<>();
    newList.add(writeRequest.getChunkData());
    blockData.setChunks(newList);
    blockData.setBlockCommitSequenceId(0);
    putRequest.setBlockData(blockData.getProtoBufMessage());

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.PutBlock);
    request.setContainerID(blockData.getContainerID());
    request.setPutBlock(putRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request.build();
  }

  /**
   * Gets a GetBlockRequest for test purpose.
   * @param  pipeline - pipeline
   * @param putBlockRequest - putBlockRequest.
   * @return - Request
   * immediately.
   */
  public static ContainerCommandRequestProto getBlockRequest(
      Pipeline pipeline, ContainerProtos.PutBlockRequestProto putBlockRequest)
      throws IOException {
    ContainerProtos.DatanodeBlockID blockID =
        putBlockRequest.getBlockData().getBlockID();
    LOG.trace("getKey: blockID={}", blockID);

    ContainerProtos.GetBlockRequestProto.Builder getRequest =
        ContainerProtos.GetBlockRequestProto.newBuilder();
    getRequest.setBlockID(blockID);

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.GetBlock);
    request.setContainerID(blockID.getContainerID());
    request.setGetBlock(getRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request.build();
  }

  /**
   * Verify the response against the request.
   *
   * @param request - Request
   * @param response - Response
   */
  public static void verifyGetBlock(ContainerCommandRequestProto request,
      ContainerCommandResponseProto response, int expectedChunksCount) {
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    Assert.assertEquals(expectedChunksCount,
        response.getGetBlock().getBlockData().getChunksCount());
  }

  /**
   * @param pipeline - pipeline.
   * @param putBlockRequest - putBlockRequest.
   * @return - Request
   */
  public static ContainerCommandRequestProto getDeleteBlockRequest(
      Pipeline pipeline, ContainerProtos.PutBlockRequestProto putBlockRequest)
      throws IOException {
    ContainerProtos.DatanodeBlockID blockID = putBlockRequest.getBlockData()
        .getBlockID();
    LOG.trace("deleteBlock: name={}", blockID);
    ContainerProtos.DeleteBlockRequestProto.Builder delRequest =
        ContainerProtos.DeleteBlockRequestProto.newBuilder();
    delRequest.setBlockID(blockID);
    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteBlock);
    request.setContainerID(blockID.getContainerID());
    request.setDeleteBlock(delRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request.build();
  }

  /**
   * Returns a close container request.
   * @param pipeline - pipeline
   * @param containerID - ID of the container.
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCloseContainer(
      Pipeline pipeline, long containerID) throws IOException {
    ContainerProtos.ContainerCommandRequestProto cmd =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CloseContainer)
            .setContainerID(containerID)
            .setCloseContainer(
                ContainerProtos.CloseContainerRequestProto.getDefaultInstance())
            .setDatanodeUuid(pipeline.getFirstNode().getUuidString())
            .build();

    return cmd;
  }

  /**
   * Returns a simple request without traceId.
   * @param pipeline - pipeline
   * @param containerID - ID of the container.
   * @return ContainerCommandRequestProto without traceId.
   */
  public static ContainerCommandRequestProto getRequestWithoutTraceId(
      Pipeline pipeline, long containerID) throws IOException {
    Preconditions.checkNotNull(pipeline);
    ContainerProtos.ContainerCommandRequestProto cmd =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CloseContainer)
            .setContainerID(containerID)
            .setCloseContainer(
                ContainerProtos.CloseContainerRequestProto.getDefaultInstance())
            .setDatanodeUuid(pipeline.getFirstNode().getUuidString())
            .build();
    return cmd;
  }

  /**
   * Returns a delete container request.
   * @param pipeline - pipeline
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getDeleteContainer(
      Pipeline pipeline, long containerID, boolean forceDelete)
      throws IOException {
    Preconditions.checkNotNull(pipeline);
    ContainerProtos.DeleteContainerRequestProto deleteRequest =
        ContainerProtos.DeleteContainerRequestProto.newBuilder().
            setForceDelete(forceDelete).build();
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.DeleteContainer)
        .setContainerID(containerID)
        .setDeleteContainer(
            ContainerProtos.DeleteContainerRequestProto.getDefaultInstance())
        .setDeleteContainer(deleteRequest)
        .setDatanodeUuid(pipeline.getFirstNode().getUuidString())
        .build();
  }

  private static void sleep(long milliseconds) {
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public static BlockID getTestBlockID(long containerID) {
    // Add 2ms delay so that localID based on UtcTime
    // won't collide.
    sleep(2);
    return new BlockID(containerID, HddsUtils.getUtcTime());
  }

  public static long getTestContainerID() {
    return HddsUtils.getUtcTime();
  }

  public static boolean isContainerClosed(MiniOzoneCluster cluster,
      long containerID, DatanodeDetails datanode) {
    ContainerData containerData;
    for (HddsDatanodeService datanodeService : cluster.getHddsDatanodes()) {
      if (datanode.equals(datanodeService.getDatanodeDetails())) {
        Container container =
            datanodeService.getDatanodeStateMachine().getContainer()
                .getContainerSet().getContainer(containerID);
        if (container != null) {
          containerData = container.getContainerData();
          return containerData.isClosed();
        }
      }
    }
    return false;
  }

  public static boolean isContainerPresent(MiniOzoneCluster cluster,
      long containerID, DatanodeDetails datanode) {
    for (HddsDatanodeService datanodeService : cluster.getHddsDatanodes()) {
      if (datanode.equals(datanodeService.getDatanodeDetails())) {
        Container container =
            datanodeService.getDatanodeStateMachine().getContainer()
                .getContainerSet().getContainer(containerID);
        if (container != null) {
          return true;
        }
      }
    }
    return false;
  }

  public static OzoneOutputStream createKey(String keyName,
      ReplicationType type, long size, ObjectStore objectStore,
      String volumeName, String bucketName) throws Exception {
    org.apache.hadoop.hdds.client.ReplicationFactor factor =
        type == ReplicationType.STAND_ALONE ?
            org.apache.hadoop.hdds.client.ReplicationFactor.ONE :
            org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
    return objectStore.getVolume(volumeName).getBucket(bucketName)
        .createKey(keyName, size, type, factor, new HashMap<>());
  }

  public static OzoneOutputStream createKey(String keyName,
      ReplicationType type,
      org.apache.hadoop.hdds.client.ReplicationFactor factor, long size,
      ObjectStore objectStore, String volumeName, String bucketName)
      throws Exception {
    return objectStore.getVolume(volumeName).getBucket(bucketName)
        .createKey(keyName, size, type, factor, new HashMap<>());
  }

  public static void validateData(String keyName, byte[] data,
      ObjectStore objectStore, String volumeName, String bucketName)
      throws Exception {
    byte[] readData = new byte[data.length];
    OzoneInputStream is =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .readKey(keyName);
    is.read(readData);
    MessageDigest sha1 = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    sha1.update(data);
    MessageDigest sha2 = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    sha2.update(readData);
    Assert.assertTrue(Arrays.equals(sha1.digest(), sha2.digest()));
    is.close();
  }

  public static String getFixedLengthString(String string, int length) {
    return String.format("%1$" + length + "s", string);
  }

  public static void waitForContainerClose(OzoneOutputStream outputStream,
      MiniOzoneCluster cluster) throws Exception {
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) outputStream.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        keyOutputStream.getLocationInfoList();
    List<Long> containerIdList = new ArrayList<>();
    for (OmKeyLocationInfo info : locationInfoList) {
      long id = info.getContainerID();
      if (!containerIdList.contains(id)) {
        containerIdList.add(id);
      }
    }
    Assert.assertTrue(!containerIdList.isEmpty());
    waitForContainerClose(cluster, containerIdList.toArray(new Long[0]));
  }

  public static void waitForPipelineClose(OzoneOutputStream outputStream,
      MiniOzoneCluster cluster, boolean waitForContainerCreation)
      throws Exception {
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) outputStream.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        keyOutputStream.getLocationInfoList();
    List<Long> containerIdList = new ArrayList<>();
    for (OmKeyLocationInfo info : locationInfoList) {
      containerIdList.add(info.getContainerID());
    }
    Assert.assertTrue(!containerIdList.isEmpty());
    waitForPipelineClose(cluster, waitForContainerCreation,
        containerIdList.toArray(new Long[0]));
  }

  public static void waitForPipelineClose(MiniOzoneCluster cluster,
      boolean waitForContainerCreation, Long... containerIdList)
      throws TimeoutException, InterruptedException, IOException {
    List<Pipeline> pipelineList = new ArrayList<>();
    for (long containerID : containerIdList) {
      ContainerInfo container =
          cluster.getStorageContainerManager().getContainerManager()
              .getContainer(ContainerID.valueof(containerID));
      Pipeline pipeline =
          cluster.getStorageContainerManager().getPipelineManager()
              .getPipeline(container.getPipelineID());
      if (!pipelineList.contains(pipeline)) {
        pipelineList.add(pipeline);
      }
      List<DatanodeDetails> datanodes = pipeline.getNodes();

      if (waitForContainerCreation) {
        for (DatanodeDetails details : datanodes) {
          // Client will issue write chunk and it will create the container on
          // datanodes.
          // wait for the container to be created
          GenericTestUtils
              .waitFor(() -> isContainerPresent(cluster, containerID, details),
                  500, 100 * 1000);
          Assert.assertTrue(isContainerPresent(cluster, containerID, details));

          // make sure the container gets created first
          Assert.assertFalse(ContainerTestHelper
              .isContainerClosed(cluster, containerID, details));
        }
      }
    }
    for (Pipeline pipeline1 : pipelineList) {
      // issue pipeline destroy command
      cluster.getStorageContainerManager().getPipelineManager()
          .finalizeAndDestroyPipeline(pipeline1, false);
    }

    // wait for the pipeline to get destroyed in the datanodes
    for (Pipeline pipeline : pipelineList) {
      for (DatanodeDetails dn : pipeline.getNodes()) {
        XceiverServerSpi server =
            cluster.getHddsDatanodes().get(cluster.getHddsDatanodeIndex(dn))
                .getDatanodeStateMachine().getContainer().getWriteChannel();
        Assert.assertTrue(server instanceof XceiverServerRatis);
        XceiverServerRatis raftServer = (XceiverServerRatis) server;
        GenericTestUtils.waitFor(
            () -> (!raftServer.getPipelineIds().contains(pipeline.getId())),
            500, 100 * 1000);
      }
    }
  }

  public static void waitForContainerClose(MiniOzoneCluster cluster,
      Long... containerIdList)
      throws ContainerNotFoundException, PipelineNotFoundException,
      TimeoutException, InterruptedException {
    List<Pipeline> pipelineList = new ArrayList<>();
    for (long containerID : containerIdList) {
      ContainerInfo container =
          cluster.getStorageContainerManager().getContainerManager()
              .getContainer(ContainerID.valueof(containerID));
      Pipeline pipeline =
          cluster.getStorageContainerManager().getPipelineManager()
              .getPipeline(container.getPipelineID());
      pipelineList.add(pipeline);
      List<DatanodeDetails> datanodes = pipeline.getNodes();

      for (DatanodeDetails details : datanodes) {
        // Client will issue write chunk and it will create the container on
        // datanodes.
        // wait for the container to be created
        GenericTestUtils
            .waitFor(() -> isContainerPresent(cluster, containerID, details),
                500, 100 * 1000);
        Assert.assertTrue(isContainerPresent(cluster, containerID, details));

        // make sure the container gets created first
        Assert.assertFalse(ContainerTestHelper
            .isContainerClosed(cluster, containerID, details));
        // send the order to close the container
        cluster.getStorageContainerManager().getEventQueue()
            .fireEvent(SCMEvents.CLOSE_CONTAINER,
                ContainerID.valueof(containerID));
      }
    }
    int index = 0;
    for (long containerID : containerIdList) {
      Pipeline pipeline = pipelineList.get(index);
      List<DatanodeDetails> datanodes = pipeline.getNodes();
      // Below condition avoids the case where container has been allocated
      // but not yet been used by the client. In such a case container is never
      // created.
      for (DatanodeDetails datanodeDetails : datanodes) {
        GenericTestUtils.waitFor(
            () -> isContainerClosed(cluster, containerID, datanodeDetails), 500,
            15 * 1000);
        //double check if it's really closed
        // (waitFor also throws an exception)
        Assert.assertTrue(
            isContainerClosed(cluster, containerID, datanodeDetails));
      }
      index++;
    }
  }
}
