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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Helpers for container tests.
 */
public final class ContainerTestHelper {
  public static final Logger LOG = LoggerFactory.getLogger(
      ContainerTestHelper.class);
  private static Random r = new Random();

  /**
   * Never constructed.
   */
  private ContainerTestHelper() {
  }

  public static void setOzoneLocalStorageRoot(
      Class<?> clazz, OzoneConfiguration conf) {
    String path = GenericTestUtils.getTempPath(clazz.getSimpleName());
    path += conf.getTrimmed(
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);
  }

  // TODO: mock multi-node pipeline
  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   * @throws IOException
   */
  public static Pipeline createSingleNodePipeline(String containerName) throws
      IOException {
    return createPipeline(containerName, 1);
  }

  public static String createLocalAddress() throws IOException {
    try(ServerSocket s = new ServerSocket(0)) {
      return "127.0.0.1:" + s.getLocalPort();
    }
  }
  public static DatanodeID createDatanodeID() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    DatanodeID datanodeID = new DatanodeID(socket.getInetAddress()
        .getHostAddress(), socket.getInetAddress().getHostName(),
        UUID.randomUUID().toString(), port, port, port, port);
    datanodeID.setContainerPort(port);
    socket.close();
    return datanodeID;
  }

  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   * @throws IOException
   */
  public static Pipeline createPipeline(String containerName, int numNodes)
      throws IOException {
    Preconditions.checkArgument(numNodes >= 1);
    final List<DatanodeID> ids = new ArrayList<>(numNodes);
    for(int i = 0; i < numNodes; i++) {
      ids.add(createDatanodeID());
    }
    return createPipeline(containerName, ids);
  }

  public static Pipeline createPipeline(
      String containerName, Iterable<DatanodeID> ids)
      throws IOException {
    Objects.requireNonNull(ids, "ids == null");
    final Iterator<DatanodeID> i = ids.iterator();
    Preconditions.checkArgument(i.hasNext());
    final DatanodeID leader = i.next();
    final Pipeline pipeline = new Pipeline(leader.getDatanodeUuid());
    pipeline.setContainerName(containerName);
    pipeline.addMember(leader);

    for(; i.hasNext();) {
      pipeline.addMember(i.next());
    }
    return pipeline;
  }

  /**
   * Creates a ChunkInfo for testing.
   *
   * @param keyName - Name of the key
   * @param seqNo - Chunk number.
   * @return ChunkInfo
   * @throws IOException
   */
  public static ChunkInfo getChunk(String keyName, int seqNo, long offset,
      long len) throws IOException {

    ChunkInfo info = new ChunkInfo(String.format("%s.data.%d", keyName,
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
      throws NoSuchAlgorithmException {
    MessageDigest sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    sha.update(data);
    info.setChecksum(Hex.encodeHexString(sha.digest()));
  }

  /**
   * Returns a writeChunk Request.
   *
   * @param pipeline - A set of machines where this container lives.
   * @param containerName - Name of the container.
   * @param keyName - Name of the Key this chunk is part of.
   * @param datalen - Length of data.
   * @return ContainerCommandRequestProto
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  public static ContainerCommandRequestProto getWriteChunkRequest(
      Pipeline pipeline, String containerName, String keyName, int datalen)
      throws IOException, NoSuchAlgorithmException {
    LOG.trace("writeChunk {} (key={}) to pipeline=",
        datalen, keyName, pipeline);
    ContainerProtos.WriteChunkRequestProto.Builder writeRequest =
        ContainerProtos.WriteChunkRequestProto
            .newBuilder();

    pipeline.setContainerName(containerName);
    writeRequest.setPipeline(pipeline.getProtobufMessage());
    writeRequest.setKeyName(keyName);

    byte[] data = getData(datalen);
    ChunkInfo info = getChunk(keyName, 0, 0, datalen);
    setDataChecksum(info, data);

    writeRequest.setChunkData(info.getProtoBufMessage());
    writeRequest.setData(ByteString.copyFrom(data));

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.WriteChunk);
    request.setWriteChunk(writeRequest);
    request.setTraceID(UUID.randomUUID().toString());

    return request.build();
  }

  /**
   * Returns PutSmallFile Request that we can send to the container.
   *
   * @param pipeline - Pipeline
   * @param containerName - ContainerName.
   * @param keyName - KeyName
   * @param dataLen - Number of bytes in the data
   * @return ContainerCommandRequestProto
   */
  public static ContainerCommandRequestProto getWriteSmallFileRequest(
      Pipeline pipeline, String containerName, String keyName, int dataLen)
      throws Exception {
    ContainerProtos.PutSmallFileRequestProto.Builder smallFileRequest =
        ContainerProtos.PutSmallFileRequestProto.newBuilder();
    pipeline.setContainerName(containerName);
    byte[] data = getData(dataLen);
    ChunkInfo info = getChunk(keyName, 0, 0, dataLen);
    setDataChecksum(info, data);


    ContainerProtos.PutKeyRequestProto.Builder putRequest =
        ContainerProtos.PutKeyRequestProto.newBuilder();

    putRequest.setPipeline(pipeline.getProtobufMessage());
    KeyData keyData = new KeyData(containerName, keyName);

    List<ContainerProtos.ChunkInfo> newList = new LinkedList<>();
    newList.add(info.getProtoBufMessage());
    keyData.setChunks(newList);
    putRequest.setKeyData(keyData.getProtoBufMessage());

    smallFileRequest.setChunkInfo(info.getProtoBufMessage());
    smallFileRequest.setData(ByteString.copyFrom(data));
    smallFileRequest.setKey(putRequest);

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.PutSmallFile);
    request.setPutSmallFile(smallFileRequest);
    request.setTraceID(UUID.randomUUID().toString());
    return request.build();
  }


  public static ContainerCommandRequestProto getReadSmallFileRequest(
      ContainerProtos.PutKeyRequestProto putKey)
      throws Exception {
    ContainerProtos.GetSmallFileRequestProto.Builder smallFileRequest =
        ContainerProtos.GetSmallFileRequestProto.newBuilder();

    ContainerCommandRequestProto getKey = getKeyRequest(putKey);
    smallFileRequest.setKey(getKey.getGetKey());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.GetSmallFile);
    request.setGetSmallFile(smallFileRequest);
    request.setTraceID(UUID.randomUUID().toString());
    return request.build();
  }

  /**
   * Returns a read Request.
   *
   * @param request writeChunkRequest.
   * @return Request.
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  public static ContainerCommandRequestProto getReadChunkRequest(
      ContainerProtos.WriteChunkRequestProto request)
      throws IOException, NoSuchAlgorithmException {
    LOG.trace("readChunk key={} from pipeline={}",
        request.getKeyName(), request.getPipeline());

    ContainerProtos.ReadChunkRequestProto.Builder readRequest =
        ContainerProtos.ReadChunkRequestProto.newBuilder();

    readRequest.setPipeline(request.getPipeline());

    readRequest.setKeyName(request.getKeyName());
    readRequest.setChunkData(request.getChunkData());

    ContainerCommandRequestProto.Builder newRequest =
        ContainerCommandRequestProto.newBuilder();
    newRequest.setCmdType(ContainerProtos.Type.ReadChunk);
    newRequest.setReadChunk(readRequest);
    newRequest.setTraceID(UUID.randomUUID().toString());
    return newRequest.build();
  }

  /**
   * Returns a delete Request.
   *
   * @param writeRequest - write request
   * @return request
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  public static ContainerCommandRequestProto getDeleteChunkRequest(
      ContainerProtos.WriteChunkRequestProto writeRequest)
      throws
      IOException, NoSuchAlgorithmException {
    LOG.trace("deleteChunk key={} from pipeline={}",
        writeRequest.getKeyName(), writeRequest.getPipeline());

    ContainerProtos.DeleteChunkRequestProto.Builder deleteRequest =
        ContainerProtos.DeleteChunkRequestProto
            .newBuilder();

    deleteRequest.setPipeline(writeRequest.getPipeline());
    deleteRequest.setChunkData(writeRequest.getChunkData());
    deleteRequest.setKeyName(writeRequest.getKeyName());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteChunk);
    request.setDeleteChunk(deleteRequest);
    request.setTraceID(UUID.randomUUID().toString());
    return request.build();
  }

  /**
   * Returns a create container command for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCreateContainerRequest(
      String containerName) throws IOException {
    LOG.trace("createContainer: {}", containerName);

    ContainerProtos.CreateContainerRequestProto.Builder createRequest =
        ContainerProtos.CreateContainerRequestProto
            .newBuilder();
    ContainerProtos.ContainerData.Builder containerData = ContainerProtos
        .ContainerData.newBuilder();
    containerData.setName(containerName);
    createRequest.setPipeline(
        ContainerTestHelper.createSingleNodePipeline(containerName)
            .getProtobufMessage());
    createRequest.setContainerData(containerData.build());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setCreateContainer(createRequest);
    request.setTraceID(UUID.randomUUID().toString());

    return request.build();
  }

  /**
   * Return an update container command for test purposes.
   * Creates a container data based on the given meta data,
   * and request to update an existing container with it.
   *
   * @param containerName
   * @param metaData
   * @return
   * @throws IOException
   */
  public static ContainerCommandRequestProto getUpdateContainerRequest(
      String containerName, Map<String, String> metaData) throws IOException {
    ContainerProtos.UpdateContainerRequestProto.Builder updateRequestBuilder =
        ContainerProtos.UpdateContainerRequestProto.newBuilder();
    ContainerProtos.ContainerData.Builder containerData = ContainerProtos
        .ContainerData.newBuilder();
    containerData.setName(containerName);
    String[] keys = metaData.keySet().toArray(new String[]{});
    for(int i=0; i<keys.length; i++) {
      OzoneProtos.KeyValue.Builder kvBuilder =
          OzoneProtos.KeyValue.newBuilder();
      kvBuilder.setKey(keys[i]);
      kvBuilder.setValue(metaData.get(keys[i]));
      containerData.addMetadata(i, kvBuilder.build());
    }

    updateRequestBuilder.setPipeline(
        ContainerTestHelper.createSingleNodePipeline(containerName)
            .getProtobufMessage());
    updateRequestBuilder.setContainerData(containerData.build());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.UpdateContainer);
    request.setUpdateContainer(updateRequestBuilder.build());
    request.setTraceID(UUID.randomUUID().toString());
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
    ContainerProtos.CreateContainerResponseProto.Builder createResponse =
        ContainerProtos.CreateContainerResponseProto.newBuilder();

    ContainerCommandResponseProto.Builder response =
        ContainerCommandResponseProto.newBuilder();
    response.setCmdType(ContainerProtos.Type.CreateContainer);
    response.setTraceID(request.getTraceID());
    response.setCreateContainer(createResponse.build());
    response.setResult(ContainerProtos.Result.SUCCESS);
    return response.build();
  }

  /**
   * Returns the PutKeyRequest for test purpose.
   *
   * @param writeRequest - Write Chunk Request.
   * @return - Request
   */
  public static ContainerCommandRequestProto getPutKeyRequest(
      ContainerProtos.WriteChunkRequestProto writeRequest) {
    LOG.trace("putKey: {} to pipeline={}",
        writeRequest.getKeyName(), writeRequest.getPipeline());

    ContainerProtos.PutKeyRequestProto.Builder putRequest =
        ContainerProtos.PutKeyRequestProto.newBuilder();

    putRequest.setPipeline(writeRequest.getPipeline());
    KeyData keyData = new KeyData(writeRequest.getPipeline().getContainerName(),
        writeRequest.getKeyName());
    List<ContainerProtos.ChunkInfo> newList = new LinkedList<>();
    newList.add(writeRequest.getChunkData());
    keyData.setChunks(newList);
    putRequest.setKeyData(keyData.getProtoBufMessage());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.PutKey);
    request.setPutKey(putRequest);
    request.setTraceID(UUID.randomUUID().toString());
    return request.build();
  }

  /**
   * Gets a GetKeyRequest for test purpose.
   *
   * @param putKeyRequest - putKeyRequest.
   * @return - Request
   */
  public static ContainerCommandRequestProto getKeyRequest(
      ContainerProtos.PutKeyRequestProto putKeyRequest) {
    LOG.trace("getKey: name={} from pipeline={}",
        putKeyRequest.getKeyData().getName(), putKeyRequest.getPipeline());

    ContainerProtos.GetKeyRequestProto.Builder getRequest =
        ContainerProtos.GetKeyRequestProto.newBuilder();
    ContainerProtos.KeyData.Builder keyData = ContainerProtos.KeyData
        .newBuilder();
    keyData.setContainerName(putKeyRequest.getPipeline().getContainerName());
    keyData.setName(putKeyRequest.getKeyData().getName());
    getRequest.setKeyData(keyData);
    getRequest.setPipeline(putKeyRequest.getPipeline());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.GetKey);
    request.setGetKey(getRequest);
    request.setTraceID(UUID.randomUUID().toString());
    return request.build();
  }

  /**
   * Verify the response against the request.
   *
   * @param request - Request
   * @param response - Response
   */
  public static void verifyGetKey(ContainerCommandRequestProto request,
      ContainerCommandResponseProto response) {
    Assert.assertEquals(request.getTraceID(), response.getTraceID());
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    ContainerProtos.PutKeyRequestProto putKey = request.getPutKey();
    ContainerProtos.GetKeyRequestProto getKey = request.getGetKey();
    Assert.assertEquals(putKey.getKeyData().getChunksCount(),
        getKey.getKeyData().getChunksCount());
  }

  /**
   * @param putKeyRequest - putKeyRequest.
   * @return - Request
   */
  public static ContainerCommandRequestProto getDeleteKeyRequest(
      ContainerProtos.PutKeyRequestProto putKeyRequest) {
    LOG.trace("deleteKey: name={} from pipeline={}",
        putKeyRequest.getKeyData().getName(), putKeyRequest.getPipeline());

    ContainerProtos.DeleteKeyRequestProto.Builder delRequest =
        ContainerProtos.DeleteKeyRequestProto.newBuilder();
    delRequest.setPipeline(putKeyRequest.getPipeline());
    delRequest.setName(putKeyRequest.getKeyData().getName());
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteKey);
    request.setDeleteKey(delRequest);
    request.setTraceID(UUID.randomUUID().toString());
    return request.build();
  }

  /**
   * Returns a close container request.
   * @param pipeline - pipeline
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCloseContainer(
      Pipeline pipeline) {
    Preconditions.checkNotNull(pipeline);
    ContainerProtos.CloseContainerRequestProto closeRequest =
        ContainerProtos.CloseContainerRequestProto.newBuilder().setPipeline(
            pipeline.getProtobufMessage()).build();
    ContainerProtos.ContainerCommandRequestProto cmd =
        ContainerCommandRequestProto.newBuilder().setCmdType(ContainerProtos
            .Type.CloseContainer).setCloseContainer(closeRequest)
            .setTraceID(UUID.randomUUID().toString())
            .build();

    return cmd;
  }

  /**
   * Returns a simple request without traceId.
   * @param pipeline - pipeline
   * @return ContainerCommandRequestProto without traceId.
   */
  public static ContainerCommandRequestProto getRequestWithoutTraceId(
          Pipeline pipeline) {
    Preconditions.checkNotNull(pipeline);
    ContainerProtos.CloseContainerRequestProto closeRequest =
            ContainerProtos.CloseContainerRequestProto.newBuilder().setPipeline(
                    pipeline.getProtobufMessage()).build();
    ContainerProtos.ContainerCommandRequestProto cmd =
            ContainerCommandRequestProto.newBuilder().setCmdType(ContainerProtos
                    .Type.CloseContainer).setCloseContainer(closeRequest)
                    .build();
    return cmd;
  }

  /**
   * Returns a delete container request.
   * @param pipeline - pipeline
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getDeleteContainer(
      Pipeline pipeline, boolean forceDelete) {
    Preconditions.checkNotNull(pipeline);
    ContainerProtos.DeleteContainerRequestProto deleteRequest =
        ContainerProtos.DeleteContainerRequestProto.newBuilder().setName(
            pipeline.getContainerName()).setPipeline(
            pipeline.getProtobufMessage()).setForceDelete(forceDelete).build();
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.DeleteContainer)
        .setDeleteContainer(deleteRequest)
        .setTraceID(UUID.randomUUID().toString())
        .build();
  }
}
