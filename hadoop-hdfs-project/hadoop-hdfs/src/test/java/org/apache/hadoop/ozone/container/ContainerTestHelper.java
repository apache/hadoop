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

import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.junit.Assert;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Helpers for container tests.
 */
public class ContainerTestHelper {
  private static Random r = new Random();

  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   * @throws IOException
   */
  public static Pipeline createSingleNodePipeline(String containerName) throws
      IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    DatanodeID datanodeID = new DatanodeID(socket.getInetAddress()
        .getHostAddress(), socket.getInetAddress().getHostName(),
        UUID.randomUUID().toString(), port, port, port, port);
    datanodeID.setContainerPort(port);
    Pipeline pipeline = new Pipeline(datanodeID.getDatanodeUuid());
    pipeline.addMember(datanodeID);
    pipeline.setContainerName(containerName);
    socket.close();
    return pipeline;
  }

  /**
   * Creates a ChunkInfo for testing.
   *
   * @param keyName - Name of the key
   * @param seqNo   - Chunk number.
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
   * @param containerName - Name
   * @param keyName       - Name
   * @param datalen       - data len.
   * @return Request.
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  public static ContainerCommandRequestProto getWriteChunkRequest(
      Pipeline pipeline, String containerName, String keyName, int datalen)
      throws
      IOException, NoSuchAlgorithmException {
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
      throws
      IOException, NoSuchAlgorithmException {
    ContainerProtos.ReadChunkRequestProto.Builder readRequest =
        ContainerProtos.ReadChunkRequestProto.newBuilder();

    readRequest.setPipeline(request.getPipeline());

    readRequest.setKeyName(request.getKeyName());
    readRequest.setChunkData(request.getChunkData());

    ContainerCommandRequestProto.Builder newRequest =
        ContainerCommandRequestProto.newBuilder();
    newRequest.setCmdType(ContainerProtos.Type.ReadChunk);
    newRequest.setReadChunk(readRequest);
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
    return request.build();
  }

  /**
   * Returns a create container response for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandResponseProto
  getCreateContainerResponse(ContainerCommandRequestProto request) throws
      IOException {
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
    return request.build();
  }

  /**
   *  Verify the response against the request.
   * @param request  - Request
   * @param response  - Response
   */
  public static void verifyGetKey(ContainerCommandRequestProto request,
                             ContainerCommandResponseProto response) {
    Assert.assertEquals(request.getTraceID(), response.getTraceID());
    Assert.assertEquals(response.getResult(), ContainerProtos.Result.SUCCESS);
    ContainerProtos.PutKeyRequestProto putKey = request.getPutKey();
    ContainerProtos. GetKeyRequestProto getKey = request.getGetKey();
    Assert.assertEquals(putKey.getKeyData().getChunksCount(),
                        getKey.getKeyData().getChunksCount());
  }


  /**
   *
   * @param putKeyRequest - putKeyRequest.
   * @return - Request
   */
  public static ContainerCommandRequestProto getDeleteKeyRequest(
      ContainerProtos.PutKeyRequestProto putKeyRequest) {
    ContainerProtos.DeleteKeyRequestProto.Builder delRequest =
        ContainerProtos.DeleteKeyRequestProto.newBuilder();
    delRequest.setPipeline(putKeyRequest.getPipeline());
    delRequest.setName(putKeyRequest.getKeyData().getName());
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteKey);
    request.setDeleteKey(delRequest);
    return request.build();
  }

}
