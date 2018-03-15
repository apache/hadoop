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

package org.apache.hadoop.scm.storage;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .GetKeyRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .GetKeyResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .GetSmallFileRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .GetSmallFileResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.KeyData;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .PutKeyRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .PutSmallFileRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .ReadChunkRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .ReadChunkResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.Type;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .WriteChunkRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .ReadContainerResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .ReadContainerRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.KeyValue;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;

import java.io.IOException;
import org.apache.hadoop.scm.XceiverClientSpi;

/**
 * Implementation of all container protocol calls performed by Container
 * clients.
 */
public final class ContainerProtocolCalls  {

  /**
   * There is no need to instantiate this class.
   */
  private ContainerProtocolCalls() {
  }

  /**
   * Calls the container protocol to get a container key.
   *
   * @param xceiverClient client to perform call
   * @param containerKeyData key data to identify container
   * @param traceID container protocol call args
   * @return container protocol get key response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static GetKeyResponseProto getKey(XceiverClientSpi xceiverClient,
      KeyData containerKeyData, String traceID) throws IOException {
    GetKeyRequestProto.Builder readKeyRequest = GetKeyRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyData(containerKeyData);
    String id = xceiverClient.getPipeline().getLeader().getDatanodeUuid();
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetKey)
        .setTraceID(traceID)
        .setDatanodeID(id)
        .setGetKey(readKeyRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response);
    return response.getGetKey();
  }

  /**
   * Calls the container protocol to put a container key.
   *
   * @param xceiverClient client to perform call
   * @param containerKeyData key data to identify container
   * @param traceID container protocol call args
   * @throws IOException if there is an I/O error while performing the call
   */
  public static void putKey(XceiverClientSpi xceiverClient,
      KeyData containerKeyData, String traceID) throws IOException {
    PutKeyRequestProto.Builder createKeyRequest = PutKeyRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyData(containerKeyData);
    String id = xceiverClient.getPipeline().getLeader().getDatanodeUuid();
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.PutKey)
        .setTraceID(traceID)
        .setDatanodeID(id)
        .setPutKey(createKeyRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response);
  }

  /**
   * Calls the container protocol to read a chunk.
   *
   * @param xceiverClient client to perform call
   * @param chunk information about chunk to read
   * @param key the key name
   * @param traceID container protocol call args
   * @return container protocol read chunk response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static ReadChunkResponseProto readChunk(XceiverClientSpi xceiverClient,
      ChunkInfo chunk, String key, String traceID)
      throws IOException {
    ReadChunkRequestProto.Builder readChunkRequest = ReadChunkRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyName(key)
        .setChunkData(chunk);
    String id = xceiverClient.getPipeline().getLeader().getDatanodeUuid();
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.ReadChunk)
        .setTraceID(traceID)
        .setDatanodeID(id)
        .setReadChunk(readChunkRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response);
    return response.getReadChunk();
  }

  /**
   * Calls the container protocol to write a chunk.
   *
   * @param xceiverClient client to perform call
   * @param chunk information about chunk to write
   * @param key the key name
   * @param data the data of the chunk to write
   * @param traceID container protocol call args
   * @throws IOException if there is an I/O error while performing the call
   */
  public static void writeChunk(XceiverClientSpi xceiverClient, ChunkInfo chunk,
      String key, ByteString data, String traceID)
      throws IOException {
    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyName(key)
        .setChunkData(chunk)
        .setData(data);
    String id = xceiverClient.getPipeline().getLeader().getDatanodeUuid();
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.WriteChunk)
        .setTraceID(traceID)
        .setDatanodeID(id)
        .setWriteChunk(writeChunkRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response);
  }

  /**
   * Allows writing a small file using single RPC. This takes the container
   * name, key name and data to write sends all that data to the container using
   * a single RPC. This API is designed to be used for files which are smaller
   * than 1 MB.
   *
   * @param client - client that communicates with the container.
   * @param containerName - Name of the container
   * @param key - Name of the Key
   * @param data - Data to be written into the container.
   * @param traceID - Trace ID for logging purpose.
   * @throws IOException
   */
  public static void writeSmallFile(XceiverClientSpi client,
      String containerName, String key, byte[] data, String traceID)
      throws IOException {

    KeyData containerKeyData =
        KeyData.newBuilder().setContainerName(containerName).setName(key)
            .build();
    PutKeyRequestProto.Builder createKeyRequest =
        PutKeyRequestProto.newBuilder()
            .setPipeline(client.getPipeline().getProtobufMessage())
            .setKeyData(containerKeyData);

    KeyValue keyValue =
        KeyValue.newBuilder().setKey("OverWriteRequested").setValue("true")
            .build();
    ChunkInfo chunk =
        ChunkInfo.newBuilder().setChunkName(key + "_chunk").setOffset(0)
            .setLen(data.length).addMetadata(keyValue).build();

    PutSmallFileRequestProto putSmallFileRequest =
        PutSmallFileRequestProto.newBuilder().setChunkInfo(chunk)
            .setKey(createKeyRequest).setData(ByteString.copyFrom(data))
            .build();

    String id = client.getPipeline().getLeader().getDatanodeUuid();
    ContainerCommandRequestProto request =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.PutSmallFile)
            .setTraceID(traceID)
            .setDatanodeID(id)
            .setPutSmallFile(putSmallFileRequest)
            .build();
    ContainerCommandResponseProto response = client.sendCommand(request);
    validateContainerResponse(response);
  }

  /**
   * createContainer call that creates a container on the datanode.
   * @param client  - client
   * @param traceID - traceID
   * @throws IOException
   */
  public static void createContainer(XceiverClientSpi client, String traceID)
      throws IOException {
    ContainerProtos.CreateContainerRequestProto.Builder createRequest =
        ContainerProtos.CreateContainerRequestProto
            .newBuilder();
    ContainerProtos.ContainerData.Builder containerData = ContainerProtos
        .ContainerData.newBuilder();
    containerData.setName(client.getPipeline().getContainerName());
    createRequest.setPipeline(client.getPipeline().getProtobufMessage());
    createRequest.setContainerData(containerData.build());

    String id = client.getPipeline().getLeader().getDatanodeUuid();
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setCreateContainer(createRequest);
    request.setDatanodeID(id);
    request.setTraceID(traceID);
    ContainerCommandResponseProto response = client.sendCommand(
        request.build());
    validateContainerResponse(response);
  }

  /**
   * Deletes a container from a pipeline.
   *
   * @param client
   * @param force whether or not to forcibly delete the container.
   * @param traceID
   * @throws IOException
   */
  public static void deleteContainer(XceiverClientSpi client,
      boolean force, String traceID) throws IOException {
    ContainerProtos.DeleteContainerRequestProto.Builder deleteRequest =
        ContainerProtos.DeleteContainerRequestProto.newBuilder();
    deleteRequest.setName(client.getPipeline().getContainerName());
    deleteRequest.setPipeline(client.getPipeline().getProtobufMessage());
    deleteRequest.setForceDelete(force);
    String id = client.getPipeline().getLeader().getDatanodeUuid();
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteContainer);
    request.setDeleteContainer(deleteRequest);
    request.setTraceID(traceID);
    request.setDatanodeID(id);
    ContainerCommandResponseProto response =
        client.sendCommand(request.build());
    validateContainerResponse(response);
  }

  /**
   * Close a container.
   *
   * @param client
   * @param traceID
   * @throws IOException
   */
  public static void closeContainer(XceiverClientSpi client, String traceID)
      throws IOException {
    ContainerProtos.CloseContainerRequestProto.Builder closeRequest =
        ContainerProtos.CloseContainerRequestProto.newBuilder();
    closeRequest.setPipeline(client.getPipeline().getProtobufMessage());

    String id = client.getPipeline().getLeader().getDatanodeUuid();
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(Type.CloseContainer);
    request.setCloseContainer(closeRequest);
    request.setTraceID(traceID);
    request.setDatanodeID(id);
    ContainerCommandResponseProto response =
        client.sendCommand(request.build());
    validateContainerResponse(response);
  }

  /**
   * readContainer call that gets meta data from an existing container.
   *
   * @param client - client
   * @param traceID - trace ID
   * @throws IOException
   */
  public static ReadContainerResponseProto readContainer(
      XceiverClientSpi client, String containerName,
      String traceID) throws IOException {
    ReadContainerRequestProto.Builder readRequest =
        ReadContainerRequestProto.newBuilder();
    readRequest.setName(containerName);
    readRequest.setPipeline(client.getPipeline().getProtobufMessage());
    String id = client.getPipeline().getLeader().getDatanodeUuid();
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(Type.ReadContainer);
    request.setReadContainer(readRequest);
    request.setDatanodeID(id);
    request.setTraceID(traceID);
    ContainerCommandResponseProto response =
        client.sendCommand(request.build());
    validateContainerResponse(response);
    return response.getReadContainer();
  }

  /**
   * Reads the data given the container name and key.
   *
   * @param client
   * @param containerName - name of the container
   * @param key - key
   * @param traceID - trace ID
   * @return GetSmallFileResponseProto
   * @throws IOException
   */
  public static GetSmallFileResponseProto readSmallFile(XceiverClientSpi client,
      String containerName, String key, String traceID) throws IOException {
    KeyData containerKeyData = KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(key).build();

    GetKeyRequestProto.Builder getKey = GetKeyRequestProto
        .newBuilder()
        .setPipeline(client.getPipeline().getProtobufMessage())
        .setKeyData(containerKeyData);
    ContainerProtos.GetSmallFileRequestProto getSmallFileRequest =
        GetSmallFileRequestProto
            .newBuilder().setKey(getKey)
            .build();
    String id = client.getPipeline().getLeader().getDatanodeUuid();
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetSmallFile)
        .setTraceID(traceID)
        .setDatanodeID(id)
        .setGetSmallFile(getSmallFileRequest)
        .build();
    ContainerCommandResponseProto response = client.sendCommand(request);
    validateContainerResponse(response);
    return response.getGetSmallFile();
  }

  /**
   * Validates a response from a container protocol call.  Any non-successful
   * return code is mapped to a corresponding exception and thrown.
   *
   * @param response container protocol call response
   * @throws IOException if the container protocol call failed
   */
  private static void validateContainerResponse(
      ContainerCommandResponseProto response
  ) throws StorageContainerException {
    if (response.getResult() == ContainerProtos.Result.SUCCESS) {
      return;
    }
    throw new StorageContainerException(
        response.getMessage(), response.getResult());
  }
}
