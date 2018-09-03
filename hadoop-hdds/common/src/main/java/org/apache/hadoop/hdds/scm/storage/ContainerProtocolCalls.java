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

package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.hdds.scm.container.common.helpers
    .BlockNotCommittedException;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .CloseContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .GetKeyRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .GetKeyResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .GetSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .GetSmallFileResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .PutKeyRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .PutSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.client.BlockID;

import java.io.IOException;

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
   * @param datanodeBlockID blockID to identify container
   * @param traceID container protocol call args
   * @return container protocol get key response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static GetKeyResponseProto getKey(XceiverClientSpi xceiverClient,
      DatanodeBlockID datanodeBlockID, String traceID) throws IOException {
    GetKeyRequestProto.Builder readKeyRequest = GetKeyRequestProto
        .newBuilder()
        .setBlockID(datanodeBlockID);
    String id = xceiverClient.getPipeline().getLeader().getUuidString();

    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetKey)
        .setContainerID(datanodeBlockID.getContainerID())
        .setTraceID(traceID)
        .setDatanodeUuid(id)
        .setGetKey(readKeyRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response);

    return response.getGetKey();
  }

  /**
   * Calls the container protocol to get the length of a committed block.
   *
   * @param xceiverClient client to perform call
   * @param blockID blockId for the Block
   * @param traceID container protocol call args
   * @return container protocol getLastCommittedBlockLength response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static ContainerProtos.GetCommittedBlockLengthResponseProto
      getCommittedBlockLength(
          XceiverClientSpi xceiverClient, BlockID blockID, String traceID)
      throws IOException {
    ContainerProtos.GetCommittedBlockLengthRequestProto.Builder
        getBlockLengthRequestBuilder =
        ContainerProtos.GetCommittedBlockLengthRequestProto.newBuilder().
            setBlockID(blockID.getDatanodeBlockIDProtobuf());
    String id = xceiverClient.getPipeline().getLeader().getUuidString();
    ContainerCommandRequestProto request =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.GetCommittedBlockLength)
            .setContainerID(blockID.getContainerID())
            .setTraceID(traceID)
            .setDatanodeUuid(id)
            .setGetCommittedBlockLength(getBlockLengthRequestBuilder).build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response);
    return response.getGetCommittedBlockLength();
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
        .setKeyData(containerKeyData);
    String id = xceiverClient.getPipeline().getLeader().getUuidString();
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.PutKey)
        .setContainerID(containerKeyData.getBlockID().getContainerID())
        .setTraceID(traceID)
        .setDatanodeUuid(id)
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
   * @param blockID ID of the block
   * @param traceID container protocol call args
   * @return container protocol read chunk response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static ReadChunkResponseProto readChunk(XceiverClientSpi xceiverClient,
        ChunkInfo chunk, BlockID blockID, String traceID) throws IOException {
    ReadChunkRequestProto.Builder readChunkRequest = ReadChunkRequestProto
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf())
        .setChunkData(chunk);
    String id = xceiverClient.getPipeline().getLeader().getUuidString();
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.ReadChunk)
        .setContainerID(blockID.getContainerID())
        .setTraceID(traceID)
        .setDatanodeUuid(id)
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
   * @param blockID ID of the block
   * @param data the data of the chunk to write
   * @param traceID container protocol call args
   * @throws IOException if there is an I/O error while performing the call
   */
  public static void writeChunk(XceiverClientSpi xceiverClient, ChunkInfo chunk,
      BlockID blockID, ByteString data, String traceID)
      throws IOException {
    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf())
        .setChunkData(chunk)
        .setData(data);
    String id = xceiverClient.getPipeline().getLeader().getUuidString();
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.WriteChunk)
        .setContainerID(blockID.getContainerID())
        .setTraceID(traceID)
        .setDatanodeUuid(id)
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
   * @param blockID - ID of the block
   * @param data - Data to be written into the container.
   * @param traceID - Trace ID for logging purpose.
   * @throws IOException
   */
  public static void writeSmallFile(XceiverClientSpi client,
      BlockID blockID, byte[] data, String traceID)
      throws IOException {

    KeyData containerKeyData =
        KeyData.newBuilder().setBlockID(blockID.getDatanodeBlockIDProtobuf())
            .build();
    PutKeyRequestProto.Builder createKeyRequest =
        PutKeyRequestProto.newBuilder()
            .setKeyData(containerKeyData);

    KeyValue keyValue =
        KeyValue.newBuilder().setKey("OverWriteRequested").setValue("true")
            .build();
    ChunkInfo chunk =
        ChunkInfo.newBuilder().setChunkName(blockID.getLocalID()
            + "_chunk").setOffset(0).setLen(data.length).
            addMetadata(keyValue).build();

    PutSmallFileRequestProto putSmallFileRequest =
        PutSmallFileRequestProto.newBuilder().setChunkInfo(chunk)
            .setKey(createKeyRequest).setData(ByteString.copyFrom(data))
            .build();

    String id = client.getPipeline().getLeader().getUuidString();
    ContainerCommandRequestProto request =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.PutSmallFile)
            .setContainerID(blockID.getContainerID())
            .setTraceID(traceID)
            .setDatanodeUuid(id)
            .setPutSmallFile(putSmallFileRequest)
            .build();
    ContainerCommandResponseProto response = client.sendCommand(request);
    validateContainerResponse(response);
  }

  /**
   * createContainer call that creates a container on the datanode.
   * @param client  - client
   * @param containerID - ID of container
   * @param traceID - traceID
   * @throws IOException
   */
  public static void createContainer(XceiverClientSpi client, long containerID,
      String traceID) throws IOException {
    ContainerProtos.CreateContainerRequestProto.Builder createRequest =
        ContainerProtos.CreateContainerRequestProto
            .newBuilder();
    createRequest.setContainerType(ContainerProtos.ContainerType
        .KeyValueContainer);

    String id = client.getPipeline().getLeader().getUuidString();
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setContainerID(containerID);
    request.setCreateContainer(createRequest.build());
    request.setDatanodeUuid(id);
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
  public static void deleteContainer(XceiverClientSpi client, long containerID,
      boolean force, String traceID) throws IOException {
    ContainerProtos.DeleteContainerRequestProto.Builder deleteRequest =
        ContainerProtos.DeleteContainerRequestProto.newBuilder();
    deleteRequest.setForceDelete(force);
    String id = client.getPipeline().getLeader().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteContainer);
    request.setContainerID(containerID);
    request.setDeleteContainer(deleteRequest);
    request.setTraceID(traceID);
    request.setDatanodeUuid(id);
    ContainerCommandResponseProto response =
        client.sendCommand(request.build());
    validateContainerResponse(response);
  }

  /**
   * Close a container.
   *
   * @param client
   * @param containerID
   * @param traceID
   * @throws IOException
   */
  public static void closeContainer(XceiverClientSpi client,
      long containerID, String traceID) throws IOException {
    String id = client.getPipeline().getLeader().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(CloseContainerRequestProto.getDefaultInstance());
    request.setTraceID(traceID);
    request.setDatanodeUuid(id);
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
      XceiverClientSpi client, long containerID,
      String traceID) throws IOException {
    String id = client.getPipeline().getLeader().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(Type.ReadContainer);
    request.setContainerID(containerID);
    request.setReadContainer(ReadContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(id);
    request.setTraceID(traceID);
    ContainerCommandResponseProto response =
        client.sendCommand(request.build());
    validateContainerResponse(response);

    return response.getReadContainer();
  }

  /**
   * Reads the data given the blockID.
   *
   * @param client
   * @param blockID - ID of the block
   * @param traceID - trace ID
   * @return GetSmallFileResponseProto
   * @throws IOException
   */
  public static GetSmallFileResponseProto readSmallFile(XceiverClientSpi client,
      BlockID blockID, String traceID) throws IOException {
    GetKeyRequestProto.Builder getKey = GetKeyRequestProto
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf());
    ContainerProtos.GetSmallFileRequestProto getSmallFileRequest =
        GetSmallFileRequestProto
            .newBuilder().setKey(getKey)
            .build();
    String id = client.getPipeline().getLeader().getUuidString();

    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetSmallFile)
        .setContainerID(blockID.getContainerID())
        .setTraceID(traceID)
        .setDatanodeUuid(id)
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
    } else if (response.getResult()
        == ContainerProtos.Result.BLOCK_NOT_COMMITTED) {
      throw new BlockNotCommittedException(response.getMessage());
    }
    throw new StorageContainerException(
        response.getMessage(), response.getResult());
  }
}
