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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .BlockNotCommittedException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
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
    .GetBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .GetSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .GetSmallFileResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .PutBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .PutSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    PutSmallFileResponseProto;
import org.apache.hadoop.hdds.client.BlockID;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
   * Calls the container protocol to get a container block.
   *
   * @param xceiverClient client to perform call
   * @param datanodeBlockID blockID to identify container
   * @param traceID container protocol call args
   * @return container protocol get block response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static GetBlockResponseProto getBlock(XceiverClientSpi xceiverClient,
      DatanodeBlockID datanodeBlockID, String traceID) throws IOException {
    GetBlockRequestProto.Builder readBlockRequest = GetBlockRequestProto
        .newBuilder()
        .setBlockID(datanodeBlockID);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetBlock)
        .setContainerID(datanodeBlockID.getContainerID())
        .setTraceID(traceID)
        .setDatanodeUuid(id)
        .setGetBlock(readBlockRequest);
    String encodedToken = getEncodedBlockToken(getService(datanodeBlockID));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }

    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response);

    return response.getGetBlock();
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
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.GetCommittedBlockLength)
            .setContainerID(blockID.getContainerID())
            .setTraceID(traceID)
            .setDatanodeUuid(id)
            .setGetCommittedBlockLength(getBlockLengthRequestBuilder);
    String encodedToken = getEncodedBlockToken(new Text(blockID.
        getContainerBlockID().toString()));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response);
    return response.getGetCommittedBlockLength();
  }

  /**
   * Calls the container protocol to put a container block.
   *
   * @param xceiverClient client to perform call
   * @param containerBlockData block data to identify container
   * @param traceID container protocol call args
   * @return putBlockResponse
   * @throws IOException if there is an I/O error while performing the call
   */
  public static ContainerProtos.PutBlockResponseProto putBlock(
      XceiverClientSpi xceiverClient, BlockData containerBlockData,
      String traceID) throws IOException {
    PutBlockRequestProto.Builder createBlockRequest =
        PutBlockRequestProto.newBuilder().setBlockData(containerBlockData);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder().setCmdType(Type.PutBlock)
            .setContainerID(containerBlockData.getBlockID().getContainerID())
            .setTraceID(traceID).setDatanodeUuid(id)
            .setPutBlock(createBlockRequest);
    String encodedToken =
        getEncodedBlockToken(getService(containerBlockData.getBlockID()));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response);
    return response.getPutBlock();
  }

  /**
   * Calls the container protocol to put a container block.
   *
   * @param xceiverClient client to perform call
   * @param containerBlockData block data to identify container
   * @param traceID container protocol call args
   * @return putBlockResponse
   * @throws IOException if there is an error while performing the call
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public static XceiverClientReply putBlockAsync(
      XceiverClientSpi xceiverClient, BlockData containerBlockData,
      String traceID)
      throws IOException, InterruptedException, ExecutionException {
    PutBlockRequestProto.Builder createBlockRequest =
        PutBlockRequestProto.newBuilder().setBlockData(containerBlockData);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder().setCmdType(Type.PutBlock)
            .setContainerID(containerBlockData.getBlockID().getContainerID())
            .setTraceID(traceID).setDatanodeUuid(id)
            .setPutBlock(createBlockRequest);
    String encodedToken =
        getEncodedBlockToken(getService(containerBlockData.getBlockID()));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }
    ContainerCommandRequestProto request = builder.build();
    return xceiverClient.sendCommandAsync(request);
  }

  /**
   * Calls the container protocol to read a chunk.
   *
   * @param xceiverClient client to perform call
   * @param chunk information about chunk to read
   * @param blockID ID of the block
   * @param traceID container protocol call args
   * @param excludeDns datamode to exclude while executing the command
   * @return container protocol read chunk response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static XceiverClientReply readChunk(XceiverClientSpi xceiverClient,
      ChunkInfo chunk, BlockID blockID, String traceID,
      List<DatanodeDetails> excludeDns)
      throws IOException {
    ReadChunkRequestProto.Builder readChunkRequest = ReadChunkRequestProto
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf())
        .setChunkData(chunk);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.ReadChunk)
        .setContainerID(blockID.getContainerID())
        .setTraceID(traceID)
        .setDatanodeUuid(id)
        .setReadChunk(readChunkRequest);
    String encodedToken = getEncodedBlockToken(new Text(blockID.
        getContainerBlockID().toString()));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }
    ContainerCommandRequestProto request = builder.build();
    XceiverClientReply reply =
        xceiverClient.sendCommand(request, excludeDns);
    return reply;
  }

  /**
   * Calls the container protocol to write a chunk.
   *
   * @param xceiverClient client to perform call
   * @param chunk information about chunk to write
   * @param blockID ID of the block
   * @param data the data of the chunk to write
   * @param traceID container protocol call args
   * @throws IOException if there is an error while performing the call
   */
  public static void writeChunk(XceiverClientSpi xceiverClient, ChunkInfo chunk,
      BlockID blockID, ByteString data, String traceID)
      throws IOException {
    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf())
        .setChunkData(chunk)
        .setData(data);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.WriteChunk)
        .setContainerID(blockID.getContainerID())
        .setTraceID(traceID)
        .setDatanodeUuid(id)
        .setWriteChunk(writeChunkRequest);
    String encodedToken = getEncodedBlockToken(new Text(blockID.
        getContainerBlockID().toString()));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response);
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
  public static XceiverClientReply writeChunkAsync(
      XceiverClientSpi xceiverClient, ChunkInfo chunk, BlockID blockID,
      ByteString data, String traceID)
      throws IOException, ExecutionException, InterruptedException {
    WriteChunkRequestProto.Builder writeChunkRequest =
        WriteChunkRequestProto.newBuilder()
            .setBlockID(blockID.getDatanodeBlockIDProtobuf())
            .setChunkData(chunk).setData(data);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder().setCmdType(Type.WriteChunk)
            .setContainerID(blockID.getContainerID()).setTraceID(traceID)
            .setDatanodeUuid(id).setWriteChunk(writeChunkRequest);
    String encodedToken = getEncodedBlockToken(new Text(blockID.
        getContainerBlockID().toString()));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }
    ContainerCommandRequestProto request = builder.build();
    return xceiverClient.sendCommandAsync(request);
  }

  /**
   * Allows writing a small file using single RPC. This takes the container
   * name, block name and data to write sends all that data to the container
   * using a single RPC. This API is designed to be used for files which are
   * smaller than 1 MB.
   *
   * @param client - client that communicates with the container.
   * @param blockID - ID of the block
   * @param data - Data to be written into the container.
   * @param traceID - Trace ID for logging purpose.
   * @return container protocol writeSmallFile response
   * @throws IOException
   */
  public static PutSmallFileResponseProto writeSmallFile(
      XceiverClientSpi client, BlockID blockID, byte[] data,
      String traceID) throws IOException {

    BlockData containerBlockData =
        BlockData.newBuilder().setBlockID(blockID.getDatanodeBlockIDProtobuf())
            .build();
    PutBlockRequestProto.Builder createBlockRequest =
        PutBlockRequestProto.newBuilder()
            .setBlockData(containerBlockData);

    KeyValue keyValue =
        KeyValue.newBuilder().setKey("OverWriteRequested").setValue("true")
            .build();
    Checksum checksum = new Checksum();
    ChecksumData checksumData = checksum.computeChecksum(data, 0, data.length);
    ChunkInfo chunk =
        ChunkInfo.newBuilder()
            .setChunkName(blockID.getLocalID() + "_chunk")
            .setOffset(0)
            .setLen(data.length)
            .addMetadata(keyValue)
            .setChecksumData(checksumData.getProtoBufMessage())
            .build();

    PutSmallFileRequestProto putSmallFileRequest =
        PutSmallFileRequestProto.newBuilder().setChunkInfo(chunk)
            .setBlock(createBlockRequest).setData(ByteString.copyFrom(data))
            .build();

    String id = client.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.PutSmallFile)
            .setContainerID(blockID.getContainerID())
            .setTraceID(traceID)
            .setDatanodeUuid(id)
            .setPutSmallFile(putSmallFileRequest);
    String encodedToken = getEncodedBlockToken(new Text(blockID.
        getContainerBlockID().toString()));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response = client.sendCommand(request);
    validateContainerResponse(response);
    return response.getPutSmallFile();
  }

  /**
   * createContainer call that creates a container on the datanode.
   * @param client  - client
   * @param containerID - ID of container
   * @param traceID - traceID
   * @param encodedToken - encodedToken if security is enabled
   * @throws IOException
   */
  public static void createContainer(XceiverClientSpi client, long containerID,
      String traceID, String encodedToken) throws IOException {
    ContainerProtos.CreateContainerRequestProto.Builder createRequest =
        ContainerProtos.CreateContainerRequestProto
            .newBuilder();
    createRequest.setContainerType(ContainerProtos.ContainerType
        .KeyValueContainer);

    String id = client.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    if (encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
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
   * @param encodedToken - encodedToken if security is enabled
   * @throws IOException
   */
  public static void deleteContainer(XceiverClientSpi client, long containerID,
      boolean force, String traceID, String encodedToken) throws IOException {
    ContainerProtos.DeleteContainerRequestProto.Builder deleteRequest =
        ContainerProtos.DeleteContainerRequestProto.newBuilder();
    deleteRequest.setForceDelete(force);
    String id = client.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteContainer);
    request.setContainerID(containerID);
    request.setDeleteContainer(deleteRequest);
    request.setTraceID(traceID);
    request.setDatanodeUuid(id);
    if(encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
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
   * @param encodedToken - encodedToken if security is enabled
   * @throws IOException
   */
  public static void closeContainer(XceiverClientSpi client,
      long containerID, String traceID, String encodedToken)
      throws IOException {
    String id = client.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(CloseContainerRequestProto.getDefaultInstance());
    request.setTraceID(traceID);
    request.setDatanodeUuid(id);
    if(encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
    ContainerCommandResponseProto response =
        client.sendCommand(request.build());
    validateContainerResponse(response);
  }

  /**
   * readContainer call that gets meta data from an existing container.
   *
   * @param client       - client
   * @param traceID      - trace ID
   * @param encodedToken - encodedToken if security is enabled
   * @throws IOException
   */
  public static ReadContainerResponseProto readContainer(
      XceiverClientSpi client, long containerID,
      String traceID, String encodedToken) throws IOException {
    String id = client.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(Type.ReadContainer);
    request.setContainerID(containerID);
    request.setReadContainer(ReadContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(id);
    request.setTraceID(traceID);
    if(encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
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
    GetBlockRequestProto.Builder getBlock = GetBlockRequestProto
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf());
    ContainerProtos.GetSmallFileRequestProto getSmallFileRequest =
        GetSmallFileRequestProto
            .newBuilder().setBlock(getBlock)
            .build();
    String id = client.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetSmallFile)
        .setContainerID(blockID.getContainerID())
        .setTraceID(traceID)
        .setDatanodeUuid(id)
        .setGetSmallFile(getSmallFileRequest);
    String encodedToken = getEncodedBlockToken(new Text(blockID.
        getContainerBlockID().toString()));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response = client.sendCommand(request);
    validateContainerResponse(response);

    return response.getGetSmallFile();
  }

  /**
   * Validates a response from a container protocol call.  Any non-successful
   * return code is mapped to a corresponding exception and thrown.
   *
   * @param response container protocol call response
   * @throws StorageContainerException if the container protocol call failed
   */
  public static void validateContainerResponse(
      ContainerCommandResponseProto response
  ) throws StorageContainerException {
    if (response.getResult() == ContainerProtos.Result.SUCCESS) {
      return;
    } else if (response.getResult()
        == ContainerProtos.Result.BLOCK_NOT_COMMITTED) {
      throw new BlockNotCommittedException(response.getMessage());
    } else if (response.getResult()
        == ContainerProtos.Result.CLOSED_CONTAINER_IO) {
      throw new ContainerNotOpenException(response.getMessage());
    }
    throw new StorageContainerException(
        response.getMessage(), response.getResult());
  }

  /**
   * Returns a url encoded block token. Service param should match the service
   * field of token.
   * @param service
   *
   * */
  private static String getEncodedBlockToken(Text service)
      throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Token<OzoneBlockTokenIdentifier> token =
        OzoneBlockTokenSelector.selectBlockToken(service, ugi.getTokens());
    if (token != null) {
      return token.encodeToUrlString();
    }
    return null;
  }

  private static Text getService(DatanodeBlockID blockId) {
    return new Text(new StringBuffer()
        .append("conID: ")
        .append(blockId.getContainerID())
        .append(" locID: ")
        .append(blockId.getLocalID())
        .toString());
  }
}
