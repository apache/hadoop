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

package org.apache.hadoop.ozone.web.storage;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

import java.io.IOException;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.GetKeyRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.GetKeyResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.KeyData;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.PutKeyRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ReadChunkRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Type;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClient;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;

/**
 * Implementation of all container protocol calls performed by
 * {@link DistributedStorageHandler}.
 */
final class ContainerProtocolCalls {

  /**
   * Calls the container protocol to get a container key.
   *
   * @param xceiverClient client to perform call
   * @param containerKeyData key data to identify container
   * @param args container protocol call args
   * @returns container protocol get key response
   * @throws IOException if there is an I/O error while performing the call
   * @throws OzoneException if the container protocol call failed
   */
  public static GetKeyResponseProto getKey(XceiverClient xceiverClient,
      KeyData containerKeyData, UserArgs args) throws IOException,
      OzoneException {
    GetKeyRequestProto.Builder readKeyRequest = GetKeyRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyData(containerKeyData);
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetKey)
        .setTraceID(args.getRequestID())
        .setGetKey(readKeyRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response, args);
    return response.getGetKey();
  }

  /**
   * Calls the container protocol to put a container key.
   *
   * @param xceiverClient client to perform call
   * @param containerKeyData key data to identify container
   * @param args container protocol call args
   * @throws IOException if there is an I/O error while performing the call
   * @throws OzoneException if the container protocol call failed
   */
  public static void putKey(XceiverClient xceiverClient,
      KeyData containerKeyData, UserArgs args) throws IOException,
      OzoneException {
    PutKeyRequestProto.Builder createKeyRequest = PutKeyRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyData(containerKeyData);
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.PutKey)
        .setTraceID(args.getRequestID())
        .setPutKey(createKeyRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response, args);
  }

  /**
   * Calls the container protocol to read a chunk.
   *
   * @param xceiverClient client to perform call
   * @param chunk information about chunk to read
   * @param key the key name
   * @param args container protocol call args
   * @returns container protocol read chunk response
   * @throws IOException if there is an I/O error while performing the call
   * @throws OzoneException if the container protocol call failed
   */
  public static ReadChunkResponseProto readChunk(XceiverClient xceiverClient,
      ChunkInfo chunk, String key, UserArgs args)
      throws IOException, OzoneException {
    ReadChunkRequestProto.Builder readChunkRequest = ReadChunkRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyName(key)
        .setChunkData(chunk);
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.ReadChunk)
        .setTraceID(args.getRequestID())
        .setReadChunk(readChunkRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response, args);
    return response.getReadChunk();
  }

  /**
   * Calls the container protocol to write a chunk.
   *
   * @param xceiverClient client to perform call
   * @param chunk information about chunk to write
   * @param key the key name
   * @param data the data of the chunk to write
   * @param args container protocol call args
   * @throws IOException if there is an I/O error while performing the call
   * @throws OzoneException if the container protocol call failed
   */
  public static void writeChunk(XceiverClient xceiverClient, ChunkInfo chunk,
      String key, ByteString data, UserArgs args)
      throws IOException, OzoneException {
    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyName(key)
        .setChunkData(chunk)
        .setData(data);
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.WriteChunk)
        .setTraceID(args.getRequestID())
        .setWriteChunk(writeChunkRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response, args);
  }

  /**
   * Validates a response from a container protocol call.  Any non-successful
   * return code is mapped to a corresponding exception and thrown.
   *
   * @param response container protocol call response
   * @param args container protocol call args
   * @throws OzoneException if the container protocol call failed
   */
  private static void validateContainerResponse(
      ContainerCommandResponseProto response, UserArgs args)
      throws OzoneException {
    switch (response.getResult()) {
    case SUCCESS:
      break;
    case MALFORMED_REQUEST:
      throw ErrorTable.newError(new OzoneException(HTTP_BAD_REQUEST,
          "badRequest", "Bad container request."), args);
    case UNSUPPORTED_REQUEST:
      throw ErrorTable.newError(new OzoneException(HTTP_INTERNAL_ERROR,
          "internalServerError", "Unsupported container request."), args);
    case CONTAINER_INTERNAL_ERROR:
      throw ErrorTable.newError(new OzoneException(HTTP_INTERNAL_ERROR,
          "internalServerError", "Container internal error."), args);
    default:
      throw ErrorTable.newError(new OzoneException(HTTP_INTERNAL_ERROR,
          "internalServerError", "Unrecognized container response."), args);
    }
  }

  /**
   * There is no need to instantiate this class.
   */
  private ContainerProtocolCalls() {
  }
}
