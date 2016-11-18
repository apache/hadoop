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

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

import java.io.IOException;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
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
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.PutSmallFileRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.GetSmallFileResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.GetSmallFileRequestProto;
import org.apache.hadoop.scm.XceiverClient;

/**
 * Implementation of all container protocol calls performed by Container
 * clients.
 */
public final class ContainerProtocolCalls {

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
  public static GetKeyResponseProto getKey(XceiverClient xceiverClient,
      KeyData containerKeyData, String traceID) throws IOException {
    GetKeyRequestProto.Builder readKeyRequest = GetKeyRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyData(containerKeyData);
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetKey)
        .setTraceID(traceID)
        .setGetKey(readKeyRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response, traceID);
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
  public static void putKey(XceiverClient xceiverClient,
      KeyData containerKeyData, String traceID) throws IOException {
    PutKeyRequestProto.Builder createKeyRequest = PutKeyRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyData(containerKeyData);
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.PutKey)
        .setTraceID(traceID)
        .setPutKey(createKeyRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response, traceID);
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
  public static ReadChunkResponseProto readChunk(XceiverClient xceiverClient,
      ChunkInfo chunk, String key, String traceID)
      throws IOException {
    ReadChunkRequestProto.Builder readChunkRequest = ReadChunkRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyName(key)
        .setChunkData(chunk);
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.ReadChunk)
        .setTraceID(traceID)
        .setReadChunk(readChunkRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response, traceID);
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
  public static void writeChunk(XceiverClient xceiverClient, ChunkInfo chunk,
      String key, ByteString data, String traceID)
      throws IOException {
    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setKeyName(key)
        .setChunkData(chunk)
        .setData(data);
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.WriteChunk)
        .setTraceID(traceID)
        .setWriteChunk(writeChunkRequest)
        .build();
    ContainerCommandResponseProto response = xceiverClient.sendCommand(request);
    validateContainerResponse(response, traceID);
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
  public static void writeSmallFile(XceiverClient client, String containerName,
      String key, byte[] data, String traceID) throws IOException {

    KeyData containerKeyData = KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(key).build();
    PutKeyRequestProto.Builder createKeyRequest = PutKeyRequestProto
        .newBuilder()
        .setPipeline(client.getPipeline().getProtobufMessage())
        .setKeyData(containerKeyData);

    ChunkInfo chunk = ChunkInfo
        .newBuilder()
        .setChunkName(key + "_chunk")
        .setOffset(0)
        .setLen(data.length)
        .build();

    PutSmallFileRequestProto putSmallFileRequest = PutSmallFileRequestProto
        .newBuilder().setChunkInfo(chunk)
        .setKey(createKeyRequest)
        .setData(ByteString.copyFrom(data))
        .build();

    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.PutSmallFile)
        .setTraceID(traceID)
        .setPutSmallFile(putSmallFileRequest)
        .build();
    ContainerCommandResponseProto response = client.sendCommand(request);
    validateContainerResponse(response, traceID);
  }

  /**
   * Reads the data given the container name and key.
   *
   * @param client - client
   * @param containerName - name of the container
   * @param key - key
   * @param traceID - trace ID
   * @return GetSmallFileResponseProto
   * @throws IOException
   */
  public static GetSmallFileResponseProto readSmallFile(XceiverClient client,
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
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetSmallFile)
        .setTraceID(traceID)
        .setGetSmallFile(getSmallFileRequest)
        .build();
    ContainerCommandResponseProto response = client.sendCommand(request);
    validateContainerResponse(response, traceID);
    return response.getGetSmallFile();
  }

  /**
   * Validates a response from a container protocol call.  Any non-successful
   * return code is mapped to a corresponding exception and thrown.
   *
   * @param response container protocol call response
   * @param traceID container protocol call args
   * @throws IOException if the container protocol call failed
   */
  private static void validateContainerResponse(
      ContainerCommandResponseProto response, String traceID
  ) throws IOException {
    // TODO : throw the right type of exception
    switch (response.getResult()) {
    case SUCCESS:
      break;
    case MALFORMED_REQUEST:
      throw new IOException(HTTP_BAD_REQUEST +
          ":Bad container request: " + traceID);
    case UNSUPPORTED_REQUEST:
      throw new IOException(HTTP_INTERNAL_ERROR +
          "Unsupported container request: " + traceID);
    case CONTAINER_INTERNAL_ERROR:
      throw new IOException(HTTP_INTERNAL_ERROR +
          "Container internal error:" + traceID);
    default:
      throw new IOException(HTTP_INTERNAL_ERROR +
          "Unrecognized container response:" + traceID);
    }
  }
}
