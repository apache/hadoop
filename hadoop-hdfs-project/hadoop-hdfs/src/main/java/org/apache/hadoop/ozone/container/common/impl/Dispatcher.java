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

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Type;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Ozone Container dispatcher takes a call from the netty server and routes it
 * to the right handler function.
 */
public class Dispatcher implements ContainerDispatcher {
  static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);

  private final ContainerManager containerManager;

  /**
   * Constructs an OzoneContainer that receives calls from
   * XceiverServerHandler.
   *
   * @param containerManager - A class that manages containers.
   */
  public Dispatcher(ContainerManager containerManager) {
    Preconditions.checkNotNull(containerManager);
    this.containerManager = containerManager;
  }

  @Override
  public ContainerCommandResponseProto dispatch(
      ContainerCommandRequestProto msg) throws IOException {
    Preconditions.checkNotNull(msg);
    Type cmdType = msg.getCmdType();
    if ((cmdType == Type.CreateContainer) ||
        (cmdType == Type.DeleteContainer) ||
        (cmdType == Type.ReadContainer) ||
        (cmdType == Type.ListContainer) ||
        (cmdType == Type.UpdateContainer)) {

      return containerProcessHandler(msg);
    }

    if ((cmdType == Type.PutKey) ||
        (cmdType == Type.GetKey) ||
        (cmdType == Type.DeleteKey) ||
        (cmdType == Type.ListKey)) {
      return keyProcessHandler(msg);
    }

    if ((cmdType == Type.WriteChunk) ||
        (cmdType == Type.ReadChunk) ||
        (cmdType == Type.DeleteChunk)) {
      return chunkProcessHandler(msg);
    }

    return ContainerUtils.unsupportedRequest(msg);
  }

  /**
   * Handles the all Container related functionality.
   *
   * @param msg - command
   * @return - response
   * @throws IOException
   */
  private ContainerCommandResponseProto containerProcessHandler(
      ContainerCommandRequestProto msg) throws IOException {
    try {

      switch (msg.getCmdType()) {
      case CreateContainer:
        return handleCreateContainer(msg);

      case DeleteContainer:
        return handleDeleteContainer(msg);

      case ListContainer:
        // TODO : Support List Container.
        return ContainerUtils.unsupportedRequest(msg);

      case UpdateContainer:
        // TODO : Support Update Container.
        return ContainerUtils.unsupportedRequest(msg);

      case ReadContainer:
        return handleReadContainer(msg);

      default:
        return ContainerUtils.unsupportedRequest(msg);
      }
    } catch (IOException ex) {
      LOG.warn("Container operation failed. " +
              "Container: {} Operation: {}  trace ID: {} Error: {}",
          msg.getCreateContainer().getContainerData().getName(),
          msg.getCmdType().name(),
          msg.getTraceID(),
          ex.toString());

      // TODO : Replace with finer error codes.
      return ContainerUtils.getContainerResponse(msg,
          ContainerProtos.Result.CONTAINER_INTERNAL_ERROR,
          ex.toString()).build();
    }
  }

  /**
   * Handles the all key related functionality.
   *
   * @param msg - command
   * @return - response
   * @throws IOException
   */
  private ContainerCommandResponseProto keyProcessHandler(
      ContainerCommandRequestProto msg) throws IOException {
    try {
      switch (msg.getCmdType()) {
      case PutKey:
        return handlePutKey(msg);

      case GetKey:
        return handleGetKey(msg);

      case DeleteKey:
        return handleDeleteKey(msg);

      case ListKey:
        return ContainerUtils.unsupportedRequest(msg);

      default:
        return ContainerUtils.unsupportedRequest(msg);

      }
    } catch (IOException ex) {
      LOG.warn("Container operation failed. " +
              "Container: {} Operation: {}  trace ID: {} Error: {}",
          msg.getCreateContainer().getContainerData().getName(),
          msg.getCmdType().name(),
          msg.getTraceID(),
          ex.toString());

      // TODO : Replace with finer error codes.
      return ContainerUtils.getContainerResponse(msg,
          ContainerProtos.Result.CONTAINER_INTERNAL_ERROR,
          ex.toString()).build();
    }
  }

  /**
   * Handles the all chunk related functionality.
   *
   * @param msg - command
   * @return - response
   * @throws IOException
   */
  private ContainerCommandResponseProto chunkProcessHandler(
      ContainerCommandRequestProto msg) throws IOException {
    try {
      switch (msg.getCmdType()) {
      case WriteChunk:
        return handleWriteChunk(msg);

      case ReadChunk:
        return handleReadChunk(msg);

      case DeleteChunk:
        return handleDeleteChunk(msg);

      case ListChunk:
        return ContainerUtils.unsupportedRequest(msg);

      default:
        return ContainerUtils.unsupportedRequest(msg);
      }
    } catch (IOException ex) {
      LOG.warn("Container operation failed. " +
              "Container: {} Operation: {}  trace ID: {} Error: {}",
          msg.getCreateContainer().getContainerData().getName(),
          msg.getCmdType().name(),
          msg.getTraceID(),
          ex.toString());

      // TODO : Replace with finer error codes.
      return ContainerUtils.getContainerResponse(msg,
          ContainerProtos.Result.CONTAINER_INTERNAL_ERROR,
          ex.toString()).build();
    }
  }

  /**
   * Calls into container logic and returns appropriate response.
   *
   * @param msg - Request
   * @return ContainerCommandResponseProto
   * @throws IOException
   */
  private ContainerCommandResponseProto handleReadContainer(
      ContainerCommandRequestProto msg)
      throws IOException {

    if (!msg.hasReadContainer()) {
      LOG.debug("Malformed read container request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }

    String name = msg.getReadContainer().getName();
    ContainerData container = this.containerManager.readContainer(name);
    return ContainerUtils.getReadContainerResponse(msg, container);
  }

  /**
   * Calls into container logic and returns appropriate response.
   *
   * @param msg - Request
   * @return Response.
   * @throws IOException
   */
  private ContainerCommandResponseProto handleDeleteContainer(
      ContainerCommandRequestProto msg) throws IOException {

    if (!msg.hasDeleteContainer()) {
      LOG.debug("Malformed delete container request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }

    String name = msg.getDeleteContainer().getName();
    Pipeline pipeline = Pipeline.getFromProtoBuf(
        msg.getDeleteContainer().getPipeline());
    Preconditions.checkNotNull(pipeline);

    this.containerManager.deleteContainer(pipeline, name);
    return ContainerUtils.getContainerResponse(msg);
  }

  /**
   * Calls into container logic and returns appropriate response.
   *
   * @param msg - Request
   * @return Response.
   * @throws IOException
   */
  private ContainerCommandResponseProto handleCreateContainer(
      ContainerCommandRequestProto msg) throws IOException {
    if (!msg.hasCreateContainer()) {
      LOG.debug("Malformed create container request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }
    ContainerData cData = ContainerData.getFromProtBuf(
        msg.getCreateContainer().getContainerData());
    Preconditions.checkNotNull(cData);

    Pipeline pipeline = Pipeline.getFromProtoBuf(
        msg.getCreateContainer().getPipeline());
    Preconditions.checkNotNull(pipeline);

    this.containerManager.createContainer(pipeline, cData);
    return ContainerUtils.getContainerResponse(msg);
  }

  /**
   * Calls into chunk manager to write a chunk.
   *
   * @param msg - Request.
   * @return Response.
   * @throws IOException
   */
  private ContainerCommandResponseProto handleWriteChunk(
      ContainerCommandRequestProto msg) throws IOException {
    if (!msg.hasWriteChunk()) {
      LOG.debug("Malformed write chunk request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }

    String keyName = msg.getWriteChunk().getKeyName();
    Pipeline pipeline = Pipeline.getFromProtoBuf(
        msg.getWriteChunk().getPipeline());
    Preconditions.checkNotNull(pipeline);

    ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(msg.getWriteChunk()
        .getChunkData());
    Preconditions.checkNotNull(chunkInfo);
    byte[] data = msg.getWriteChunk().getData().toByteArray();
    this.containerManager.getChunkManager().writeChunk(pipeline, keyName,
        chunkInfo, data);
    return ChunkUtils.getChunkResponse(msg);
  }

  /**
   * Calls into chunk manager to read a chunk.
   *
   * @param msg - Request.
   * @return - Response.
   * @throws IOException
   */
  private ContainerCommandResponseProto handleReadChunk(
      ContainerCommandRequestProto msg) throws IOException {
    if (!msg.hasReadChunk()) {
      LOG.debug("Malformed read chunk request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }

    String keyName = msg.getReadChunk().getKeyName();
    Pipeline pipeline = Pipeline.getFromProtoBuf(
        msg.getReadChunk().getPipeline());
    Preconditions.checkNotNull(pipeline);

    ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(msg.getReadChunk()
        .getChunkData());
    Preconditions.checkNotNull(chunkInfo);
    byte[] data = this.containerManager.getChunkManager().readChunk(pipeline,
        keyName, chunkInfo);
    return ChunkUtils.getReadChunkResponse(msg, data, chunkInfo);
  }

  /**
   * Calls into chunk manager to write a chunk.
   *
   * @param msg - Request.
   * @return Response.
   * @throws IOException
   */
  private ContainerCommandResponseProto handleDeleteChunk(
      ContainerCommandRequestProto msg) throws IOException {
    if (!msg.hasDeleteChunk()) {
      LOG.debug("Malformed delete chunk request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }

    String keyName = msg.getDeleteChunk().getKeyName();
    Pipeline pipeline = Pipeline.getFromProtoBuf(
        msg.getDeleteChunk().getPipeline());
    Preconditions.checkNotNull(pipeline);

    ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(msg.getDeleteChunk()
        .getChunkData());
    Preconditions.checkNotNull(chunkInfo);

    this.containerManager.getChunkManager().deleteChunk(pipeline, keyName,
        chunkInfo);
    return ChunkUtils.getChunkResponse(msg);
  }

  /**
   * Put Key handler.
   *
   * @param msg - Request.
   * @return - Response.
   * @throws IOException
   */
  private ContainerCommandResponseProto handlePutKey(
      ContainerCommandRequestProto msg) throws IOException {
    if(!msg.hasPutKey()){
      LOG.debug("Malformed put key request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }
    Pipeline pipeline = Pipeline.getFromProtoBuf(msg.getPutKey().getPipeline());
    Preconditions.checkNotNull(pipeline);
    KeyData keyData = KeyData.getFromProtoBuf(msg.getPutKey().getKeyData());
    Preconditions.checkNotNull(keyData);
    this.containerManager.getKeyManager().putKey(pipeline, keyData);
    return KeyUtils.getKeyResponse(msg);
  }

  /**
   * Handle Get Key.
   *
   * @param msg - Request.
   * @return - Response.
   * @throws IOException
   */
  private ContainerCommandResponseProto handleGetKey(
      ContainerCommandRequestProto msg) throws IOException {
    if(!msg.hasGetKey()){
      LOG.debug("Malformed get key request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }
    KeyData keyData = KeyData.getFromProtoBuf(msg.getGetKey().getKeyData());
    Preconditions.checkNotNull(keyData);
    KeyData responseData =
        this.containerManager.getKeyManager().getKey(keyData);
    return KeyUtils.getKeyDataResponse(msg, responseData);
  }

  /**
   * Handle Delete Key.
   *
   * @param msg - Request.
   * @return - Response.
   * @throws IOException
   */
  private ContainerCommandResponseProto handleDeleteKey(
      ContainerCommandRequestProto msg) throws IOException {
    if(!msg.hasDeleteKey()){
      LOG.debug("Malformed delete key request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }

    Pipeline pipeline =
        Pipeline.getFromProtoBuf(msg.getDeleteKey().getPipeline());
    Preconditions.checkNotNull(pipeline);
    String keyName = msg.getDeleteKey().getName();
    Preconditions.checkNotNull(keyName);
    Preconditions.checkState(!keyName.isEmpty());

    this.containerManager.getKeyManager().deleteKey(pipeline, keyName);
    return KeyUtils.getKeyResponse(msg);
  }

}
