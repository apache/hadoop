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
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Type;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.helpers.FileUtils;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Result.CLOSED_CONTAINER_IO;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Result.GET_SMALL_FILE_ERROR;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Result.NO_SUCH_ALGORITHM;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Result.PUT_SMALL_FILE_ERROR;

/**
 * Ozone Container dispatcher takes a call from the netty server and routes it
 * to the right handler function.
 */
public class Dispatcher implements ContainerDispatcher {
  static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);

  private final ContainerManager containerManager;
  private ContainerMetrics metrics;
  private Configuration conf;

  /**
   * Constructs an OzoneContainer that receives calls from
   * XceiverServerHandler.
   *
   * @param containerManager - A class that manages containers.
   */
  public Dispatcher(ContainerManager containerManager, Configuration config) {
    Preconditions.checkNotNull(containerManager);
    this.containerManager = containerManager;
    this.metrics = null;
    this.conf = config;
  }

  @Override
  public void init() {
    this.metrics = ContainerMetrics.create(conf);
  }

  @Override
  public void shutdown() {
  }

  @Override
  public ContainerCommandResponseProto dispatch(
      ContainerCommandRequestProto msg) {
    LOG.trace("Command {}, trace ID: {} ", msg.getCmdType().toString(),
        msg.getTraceID());
    long startNanos = System.nanoTime();
    ContainerCommandResponseProto resp = null;
    try {
      Preconditions.checkNotNull(msg);
      Type cmdType = msg.getCmdType();
      metrics.incContainerOpcMetrics(cmdType);
      if ((cmdType == Type.CreateContainer) ||
          (cmdType == Type.DeleteContainer) ||
          (cmdType == Type.ReadContainer) ||
          (cmdType == Type.ListContainer) ||
          (cmdType == Type.UpdateContainer) ||
          (cmdType == Type.CloseContainer)) {
        resp = containerProcessHandler(msg);
      }

      if ((cmdType == Type.PutKey) ||
          (cmdType == Type.GetKey) ||
          (cmdType == Type.DeleteKey) ||
          (cmdType == Type.ListKey)) {
        resp = keyProcessHandler(msg);
      }

      if ((cmdType == Type.WriteChunk) ||
          (cmdType == Type.ReadChunk) ||
          (cmdType == Type.DeleteChunk)) {
        resp = chunkProcessHandler(msg);
      }

      if ((cmdType == Type.PutSmallFile) ||
          (cmdType == Type.GetSmallFile)) {
        resp = smallFileHandler(msg);
      }

      if (resp != null) {
        metrics.incContainerOpsLatencies(cmdType,
            System.nanoTime() - startNanos);
        return resp;
      }

      return ContainerUtils.unsupportedRequest(msg);
    } catch (StorageContainerException e) {
      // This useful since the trace ID will allow us to correlate failures.
      return ContainerUtils.logAndReturnError(LOG, e, msg);
    } catch (IllegalStateException | NullPointerException e) {
      return ContainerUtils.logAndReturnError(LOG, e, msg);
    }
  }

  public ContainerMetrics getContainerMetrics() {
    return metrics;
  }

  /**
   * Handles the all Container related functionality.
   *
   * @param msg - command
   * @return - response
   * @throws StorageContainerException
   */
  private ContainerCommandResponseProto containerProcessHandler(
      ContainerCommandRequestProto msg) throws StorageContainerException {
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
        return handleUpdateContainer(msg);

      case ReadContainer:
        return handleReadContainer(msg);

      case CloseContainer:
        return handleCloseContainer(msg);

      default:
        return ContainerUtils.unsupportedRequest(msg);
      }
    } catch (StorageContainerException e) {
      return ContainerUtils.logAndReturnError(LOG, e, msg);
    } catch (IOException ex) {
      LOG.warn("Container operation failed. " +
              "Container: {} Operation: {}  trace ID: {} Error: {}",
          msg.getCreateContainer().getContainerData().getName(),
          msg.getCmdType().name(),
          msg.getTraceID(),
          ex.toString(), ex);

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
   * @throws StorageContainerException
   */
  private ContainerCommandResponseProto keyProcessHandler(
      ContainerCommandRequestProto msg) throws StorageContainerException {
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
    } catch (StorageContainerException e) {
      return ContainerUtils.logAndReturnError(LOG, e, msg);
    } catch (IOException ex) {
      LOG.warn("Container operation failed. " +
              "Container: {} Operation: {}  trace ID: {} Error: {}",
          msg.getCreateContainer().getContainerData().getName(),
          msg.getCmdType().name(),
          msg.getTraceID(),
          ex.toString(), ex);

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
   * @throws StorageContainerException
   */
  private ContainerCommandResponseProto chunkProcessHandler(
      ContainerCommandRequestProto msg) throws StorageContainerException {
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
    } catch (StorageContainerException e) {
      return ContainerUtils.logAndReturnError(LOG, e, msg);
    } catch (IOException ex) {
      LOG.warn("Container operation failed. " +
              "Container: {} Operation: {}  trace ID: {} Error: {}",
          msg.getCreateContainer().getContainerData().getName(),
          msg.getCmdType().name(),
          msg.getTraceID(),
          ex.toString(), ex);

      // TODO : Replace with finer error codes.
      return ContainerUtils.getContainerResponse(msg,
          ContainerProtos.Result.CONTAINER_INTERNAL_ERROR,
          ex.toString()).build();
    }
  }

  /**
   * Dispatch calls to small file hanlder.
   * @param msg - request
   * @return response
   * @throws StorageContainerException
   */
  private ContainerCommandResponseProto smallFileHandler(
      ContainerCommandRequestProto msg) throws StorageContainerException {
    switch (msg.getCmdType()) {
    case PutSmallFile:
      return handlePutSmallFile(msg);
    case GetSmallFile:
      return handleGetSmallFile(msg);
    default:
      return ContainerUtils.unsupportedRequest(msg);
    }
  }

  /**
   * Update an existing container with the new container data.
   *
   * @param msg Request
   * @return ContainerCommandResponseProto
   * @throws IOException
   */
  private ContainerCommandResponseProto handleUpdateContainer(
      ContainerCommandRequestProto msg)
      throws IOException {
    if (!msg.hasUpdateContainer()) {
      LOG.debug("Malformed read container request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }

    Pipeline pipeline = Pipeline.getFromProtoBuf(
        msg.getUpdateContainer().getPipeline());
    String containerName = msg.getUpdateContainer()
        .getContainerData().getName();

    ContainerData data = ContainerData.getFromProtBuf(
        msg.getUpdateContainer().getContainerData(), conf);
    boolean forceUpdate = msg.getUpdateContainer().getForceUpdate();
    this.containerManager.updateContainer(
        pipeline, containerName, data, forceUpdate);
    return ContainerUtils.getContainerResponse(msg);
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

    Pipeline pipeline = Pipeline.getFromProtoBuf(
        msg.getDeleteContainer().getPipeline());
    Preconditions.checkNotNull(pipeline);
    String name = msg.getDeleteContainer().getName();
    boolean forceDelete = msg.getDeleteContainer().getForceDelete();
    this.containerManager.deleteContainer(pipeline, name, forceDelete);
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
        msg.getCreateContainer().getContainerData(), conf);
    Preconditions.checkNotNull(cData, "Container data is null");

    Pipeline pipeline = Pipeline.getFromProtoBuf(
        msg.getCreateContainer().getPipeline());
    Preconditions.checkNotNull(pipeline, "Pipeline cannot be null");

    this.containerManager.createContainer(pipeline, cData);
    return ContainerUtils.getContainerResponse(msg);
  }

  /**
   * closes an open container.
   *
   * @param msg -
   * @return
   * @throws IOException
   */
  private ContainerCommandResponseProto handleCloseContainer(
      ContainerCommandRequestProto msg) throws IOException {
    try {
      if (!msg.hasCloseContainer()) {
        LOG.debug("Malformed close Container request. trace ID: {}",
            msg.getTraceID());
        return ContainerUtils.malformedRequest(msg);
      }
      Pipeline pipeline = Pipeline.getFromProtoBuf(msg.getCloseContainer()
          .getPipeline());
      Preconditions.checkNotNull(pipeline, "Pipeline cannot be null");
      if (!this.containerManager.isOpen(pipeline.getContainerName())) {
        throw new StorageContainerException("Attempting to close a closed " +
            "container.", CLOSED_CONTAINER_IO);
      }
      this.containerManager.closeContainer(pipeline.getContainerName());
      return ContainerUtils.getContainerResponse(msg);
    } catch (NoSuchAlgorithmException e) {
      throw new StorageContainerException("No such Algorithm", e,
          NO_SUCH_ALGORITHM);
    }
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
    if (!this.containerManager.isOpen(pipeline.getContainerName())) {
      throw new StorageContainerException("Write to closed container.",
          CLOSED_CONTAINER_IO);
    }

    ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(msg.getWriteChunk()
        .getChunkData());
    Preconditions.checkNotNull(chunkInfo);
    byte[] data = null;
    if (msg.getWriteChunk().getStage() == ContainerProtos.Stage.WRITE_DATA
        || msg.getWriteChunk().getStage() == ContainerProtos.Stage.COMBINED) {
       data = msg.getWriteChunk().getData().toByteArray();
      metrics.incContainerBytesStats(Type.WriteChunk, data.length);

    }
    this.containerManager.getChunkManager()
        .writeChunk(pipeline, keyName, chunkInfo,
            data, msg.getWriteChunk().getStage());

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
    metrics.incContainerBytesStats(Type.ReadChunk, data.length);
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
    if (!this.containerManager.isOpen(pipeline.getContainerName())) {
      throw new StorageContainerException("Write to closed container.",
          CLOSED_CONTAINER_IO);
    }
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
    if (!msg.hasPutKey()) {
      LOG.debug("Malformed put key request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }
    Pipeline pipeline = Pipeline.getFromProtoBuf(msg.getPutKey().getPipeline());
    Preconditions.checkNotNull(pipeline);
    if (!this.containerManager.isOpen(pipeline.getContainerName())) {
      throw new StorageContainerException("Write to closed container.",
          CLOSED_CONTAINER_IO);
    }
    KeyData keyData = KeyData.getFromProtoBuf(msg.getPutKey().getKeyData());
    Preconditions.checkNotNull(keyData);
    this.containerManager.getKeyManager().putKey(pipeline, keyData);
    long numBytes = keyData.getProtoBufMessage().toByteArray().length;
    metrics.incContainerBytesStats(Type.PutKey, numBytes);
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
    if (!msg.hasGetKey()) {
      LOG.debug("Malformed get key request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }
    KeyData keyData = KeyData.getFromProtoBuf(msg.getGetKey().getKeyData());
    Preconditions.checkNotNull(keyData);
    KeyData responseData =
        this.containerManager.getKeyManager().getKey(keyData);
    long numBytes = responseData.getProtoBufMessage().toByteArray().length;
    metrics.incContainerBytesStats(Type.GetKey, numBytes);
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
    if (!msg.hasDeleteKey()) {
      LOG.debug("Malformed delete key request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }
    Pipeline pipeline =
        Pipeline.getFromProtoBuf(msg.getDeleteKey().getPipeline());
    Preconditions.checkNotNull(pipeline);
    if (!this.containerManager.isOpen(pipeline.getContainerName())) {
      throw new StorageContainerException("Write to closed container.",
          CLOSED_CONTAINER_IO);
    }
    String keyName = msg.getDeleteKey().getName();
    Preconditions.checkNotNull(keyName);
    Preconditions.checkState(!keyName.isEmpty());
    this.containerManager.getKeyManager().deleteKey(pipeline, keyName);
    return KeyUtils.getKeyResponse(msg);
  }

  /**
   * Handles writing a chunk and associated key using single RPC.
   *
   * @param msg - Message.
   * @return ContainerCommandResponseProto
   * @throws StorageContainerException
   */
  private ContainerCommandResponseProto handlePutSmallFile(
      ContainerCommandRequestProto msg) throws StorageContainerException {

    if (!msg.hasPutSmallFile()) {
      LOG.debug("Malformed put small file request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }
    try {

      Pipeline pipeline =
          Pipeline.getFromProtoBuf(msg.getPutSmallFile()
              .getKey().getPipeline());

      Preconditions.checkNotNull(pipeline);
      if (!this.containerManager.isOpen(pipeline.getContainerName())) {
        throw new StorageContainerException("Write to closed container.",
            CLOSED_CONTAINER_IO);
      }
      KeyData keyData = KeyData.getFromProtoBuf(msg.getPutSmallFile().getKey()
          .getKeyData());
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(msg.getPutSmallFile()
          .getChunkInfo());
      byte[] data = msg.getPutSmallFile().getData().toByteArray();

      metrics.incContainerBytesStats(Type.PutSmallFile, data.length);
      this.containerManager.getChunkManager().writeChunk(pipeline, keyData
          .getKeyName(), chunkInfo, data, ContainerProtos.Stage.COMBINED);
      List<ContainerProtos.ChunkInfo> chunks = new LinkedList<>();
      chunks.add(chunkInfo.getProtoBufMessage());
      keyData.setChunks(chunks);
      this.containerManager.getKeyManager().putKey(pipeline, keyData);
      return FileUtils.getPutFileResponse(msg);
    } catch (StorageContainerException e) {
      return ContainerUtils.logAndReturnError(LOG, e, msg);
    } catch (IOException e) {
      throw new StorageContainerException("Put Small File Failed.", e,
          PUT_SMALL_FILE_ERROR);
    }
  }

  /**
   * Handles getting a data stream using a key. This helps in reducing the RPC
   * overhead for small files.
   *
   * @param msg - ContainerCommandRequestProto
   * @return ContainerCommandResponseProto
   * @throws StorageContainerException
   */
  private ContainerCommandResponseProto handleGetSmallFile(
      ContainerCommandRequestProto msg) throws StorageContainerException {
    ByteString dataBuf = ByteString.EMPTY;
    if (!msg.hasGetSmallFile()) {
      LOG.debug("Malformed get small file request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }
    try {
      Pipeline pipeline =
          Pipeline.getFromProtoBuf(msg.getGetSmallFile()
              .getKey().getPipeline());

      long bytes = 0;
      Preconditions.checkNotNull(pipeline);
      KeyData keyData = KeyData.getFromProtoBuf(msg.getGetSmallFile()
          .getKey().getKeyData());
      KeyData data = this.containerManager.getKeyManager().getKey(keyData);
      ContainerProtos.ChunkInfo c = null;
      for (ContainerProtos.ChunkInfo chunk : data.getChunks()) {
        bytes += chunk.getSerializedSize();
        ByteString current =
            ByteString.copyFrom(this.containerManager.getChunkManager()
                .readChunk(
                    pipeline, keyData.getKeyName(), ChunkInfo.getFromProtoBuf(
                        chunk)));
        dataBuf = dataBuf.concat(current);
        c = chunk;
      }
      metrics.incContainerBytesStats(Type.GetSmallFile, bytes);
      return FileUtils.getGetSmallFileResponse(msg, dataBuf.toByteArray(),
          ChunkInfo.getFromProtoBuf(c));
    } catch (StorageContainerException e) {
      return ContainerUtils.logAndReturnError(LOG, e, msg);
    } catch (IOException e) {
      throw new StorageContainerException("Get Small File Failed", e,
          GET_SMALL_FILE_ERROR);
    }
  }
}
