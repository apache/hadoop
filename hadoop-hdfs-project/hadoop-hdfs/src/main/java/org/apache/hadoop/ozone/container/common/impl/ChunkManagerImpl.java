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
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.UNSUPPORTED_REQUEST;

/**
 * An implementation of ChunkManager that is used by default in ozone.
 */
public class ChunkManagerImpl implements ChunkManager {
  static final Logger LOG =
      LoggerFactory.getLogger(ChunkManagerImpl.class);

  private final ContainerManager containerManager;

  /**
   * Constructs a ChunkManager.
   *
   * @param manager - ContainerManager.
   */
  public ChunkManagerImpl(ContainerManager manager) {
    this.containerManager = manager;
  }

  /**
   * writes a given chunk.
   *
   * @param pipeline - Name and the set of machines that make this container.
   * @param keyName - Name of the Key.
   * @param info - ChunkInfo.
   * @throws StorageContainerException
   */
  @Override
  public void writeChunk(Pipeline pipeline, String keyName, ChunkInfo info,
      byte[] data)
      throws StorageContainerException {
    // we don't want container manager to go away while we are writing chunks.
    containerManager.readLock();

    // TODO : Take keyManager Write lock here.
    try {
      Preconditions.checkNotNull(pipeline, "Pipeline cannot be null");
      String containerName = pipeline.getContainerName();
      Preconditions.checkNotNull(containerName,
          "Container name cannot be null");
      ContainerData container =
          containerManager.readContainer(containerName);
      File chunkFile = ChunkUtils.validateChunk(pipeline, container, info);
      long oldSize = chunkFile.length();
      ChunkUtils.writeData(chunkFile, info, data);
      containerManager.incrWriteBytes(containerName, info.getLen());
      containerManager.incrWriteCount(containerName);
      long newSize = chunkFile.length();
      containerManager.incrBytesUsed(containerName, newSize - oldSize);
    } catch (ExecutionException | NoSuchAlgorithmException e) {
      LOG.error("write data failed. error: {}", e);
      throw new StorageContainerException("Internal error: ", e,
          CONTAINER_INTERNAL_ERROR);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("write data failed. error: {}", e);
      throw new StorageContainerException("Internal error: ", e,
          CONTAINER_INTERNAL_ERROR);
    } finally {
      containerManager.readUnlock();
    }
  }

  /**
   * reads the data defined by a chunk.
   *
   * @param pipeline - container pipeline.
   * @param keyName - Name of the Key
   * @param info - ChunkInfo.
   * @return byte array
   * @throws StorageContainerException
   * TODO: Right now we do not support partial reads and writes of chunks.
   * TODO: Explore if we need to do that for ozone.
   */
  @Override
  public byte[] readChunk(Pipeline pipeline, String keyName, ChunkInfo info)
      throws StorageContainerException {
    containerManager.readLock();
    try {
      Preconditions.checkNotNull(pipeline, "Pipeline cannot be null");
      String containerName = pipeline.getContainerName();
      Preconditions.checkNotNull(containerName,
          "Container name cannot be null");
      ContainerData container =
          containerManager.readContainer(containerName);
      File chunkFile = ChunkUtils.getChunkFile(pipeline, container, info);
      ByteBuffer data =  ChunkUtils.readData(chunkFile, info);
      containerManager.incrReadCount(containerName);
      containerManager.incrReadBytes(containerName, chunkFile.length());
      return data.array();
    } catch (ExecutionException | NoSuchAlgorithmException e) {
      LOG.error("read data failed. error: {}", e);
      throw new StorageContainerException("Internal error: ",
          e, CONTAINER_INTERNAL_ERROR);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("read data failed. error: {}", e);
      throw new StorageContainerException("Internal error: ",
          e, CONTAINER_INTERNAL_ERROR);
    } finally {
      containerManager.readUnlock();
    }
  }

  /**
   * Deletes a given chunk.
   *
   * @param pipeline - Pipeline.
   * @param keyName - Key Name
   * @param info - Chunk Info
   * @throws StorageContainerException
   */
  @Override
  public void deleteChunk(Pipeline pipeline, String keyName, ChunkInfo info)
      throws StorageContainerException {
    containerManager.readLock();
    try {
      Preconditions.checkNotNull(pipeline, "Pipeline cannot be null");
      String containerName = pipeline.getContainerName();
      Preconditions.checkNotNull(containerName,
          "Container name cannot be null");
      File chunkFile = ChunkUtils.getChunkFile(pipeline, containerManager
          .readContainer(containerName), info);
      if ((info.getOffset() == 0) && (info.getLen() == chunkFile.length())) {
        FileUtil.fullyDelete(chunkFile);
        containerManager.decrBytesUsed(containerName, chunkFile.length());
      } else {
        LOG.error("Not Supported Operation. Trying to delete a " +
            "chunk that is in shared file. chunk info : " + info.toString());
        throw new StorageContainerException("Not Supported Operation. " +
            "Trying to delete a chunk that is in shared file. chunk info : "
            + info.toString(), UNSUPPORTED_REQUEST);
      }
    } finally {
      containerManager.readUnlock();
    }
  }

  /**
   * Shutdown the chunkManager.
   *
   * In the chunkManager we haven't acquired any resources, so nothing to do
   * here. This call is made with containerManager Writelock held.
   */
  @Override
  public void shutdown() {
    Preconditions.checkState(this.containerManager.hasWriteLock());
  }
}
