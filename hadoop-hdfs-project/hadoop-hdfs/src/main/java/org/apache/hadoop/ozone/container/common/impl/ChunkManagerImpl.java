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
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.common.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

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
   * @param keyName  - Name of the Key.
   * @param info     - ChunkInfo.
   * @throws IOException
   */
  @Override
  public void writeChunk(Pipeline pipeline, String keyName, ChunkInfo info,
                         byte[] data)
      throws IOException {

    // we don't want container manager to go away while we are writing chunks.
    containerManager.readLock();

    // TODO : Take keyManager Write lock here.
    try {
      Preconditions.checkNotNull(pipeline);
      Preconditions.checkNotNull(pipeline.getContainerName());
      File chunkFile = ChunkUtils.validateChunk(pipeline,
          containerManager.readContainer(pipeline.getContainerName()), info);
      ChunkUtils.writeData(chunkFile, info, data);

    } catch (ExecutionException |
        NoSuchAlgorithmException e) {
      LOG.error("write data failed. error: {}", e);
      throw new IOException("Internal error: ", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("write data failed. error: {}", e);
      throw new IOException("Internal error: ", e);
    } finally {
      containerManager.readUnlock();
    }
  }

  /**
   * reads the data defined by a chunk.
   *
   * @param pipeline - container pipeline.
   * @param keyName  - Name of the Key
   * @param info     - ChunkInfo.
   * @return byte array
   * @throws IOException TODO: Right now we do not support partial reads and
   *                     writes of chunks. TODO: Explore if we need to do that
   *                     for ozone.
   */
  @Override
  public byte[] readChunk(Pipeline pipeline, String keyName, ChunkInfo info)
      throws IOException {
    containerManager.readLock();
    try {
      File chunkFile = ChunkUtils.getChunkFile(pipeline, containerManager
          .readContainer(pipeline.getContainerName()), info);
      return ChunkUtils.readData(chunkFile, info).array();
    } catch (ExecutionException | NoSuchAlgorithmException e) {
      LOG.error("read data failed. error: {}", e);
      throw new IOException("Internal error: ", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("read data failed. error: {}", e);
      throw new IOException("Internal error: ", e);
    } finally {
      containerManager.readUnlock();
    }
  }

  /**
   * Deletes a given chunk.
   *
   * @param pipeline - Pipeline.
   * @param keyName  - Key Name
   * @param info     - Chunk Info
   * @throws IOException
   */
  @Override
  public void deleteChunk(Pipeline pipeline, String keyName, ChunkInfo info)
      throws IOException {

    containerManager.readLock();
    try {
      File chunkFile = ChunkUtils.getChunkFile(pipeline, containerManager
          .readContainer(pipeline.getContainerName()), info);
      if ((info.getOffset() == 0) && (info.getLen() == chunkFile.length())) {
        FileUtil.fullyDelete(chunkFile);
      } else {
        LOG.error("Not Supported Operation. Trying to delete a " +
            "chunk that is in shared file. chunk info : " + info.toString());
        throw new IOException("Not Supported Operation. Trying to delete a " +
            "chunk that is in shared file. chunk info : " + info.toString());
      }
    } finally {
      containerManager.readUnlock();
    }
  }
}
