package org.apache.hadoop.ozone.container.keyvalue.interfaces;

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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;

import java.nio.ByteBuffer;

/**
 * Chunk Manager allows read, write, delete and listing of chunks in
 * a container.
 */

public interface ChunkManager {

  /**
   * writes a given chunk.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block.
   * @param info - ChunkInfo.
   * @param dispatcherContext - dispatcher context info.
   * @throws StorageContainerException
   */
  void writeChunk(Container container, BlockID blockID, ChunkInfo info,
      ByteBuffer data, DispatcherContext dispatcherContext)
      throws StorageContainerException;

  /**
   * reads the data defined by a chunk.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block.
   * @param info - ChunkInfo.
   * @param dispatcherContext - dispatcher context info.
   * @return  byte array
   * @throws StorageContainerException
   *
   * TODO: Right now we do not support partial reads and writes of chunks.
   * TODO: Explore if we need to do that for ozone.
   */
  byte[] readChunk(Container container, BlockID blockID, ChunkInfo info,
      DispatcherContext dispatcherContext) throws StorageContainerException;

  /**
   * Deletes a given chunk.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block.
   * @param info  - Chunk Info
   * @throws StorageContainerException
   */
  void deleteChunk(Container container, BlockID blockID, ChunkInfo info) throws
      StorageContainerException;

  // TODO : Support list operations.

  /**
   * Shutdown the chunkManager.
   */
  void shutdown();

}
