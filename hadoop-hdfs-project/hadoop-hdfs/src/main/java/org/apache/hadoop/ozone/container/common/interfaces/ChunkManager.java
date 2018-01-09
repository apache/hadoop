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

package org.apache.hadoop.ozone.container.common.interfaces;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

/**
 * Chunk Manager allows read, write, delete and listing of chunks in
 * a container.
 */
public interface ChunkManager {

  /**
   * writes a given chunk.
   * @param pipeline - Name and the set of machines that make this container.
   * @param keyName - Name of the Key.
   * @param info - ChunkInfo.
   * @param stage - Chunk Stage write.
   * @throws StorageContainerException
   */
  void writeChunk(Pipeline pipeline, String keyName,
                  ChunkInfo info, byte[] data, ContainerProtos.Stage stage)
      throws StorageContainerException;

  /**
   * reads the data defined by a chunk.
   * @param pipeline - container pipeline.
   * @param keyName - Name of the Key
   * @param info - ChunkInfo.
   * @return  byte array
   * @throws StorageContainerException
   *
   * TODO: Right now we do not support partial reads and writes of chunks.
   * TODO: Explore if we need to do that for ozone.
   */
  byte[] readChunk(Pipeline pipeline, String keyName, ChunkInfo info) throws
      StorageContainerException;

  /**
   * Deletes a given chunk.
   * @param pipeline  - Pipeline.
   * @param keyName   - Key Name
   * @param info  - Chunk Info
   * @throws StorageContainerException
   */
  void deleteChunk(Pipeline pipeline, String keyName, ChunkInfo info) throws
      StorageContainerException;

  // TODO : Support list operations.

  /**
   * Shutdown the chunkManager.
   */
  void shutdown();

}
