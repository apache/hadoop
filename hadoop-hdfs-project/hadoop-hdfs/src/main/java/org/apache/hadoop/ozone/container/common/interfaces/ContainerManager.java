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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Interface for container operations.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ContainerManager extends RwLock {

  /**
   * Init call that sets up a container Manager.
   *
   * @param config        - Configuration.
   * @param containerDirs - List of Metadata Container locations.
   * @param dataset       - FSDataset.
   * @throws IOException
   */
  void init(Configuration config, List<Path> containerDirs,
            FsDatasetSpi dataset)
      throws IOException;

  /**
   * Creates a container with the given name.
   *
   * @param pipeline      -- Nodes which make up this container.
   * @param containerData - Container Name and metadata.
   * @throws IOException
   */
  void createContainer(Pipeline pipeline, ContainerData containerData)
      throws IOException;

  /**
   * Deletes an existing container.
   *
   * @param pipeline      - nodes that make this container.
   * @param containerName - name of the container.
   * @throws IOException
   */
  void deleteContainer(Pipeline pipeline, String containerName)
      throws IOException;

  /**
   * As simple interface for container Iterations.
   *
   * @param prevKey - Starting KeyValue
   * @param count   - how many to return
   * @param data    - Actual containerData
   * @throws IOException
   */
  void listContainer(String prevKey, long count, List<ContainerData> data)
      throws IOException;

  /**
   * Get metadata about a specific container.
   *
   * @param containerName - Name of the container
   * @return ContainerData - Container Data.
   * @throws IOException
   */
  ContainerData readContainer(String containerName) throws IOException;

  /**
   * Supports clean shutdown of container.
   *
   * @throws IOException
   */
  void shutdown() throws IOException;

  /**
   * Sets the Chunk Manager.
   *
   * @param chunkManager - ChunkManager.
   */
  void setChunkManager(ChunkManager chunkManager);

  /**
   * Gets the Chunk Manager.
   *
   * @return ChunkManager.
   */
  ChunkManager getChunkManager();

  /**
   * Sets the Key Manager.
   *
   * @param keyManager - Key Manager.
   */
  void setKeyManager(KeyManager keyManager);

  /**
   * Gets the Key Manager.
   *
   * @return KeyManager.
   */
  KeyManager getKeyManager();

}
