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

package org.apache.hadoop.ozone.container.interfaces;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ozone.container.helpers.ContainerData;
import org.apache.hadoop.ozone.container.helpers.Pipeline;

import java.io.IOException;
import java.util.List;

/**
 * Interface for container operations.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ContainerManager {

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
   * @param start - Starting index
   * @param count - how many to return
   * @param data  - Actual containerData
   * @throws IOException
   */
  void listContainer(long start, long count, List<ContainerData> data)
      throws IOException;

  /**
   * Get metadata about a specific container.
   *
   * @param containerName - Name of the container
   * @return ContainerData
   * @throws IOException
   */
  ContainerData readContainer(String containerName) throws IOException;
}
