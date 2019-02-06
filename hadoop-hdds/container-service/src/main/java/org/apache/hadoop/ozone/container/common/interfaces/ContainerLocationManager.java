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

import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Returns physical path locations, where the containers will be created.
 */
public interface ContainerLocationManager {
  /**
   * Returns the path where the container should be placed from a set of
   * locations.
   *
   * @return A path where we should place this container and metadata.
   * @throws IOException
   */
  Path getContainerPath() throws IOException;

  /**
   * Returns the path where the container Data file are stored.
   *
   * @return a path where we place the LevelDB and data files of a container.
   * @throws IOException
   */
  Path getDataPath(String containerName) throws IOException;

  /**
   * Returns an array of storage location usage report.
   * @return storage location usage report.
   */
  StorageLocationReport[] getLocationReport() throws IOException;

  /**
   * Supports clean shutdown of container.
   *
   * @throws IOException
   */
  void shutdown() throws IOException;
}
