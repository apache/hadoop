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
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces
    .ContainerLocationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * A class that tells the ContainerManager where to place the containers.
 * Please note : There is *no* one-to-one correlation between metadata
 * metadataLocations and data metadataLocations.
 *
 *  For example : A user could map all container files to a
 *  SSD but leave data/metadata on bunch of other disks.
 */
public class ContainerLocationManagerImpl implements ContainerLocationManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerLocationManagerImpl.class);

  private final List<StorageLocation> dataLocations;
  private int currentIndex;
  private final List<StorageLocation> metadataLocations;

  /**
   * Constructs a Location Manager.
   * @param metadataLocations  - Refers to the metadataLocations
   * where we store the container metadata.
   * @param dataDirs - metadataLocations where we store the actual
   * data or chunk files.
   * @throws IOException
   */
  public ContainerLocationManagerImpl(List<StorageLocation> metadataLocations,
      List<StorageLocation> dataDirs)
      throws IOException {
    dataLocations = dataDirs;
    this.metadataLocations = metadataLocations;
  }
  /**
   * Returns the path where the container should be placed from a set of
   * metadataLocations.
   *
   * @return A path where we should place this container and metadata.
   * @throws IOException
   */
  @Override
  public Path getContainerPath()
      throws IOException {
    Preconditions.checkState(metadataLocations.size() > 0);
    int index = currentIndex % metadataLocations.size();
    Path path = Paths.get(metadataLocations.get(index).getNormalizedUri());
    return path.resolve(OzoneConsts.CONTAINER_ROOT_PREFIX);
  }

  /**
   * Returns the path where the container Data file are stored.
   *
   * @return  a path where we place the LevelDB and data files of a container.
   * @throws IOException
   */
  @Override
  public Path getDataPath(String containerName) throws IOException {
    Path currentPath = Paths.get(
        dataLocations.get(currentIndex++ % dataLocations.size())
            .getNormalizedUri());
    currentPath = currentPath.resolve(OzoneConsts.CONTAINER_PREFIX);
    return currentPath.resolve(containerName);
  }
}
