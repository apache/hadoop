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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerLocationManager;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

/**
 * A class that tells the ContainerManager where to place the containers.
 * Please note : There is *no* one-to-one correlation between metadata
 * locations and data locations.
 *
 *  For example : A user could map all container files to a
 *  SSD but leave data/metadata on bunch of other disks.
 */
public class ContainerLocationManagerImpl implements ContainerLocationManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerLocationManagerImpl.class);


  private final Configuration conf;
  private final FsDatasetSpi dataset;
  private final Path[] volumePaths;
  private int currentIndex;
  private final List<Path> locations;


  /**
   * Constructs a Location Manager.
   * @param conf - Configuration.
   */
  public ContainerLocationManagerImpl(Configuration conf, List<Path> locations,
                                      FsDatasetSpi dataset) throws IOException {
    this.conf = conf;
    this.dataset = dataset;
    List<Path> pathList = new LinkedList<>();
    FsDatasetSpi.FsVolumeReferences references;
    try {
      synchronized (this.dataset) {
        references = this.dataset.getFsVolumeReferences();
        for (int ndx = 0; ndx < references.size(); ndx++) {
          FsVolumeSpi vol = references.get(ndx);
          pathList.add(Paths.get(vol.getBasePath()));
        }
        references.close();
        volumePaths = pathList.toArray(new Path[pathList.size()]);
        this.locations = locations;
      }
    } catch (IOException ex) {
      LOG.error("Unable to get volume paths.", ex);
      throw new IOException("Internal error", ex);
    }

  }

  /**
   * Returns the path where the container should be placed from a set of
   * locations.
   *
   * @return A path where we should place this container and metadata.
   * @throws IOException
   */
  @Override
  public Path getContainerPath()
      throws IOException {
    Preconditions.checkState(locations.size() > 0);
    int index = currentIndex % locations.size();
    return locations.get(index).resolve(OzoneConsts.CONTAINER_ROOT_PREFIX);
  }

  /**
   * Returns the path where the container Data file are stored.
   *
   * @return  a path where we place the LevelDB and data files of a container.
   * @throws IOException
   */
  @Override
  public Path getDataPath(String containerName) throws IOException {
    Path currentPath = volumePaths[currentIndex++ % volumePaths.length];
    currentPath = currentPath.resolve(OzoneConsts.CONTAINER_PREFIX);
    return currentPath.resolve(containerName);
  }
}
