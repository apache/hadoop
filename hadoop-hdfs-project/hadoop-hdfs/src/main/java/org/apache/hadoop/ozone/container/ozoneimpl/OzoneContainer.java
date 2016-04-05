/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.common.impl.ContainerManagerImpl;
import org.apache.hadoop.ozone.container.common.impl.Dispatcher;
import org.apache.hadoop.ozone.container.common.impl.KeyManagerImpl;
import org.apache.hadoop.ozone.container.common.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.interfaces.KeyManager;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.nio.file.Path;

/**
 * Ozone main class sets up the network server and initializes the container
 * layer.
 */
public class OzoneContainer {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneContainer.class);

  private final Configuration ozoneConfig;
  private final FsDatasetSpi dataSet;
  private final ContainerDispatcher dispatcher;
  private final ContainerManager manager;
  private final XceiverServer server;
  private final ChunkManager chunkManager;
  private final KeyManager keyManager;

  /**
   * Creates a network endpoint and enables Ozone container.
   *
   * @param ozoneConfig - Config
   * @param dataSet     - FsDataset.
   * @throws IOException
   */
  public OzoneContainer(Configuration ozoneConfig, FsDatasetSpi dataSet) throws
      Exception {
    List<Path> locations = new LinkedList<>();
    String[] paths = ozoneConfig.getStrings(OzoneConfigKeys
        .DFS_OZONE_METADATA_DIRS);
    if (paths != null && paths.length > 0) {
      for (String p : paths) {
        locations.add(Paths.get(p));
      }
    } else {
      getDataDir(dataSet, locations);
    }

    this.ozoneConfig = ozoneConfig;
    this.dataSet = dataSet;

    manager = new ContainerManagerImpl();
    manager.init(this.ozoneConfig, locations, this.dataSet);
    this.chunkManager = new ChunkManagerImpl(manager);
    manager.setChunkManager(this.chunkManager);

    this.keyManager = new KeyManagerImpl(manager, ozoneConfig);
    manager.setKeyManager(this.keyManager);

    this.dispatcher = new Dispatcher(manager);
    server = new XceiverServer(this.ozoneConfig, this.dispatcher);
  }

  /**
   * Starts serving requests to ozone container.
   * @throws Exception
   */
  public void start() throws Exception {
    server.start();
  }

  /**
   * Stops the ozone container.
   * @throws Exception
   */
  public void stop() throws  Exception {
    server.stop();
  }

  /**
   * Returns a paths to data dirs.
   * @param dataset - FSDataset.
   * @param pathList - List of paths.
   * @throws IOException
   */
  private void getDataDir(FsDatasetSpi dataset, List<Path> pathList) throws
      IOException {
    FsDatasetSpi.FsVolumeReferences references;
    try {
      synchronized (dataset) {
        references = dataset.getFsVolumeReferences();
        for (int ndx = 0; ndx < references.size(); ndx++) {
          FsVolumeSpi vol = references.get(ndx);
          pathList.add(Paths.get(vol.getBasePath()));
        }
        references.close();
      }
    } catch (IOException ex) {
      LOG.error("Unable to get volume paths.", ex);
      throw new IOException("Internal error", ex);
    }
  }
}
