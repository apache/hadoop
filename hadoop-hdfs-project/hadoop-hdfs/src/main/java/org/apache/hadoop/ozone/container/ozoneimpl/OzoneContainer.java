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
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
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
import java.util.LinkedList;
import java.util.List;

/**
 * Ozone main class sets up the network server and initializes the container
 * layer.
 */
public class OzoneContainer {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneContainer.class);

  private final Configuration ozoneConfig;
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
  public OzoneContainer(
      Configuration ozoneConfig,
      FsDatasetSpi<? extends FsVolumeSpi> dataSet) throws Exception {
    List<StorageLocation> locations = new LinkedList<>();
    String[] paths = ozoneConfig.getStrings(
        OzoneConfigKeys.OZONE_CONTAINER_METADATA_DIRS);
    if (paths != null && paths.length > 0) {
      for (String p : paths) {
        locations.add(StorageLocation.parse(p));
      }
    } else {
      getDataDir(dataSet, locations);
    }

    this.ozoneConfig = ozoneConfig;

    manager = new ContainerManagerImpl();
    manager.init(this.ozoneConfig, locations);
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
   *
   * Shutdown logic is not very obvious from the following code.
   * if you need to  modify the logic, please keep these comments in mind.
   * Here is the shutdown sequence.
   *
   * 1. We shutdown the network ports.
   *
   * 2. Now we need to wait for all requests in-flight to finish.
   *
   * 3. The container manager lock is a read-write lock with "Fairness" enabled.
   *
   * 4. This means that the waiting threads are served in a "first-come-first
   * -served" manner. Please note that this applies to waiting threads only.
   *
   * 5. Since write locks are exclusive, if we are waiting to get a lock it
   * implies that we are waiting for in-flight operations to complete.
   *
   * 6. if there are other write operations waiting on the reader-writer lock,
   * fairness guarantees that they will proceed before the shutdown lock
   * request.
   *
   * 7. Since all operations either take a reader or writer lock of container
   * manager, we are guaranteed that we are the last operation since we have
   * closed the network port, and we wait until close is successful.
   *
   * 8. We take the writer lock and call shutdown on each of the managers in
   * reverse order. That is chunkManager, keyManager and containerManager is
   * shutdown.
   *
   */
  public void stop() {
    LOG.info("Attempting to stop container services.");
    server.stop();
    try {
      this.manager.writeLock();
      this.chunkManager.shutdown();
      this.keyManager.shutdown();
      this.manager.shutdown();
      LOG.info("container services shutdown complete.");
    } finally {
      this.manager.writeUnlock();
    }
  }

  /**
   * Returns a paths to data dirs.
   * @param dataset - FSDataset.
   * @param pathList - List of paths.
   * @throws IOException
   */
  private void getDataDir(
      FsDatasetSpi<? extends FsVolumeSpi> dataset,
      List<StorageLocation> pathList) throws IOException {
    FsDatasetSpi.FsVolumeReferences references;
    try {
      synchronized (dataset) {
        references = dataset.getFsVolumeReferences();
        for (int ndx = 0; ndx < references.size(); ndx++) {
          FsVolumeSpi vol = references.get(ndx);
          pathList.add(StorageLocation.parse(vol.getBaseURI().getPath()));
        }
        references.close();
      }
    } catch (IOException ex) {
      LOG.error("Unable to get volume paths.", ex);
      throw new IOException("Internal error", ex);
    }
  }
}
