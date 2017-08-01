/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.common.impl.ContainerManagerImpl;
import org.apache.hadoop.ozone.container.common.impl.Dispatcher;
import org.apache.hadoop.ozone.container.common.impl.KeyManagerImpl;
import org.apache.hadoop.ozone.container.common.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.interfaces.KeyManager;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServer;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_ROOT_PREFIX;

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
  private final XceiverServerSpi server;
  private final ChunkManager chunkManager;
  private final KeyManager keyManager;

  /**
   * Creates a network endpoint and enables Ozone container.
   *
   * @param ozoneConfig - Config
   * @throws IOException
   */
  public OzoneContainer(
      Configuration ozoneConfig) throws IOException {
    this.ozoneConfig = ozoneConfig;
    List<StorageLocation> locations = new LinkedList<>();
    String[] paths = ozoneConfig.getStrings(
        OzoneConfigKeys.OZONE_CONTAINER_METADATA_DIRS);
    if (paths != null && paths.length > 0) {
      for (String p : paths) {
        locations.add(StorageLocation.parse(
            Paths.get(p).resolve(CONTAINER_ROOT_PREFIX).toString()));
      }
    } else {
      getDataDir(locations);
    }

    manager = new ContainerManagerImpl();
    manager.init(this.ozoneConfig, locations);
    this.chunkManager = new ChunkManagerImpl(manager);
    manager.setChunkManager(this.chunkManager);

    this.keyManager = new KeyManagerImpl(manager, ozoneConfig);
    manager.setKeyManager(this.keyManager);

    this.dispatcher = new Dispatcher(manager, this.ozoneConfig);

    final boolean useRatis = ozoneConfig.getBoolean(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    server = useRatis?
        XceiverServerRatis.newXceiverServerRatis(ozoneConfig, dispatcher)
        : new XceiverServer(this.ozoneConfig, this.dispatcher);
  }

  /**
   * Starts serving requests to ozone container.
   *
   * @throws IOException
   */
  public void start() throws IOException {
    server.start();
    dispatcher.init();
  }

  /**
   * Stops the ozone container.
   * <p>
   * Shutdown logic is not very obvious from the following code. if you need to
   * modify the logic, please keep these comments in mind. Here is the shutdown
   * sequence.
   * <p>
   * 1. We shutdown the network ports.
   * <p>
   * 2. Now we need to wait for all requests in-flight to finish.
   * <p>
   * 3. The container manager lock is a read-write lock with "Fairness"
   * enabled.
   * <p>
   * 4. This means that the waiting threads are served in a "first-come-first
   * -served" manner. Please note that this applies to waiting threads only.
   * <p>
   * 5. Since write locks are exclusive, if we are waiting to get a lock it
   * implies that we are waiting for in-flight operations to complete.
   * <p>
   * 6. if there are other write operations waiting on the reader-writer lock,
   * fairness guarantees that they will proceed before the shutdown lock
   * request.
   * <p>
   * 7. Since all operations either take a reader or writer lock of container
   * manager, we are guaranteed that we are the last operation since we have
   * closed the network port, and we wait until close is successful.
   * <p>
   * 8. We take the writer lock and call shutdown on each of the managers in
   * reverse order. That is chunkManager, keyManager and containerManager is
   * shutdown.
   */
  public void stop() {
    LOG.info("Attempting to stop container services.");
    server.stop();
    dispatcher.shutdown();

    try {
      this.manager.writeLock();
      this.chunkManager.shutdown();
      this.keyManager.shutdown();
      this.manager.shutdown();
      LOG.info("container services shutdown complete.");
    } catch (IOException ex) {
      LOG.warn("container service shutdown error:", ex);
    } finally {
      this.manager.writeUnlock();
    }
  }

  /**
   * Returns a paths to data dirs.
   *
   * @param pathList - List of paths.
   * @throws IOException
   */
  private void getDataDir(List<StorageLocation> pathList) throws IOException {
    for (String dir : ozoneConfig.getStrings(DFS_DATANODE_DATA_DIR_KEY)) {
      StorageLocation location = StorageLocation.parse(dir);
      pathList.add(location);
    }
  }

  /**
   * Returns node report of container storage usage.
   */
  public SCMNodeReport getNodeReport() throws IOException {
    return this.manager.getNodeReport();
  }

  /**
   * Returns the container server IPC port.
   *
   * @return Container server IPC port.
   */
  public int getContainerServerPort() {
    return server.getIPCPort();
  }

  /**
   * Returns the list of closed containers.
   * @return - List of closed containers.
   * @throws IOException
   */
  public List<ContainerData> getContainerReports() throws IOException {
    return this.manager.getContainerReports();
  }

  @VisibleForTesting
  public ContainerManager getContainerManager() {
    return this.manager;
  }
}
