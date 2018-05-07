/**
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
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ReportState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
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
   * @param datanodeDetails - DatanodeDetails
   * @throws StorageContainerException
   */
  void init(Configuration config, List<StorageLocation> containerDirs,
            DatanodeDetails datanodeDetails) throws IOException;

  /**
   * Creates a container with the given name.
   *
   * @param containerData - Container Name and metadata.
   * @throws StorageContainerException
   */
  void createContainer(ContainerData containerData)
      throws StorageContainerException;

  /**
   * Deletes an existing container.
   *
   * @param containerID - ID of the container.
   * @param forceDelete   - whether this container should be deleted forcibly.
   * @throws StorageContainerException
   */
  void deleteContainer(long containerID,
      boolean forceDelete) throws StorageContainerException;

  /**
   * Update an existing container.
   *
   * @param containerID ID of the container
   * @param data container data
   * @param forceUpdate if true, update container forcibly.
   * @throws StorageContainerException
   */
  void updateContainer(long containerID, ContainerData data,
      boolean forceUpdate) throws StorageContainerException;

  /**
   * As simple interface for container Iterations.
   *
   * @param startContainerID -  Return containers with ID >= startContainerID.
   * @param count - how many to return
   * @param data - Actual containerData
   * @throws StorageContainerException
   */
  void listContainer(long startContainerID, long count,
      List<ContainerData> data) throws StorageContainerException;

  /**
   * Choose containers for block deletion.
   *
   * @param count   - how many to return
   * @throws StorageContainerException
   */
  List<ContainerData> chooseContainerForBlockDeletion(int count)
      throws StorageContainerException;

  /**
   * Get metadata about a specific container.
   *
   * @param containerID - ID of the container.
   * @return ContainerData - Container Data.
   * @throws StorageContainerException
   */
  ContainerData readContainer(long containerID)
      throws StorageContainerException;

  /**
   * Closes a open container, if it is already closed or does not exist a
   * StorageContainerException is thrown.
   * @param containerID - ID of the container.
   * @throws StorageContainerException
   */
  void closeContainer(long containerID)
      throws StorageContainerException, NoSuchAlgorithmException;

  /**
   * Checks if a container exists.
   * @param containerID - ID of the container.
   * @return true if the container is open false otherwise.
   * @throws StorageContainerException  - Throws Exception if we are not
   * able to find the container.
   */
  boolean isOpen(long containerID) throws StorageContainerException;

  /**
   * Supports clean shutdown of container.
   *
   * @throws StorageContainerException
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

  /**
   * Get the Node Report of container storage usage.
   * @return node report.
   */
  SCMNodeReport getNodeReport() throws IOException;

  /**
   * Gets container report.
   * @return container report.
   * @throws IOException
   */
  ContainerReportsRequestProto getContainerReport() throws IOException;

  /**
   * Gets container reports.
   * @return List of all closed containers.
   * @throws IOException
   */
  List<ContainerData> getContainerReports() throws IOException;

  /**
   * Increase pending deletion blocks count number of specified container.
   *
   * @param numBlocks
   *          increment  count number
   * @param containerId
   *          container id
   */
  void incrPendingDeletionBlocks(int numBlocks, long containerId);

  /**
   * Decrease pending deletion blocks count number of specified container.
   *
   * @param numBlocks
   *          decrement count number
   * @param containerId
   *          container id
   */
  void decrPendingDeletionBlocks(int numBlocks, long containerId);

  /**
   * Increase the read count of the container.
   * @param containerId - ID of the container.
   */
  void incrReadCount(long containerId);

  /**
   * Increse the read counter for bytes read from the container.
   * @param containerId - ID of the container.
   * @param readBytes - bytes read from the container.
   */
  void incrReadBytes(long containerId, long readBytes);


  /**
   * Increase the write count of the container.
   * @param containerId - ID of the container.
   */
  void incrWriteCount(long containerId);

  /**
   * Increase the write counter for bytes write into the container.
   * @param containerId - ID of the container.
   * @param writeBytes - bytes write into the container.
   */
  void incrWriteBytes(long containerId, long writeBytes);

  /**
   * Increase the bytes used by the container.
   * @param containerId - ID of the container.
   * @param used - additional bytes used by the container.
   * @return the current bytes used.
   */
  long incrBytesUsed(long containerId, long used);

  /**
   * Decrease the bytes used by the container.
   * @param containerId - ID of the container.
   * @param used - additional bytes reclaimed by the container.
   * @return the current bytes used.
   */
  long decrBytesUsed(long containerId, long used);

  /**
   * Get the bytes used by the container.
   * @param containerId - ID of the container.
   * @return the current bytes used by the container.
   */
  long getBytesUsed(long containerId);

  /**
   * Get the number of keys in the container.
   * @param containerId - ID of the container.
   * @return the current key count.
   */
  long getNumKeys(long containerId);

  /**
   * Get the container report state to send via HB to SCM.
   * @return container report state.
   */
  ReportState getContainerReportState();
}
