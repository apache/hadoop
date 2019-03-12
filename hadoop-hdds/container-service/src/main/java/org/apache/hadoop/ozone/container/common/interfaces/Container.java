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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;

import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;

/**
 * Interface for Container Operations.
 */
public interface Container<CONTAINERDATA extends ContainerData> extends RwLock {

  /**
   * Creates a container.
   *
   * @throws StorageContainerException
   */
  void create(VolumeSet volumeSet, VolumeChoosingPolicy volumeChoosingPolicy,
              String scmId) throws StorageContainerException;

  /**
   * Deletes the container.
   *
   * @throws StorageContainerException
   */
  void delete() throws StorageContainerException;

  /**
   * Update the container.
   *
   * @param metaData
   * @param forceUpdate if true, update container forcibly.
   * @throws StorageContainerException
   */
  void update(Map<String, String> metaData, boolean forceUpdate)
      throws StorageContainerException;

  /**
   * Get metadata about the container.
   *
   * @return ContainerData - Container Data.
   */
  CONTAINERDATA getContainerData();

  /**
   * Get the Container Lifecycle state.
   *
   * @return ContainerLifeCycleState - Container State.
   */
  ContainerProtos.ContainerDataProto.State getContainerState();

  /**
   * Marks the container for closing. Moves the container to CLOSING state.
   */
  void markContainerForClose() throws StorageContainerException;

  /**
   * Marks the container replica as unhealthy.
   */
  void markContainerUnhealthy() throws StorageContainerException;

  /**
   * Quasi Closes a open container, if it is already closed or does not exist a
   * StorageContainerException is thrown.
   *
   * @throws StorageContainerException
   */
  void quasiClose() throws StorageContainerException;

  /**
   * Closes a open/quasi closed container, if it is already closed or does not
   * exist a StorageContainerException is thrown.
   *
   * @throws StorageContainerException
   */
  void close() throws StorageContainerException;

  /**
   * Return the ContainerType for the container.
   */
  ContainerProtos.ContainerType getContainerType();

  /**
   * Returns containerFile.
   */
  File getContainerFile();

  /**
   * updates the DeleteTransactionId.
   * @param deleteTransactionId
   */
  void updateDeleteTransactionId(long deleteTransactionId);

  /**
   * Returns blockIterator for the container.
   * @return BlockIterator
   * @throws IOException
   */
  BlockIterator blockIterator() throws IOException;

  /**
   * Import the container from an external archive.
   */
  void importContainerData(InputStream stream,
      ContainerPacker<CONTAINERDATA> packer) throws IOException;

  /**
   * Export all the data of the container to one output archive with the help
   * of the packer.
   *
   */
  void exportContainerData(OutputStream stream,
      ContainerPacker<CONTAINERDATA> packer) throws IOException;

  /**
   * Returns containerReport for the container.
   */
  ContainerReplicaProto getContainerReport()
      throws StorageContainerException;

  /**
   * updates the blockCommitSequenceId.
   */
  void updateBlockCommitSequenceId(long blockCommitSequenceId);

  /**
   * check and report the structural integrity of the container.
   */
  void check() throws StorageContainerException;
}
