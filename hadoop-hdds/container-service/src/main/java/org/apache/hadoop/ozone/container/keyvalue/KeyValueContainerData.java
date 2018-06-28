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

package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;


import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * This class represents the KeyValueContainer metadata, which is the
 * in-memory representation of container metadata and is represented on disk
 * by the .container file.
 */
public class KeyValueContainerData extends ContainerData {

  // Path to Container metadata Level DB/RocksDB Store and .container file.
  private String metadataPath;

  // Path to Physical file system where chunks are stored.
  private String chunksPath;

  //Type of DB used to store key to chunks mapping
  private String containerDBType;

  //Number of pending deletion blocks in container.
  private int numPendingDeletionBlocks;

  private File dbFile = null;

  /**
   * Constructs KeyValueContainerData object.
   * @param type - containerType
   * @param id - ContainerId
   */
  public KeyValueContainerData(ContainerProtos.ContainerType type, long id) {
    super(type, id);
    this.numPendingDeletionBlocks = 0;
  }

  /**
   * Constructs KeyValueContainerData object.
   * @param type - containerType
   * @param id - ContainerId
   * @param layOutVersion
   */
  public KeyValueContainerData(ContainerProtos.ContainerType type, long id,
                               int layOutVersion) {
    super(type, id, layOutVersion);
    this.numPendingDeletionBlocks = 0;
  }


  /**
   * Sets Container dbFile. This should be called only during creation of
   * KeyValue container.
   * @param containerDbFile
   */
  public void setDbFile(File containerDbFile) {
    dbFile = containerDbFile;
  }

  /**
   * Returns container DB file.
   * @return dbFile
   */
  public File getDbFile() {
    return dbFile;
  }
  /**
   * Returns container metadata path.
   *
   * @return - path
   */
  public String getMetadataPath() {
    return metadataPath;
  }

  /**
   * Sets container metadata path.
   *
   * @param path - String.
   */
  public void setMetadataPath(String path) {
    this.metadataPath = path;
  }

  /**
   * Get chunks path.
   * @return - Physical path where container file and checksum is stored.
   */
  public String getChunksPath() {
    return chunksPath;
  }

  /**
   * Set chunks Path.
   * @param chunkPath - File path.
   */
  public void setChunksPath(String chunkPath) {
    this.chunksPath = chunkPath;
  }

  /**
   * Returns the DBType used for the container.
   * @return containerDBType
   */
  public String getContainerDBType() {
    return containerDBType;
  }

  /**
   * Sets the DBType used for the container.
   * @param containerDBType
   */
  public void setContainerDBType(String containerDBType) {
    this.containerDBType = containerDBType;
  }

  /**
   * Returns the number of pending deletion blocks in container.
   * @return numPendingDeletionBlocks
   */
  public int getNumPendingDeletionBlocks() {
    return numPendingDeletionBlocks;
  }


  /**
   * Increase the count of pending deletion blocks.
   *
   * @param numBlocks increment number
   */
  public void incrPendingDeletionBlocks(int numBlocks) {
    this.numPendingDeletionBlocks += numBlocks;
  }

  /**
   * Decrease the count of pending deletion blocks.
   *
   * @param numBlocks decrement number
   */
  public void decrPendingDeletionBlocks(int numBlocks) {
    this.numPendingDeletionBlocks -= numBlocks;
  }

  /**
   * Returns a ProtoBuf Message from ContainerData.
   *
   * @return Protocol Buffer Message
   */
  public ContainerProtos.ContainerData getProtoBufMessage() {
    ContainerProtos.ContainerData.Builder builder = ContainerProtos
        .ContainerData.newBuilder();
    builder.setContainerID(this.getContainerId());
    builder.setDbPath(this.getDbFile().getPath());
    builder.setContainerPath(this.getMetadataPath());
    builder.setState(this.getState());

    for (Map.Entry<String, String> entry : getMetadata().entrySet()) {
      ContainerProtos.KeyValue.Builder keyValBuilder =
          ContainerProtos.KeyValue.newBuilder();
      builder.addMetadata(keyValBuilder.setKey(entry.getKey())
          .setValue(entry.getValue()).build());
    }

    if (this.getBytesUsed() >= 0) {
      builder.setBytesUsed(this.getBytesUsed());
    }

    if(this.getContainerType() != null) {
      builder.setContainerType(ContainerProtos.ContainerType.KeyValueContainer);
    }

    if(this.getContainerDBType() != null) {
      builder.setContainerDBType(containerDBType);
    }

    return builder.build();
  }
}
