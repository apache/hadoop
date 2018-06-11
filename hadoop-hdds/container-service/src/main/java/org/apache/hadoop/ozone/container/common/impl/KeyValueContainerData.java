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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;


import java.io.IOException;

/**
 * This class represents the KeyValueContainer metadata, which is the
 * in-memory representation of container metadata and is represented on disk
 * by the .container file.
 */
public class KeyValueContainerData extends ContainerData {

  // Path to Level DB/RocksDB Store.
  private String dbPath;

  // Path to Physical file system where container and checksum are stored.
  private String containerFilePath;

  //Type of DB used to store key to chunks mapping
  private String containerDBType;

  //Number of pending deletion blocks in container.
  private int numPendingDeletionBlocks;

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
   * Returns path.
   *
   * @return - path
   */
  public String getDbPath() {
    return dbPath;
  }

  /**
   * Sets path.
   *
   * @param path - String.
   */
  public void setDbPath(String path) {
    this.dbPath = path;
  }

  /**
   * Get container file path.
   * @return - Physical path where container file and checksum is stored.
   */
  public String getContainerFilePath() {
    return containerFilePath;
  }

  /**
   * Set container Path.
   * @param containerPath - File path.
   */
  public void setContainerFilePath(String containerPath) {
    this.containerFilePath = containerPath;
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
   * Constructs a KeyValueContainerData object from ProtoBuf classes.
   *
   * @param protoData - ProtoBuf Message
   * @throws IOException
   */
  public static KeyValueContainerData getFromProtoBuf(
      ContainerProtos.CreateContainerData protoData) throws IOException {

    long containerID;
    ContainerProtos.ContainerType containerType;

    containerID = protoData.getContainerId();
    containerType = protoData.getContainerType();

    KeyValueContainerData keyValueContainerData = new KeyValueContainerData(
        containerType, containerID);

    for (int x = 0; x < protoData.getMetadataCount(); x++) {
      keyValueContainerData.addMetadata(protoData.getMetadata(x).getKey(),
          protoData.getMetadata(x).getValue());
    }

    return keyValueContainerData;
  }

}
