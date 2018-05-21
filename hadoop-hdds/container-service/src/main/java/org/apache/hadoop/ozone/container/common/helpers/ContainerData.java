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

package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerLifeCycleState;
import org.apache.hadoop.ozone.OzoneConsts;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class maintains the information about a container in the ozone world.
 * <p>
 * A container is a name, along with metadata- which is a set of key value
 * pair.
 */
public class ContainerData {

  private final Map<String, String> metadata;
  private String dbPath;  // Path to Level DB Store.
  // Path to Physical file system where container and checksum are stored.
  private String containerFilePath;
  private AtomicLong bytesUsed;
  private long maxSize;
  private long containerID;
  private ContainerLifeCycleState state;
  private ContainerType containerType;
  private String containerDBType;


  /**
   * Number of pending deletion blocks in container.
   */
  private int numPendingDeletionBlocks;
  private AtomicLong readBytes;
  private AtomicLong writeBytes;
  private AtomicLong readCount;
  private AtomicLong writeCount;


  /**
   * Constructs a  ContainerData Object.
   *
   * @param containerID - ID
   * @param conf - Configuration
   */
  public ContainerData(long containerID,
      Configuration conf) {
    this.metadata = new TreeMap<>();
    this.maxSize = conf.getLong(ScmConfigKeys.SCM_CONTAINER_CLIENT_MAX_SIZE_KEY,
        ScmConfigKeys.SCM_CONTAINER_CLIENT_MAX_SIZE_DEFAULT) * OzoneConsts.GB;
    this.bytesUsed =  new AtomicLong(0L);
    this.containerID = containerID;
    this.state = ContainerLifeCycleState.OPEN;
    this.numPendingDeletionBlocks = 0;
    this.readCount = new AtomicLong(0L);
    this.readBytes =  new AtomicLong(0L);
    this.writeCount =  new AtomicLong(0L);
    this.writeBytes =  new AtomicLong(0L);
  }

  /**
   * Constructs a  ContainerData Object.
   *
   * @param containerID - ID
   * @param conf - Configuration
   * @param state - ContainerLifeCycleState
   * @param
   */
  public ContainerData(long containerID, Configuration conf,
                       ContainerLifeCycleState state) {
    this.metadata = new TreeMap<>();
    this.maxSize = conf.getLong(ScmConfigKeys.SCM_CONTAINER_CLIENT_MAX_SIZE_KEY,
        ScmConfigKeys.SCM_CONTAINER_CLIENT_MAX_SIZE_DEFAULT) * OzoneConsts.GB;
    this.bytesUsed =  new AtomicLong(0L);
    this.containerID = containerID;
    this.state = state;
    this.numPendingDeletionBlocks = 0;
    this.readCount = new AtomicLong(0L);
    this.readBytes =  new AtomicLong(0L);
    this.writeCount =  new AtomicLong(0L);
    this.writeBytes =  new AtomicLong(0L);
  }

  /**
   * Constructs a ContainerData object from ProtoBuf classes.
   *
   * @param protoData - ProtoBuf Message
   * @throws IOException
   */
  public static ContainerData getFromProtBuf(
      ContainerProtos.ContainerData protoData, Configuration conf)
      throws IOException {
    ContainerData data = new ContainerData(
        protoData.getContainerID(), conf);
    for (int x = 0; x < protoData.getMetadataCount(); x++) {
      data.addMetadata(protoData.getMetadata(x).getKey(),
          protoData.getMetadata(x).getValue());
    }

    if (protoData.hasContainerPath()) {
      data.setContainerPath(protoData.getContainerPath());
    }

    if (protoData.hasDbPath()) {
      data.setDBPath(protoData.getDbPath());
    }

    if (protoData.hasState()) {
      data.setState(protoData.getState());
    }

    if (protoData.hasBytesUsed()) {
      data.setBytesUsed(protoData.getBytesUsed());
    }

    if (protoData.hasSize()) {
      data.setMaxSize(protoData.getSize());
    }

    if(protoData.hasContainerType()) {
      data.setContainerType(protoData.getContainerType());
    }

    if(protoData.hasContainerDBType()) {
      data.setContainerDBType(protoData.getContainerDBType());
    }

    return data;
  }

  public String getContainerDBType() {
    return containerDBType;
  }

  public void setContainerDBType(String containerDBType) {
    this.containerDBType = containerDBType;
  }

  /**
   * Returns a ProtoBuf Message from ContainerData.
   *
   * @return Protocol Buffer Message
   */
  public ContainerProtos.ContainerData getProtoBufMessage() {
    ContainerProtos.ContainerData.Builder builder = ContainerProtos
        .ContainerData.newBuilder();
    builder.setContainerID(this.getContainerID());

    if (this.getDBPath() != null) {
      builder.setDbPath(this.getDBPath());
    }

    if (this.getContainerPath() != null) {
      builder.setContainerPath(this.getContainerPath());
    }

    builder.setState(this.getState());

    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      ContainerProtos.KeyValue.Builder keyValBuilder =
          ContainerProtos.KeyValue.newBuilder();
      builder.addMetadata(keyValBuilder.setKey(entry.getKey())
          .setValue(entry.getValue()).build());
    }

    if (this.getBytesUsed() >= 0) {
      builder.setBytesUsed(this.getBytesUsed());
    }

    if (this.getKeyCount() >= 0) {
      builder.setKeyCount(this.getKeyCount());
    }

    if (this.getMaxSize() >= 0) {
      builder.setSize(this.getMaxSize());
    }

    if(this.getContainerType() != null) {
      builder.setContainerType(containerType);
    }

    if(this.getContainerDBType() != null) {
      builder.setContainerDBType(containerDBType);
    }

    return builder.build();
  }

  public void setContainerType(ContainerType containerType) {
    this.containerType = containerType;
  }

  public ContainerType getContainerType() {
    return this.containerType;
  }
  /**
   * Adds metadata.
   */
  public void addMetadata(String key, String value) throws IOException {
    synchronized (this.metadata) {
      if (this.metadata.containsKey(key)) {
        throw new IOException("This key already exists. Key " + key);
      }
      metadata.put(key, value);
    }
  }

  /**
   * Returns all metadata.
   */
  public Map<String, String> getAllMetadata() {
    synchronized (this.metadata) {
      return Collections.unmodifiableMap(this.metadata);
    }
  }

  /**
   * Returns value of a key.
   */
  public String getValue(String key) {
    synchronized (this.metadata) {
      return metadata.get(key);
    }
  }

  /**
   * Deletes a metadata entry from the map.
   *
   * @param key - Key
   */
  public void deleteKey(String key) {
    synchronized (this.metadata) {
      metadata.remove(key);
    }
  }

  /**
   * Returns path.
   *
   * @return - path
   */
  public String getDBPath() {
    return dbPath;
  }

  /**
   * Sets path.
   *
   * @param path - String.
   */
  public void setDBPath(String path) {
    this.dbPath = path;
  }

  /**
   * This function serves as the generic key for ContainerCache class. Both
   * ContainerData and ContainerKeyData overrides this function to appropriately
   * return the right name that can  be used in ContainerCache.
   *
   * @return String Name.
   */
    // TODO: check the ContainerCache class to see if we are using the ContainerID instead.
   /*
   public String getName() {
    return getContainerID();
  }*/

  /**
   * Get container file path.
   * @return - Physical path where container file and checksum is stored.
   */
  public String getContainerPath() {
    return containerFilePath;
  }

  /**
   * Set container Path.
   * @param containerPath - File path.
   */
  public void setContainerPath(String containerPath) {
    this.containerFilePath = containerPath;
  }

  /**
   * Get container ID.
   * @return - container ID.
   */
  public synchronized long getContainerID() {
    return containerID;
  }

  public synchronized void setState(ContainerLifeCycleState state) {
    this.state = state;
  }

  public synchronized ContainerLifeCycleState getState() {
    return this.state;
  }

  /**
   * checks if the container is open.
   * @return - boolean
   */
  public synchronized  boolean isOpen() {
    return ContainerLifeCycleState.OPEN == state;
  }

  /**
   * checks if the container is invalid.
   * @return - boolean
   */
  public boolean isValid() {
    return !(ContainerLifeCycleState.INVALID == state);
  }

  /**
   * Marks this container as closed.
   */
  public synchronized void closeContainer() {
    // TODO: closed or closing here
    setState(ContainerLifeCycleState.CLOSED);

  }

  public void setMaxSize(long maxSize) {
    this.maxSize = maxSize;
  }

  public long getMaxSize() {
    return maxSize;
  }

  public long getKeyCount() {
    return metadata.size();
  }

  public void setBytesUsed(long used) {
    this.bytesUsed.set(used);
  }

  /**
   * Get the number of bytes used by the container.
   * @return the number of bytes used by the container.
   */
  public long getBytesUsed() {
    return bytesUsed.get();
  }

  /**
   * Increase the number of bytes used by the container.
   * @param used number of bytes used by the container.
   * @return the current number of bytes used by the container afert increase.
   */
  public long incrBytesUsed(long used) {
    return this.bytesUsed.addAndGet(used);
  }


  /**
   * Decrease the number of bytes used by the container.
   * @param reclaimed the number of bytes reclaimed from the container.
   * @return the current number of bytes used by the container after decrease.
   */
  public long decrBytesUsed(long reclaimed) {
    return this.bytesUsed.addAndGet(-1L * reclaimed);
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
   * Get the number of pending deletion blocks.
   */
  public int getNumPendingDeletionBlocks() {
    return this.numPendingDeletionBlocks;
  }

  /**
   * Get the number of bytes read from the container.
   * @return the number of bytes read from the container.
   */
  public long getReadBytes() {
    return readBytes.get();
  }

  /**
   * Increase the number of bytes read from the container.
   * @param bytes number of bytes read.
   */
  public void incrReadBytes(long bytes) {
    this.readBytes.addAndGet(bytes);
  }

  /**
   * Get the number of times the container is read.
   * @return the number of times the container is read.
   */
  public long getReadCount() {
    return readCount.get();
  }

  /**
   * Increase the number of container read count by 1.
   */
  public void incrReadCount() {
    this.readCount.incrementAndGet();
  }

  /**
   * Get the number of bytes write into the container.
   * @return the number of bytes write into the container.
   */
  public long getWriteBytes() {
    return writeBytes.get();
  }

  /**
   * Increase the number of bytes write into the container.
   * @param bytes the number of bytes write into the container.
   */
  public void incrWriteBytes(long bytes) {
    this.writeBytes.addAndGet(bytes);
  }

  /**
   * Get the number of writes into the container.
   * @return the number of writes into the container.
   */
  public long getWriteCount() {
    return writeCount.get();
  }

  /**
   * Increase the number of writes into the container by 1.
   */
  public void incrWriteCount() {
    this.writeCount.incrementAndGet();
  }


}
