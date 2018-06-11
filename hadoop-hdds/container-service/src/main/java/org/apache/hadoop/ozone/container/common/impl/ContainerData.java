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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerLifeCycleState;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ContainerData is the in-memory representation of container metadata and is
 * represented on disk by the .container file.
 */
public class ContainerData {

  //Type of the container.
  // For now, we support only KeyValueContainer.
  private final ContainerType containerType;

  // Unique identifier for the container
  private final long containerId;

  // Layout version of the container data
  private final int layOutVersion;

  // Metadata of the container will be a key value pair.
  // This can hold information like volume name, owner etc.,
  private final Map<String, String> metadata;

  // State of the Container
  private ContainerLifeCycleState state;

  /** parameters for read/write statistics on the container. **/
  private final AtomicLong readBytes;
  private final AtomicLong writeBytes;
  private final AtomicLong readCount;
  private final AtomicLong writeCount;
  private final AtomicLong bytesUsed;


  /**
   * Creates a ContainerData Object, which holds metadata of the container.
   * @param type - ContainerType
   * @param containerId - ContainerId
   */
  public ContainerData(ContainerType type, long containerId) {
    this.containerType = type;
    this.containerId = containerId;
    this.layOutVersion = ChunkLayOutVersion.getLatestVersion().getVersion();
    this.metadata = new TreeMap<>();
    this.state = ContainerLifeCycleState.OPEN;
    this.readCount = new AtomicLong(0L);
    this.readBytes =  new AtomicLong(0L);
    this.writeCount =  new AtomicLong(0L);
    this.writeBytes =  new AtomicLong(0L);
    this.bytesUsed = new AtomicLong(0L);
  }

  /**
   * Creates a ContainerData Object, which holds metadata of the container.
   * @param type - ContainerType
   * @param containerId - ContainerId
   * @param layOutVersion - Container layOutVersion
   */
  public ContainerData(ContainerType type, long containerId, int
      layOutVersion) {
    this.containerType = type;
    this.containerId = containerId;
    this.layOutVersion = layOutVersion;
    this.metadata = new TreeMap<>();
    this.state = ContainerLifeCycleState.OPEN;
    this.readCount = new AtomicLong(0L);
    this.readBytes =  new AtomicLong(0L);
    this.writeCount =  new AtomicLong(0L);
    this.writeBytes =  new AtomicLong(0L);
    this.bytesUsed = new AtomicLong(0L);
  }

  /**
   * Returns the containerId.
   */
  public long getContainerId() {
    return containerId;
  }

  /**
   * Returns the type of the container.
   * @return ContainerType
   */
  public ContainerType getContainerType() {
    return containerType;
  }


  /**
   * Returns the state of the container.
   * @return ContainerLifeCycleState
   */
  public synchronized ContainerLifeCycleState getState() {
    return state;
  }

  /**
   * Set the state of the container.
   * @param state
   */
  public synchronized void setState(ContainerLifeCycleState state) {
    this.state = state;
  }

  /**
   * Returns the layOutVersion of the actual container data format.
   * @return layOutVersion
   */
  public int getLayOutVersion() {
    return ChunkLayOutVersion.getChunkLayOutVersion(layOutVersion).getVersion();
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
   * Retuns metadata of the container.
   * @return metadata
   */
  public Map<String, String> getMetadata() {
    synchronized (this.metadata) {
      return Collections.unmodifiableMap(this.metadata);
    }
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
  public synchronized boolean isValid() {
    return !(ContainerLifeCycleState.INVALID == state);
  }

  /**
   * checks if the container is closed.
   * @return - boolean
   */
  public synchronized  boolean isClosed() {
    return ContainerLifeCycleState.CLOSED == state;
  }

  /**
   * Marks this container as closed.
   */
  public synchronized void closeContainer() {
    // TODO: closed or closing here
    setState(ContainerLifeCycleState.CLOSED);
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

  /**
   * Sets the number of bytes used by the container.
   * @param used
   */
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


}
