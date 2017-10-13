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
package org.apache.hadoop.ozone.container.common.impl;

import org.apache.hadoop.ozone.container.common.helpers.ContainerData;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class represents the state of a container. if the
 * container reading encountered an error when we boot up we will post that
 * info to a recovery queue and keep the info in the containerMap.
 * <p/>
 * if and when the issue is fixed, the expectation is that this entry will be
 * deleted by the recovery thread from the containerMap and will insert entry
 * instead of modifying this class.
 */
public class ContainerStatus {
  private final ContainerData containerData;

  /**
   * Number of pending deletion blocks in container.
   */
  private int numPendingDeletionBlocks;

  private AtomicLong readBytes;

  private AtomicLong writeBytes;

  private AtomicLong readCount;

  private AtomicLong writeCount;

  /**
   * Creates a Container Status class.
   *
   * @param containerData - ContainerData.
   */
  ContainerStatus(ContainerData containerData) {
    this.numPendingDeletionBlocks = 0;
    this.containerData = containerData;
    this.readCount = new AtomicLong(0L);
    this.readBytes =  new AtomicLong(0L);
    this.writeCount =  new AtomicLong(0L);
    this.writeBytes =  new AtomicLong(0L);
  }

  /**
   * Returns container if it is active. It is not active if we have had an
   * error and we are waiting for the background threads to fix the issue.
   *
   * @return ContainerData.
   */
  public ContainerData getContainer() {
    return containerData;
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

  /**
   * Get the number of bytes used by the container.
   * @return the number of bytes used by the container.
   */
  public long getBytesUsed() {
    return containerData.getBytesUsed();
  }

  /**
   * Increase the number of bytes used by the container.
   * @param used number of bytes used by the container.
   * @return the current number of bytes used by the container afert increase.
   */
  public long incrBytesUsed(long used) {
    return containerData.addBytesUsed(used);
  }

  /**
   * Set the number of bytes used by the container.
   * @param used the number of bytes used by the container.
   */
  public void setBytesUsed(long used) {
    containerData.setBytesUsed(used);
  }

  /**
   * Decrease the number of bytes used by the container.
   * @param reclaimed the number of bytes reclaimed from the container.
   * @return the current number of bytes used by the container after decrease.
   */
  public long decrBytesUsed(long reclaimed) {
    return this.containerData.addBytesUsed(-1L * reclaimed);
  }

  /**
   * Get the maximum container size.
   * @return the maximum container size.
   */
  public long getMaxSize() {
    return containerData.getMaxSize();
  }

  /**
   * Set the maximum container size.
   * @param size the maximum container size.
   */
  public void setMaxSize(long size) {
    this.containerData.setMaxSize(size);
  }

  /**
   * Get the number of keys in the container.
   * @return the number of keys in the container.
   */
  public long getNumKeys() {
    return containerData.getKeyCount();
  }
}