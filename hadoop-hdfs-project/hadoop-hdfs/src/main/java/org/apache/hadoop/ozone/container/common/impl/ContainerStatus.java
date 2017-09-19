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

/**
 * This is an immutable class that represents the state of a container. if the
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

  /**
   * Creates a Container Status class.
   *
   * @param containerData - ContainerData.
   */
  ContainerStatus(ContainerData containerData) {
    this.numPendingDeletionBlocks = 0;
    this.containerData = containerData;
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
}