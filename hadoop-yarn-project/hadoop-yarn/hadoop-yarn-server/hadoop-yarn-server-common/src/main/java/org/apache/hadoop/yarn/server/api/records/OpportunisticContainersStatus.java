/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p> <code>OpportunisticContainersStatus</code> captures information
 * pertaining to the state of execution of the opportunistic containers within a
 * node. </p>
 */
@Private
@Unstable
public abstract class OpportunisticContainersStatus {
  public static OpportunisticContainersStatus newInstance() {
    return Records.newRecord(OpportunisticContainersStatus.class);
  }

  /**
   * Returns the number of currently running opportunistic containers on the
   * node.
   *
   * @return number of running opportunistic containers.
   */
  @Private
  @Unstable
  public abstract int getRunningOpportContainers();

  /**
   * Sets the number of running opportunistic containers.
   *
   * @param runningOpportContainers number of running opportunistic containers.
   */
  @Private
  @Unstable
  public abstract void setRunningOpportContainers(int runningOpportContainers);

  /**
   * Returns memory currently used on the node for running opportunistic
   * containers.
   *
   * @return memory (in bytes) used for running opportunistic containers.
   */
  @Private
  @Unstable
  public abstract long getOpportMemoryUsed();

  /**
   * Sets the memory used on the node for running opportunistic containers.
   *
   * @param opportMemoryUsed memory (in bytes) used for running opportunistic
   *                         containers.
   */
  @Private
  @Unstable
  public abstract void setOpportMemoryUsed(long opportMemoryUsed);

  /**
   * Returns CPU cores currently used on the node for running opportunistic
   * containers.
   *
   * @return CPU cores used for running opportunistic containers.
   */
  @Private
  @Unstable
  public abstract int getOpportCoresUsed();

  /**
   * Sets the CPU cores used on the node for running opportunistic containers.
   *
   * @param opportCoresUsed memory (in bytes) used for running opportunistic
   *                         containers.
   */
  @Private
  @Unstable
  public abstract void setOpportCoresUsed(int opportCoresUsed);

  /**
   * Returns the number of queued opportunistic containers on the node.
   *
   * @return number of queued opportunistic containers.
   */
  @Private
  @Unstable
  public abstract int getQueuedOpportContainers();

  /**
   * Sets the number of queued opportunistic containers on the node.
   *
   * @param queuedOpportContainers number of queued opportunistic containers.
   */
  @Private
  @Unstable
  public abstract void setQueuedOpportContainers(int queuedOpportContainers);

  /**
   * Returns the length of the containers queue on the node.
   *
   * @return length of the containers queue.
   */
  @Private
  @Unstable
  public abstract int getWaitQueueLength();

  /**
   * Sets the length of the containers queue on the node.
   *
   * @param waitQueueLength length of the containers queue.
   */
  @Private
  @Unstable
  public abstract void setWaitQueueLength(int waitQueueLength);

  /**
   * Returns the estimated time that a container will have to wait if added to
   * the queue of the node.
   *
   * @return estimated queuing time.
   */
  @Private
  @Unstable
  public abstract int getEstimatedQueueWaitTime();

  /**
   * Sets the estimated time that a container will have to wait if added to the
   * queue of the node.
   *
   * @param queueWaitTime estimated queuing time.
   */
  @Private
  @Unstable
  public abstract void setEstimatedQueueWaitTime(int queueWaitTime);


  /**
   * Gets the capacity of the opportunistic containers queue on the node.
   *
   * @return queue capacity.
   */
  @Private
  @Unstable
  public abstract int getOpportQueueCapacity();


  /**
   * Sets the capacity of the opportunistic containers queue on the node.
   *
   * @param queueCapacity queue capacity.
   */
  @Private
  @Unstable
  public abstract void setOpportQueueCapacity(int queueCapacity);
}
