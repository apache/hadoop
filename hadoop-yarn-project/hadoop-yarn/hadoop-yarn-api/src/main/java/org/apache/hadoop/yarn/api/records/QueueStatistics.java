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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class QueueStatistics {

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static QueueStatistics newInstance(long submitted, long running,
      long pending, long completed, long killed, long failed, long activeUsers,
      long availableMemoryMB, long allocatedMemoryMB, long pendingMemoryMB,
      long reservedMemoryMB, long availableVCores, long allocatedVCores,
      long pendingVCores, long reservedVCores) {
    QueueStatistics statistics = Records.newRecord(QueueStatistics.class);
    statistics.setNumAppsSubmitted(submitted);
    statistics.setNumAppsRunning(running);
    statistics.setNumAppsPending(pending);
    statistics.setNumAppsCompleted(completed);
    statistics.setNumAppsKilled(killed);
    statistics.setNumAppsFailed(failed);
    statistics.setNumActiveUsers(activeUsers);
    statistics.setAvailableMemoryMB(availableMemoryMB);
    statistics.setAllocatedMemoryMB(allocatedMemoryMB);
    statistics.setPendingMemoryMB(pendingMemoryMB);
    statistics.setReservedMemoryMB(reservedMemoryMB);
    statistics.setAvailableVCores(availableVCores);
    statistics.setAllocatedVCores(allocatedVCores);
    statistics.setPendingVCores(pendingVCores);
    statistics.setReservedVCores(reservedVCores);
    return statistics;
  }

  /**
   * Get the number of apps submitted
   * 
   * @return the number of apps submitted
   */
  public abstract long getNumAppsSubmitted();

  /**
   * Set the number of apps submitted
   * 
   * @param numAppsSubmitted
   *          the number of apps submitted
   */
  public abstract void setNumAppsSubmitted(long numAppsSubmitted);

  /**
   * Get the number of running apps
   * 
   * @return the number of running apps
   */
  public abstract long getNumAppsRunning();

  /**
   * Set the number of running apps
   * 
   * @param numAppsRunning
   *          the number of running apps
   */
  public abstract void setNumAppsRunning(long numAppsRunning);

  /**
   * Get the number of pending apps
   * 
   * @return the number of pending apps
   */
  public abstract long getNumAppsPending();

  /**
   * Set the number of pending apps
   * 
   * @param numAppsPending
   *          the number of pending apps
   */
  public abstract void setNumAppsPending(long numAppsPending);

  /**
   * Get the number of completed apps
   * 
   * @return the number of completed apps
   */
  public abstract long getNumAppsCompleted();

  /**
   * Set the number of completed apps
   * 
   * @param numAppsCompleted
   *          the number of completed apps
   */
  public abstract void setNumAppsCompleted(long numAppsCompleted);

  /**
   * Get the number of killed apps
   * 
   * @return the number of killed apps
   */
  public abstract long getNumAppsKilled();

  /**
   * Set the number of killed apps
   * 
   * @param numAppsKilled
   *          the number of killed apps
   */
  public abstract void setNumAppsKilled(long numAppsKilled);

  /**
   * Get the number of failed apps
   * 
   * @return the number of failed apps
   */
  public abstract long getNumAppsFailed();

  /**
   * Set the number of failed apps
   * 
   * @param numAppsFailed
   *          the number of failed apps
   */
  public abstract void setNumAppsFailed(long numAppsFailed);

  /**
   * Get the number of active users
   * 
   * @return the number of active users
   */
  public abstract long getNumActiveUsers();

  /**
   * Set the number of active users
   * 
   * @param numActiveUsers
   *          the number of active users
   */
  public abstract void setNumActiveUsers(long numActiveUsers);

  /**
   * Get the available memory in MB
   * 
   * @return the available memory
   */
  public abstract long getAvailableMemoryMB();

  /**
   * Set the available memory in MB
   * 
   * @param availableMemoryMB
   *          the available memory
   */
  public abstract void setAvailableMemoryMB(long availableMemoryMB);

  /**
   * Get the allocated memory in MB
   * 
   * @return the allocated memory
   */
  public abstract long getAllocatedMemoryMB();

  /**
   * Set the allocated memory in MB
   * 
   * @param allocatedMemoryMB
   *          the allocate memory
   */
  public abstract void setAllocatedMemoryMB(long allocatedMemoryMB);

  /**
   * Get the pending memory in MB
   * 
   * @return the pending memory
   */
  public abstract long getPendingMemoryMB();

  /**
   * Set the pending memory in MB
   * 
   * @param pendingMemoryMB
   *          the pending memory
   */
  public abstract void setPendingMemoryMB(long pendingMemoryMB);

  /**
   * Get the reserved memory in MB
   * 
   * @return the reserved memory
   */
  public abstract long getReservedMemoryMB();

  /**
   * Set the reserved memory in MB
   * 
   * @param reservedMemoryMB
   *          the reserved memory
   */
  public abstract void setReservedMemoryMB(long reservedMemoryMB);

  /**
   * Get the available vcores
   * 
   * @return the available vcores
   */
  public abstract long getAvailableVCores();

  /**
   * Set the available vcores
   * 
   * @param availableVCores
   *          the available vcores
   */
  public abstract void setAvailableVCores(long availableVCores);

  /**
   * Get the allocated vcores
   * 
   * @return the allocated vcores
   */
  public abstract long getAllocatedVCores();

  /**
   * Set the allocated vcores
   * 
   * @param allocatedVCores
   *          the allocated vcores
   */
  public abstract void setAllocatedVCores(long allocatedVCores);

  /**
   * Get the pending vcores
   * 
   * @return the pending vcores
   */
  public abstract long getPendingVCores();

  /**
   * Set the pending vcores
   * 
   * @param pendingVCores
   *          the pending vcores
   */
  public abstract void setPendingVCores(long pendingVCores);

  /**
   * Get the number of pending containers.
   * @return the number of pending containers.
   */
  public abstract long getPendingContainers();

  /**
   * Set the number of pending containers.
   * @param pendingContainers the pending containers.
   */
  public abstract void setPendingContainers(long pendingContainers);

  /**
   * Get the number of allocated containers.
   * @return the number of allocated containers.
   */
  public abstract long getAllocatedContainers();

  /**
   * Set the number of allocated containers.
   * @param allocatedContainers the allocated containers.
   */
  public abstract void setAllocatedContainers(long allocatedContainers);

  /**
   * Get the number of reserved containers.
   * @return the number of reserved containers.
   */
  public abstract long getReservedContainers();

  /**
   * Set the number of reserved containers.
   * @param reservedContainers the reserved containers.
   */
  public abstract void setReservedContainers(long reservedContainers);

  /**
   * Get the reserved vcores
   * 
   * @return the reserved vcores
   */
  public abstract long getReservedVCores();

  /**
   * Set the reserved vcores
   * 
   * @param reservedVCores
   *          the reserved vcores
   */
  public abstract void setReservedVCores(long reservedVCores);
}
