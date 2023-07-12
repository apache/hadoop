/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;


import java.io.IOException;
import java.util.List;

public interface AutoCreatedQueueManagementPolicy {

  /**
   * Initialize policy.
   * @param parentQueue parent queue
   * @throws IOException an I/O exception has occurred.
   */
  void init(AbstractParentQueue parentQueue) throws IOException;

  /**
   * Reinitialize policy state ( if required ).
   * @param parentQueue parent queue
   * @throws IOException an I/O exception has occurred.
   */
  void reinitialize(AbstractParentQueue parentQueue) throws IOException;

  /**
   * Get initial template for the specified leaf queue.
   * @param leafQueue the leaf queue
   * @return initial leaf queue template configurations and capacities for
   * auto created queue
   * @throws SchedulerDynamicEditException when get initialLeafQueue Configuration fails.
   */
  AutoCreatedLeafQueueConfig getInitialLeafQueueConfiguration(
      AbstractAutoCreatedLeafQueue leafQueue)
      throws SchedulerDynamicEditException;

  /**
   * Compute/Adjust child queue capacities
   * for auto created leaf queues
   * This computes queue entitlements but does not update LeafQueueState or
   * queue capacities. Scheduler calls commitQueueManagemetChanges after
   * validation after applying queue changes and commits to LeafQueueState
   * are done in commitQueueManagementChanges.
   *
   * @return returns a list of suggested QueueEntitlementChange(s) which may
   * or may not be enforced by the scheduler
   * @throws SchedulerDynamicEditException when compute QueueManagementChanges fails.
   */
  List<QueueManagementChange> computeQueueManagementChanges()
      throws SchedulerDynamicEditException;

  /**
   * Commit/Update state for the specified queue management changes.
   *
   * @param queueManagementChanges QueueManagementChange List.
   * @throws SchedulerDynamicEditException when commit QueueManagementChanges fails.
   */
  void commitQueueManagementChanges(
      List<QueueManagementChange> queueManagementChanges)
      throws SchedulerDynamicEditException;
}
