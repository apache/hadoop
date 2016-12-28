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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 *
 * Represents a queue in Scheduler.
 *
 */
@SuppressWarnings("rawtypes")
@LimitedPrivate("yarn")
public interface SchedulerQueue<T extends SchedulerQueue> extends Queue {

  /**
   * Get list of child queues.
   * @return a list of child queues
   */
  List<T> getChildQueues();

  /**
   * Get the parent queue.
   * @return the parent queue
   */
  T getParent();

  /**
   * Get current queue state.
   * @return the queue state
   */
  QueueState getState();

  /**
   * Update the queue state.
   * @param state the queue state
   */
  void updateQueueState(QueueState state);

  /**
   * Stop the queue.
   */
  void stopQueue();

  /**
   * Active the queue.
   * @throws YarnException if the queue can not be activated.
   */
  void activeQueue() throws YarnException;
}
