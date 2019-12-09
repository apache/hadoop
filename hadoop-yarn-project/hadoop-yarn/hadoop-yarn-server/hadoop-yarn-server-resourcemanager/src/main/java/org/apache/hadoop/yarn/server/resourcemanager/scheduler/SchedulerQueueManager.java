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

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;

/**
 *
 * Context of the Queues in Scheduler.
 *
 */
@SuppressWarnings("rawtypes")
@Private
@Unstable
public interface SchedulerQueueManager<T extends SchedulerQueue,
    E extends ReservationSchedulerConfiguration> {

  /**
   * Get the root queue.
   * @return root queue
   */
  T getRootQueue();

  /**
   * Get all the queues.
   * @return a map contains all the queues as well as related queue names
   */
  Map<String, T> getQueues();

  /**
   * Remove the queue from the existing queue.
   * @param queueName the queue name
   */
  void removeQueue(String queueName);

  /**
   * Add a new queue to the existing queues.
   * @param queueName the queue name
   * @param queue the queue object
   */
  void addQueue(String queueName, T queue);

  /**
   * Get a queue matching the specified queue name.
   * @param queueName the queue name
   * @return a queue object
   */
  T getQueue(String queueName);

  /**
   * Reinitialize the queues.
   * @param newConf the configuration
   * @throws IOException if fails to re-initialize queues
   */
  void reinitializeQueues(E newConf) throws IOException;
}
