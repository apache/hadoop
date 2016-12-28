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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;

/**
 *
 * QueueStateManager which can be used by Scheduler to manage the queue state.
 *
 */
// TODO: The class will be used by YARN-5734-OrgQueue for
// easy CapacityScheduler queue configuration management.
@SuppressWarnings("rawtypes")
@Private
@Unstable
public class QueueStateManager<T extends SchedulerQueue,
    E extends ReservationSchedulerConfiguration> {

  private static final Log LOG = LogFactory.getLog(QueueStateManager.class);

  private SchedulerQueueManager<T, E> queueManager;

  public synchronized void initialize(SchedulerQueueManager<T, E>
      newQueueManager) {
    this.queueManager = newQueueManager;
  }

  /**
   * Stop the queue.
   * @param queueName the queue name
   * @throws YarnException if the queue does not exist
   */
  @SuppressWarnings("unchecked")
  public synchronized void stopQueue(String queueName) throws YarnException {
    SchedulerQueue<T> queue = queueManager.getQueue(queueName);
    if (queue == null) {
      throw new YarnException("The specified queue:" + queueName
          + " does not exist!");
    }
    queue.stopQueue();
  }

  /**
   * Active the queue.
   * @param queueName the queue name
   * @throws YarnException if the queue does not exist
   *         or the queue can not be activated.
   */
  @SuppressWarnings("unchecked")
  public synchronized void activateQueue(String queueName)
      throws YarnException {
    SchedulerQueue<T> queue = queueManager.getQueue(queueName);
    if (queue == null) {
      throw new YarnException("The specified queue:" + queueName
          + " does not exist!");
    }
    queue.activeQueue();
  }

  /**
   * Whether this queue can be deleted.
   * @param queueName the queue name
   * @return true if the queue can be deleted
   */
  @SuppressWarnings("unchecked")
  public boolean canDelete(String queueName) {
    SchedulerQueue<T> queue = queueManager.getQueue(queueName);
    if (queue == null) {
      LOG.info("The specified queue:" + queueName + " does not exist!");
      return false;
    }
    if (queue.getState() == QueueState.STOPPED){
      return true;
    }
    LOG.info("Need to stop the specific queue:" + queueName + " first.");
    return false;
  }
}
