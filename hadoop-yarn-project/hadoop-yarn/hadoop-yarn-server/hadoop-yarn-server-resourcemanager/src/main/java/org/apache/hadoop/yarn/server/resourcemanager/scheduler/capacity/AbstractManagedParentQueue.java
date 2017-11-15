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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * A container class for automatically created child leaf queues.
 * From the user perspective this is equivalent to a LeafQueue,
 * but functionality wise is a sub-class of ParentQueue
 */
public abstract class AbstractManagedParentQueue extends ParentQueue {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractManagedParentQueue.class);

  private int maxAppsForAutoCreatedQueues;
  private int maxAppsPerUserForAutoCreatedQueues;
  private int userLimit;
  private float userLimitFactor;

  public AbstractManagedParentQueue(CapacitySchedulerContext cs,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);

    super.setupQueueConfigs(csContext.getClusterResource());
    initializeLeafQueueConfigs();

    StringBuffer queueInfo = new StringBuffer();
    queueInfo.append("Created Managed Parent Queue: ").append(queueName)
        .append("\nof type : [" + getClass())
        .append("]\nwith capacity: [")
        .append(super.getCapacity()).append("]\nwith max capacity: [")
        .append(super.getMaximumCapacity()).append("\nwith max apps: [")
        .append(getMaxApplicationsForAutoCreatedQueues())
        .append("]\nwith max apps per user: [")
        .append(getMaxApplicationsPerUserForAutoCreatedQueues())
        .append("]\nwith user limit: [").append(getUserLimit())
        .append("]\nwith user limit factor: [")
        .append(getUserLimitFactor()).append("].");
    LOG.info(queueInfo.toString());
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
      throws IOException {
    try {
      writeLock.lock();

      // Set new configs
      setupQueueConfigs(clusterResource);

      initializeLeafQueueConfigs();

      // run reinitialize on each existing queue, to trigger absolute cap
      // recomputations
      for (CSQueue res : this.getChildQueues()) {
        res.reinitialize(res, clusterResource);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Initialize leaf queue configs from template configurations specified on
   * parent queue.
   */
  protected void initializeLeafQueueConfigs() {

    CapacitySchedulerConfiguration conf = csContext.getConfiguration();

    final String queuePath = super.getQueuePath();
    int maxApps = conf.getMaximumApplicationsPerQueue(queuePath);
    if (maxApps < 0) {
      maxApps = (int) (
          CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS
              * getAbsoluteCapacity());
    }
    userLimit = conf.getUserLimit(queuePath);
    userLimitFactor = conf.getUserLimitFactor(queuePath);
    maxAppsForAutoCreatedQueues = maxApps;
    maxAppsPerUserForAutoCreatedQueues =
        (int) (maxApps * (userLimit / 100.0f) * userLimitFactor);

  }

  /**
   * Number of maximum applications for each of the auto created leaf queues.
   *
   * @return maxAppsForAutoCreatedQueues
   */
  public int getMaxApplicationsForAutoCreatedQueues() {
    return maxAppsForAutoCreatedQueues;
  }

  /**
   * Number of maximum applications per user for each of the auto created
   * leaf queues.
   *
   * @return maxAppsPerUserForAutoCreatedQueues
   */
  public int getMaxApplicationsPerUserForAutoCreatedQueues() {
    return maxAppsPerUserForAutoCreatedQueues;
  }

  /**
   * User limit value for each of the  auto created leaf queues.
   *
   * @return userLimit
   */
  public int getUserLimitForAutoCreatedQueues() {
    return userLimit;
  }

  /**
   * User limit factor value for each of the  auto created leaf queues.
   *
   * @return userLimitFactor
   */
  public float getUserLimitFactor() {
    return userLimitFactor;
  }

  public int getMaxAppsForAutoCreatedQueues() {
    return maxAppsForAutoCreatedQueues;
  }

  public int getMaxAppsPerUserForAutoCreatedQueues() {
    return maxAppsPerUserForAutoCreatedQueues;
  }

  public int getUserLimit() {
    return userLimit;
  }

  /**
   * Add the specified child queue.
   * @param childQueue reference to the child queue to be added
   * @throws SchedulerDynamicEditException
   */
  public void addChildQueue(CSQueue childQueue)
      throws SchedulerDynamicEditException {
    try {
      writeLock.lock();
      if (childQueue.getCapacity() > 0) {
        throw new SchedulerDynamicEditException(
            "Queue " + childQueue + " being added has non zero capacity.");
      }
      boolean added = this.childQueues.add(childQueue);
      if (LOG.isDebugEnabled()) {
        LOG.debug("updateChildQueues (action: add queue): " + added + " "
            + getChildQueuesToPrint());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Remove the specified child queue.
   * @param childQueue reference to the child queue to be removed
   * @throws SchedulerDynamicEditException
   */
  public void removeChildQueue(CSQueue childQueue)
      throws SchedulerDynamicEditException {
    try {
      writeLock.lock();
      if (childQueue.getCapacity() > 0) {
        throw new SchedulerDynamicEditException(
            "Queue " + childQueue + " being removed has non zero capacity.");
      }
      Iterator<CSQueue> qiter = childQueues.iterator();
      while (qiter.hasNext()) {
        CSQueue cs = qiter.next();
        if (cs.equals(childQueue)) {
          qiter.remove();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Removed child queue: {}" + cs.getQueueName());
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Remove the specified child queue.
   * @param childQueueName name of the child queue to be removed
   * @throws SchedulerDynamicEditException
   */
  public CSQueue removeChildQueue(String childQueueName)
      throws SchedulerDynamicEditException {
    CSQueue childQueue;
    try {
      writeLock.lock();
      childQueue = this.csContext.getCapacitySchedulerQueueManager().getQueue(
          childQueueName);
      if (childQueue != null) {
        removeChildQueue(childQueue);
      } else {
        throw new SchedulerDynamicEditException("Cannot find queue to delete "
            + ": " + childQueueName);
      }
    } finally {
      writeLock.unlock();
    }
    return childQueue;
  }
}
