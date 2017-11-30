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

  protected AutoCreatedLeafQueueTemplate leafQueueTemplate;

  public AbstractManagedParentQueue(CapacitySchedulerContext cs,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);

    super.setupQueueConfigs(csContext.getClusterResource());
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
      throws IOException {
    try {
      writeLock.lock();

      // Set new configs
      setupQueueConfigs(clusterResource);

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
  protected AutoCreatedLeafQueueTemplate.Builder initializeLeafQueueConfigs
    (String queuePath) {

    CapacitySchedulerConfiguration conf = csContext.getConfiguration();

    AutoCreatedLeafQueueTemplate.Builder leafQueueTemplateBuilder = new
        AutoCreatedLeafQueueTemplate.Builder();
    int maxApps = conf.getMaximumApplicationsPerQueue(queuePath);
    if (maxApps < 0) {
      maxApps = (int) (
          CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS
              * getAbsoluteCapacity());
    }

    int userLimit = conf.getUserLimit(queuePath);
    float userLimitFactor = conf.getUserLimitFactor(queuePath);
    leafQueueTemplateBuilder.userLimit(userLimit)
          .userLimitFactor(userLimitFactor)
          .maxApps(maxApps)
          .maxAppsPerUser(
              (int) (maxApps * (userLimit / 100.0f) * userLimitFactor));

    return leafQueueTemplateBuilder;
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

  protected float sumOfChildCapacities() {
    try {
      writeLock.lock();
      float ret = 0;
      for (CSQueue l : childQueues) {
        ret += l.getCapacity();
      }
      return ret;
    } finally {
      writeLock.unlock();
    }
  }

  protected float sumOfChildAbsCapacities() {
    try {
      writeLock.lock();
      float ret = 0;
      for (CSQueue l : childQueues) {
        ret += l.getAbsoluteCapacity();
      }
      return ret;
    } finally {
      writeLock.unlock();
    }
  }

  public static class AutoCreatedLeafQueueTemplate {

    private QueueCapacities queueCapacities;

    private int maxApps;
    private int maxAppsPerUser;
    private int userLimit;
    private float userLimitFactor;

    AutoCreatedLeafQueueTemplate(Builder builder) {
      this.maxApps = builder.maxApps;
      this.maxAppsPerUser = builder.maxAppsPerUser;
      this.userLimit = builder.userLimit;
      this.userLimitFactor = builder.userLimitFactor;
      this.queueCapacities = builder.queueCapacities;
    }

    public static class Builder {
      private int maxApps;
      private int maxAppsPerUser;

      private int userLimit;
      private float userLimitFactor;

      private QueueCapacities queueCapacities;

      Builder maxApps(int maxApplications) {
        this.maxApps =  maxApplications;
        return this;
      }

      Builder maxAppsPerUser(int maxApplicationsPerUser) {
        this.maxAppsPerUser = maxApplicationsPerUser;
        return this;
      }

      Builder userLimit(int usrLimit) {
        this.userLimit = usrLimit;
        return this;
      }

      Builder userLimitFactor(float ulf) {
        this.userLimitFactor = ulf;
        return this;
      }

      Builder capacities(QueueCapacities capacities) {
        this.queueCapacities = capacities;
        return this;
      }

      AutoCreatedLeafQueueTemplate build() {
        return new AutoCreatedLeafQueueTemplate(this);
      }
    }

    public int getUserLimit() {
      return userLimit;
    }

    public float getUserLimitFactor() {
      return userLimitFactor;
    }

    public QueueCapacities getQueueCapacities() {
      return queueCapacities;
    }

    public int getMaxApps() {
      return maxApps;
    }

    public int getMaxAppsPerUser() {
      return maxAppsPerUser;
    }
  }

  public AutoCreatedLeafQueueTemplate getLeafQueueTemplate() {
    return leafQueueTemplate;
  }
}
