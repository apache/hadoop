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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .AbstractManagedParentQueue.AutoCreatedLeafQueueTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Leaf queues which are auto created by an underkying implementation of
 * AbstractManagedParentQueue. Eg: PlanQueue for reservations or
 * ManagedParentQueue for auto created dynamic queues
 */
public class AutoCreatedLeafQueue extends LeafQueue {

  private static final Logger LOG = LoggerFactory
      .getLogger(AutoCreatedLeafQueue.class);

  private AbstractManagedParentQueue parent;

  public AutoCreatedLeafQueue(CapacitySchedulerContext cs, String queueName,
      AbstractManagedParentQueue parent) throws IOException {
    super(cs, queueName, parent, null);

    AutoCreatedLeafQueueTemplate leafQueueTemplate =
        parent.getLeafQueueTemplate();
    updateApplicationAndUserLimits(leafQueueTemplate.getUserLimit(),
        leafQueueTemplate.getUserLimitFactor(),
        leafQueueTemplate.getMaxApps(),
        leafQueueTemplate.getMaxAppsPerUser());
    this.parent = parent;
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
      throws IOException {
    try {
      writeLock.lock();

      validate(newlyParsedQueue);

      super.reinitialize(newlyParsedQueue, clusterResource);
      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, null);

      AutoCreatedLeafQueueTemplate leafQueueTemplate =
          parent.getLeafQueueTemplate();
      updateApplicationAndUserLimits(leafQueueTemplate.getUserLimit(),
          leafQueueTemplate.getUserLimitFactor(),
          leafQueueTemplate.getMaxApps(),
          leafQueueTemplate.getMaxAppsPerUser());

    } finally {
      writeLock.unlock();
    }
  }

  /**
   * This methods to change capacity for a queue and adjusts its
   * absoluteCapacity.
   *
   * @param entitlement the new entitlement for the queue (capacity,
   *                    maxCapacity)
   * @throws SchedulerDynamicEditException
   */
  public void setEntitlement(QueueEntitlement entitlement)
      throws SchedulerDynamicEditException {
    try {
      writeLock.lock();
      float capacity = entitlement.getCapacity();
      if (capacity < 0 || capacity > 1.0f) {
        throw new SchedulerDynamicEditException(
            "Capacity demand is not in the [0,1] range: " + capacity);
      }
      setCapacity(capacity);
      setAbsoluteCapacity(getParent().getAbsoluteCapacity() * getCapacity());
      setMaxCapacity(entitlement.getMaxCapacity());
      if (LOG.isDebugEnabled()) {
        LOG.debug("successfully changed to " + capacity + " for queue " + this
            .getQueueName());
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void validate(final CSQueue newlyParsedQueue) throws IOException {
    if (!(newlyParsedQueue instanceof AutoCreatedLeafQueue) || !newlyParsedQueue
        .getQueuePath().equals(getQueuePath())) {
      throw new IOException(
          "Error trying to reinitialize " + getQueuePath() + " from "
              + newlyParsedQueue.getQueuePath());
    }

  }

  @Override
  protected void setupConfigurableCapacities() {
    CSQueueUtils.updateAndCheckCapacitiesByLabel(getQueuePath(),
        queueCapacities, parent == null ? null : parent.getQueueCapacities());
  }

  private void updateApplicationAndUserLimits(int userLimit,
      float userLimitFactor,
      int maxAppsForAutoCreatedQueues,
      int maxAppsPerUserForAutoCreatedQueues) {
    setUserLimit(userLimit);
    setUserLimitFactor(userLimitFactor);
    setMaxApplications(maxAppsForAutoCreatedQueues);
    setMaxApplicationsPerUser(maxAppsPerUserForAutoCreatedQueues);
  }
}
