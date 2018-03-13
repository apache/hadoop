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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common
    .QueueEntitlement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;

/**
 * Abstract class for dynamic auto created queues managed by an implementation
 * of AbstractManagedParentQueue
 */
public class AbstractAutoCreatedLeafQueue extends LeafQueue {

  protected AbstractManagedParentQueue parent;

  public AbstractAutoCreatedLeafQueue(CapacitySchedulerContext cs,
      String queueName, AbstractManagedParentQueue parent, CSQueue old)
      throws IOException {
    super(cs, queueName, parent, old);
    this.parent = parent;
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractAutoCreatedLeafQueue.class);

  public AbstractAutoCreatedLeafQueue(CapacitySchedulerContext cs,
      CapacitySchedulerConfiguration leafQueueConfigs, String queueName,
      AbstractManagedParentQueue parent, CSQueue old) throws IOException {
    super(cs, leafQueueConfigs, queueName, parent, old);
    this.parent = parent;
  }

  /**
   * This methods to change capacity for a queue and adjusts its
   * absoluteCapacity
   *
   * @param entitlement the new entitlement for the queue (capacity,
   *                    maxCapacity, etc..)
   * @throws SchedulerDynamicEditException
   */
  public void setEntitlement(QueueEntitlement entitlement)
      throws SchedulerDynamicEditException {
     setEntitlement(NO_LABEL, entitlement);
  }

  /**
   * This methods to change capacity for a queue and adjusts its
   * absoluteCapacity
   *
   * @param entitlement the new entitlement for the queue (capacity,
   *                    maxCapacity, etc..)
   * @throws SchedulerDynamicEditException
   */
  public void setEntitlement(String nodeLabel, QueueEntitlement entitlement)
      throws SchedulerDynamicEditException {
    try {
      writeLock.lock();
      float capacity = entitlement.getCapacity();
      if (capacity < 0 || capacity > 1.0f) {
        throw new SchedulerDynamicEditException(
            "Capacity demand is not in the [0,1] range: " + capacity);
      }
      setCapacity(nodeLabel, capacity);
      setAbsoluteCapacity(nodeLabel,
          getParent().getQueueCapacities().
              getAbsoluteCapacity(nodeLabel)
              * getQueueCapacities().getCapacity(nodeLabel));
      // note: we currently set maxCapacity to capacity
      // this might be revised later
      setMaxCapacity(nodeLabel, entitlement.getMaxCapacity());
      if (LOG.isDebugEnabled()) {
        LOG.debug("successfully changed to " + capacity + " for queue " + this
            .getQueueName());
      }

      //update queue used capacity etc
      CSQueueUtils.updateQueueStatistics(resourceCalculator,
          csContext.getClusterResource(),
          this, labelManager, nodeLabel);
    } finally {
      writeLock.unlock();
    }
  }

  protected void setupConfigurableCapacities(QueueCapacities queueCapacities) {
    CSQueueUtils.updateAndCheckCapacitiesByLabel(getQueuePath(),
        queueCapacities, parent == null ? null : parent.getQueueCapacities());
  }
}
