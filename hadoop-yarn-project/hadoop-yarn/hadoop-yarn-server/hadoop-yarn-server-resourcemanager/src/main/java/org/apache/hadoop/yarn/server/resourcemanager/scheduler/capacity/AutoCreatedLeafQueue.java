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

import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Leaf queues which are auto created by an underlying implementation of
 * AbstractManagedParentQueue. Eg: PlanQueue for reservations or
 * ManagedParentQueue for auto created dynamic queues
 */
public class AutoCreatedLeafQueue extends AbstractAutoCreatedLeafQueue {

  private static final Logger LOG = LoggerFactory
      .getLogger(AutoCreatedLeafQueue.class);

  public AutoCreatedLeafQueue(CapacitySchedulerContext cs, String queueName,
      ManagedParentQueue parent) throws IOException {
    super(cs, parent.getLeafQueueConfigs(queueName),
        queueName,
        parent, null);
    updateCapacitiesToZero();
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
      throws IOException {
    try {
      writeLock.lock();

      validate(newlyParsedQueue);

      ManagedParentQueue managedParentQueue = (ManagedParentQueue) parent;

      super.reinitialize(newlyParsedQueue, clusterResource, managedParentQueue
          .getLeafQueueConfigs(newlyParsedQueue.getQueueName()));

      //Reset capacities to 0 since reinitialize above
      // queueCapacities to initialize to configured capacity which might
      // overcommit resources from parent queue
      updateCapacitiesToZero();

    } finally {
      writeLock.unlock();
    }
  }

  public void reinitializeFromTemplate(AutoCreatedLeafQueueConfig
      leafQueueTemplate) throws SchedulerDynamicEditException, IOException {

    try {
      writeLock.lock();

      // TODO:
      // reinitialize only capacities for now since 0 capacity updates
      // can cause
      // abs capacity related config computations to be incorrect if we go
      // through reinitialize
      QueueCapacities capacities = leafQueueTemplate.getQueueCapacities();

      //update abs capacities
      setupConfigurableCapacities(capacities);

      //reset capacities for the leaf queue
      mergeCapacities(capacities);

      //update queue used capacity for all the node labels
      CSQueueUtils.updateQueueStatistics(resourceCalculator,
          csContext.getClusterResource(),
          this, labelManager, null);

      //activate applications if any are pending
      activateApplications();

    } finally {
      writeLock.unlock();
    }
  }

  private void mergeCapacities(QueueCapacities capacities) {
    for ( String nodeLabel : capacities.getExistingNodeLabels()) {
      queueCapacities.setCapacity(nodeLabel,
          capacities.getCapacity(nodeLabel));
      queueCapacities.setAbsoluteCapacity(nodeLabel, capacities
          .getAbsoluteCapacity(nodeLabel));
      queueCapacities.setMaximumCapacity(nodeLabel, capacities
          .getMaximumCapacity(nodeLabel));
      queueCapacities.setAbsoluteMaximumCapacity(nodeLabel, capacities
          .getAbsoluteMaximumCapacity(nodeLabel));

      Resource resourceByLabel = labelManager.getResourceByLabel(nodeLabel,
          csContext.getClusterResource());
      getQueueResourceQuotas().setEffectiveMinResource(nodeLabel,
          Resources.multiply(resourceByLabel,
              queueCapacities.getAbsoluteCapacity(nodeLabel)));
      getQueueResourceQuotas().setEffectiveMaxResource(nodeLabel,
          Resources.multiply(resourceByLabel, queueCapacities
              .getAbsoluteMaximumCapacity(nodeLabel)));
    }
  }

  public void validateConfigurations(AutoCreatedLeafQueueConfig template)
      throws SchedulerDynamicEditException {
    QueueCapacities capacities = template.getQueueCapacities();
    for (String label : capacities.getExistingNodeLabels()) {
      float capacity = capacities.getCapacity(label);
      if (capacity < 0 || capacity > 1.0f) {
        throw new SchedulerDynamicEditException(
            "Capacity demand is not in the [0,1] range: " + capacity);
      }
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

  private void updateCapacitiesToZero() throws IOException {
    try {
      for( String nodeLabel : parent.getQueueCapacities().getExistingNodeLabels
          ()) {
        //TODO - update to use getMaximumCapacity(nodeLabel) in YARN-7574
        setEntitlement(nodeLabel, new QueueEntitlement(0.0f,
            parent.getLeafQueueTemplate()
                .getQueueCapacities()
                .getMaximumCapacity()));
      }
    } catch (SchedulerDynamicEditException e) {
      throw new IOException(e);
    }
  }
}
