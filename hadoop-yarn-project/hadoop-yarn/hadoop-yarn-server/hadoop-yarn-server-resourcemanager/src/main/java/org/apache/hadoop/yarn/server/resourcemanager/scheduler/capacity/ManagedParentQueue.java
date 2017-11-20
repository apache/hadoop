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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerDynamicEditException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Auto Creation enabled Parent queue. This queue initially does not have any
 * children to start with and all child
 * leaf queues will be auto created. Currently this does not allow other
 * pre-configured leaf or parent queues to
 * co-exist along with auto-created leaf queues. The auto creation is limited
 * to leaf queues currently.
 */
public class ManagedParentQueue extends AbstractManagedParentQueue {

  private boolean shouldFailAutoCreationWhenGuaranteedCapacityExceeded = false;

  private static final Logger LOG = LoggerFactory.getLogger(
      ManagedParentQueue.class);

  public ManagedParentQueue(final CapacitySchedulerContext cs,
      final String queueName, final CSQueue parent, final CSQueue old)
      throws IOException {
    super(cs, queueName, parent, old);
    String leafQueueTemplateConfPrefix = getLeafQueueConfigPrefix(
        csContext.getConfiguration());
    this.leafQueueTemplate = initializeLeafQueueConfigs(
        leafQueueTemplateConfPrefix).build();

    StringBuffer queueInfo = new StringBuffer();
    queueInfo.append("Created Managed Parent Queue: ").append(queueName).append(
        "]\nwith capacity: [").append(super.getCapacity()).append(
        "]\nwith max capacity: [").append(super.getMaximumCapacity()).append(
        "\nwith max apps: [").append(leafQueueTemplate.getMaxApps()).append(
        "]\nwith max apps per user: [").append(
        leafQueueTemplate.getMaxAppsPerUser()).append("]\nwith user limit: [")
        .append(leafQueueTemplate.getUserLimit()).append(
        "]\nwith user limit factor: [").append(
        leafQueueTemplate.getUserLimitFactor()).append("].");
    LOG.info(queueInfo.toString());
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
      throws IOException {
    validate(newlyParsedQueue);
    super.reinitialize(newlyParsedQueue, clusterResource);
    String leafQueueTemplateConfPrefix = getLeafQueueConfigPrefix(
        csContext.getConfiguration());
    this.leafQueueTemplate = initializeLeafQueueConfigs(
        leafQueueTemplateConfPrefix).build();
  }

  @Override
  protected AutoCreatedLeafQueueTemplate.Builder initializeLeafQueueConfigs(
      String queuePath) {

    AutoCreatedLeafQueueTemplate.Builder leafQueueTemplate =
        super.initializeLeafQueueConfigs(queuePath);

    CapacitySchedulerConfiguration conf = csContext.getConfiguration();
    String leafQueueTemplateConfPrefix = getLeafQueueConfigPrefix(conf);
    QueueCapacities queueCapacities = new QueueCapacities(false);
    CSQueueUtils.loadUpdateAndCheckCapacities(leafQueueTemplateConfPrefix,
        csContext.getConfiguration(), queueCapacities, getQueueCapacities());
    leafQueueTemplate.capacities(queueCapacities);

    shouldFailAutoCreationWhenGuaranteedCapacityExceeded =
        conf.getShouldFailAutoQueueCreationWhenGuaranteedCapacityExceeded(
            getQueuePath());

    return leafQueueTemplate;
  }

  protected void validate(final CSQueue newlyParsedQueue) throws IOException {
    // Sanity check
    if (!(newlyParsedQueue instanceof ManagedParentQueue) || !newlyParsedQueue
        .getQueuePath().equals(getQueuePath())) {
      throw new IOException(
          "Trying to reinitialize " + getQueuePath() + " from "
              + newlyParsedQueue.getQueuePath());
    }
  }

  @Override
  public void addChildQueue(CSQueue childQueue)
      throws SchedulerDynamicEditException {
    try {
      writeLock.lock();

      if (childQueue == null || !(childQueue instanceof AutoCreatedLeafQueue)) {
        throw new SchedulerDynamicEditException(
            "Expected child queue to be an instance of AutoCreatedLeafQueue");
      }

      CapacitySchedulerConfiguration conf = csContext.getConfiguration();
      ManagedParentQueue parentQueue =
          (ManagedParentQueue) childQueue.getParent();

      String leafQueueName = childQueue.getQueueName();
      int maxQueues = conf.getAutoCreatedQueuesMaxChildQueuesLimit(
          parentQueue.getQueuePath());

      if (parentQueue.getChildQueues().size() >= maxQueues) {
        throw new SchedulerDynamicEditException(
            "Cannot auto create leaf queue " + leafQueueName + ".Max Child "
                + "Queue limit exceeded which is configured as : " + maxQueues
                + " and number of child queues is : " + parentQueue
                .getChildQueues().size());
      }

      if (shouldFailAutoCreationWhenGuaranteedCapacityExceeded) {
        if (getLeafQueueTemplate().getQueueCapacities().getAbsoluteCapacity()
            + parentQueue.sumOfChildAbsCapacities() > parentQueue
            .getAbsoluteCapacity()) {
          throw new SchedulerDynamicEditException(
              "Cannot auto create leaf queue " + leafQueueName + ". Child "
                  + "queues capacities have reached parent queue : "
                  + parentQueue.getQueuePath() + " guaranteed capacity");
        }
      }

      AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) childQueue;
      super.addChildQueue(leafQueue);
      //TODO - refresh policy queue after capacity management is added

    } finally {
      writeLock.unlock();
    }
  }

  private String getLeafQueueConfigPrefix(CapacitySchedulerConfiguration conf) {
    return conf.getAutoCreatedQueueTemplateConfPrefix(getQueuePath());
  }

}