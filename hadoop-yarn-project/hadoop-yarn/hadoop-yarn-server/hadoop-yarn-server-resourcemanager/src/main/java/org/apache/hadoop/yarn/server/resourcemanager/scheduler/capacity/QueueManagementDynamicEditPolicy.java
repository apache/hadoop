/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;


import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;


import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .QueueManagementChangeEvent;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Queue Management scheduling policy for managed parent queues which enable
 * auto child queue creation
 */
public class QueueManagementDynamicEditPolicy implements SchedulingEditPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(QueueManagementDynamicEditPolicy.class);

  private Clock clock;

  // Pointer to other RM components
  private RMContext rmContext;
  private ResourceCalculator rc;
  private CapacityScheduler scheduler;
  private RMNodeLabelsManager nlm;

  private long monitoringInterval;

  private Set<String> managedParentQueues = new HashSet<>();

  /**
   * Instantiated by CapacitySchedulerConfiguration
   */
  public QueueManagementDynamicEditPolicy() {
    clock = SystemClock.getInstance();
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  public QueueManagementDynamicEditPolicy(RMContext context,
      CapacityScheduler scheduler) {
    init(context.getYarnConfiguration(), context, scheduler);
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  public QueueManagementDynamicEditPolicy(RMContext context,
      CapacityScheduler scheduler, Clock clock) {
    init(context.getYarnConfiguration(), context, scheduler);
    this.clock = clock;
  }

  @Override
  public void init(final Configuration config, final RMContext context,
      final ResourceScheduler sched) {
    LOG.info("Queue Management Policy monitor: {}" + this.
        getClass().getCanonicalName());
    assert null == scheduler : "Unexpected duplicate call to init";
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class " +
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    rmContext = context;
    scheduler = (CapacityScheduler) sched;
    clock = scheduler.getClock();

    rc = scheduler.getResourceCalculator();
    nlm = scheduler.getRMContext().getNodeLabelManager();

    CapacitySchedulerConfiguration csConfig = scheduler.getConfiguration();

    monitoringInterval = csConfig.getLong(
        CapacitySchedulerConfiguration.QUEUE_MANAGEMENT_MONITORING_INTERVAL,
        CapacitySchedulerConfiguration.
            DEFAULT_QUEUE_MANAGEMENT_MONITORING_INTERVAL);

    initQueues();
  }

  /**
   * Reinitializes queues(Called on scheduler.reinitialize)
   * @param config Configuration
   * @param context The resourceManager's context
   * @param sched The scheduler
   */
  public void reinitialize(final Configuration config, final RMContext context,
      final ResourceScheduler sched) {
    //TODO - Wire with scheduler reinitialize and remove initQueues below?
    initQueues();
  }

  private void initQueues() {
    managedParentQueues.clear();
    for (Map.Entry<String, CSQueue> queues : scheduler
        .getCapacitySchedulerQueueManager()
        .getQueues().entrySet()) {

      String queueName = queues.getKey();
      CSQueue queue = queues.getValue();

      if ( queue instanceof ManagedParentQueue) {
        managedParentQueues.add(queueName);
      }
    }
  }

  @Override
  public void editSchedule() {
    long startTs = clock.getTime();

    initQueues();
    manageAutoCreatedLeafQueues();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Total time used=" + (clock.getTime() - startTs) + " ms.");
    }
  }

  @VisibleForTesting
  List<QueueManagementChange> manageAutoCreatedLeafQueues()
  {

    List<QueueManagementChange> queueManagementChanges = new ArrayList<>();
    // All partitions to look at

    //Proceed only if there are queues to process
    if (managedParentQueues.size() > 0) {
      for (String parentQueueName : managedParentQueues) {
        ManagedParentQueue parentQueue =
            (ManagedParentQueue) scheduler.getCapacitySchedulerQueueManager().
                getQueue(parentQueueName);

        queueManagementChanges.addAll(
            computeQueueManagementChanges
            (parentQueue));
      }
    }
    return queueManagementChanges;
  }


  @VisibleForTesting
  List<QueueManagementChange> computeQueueManagementChanges
      (ManagedParentQueue parentQueue) {

    List<QueueManagementChange> queueManagementChanges =
        Collections.emptyList();
    if (!parentQueue.shouldFailAutoCreationWhenGuaranteedCapacityExceeded()) {

      AutoCreatedQueueManagementPolicy policyClazz =
          parentQueue.getAutoCreatedQueueManagementPolicy();
      long startTime = 0;
      try {
        startTime = clock.getTime();

        queueManagementChanges = policyClazz.computeQueueManagementChanges();

        //Scheduler update is asynchronous
        if (queueManagementChanges.size() > 0) {
          QueueManagementChangeEvent queueManagementChangeEvent =
              new QueueManagementChangeEvent(parentQueue,
                  queueManagementChanges);
          scheduler.getRMContext().getDispatcher().getEventHandler().handle(
              queueManagementChangeEvent);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("{} uses {} millisecond" + " to run",
              policyClazz.getClass().getName(), clock.getTime() - startTime);
          if (queueManagementChanges.size() > 0) {
            LOG.debug(" Updated queue management changes for parent queue" + " "
                    + "{}: [{}]", parentQueue.getQueueName(),
                queueManagementChanges.size() < 25 ?
                    queueManagementChanges.toString() :
                    queueManagementChanges.size());
          }
        }
      } catch (YarnException e) {
        LOG.error(
            "Could not compute child queue management updates for parent "
                + "queue "
                + parentQueue.getQueueName(), e);
      }
    } else{
      LOG.debug("Skipping queue management updates for parent queue {} "
          + "since configuration for auto creating queues beyond "
          + "parent's guaranteed capacity is disabled",
          parentQueue.getQueuePath());
    }
    return queueManagementChanges;
  }

  @Override
  public long getMonitoringInterval() {
    return monitoringInterval;
  }

  @Override
  public String getPolicyName() {
    return "QueueManagementDynamicEditPolicy";
  }

  public ResourceCalculator getResourceCalculator() {
    return rc;
  }

  public RMContext getRmContext() {
    return rmContext;
  }

  public ResourceCalculator getRC() {
    return rc;
  }

  public CapacityScheduler getScheduler() {
    return scheduler;
  }

  public Set<String> getManagedParentQueues() {
    return managedParentQueues;
  }
}
