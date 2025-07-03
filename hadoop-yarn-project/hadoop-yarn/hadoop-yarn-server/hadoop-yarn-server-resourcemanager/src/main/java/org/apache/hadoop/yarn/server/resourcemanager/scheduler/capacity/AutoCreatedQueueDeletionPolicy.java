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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AutoCreatedQueueDeletionEvent;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Auto deletion policy for auto created queue V2.
 * Just for weight based auto created queues.
 */
public class AutoCreatedQueueDeletionPolicy implements SchedulingEditPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(AutoCreatedQueueDeletionPolicy.class);

  private Clock clock;

  // Pointer to other RM components
  private RMContext rmContext;
  private ResourceCalculator rc;
  private CapacityScheduler scheduler;

  private long monitoringInterval;

  // markedForDeletion: in each interval,
  // this set is extended by queues that are eligible for auto deletion.
  private Set<String> markedForDeletion = new HashSet<>();
  // sentForDeletion: if in the next interval,
  // there is queue, that is eligible for auto deletion,
  // and is already marked for deletion, move it to this queue.
  private Set<String> sentForDeletion = new HashSet<>();

  @Override
  public void init(final Configuration config, final RMContext context,
                   final ResourceScheduler sched) {
    LOG.info("Auto Deletion Policy monitor: {}" + this.
        getClass().getCanonicalName());
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class " +
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    rmContext = context;
    scheduler = (CapacityScheduler) sched;
    clock = scheduler.getClock();

    rc = scheduler.getResourceCalculator();

    CapacitySchedulerConfiguration csConfig = scheduler.getConfiguration();

    // The monitor time will equal the
    // auto deletion expired time default.
    monitoringInterval =
        csConfig.getLong(CapacitySchedulerConfiguration.
                AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME,
            CapacitySchedulerConfiguration.
                DEFAULT_AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME) * 1000;

    prepareForAutoDeletion();
  }

  public void prepareForAutoDeletion() {
    Set<String> newMarks = new HashSet<>();
    for (Map.Entry<String, CSQueue> queueEntry :
        scheduler.getCapacitySchedulerQueueManager().getQueues().entrySet()) {
      String queuePath = queueEntry.getKey();
      CSQueue queue = queueEntry.getValue();
      if (queue instanceof AbstractCSQueue &&
          ((AbstractCSQueue) queue).isEligibleForAutoDeletion()) {
        if (markedForDeletion.contains(queuePath)) {
          sentForDeletion.add(queuePath);
          markedForDeletion.remove(queuePath);
        } else {
          newMarks.add(queuePath);
        }
      }
    }
    markedForDeletion.clear();
    markedForDeletion.addAll(newMarks);
  }

  @Override
  public void editSchedule() {
    long startTs = clock.getTime();

    prepareForAutoDeletion();
    triggerAutoDeletionForExpiredQueues();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Total time used=" + (clock.getTime() - startTs) + " ms.");
    }
  }

  public void triggerAutoDeletionForExpiredQueues() {
    // Proceed new auto created queues
    for (String queueName : sentForDeletion) {
      CSQueue checkQueue =
          scheduler.getCapacitySchedulerQueueManager().
              getQueue(queueName);
      deleteAutoCreatedQueue(checkQueue);
    }
    sentForDeletion.clear();
  }

  private void deleteAutoCreatedQueue(CSQueue queue) {
    if (queue != null) {
      AutoCreatedQueueDeletionEvent autoCreatedQueueDeletionEvent =
          new AutoCreatedQueueDeletionEvent(queue);
      LOG.info("Queue:" + queue.getQueuePath() +
          " will trigger deletion event to CS.");
      scheduler.getRMContext().getDispatcher().getEventHandler().handle(
          autoCreatedQueueDeletionEvent);
    }
  }

  @Override
  public long getMonitoringInterval() {
    return monitoringInterval;
  }

  @Override
  public String getPolicyName() {
    return AutoCreatedQueueDeletionPolicy.class.getCanonicalName();
  }

  @VisibleForTesting
  public Set<String> getMarkedForDeletion() {
    return markedForDeletion;
  }

  @VisibleForTesting
  public Set<String> getSentForDeletion() {
    return sentForDeletion;
  }
}
