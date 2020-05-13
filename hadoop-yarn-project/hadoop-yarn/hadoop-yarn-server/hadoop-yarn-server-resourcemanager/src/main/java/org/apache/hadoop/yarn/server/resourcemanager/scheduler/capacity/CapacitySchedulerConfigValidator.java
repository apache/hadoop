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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public final class CapacitySchedulerConfigValidator {
  private static final Logger LOG = LoggerFactory.getLogger(
          CapacitySchedulerConfigValidator.class);

  private CapacitySchedulerConfigValidator() {
    throw new IllegalStateException("Utility class");
  }

  public static boolean validateCSConfiguration(
          final Configuration oldConf, final Configuration newConf,
          final RMContext rmContext) throws IOException {
    CapacityScheduler newCs = new CapacityScheduler();
    try {
      //TODO: extract all the validation steps and replace reinitialize with
      //the specific validation steps
      newCs.setConf(oldConf);
      newCs.setRMContext(rmContext);
      newCs.init(oldConf);
      newCs.reinitialize(newConf, rmContext, true);
      return true;
    } finally {
      newCs.stop();
    }
  }

  public static Set<String> validatePlacementRules(
          Collection<String> placementRuleStrs) throws IOException {
    Set<String> distinguishRuleSet = new LinkedHashSet<>();
    // fail the case if we get duplicate placementRule add in
    for (String pls : placementRuleStrs) {
      if (!distinguishRuleSet.add(pls)) {
        throw new IOException("Invalid PlacementRule inputs which "
                + "contains duplicate rule strings");
      }
    }
    return distinguishRuleSet;
  }

  public static void validateMemoryAllocation(Configuration conf) {
    int minMem = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
              + " allocation configuration"
              + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
              + "=" + minMem
              + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
              + "=" + maxMem + ", min and max should be greater than 0"
              + ", max should be no smaller than min.");
    }
  }
  public static void validateVCores(Configuration conf) {
    int minVcores = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    int maxVcores = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    if (minVcores <= 0 || minVcores > maxVcores) {
      throw new YarnRuntimeException("Invalid resource scheduler vcores"
              + " allocation configuration"
              + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
              + "=" + minVcores
              + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
              + "=" + maxVcores + ", min and max should be greater than 0"
              + ", max should be no smaller than min.");
    }
  }

  /**
   * Ensure all existing queues are present. Queues cannot be deleted if its not
   * in Stopped state, Queue's cannot be moved from one hierarchy to other also.
   * Previous child queue could be converted into parent queue if it is in
   * STOPPED state.
   *
   * @param queues existing queues
   * @param newQueues new queues
   */
  public static void validateQueueHierarchy(
      CSQueueStore queues,
      CSQueueStore newQueues,
      CapacitySchedulerConfiguration newConf) throws IOException {
    // check that all static queues are included in the newQueues list
    for (CSQueue oldQueue : queues.getQueues()) {
      if (!(AbstractAutoCreatedLeafQueue.class.isAssignableFrom(
          oldQueue.getClass()))) {
        String queuePath = oldQueue.getQueuePath();
        CSQueue newQueue = newQueues.get(queuePath);
        String configPrefix = newConf.getQueuePrefix(
            oldQueue.getQueuePath());
        String state = newConf.get(configPrefix + "state");
        QueueState newQueueState = null;
        if (state != null) {
          try {
            newQueueState = QueueState.valueOf(state);
          } catch (Exception ex) {
            LOG.warn("Not a valid queue state for queue "
                + oldQueue.getQueuePath());
          }
        }
        if (null == newQueue) {
          // old queue doesn't exist in the new XML
          if (oldQueue.getState() == QueueState.STOPPED ||
              newQueueState == QueueState.STOPPED) {
            LOG.info("Deleting Queue " + queuePath + ", as it is not"
                + " present in the modified capacity configuration xml");
          } else {
            throw new IOException(oldQueue.getQueuePath() + " cannot be"
              + " deleted from the capacity scheduler configuration, as the"
              + " queue is not yet in stopped state. Current State : "
              + oldQueue.getState());
          }
        } else if (!oldQueue.getQueuePath().equals(newQueue.getQueuePath())) {
          //Queue's cannot be moved from one hierarchy to other
          throw new IOException(
            queuePath + " is moved from:" + oldQueue.getQueuePath() + " to:"
              + newQueue.getQueuePath()
              + " after refresh, which is not allowed.");
        } else if (oldQueue instanceof ParentQueue
            && !(oldQueue instanceof ManagedParentQueue)
            && newQueue instanceof ManagedParentQueue) {
          throw new IOException(
            "Can not convert parent queue: " + oldQueue.getQueuePath()
                + " to auto create enabled parent queue since "
                + "it could have other pre-configured queues which is not "
                + "supported");
        } else if (oldQueue instanceof ManagedParentQueue
            && !(newQueue instanceof ManagedParentQueue)) {
          throw new IOException(
            "Cannot convert auto create enabled parent queue: "
              + oldQueue.getQueuePath() + " to leaf queue. Please check "
              + " parent queue's configuration "
              + CapacitySchedulerConfiguration
              .AUTO_CREATE_CHILD_QUEUE_ENABLED
              + " is set to true");
        } else if (oldQueue instanceof LeafQueue
                && newQueue instanceof ParentQueue) {
          if (oldQueue.getState() == QueueState.STOPPED ||
              newQueueState == QueueState.STOPPED) {
            LOG.info("Converting the leaf queue: " + oldQueue.getQueuePath()
                + " to parent queue.");
          } else{
            throw new IOException(
              "Can not convert the leaf queue: " + oldQueue.getQueuePath()
                + " to parent queue since "
                + "it is not yet in stopped state. Current State : "
                + oldQueue.getState());
          }
        } else if (oldQueue instanceof ParentQueue
            && newQueue instanceof LeafQueue) {
          LOG.info("Converting the parent queue: " + oldQueue.getQueuePath()
              + " to leaf queue.");
        }
      }
    }
  }
}
