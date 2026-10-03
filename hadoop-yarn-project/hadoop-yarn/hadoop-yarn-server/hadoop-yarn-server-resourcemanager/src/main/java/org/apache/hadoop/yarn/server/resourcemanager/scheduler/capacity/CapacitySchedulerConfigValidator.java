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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueHierarchyTransitionChecks.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueHierarchyTransitionChecks.QueueSnapshot;
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
          final Configuration oldConfParam, final Configuration newConf,
          final RMContext rmContext) throws IOException {
    // ensure that the oldConf is deep copied
    Configuration oldConf = new Configuration(oldConfParam);
    QueueMetrics.setConfigurationValidation(oldConf, true);
    QueueMetrics.setConfigurationValidation(newConf, true);

    CapacityScheduler liveScheduler = (CapacityScheduler) rmContext.getScheduler();
    CapacityScheduler newCs = new CapacityScheduler();
    try {
      //TODO: extract all the validation steps and replace reinitialize with
      //the specific validation steps
      newCs.setConf(oldConf);
      newCs.setRMContext(rmContext);
      newCs.init(oldConf);
      newCs.addNodes(liveScheduler.getAllNodes());
      newCs.reinitialize(newConf, rmContext, true);
      return true;
    } finally {
      newCs.stop();
    }
  }

  public static Set<String> validatePlacementRules(
          Collection<String> placementRuleStrs) throws IOException {
    // fail the case if we get duplicate placementRule add in
    String error = PlacementRuleChecks.checkDuplicatePlacementRules(
            new PlacementRuleChecks.PlacementRuleNames(placementRuleStrs));
    if (error != null) {
      throw new IOException(error);
    }
    return new LinkedHashSet<>(placementRuleStrs);
  }

  public static void validateMemoryAllocation(Configuration conf) {
    int minMem = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    String error = QueueAllocationChecks.checkMemoryAllocation(
            new QueueAllocationChecks.AllocationRange(minMem, maxMem));
    if (error != null) {
      throw new YarnRuntimeException(error);
    }
  }
  public static void validateVCores(Configuration conf) {
    int minVcores = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    int maxVcores = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    String error = QueueAllocationChecks.checkVcoresAllocation(
            new QueueAllocationChecks.AllocationRange(minVcores, maxVcores));
    if (error != null) {
      throw new YarnRuntimeException(error);
    }
  }

  /**
   * Ensure all existing queues are present. Queues cannot be deleted if it's not
   * in Stopped state, Queue's cannot be moved from one hierarchy to other also.
   * Previous child queue could be converted into parent queue if it is in
   * STOPPED state.
   *
   * @param queues existing queues
   * @param newQueues new queues
   * @param newConf Capacity Scheduler Configuration.
   * @throws IOException an I/O exception has occurred.
   */
  public static void validateQueueHierarchy(
      CSQueueStore queues,
      CSQueueStore newQueues,
      CapacitySchedulerConfiguration newConf) throws IOException {
    // check that all static queues are included in the newQueues list
    for (CSQueue oldQueue : queues.getQueues()) {
      if (AbstractAutoCreatedLeafQueue.class.isAssignableFrom(oldQueue.getClass())) {
        continue;
      }

      final String queuePath = oldQueue.getQueuePath();
      final String configPrefix = QueuePrefixes.getQueuePrefix(
          oldQueue.getQueuePathObject());
      final QueueState newQueueState = QueueHierarchyTransitionChecks.parseConfiguredState(
          newConf.get(configPrefix + "state"), queuePath);
      final CSQueue newQueue = newQueues.get(queuePath);
      final QueueSnapshot oldSnapshot = toSnapshot(oldQueue);

      if (null == newQueue) {
        // old queue doesn't exist in the new XML
        String removalError = QueueHierarchyTransitionChecks.checkQueueRemoval(
            oldSnapshot, newQueueState);
        if (removalError != null) {
          throw new IOException(removalError);
        }
        if (isEitherQueueStopped(oldQueue.getState(), newQueueState)) {
          LOG.info("Deleting Queue {}, as it is not present in the modified capacity " +
              "configuration xml", queuePath);
        }
      } else {
        QueueSnapshot newSnapshot = toSnapshot(newQueue);
        validateSameQueuePath(oldSnapshot, newSnapshot);
        validateParentQueueConversion(oldSnapshot, newSnapshot);
        validateLeafQueueConversion(oldSnapshot, newSnapshot);
      }
    }
  }

  private static void validateSameQueuePath(QueueSnapshot oldQueue, QueueSnapshot newQueue)
      throws IOException {
    String error = QueueHierarchyTransitionChecks.checkSameQueuePath(oldQueue, newQueue);
    if (error != null) {
      // Queues cannot be moved from one hierarchy to another
      throw new IOException(error);
    }
  }

  private static void validateParentQueueConversion(QueueSnapshot oldQueue,
                                                    QueueSnapshot newQueue) throws IOException {
    String error = QueueHierarchyTransitionChecks.checkParentQueueConversion(oldQueue, newQueue);
    if (error != null) {
      throw new IOException(error);
    }

    if (QueueHierarchyTransitionChecks.isParent(oldQueue.getKind())
        && newQueue.getKind() == QueueKind.LEAF) {
      LOG.info("Converting the parent queue: {} to leaf queue.", oldQueue.getQueuePath());
    }
  }

  private static void validateLeafQueueConversion(QueueSnapshot oldQueue,
                                                  QueueSnapshot newQueue) throws IOException {
    String error = QueueHierarchyTransitionChecks.checkLeafQueueConversion(oldQueue, newQueue);
    if (error != null) {
      throw new IOException(error);
    }

    if (QueueHierarchyTransitionChecks.isLeafToParentConversion(oldQueue, newQueue)) {
      LOG.info("Converting the leaf queue: {} to parent queue.", oldQueue.getQueuePath());
    }
  }

  private static QueueSnapshot toSnapshot(CSQueue queue) {
    QueueKind kind;
    if (queue instanceof ManagedParentQueue) {
      kind = QueueKind.MANAGED_PARENT;
    } else if (queue instanceof AbstractParentQueue) {
      kind = QueueKind.PARENT;
    } else if (queue instanceof AbstractLeafQueue) {
      kind = QueueKind.LEAF;
    } else {
      kind = QueueKind.OTHER;
    }
    return new QueueSnapshot(queue.getQueuePath(), kind, queue.getState(),
        isDynamicQueue(queue));
  }

  private static boolean isDynamicQueue(CSQueue csQueue) {
    return ((AbstractCSQueue)csQueue).isDynamicQueue();
  }

  private static boolean isEitherQueueStopped(QueueState a, QueueState b) {
    return QueueHierarchyTransitionChecks.isEitherQueueStopped(a, b);
  }
}
