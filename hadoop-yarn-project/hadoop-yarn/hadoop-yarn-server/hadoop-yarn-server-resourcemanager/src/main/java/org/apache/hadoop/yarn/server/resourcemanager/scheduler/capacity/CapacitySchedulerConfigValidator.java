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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueHierarchyTransitionChecks.QueueSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.AllocationRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.PlacementRulesRule;
import org.apache.hadoop.yarn.util.resource.Resources;
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

  /**
   * Validates a new configuration against the queues of an old one, without
   * building a scheduler or queues and without touching the live state the
   * RMContext holds.
   *
   * @param oldConfParam the configuration the queues are currently built
   *                     from
   * @param newConf the new configuration
   * @param rmContext the RMContext, for the cluster resource and node labels
   * @return true if the new configuration is valid
   * @throws IOException the exception a refresh to the new configuration
   *         fails with
   */
  public static boolean validateCSConfiguration(
          final Configuration oldConfParam, final Configuration newConf,
          final RMContext rmContext) throws IOException {
    ClusterFacts baseline = baselineFacts(oldConfParam, rmContext);
    throwIfInvalid(new CSConfigValidator().validate(
        ConfigSnapshot.of(newConf), baseline));
    return true;
  }

  /**
   * Throws the exception a refresh fails with for the first ERROR of a
   * validation result, the way the scheduler-conf validate endpoint has
   * always reported failures. The failure a rule caught is the cause, as it
   * is the cause of the refresh failure.
   *
   * @param result a validation result
   * @throws IOException {@code Failed to re-init queues : <cause>} for a
   *         failure of the queue refresh
   * @throws RuntimeException the failure of an invalid scheduler allocation,
   *         which is validated before the queues are refreshed
   */
  @InterfaceAudience.Private
  public static void throwIfInvalid(ValidationResult result)
      throws IOException {
    ValidationIssue error = null;
    for (ValidationIssue issue : result.getIssues()) {
      if (issue.isError()) {
        error = issue;
        break;
      }
    }
    if (error == null) {
      return;
    }
    String ruleId = error.getRuleId();
    RuntimeException failure = error.getFailure();
    if (ruleId.equals(AllocationRule.INVALID_MEMORY_ALLOCATION)
        || ruleId.equals(AllocationRule.INVALID_VCORES_ALLOCATION)) {
      throw failure != null ? failure
          : new YarnRuntimeException(error.getMessage());
    }
    String cause = error.getMessage();
    if (ruleId.equals(PlacementRulesRule.INVALID_MAPPING_RULE_TARGET)) {
      // The placement rule wraps the YarnException of the rule
      cause = "org.apache.hadoop.yarn.exceptions.YarnException: " + cause;
    }
    throw new IOException("Failed to re-init queues : " + cause,
        failure != null ? failure : new IOException(error.getMessage()));
  }

  /**
   * The queues as a scheduler built from a configuration has them: no
   * dynamic queues, no applications, states as configured.
   */
  private static ClusterFacts baselineFacts(Configuration conf,
      RMContext rmContext) {
    ValidationContext baseline = new ValidationContext(
        ConfigSnapshot.of(conf), ClusterFacts.empty());
    ClusterFacts.Builder facts = new ClusterFacts.Builder();
    Resource cluster = null;
    if (rmContext != null
        && rmContext.getScheduler() instanceof CapacityScheduler) {
      cluster = rmContext.getScheduler().getClusterResource();
    }
    if (cluster == null) {
      cluster = Resources.none();
    }
    facts.clusterResource(cluster);
    // Like a refresh, the scheduler skips the hierarchy checks on a standby
    // RM with a mutable configuration
    facts.hierarchyChecksSkipped(ClusterFacts.isHierarchyChecksSkipped(
        rmContext != null
            && rmContext.getScheduler() instanceof CapacityScheduler
            && ((CapacityScheduler) rmContext.getScheduler())
                .isConfigurationMutable(), rmContext));
    try {
      facts.resourceCalculator(
          baseline.getConfiguration().getResourceCalculator());
    } catch (RuntimeException e) {
      LOG.debug("Using the default resource calculator", e);
    }
    facts.clusterNodeLabels(rmContext, cluster);
    for (ResolvedQueueConfig queue : baseline.getTree().getQueues()) {
      Resolved<QueueState> state = queue.get(QueueProperties.STATE);
      Resolved<Resource> maximumAllocation =
          queue.get(QueueProperties.MAXIMUM_ALLOCATION);
      facts.queue(new ClusterFacts.QueueFacts(
          queue.getQueuePath().getFullPath(), baseline.getQueueKind(queue),
          state == null || state.isFailed() ? QueueState.RUNNING
              : state.getValue(), false, queue.getKind() == Kind.RESERVATION,
          maximumAllocation == null || maximumAllocation.isFailed()
              ? null : maximumAllocation.getValue()));
    }
    return facts.build();
  }

  public static Set<String> validatePlacementRules(
          Collection<String> placementRuleStrs) throws IOException {
    // fail the case if we get duplicate placementRule add in
    String error =
            PlacementRuleChecks.checkDuplicatePlacementRules(placementRuleStrs);
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

    String error = QueueAllocationChecks.checkMemoryAllocation(minMem, maxMem);
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

    String error =
            QueueAllocationChecks.checkVcoresAllocation(minVcores, maxVcores);
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

    if (oldQueue.getKind().isParent()
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
    return new QueueSnapshot(queue.getQueuePath(), QueueKind.of(queue), queue.getState(),
        isDynamicQueue(queue));
  }

  private static boolean isDynamicQueue(CSQueue csQueue) {
    return ((AbstractCSQueue)csQueue).isDynamicQueue();
  }

  private static boolean isEitherQueueStopped(QueueState a, QueueState b) {
    return QueueHierarchyTransitionChecks.isEitherQueueStopped(a, b);
  }
}
