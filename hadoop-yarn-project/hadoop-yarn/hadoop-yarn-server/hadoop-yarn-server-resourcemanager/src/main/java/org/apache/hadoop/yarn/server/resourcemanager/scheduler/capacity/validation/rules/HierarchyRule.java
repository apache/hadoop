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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueHierarchyTransitionChecks;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueHierarchyTransitionChecks.QueueSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.STATE;

/**
 * Checks of the transition from the live hierarchy to the proposed one
 * ({@link QueueHierarchyTransitionChecks}): a removed queue is stopped, a
 * leaf turned into a parent is stopped, and AQC v1 parents are neither
 * created from nor turned into other queues. Like a refresh, it skips the
 * checks on a standby ResourceManager with a mutable configuration.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class HierarchyRule implements ValidationRule {
  /** S04. */
  public static final String QUEUE_REMOVAL_NOT_STOPPED =
      "queue-removal-not-stopped";
  /** S05. */
  public static final String LEAF_TO_PARENT_CONVERSION =
      "leaf-to-parent-conversion";
  /** S06 and S07. */
  public static final String MANAGED_PARENT_CONVERSION =
      "managed-parent-conversion";
  /** A removed queue that still has applications, warning. */
  public static final String QUEUE_REMOVAL_WITH_APPLICATIONS =
      "queue-removal-with-applications";

  @Override
  public String getId() {
    return "queue-hierarchy";
  }

  @Override
  public void check(ValidationContext context, List<ValidationIssue> issues) {
    if (context.getFacts().isHierarchyChecksSkipped()) {
      return;
    }
    for (ClusterFacts.QueueFacts live
        : context.getFacts().getQueues().values()) {
      if (live.isAutoCreatedLeaf()) {
        continue;
      }
      String path = live.getQueuePath();
      QueueSnapshot oldQueue = new QueueSnapshot(path, live.getKind(),
          live.getState(), live.isDynamic());
      ResolvedQueueConfig newQueue =
          context.getTree().get(new QueuePath(path));
      if (newQueue == null) {
        String stateKey = QueuePrefixes.getQueuePrefix(new QueuePath(path))
            + CapacitySchedulerConfiguration.STATE;
        QueueState newState = QueueHierarchyTransitionChecks
            .parseConfiguredState(context.getProposed().get(stateKey), path);
        String error = QueueHierarchyTransitionChecks.checkQueueRemoval(
            oldQueue, newState);
        if (error != null) {
          issues.add(ValidationIssue.error(path, null,
              QUEUE_REMOVAL_NOT_STOPPED, error));
        } else if (live.getRunningApplications()
            + live.getPendingApplications() > 0) {
          issues.add(ValidationIssue.warning(path, null,
              QUEUE_REMOVAL_WITH_APPLICATIONS, "Queue " + path + " has "
                  + live.getRunningApplications() + " running and "
                  + live.getPendingApplications() + " pending applications;"
                  + " removing it does not wait for them."));
        }
        continue;
      }

      Resolved<QueueState> state = newQueue.get(STATE);
      if (!RuleSupport.ok(state)) {
        continue;
      }
      QueueSnapshot snapshot = new QueueSnapshot(path,
          context.getQueueKind(newQueue), state.getValue(),
          newQueue.isDynamic());
      String error = QueueHierarchyTransitionChecks.checkSameQueuePath(
          oldQueue, snapshot);
      if (error == null) {
        error = QueueHierarchyTransitionChecks.checkParentQueueConversion(
            oldQueue, snapshot);
        if (error != null) {
          issues.add(ValidationIssue.error(path, null,
              MANAGED_PARENT_CONVERSION, error));
          continue;
        }
        error = QueueHierarchyTransitionChecks.checkLeafQueueConversion(
            oldQueue, snapshot);
        if (error != null) {
          issues.add(ValidationIssue.error(path, null,
              LEAF_TO_PARENT_CONVERSION, error));
        }
      }
    }
  }
}
