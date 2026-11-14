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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueStateHelper;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ValueSource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue.Severity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.STATE;

/**
 * Queue state checks ({@link QueueStateHelper}): the configured state is
 * RUNNING or STOPPED, a newly parsed RUNNING queue is not under a parent that
 * is not RUNNING, and a live queue is only activated under a running parent.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class StateRule implements ValidationRule {
  /** S01. */
  public static final String INVALID_STATE = "invalid-state";
  /** S02. */
  public static final String RUNNING_QUEUE_UNDER_STOPPED_PARENT =
      "running-queue-under-stopped-parent";
  /** S03. */
  public static final String PARENT_QUEUE_NOT_RUNNING =
      "parent-queue-not-running";

  @Override
  public String getId() {
    return "queue-state";
  }

  @Override
  public void check(ValidationContext context, List<ValidationIssue> issues) {
    // The state each queue has after a refresh, parents first
    Map<QueuePath, QueueState> refreshed = new HashMap<>();
    for (ResolvedQueueConfig queue : context.getTree().getQueues()) {
      String path = queue.getQueuePath().getFullPath();
      Resolved<QueueState> state = queue.get(STATE);
      if (state == null) {
        continue;
      }
      if (state.isFailed()) {
        RuleSupport.reportFailure(issues, queue, STATE, "", INVALID_STATE,
            Severity.ERROR);
        continue;
      }
      QueueState configured = isConfigured(state) ? state.getValue() : null;
      String error = QueueStateHelper.checkConfiguredState(configured);
      if (error != null) {
        issues.add(ValidationIssue.error(path,
            RuleSupport.keyOf(queue, STATE, "", state), INVALID_STATE, error));
        continue;
      }

      ResolvedQueueConfig parent = context.getParent(queue);
      ClusterFacts.QueueFacts live = context.getFacts().getQueue(path);
      boolean reinitialized = live != null
          && live.getKind() == context.getQueueKind(queue)
          || live != null && queue.isDynamic();
      if (!queue.isDynamic() && parent != null) {
        // Every newly parsed queue starts without a state
        Resolved<QueueState> parentState = parent.get(STATE);
        if (RuleSupport.ok(parentState)) {
          error = QueueStateHelper.checkInitialState(
              path, configured, parent.getQueuePath().getFullPath(),
              parentState.getValue());
          if (error != null) {
            issues.add(ValidationIssue.error(path,
                RuleSupport.keyOf(queue, STATE, "", state),
                RUNNING_QUEUE_UNDER_STOPPED_PARENT, error));
          }
        }
      }

      if (!reinitialized) {
        refreshed.put(queue.getQueuePath(), state.getValue());
        continue;
      }
      // A live queue keeps its state unless the configuration changes it
      QueueState previous = live.getState();
      QueueState next = previous;
      if (configured == QueueState.STOPPED && previous == QueueState.RUNNING) {
        next = QueueState.STOPPED;
      } else if (configured == QueueState.RUNNING
          && previous != QueueState.RUNNING) {
        String parentError = parent == null ? null
            : QueueStateHelper.checkParentRunning(
                parent.getQueuePath().getFullPath(),
                refreshed.get(parent.getQueuePath()));
        if (parentError == null) {
          next = QueueState.RUNNING;
        } else {
          issues.add(ValidationIssue.error(path,
              RuleSupport.keyOf(queue, STATE, "", state),
              PARENT_QUEUE_NOT_RUNNING, parentError));
        }
      }
      refreshed.put(queue.getQueuePath(), next);
    }
  }

  private static boolean isConfigured(Resolved<QueueState> state) {
    return state.getSource() == ValueSource.QUEUE
        || state.getSource() == ValueSource.TEMPLATE_V1
        || state.getSource() == ValueSource.TEMPLATE_V2;
  }
}
