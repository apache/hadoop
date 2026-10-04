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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueStructureChecks;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue.Severity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.ACL_APPLICATION_MAX_PRIORITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.APP_ORDERING_POLICY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.AUTO_CREATE_CHILD_QUEUE_MAX_QUEUES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.AUTO_QUEUE_CREATION_V2_MAX_DEPTH;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.AUTO_QUEUE_CREATION_V2_MAX_QUEUES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.AVERAGE_CAPACITY_PERCENT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.DEFAULT_APPLICATION_LIFETIME;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.DEFAULT_APPLICATION_PRIORITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.INSTANTANEOUS_MAX_CAPACITY_PERCENT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MAXIMUM_APPLICATIONS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MAXIMUM_APPLICATION_LIFETIME;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MAX_PARALLEL_APPS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MULTI_NODE_SORTING_POLICY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.PRIORITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.QUEUE_ORDERING_POLICY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.RESERVATION_ENFORCEMENT_WINDOW_MS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.RESERVATION_WINDOW_MS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.USER_LIMIT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.USER_LIMIT_FACTOR;

/**
 * The per-queue values that only have a parse or load check: the throwing
 * getters of {@code CapacitySchedulerConfiguration} and the policy classes
 * a queue loads when it is built. Values read only when a dynamic queue is
 * created or by the reservation system are reported as warnings.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ValueRule implements ValidationRule {
  /** U12 and other values that fail to parse. */
  public static final String INVALID_VALUE = "invalid-value";
  /** U04. */
  public static final String INVALID_PRIORITY_ACL = "invalid-priority-acl";
  /** U06. */
  public static final String INVALID_ORDERING_POLICY =
      "invalid-ordering-policy";
  /** U07. */
  public static final String INVALID_QUEUE_ORDERING_POLICY =
      "invalid-queue-ordering-policy";
  /** U08. */
  public static final String INVALID_MULTI_NODE_POLICY =
      "invalid-multi-node-policy";
  /** U11. */
  public static final String INVALID_QUEUE_MANAGEMENT_POLICY =
      "invalid-queue-management-policy";
  /** U10, warning. */
  public static final String OFFSWITCH_ASSIGNMENTS_BELOW_ONE =
      "offswitch-assignments-below-one";

  /** Values queue setup reads, in its order. */
  private static final List<QueueProperty<?>> SETUP_VALUES = Arrays.asList(
      AUTO_QUEUE_CREATION_V2_MAX_DEPTH, PRIORITY, MAX_PARALLEL_APPS,
      MAXIMUM_APPLICATION_LIFETIME, DEFAULT_APPLICATION_LIFETIME, USER_LIMIT,
      USER_LIMIT_FACTOR, MAXIMUM_APPLICATIONS, DEFAULT_APPLICATION_PRIORITY);
  /** Values read when a dynamic queue is created or by the reservation
   * system. */
  private static final List<QueueProperty<?>> RUNTIME_VALUES = Arrays.asList(
      AUTO_CREATE_CHILD_QUEUE_MAX_QUEUES, AUTO_QUEUE_CREATION_V2_MAX_QUEUES,
      RESERVATION_WINDOW_MS, AVERAGE_CAPACITY_PERCENT,
      INSTANTANEOUS_MAX_CAPACITY_PERCENT, RESERVATION_ENFORCEMENT_WINDOW_MS);

  @Override
  public String getId() {
    return "queue-values";
  }

  @Override
  public void check(ValidationContext context, List<ValidationIssue> issues) {
    CapacitySchedulerConfiguration conf = context.getConfiguration();
    if (context.getClusterMaximumPriorityFailure() != null) {
      issues.add(ValidationIssue.error(null,
          YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
          INVALID_VALUE, context.getClusterMaximumPriorityFailure()));
    }
    try {
      int limit = conf.getInt(
          CapacitySchedulerConfiguration.OFFSWITCH_PER_HEARTBEAT_LIMIT,
          CapacitySchedulerConfiguration.DEFAULT_OFFSWITCH_PER_HEARTBEAT_LIMIT);
      if (limit < 1) {
        issues.add(ValidationIssue.warning(null,
            CapacitySchedulerConfiguration.OFFSWITCH_PER_HEARTBEAT_LIMIT,
            OFFSWITCH_ASSIGNMENTS_BELOW_ONE,
            CapacitySchedulerConfiguration.OFFSWITCH_PER_HEARTBEAT_LIMIT + "("
                + limit + ") < 1. Using 1."));
      }
    } catch (NumberFormatException e) {
      issues.add(ValidationIssue.warning(null,
          CapacitySchedulerConfiguration.OFFSWITCH_PER_HEARTBEAT_LIMIT,
          INVALID_VALUE, RuleSupport.message(e)));
    }

    for (ResolvedQueueConfig queue : context.getTree().getQueues()) {
      for (QueueProperty<?> property : SETUP_VALUES) {
        RuleSupport.reportFailure(issues, queue, property, "", INVALID_VALUE,
            Severity.ERROR);
      }
      RuleSupport.reportFailure(issues, queue, ACL_APPLICATION_MAX_PRIORITY,
          "", INVALID_PRIORITY_ACL, Severity.ERROR);
      for (QueueProperty<?> property : RUNTIME_VALUES) {
        RuleSupport.reportFailure(issues, queue, property, "", INVALID_VALUE,
            Severity.WARNING);
      }
      boolean policyFailed = RuleSupport.reportFailure(issues, queue,
          MULTI_NODE_SORTING_POLICY, "", INVALID_MULTI_NODE_POLICY,
          Severity.ERROR);
      if (!queue.isDynamic()) {
        checkPolicies(context, queue, policyFailed, issues);
      }
    }
  }

  /**
   * Whether the application ordering policy is one of the built-in policies,
   * which always load and configure; the getter scans the whole
   * configuration for the policy parameters, so it is skipped for them.
   */
  private static boolean isBuiltInAppOrderingPolicy(
      ResolvedQueueConfig queue) {
    Resolved<String> policy = queue.get(APP_ORDERING_POLICY);
    if (!RuleSupport.ok(policy)) {
      return false;
    }
    String name = policy.getValue().trim();
    return name.equals(CapacitySchedulerConfiguration.FIFO_APP_ORDERING_POLICY)
        || name.equals(CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY)
        || name.equals(CapacitySchedulerConfiguration
            .FIFO_WITH_PARTITIONS_APP_ORDERING_POLICY)
        || name.equals(CapacitySchedulerConfiguration.FIFO_FOR_PENDING_APPS);
  }

  /**
   * U06, U07, U08 and U11: the getters that load policy classes. They read
   * the configuration directly, so they run for configured queues only.
   */
  private static void checkPolicies(ValidationContext context,
      ResolvedQueueConfig queue, boolean multiNodePolicyFailed,
      List<ValidationIssue> issues) {
    CapacitySchedulerConfiguration conf = context.getConfiguration();
    String path = queue.getQueuePath().getFullPath();
    if (!multiNodePolicyFailed) {
      try {
        conf.getMultiNodesSortingAlgorithmPolicy(queue.getQueuePath());
      } catch (RuntimeException e) {
        issues.add(ValidationIssue.error(path,
            RuleSupport.keyOf(queue, MULTI_NODE_SORTING_POLICY, "",
                queue.get(MULTI_NODE_SORTING_POLICY)),
            INVALID_MULTI_NODE_POLICY, e));
      }
    }
    if (queue.getKind() == Kind.LEAF) {
      if (!isBuiltInAppOrderingPolicy(queue)) {
        try {
          conf.getAppOrderingPolicy(queue.getQueuePath());
        } catch (RuntimeException e) {
          issues.add(ValidationIssue.error(path, RuleSupport.key(queue,
              CapacitySchedulerConfiguration.ORDERING_POLICY),
              INVALID_ORDERING_POLICY, e));
        }
      }
    } else if (queue.getKind() != Kind.RESERVATION) {
      ResolvedQueueConfig parent = context.getParent(queue);
      Resolved<String> parentPolicy =
          parent == null ? null : parent.get(QUEUE_ORDERING_POLICY);
      if (parent == null || RuleSupport.ok(parentPolicy)) {
        try {
          conf.getQueueOrderingPolicy(queue.getQueuePath(),
              parentPolicy == null ? null : parentPolicy.getValue());
        } catch (RuntimeException e) {
          issues.add(ValidationIssue.error(path, RuleSupport.key(queue,
              CapacitySchedulerConfiguration.ORDERING_POLICY),
              INVALID_QUEUE_ORDERING_POLICY, e));
        }
      }
    }
    if (context.isManagedParent(queue)) {
      try {
        QueueStructureChecks.loadQueueManagementPolicy(conf,
            queue.getQueuePath());
      } catch (RuntimeException e) {
        issues.add(ValidationIssue.error(path, RuleSupport.key(queue,
            CapacitySchedulerConfiguration.AUTO_CREATED_QUEUE_MANAGEMENT_POLICY),
            INVALID_QUEUE_MANAGEMENT_POLICY, e));
      }
    }
  }
}
