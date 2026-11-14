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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.AllocationRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.CapacityRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.HierarchyRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.LimitRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.NodeLabelRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.PlacementRulesRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.StateRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.StructureRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.ValueRule;

/**
 * Orders issues the way a refresh evaluates the checks
 * ({@code inventory-checks.md} section 1.10), so that the first ERROR is the
 * one a refresh fails with: the scheduler allocation settings, then queue
 * construction depth first (each queue's setup steps in order, the checks of
 * a parent's children after its last descendant), the hierarchy transition,
 * the reinitialization of live queues, the capacity update and the placement
 * rules. Warnings come last.
 */
final class IssueOrder implements Comparator<ValidationIssue> {
  private static final int GLOBAL = 0;
  private static final int PARSE = 1;
  private static final int HIERARCHY = 2;
  private static final int REINITIALIZE = 3;
  private static final int CAPACITY_UPDATE = 4;
  private static final int PLACEMENT = 5;
  private static final int WARNINGS = 6;

  /** Steps of the checks a parent runs over its children. */
  private static final int CHILDREN = 200;

  private static final Map<String, int[]> RULES = new HashMap<>();
  /** Setup steps of values reported as invalid-value, by key suffix. */
  private static final Map<String, Integer> VALUE_STEPS = new HashMap<>();

  static {
    rule(AllocationRule.INVALID_MEMORY_ALLOCATION, GLOBAL, 0);
    rule(AllocationRule.INVALID_VCORES_ALLOCATION, GLOBAL, 1);
    rule(StructureRule.MISSING_CHILD_QUEUES, PARSE, 0);
    rule(StructureRule.RESERVABLE_PARENT_QUEUE, PARSE, 0);
    rule(NodeLabelRule.INVALID_NODE_LABEL_KEY, PARSE, 0);
    rule(NodeLabelRule.CHILD_LABELS_NOT_SUBSET, PARSE, 10);
    rule(CapacityRule.INVALID_CAPACITY, PARSE, 20);
    rule(CapacityRule.INVALID_CAPACITY_RESOURCE, PARSE, 30);
    rule(CapacityRule.MAXIMUM_RESOURCE_EXCEEDS_PARENT, PARSE, 30);
    rule(CapacityRule.MINIMUM_RESOURCE_EXCEEDS_MAXIMUM, PARSE, 30);
    rule(AllocationRule.INVALID_MAXIMUM_ALLOCATION, PARSE, 40);
    rule(AllocationRule.MAXIMUM_ALLOCATION_EXCEEDS_CLUSTER, PARSE, 40);
    rule(StateRule.INVALID_STATE, PARSE, 50);
    rule(StateRule.RUNNING_QUEUE_UNDER_STOPPED_PARENT, PARSE, 50);
    rule(LimitRule.INVALID_USER_WEIGHT, PARSE, 55);
    rule(CapacityRule.INVALID_CAPACITY_VECTOR, PARSE, 60);
    rule(CapacityRule.MIXED_CAPACITY_CONFIG_TYPE, PARSE, 65);
    rule(ValueRule.INVALID_VALUE, PARSE, 70);
    rule(ValueRule.INVALID_MULTI_NODE_POLICY, PARSE, 75);
    rule(LimitRule.DEFAULT_LIFETIME_EXCEEDS_MAXIMUM, PARSE, 80);
    rule(ValueRule.INVALID_QUEUE_ORDERING_POLICY, PARSE, 90);
    rule(ValueRule.INVALID_ORDERING_POLICY, PARSE, 100);
    rule(ValueRule.INVALID_PRIORITY_ACL, PARSE, 110);
    rule(NodeLabelRule.INVALID_DEFAULT_LABEL_EXPRESSION, PARSE, 115);
    rule(LimitRule.USER_WEIGHT_OUT_OF_RANGE, PARSE, 125);
    rule(StructureRule.MANAGED_PARENT_TEMPLATE_TYPE, PARSE, 130);
    rule(ValueRule.INVALID_QUEUE_MANAGEMENT_POLICY, PARSE, 135);
    rule(NodeLabelRule.INVALID_TEMPLATE_LABEL, PARSE, 140);
    rule(CapacityRule.MIXED_CHILDREN_CAPACITY_TYPES, PARSE, CHILDREN);
    rule(CapacityRule.ABSOLUTE_CAPACITY_MISMATCH, PARSE, CHILDREN + 2);
    rule(CapacityRule.CHILDREN_MINIMUM_RESOURCE_EXCEEDS_PARENT, PARSE,
        CHILDREN + 3);
    rule(CapacityRule.CHILDREN_CAPACITY_SUM, PARSE, CHILDREN + 4);
    rule(HierarchyRule.QUEUE_REMOVAL_NOT_STOPPED, HIERARCHY, 0);
    rule(HierarchyRule.MANAGED_PARENT_CONVERSION, HIERARCHY, 0);
    rule(HierarchyRule.LEAF_TO_PARENT_CONVERSION, HIERARCHY, 0);
    rule(AllocationRule.MAXIMUM_ALLOCATION_DECREASED, REINITIALIZE, 0);
    rule(StateRule.PARENT_QUEUE_NOT_RUNNING, REINITIALIZE, 50);
    rule(CapacityRule.MIXED_DYNAMIC_CHILDREN_CAPACITY_TYPES, CAPACITY_UPDATE,
        0);
    rule(PlacementRulesRule.DUPLICATE_PLACEMENT_RULES, PLACEMENT, 0);
    rule(PlacementRulesRule.INVALID_PLACEMENT_RULE, PLACEMENT, 1);
    rule(PlacementRulesRule.INVALID_MAPPING_RULES, PLACEMENT, 2);
    rule(PlacementRulesRule.INVALID_MAPPING_RULE_TARGET, PLACEMENT, 3);
    rule(PlacementRulesRule.INVALID_WORKFLOW_PRIORITY_MAPPING, PLACEMENT, 4);

    VALUE_STEPS.put("maximum-am-resource-percent", 20);
    VALUE_STEPS.put("maximum-queue-depth", 1);
    VALUE_STEPS.put("priority", 70);
    VALUE_STEPS.put("max-parallel-apps", 80);
    VALUE_STEPS.put("maximum-application-lifetime", 80);
    VALUE_STEPS.put("default-application-lifetime", 80);
    VALUE_STEPS.put("user-limit", 105);
    VALUE_STEPS.put("user-limit-factor", 105);
    VALUE_STEPS.put("maximum-applications", 105);
    VALUE_STEPS.put("default-application-priority", 120);
  }

  private final ValidationContext context;
  private Map<String, Integer> hierarchyPositions;

  IssueOrder(ValidationContext context) {
    this.context = context;
  }

  /**
   * The hierarchy transition checks run over the live queues in the
   * iteration order of the queue store, a HashMap keyed by full queue path,
   * and stop at the first failure.
   */
  private int getHierarchyPosition(String queuePath) {
    if (hierarchyPositions == null) {
      Map<String, Boolean> store = new HashMap<>();
      for (String live : context.getFacts().getQueues().keySet()) {
        store.put(live, Boolean.TRUE);
      }
      hierarchyPositions = new HashMap<>();
      for (String live : store.keySet()) {
        hierarchyPositions.put(live, hierarchyPositions.size());
      }
    }
    Integer position = hierarchyPositions.get(queuePath);
    return position == null ? -1 : position;
  }

  private static void rule(String ruleId, int phase, int step) {
    RULES.put(ruleId, new int[] {phase, step});
  }

  @Override
  public int compare(ValidationIssue a, ValidationIssue b) {
    int[] x = key(a);
    int[] y = key(b);
    for (int i = 0; i < x.length; i++) {
      if (x[i] != y[i]) {
        return Integer.compare(x[i], y[i]);
      }
    }
    return 0;
  }

  /** Phase, position, step, negated depth. */
  private int[] key(ValidationIssue issue) {
    int[] rule = RULES.get(issue.getRuleId());
    if (issue.getSeverity() == ValidationIssue.Severity.WARNING
        || rule == null) {
      return new int[] {WARNINGS, 0, 0, 0};
    }
    int phase = rule[0];
    int step = rule[1];
    if (phase == GLOBAL || phase == PLACEMENT || issue.getQueuePath() == null) {
      return new int[] {phase, -1, step, 0};
    }
    QueuePath path = new QueuePath(issue.getQueuePath());
    ResolvedQueueConfig queue = context.getTree().get(path);
    if (phase == PARSE && queue != null && queue.isDynamic()) {
      // Dynamic queues are not parsed, a refresh reinitializes them
      phase = REINITIALIZE;
    }
    if (ValueRule.INVALID_VALUE.equals(issue.getRuleId())
        && issue.getPropertyKey() != null) {
      String key = issue.getPropertyKey();
      Integer valueStep =
          VALUE_STEPS.get(key.substring(key.lastIndexOf('.') + 1));
      if (valueStep != null) {
        step = valueStep;
      }
    }
    int position;
    if (phase == HIERARCHY) {
      position = getHierarchyPosition(issue.getQueuePath());
    } else {
      position = step >= CHILDREN ? context.getSubtreeEnd(path)
          : context.getPosition(path);
    }
    return new int[] {phase, position, step,
        -path.getPathComponents().length};
  }
}
