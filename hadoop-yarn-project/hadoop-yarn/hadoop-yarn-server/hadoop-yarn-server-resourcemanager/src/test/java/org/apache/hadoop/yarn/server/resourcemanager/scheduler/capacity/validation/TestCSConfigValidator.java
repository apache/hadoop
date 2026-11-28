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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue.Severity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult.ExplainEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.AllocationRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.CapacityRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.HierarchyRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.NodeLabelRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.PlacementRulesRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.StateRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.StructureRule;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Rule level tests of {@link CSConfigValidator} on configurations and cluster
 * facts built in memory. The corpus test checks the severity contract
 * against the live refresh path.
 */
public class TestCSConfigValidator {
  private static final String P = "yarn.scheduler.capacity.";

  private static Map<String, String> twoLeaves() {
    Map<String, String> conf = new LinkedHashMap<>();
    conf.put(P + "root.queues", "a,b");
    conf.put(P + "root.a.capacity", "50");
    conf.put(P + "root.b.capacity", "50");
    return conf;
  }

  /** Facts of a scheduler running the two leaf configuration. */
  private static ClusterFacts.Builder running() {
    return new ClusterFacts.Builder()
        .clusterResource(Resource.newInstance(100 * 1024, 100))
        .queue(queue("root", QueueKind.PARENT, QueueState.RUNNING))
        .queue(queue("root.a", QueueKind.LEAF, QueueState.RUNNING))
        .queue(queue("root.b", QueueKind.LEAF, QueueState.RUNNING));
  }

  private static ClusterFacts.QueueFacts queue(String path, QueueKind kind,
      QueueState state) {
    return new ClusterFacts.QueueFacts(path, kind, state, false, false,
        kind == QueueKind.LEAF ? Resource.newInstance(8192, 4) : null);
  }

  private static ValidationResult validate(Map<String, String> conf,
      ClusterFacts facts) {
    return new CSConfigValidator().validate(ConfigSnapshot.of(conf), facts);
  }

  private static List<ValidationIssue> issues(ValidationResult result,
      String ruleId) {
    List<ValidationIssue> found = new ArrayList<>();
    for (ValidationIssue issue : result.getIssues()) {
      if (issue.getRuleId().equals(ruleId)) {
        found.add(issue);
      }
    }
    return found;
  }

  private static ValidationIssue single(ValidationResult result,
      String ruleId) {
    List<ValidationIssue> found = issues(result, ruleId);
    assertEquals(1, found.size(), result.toString());
    return found.get(0);
  }

  @Test
  public void testUnchangedConfigurationHasNoIssues() {
    ValidationResult result = validate(twoLeaves(), running().build());
    assertTrue(result.isValid());
    assertEquals(Collections.emptyList(), result.getIssues());
  }

  @Test
  public void testChildrenCapacitySum() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "root.a.capacity", "60");
    ValidationIssue issue = single(validate(conf, running().build()),
        CapacityRule.CHILDREN_CAPACITY_SUM);
    assertEquals(Severity.ERROR, issue.getSeverity());
    assertEquals("root", issue.getQueuePath());
    assertEquals(P + "root.queues", issue.getPropertyKey());
    assertEquals("Illegal capacity sum of 1.1 for children of queue root for"
        + " label=. It should be either 0 or 1.0", issue.getMessage());
  }

  @Test
  public void testParseFailureIsReportedOnceWithTheKey() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "root.a.capacity", "half");
    ValidationResult result = validate(conf, running().build());
    ValidationIssue issue = single(result, CapacityRule.INVALID_CAPACITY);
    assertEquals("root.a", issue.getQueuePath());
    assertEquals(P + "root.a.capacity", issue.getPropertyKey());
    assertEquals("For input string: \"half\"", issue.getMessage());
    // The sum is not checked over a value that failed
    assertTrue(issues(result, CapacityRule.CHILDREN_CAPACITY_SUM).isEmpty());
    assertEquals(1, result.getIssues().size(), result.toString());
  }

  @Test
  public void testRemovingARunningQueue() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "root.queues", "a");
    conf.put(P + "root.a.capacity", "100");
    conf.remove(P + "root.b.capacity");
    ValidationIssue issue = single(validate(conf, running().build()),
        HierarchyRule.QUEUE_REMOVAL_NOT_STOPPED);
    assertEquals("root.b cannot be deleted from the capacity scheduler"
        + " configuration, as the queue is not yet in stopped state. Current"
        + " State : RUNNING", issue.getMessage());

    // A leftover state key of the removed queue allows the removal
    conf.put(P + "root.b.state", "STOPPED");
    ValidationResult result = validate(conf, running().build());
    assertEquals(Collections.emptyList(), result.getIssues());

    // A refresh on a standby RM with a mutable store skips the check
    conf.remove(P + "root.b.state");
    assertTrue(validate(conf, running().hierarchyChecksSkipped(true).build())
        .getIssues().isEmpty());
  }

  @Test
  public void testRemovingAQueueWithApplicationsWarns() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "root.queues", "a");
    conf.put(P + "root.a.capacity", "100");
    conf.put(P + "root.b.state", "STOPPED");
    ClusterFacts facts = running().queue(new ClusterFacts.QueueFacts("root.b",
        QueueKind.LEAF, QueueState.RUNNING, false, false, null)
        .withApplications(2, 1)).build();
    ValidationResult result = validate(conf, facts);
    assertTrue(result.isValid());
    assertEquals(Severity.WARNING, single(result,
        HierarchyRule.QUEUE_REMOVAL_WITH_APPLICATIONS).getSeverity());
  }

  @Test
  public void testLeafToParentConversion() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "root.a.queues", "a1");
    conf.put(P + "root.a.a1.capacity", "100");
    assertEquals("Can not convert the leaf queue: root.a to parent queue since"
        + " it is not yet in stopped state. Current State : RUNNING",
        single(validate(conf, running().build()),
            HierarchyRule.LEAF_TO_PARENT_CONVERSION).getMessage());
  }

  @Test
  public void testStoppedParentWithRunningChild() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "root.state", "STOPPED");
    conf.put(P + "root.a.state", "RUNNING");
    ValidationIssue issue = single(validate(conf, running().build()),
        StateRule.RUNNING_QUEUE_UNDER_STOPPED_PARENT);
    assertEquals("The parent queue:root cannot be STOPPED as the child"
        + " queue:root.a is in RUNNING state.", issue.getMessage());
  }

  @Test
  public void testActivatingAQueueUnderAStoppedParent() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "root.queues", "p");
    conf.put(P + "root.p.capacity", "100");
    conf.put(P + "root.p.queues", "a");
    conf.put(P + "root.p.a.capacity", "100");
    conf.put(P + "root.p.a.state", "RUNNING");
    conf.remove(P + "root.a.capacity");
    conf.remove(P + "root.b.capacity");
    ClusterFacts facts = new ClusterFacts.Builder()
        .queue(queue("root", QueueKind.PARENT, QueueState.RUNNING))
        .queue(queue("root.p", QueueKind.PARENT, QueueState.STOPPED))
        .queue(queue("root.p.a", QueueKind.LEAF, QueueState.STOPPED))
        .build();
    ValidationIssue issue = single(validate(conf, facts),
        StateRule.PARENT_QUEUE_NOT_RUNNING);
    assertEquals("The parent Queue:root.p is not running. Please activate the"
        + " parent queue first", issue.getMessage());
  }

  @Test
  public void testDecreasingTheMaximumAllocationOfALiveLeaf() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "root.a.maximum-allocation-mb", "4096");
    ValidationIssue issue = single(validate(conf, running().build()),
        AllocationRule.MAXIMUM_ALLOCATION_DECREASED);
    assertTrue(issue.getMessage().startsWith("Trying to reinitialize root.a"
        + " the maximum allocation size can not be decreased!"),
        issue.getMessage());
  }

  @Test
  public void testSchedulerAllocationFailsFirst() {
    Map<String, String> conf = twoLeaves();
    conf.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "0");
    conf.put(P + "root.a.capacity", "60");
    ValidationResult result = validate(conf, running().build());
    assertEquals(AllocationRule.INVALID_MEMORY_ALLOCATION,
        result.getIssues().get(0).getRuleId());
    assertEquals(CapacityRule.CHILDREN_CAPACITY_SUM,
        result.getIssues().get(1).getRuleId());
  }

  @Test
  public void testMixedCapacityTypes() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "root.a.capacity", "[memory=1024,vcores=1]");
    ValidationIssue issue = single(validate(conf, running().build()),
        CapacityRule.MIXED_CHILDREN_CAPACITY_TYPES);
    assertTrue(issue.getMessage().startsWith("Parent queue 'root' have"
        + " children queue used mixed of"), issue.getMessage());

    // Non-legacy mode allows mixing
    conf.put(P + "legacy-queue-mode.enabled", "false");
    assertTrue(validate(conf, running().build()).isValid());
  }

  @Test
  public void testMaximumResourceAboveParent() {
    Map<String, String> conf = new LinkedHashMap<>();
    conf.put(P + "root.queues", "p");
    conf.put(P + "root.p.capacity", "[memory=8192,vcores=8]");
    conf.put(P + "root.p.maximum-capacity", "[memory=8192,vcores=8]");
    conf.put(P + "root.p.queues", "a");
    conf.put(P + "root.p.a.capacity", "[memory=4096,vcores=4]");
    conf.put(P + "root.p.a.maximum-capacity", "[memory=16384,vcores=4]");
    ClusterFacts facts = new ClusterFacts.Builder()
        .clusterResource(Resource.newInstance(100 * 1024, 100))
        .resourceCalculator(new DominantResourceCalculator()).build();
    ValidationIssue issue = single(validate(conf, facts),
        CapacityRule.MAXIMUM_RESOURCE_EXCEEDS_PARENT);
    assertEquals("root.p.a", issue.getQueuePath());
    assertEquals(P + "root.p.a.maximum-capacity", issue.getPropertyKey());
  }

  @Test
  public void testNodeLabels() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "root.a.accessible-node-labels", "x,y");
    conf.put(P + "root.queues", "a,b");
    conf.put(P + "root.b.accessible-node-labels", "x");
    conf.put(P + "root.b.default-node-label-expression", "y");
    ValidationResult result = validate(conf, running().nodeLabel("x")
        .build());
    assertEquals("root.b", single(result,
        NodeLabelRule.INVALID_DEFAULT_LABEL_EXPRESSION).getQueuePath());
    ValidationIssue unknown = single(result, NodeLabelRule.UNKNOWN_NODE_LABEL);
    assertEquals(Severity.WARNING, unknown.getSeverity());
    assertEquals("root.a", unknown.getQueuePath());

    conf.remove(P + "root.b.default-node-label-expression");
    conf.put(P + "root.a.queues", "a1");
    conf.put(P + "root.a.a1.capacity", "100");
    conf.put(P + "root.a.a1.accessible-node-labels", "z");
    conf.put(P + "root.a.state", "STOPPED");
    assertEquals("Some labels of child queue is not a subset of parent queue,"
        + " these labels=[z]", single(validate(conf, running().build()),
            NodeLabelRule.CHILD_LABELS_NOT_SUBSET).getMessage());
  }

  @Test
  public void testRootWithoutChildren() {
    Map<String, String> conf = new LinkedHashMap<>();
    conf.put(P + "root.capacity", "100");
    assertEquals("Queue configuration missing child queue names for root",
        single(validate(conf, ClusterFacts.empty()),
            StructureRule.MISSING_CHILD_QUEUES).getMessage());
  }

  @Test
  public void testMappingRuleTargets() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "queue-mappings", "u:alice:missing");
    ValidationIssue issue = single(validate(conf, running().build()),
        PlacementRulesRule.INVALID_MAPPING_RULE_TARGET);
    assertEquals(Severity.ERROR, issue.getSeverity());

    // A static target under an AQC v2 parent can be created
    conf.put(P + "root.queues", "a,b,v2");
    conf.put(P + "root.a.capacity", "1w");
    conf.put(P + "root.b.capacity", "1w");
    conf.put(P + "root.v2.capacity", "1w");
    conf.put(P + "root.v2.auto-queue-creation-v2.enabled", "true");
    conf.put(P + "queue-mappings", "u:alice:root.v2.alice,u:bob:a");
    assertTrue(validate(conf, running().build()).isValid());

    conf.put(YarnConfiguration.QUEUE_PLACEMENT_RULES,
        "user-group,user-group");
    assertEquals(PlacementRulesRule.DUPLICATE_PLACEMENT_RULES,
        validate(conf, running().build()).getIssues().get(0).getRuleId());
  }

  @Test
  public void testWorkflowPriorityMappings() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "workflow-priority-mappings", "wf1:a:2,wf2:b");
    ValidationIssue issue = single(validate(conf, running().build()),
        PlacementRulesRule.INVALID_WORKFLOW_PRIORITY_MAPPING);
    assertEquals("Illegal workflow priority mapping wf2:b",
        issue.getMessage());
    conf.put(P + "workflow-priority-mappings", "wf1:a:2");
    assertTrue(validate(conf, running().build()).isValid());
  }

  @Test
  public void testDynamicChildrenAreChecked() {
    Map<String, String> conf = new LinkedHashMap<>();
    conf.put(P + "root.queues", "p");
    conf.put(P + "root.p.capacity", "100");
    conf.put(P + "root.p.auto-queue-creation-v2.enabled", "true");
    conf.put(P + "root.p.queues", "s");
    conf.put(P + "root.p.s.capacity", "100");
    ClusterFacts facts = new ClusterFacts.Builder()
        .queue(queue("root", QueueKind.PARENT, QueueState.RUNNING))
        .queue(queue("root.p", QueueKind.PARENT, QueueState.RUNNING))
        .queue(queue("root.p.s", QueueKind.LEAF, QueueState.RUNNING))
        .queue(new ClusterFacts.QueueFacts("root.p.d", QueueKind.LEAF,
            QueueState.RUNNING, true, false, null))
        .build();
    // The dynamic leaf uses a weight next to a percentage sibling
    ValidationIssue issue = single(validate(conf, facts),
        CapacityRule.MIXED_DYNAMIC_CHILDREN_CAPACITY_TYPES);
    assertEquals("root.p", issue.getQueuePath());
  }

  @Test
  public void testWarningsDoNotInvalidate() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "root.a.maximum-capacity", "40");
    conf.put(P + "root.b.user-limit-factor", "0");
    ValidationResult result = validate(conf, running().build());
    assertTrue(result.isValid(), result.toString());
    assertEquals(Severity.WARNING, single(result,
        CapacityRule.CAPACITY_EXCEEDS_MAXIMUM_CAPACITY).getSeverity());
    assertFalse(result.getIssues().isEmpty());
  }

  @Test
  public void testExplainUsesResolvedSources() {
    Map<String, String> conf = twoLeaves();
    conf.put(P + "user-limit-factor", "3");
    conf.put(P + "root.a.ordering-policy", "fair");
    ValidationResult result = new CSConfigValidator().validate(
        ConfigSnapshot.of(conf), running().build(),
        Arrays.asList("root.a", "root.missing"));
    assertEquals(Collections.singleton("root.a"),
        result.getExplain().keySet());
    Map<String, ExplainEntry> entries = new LinkedHashMap<>();
    for (ExplainEntry entry : result.getExplain().get("root.a")) {
      entries.put(entry.getKey(), entry);
    }
    ExplainEntry capacity = entries.get(P + "root.a.capacity");
    assertEquals("50.0", capacity.getValue());
    assertEquals("QUEUE", capacity.getSource());
    assertEquals(P + "root.a.capacity", capacity.getSourceDetail());
    ExplainEntry factor = entries.get(P + "root.a.user-limit-factor");
    assertEquals("3.0", factor.getValue());
    assertEquals("GLOBAL", factor.getSource());
    ExplainEntry policy = entries.get(P + "root.a.ordering-policy");
    assertEquals("fair", policy.getValue());
    ExplainEntry maximum = entries.get(P + "root.a.maximum-capacity");
    assertEquals("DEFAULT", maximum.getSource());
  }
}
