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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleActionBase;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleActions;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleMatchers;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleResult;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleValidationContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlacementRuleChecks.PlacementRuleNames;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlacementRuleChecks.QueueIndex;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlacementRuleChecks.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlacementRuleChecks.QueueRef;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPlacementRuleChecks {

  /**
   * Queue index over plain data: full paths, unambiguous short names and the
   * set of ambiguous short names.
   */
  private static final class DataQueueIndex implements QueueIndex {
    private final Map<String, QueueRef> queues = new HashMap<>();
    private final Map<String, Integer> shortNameCounts = new HashMap<>();

    DataQueueIndex add(String path, QueueKind kind, boolean eligible) {
      QueueRef ref = new QueueRef(path, kind, eligible);
      queues.put(path, ref);
      String shortName = path.substring(path.lastIndexOf('.') + 1);
      shortNameCounts.merge(shortName, 1, Integer::sum);
      if (!path.equals(shortName)) {
        queues.put(shortName, ref);
      }
      return this;
    }

    @Override
    public QueueRef getQueue(String queueName) {
      if (isAmbiguous(queueName)) {
        return queueName.contains(".") ? queues.get(queueName) : null;
      }
      return queues.get(queueName);
    }

    @Override
    public boolean isAmbiguous(String shortName) {
      Integer count = shortNameCounts.get(shortName);
      return count != null && count > 1;
    }
  }

  /** Mirrors the variable update action of a set-variable mapping rule. */
  private static final class SetVariableAction extends MappingRuleActionBase {
    private final String variable;

    SetVariableAction(String variable) {
      this.variable = variable;
    }

    @Override
    public MappingRuleResult execute(VariableContext variables) {
      return MappingRuleResult.createSkipResult();
    }

    @Override
    public void validate(MappingRuleValidationContext ctx)
        throws YarnException {
      ctx.addVariable(variable);
    }
  }

  private static DataQueueIndex index() {
    return new DataQueueIndex()
        .add("root", QueueKind.PARENT, false)
        .add("root.static", QueueKind.PARENT, false)
        .add("root.static.sleaf", QueueKind.LEAF, false)
        .add("root.leaf", QueueKind.LEAF, false)
        .add("root.managed", QueueKind.MANAGED_PARENT, false)
        .add("root.v2", QueueKind.PARENT, true)
        // an existing dynamic parent created under root.v2
        .add("root.v2.dyn", QueueKind.PARENT, true)
        .add("root.plan", QueueKind.OTHER_PARENT, false)
        .add("root.x.dup", QueueKind.LEAF, false)
        .add("root.y.dup", QueueKind.LEAF, false);
  }

  private static MappingRuleValidationContext context(QueueIndex index)
      throws YarnException {
    MappingRuleValidationContext ctx =
        new MappingRuleValidationContextImpl(index);
    ctx.addImmutableVariable("%user");
    ctx.addImmutableVariable("%primary_group");
    ctx.addVariable("%default");
    return ctx;
  }

  private static String checkMappingRule(MappingRule rule,
      MappingRuleValidationContext ctx) {
    try {
      rule.validate(ctx);
      return null;
    } catch (YarnException e) {
      return e.getMessage();
    }
  }

  private static String checkTarget(String target) throws YarnException {
    MappingRule rule = new MappingRule(MappingRuleMatchers.createAllMatcher(),
        MappingRuleActions.createPlaceToQueueAction(target, true));
    return checkMappingRule(rule, context(index()));
  }

  @Test
  public void testDuplicatePlacementRules() throws IOException {
    assertNull(PlacementRuleChecks.checkDuplicatePlacementRules(
        new PlacementRuleNames(Arrays.asList("user-group", "app-name"))));
    assertEquals("Invalid PlacementRule inputs which contains duplicate rule"
            + " strings",
        PlacementRuleChecks.checkDuplicatePlacementRules(new PlacementRuleNames(
            Arrays.asList("user-group", "app-name", "user-group"))));

    Set<String> rules = CapacitySchedulerConfigValidator.validatePlacementRules(
        Arrays.asList("b", "a"));
    assertEquals(Arrays.asList("b", "a"), Arrays.asList(rules.toArray()));
    IOException e = assertThrows(IOException.class,
        () -> CapacitySchedulerConfigValidator.validatePlacementRules(
            Arrays.asList("a", "a")));
    assertEquals("Invalid PlacementRule inputs which contains duplicate rule"
        + " strings", e.getMessage());
  }

  @Test
  public void testValidTargets() throws YarnException {
    assertNull(checkTarget("root.leaf"));
    assertNull(checkTarget("leaf"));
    assertNull(checkTarget("static.sleaf"));
    assertNull(checkTarget("root.managed.new"));
    assertNull(checkTarget("root.v2.new"));
    // two levels below an AQC v2 parent
    assertNull(checkTarget("root.v2.new.newer"));
    // below an existing dynamic parent, and two levels below it
    assertNull(checkTarget("root.v2.dyn.new"));
    assertNull(checkTarget("dyn.new.newer"));
    assertNull(checkTarget("root.%user"));
    assertNull(checkTarget("root.v2.%user.%primary_group"));
    assertNull(checkTarget("root.managed.%user"));
    assertNull(checkTarget("dyn.%user"));
    assertNull(checkTarget("%user"));
  }

  @Test
  public void testEmptyTarget() throws YarnException {
    assertEquals("Queue path is empty.", checkTarget(""));
    assertEquals("Path segment cannot be empty 'root..a'.",
        checkTarget("root..a"));
  }

  @Test
  public void testUnknownOrAmbiguousPathRoot() throws YarnException {
    assertEquals("Path root 'missing' does not exist. Path 'missing.a' is"
        + " invalid", checkTarget("missing.a"));
    assertEquals("Path root 'dup' is ambiguous. Path 'dup.a' is invalid",
        checkTarget("dup.a"));
    // only the static prefix of a dynamic target is reported
    assertEquals("Path root 'missing' does not exist. Path 'missing' is"
        + " invalid", checkTarget("missing.%user"));
  }

  @Test
  public void testStaticTargetNotCreatable() throws YarnException {
    assertEquals("Mapping rule specified a parent queue 'root.static', but it"
            + " is not a dynamic parent queue, and no queue exists with name"
            + " 'missing' under it.",
        checkTarget("root.static.missing"));
    assertEquals("Mapping rule specified a parent queue 'root.leaf', but it"
            + " is not a dynamic parent queue, and no queue exists with name"
            + " 'missing' under it.",
        checkTarget("root.leaf.missing"));
    // a plan queue parent is not a dynamic parent
    assertEquals("Mapping rule specified a parent queue 'root.plan', but it"
            + " is not a dynamic parent queue, and no queue exists with name"
            + " 'missing' under it.",
        checkTarget("root.plan.missing"));
    // a managed parent allows one level only
    assertEquals("Mapping rule specified a parent queue 'root.managed.a', but"
            + " it is not a dynamic parent queue, and no queue exists with"
            + " name 'b' under it.",
        checkTarget("root.managed.a.b"));
  }

  @Test
  public void testStaticTargetNotLeaf() throws YarnException {
    assertEquals("Target queue 'root' but it's not a leaf queue.",
        checkTarget("root"));
    assertEquals("Target queue 'root.static' but it's not a leaf queue.",
        checkTarget("root.static"));
    assertEquals("Target queue 'root.v2.dyn' but it's not a leaf queue.",
        checkTarget("root.v2.dyn"));
  }

  @Test
  public void testDynamicTargets() throws YarnException {
    assertEquals("Queue path 'root.leaf.%user' is invalid because 'root.leaf'"
            + " is a leaf queue, which can have no other queues under it.",
        checkTarget("root.leaf.%user"));
    assertEquals("No eligible parent found on path"
        + " 'root.static.missing.%user'.",
        checkTarget("root.static.missing.%user"));
    assertEquals("No eligible parent found on path 'root.plan.x.%user'.",
        checkTarget("root.plan.x.%user"));
  }

  @Test
  public void testImmutableVariable() throws YarnException {
    MappingRule rule = new MappingRule(MappingRuleMatchers.createAllMatcher(),
        new SetVariableAction("%user"));
    assertEquals("Variable '%user' is immutable cannot add to the modified"
            + " variable list.",
        checkMappingRule(rule, context(index())));
    MappingRule custom = new MappingRule(MappingRuleMatchers.createAllMatcher(),
        new SetVariableAction("%custom"));
    assertNull(checkMappingRule(custom, context(index())));
  }

  @Test
  public void testQueueRefOfLiveQueues() {
    assertNull(QueueRef.of(null));

    AbstractLeafQueue leaf = mock(LeafQueue.class);
    when(leaf.getQueuePath()).thenReturn("root.leaf");
    QueueRef leafRef = QueueRef.of(leaf);
    assertEquals("root.leaf", leafRef.getQueuePath());
    assertEquals(QueueKind.LEAF, leafRef.getKind());
    assertTrue(leafRef.isLeaf());
    assertFalse(leafRef.isParent());

    ParentQueue parent = mock(ParentQueue.class);
    when(parent.isEligibleForAutoQueueCreation()).thenReturn(true);
    QueueRef parentRef = QueueRef.of(parent);
    assertEquals(QueueKind.PARENT, parentRef.getKind());
    assertTrue(parentRef.isEligibleForAutoQueueCreation());
    assertTrue(parentRef.isParent());

    assertEquals(QueueKind.MANAGED_PARENT,
        QueueRef.of(mock(ManagedParentQueue.class)).getKind());
    assertEquals(QueueKind.OTHER_PARENT,
        QueueRef.of(mock(PlanQueue.class)).getKind());
    assertEquals(QueueKind.OTHER, QueueRef.of(mock(CSQueue.class)).getKind());
  }

  @Test
  public void testQueueIndexOfQueueManager() {
    CapacitySchedulerQueueManager qm = mock(CapacitySchedulerQueueManager.class);
    AbstractLeafQueue leaf = mock(LeafQueue.class);
    when(leaf.getQueuePath()).thenReturn("root.a");
    when(qm.getQueue("a")).thenReturn(leaf);
    when(qm.isAmbiguous("b")).thenReturn(true);

    QueueIndex queueIndex = PlacementRuleChecks.queueIndexOf(qm);
    assertEquals("root.a", queueIndex.getQueue("a").getQueuePath());
    assertNull(queueIndex.getQueue("missing"));
    assertTrue(queueIndex.isAmbiguous("b"));
    assertFalse(queueIndex.isAmbiguous("a"));
    assertSame(QueueKind.LEAF, queueIndex.getQueue("a").getKind());
  }
}
