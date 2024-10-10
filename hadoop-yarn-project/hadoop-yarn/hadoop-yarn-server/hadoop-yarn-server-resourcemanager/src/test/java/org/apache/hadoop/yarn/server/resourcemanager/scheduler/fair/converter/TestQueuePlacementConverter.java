/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.DefaultPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.FSPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PrimaryGroupPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.RejectPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.SecondaryGroupExistingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.SpecifiedPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.FallbackResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Policy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Type;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for QueuePlacementConverter.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class TestQueuePlacementConverter {
  private static final String DEFAULT_QUEUE = "root.default";

  @Mock
  private PlacementManager placementManager;

  @Mock
  private FSConfigToCSConfigRuleHandler ruleHandler;

  private QueuePlacementConverter converter;

  private CapacitySchedulerConfiguration csConf;

  @Before
  public void setup() {
    this.converter = new QueuePlacementConverter();
    this.csConf = new CapacitySchedulerConfiguration(
        new Configuration(false));
  }

  @Test
  public void testConvertUserRule() {
    PlacementRule fsRule = mock(UserPlacementRule.class);
    initPlacementManagerMock(fsRule);

    MappingRulesDescription description = convert();
    assertEquals("Number of rules", 1, description.getRules().size());
    verifyRule(description.getRules().get(0), Policy.USER);
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertSpecifiedRule() {
    PlacementRule fsRule = mock(SpecifiedPlacementRule.class);
    initPlacementManagerMock(fsRule);

    MappingRulesDescription description = convert();
    assertEquals("Number of rules", 1, description.getRules().size());
    verifyRule(description.getRules().get(0), Policy.SPECIFIED);
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertPrimaryGroupRule() {
    PlacementRule fsRule = mock(PrimaryGroupPlacementRule.class);
    initPlacementManagerMock(fsRule);

    MappingRulesDescription description = convert();

    assertEquals("Number of rules", 1, description.getRules().size());
    verifyRule(description.getRules().get(0), Policy.PRIMARY_GROUP);
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertSecondaryGroupRule() {
    PlacementRule fsRule = mock(SecondaryGroupExistingPlacementRule.class);
    initPlacementManagerMock(fsRule);

    MappingRulesDescription description = convert();

    assertEquals("Number of rules", 1, description.getRules().size());
    verifyRule(description.getRules().get(0), Policy.SECONDARY_GROUP);
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertDefaultRuleWithQueueName() {
    DefaultPlacementRule fsRule = mock(DefaultPlacementRule.class);
    fsRule.defaultQueueName = "abc";
    initPlacementManagerMock(fsRule);

    MappingRulesDescription description = convert();

    assertEquals("Number of rules", 1, description.getRules().size());

    verifyRule(description.getRules().get(0), Policy.CUSTOM);
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertDefaultRule() {
    DefaultPlacementRule fsRule = mock(DefaultPlacementRule.class);
    fsRule.defaultQueueName = DEFAULT_QUEUE;
    initPlacementManagerMock(fsRule);

    MappingRulesDescription description = convert();

    assertEquals("Number of rules", 1, description.getRules().size());
    verifyRule(description.getRules().get(0), Policy.DEFAULT_QUEUE);
    verifyZeroInteractions(ruleHandler);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertUnsupportedRule() {
    PlacementRule rule = mock(TestPlacementRule.class);
    initPlacementManagerMock(rule);

    // throws exception
    convert();
  }

  @Test
  public void testConvertRejectRule() {
    PlacementRule rule = mock(RejectPlacementRule.class);
    initPlacementManagerMock(rule);

    MappingRulesDescription description = convert();

    assertEquals("Number of rules", 1, description.getRules().size());
    verifyRule(description.getRules().get(0), Policy.REJECT);
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertNestedPrimaryGroupRule() {
    UserPlacementRule rule = mock(UserPlacementRule.class);
    PrimaryGroupPlacementRule parent = mock(PrimaryGroupPlacementRule.class);
    when(rule.getParentRule()).thenReturn(parent);
    initPlacementManagerMock(rule);

    MappingRulesDescription description = convert();

    assertEquals("Number of rules", 1, description.getRules().size());
    verifyRule(description.getRules().get(0), Policy.PRIMARY_GROUP_USER);
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertNestedSecondaryGroupRule() {
    UserPlacementRule rule = mock(UserPlacementRule.class);
    SecondaryGroupExistingPlacementRule parent =
        mock(SecondaryGroupExistingPlacementRule.class);
    when(rule.getParentRule()).thenReturn(parent);
    initPlacementManagerMock(rule);

    MappingRulesDescription description = convert();

    assertEquals("Number of rules", 1, description.getRules().size());
    verifyRule(description.getRules().get(0), Policy.SECONDARY_GROUP_USER);
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertNestedDefaultRule() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    DefaultPlacementRule parent =
        mock(DefaultPlacementRule.class);
    parent.defaultQueueName = "root.abc";
    when(fsRule.getParentRule()).thenReturn(parent);
    initPlacementManagerMock(fsRule);

    MappingRulesDescription description = convert();

    assertEquals("Number of rules", 1, description.getRules().size());
    Rule rule = description.getRules().get(0);
    verifyRule(description.getRules().get(0), Policy.USER);
    assertEquals("Parent path", "root.abc", rule.getParentQueue());
    verifyZeroInteractions(ruleHandler);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnsupportedNestedParentRule() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    TestPlacementRule parent =
        mock(TestPlacementRule.class);
    when(fsRule.getParentRule()).thenReturn(parent);
    initPlacementManagerMock(fsRule);

    // throws exception
    convert();
  }

  @Test
  public void testConvertMultiplePlacementRules() {
    UserPlacementRule rule1 = mock(UserPlacementRule.class);
    PrimaryGroupPlacementRule rule2 =
        mock(PrimaryGroupPlacementRule.class);
    SecondaryGroupExistingPlacementRule rule3 =
        mock(SecondaryGroupExistingPlacementRule.class);
    initPlacementManagerMock(rule1, rule2, rule3);

    MappingRulesDescription description = convert();

    assertEquals("Number of rules", 3, description.getRules().size());
    verifyRule(description.getRules().get(0), Policy.USER);
    verifyRule(description.getRules().get(1), Policy.PRIMARY_GROUP);
    verifyRule(description.getRules().get(2), Policy.SECONDARY_GROUP);
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertPrimaryGroupRuleWithCreate() {
    FSPlacementRule fsRule = mock(PrimaryGroupPlacementRule.class);
    when(fsRule.getCreateFlag()).thenReturn(true);
    initPlacementManagerMock(fsRule);

    convert();

    verify(ruleHandler).handleRuleAutoCreateFlag(eq("root.<primaryGroup>"));
    verifyNoMoreInteractions(ruleHandler);
  }

  @Test
  public void testConvertSecondaryGroupRuleWithCreate() {
    FSPlacementRule fsRule = mock(SecondaryGroupExistingPlacementRule.class);
    when(fsRule.getCreateFlag()).thenReturn(true);
    initPlacementManagerMock(fsRule);

    convert();

    verify(ruleHandler).handleRuleAutoCreateFlag(eq("root.<secondaryGroup>"));
    verifyNoMoreInteractions(ruleHandler);
  }

  @Test
  public void testConvertNestedPrimaryGroupRuleWithCreate() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    PrimaryGroupPlacementRule parent = mock(PrimaryGroupPlacementRule.class);
    when(fsRule.getParentRule()).thenReturn(parent);
    when(fsRule.getCreateFlag()).thenReturn(true);
    initPlacementManagerMock(fsRule);

    convert();

    verify(ruleHandler).handleRuleAutoCreateFlag(eq("root.<primaryGroup>"));
    verifyNoMoreInteractions(ruleHandler);
  }

  @Test
  public void testConvertNestedSecondaryGroupRuleWithCreate() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    SecondaryGroupExistingPlacementRule parent =
        mock(SecondaryGroupExistingPlacementRule.class);
    when(fsRule.getParentRule()).thenReturn(parent);
    when(fsRule.getCreateFlag()).thenReturn(true);
    initPlacementManagerMock(fsRule);

    convert();

    verify(ruleHandler).handleRuleAutoCreateFlag(eq("root.<secondaryGroup>"));
    verifyNoMoreInteractions(ruleHandler);
  }

  @Test
  public void testConvertNestedDefaultGroupWithCreate() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    DefaultPlacementRule parent =
        mock(DefaultPlacementRule.class);
    parent.defaultQueueName = "root.abc";
    when(fsRule.getParentRule()).thenReturn(parent);
    when(fsRule.getCreateFlag()).thenReturn(true);
    initPlacementManagerMock(fsRule);

    convert();

    verify(ruleHandler).handleRuleAutoCreateFlag(eq("root.abc"));
    verifyNoMoreInteractions(ruleHandler);
  }

  @Test
  public void testConvertNestedRuleCreateFalseFalseInWeightMode() {
    testConvertNestedRuleCreateFlagInWeightMode(false, false,
        false, false);
  }

  @Test
  public void testConvertNestedRuleCreateFalseTrueInWeightMode() {
    testConvertNestedRuleCreateFlagInWeightMode(false, true,
        true, true);
  }

  @Test
  public void testConvertNestedRuleCreateTrueFalseInWeightMode() {
    testConvertNestedRuleCreateFlagInWeightMode(true, false,
        true, true);
  }

  @Test
  public void testConvertNestedRuleCreateTrueTrueInWeightMode() {
    testConvertNestedRuleCreateFlagInWeightMode(true, true,
        true, false);
  }

  private void testConvertNestedRuleCreateFlagInWeightMode(
      boolean parentCreate,
      boolean childCreate,
      boolean expectedFlagOnRule,
      boolean ruleHandlerShouldBeInvoked) {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    PrimaryGroupPlacementRule parent = mock(PrimaryGroupPlacementRule.class);
    when(parent.getCreateFlag()).thenReturn(parentCreate);
    when(fsRule.getParentRule()).thenReturn(parent);
    when(fsRule.getCreateFlag()).thenReturn(childCreate);
    initPlacementManagerMock(fsRule);

    MappingRulesDescription desc = convertInWeightMode();
    Rule rule = desc.getRules().get(0);

    assertEquals("Expected create flag", expectedFlagOnRule, rule.getCreate());

    if (ruleHandlerShouldBeInvoked) {
      verify(ruleHandler).handleFSParentAndChildCreateFlagDiff(
          any(Policy.class));
      verifyNoMoreInteractions(ruleHandler);
    } else {
      verifyZeroInteractions(ruleHandler);
    }
  }

  @Test
  public void testParentSetToRootInWeightModeUserPolicy() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    testParentSetToRootInWeightMode(fsRule);
  }

  @Test
  public void testParentSetToRootInWeightModePrimaryGroupPolicy() {
    PrimaryGroupPlacementRule fsRule = mock(PrimaryGroupPlacementRule.class);
    testParentSetToRootInWeightMode(fsRule);
  }

  @Test
  public void testParentSetToRootInWeightModePrimaryGroupUserPolicy() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    PrimaryGroupPlacementRule parent = mock(PrimaryGroupPlacementRule.class);
    when(fsRule.getParentRule()).thenReturn(parent);
    testParentSetToRootInWeightMode(fsRule);
  }

  @Test
  public void testParentSetToRootInWeightModeSecondaryGroupPolicy() {
    SecondaryGroupExistingPlacementRule fsRule =
        mock(SecondaryGroupExistingPlacementRule.class);
    testParentSetToRootInWeightMode(fsRule);
  }

  @Test
  public void testParentSetToRootInWeightModeSecondaryGroupUserPolicy() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    SecondaryGroupExistingPlacementRule parent =
        mock(SecondaryGroupExistingPlacementRule.class);
    when(fsRule.getParentRule()).thenReturn(parent);
    testParentSetToRootInWeightMode(fsRule);
  }

  private void testParentSetToRootInWeightMode(FSPlacementRule fsRule) {
    initPlacementManagerMock(fsRule);

    MappingRulesDescription desc = convertInWeightMode();
    Rule rule = desc.getRules().get(0);

    assertEquals("Parent queue", "root", rule.getParentQueue());
  }

  @Test
  public void testConvertNestedPrimaryGroupRuleWithParentCreate() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    PrimaryGroupPlacementRule parent = mock(PrimaryGroupPlacementRule.class);
    when(fsRule.getParentRule()).thenReturn(parent);
    when(parent.getCreateFlag()).thenReturn(true);
    initPlacementManagerMock(fsRule);

    convert();

    verify(ruleHandler).handleFSParentCreateFlag(eq("root.<primaryGroup>"));
    verifyNoMoreInteractions(ruleHandler);
  }

  @Test
  public void testConvertNestedSecondaryGroupRuleWithParentCreate() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    SecondaryGroupExistingPlacementRule parent =
        mock(SecondaryGroupExistingPlacementRule.class);
    when(fsRule.getParentRule()).thenReturn(parent);
    when(parent.getCreateFlag()).thenReturn(true);
    initPlacementManagerMock(fsRule);

    convert();

    verify(ruleHandler).handleFSParentCreateFlag(eq("root.<secondaryGroup>"));
    verifyNoMoreInteractions(ruleHandler);
  }

  @Test
  public void testConvertNestedDefaultGroupWithParentCreate() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    DefaultPlacementRule parent =
        mock(DefaultPlacementRule.class);
    parent.defaultQueueName = "root.abc";
    when(fsRule.getParentRule()).thenReturn(parent);
    when(parent.getCreateFlag()).thenReturn(true);
    initPlacementManagerMock(fsRule);

    convert();

    verify(ruleHandler).handleFSParentCreateFlag(eq("root.abc"));
    verifyNoMoreInteractions(ruleHandler);
  }

  @Test
  public void testConvertNestedDefaultWithConflictingQueues() {
    UserPlacementRule fsRule = mock(UserPlacementRule.class);
    DefaultPlacementRule parent =
        mock(DefaultPlacementRule.class);
    parent.defaultQueueName = "root.users";
    when(fsRule.getParentRule()).thenReturn(parent);
    when(fsRule.getCreateFlag()).thenReturn(true);
    initPlacementManagerMock(fsRule);
    csConf.setQueues(new QueuePath("root.users"), new String[] {"hadoop"});

    convert();

    verify(ruleHandler).handleRuleAutoCreateFlag(eq("root.users"));
    verify(ruleHandler).handleChildStaticDynamicConflict(eq("root.users"));
    verifyNoMoreInteractions(ruleHandler);
  }

  private void initPlacementManagerMock(
      PlacementRule... rules) {
    List<PlacementRule> listOfRules = Lists.newArrayList(rules);
    when(placementManager.getPlacementRules()).thenReturn(listOfRules);
  }

  private MappingRulesDescription convert() {
    return converter.convertPlacementPolicy(placementManager,
        ruleHandler, csConf, true);
  }

  private MappingRulesDescription convertInWeightMode() {
    return converter.convertPlacementPolicy(placementManager,
        ruleHandler, csConf, false);
  }

  private void verifyRule(Rule rule, Policy expectedPolicy) {
    assertEquals("Policy type", expectedPolicy, rule.getPolicy());
    assertEquals("Match string", "*", rule.getMatches());
    assertEquals("Fallback result",
        FallbackResult.SKIP, rule.getFallbackResult());
    assertEquals("Type", Type.USER, rule.getType());
  }

  private class TestPlacementRule extends FSPlacementRule {
    @Override
    public ApplicationPlacementContext getPlacementForApp(
        ApplicationSubmissionContext asc, String user) throws YarnException {
      return null;
    }
  }
}
