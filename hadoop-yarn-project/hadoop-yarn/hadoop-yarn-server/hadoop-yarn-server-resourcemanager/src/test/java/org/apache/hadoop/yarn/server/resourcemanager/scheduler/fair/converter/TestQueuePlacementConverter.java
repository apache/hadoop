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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ENABLE_QUEUE_MAPPING_OVERRIDE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.QUEUE_MAPPING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
 * Unit tests for QueuePlacementConverter.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class TestQueuePlacementConverter {
  @Mock
  private PlacementManager placementManager;

  @Mock
  private FSConfigToCSConfigRuleHandler ruleHandler;

  private QueuePlacementConverter converter;

  @Before
  public void setup() {
    this.converter = new QueuePlacementConverter();
  }

  @Test
  public void testConvertUserAsDefaultQueue() {
    Map<String, String> properties = convert(true);

    verifyMapping(properties, "u:%user:%user");
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertUserPlacementRuleWithoutUserAsDefaultQueue() {
    testConvertUserPlacementRule(false);
  }

  @Test
  public void testConvertUserPlacementRuleWithUserAsDefaultQueue() {
    testConvertUserPlacementRule(true);
  }

  private void testConvertUserPlacementRule(boolean userAsDefaultQueue) {
    PlacementRule rule = mock(UserPlacementRule.class);
    initPlacementManagerMock(rule);

    Map<String, String> properties = convert(userAsDefaultQueue);

    verifyMapping(properties, "u:%user:%user");
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertSpecifiedPlacementRule() {
    PlacementRule rule = mock(SpecifiedPlacementRule.class);
    initPlacementManagerMock(rule);

    Map<String, String> properties = convert(false);

    verifyMappingNoOverride(properties, 1);
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertSpecifiedPlacementRuleAtSecondPlace() {
    PlacementRule rule = mock(UserPlacementRule.class);
    PlacementRule rule2 = mock(SpecifiedPlacementRule.class);
    initPlacementManagerMock(rule, rule2);

    Map<String, String> properties = convert(false);

    verifyMappingNoOverride(properties, 2);
    verify(ruleHandler).handleSpecifiedNotFirstRule();
  }

  @Test
  public void testConvertPrimaryGroupPlacementRule() {
    PlacementRule rule = mock(PrimaryGroupPlacementRule.class);
    initPlacementManagerMock(rule);

    Map<String, String> properties = convert(false);

    verifyMapping(properties, "u:%user:%primary_group");
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertSecondaryGroupPlacementRule() {
    PlacementRule rule = mock(SecondaryGroupExistingPlacementRule.class);
    initPlacementManagerMock(rule);

    Map<String, String> properties = convert(false);

    verifyMapping(properties, "u:%user:%secondary_group");
    verifyZeroInteractions(ruleHandler);
  }

  @Test
  public void testConvertDefaultPlacementRule() {
    DefaultPlacementRule rule = mock(DefaultPlacementRule.class);
    rule.defaultQueueName = "abc";
    initPlacementManagerMock(rule);

    Map<String, String> properties = convert(false);

    verifyMapping(properties, "u:%user:abc");
    verifyZeroInteractions(ruleHandler);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertUnsupportedPlacementRule() {
    PlacementRule rule = mock(TestPlacementRule.class);
    initPlacementManagerMock(rule);

    // throws exception
    convert(false);
  }

  @Test
  public void testConvertRejectPlacementRule() {
    PlacementRule rule = mock(RejectPlacementRule.class);
    initPlacementManagerMock(rule);

    Map<String, String> properties = convert(false);

    assertEquals("Map is not empty", 0, properties.size());
  }

  @Test
  public void testConvertNestedPrimaryGroupRule() {
    UserPlacementRule rule = mock(UserPlacementRule.class);
    PrimaryGroupPlacementRule parent = mock(PrimaryGroupPlacementRule.class);
    when(rule.getParentRule()).thenReturn(parent);
    initPlacementManagerMock(rule);

    Map<String, String> properties = convert(false);

    verifyMapping(properties, "u:%user:%primary_group.%user");
    verify(ruleHandler).handleDynamicMappedQueue(
        eq("u:%user:%primary_group.%user"), eq(false));
  }

  @Test
  public void testConvertNestedSecondaryGroupRule() {
    UserPlacementRule rule = mock(UserPlacementRule.class);
    SecondaryGroupExistingPlacementRule parent =
        mock(SecondaryGroupExistingPlacementRule.class);
    when(rule.getParentRule()).thenReturn(parent);
    initPlacementManagerMock(rule);

    Map<String, String> properties = convert(false);

    verifyMapping(properties, "u:%user:%secondary_group.%user");
    verify(ruleHandler).handleDynamicMappedQueue(
        eq("u:%user:%secondary_group.%user"), eq(false));
  }

  @Test
  public void testConvertNestedDefaultRule() {
    UserPlacementRule rule = mock(UserPlacementRule.class);
    DefaultPlacementRule parent =
        mock(DefaultPlacementRule.class);
    parent.defaultQueueName = "abc";
    when(rule.getParentRule()).thenReturn(parent);
    initPlacementManagerMock(rule);

    Map<String, String> properties = convert(false);

    verifyMapping(properties, "u:%user:abc.%user");
    verify(ruleHandler).handleDynamicMappedQueue(
        eq("u:%user:abc.%user"), eq(false));
  }

  @Test
  public void testConvertMultiplePlacementRules() {
    UserPlacementRule rule1 = mock(UserPlacementRule.class);
    PrimaryGroupPlacementRule rule2 =
        mock(PrimaryGroupPlacementRule.class);
    SecondaryGroupExistingPlacementRule rule3 =
        mock(SecondaryGroupExistingPlacementRule.class);
    initPlacementManagerMock(rule1, rule2, rule3);

    Map<String, String> properties = convert(false);

    verifyMapping(properties,
        "u:%user:%user,u:%user:%primary_group,u:%user:%secondary_group");
    verifyZeroInteractions(ruleHandler);
  }

  private void initPlacementManagerMock(
      PlacementRule... rules) {
    List<PlacementRule> listOfRules = Lists.newArrayList(rules);
    when(placementManager.getPlacementRules()).thenReturn(listOfRules);
  }

  private Map<String, String> convert(boolean userAsDefaultQueue) {
    return converter.convertPlacementPolicy(placementManager, ruleHandler,
        userAsDefaultQueue);
  }

  private void verifyMapping(Map<String, String> properties,
      String expectedValue) {
    assertEquals("Map size", 1, properties.size());
    String value = properties.get(QUEUE_MAPPING);
    assertNotNull("No mapping property found", value);
    assertEquals("Mapping", expectedValue, value);
  }

  private void verifyMappingNoOverride(Map<String, String> properties,
      int expectedSize) {
    assertEquals("Map size", expectedSize, properties.size());
    String value = properties.get(ENABLE_QUEUE_MAPPING_OVERRIDE);
    assertNotNull("No mapping property found", value);
    assertEquals("Override mapping", "false", value);
  }

  private class TestPlacementRule extends FSPlacementRule {
    @Override
    public ApplicationPlacementContext getPlacementForApp(
        ApplicationSubmissionContext asc, String user) throws YarnException {
      return null;
    }
  }
}
