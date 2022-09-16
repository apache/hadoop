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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleResult;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleResultType;
import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.FallbackResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Policy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Type;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestMappingRuleCreator {
  private static final String MATCH_ALL = "*";

  private static final String DEFAULT_QUEUE = "root.default";
  private static final String SECONDARY_GROUP = "users";
  private static final String PRIMARY_GROUP = "superuser";
  private static final String APPLICATION_NAME = "testapplication";
  private static final String SPECIFIED_QUEUE = "root.users.hadoop";
  private static final String USER_NAME = "testuser";

  private MappingRuleCreator ruleCreator;

  private VariableContext variableContext;
  private MappingRulesDescription description;
  private Rule rule;

  @BeforeEach
  public void setup() {
    ruleCreator = new MappingRuleCreator();
    prepareMappingRuleDescription();
    variableContext = new VariableContext();

    variableContext.put("%user", USER_NAME);
    variableContext.put("%specified", SPECIFIED_QUEUE);
    variableContext.put("%application", APPLICATION_NAME);
    variableContext.put("%primary_group", PRIMARY_GROUP);
    variableContext.put("%secondary_group", SECONDARY_GROUP);
    variableContext.put("%default", DEFAULT_QUEUE);
    variableContext.putExtraDataset("groups",
        Sets.newLinkedHashSet(PRIMARY_GROUP, SECONDARY_GROUP));
  }

  @Test
  void testAllUserMatcher() {
    variableContext.put("%user", USER_NAME);
    verifyPlacementSucceeds(USER_NAME);

    variableContext.put("%user", "dummyuser");
    verifyPlacementSucceeds("dummyuser");
  }

  @Test
  void testSpecificUserMatcherPasses() {
    rule.setMatches(USER_NAME);

    verifyPlacementSucceeds(USER_NAME);
  }

  @Test
  void testSpecificUserMatcherFails() {
    rule.setMatches(USER_NAME);
    variableContext.put("%user", "dummyuser");

    verifyNoPlacementOccurs();
  }

  @Test
  void testSpecificGroupMatcher() {
    rule.setMatches(PRIMARY_GROUP);
    rule.setType(Type.GROUP);

    verifyPlacementSucceeds();
  }

  @Test
  void testAllGroupMatcherFailsDueToMatchString() {
    rule.setType(Type.GROUP);

    // fails because "*" is not applicable to group type
    final Exception ex = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ruleCreator.getMappingRules(description)
    );
    Assertions.assertEquals(
        ex.getMessage(),
        "Cannot match '*' for groups"
    );
  }

  @Test
  void testApplicationNameMatcherPasses() {
    rule.setType(Type.APPLICATION);
    rule.setMatches(APPLICATION_NAME);

    verifyPlacementSucceeds();
  }

  @Test
  void testApplicationNameMatcherFails() {
    rule.setType(Type.APPLICATION);
    rule.setMatches("dummyApplication");

    verifyNoPlacementOccurs();
  }

  @Test
  void testDefaultRule() {
    rule.setPolicy(Policy.DEFAULT_QUEUE);

    verifyPlacementSucceeds(DEFAULT_QUEUE, false);
  }

  @Test
  void testSpecifiedRule() {
    rule.setPolicy(Policy.SPECIFIED);

    verifyPlacementSucceeds(SPECIFIED_QUEUE);
  }

  @Test
  void testSpecifiedRuleWithNoCreate() {
    rule.setPolicy(Policy.SPECIFIED);
    rule.setCreate(false);

    verifyPlacementSucceeds(SPECIFIED_QUEUE, false);
  }

  @Test
  void testRejectRule() {
    rule.setPolicy(Policy.REJECT);

    verifyPlacementRejected();
  }

  @Test
  void testSetDefaultRule() {
    rule.setPolicy(Policy.SET_DEFAULT_QUEUE);
    rule.setValue("root.users.default");

    verifyNoPlacementOccurs();
    Assertions.assertEquals("root.users.default", variableContext.get("%default"),
        "Default queue");
  }

  @Test
  void testSetDefaultRuleWithMissingQueue() {
    rule.setPolicy(Policy.SET_DEFAULT_QUEUE);

    final Exception ex = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ruleCreator.getMappingRules(description)
    );
    Assertions.assertEquals(
        ex.getMessage(),
        "default queue is undefined"
    );
  }

  @Test
  void testPrimaryGroupRule() {
    rule.setPolicy(Policy.PRIMARY_GROUP);

    verifyPlacementSucceeds(PRIMARY_GROUP);
  }

  @Test
  void testPrimaryGroupRuleWithNoCreate() {
    rule.setPolicy(Policy.PRIMARY_GROUP);
    rule.setCreate(false);

    verifyPlacementSucceeds(PRIMARY_GROUP, false);
  }

  @Test
  void testPrimaryGroupRuleWithParent() {
    rule.setPolicy(Policy.PRIMARY_GROUP);
    rule.setParentQueue("root");

    verifyPlacementSucceeds("root." + PRIMARY_GROUP);
  }

  @Test
  void testSecondaryGroupRule() {
    rule.setPolicy(Policy.SECONDARY_GROUP);

    verifyPlacementSucceeds(SECONDARY_GROUP);
  }

  @Test
  void testSecondaryGroupRuleWithNoCreate() {
    rule.setPolicy(Policy.SECONDARY_GROUP);
    rule.setCreate(false);

    verifyPlacementSucceeds(SECONDARY_GROUP, false);
  }

  @Test
  void testSecondaryGroupRuleWithParent() {
    rule.setPolicy(Policy.SECONDARY_GROUP);
    rule.setParentQueue("root");

    verifyPlacementSucceeds("root." + SECONDARY_GROUP);
  }

  @Test
  void testUserRule() {
    rule.setPolicy(Policy.USER);

    verifyPlacementSucceeds(USER_NAME);
  }

  @Test
  void testUserRuleWithParent() {
    rule.setPolicy(Policy.USER);
    rule.setParentQueue("root.users");

    verifyPlacementSucceeds("root.users." + USER_NAME);
  }

  @Test
  void testCustomRule() {
    rule.setPolicy(Policy.CUSTOM);
    rule.setCustomPlacement("root.%primary_group.%secondary_group");

    verifyPlacementSucceeds(
        String.format("root.%s.%s", PRIMARY_GROUP, SECONDARY_GROUP));
  }

  @Test
  void testCustomRuleWithNoCreate() {
    rule.setPolicy(Policy.CUSTOM);
    rule.setCustomPlacement("root.%primary_group.%secondary_group");
    rule.setCreate(false);

    verifyPlacementSucceeds(
        String.format("root.%s.%s", PRIMARY_GROUP, SECONDARY_GROUP), false);
  }

  @Test
  void testCustomRuleWithMissingQueue() {
    rule.setPolicy(Policy.CUSTOM);

    final Exception ex = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ruleCreator.getMappingRules(description)
    );
    Assertions.assertEquals(
        ex.getMessage(),
        "custom queue is undefined"
    );
  }

  @Test
  void testPrimaryGroupUserRule() {
    rule.setPolicy(Policy.PRIMARY_GROUP_USER);

    verifyPlacementSucceeds("superuser.testuser");
  }

  @Test
  void testPrimaryGroupUserRuleWithNoCreate() {
    rule.setPolicy(Policy.PRIMARY_GROUP_USER);
    rule.setCreate(false);

    verifyPlacementSucceeds("superuser.testuser", false);
  }

  @Test
  void testPrimaryGroupNestedRuleWithParent() {
    rule.setPolicy(Policy.PRIMARY_GROUP_USER);
    rule.setParentQueue("root");

    verifyPlacementSucceeds("root.superuser.testuser");
  }

  @Test
  void testSecondaryGroupNestedRule() {
    rule.setPolicy(Policy.SECONDARY_GROUP_USER);

    verifyPlacementSucceeds("users.testuser");
  }

  @Test
  void testSecondaryGroupNestedRuleWithNoCreate() {
    rule.setPolicy(Policy.SECONDARY_GROUP_USER);
    rule.setCreate(false);

    verifyPlacementSucceeds("users.testuser", false);
  }

  @Test
  void testSecondaryGroupNestedRuleWithParent() {
    rule.setPolicy(Policy.SECONDARY_GROUP_USER);
    rule.setParentQueue("root");

    verifyPlacementSucceeds("root.users.testuser");
  }

  @Test
  void testApplicationNamePlacement() {
    rule.setPolicy(Policy.APPLICATION_NAME);

    verifyPlacementSucceeds(APPLICATION_NAME);
  }

  @Test
  void testApplicationNamePlacementWithParent() {
    rule.setPolicy(Policy.APPLICATION_NAME);
    rule.setParentQueue("root.applications");

    verifyPlacementSucceeds("root.applications." + APPLICATION_NAME);
  }

  @Test
  void testDefaultQueueFallback() {
    rule.setFallbackResult(FallbackResult.PLACE_DEFAULT);

    testFallback(MappingRuleResultType.PLACE_TO_DEFAULT);
  }

  @Test
  void testRejectFallback() {
    rule.setFallbackResult(FallbackResult.REJECT);

    testFallback(MappingRuleResultType.REJECT);
  }

  @Test
  void testSkipFallback() {
    rule.setFallbackResult(FallbackResult.SKIP);

    testFallback(MappingRuleResultType.SKIP);
  }

  private void testFallback(MappingRuleResultType expectedType) {
    List<MappingRule> rules = ruleCreator.getMappingRules(description);
    MappingRule mpr = rules.get(0);

    Assertions.assertEquals(
        expectedType,
        mpr.getFallback().getResult(),
        "Fallback result"
    );
  }

  @Test
  void testFallbackResultUnset() {
    rule.setFallbackResult(null);
    List<MappingRule> rules = ruleCreator.getMappingRules(description);
    MappingRule mpr = rules.get(0);
    Assertions.assertEquals(MappingRuleResultType.SKIP, mpr.getFallback().getResult(),
        "Fallback result");
  }

  @Test
  void testTypeUnset() {
    rule.setType(null);

    final Exception ex = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ruleCreator.getMappingRules(description)
    );
    Assertions.assertEquals(
        ex.getMessage(),
        "Rule type is undefined"
    );
  }

  @Test
  void testMatchesUnset() {
    rule.setMatches(null);

    final Exception ex = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ruleCreator.getMappingRules(description)
    );
    Assertions.assertEquals(
        ex.getMessage(),
        "Match string is undefined"
    );
  }

  @Test
  void testMatchesEmpty() {
    rule.setMatches("");

    final Exception ex = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ruleCreator.getMappingRules(description)
    );
    Assertions.assertEquals(
        ex.getMessage(),
        "Match string is empty"
    );
  }

  @Test
  void testPolicyUnset() {
    rule.setPolicy(null);

    final Exception ex = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ruleCreator.getMappingRules(description)
    );
    Assertions.assertEquals(
        ex.getMessage(),
        "Rule policy is undefined"
    );
  }

  private void prepareMappingRuleDescription() {
    description = new MappingRulesDescription();

    rule = new Rule();
    rule.setType(Type.USER);
    rule.setFallbackResult(FallbackResult.SKIP);
    rule.setPolicy(Policy.USER);
    rule.setMatches(MATCH_ALL);

    List<Rule> rules = new ArrayList<>();
    rules.add(rule);

    description.setRules(rules);
  }

  private void verifyPlacementSucceeds() {
    verifyPlacement(MappingRuleResultType.PLACE, null, true);
  }

  private void verifyPlacementSucceeds(String expectedQueue) {
    verifyPlacement(MappingRuleResultType.PLACE, expectedQueue, true);
  }

  private void verifyPlacementSucceeds(String expectedQueue,
      boolean allowCreate) {
    verifyPlacement(MappingRuleResultType.PLACE, expectedQueue,
        allowCreate);
  }

  private void verifyPlacementRejected() {
    verifyPlacement(MappingRuleResultType.REJECT, null, true);
  }

  private void verifyNoPlacementOccurs() {
    verifyPlacement(null, null, true);
  }

  private void verifyPlacement(MappingRuleResultType expectedResultType,
      String expectedQueue, boolean allowCreate) {
    List<MappingRule> rules = ruleCreator.getMappingRules(description);
    Assertions.assertEquals(1, rules.size(), "Number of rules");
    MappingRule mpr = rules.get(0);
    MappingRuleResult result = mpr.evaluate(variableContext);

    Assertions.assertEquals(allowCreate, result.isCreateAllowed(), "Create flag");

    if (expectedResultType != null) {
      Assertions.assertEquals(expectedResultType,
          result.getResult(), "Mapping rule result");
    } else {
      Assertions.assertEquals(MappingRuleResultType.SKIP,
          result.getResult(), "Mapping rule result");
    }

    if (expectedQueue != null) {
      Assertions.assertEquals(expectedQueue, result.getQueue(), "Evaluated queue");
    }
  }
}
