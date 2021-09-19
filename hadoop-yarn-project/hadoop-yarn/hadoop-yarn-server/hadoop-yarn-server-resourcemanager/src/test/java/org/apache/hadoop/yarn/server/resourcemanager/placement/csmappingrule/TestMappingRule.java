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

package org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;
import org.junit.Test;

public class TestMappingRule {
  VariableContext setupVariables(
      String user, String group, String secGroup, String appName) {
    VariableContext variables = new VariableContext();
    variables.put("%default", "root.default");
    variables.put("%user", user);
    variables.put("%primary_group", group);
    variables.put("%secondary_group", secGroup);
    variables.put("%application", appName);
    variables.put("%sub", "xxx");
    variables.put("%empty", "");
    variables.put("%null", null);
    variables.setImmutables("%user", "%primary_group", "%secondary_group",
        "%application");

    return variables;
  }

  void assertSkipResult(MappingRuleResult result) {
    assertTrue(
        MappingRuleResultType.SKIP == result.getResult());
  }

  void assertPlaceResult(MappingRuleResult result, String queue) {
    assertTrue(
        MappingRuleResultType.PLACE == result.getResult());
    assertEquals(queue, result.getQueue());
  }

  @Test
  public void testMappingRuleEvaluation() {
    VariableContext matching = setupVariables(
        "bob", "developer", "users", "MR");
    VariableContext mismatching = setupVariables(
        "joe", "tester", "admins", "Spark");

    MappingRule rule = new MappingRule(
        MappingRuleMatchers.createUserMatcher("bob"),
        (new MappingRuleActions.PlaceToQueueAction("%default.%default", true))
            .setFallbackSkip()
    );

    assertSkipResult(rule.getFallback());

    MappingRuleResult matchingResult = rule.evaluate(matching);
    MappingRuleResult mismatchingResult = rule.evaluate(mismatching);

    assertSkipResult(mismatchingResult);
    assertPlaceResult(matchingResult, "root.default.root.default");
  }

  MappingRule createMappingRuleFromLegacyString(String legacyMapping) {
    String[] mapping =
        StringUtils
            .getTrimmedStringCollection(legacyMapping, ":")
            .toArray(new String[] {});

    if (mapping.length == 2) {
      return MappingRule.createLegacyRule(mapping[0], mapping[1]);
    }

    return MappingRule.createLegacyRule(mapping[0], mapping[1], mapping[2]);
  }

  void evaluateLegacyStringTestcase(
      String legacyString, VariableContext variables, String expectedQueue) {
    MappingRule rule = createMappingRuleFromLegacyString(legacyString);
    MappingRuleResult result = rule.evaluate(variables);
    assertEquals(
        rule.getFallback().getResult(), MappingRuleResultType.PLACE_TO_DEFAULT);

    if (expectedQueue == null) {
      assertSkipResult(result);
      return;
    }

    assertPlaceResult(result, expectedQueue);
  }

  @Test
  public void testLegacyEvaluation() {
    VariableContext matching = setupVariables(
        "bob", "developer", "users", "MR");
    matching.putExtraDataset("groups", Sets.newHashSet("developer"));
    VariableContext mismatching = setupVariables(
        "joe", "tester", "admins", "Spark");

    evaluateLegacyStringTestcase(
        "u:bob:root.%primary_group", matching, "root.developer");
    evaluateLegacyStringTestcase(
        "u:bob:root.%primary_group", mismatching, null);
    evaluateLegacyStringTestcase(
        "g:developer:%secondary_group.%user", matching, "users.bob");
    evaluateLegacyStringTestcase(
        "g:developer:%secondary_group.%user", mismatching, null);
    evaluateLegacyStringTestcase(
        "MR:root.static", matching, "root.static");
    evaluateLegacyStringTestcase(
        "MR:root.static", mismatching, null);

    //catch all tests
    evaluateLegacyStringTestcase(
        "u:%user:root.%primary_group", matching, "root.developer");
    evaluateLegacyStringTestcase(
        "u:%user:root.%primary_group", mismatching, "root.tester");
  }

  @Test
  public void testToStrings() {
    MappingRuleAction action = new MappingRuleActions.PlaceToQueueAction(
        "queue", true);
    MappingRuleMatcher matcher = MappingRuleMatchers.createUserMatcher("bob");
    MappingRule rule = new MappingRule(matcher, action);

    assertEquals("MappingRule{matcher=" + matcher.toString() +
        ", action=" + action.toString() + "}", rule.toString());
  }
}