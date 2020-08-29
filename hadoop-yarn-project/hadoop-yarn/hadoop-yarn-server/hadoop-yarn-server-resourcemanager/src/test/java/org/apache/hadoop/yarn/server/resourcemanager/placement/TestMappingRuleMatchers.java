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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestMappingRuleMatchers extends TestCase {

  @Test
  public void testCatchAll() {
    VariableContext variables = new VariableContext();
    MappingRuleMatcher matcher = new MappingRuleMatchers.MatchAllMatcher();

    assertTrue(matcher.match(variables));
  }

  @Test
  public void testVariableMatcher() {
    VariableContext matchingContext = new VariableContext();
    matchingContext.put("%user", "bob");
    matchingContext.put("%primary_group", "developers");
    matchingContext.put("%application", "TurboMR");
    matchingContext.put("%custom", "Matching string");

    VariableContext mismatchingContext = new VariableContext();
    mismatchingContext.put("%user", "dave");
    mismatchingContext.put("%primary_group", "testers");
    mismatchingContext.put("%application", "Tester APP");
    mismatchingContext.put("%custom", "Not matching string");

    VariableContext emptyContext = new VariableContext();

    Map<String, MappingRuleMatcher> matchers = new HashMap<>();
    matchers.put("User matcher", MappingRuleMatchers.createUserMatcher("bob"));
    matchers.put("Group matcher",
        MappingRuleMatchers.createGroupMatcher("developers"));
    matchers.put("Application name matcher",
        MappingRuleMatchers.createApplicationNameMatcher("TurboMR"));
    matchers.put("Custom matcher",
        new MappingRuleMatchers.VariableMatcher("%custom", "Matching string"));

    matchers.forEach((matcherName, matcher) -> {
      assertTrue(matcherName + " with matchingContext should match",
          matcher.match(matchingContext));
      assertFalse(matcherName + " with mismatchingContext shouldn't match",
          matcher.match(mismatchingContext));
      assertFalse(matcherName + " with emptyContext shouldn't match",
          matcher.match(emptyContext));
    });
  }

  @Test
  public void testVariableMatcherSubstitutions() {
    VariableContext matchingContext = new VariableContext();
    matchingContext.put("%user", "bob");
    matchingContext.put("%primary_group", "developers");
    matchingContext.put("%application", "TurboMR");
    matchingContext.put("%custom", "bob");
    matchingContext.put("%cus", "b");
    matchingContext.put("%tom", "ob");

    VariableContext mismatchingContext = new VariableContext();
    mismatchingContext.put("%user", "dave");
    mismatchingContext.put("%primary_group", "testers");
    mismatchingContext.put("%application", "Tester APP");
    mismatchingContext.put("%custom", "bob");
    mismatchingContext.put("%cus", "b");
    mismatchingContext.put("%tom", "ob");

    MappingRuleMatcher customUser =
        new MappingRuleMatchers.VariableMatcher("%custom", "%user");

    MappingRuleMatcher userCusTom =
        new MappingRuleMatchers.VariableMatcher("%user", "%cus%tom");

    MappingRuleMatcher userCustom =
        new MappingRuleMatchers.VariableMatcher("%user", "%custom");

    MappingRuleMatcher userUser =
        new MappingRuleMatchers.VariableMatcher("%user", "%user");

    MappingRuleMatcher userStatic =
        new MappingRuleMatchers.VariableMatcher("%user", "bob");

    assertTrue("%custom should match %user in matching context",
        customUser.match(matchingContext));
    assertTrue("%user should match %custom in matching context",
        userCustom.match(matchingContext));
    assertTrue("%user (bob) should match %cus%tom (b + ob) in matching context",
        userCusTom.match(matchingContext));
    assertTrue("%user should match %user in any context",
        userUser.match(matchingContext));
    assertTrue("%user (bob) should match bob in in matching context",
        userStatic.match(matchingContext));

    assertFalse(
        "%custom (bob) should NOT match %user (dave) in mismatching context",
        customUser.match(mismatchingContext));
    assertFalse(
        "%user (dave) should NOT match %custom (bob) in mismatching context",
        userCustom.match(mismatchingContext));
    assertFalse(
        "%user (dave) should NOT match %cus%tom (b+ob) in mismatching context",
        userCusTom.match(mismatchingContext));
    assertTrue("%user should match %user in any context",
        userUser.match(mismatchingContext));
    assertFalse(
        "%user (dave) should NOT match match bob in in matching context",
        userStatic.match(mismatchingContext));
  }

  @Test
  public void testVariableMatcherWithEmpties() {
    VariableContext emptyContext = new VariableContext();
    VariableContext allNull = new VariableContext();
    allNull.put("%null", null);
    allNull.put("%empty", null);

    VariableContext allEmpty = new VariableContext();
    allEmpty.put("%null", "");
    allEmpty.put("%empty", "");

    VariableContext allMakesSense = new VariableContext();
    allMakesSense.put("%null", null);
    allMakesSense.put("%empty", "");

    VariableContext allFull = new VariableContext();
    allFull.put("%null", "full");
    allFull.put("%empty", "full");

    MappingRuleMatcher nullMatcher =
        new MappingRuleMatchers.VariableMatcher("%null", null);

    MappingRuleMatcher emptyMatcher =
        new MappingRuleMatchers.VariableMatcher("%empty", "");

    //nulls should be handled as empty strings, so all should match
    assertTrue(nullMatcher.match(emptyContext));
    assertTrue(emptyMatcher.match(emptyContext));
    assertTrue(nullMatcher.match(allEmpty));
    assertTrue(emptyMatcher.match(allEmpty));
    assertTrue(nullMatcher.match(allNull));
    assertTrue(emptyMatcher.match(allNull));
    assertTrue(nullMatcher.match(allMakesSense));
    assertTrue(emptyMatcher.match(allMakesSense));

    //neither null nor "" should match an actual string
    assertFalse(nullMatcher.match(allFull));
    assertFalse(emptyMatcher.match(allFull));

    MappingRuleMatcher nullVarMatcher =
        new MappingRuleMatchers.VariableMatcher(null, "");

    //null variable should never match
    assertFalse(nullVarMatcher.match(emptyContext));
    assertFalse(nullVarMatcher.match(allNull));
    assertFalse(nullVarMatcher.match(allEmpty));
    assertFalse(nullVarMatcher.match(allMakesSense));
    assertFalse(nullVarMatcher.match(allFull));
  }

  @Test
  public void testBoolOperatorMatchers() {
    VariableContext developerBob = new VariableContext();
    developerBob.put("%user", "bob");
    developerBob.put("%primary_group", "developers");


    VariableContext testerBob = new VariableContext();
    testerBob.put("%user", "bob");
    testerBob.put("%primary_group", "testers");

    VariableContext testerDave = new VariableContext();
    testerDave.put("%user", "dave");
    testerDave.put("%primary_group", "testers");


    VariableContext accountantDave = new VariableContext();
    accountantDave.put("%user", "dave");
    accountantDave.put("%primary_group", "accountants");

    MappingRuleMatcher userBob =
        new MappingRuleMatchers.VariableMatcher("%user", "bob");

    MappingRuleMatcher groupDevelopers =
        new MappingRuleMatchers.VariableMatcher(
           "%primary_group", "developers");

    MappingRuleMatcher groupAccountants =
        new MappingRuleMatchers.VariableMatcher(
            "%primary_group", "accountants");

    MappingRuleMatcher developerBobMatcher = new MappingRuleMatchers.AndMatcher(
            userBob, groupDevelopers);

    MappingRuleMatcher testerDaveMatcher =
        MappingRuleMatchers.createUserGroupMatcher("dave", "testers");

    MappingRuleMatcher accountantOrBobMatcher =
        new MappingRuleMatchers.OrMatcher(groupAccountants, userBob);

    assertTrue(developerBobMatcher.match(developerBob));
    assertFalse(developerBobMatcher.match(testerBob));
    assertFalse(developerBobMatcher.match(testerDave));
    assertFalse(developerBobMatcher.match(accountantDave));

    assertFalse(testerDaveMatcher.match(developerBob));
    assertFalse(testerDaveMatcher.match(testerBob));
    assertTrue(testerDaveMatcher.match(testerDave));
    assertFalse(testerDaveMatcher.match(accountantDave));

    assertTrue(accountantOrBobMatcher.match(developerBob));
    assertTrue(accountantOrBobMatcher.match(testerBob));
    assertFalse(accountantOrBobMatcher.match(testerDave));
    assertTrue(accountantOrBobMatcher.match(accountantDave));
  }

  @Test
  public void testToStrings() {
    MappingRuleMatcher var = new MappingRuleMatchers.VariableMatcher("%a", "b");
    MappingRuleMatcher all = MappingRuleMatchers.createAllMatcher();
    MappingRuleMatcher and = new MappingRuleMatchers.AndMatcher(var, all, var);
    MappingRuleMatcher or = new MappingRuleMatchers.OrMatcher(var, all, var);

    assertEquals("VariableMatcher{variable='%a', value='b'}", var.toString());
    assertEquals("MatchAllMatcher", all.toString());
    assertEquals("AndMatcher{matchers=[" + var.toString() +
        ", " + all.toString() +
        ", " + var.toString() + "]}", and.toString());
    assertEquals("OrMatcher{matchers=[" + var.toString() +
        ", " + all.toString() +
        ", " + var.toString() + "]}", or.toString());
  }

}