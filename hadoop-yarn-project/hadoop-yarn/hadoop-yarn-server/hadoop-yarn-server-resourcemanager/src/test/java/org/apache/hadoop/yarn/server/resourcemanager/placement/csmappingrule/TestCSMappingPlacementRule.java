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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.CSMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MockQueueHierarchyBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUP_MAPPING;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCSMappingPlacementRule {
  public static final String DOT = ".";
  private static final Logger LOG = LoggerFactory
      .getLogger(TestCSMappingPlacementRule.class);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  private Map<String, Set<String>> userGroups =
      ImmutableMap.<String, Set<String>>builder()
      .put("alice", ImmutableSet.of("p_alice", "unique", "user"))
      .put("bob", ImmutableSet.of("p_bob", "user", "developer"))
      .put("charlie", ImmutableSet.of("p_charlie", "user", "tester"))
      .put("dave", ImmutableSet.of("user"))
      .put("emily", ImmutableSet.of("user", "tester", "developer"))
      .put("test.user", ImmutableSet.of("main.grp", "sec.test.grp"))
      .build();

  private void createQueueHierarchy(CapacitySchedulerQueueManager queueManager) {
    MockQueueHierarchyBuilder.create()
        .withQueueManager(queueManager)
        .withQueue("root.unman")
        .withQueue("root.default")
        .withManagedParentQueue("root.man")
        .withQueue("root.user.alice")
        .withQueue("root.user.bob")
        .withQueue("root.user.test_dot_user")
        .withQueue("root.user.testuser")
        .withQueue("root.groups.main_dot_grp")
        .withQueue("root.groups.sec_dot_test_dot_grp")
        .withQueue("root.secondaryTests.unique")
        .withQueue("root.secondaryTests.user")
        .withQueue("root.ambiguous.user.charlie")
        .withQueue("root.ambiguous.user.dave")
        .withQueue("root.ambiguous.user.ambi")
        .withQueue("root.ambiguous.group.tester")
        .withManagedParentQueue("root.ambiguous.managed")
        .withQueue("root.ambiguous.deep.user.charlie")
        .withQueue("root.ambiguous.deep.user.dave")
        .withQueue("root.ambiguous.deep.user.ambi")
        .withQueue("root.ambiguous.deep.group.tester")
        .withManagedParentQueue("root.ambiguous.deep.managed")
        .withQueue("root.disambiguous.deep.disambiuser.emily")
        .withQueue("root.disambiguous.deep.disambiuser.disambi")
        .withManagedParentQueue("root.disambiguous.deep.group.developer")
        .withManagedParentQueue("root.disambiguous.deep.dman")
        .withDynamicParentQueue("root.dynamic")
        .build();

    when(queueManager.getQueue(isNull())).thenReturn(null);
  }

  private CSMappingPlacementRule setupEngine(
      boolean overrideUserMappings, List<MappingRule> mappings)
      throws IOException {
    return setupEngine(overrideUserMappings, mappings, false);
  }

  private CSMappingPlacementRule setupEngine(
      boolean overrideUserMappings, List<MappingRule> mappings,
      boolean failOnConfigError)
      throws IOException {

    CapacitySchedulerConfiguration csConf =
        mock(CapacitySchedulerConfiguration.class);
    when(csConf.getMappingRules()).thenReturn(mappings);
    when(csConf.getOverrideWithQueueMappings())
        .thenReturn(overrideUserMappings);

    CapacitySchedulerQueueManager qm =
        mock(CapacitySchedulerQueueManager.class);
    createQueueHierarchy(qm);

    CapacityScheduler cs = mock(CapacityScheduler.class);
    when(cs.getConfiguration()).thenReturn(csConf);
    when(cs.getCapacitySchedulerQueueManager()).thenReturn(qm);

    CSMappingPlacementRule engine = new CSMappingPlacementRule();
    Groups groups = mock(Groups.class);

    //Initializing group provider to return groups specified in the userGroup
    //  map for each respective user
    for (String user : userGroups.keySet()) {
      when(groups.getGroupsSet(user)).thenReturn(userGroups.get(user));
    }
    engine.setGroups(groups);
    engine.setFailOnConfigError(failOnConfigError);
    engine.initialize(cs);

    return engine;
  }

  private ApplicationSubmissionContext createApp(String name, String queue) {
    ApplicationSubmissionContext ctx = Records.newRecord(
        ApplicationSubmissionContext.class);
    ctx.setApplicationName(name);
    ctx.setQueue(queue);
    return ctx;
  }

  private ApplicationSubmissionContext createApp(String name) {
    return createApp(name, YarnConfiguration.DEFAULT_QUEUE_NAME);
  }

  private void assertReject(String message, CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user) {
    try {
      ApplicationPlacementContext apc = engine.getPlacementForApp(asc, user);
      fail("Unexpected queue result: " + apc.getFullQueuePath() + " - " +
          message);
    } catch (YarnException e) {
      //To prevent PlacementRule chaining present in PlacementManager
      //when an application is rejected an exception is thrown to make sure
      //no other engine will try to place it.
    }
  }

  private void assertPlace(CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user, String expectedQueue) {
    assertPlace("Placement should not throw exception!",
        engine, asc, user, expectedQueue);
  }

  private void assertPlace(String message, CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user, String expectedQueue) {
    try {
      ApplicationPlacementContext apc = engine.getPlacementForApp(asc, user);
      assertNotNull(message, apc);
      String queue = apc.getParentQueue() == null ? "" :
          (apc.getParentQueue() + DOT);
      queue += apc.getQueue();
      assertEquals(message, expectedQueue,  queue);
    } catch (YarnException e) {
      LOG.error(message, e);
      fail(message);
    }
  }

  private void assertNullResult(String message, CSMappingPlacementRule engine,
                        ApplicationSubmissionContext asc, String user) {
    try {
      assertNull(message, engine.getPlacementForApp(asc, user));
    } catch (YarnException e) {
      LOG.error(message, e);
      fail(message);
    }
  }

  @Test
  public void testLegacyPlacementToExistingQueue() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(MappingRule.createLegacyRule(
        "u", "alice", "root.ambiguous.user.ambi"));
    rules.add(MappingRule.createLegacyRule("u", "bob", "ambi"));
    rules.add(MappingRule.createLegacyRule("u", "dave", "disambi"));
    rules.add(MappingRule.createLegacyRule("u", "%user", "disambiuser.%user"));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext asc = createApp("Default");
    assertPlace(engine, asc, "alice", "root.ambiguous.user.ambi");
    assertPlace("Should be placed to default because ambi is ambiguous and " +
        "legacy fallback is default", engine, asc, "bob", "root.default");
    assertPlace(engine, asc, "emily",
        "root.disambiguous.deep.disambiuser.emily");
    assertPlace("Should be placed to default because disambiuser.charlie does" +
        "not exit and legacy fallback is default", engine, asc, "charlie",
        "root.default");
    assertPlace(engine, asc, "dave",
        "root.disambiguous.deep.disambiuser.disambi");
  }

  @Test
  public void testLegacyPlacementToManagedQueues() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(MappingRule.createLegacyRule(
        "u", "alice", "root.ambiguous.managed.%user"));
    rules.add(MappingRule.createLegacyRule(
        "u", "bob", "managed.%user"));
    rules.add(MappingRule.createLegacyRule(
        "u", "charlie", "root.unman.charlie"));
    rules.add(MappingRule.createLegacyRule(
        "u", "dave", "non-existent.%user"));
    rules.add(MappingRule.createLegacyRule(
        "u", "%user", "root.man.%user"));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext asc = createApp("Default");
    assertPlace(engine, asc, "alice", "root.ambiguous.managed.alice");
    assertPlace("Should be placed to default because managed is ambiguous " +
        "and legacy fallback is default", engine, asc, "bob", "root.default");
    assertPlace("Should be placed to default because root.unman is not " +
        "managed and legacy fallback is default", engine, asc, "charlie",
        "root.default");
    assertPlace("Should be placed to default because parent queue does not " +
        "exist and legacy fallback is default",engine, asc, "dave",
        "root.default");
    assertPlace(engine, asc, "emily", "root.man.emily");
  }

  @Test
  public void testLegacyPlacementShortReference() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(MappingRule.createLegacyRule(
        "u", "alice", "non-existent"));
    rules.add(MappingRule.createLegacyRule(
        "u", "bob", "root"));
    rules.add(MappingRule.createLegacyRule(
        "u", "charlie", "man"));
    rules.add(MappingRule.createLegacyRule(
        "u", "dave", "ambi"));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext asc = createApp("Default");
    assertPlace("Should be placed to default: non-existent does not exist and " +
        "legacy fallback is default", engine, asc, "alice", "root.default");
    assertPlace("Should be placed to default: root is never managed and " +
        "legacy fallback is default", engine, asc, "bob", "root.default");
    assertPlace("Should be placed to default: managed parent is not a leaf " +
        "queue and legacy fallback is default", engine, asc, "charlie",
        "root.default");
    assertPlace("Should be placed to default: ambi is an ambiguous reference " +
        "and legacy fallback is default", engine, asc, "dave", "root.default");
  }

  @Test
  public void testRuleFallbackHandling() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("alice"),
            (new MappingRuleActions.PlaceToQueueAction("non-existent", true))
                .setFallbackReject()));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("bob"),
            (new MappingRuleActions.PlaceToQueueAction("non-existent", true))
                .setFallbackSkip()));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("bob"),
            MappingRuleActions.createUpdateDefaultAction("root.invalid")));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("bob"),
            new MappingRuleActions.PlaceToQueueAction("%default", true)));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("charlie"),
            (new MappingRuleActions.PlaceToQueueAction("non-existent", true))
                .setFallbackDefaultPlacement()));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("emily"),
            MappingRuleActions.createUpdateDefaultAction("root.invalid")));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("emily"),
            (new MappingRuleActions.PlaceToQueueAction("non-existent", true))
                .setFallbackDefaultPlacement()));
    //This rule is to catch all shouldfail applications, and place them to a
    // queue, so we can detect they were not rejected nor null-ed
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createApplicationNameMatcher("ShouldFail"),
            new MappingRuleActions.PlaceToQueueAction("root.default", true)));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext fail = createApp("ShouldFail");
    ApplicationSubmissionContext success = createApp("ShouldSucceed");

    assertReject("Alice has a straight up reject rule, " +
        "her application should be rejected",
        engine, fail, "alice");
    assertReject(
        "Bob should fail to place to non-existent -> should skip to next rule" +
        "\nBob should update the %default to root.invalid" +
        "\nBob should fail to place the app to %default which is root.invalid",
        engine, fail, "bob");
    assertPlace(
        "Charlie should be able to place the app to root.default as the" +
        "non-existent queue does not exist, but fallback is place to default",
        engine, success, "charlie", "root.default");
    assertNullResult(
        "Dave with success app has no matching rule, so we expect a null",
        engine, success, "dave");
    assertReject(
        "Emily should update the %default to root.invalid" +
        "\nBob should fail to place the app to non-existent and since the" +
        " fallback is placeToDefault, it should also fail, because we have" +
        " just updated default to an invalid value",
        engine, fail, "emily");
  }

  @Test
  public void testConfigValidation() {
    ArrayList<MappingRule> nonExistentStatic = new ArrayList<>();
    nonExistentStatic.add(MappingRule.createLegacyRule(
        "u", "alice", "non-existent"));

    //since the %token is an unknown variable, it will be considered as
    //a literal string, and since %token queue does not exist, it should fail
    ArrayList<MappingRule> tokenAsStatic = new ArrayList<>();
    tokenAsStatic.add(MappingRule.createLegacyRule(
        "u", "alice", "%token"));

    ArrayList<MappingRule> tokenAsDynamic = new ArrayList<>();
    //this rule might change the value of the %token, so the validator will be
    //aware of the %token variable
    tokenAsDynamic.add(new MappingRule(
        new MappingRuleMatchers.MatchAllMatcher(),
        new MappingRuleActions.VariableUpdateAction("%token", "non-existent")
    ));
    //since %token is an known variable, this rule is considered dynamic
    //so it cannot be entirely validated, this init should be successful
    tokenAsDynamic.add(MappingRule.createLegacyRule(
        "u", "alice", "%token"));

    try {
      setupEngine(true, nonExistentStatic, true);
      fail("We expect the setup to fail because we have a static rule " +
          "referencing a non-existent queue");
    } catch (IOException e) {
      //Exception expected
    }

    try {
      setupEngine(true, tokenAsStatic, true);
      fail("We expect the setup to fail because we have a rule containing an " +
          "unknown token, which is considered a static rule, with a " +
          "non-existent queue");
    } catch (IOException e) {
      //Exception expected
    }

    try {
      setupEngine(true, tokenAsDynamic, true);
    } catch (IOException e) {
      fail("We expect the setup to succeed because the %token is a known " +
          "variable so the rule is considered dynamic without parent, " +
          "and this always should pass");
    }
  }

  @Test
  public void testAllowCreateFlag() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("alice"),
            (new MappingRuleActions.PlaceToQueueAction("non-existent", true))
                .setFallbackReject()));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("bob"),
            (new MappingRuleActions.PlaceToQueueAction("non-existent", false))
                .setFallbackReject()));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("charlie"),
            (new MappingRuleActions.PlaceToQueueAction("root.man.create", true))
                .setFallbackReject()));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("emily"),
            (new MappingRuleActions.PlaceToQueueAction("root.man.create", false))
                .setFallbackReject()));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext app = createApp("app");

    assertReject("Alice should be rejected because the target queue" +
            " does not exist", engine, app, "alice");
    assertReject("Bob should be rejected because the target queue" +
        " does not exist", engine, app, "bob");
    assertReject("Emily should be rejected because auto queue creation is not" +
        " allowed for this action", engine, app, "emily");

    assertPlace("Charlie should be able to place since it is allowed to create",
        engine, app, "charlie", "root.man.create");

  }

  @Test
  public void testSpecified() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createAllMatcher(),
            (new MappingRuleActions.PlaceToQueueAction("%specified", true))
                .setFallbackSkip()));

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createAllMatcher(),
            (new MappingRuleActions.PlaceToQueueAction(
                "root.ambiguous.group.tester", true))
                .setFallbackSkip()));

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createAllMatcher(),
            (new MappingRuleActions.RejectAction())
                .setFallbackReject()));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext appNoQueue = createApp("app");
    ApplicationSubmissionContext appDefault = createApp("app", "default");
    ApplicationSubmissionContext appRootDefault =
        createApp("app", "root.default");
    ApplicationSubmissionContext appBob =
        createApp("app", "root.user.bob");

    assertPlace("App with non specified queue should end up in " +
        "'root.ambiguous.group.tester' because no queue was specified and " +
        "this is the only rule matching the submission",
        engine, appNoQueue, "alice", "root.ambiguous.group.tester");

    assertPlace("App with specified 'default' should end up in " +
            "'root.ambiguous.group.tester' because 'default' is the same as " +
            "no queue being specified and this is the only rule matching the " +
            "submission ",
        engine, appDefault, "alice", "root.ambiguous.group.tester");

    assertPlace("App with specified root.default should end up in " +
            "'root.default' because root.default is specifically provided",
        engine, appRootDefault, "alice", "root.default");

    assertPlace("App with specified queue should end up in the specified " +
        "queue 'root.user.bob'", engine, appBob, "alice", "root.user.bob");
  }

  @Test
  public void testGroupTargetMatching() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("alice"),
            (new MappingRuleActions.PlaceToQueueAction(
                "root.man.%primary_group", true))
                .setFallbackReject()));

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("bob"),
            (new MappingRuleActions.PlaceToQueueAction(
                "root.disambiguous.deep.group.%secondary_group.%user", true))
                .setFallbackReject()));

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("charlie"),
            (new MappingRuleActions.PlaceToQueueAction(
                "root.man.%secondary_group", true))
                .setFallbackReject()));

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("dave"),
            (new MappingRuleActions.PlaceToQueueAction(
                "root.dynamic.%secondary_group.%user", true))
                .setFallbackReject()));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext app = createApp("app");

    assertPlace(
        "Alice should be placed to root.man.p_alice based on her primary group",
        engine, app, "alice", "root.man.p_alice");
    assertPlace(
        "Bob should be placed to root.disambiguous.deep.group.developer.bob" +
        "based on his secondary group, since we have a queue named" +
        "'developer', under the path 'root.disambiguous.deep.group' bob " +
        "identifies as a user with secondary_group 'developer'", engine, app,
        "bob", "root.disambiguous.deep.group.developer.bob");
    assertReject("Charlie should get rejected because he neither of his" +
        "groups have an ambiguous queue, so effectively he has no secondary " +
        "group", engine, app, "charlie");
    assertReject("Dave should get rejected because he has no secondary group",
        engine, app, "dave");
  }

  @Test
  public void testSecondaryGroupWithoutParent() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("alice"),
            (new MappingRuleActions.PlaceToQueueAction(
                "%secondary_group", false))
                .setFallbackReject()));

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("bob"),
            (new MappingRuleActions.PlaceToQueueAction(
                "%secondary_group.%user", true))
                .setFallbackReject()));

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("charlie"),
            (new MappingRuleActions.PlaceToQueueAction(
                "%secondary_group", true))
                .setFallbackReject()));
    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext app = createApp("app");

    assertPlace(
        "Alice should be placed to root.secondaryTests.unique because " +
        "'unique' is a globally unique queue, and she has a matching group",
        engine, app, "alice", "root.secondaryTests.unique");
    assertPlace(
        "Bob should be placed to root.disambiguous.deep.group.developer.bob " +
        "because 'developer' is a globally unique PARENT queue, and he " +
        "has a matching group name, and can create a queue with '%user' " +
        "under it", engine, app, "bob",
        "root.disambiguous.deep.group.developer.bob");
    assertReject("Charlie should get rejected because neither of his" +
        "groups have a disambiguous queue, so effectively he has no " +
        "secondary group", engine, app, "charlie");
  }


  @Test
  public void testSecondaryGroupWithParent() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("alice"),
            (new MappingRuleActions.PlaceToQueueAction(
                "root.secondaryTests.%secondary_group", false))
                .setFallbackReject()));

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("bob"),
            (new MappingRuleActions.PlaceToQueueAction(
                "root.secondaryTests.%secondary_group", true))
                .setFallbackReject()));

    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("charlie"),
            (new MappingRuleActions.PlaceToQueueAction(
                "root.%secondary_group", true))
                .setFallbackReject()));
    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext app = createApp("app");

    assertPlace(
        "Alice should be placed to root.secondaryTests.unique because " +
        "both her secondary groups 'user' and 'unique' are eligible " +
        "for being a secondary group under root.secondaryTests, but " +
        "'unique' precedes 'user' in the group list.",
        engine, app, "alice", "root.secondaryTests.unique");
    assertPlace(
        "Bob should be placed to root.secondaryTests.user " +
        "bob is member of group 'user' and while 'user' is globally not " +
        "unique it is a valid secondary group target under queue " +
        "root.secondaryTests.",
        engine, app, "bob", "root.secondaryTests.user");
    assertReject("Charlie should get rejected because neither of his" +
        "groups have a matching queue under root.", engine, app, "charlie");
  }


  void assertConfigTestResult(List<MappingRule> rules) {
    assertEquals("We only specified one rule", 1, rules.size());
    MappingRule rule = rules.get(0);
    String ruleStr = rule.toString();
    assertTrue("Rule's matcher variable should be %user",
        ruleStr.contains("variable='%user'"));
    assertTrue("Rule's match value should be bob",
        ruleStr.contains("value='bob'"));
    assertTrue("Rule's action should be place to queue", ruleStr.contains(
        "action=PlaceToQueueAction{queueName='%primary_group'"));
  }

  @Test
  public void testLegacyConfiguration() throws IOException {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT,
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_LEGACY);
    conf.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "u:bob:%primary_group");

    List<MappingRule> rules = conf.getMappingRules();
    assertConfigTestResult(rules);
  }

  @Test
  public void testJSONConfiguration() throws IOException {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT,
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_JSON);
    conf.set(CapacitySchedulerConfiguration.MAPPING_RULE_JSON,
        "{\"rules\": [{" +
        "    \"type\": \"user\"," +
        "    \"matches\": \"bob\"," +
        "    \"policy\": \"custom\"," +
        "    \"customPlacement\": \"%primary_group\"," +
        "    \"fallbackResult\":\"skip\"" +
        "}]}");

    List<MappingRule> rules = conf.getMappingRules();
    assertConfigTestResult(rules);
  }

  @Test
  public void testEmptyJSONConfiguration() throws IOException {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT,
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_JSON);
    conf.set(CapacitySchedulerConfiguration.MAPPING_RULE_JSON, "");

    List<MappingRule> rules = conf.getMappingRules();
    assertEquals("We expect no rules", 0, rules.size());
  }

  @Test(expected = IOException.class)
  public void testInvalidJSONConfiguration() throws IOException {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT,
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_JSON);
    conf.set(CapacitySchedulerConfiguration.MAPPING_RULE_JSON,
        "I'm a bad JSON, since I'm not a JSON.");
    List<MappingRule> rules = conf.getMappingRules();
  }

  @Test(expected = IOException.class)
  public void testMissingJSONFileConfiguration() throws IOException {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT,
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_JSON);
    conf.set(CapacitySchedulerConfiguration.MAPPING_RULE_JSON_FILE,
        "/dev/null/nofile");
    List<MappingRule> rules = conf.getMappingRules();
  }

  @Test
  public void testJSONFileConfiguration() throws IOException {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT,
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_JSON);

    File jsonFile = folder.newFile("testJSONFileConfiguration.json");

    BufferedWriter writer = new BufferedWriter(new FileWriter(jsonFile));
    try {
      writer.write("{\"rules\": [{" +
          "    \"type\": \"user\"," +
          "    \"matches\": \"bob\"," +
          "    \"policy\": \"custom\"," +
          "    \"customPlacement\": \"%primary_group\"," +
          "    \"fallbackResult\":\"skip\"" +
          "}]}");
    } finally {
      writer.close();
    }

    conf.set(CapacitySchedulerConfiguration.MAPPING_RULE_JSON_FILE,
        jsonFile.getAbsolutePath());
    List<MappingRule> rules = conf.getMappingRules();

    assertConfigTestResult(rules);
  }

  @Test
  public void testUserNameCleanup() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createAllMatcher(),
            (new MappingRuleActions.PlaceToQueueAction("%user", true))
                .setFallbackReject()));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext app = createApp("app");
    assertPlace(
        "test.user should be placed to root.users.test_dot_user",
        engine, app, "test.user", "root.user.test_dot_user");
  }

  @Test
  public void testPrimaryGroupNameCleanup() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createAllMatcher(),
            (new MappingRuleActions.PlaceToQueueAction("%primary_group", true))
                .setFallbackReject()));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext app = createApp("app");
    assertPlace(
        "Application should have been placed to root.groups.main_dot_grp",
        engine, app, "test.user", "root.groups.main_dot_grp");
  }

  @Test
  public void testSecondaryGroupNameCleanup() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createAllMatcher(),
            (new MappingRuleActions.PlaceToQueueAction("%secondary_group", true))
                .setFallbackReject()));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext app = createApp("app");
    assertPlace(
        "Application should have been placed to root.groups.sec_dot_test_dot_grp",
        engine, app, "test.user", "root.groups.sec_dot_test_dot_grp");
  }

  /**
   * 1. Invoke Groups.reset(). This make sure that the underlying singleton {@link Groups#GROUPS}
   * is set to null.<br>
   * 2. Create a Configuration object in which the property "hadoop.security.group.mapping"
   * refers to an existing a test implementation.<br>
   * 3. Create a mock CapacityScheduler where getConf() and getConfiguration() contain different
   * settings for "hadoop.security.group.mapping". Since getConf() is the service config, this
   * should return the config object created in step #2.<br>
   * 4. Create an instance of CSMappingPlacementRule with a single primary group rule.<br>
   * 5. Run the placement evaluation.<br>
   * 6. Expected: The returned queue matches what is supposed to be coming from the test group
   * mapping service ("testuser" --> "testGroup1").<br>
   * 7. Modify "hadoop.security.group.mapping" in the config object created in step #2.
   * This step is required to guarantee that the CSMappingPlacementRule doesn't try to recreate
   * the group mapping implementation and uses the one that was previously created.<br>
   * 8. Call Groups.refresh() which changes the group mapping ("testuser" --> "testGroup0"). This
   * requires that the test group mapping service implement GroupMappingServiceProvider
   * .cacheGroupsRefresh().<br>
   * 9. Create a new instance of CSMappingPlacementRule. This is important as we want to test
   * that even this new {@link CSMappingPlacementRule} instance uses the same group mapping
   * instance.<br>
   * 10. Run the placement evaluation again<br>
   * 11. Expected: with the same user, the target queue has changed to 'testGroup0'.<br>
   * <p>
   * These all looks convoluted, but the steps above make sure all the following conditions are met:
   * <p>
   * 1. CSMappingPlacementRule will force the initialization of groups.<br>
   * 2. We select the correct configuration for group service init.<br>
   * 3. We don't create a new Groups instance if the singleton is initialized, so we cover the
   * original problem described in YARN-10597.<br>
   */
  @Test
  public void testPlacementEngineSelectsCorrectConfigurationForGroupMapping() throws IOException {
    Groups.reset();
    final String user = "testuser";

    //Create service-wide configuration object
    Configuration yarnConf = new Configuration();
    yarnConf.setClass(HADOOP_SECURITY_GROUP_MAPPING, MockUnixGroupsMapping.class,
        GroupMappingServiceProvider.class);

    //Create CS configuration object with a single, primary group mapping rule
    List<MappingRule> mappingRules = new ArrayList<>();
    mappingRules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher(user),
            (new MappingRuleActions.PlaceToQueueAction(
                "root.man.%primary_group", true))
                .setFallbackReject()));
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration() {
      @Override
      public List<MappingRule> getMappingRules() {
        return mappingRules;
      }
    };
    csConf.setOverrideWithQueueMappings(true);
    //Intentionally add a dummy implementation class -
    // The "HADOOP_SECURITY_GROUP_MAPPING" should not be read from the
    // CapacitySchedulerConfiguration instance!
    csConf.setClass(HADOOP_SECURITY_GROUP_MAPPING, String.class, Object.class);

    CapacityScheduler cs = createMockCS(yarnConf, csConf);

    //Create app, submit to placement engine, expecting queue=testGroup1
    CSMappingPlacementRule engine = initPlacementEngine(cs);
    ApplicationSubmissionContext app = createApp("app");
    assertPlace(engine, app, user, "root.man.testGroup1");

    //Intentionally add a dummy implementation class!
    // The "HADOOP_SECURITY_GROUP_MAPPING" should not be read from the
    // CapacitySchedulerConfiguration instance!
    //This makes sure that the Groups instance is not recreated by CSMappingPlacementRule
    yarnConf.setClass(HADOOP_SECURITY_GROUP_MAPPING, String.class, Object.class);

    //Refresh the groups, this makes testGroup0 as primary group for "testUser"
    engine.getGroups().refresh();

    //Create app, submit to placement engine, expecting queue=testGroup0 (the new primary group)
    engine = initPlacementEngine(cs);
    assertPlace(engine, app, user, "root.man.testGroup0");
  }

  @Test
  public void testOriginalUserNameWithDotCanBeUsedInMatchExpression() throws IOException {
    List<MappingRule> rules = new ArrayList<>();
    rules.add(
            new MappingRule(
                    MappingRuleMatchers.createUserMatcher("test.user"),
                    (MappingRuleActions.createUpdateDefaultAction("root.user.testuser"))
                            .setFallbackSkip()));
    rules.add(new MappingRule(
            MappingRuleMatchers.createUserMatcher("test.user"),
            (MappingRuleActions.createPlaceToDefaultAction())
                    .setFallbackReject()));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext app = createApp("app");
    assertPlace(
            "test.user should be placed to root.user",
            engine, app, "test.user", "root.user.testuser");
  }

  @Test
  public void testOriginalGroupNameWithDotCanBeUsedInMatchExpression() throws IOException {
    List<MappingRule> rules = new ArrayList<>();
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserGroupMatcher("sec.test.grp"),
            (MappingRuleActions.createUpdateDefaultAction("root.user.testuser"))
                .setFallbackSkip()));
    rules.add(new MappingRule(
        MappingRuleMatchers.createUserMatcher("test.user"),
        (MappingRuleActions.createPlaceToDefaultAction())
            .setFallbackReject()));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext app = createApp("app");
    assertPlace(
        "test.user should be placed to root.user",
        engine, app, "test.user", "root.user.testuser");
  }

  private CSMappingPlacementRule initPlacementEngine(CapacityScheduler cs) throws IOException {
    CSMappingPlacementRule engine = new CSMappingPlacementRule();
    engine.setFailOnConfigError(true);
    engine.initialize(cs);
    return engine;
  }

  private CapacityScheduler createMockCS(Configuration conf,
      CapacitySchedulerConfiguration csConf) {
    CapacitySchedulerQueueManager qm =
        mock(CapacitySchedulerQueueManager.class);
    createQueueHierarchy(qm);

    CapacityScheduler cs = mock(CapacityScheduler.class);
    when(cs.getConfiguration()).thenReturn(csConf);
    when(cs.getConf()).thenReturn(conf);
    when(cs.getCapacitySchedulerQueueManager()).thenReturn(qm);
    return cs;
  }

  public static class MockUnixGroupsMapping implements GroupMappingServiceProvider {

    public MockUnixGroupsMapping() {
      GROUP.clear();
      GROUP.add("testGroup1");
      GROUP.add("testGroup2");
      GROUP.add("testGroup3");
    }

    private static final List<String> GROUP = new ArrayList<>();

    @Override
    public List<String> getGroups(String user) throws IOException {
      return GROUP;
    }

    @Override
    public void cacheGroupsRefresh() {
      GROUP.add(0, "testGroup0");
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) {
      // Do nothing
    }

    @Override
    public Set<String> getGroupsSet(String user) {
      return ImmutableSet.copyOf(GROUP);
    }
  }
}