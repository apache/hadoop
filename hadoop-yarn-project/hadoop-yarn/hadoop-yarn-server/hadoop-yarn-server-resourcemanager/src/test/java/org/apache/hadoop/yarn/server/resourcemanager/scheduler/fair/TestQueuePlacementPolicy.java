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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.DefaultPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.FSPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserPlacementRule;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Tests for the queue placement policy for the {@link FairScheduler}.
 */
public class TestQueuePlacementPolicy {
  private final static FairSchedulerConfiguration CONF =
      new FairSchedulerConfiguration();
  // Base setup needed, policy is an intermediate object
  private PlacementManager placementManager;
  private FairScheduler scheduler;
  private QueueManager queueManager;

  // Locals used in each assignment
  private ApplicationSubmissionContext asc;
  private ApplicationPlacementContext context;

  @BeforeClass
  public static void setup() {
    CONF.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
  }

  @Before
  public void initTest() {
    SystemClock clock = SystemClock.getInstance();
    RMContext rmContext = mock(RMContext.class);
    placementManager = new PlacementManager();
    scheduler = mock(FairScheduler.class);
    when(scheduler.getClock()).thenReturn(clock);
    when(scheduler.getRMContext()).thenReturn(rmContext);
    when(scheduler.getConfig()).thenReturn(CONF);
    when(scheduler.getConf()).thenReturn(CONF);
    when(rmContext.getQueuePlacementManager()).thenReturn(placementManager);
    AllocationConfiguration allocConf = new AllocationConfiguration(scheduler);
    when(scheduler.getAllocationConfiguration()).thenReturn(allocConf);
    queueManager = new QueueManager(scheduler);
    queueManager.initialize();
    when(scheduler.getQueueManager()).thenReturn(queueManager);
  }

  @After
  public void cleanTest() {
    placementManager = null;
    queueManager = null;
    scheduler = null;
  }

  @Test
  public void testSpecifiedUserPolicy() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='user' />");
    sb.append("</queuePlacementPolicy>");
    createPolicy(sb.toString());

    asc = newAppSubmissionContext("specifiedq");
    context = placementManager.placeApplication(asc, "someuser");
    assertEquals("root.specifiedq", context.getQueue());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "someuser");
    assertEquals("root.someuser", context.getQueue());
    context = placementManager.placeApplication(asc, "otheruser");
    assertEquals("root.otheruser", context.getQueue());
  }

  @Test
  public void testNoCreate() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='user' create=\"false\" />");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");
    createPolicy(sb.toString());

    createQueue(FSQueueType.LEAF, "root.someuser");

    asc = newAppSubmissionContext("specifiedq");
    context = placementManager.placeApplication(asc, "someuser");
    assertEquals("root.specifiedq", context.getQueue());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "someuser");
    assertEquals("root.someuser", context.getQueue());
    asc = newAppSubmissionContext("specifiedq");
    context = placementManager.placeApplication(asc, "otheruser");
    assertEquals("root.specifiedq", context.getQueue());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "otheruser");
    assertEquals("root.default", context.getQueue());
  }

  @Test
  public void testSpecifiedThenReject() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='reject' />");
    sb.append("</queuePlacementPolicy>");
    createPolicy(sb.toString());
    asc = newAppSubmissionContext("specifiedq");
    context = placementManager.placeApplication(asc, "someuser");
    assertEquals("root.specifiedq", context.getQueue());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "someuser");
    assertNull("Assignment should have been rejected and was not", context);
  }

  @Test
  public void testOmittedTerminalRule()  {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='user' create=\"false\" />");
    sb.append("</queuePlacementPolicy>");
    assertIfExceptionThrown(sb);
  }

  @Test
  public void testTerminalRuleInMiddle() {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='default' />");
    sb.append("  <rule name='user' />");
    sb.append("</queuePlacementPolicy>");
    assertIfExceptionThrown(sb);
  }

  @Test
  public void testTerminals() {
    // The default rule is no longer considered terminal when the create flag
    // is false. The throw now happens when configuring not when assigning the
    // application
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='secondaryGroupExistingQueue' create='true'/>");
    sb.append("  <rule name='default' queue='otherdefault' create='false'/>");
    sb.append("</queuePlacementPolicy>");
    assertIfExceptionThrown(sb);
  }

  @Test
  public void testDefaultRuleWithQueueAttribute() throws Exception {
    // This test covers the use case where we would like default rule
    // to point to a different queue by default rather than root.default
    createQueue(FSQueueType.LEAF, "root.someDefaultQueue");
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='default' queue='root.someDefaultQueue'/>");
    sb.append("</queuePlacementPolicy>");

    createPolicy(sb.toString());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "user1");
    assertEquals("root.someDefaultQueue", context.getQueue());
  }

  @Test
  public void testNestedUserQueueParsingErrors() {
    // No nested rule specified in hierarchical user queue
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='nestedUserQueue'/>");
    sb.append("</queuePlacementPolicy>");

    assertIfExceptionThrown(sb);

    // Specified nested rule is not a FSPlacementRule
    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("    <rule name='unknownRule'/>");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");

    assertIfExceptionThrown(sb);

    // Parent rule is rule that cannot be one: reject or nestedUserQueue
    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("    <rule name='reject'/>");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");

    assertIfExceptionThrown(sb);

    // If the parent rule does not have the create flag the nested rule is not
    // terminal
    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("    <rule name='primaryGroup' create='false'/>");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");

    assertIfExceptionThrown(sb);
  }

  @Test
  public void testMultipleParentRules() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("    <rule name='primaryGroup'/>");
    sb.append("    <rule name='default'/>");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");

    createPolicy(sb.toString());
    PlacementRule nested = placementManager.getPlacementRules().get(0);
    if (nested instanceof UserPlacementRule) {
      PlacementRule parent = ((FSPlacementRule)nested).getParentRule();
      assertTrue("Nested rule should have been Default rule",
          parent instanceof DefaultPlacementRule);
    } else {
      fail("Policy parsing failed: rule with multiple parents not set");
    }
  }

  @Test
  public void testBrokenRules() throws Exception {
    // broken rule should fail configuring
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule />");
    sb.append("</queuePlacementPolicy>");

    assertIfExceptionThrown(sb);

    // policy without rules ignoring policy
    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <notarule />");
    sb.append("</queuePlacementPolicy>");

    createPolicy(sb.toString());

    // broken rule should fail configuring
    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='user'>");
    sb.append("    <rule />");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");

    assertIfExceptionThrown(sb);

    // parent rule not set to something known: no parent rule is required
    // required case is only for nestedUserQueue tested earlier
    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='user'>");
    sb.append("    <notarule />");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");

    createPolicy(sb.toString());
  }

  private void assertIfExceptionThrown(StringBuffer sb) {
    Throwable th = null;
    try {
      createPolicy(sb.toString());
    } catch (Exception e) {
      th = e;
    }

    assertTrue(th instanceof AllocationConfigurationException);
  }

  private void assertIfExceptionThrown(String user) {
    Throwable th = null;
    try {
      placementManager.placeApplication(asc, user);
    } catch (Exception e) {
      th = e;
    }

    assertTrue(th instanceof YarnException);
  }

  @Test
  public void testNestedUserQueueParsing() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='primaryGroup'/>");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");

    createPolicy(sb.toString());
  }

  @Test
  public void testNestedUserQueuePrimaryGroup() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='primaryGroup'/>");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");

    // User queue would be created under primary group queue
    createPolicy(sb.toString());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "user1");
    assertEquals("root.user1group.user1", context.getQueue());
    // Other rules above and below hierarchical user queue rule should work as
    // usual
    createQueue(FSQueueType.LEAF, "root.specifiedq");
    // test if specified rule(above nestedUserQueue rule) works ok
    asc = newAppSubmissionContext("root.specifiedq");
    context = placementManager.placeApplication(asc, "user2");
    assertEquals("root.specifiedq", context.getQueue());

    // Submit should fail if we cannot create the queue
    createQueue(FSQueueType.LEAF, "root.user3group");
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "user3");
    assertNull("Submission should have failed and did not", context);
  }

  @Test
  public void testNestedUserQueuePrimaryGroupNoCreate() throws Exception {
    // Primary group rule has create='false'
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='primaryGroup' create='false'/>");
    sb.append("  </rule>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    createPolicy(sb.toString());

    // Should return root.default since primary group 'root.user1group' is not
    // configured
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "user1");
    assertEquals("root.default", context.getQueue());

    // Let's configure primary group and check if user queue is created
    createQueue(FSQueueType.PARENT, "root.user1group");
    context = placementManager.placeApplication(asc, "user1");
    assertEquals("root.user1group.user1", context.getQueue());

    // Both Primary group and nestedUserQueue rule has create='false'
    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='nestedUserQueue' create='false'>");
    sb.append("       <rule name='primaryGroup' create='false'/>");
    sb.append("  </rule>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    // Should return root.default since primary group and user queue for user 2
    // are not configured.
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "user2");
    assertEquals("root.default", context.getQueue());

    // Now configure both primary group and the user queue for user2
    createQueue(FSQueueType.LEAF, "root.user2group.user2");

    // Try placing the same app again
    context = placementManager.placeApplication(asc, "user2");
    assertEquals("root.user2group.user2", context.getQueue());
  }

  @Test
  public void testNestedUserQueueSecondaryGroup() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='secondaryGroupExistingQueue'/>");
    sb.append("  </rule>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    createPolicy(sb.toString());
    // Should return root.default since secondary groups are not configured
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "user1");
    assertEquals("root.default", context.getQueue());

    // configure secondary group for user1
    createQueue(FSQueueType.PARENT, "root.user1subgroup1");
    createPolicy(sb.toString());
    // user queue created should be created under secondary group
    context = placementManager.placeApplication(asc, "user1");
    assertEquals("root.user1subgroup1.user1", context.getQueue());
  }

  @Test
  public void testNestedUserQueueSpecificRule() throws Exception {
    // This test covers the use case where users can specify different parent
    // queues and want user queues under those.
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='specified' create='false'/>");
    sb.append("  </rule>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    // Let's create couple of parent queues
    createQueue(FSQueueType.PARENT, "root.parent1");
    createQueue(FSQueueType.PARENT, "root.parent2");

    createPolicy(sb.toString());
    asc = newAppSubmissionContext("root.parent1");
    context = placementManager.placeApplication(asc, "user1");
    assertEquals("root.parent1.user1", context.getQueue());
    asc = newAppSubmissionContext("root.parent2");
    context = placementManager.placeApplication(asc, "user2");
    assertEquals("root.parent2.user2", context.getQueue());
  }

  @Test
  public void testNestedUserQueueDefaultRule() throws Exception {
    // This test covers the use case where we would like user queues to be
    // created under a default parent queue
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='default' queue='root.parent'/>");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");

    createPolicy(sb.toString());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "user1");
    assertEquals("root.parent.user1", context.getQueue());

    // Same as above but now with the create flag false for the parent
    createQueue(FSQueueType.PARENT, "root.parent");
    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("    <rule name='default' queue='root.parent' create='false'/>");
    sb.append("  </rule>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    createPolicy(sb.toString());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "user1");
    assertEquals("root.parent.user1", context.getQueue());

    // Parent queue returned is already a configured LEAF, should fail and the
    // context is null.
    createQueue(FSQueueType.LEAF, "root.parent");
    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='default' queue='root.parent' />");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");

    createPolicy(sb.toString());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "user1");
    assertNull("Submission should have failed and did not", context);
  }

  @Test
  public void testUserContainsPeriod() throws Exception {
    // This test covers the user case where the username contains periods.
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='user' />");
    sb.append("</queuePlacementPolicy>");
    createPolicy(sb.toString());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "first.last");
    assertEquals("root.first_dot_last", context.getQueue());

    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='default'/>");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");
    // specified create is false, bypass the rule
    // default rule has create which requires a PARENT queue: remove the LEAF
    queueManager.removeLeafQueue("root.default");
    createPolicy(sb.toString());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "first_dot_last");
    assertEquals("root.default.first_dot_last", context.getQueue());
  }

  @Test
  public void testGroupContainsPeriod() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='primaryGroup'/>");
    sb.append("  </rule>");
    sb.append("</queuePlacementPolicy>");

    // change the group resolution
    CONF.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        PeriodGroupsMapping.class, GroupMappingServiceProvider.class);
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(CONF);
    // User queue would be created under primary group queue, and the period
    // in the group name should be converted into _dot_
    createPolicy(sb.toString());
    asc = newAppSubmissionContext("default");
    context = placementManager.placeApplication(asc, "user1");
    assertEquals("root.user1_dot_group.user1", context.getQueue());

    // undo the group resolution change
    CONF.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(CONF);
  }

  @Test
  public void testEmptyGroupsPrimaryGroupRule() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='primaryGroup' create=\"false\" />");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    // Add a static mapping that returns empty groups for users
    CONF.setStrings(CommonConfigurationKeys
        .HADOOP_USER_GROUP_STATIC_OVERRIDES, "emptygroupuser=");
    createPolicy(sb.toString());
    asc = newAppSubmissionContext("root.fake");
    assertIfExceptionThrown("emptygroupuser");
  }

  @Test
  public void testSpecifiedQueueWithSpaces() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified'/>");
    sb.append("  <rule name='default'/>");
    sb.append("</queuePlacementPolicy>");

    createPolicy(sb.toString());
    asc = newAppSubmissionContext("A ");
    assertIfExceptionThrown("user1");

    asc = newAppSubmissionContext("A\u00a0");
    assertIfExceptionThrown("user1");
  }

  private void createPolicy(String str)
      throws AllocationConfigurationException {
    // Read and parse the allocations file.
    Element root = null;
    try {
      DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
          .newInstance();
      docBuilderFactory.setIgnoringComments(true);
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      Document doc = builder.parse(IOUtils.toInputStream(str,
          StandardCharsets.UTF_8));
      root = doc.getDocumentElement();
    } catch (Exception ex) {
      // Don't really want to test the xml parsing side,
      // let it fail with a null config below.
    }
    QueuePlacementPolicy.fromXml(root, scheduler);
  }

  private ApplicationSubmissionContext newAppSubmissionContext(String queue) {
    ApplicationId appId = ApplicationId.newInstance(1L, 1);
    Priority prio = Priority.UNDEFINED;
    Resource resource = Resource.newInstance(1, 1);
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(null, null, null, null, null, null);
    return ApplicationSubmissionContext.newInstance(appId, "test", queue,
        prio, amContainer, false, false, 1, resource, "testing");
  }

  private void createQueue(FSQueueType type, String name) {
    // Create a queue as if it is in the config.
    FSQueue queue = queueManager.createQueue(name, type);
    assertNotNull("Queue not created", queue);
    // walk up the list till we have a non dynamic queue
    // root is always non dynamic
    do {
      queue.setDynamic(false);
      queue = queue.parent;
    } while (queue.isDynamic());
  }
}
