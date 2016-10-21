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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class TestQueuePlacementPolicy {
  private final static Configuration conf = new Configuration();
  private Map<FSQueueType, Set<String>> configuredQueues;
  
  @BeforeClass
  public static void setup() {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
  }
  
  @Before
  public void initTest() {
    configuredQueues = new HashMap<FSQueueType, Set<String>>();
    for (FSQueueType type : FSQueueType.values()) {
      configuredQueues.put(type, new HashSet<String>());
    }
  }

  @Test
  public void testSpecifiedUserPolicy() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='user' />");
    sb.append("</queuePlacementPolicy>");
    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.specifiedq",
        policy.assignAppToQueue("specifiedq", "someuser"));
    assertEquals("root.someuser",
        policy.assignAppToQueue("default", "someuser"));
    assertEquals("root.otheruser",
        policy.assignAppToQueue("default", "otheruser"));
  }
  
  @Test
  public void testNoCreate() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='user' create=\"false\" />");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");
    
    configuredQueues.get(FSQueueType.LEAF).add("root.someuser");
    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.specifiedq", policy.assignAppToQueue("specifiedq", "someuser"));
    assertEquals("root.someuser", policy.assignAppToQueue("default", "someuser"));
    assertEquals("root.specifiedq", policy.assignAppToQueue("specifiedq", "otheruser"));
    assertEquals("root.default", policy.assignAppToQueue("default", "otheruser"));
  }
  
  @Test
  public void testSpecifiedThenReject() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='reject' />");
    sb.append("</queuePlacementPolicy>");
    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.specifiedq",
        policy.assignAppToQueue("specifiedq", "someuser"));
    assertEquals(null, policy.assignAppToQueue("default", "someuser"));
  }
  
  @Test (expected = AllocationConfigurationException.class)
  public void testOmittedTerminalRule() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='user' create=\"false\" />");
    sb.append("</queuePlacementPolicy>");
    parse(sb.toString());
  }
  
  @Test (expected = AllocationConfigurationException.class)
  public void testTerminalRuleInMiddle() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='default' />");
    sb.append("  <rule name='user' />");
    sb.append("</queuePlacementPolicy>");
    parse(sb.toString());
  }
  
  @Test
  public void testTerminals() throws Exception {
    // Should make it through without an exception
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='secondaryGroupExistingQueue' create='true'/>");
    sb.append("  <rule name='default' queue='otherdefault' create='false'/>");
    sb.append("</queuePlacementPolicy>");
    QueuePlacementPolicy policy = parse(sb.toString());
    try {
      policy.assignAppToQueue("root.otherdefault", "user1");
      fail("Expect exception from having default rule with create=\'false\'");
    } catch (IllegalStateException se) {
    }
  }
  
  @Test
  public void testDefaultRuleWithQueueAttribute() throws Exception {
    // This test covers the use case where we would like default rule
    // to point to a different queue by default rather than root.default
    configuredQueues.get(FSQueueType.LEAF).add("root.someDefaultQueue");
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='default' queue='root.someDefaultQueue'/>");
    sb.append("</queuePlacementPolicy>");

    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.someDefaultQueue",
        policy.assignAppToQueue("root.default", "user1"));
  }
  
  @Test
  public void testNestedUserQueueParsingErrors() {
    // No nested rule specified in hierarchical user queue
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='nestedUserQueue'/>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    assertIfExceptionThrown(sb);

    // Specified nested rule is not a QueuePlacementRule
    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='unknownRule'/>");
    sb.append("  </rule>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    assertIfExceptionThrown(sb);
  }

  private void assertIfExceptionThrown(StringBuffer sb) {
    Throwable th = null;
    try {
      parse(sb.toString());
    } catch (Exception e) {
      th = e;
    }

    assertTrue(th instanceof AllocationConfigurationException);
  }

  @Test
  public void testNestedUserQueueParsing() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='primaryGroup'/>");
    sb.append("  </rule>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    Throwable th = null;
    try {
      parse(sb.toString());
    } catch (Exception e) {
      th = e;
    }

    assertNull(th);
  }

  @Test
  public void testNestedUserQueuePrimaryGroup() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='primaryGroup'/>");
    sb.append("  </rule>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    // User queue would be created under primary group queue
    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.user1group.user1",
        policy.assignAppToQueue("root.default", "user1"));
    // Other rules above and below hierarchical user queue rule should work as
    // usual
    configuredQueues.get(FSQueueType.LEAF).add("root.specifiedq");
    // test if specified rule(above nestedUserQueue rule) works ok
    assertEquals("root.specifiedq",
        policy.assignAppToQueue("root.specifiedq", "user2"));

    // test if default rule(below nestedUserQueue rule) works
    configuredQueues.get(FSQueueType.LEAF).add("root.user3group");
    assertEquals("root.default",
        policy.assignAppToQueue("root.default", "user3"));
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

    QueuePlacementPolicy policy = parse(sb.toString());

    // Should return root.default since primary group 'root.user1group' is not
    // configured
    assertEquals("root.default",
        policy.assignAppToQueue("root.default", "user1"));

    // Let's configure primary group and check if user queue is created
    configuredQueues.get(FSQueueType.PARENT).add("root.user1group");
    policy = parse(sb.toString());
    assertEquals("root.user1group.user1",
        policy.assignAppToQueue("root.default", "user1"));

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
    assertEquals("root.default",
        policy.assignAppToQueue("root.default", "user2"));

    // Now configure both primary group and the user queue for user2
    configuredQueues.get(FSQueueType.PARENT).add("root.user2group");
    configuredQueues.get(FSQueueType.LEAF).add("root.user2group.user2");
    policy = parse(sb.toString());

    assertEquals("root.user2group.user2",
        policy.assignAppToQueue("root.default", "user2"));
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

    QueuePlacementPolicy policy = parse(sb.toString());
    // Should return root.default since secondary groups are not configured
    assertEquals("root.default",
        policy.assignAppToQueue("root.default", "user1"));

    // configure secondary group for user1
    configuredQueues.get(FSQueueType.PARENT).add("root.user1subgroup1");
    policy = parse(sb.toString());
    // user queue created should be created under secondary group
    assertEquals("root.user1subgroup1.user1",
        policy.assignAppToQueue("root.default", "user1"));
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
    configuredQueues.get(FSQueueType.PARENT).add("root.parent1");
    configuredQueues.get(FSQueueType.PARENT).add("root.parent2");

    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.parent1.user1",
        policy.assignAppToQueue("root.parent1", "user1"));
    assertEquals("root.parent2.user2",
        policy.assignAppToQueue("root.parent2", "user2"));
  }
  
  @Test
  public void testNestedUserQueueDefaultRule() throws Exception {
    // This test covers the use case where we would like user queues to be
    // created under a default parent queue
    configuredQueues.get(FSQueueType.PARENT).add("root.parentq");
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='default' queue='root.parentq'/>");
    sb.append("  </rule>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.parentq.user1",
        policy.assignAppToQueue("root.default", "user1"));
  }

  @Test
  public void testUserContainsPeriod() throws Exception {
    // This test covers the user case where the username contains periods.
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='user' />");
    sb.append("</queuePlacementPolicy>");
    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.first_dot_last",
        policy.assignAppToQueue("default", "first.last"));

    sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='default'/>");
    sb.append("  </rule>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");
    policy = parse(sb.toString());
    assertEquals("root.default.first_dot_last",
        policy.assignAppToQueue("root.default", "first.last"));
  }

  @Test
  public void testGroupContainsPeriod() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' create='false' />");
    sb.append("  <rule name='nestedUserQueue'>");
    sb.append("       <rule name='primaryGroup'/>");
    sb.append("  </rule>");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        PeriodGroupsMapping.class, GroupMappingServiceProvider.class);
    // User queue would be created under primary group queue, and the period
    // in the group name should be converted into _dot_
    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.user1_dot_group.user1",
        policy.assignAppToQueue("root.default", "user1"));

    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
  }

  @Test(expected=IOException.class)
  public void testEmptyGroupsPrimaryGroupRule() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='primaryGroup' create=\"false\" />");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");

    // Add a static mapping that returns empty groups for users
    conf.setStrings(CommonConfigurationKeys
        .HADOOP_USER_GROUP_STATIC_OVERRIDES, "emptygroupuser=");
    QueuePlacementPolicy policy = parse(sb.toString());
    policy.assignAppToQueue(null, "emptygroupuser");
  }

  private QueuePlacementPolicy parse(String str) throws Exception {
    // Read and parse the allocations file.
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
        .newInstance();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(IOUtils.toInputStream(str));
    Element root = doc.getDocumentElement();
    return QueuePlacementPolicy.fromXml(root, configuredQueues, conf);
  }
}
