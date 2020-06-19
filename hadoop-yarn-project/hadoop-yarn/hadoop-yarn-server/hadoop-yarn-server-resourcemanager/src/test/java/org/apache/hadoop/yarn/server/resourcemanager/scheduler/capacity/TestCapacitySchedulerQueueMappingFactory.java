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

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.MappingType;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.QueueMappingBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SimpleGroupsMapping;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.getQueueMapping;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.setupQueueConfiguration;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.*;

public class TestCapacitySchedulerQueueMappingFactory {

  private static final String QUEUE_MAPPING_NAME = "app-name";
  private static final String QUEUE_MAPPING_RULE_APP_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.placement.AppNameMappingPlacementRule";
  private static final String QUEUE_MAPPING_RULE_USER_GROUP =
      "org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule";
  public static final String USER = "user_";
  public static final String PARENT_QUEUE = "c";

  public static CapacitySchedulerConfiguration setupQueueMappingsForRules(
      CapacitySchedulerConfiguration conf, String parentQueue,
      boolean overrideWithQueueMappings, int[] sourceIds) {

    List<String> queuePlacementRules = new ArrayList<>();

    queuePlacementRules.add(QUEUE_MAPPING_RULE_USER_GROUP);
    queuePlacementRules.add(QUEUE_MAPPING_RULE_APP_NAME);

    conf.setQueuePlacementRules(queuePlacementRules);

    List<QueueMapping> existingMappingsForUG = conf.getQueueMappings();

    //set queue mapping
    List<QueueMapping> queueMappingsForUG = new ArrayList<>();
    for (int i = 0; i < sourceIds.length; i++) {
      //Set C as parent queue name for auto queue creation
      QueueMapping userQueueMapping = QueueMappingBuilder.create()
                                          .type(MappingType.USER)
                                          .source(USER + sourceIds[i])
                                          .queue(
                                              getQueueMapping(parentQueue,
                                                  USER + sourceIds[i]))
                                          .build();
      queueMappingsForUG.add(userQueueMapping);
    }

    existingMappingsForUG.addAll(queueMappingsForUG);
    conf.setQueueMappings(existingMappingsForUG);

    List<QueueMapping> existingMappingsForAN =
        conf.getQueueMappingEntity(QUEUE_MAPPING_NAME);

    //set queue mapping
    List<QueueMapping> queueMappingsForAN =
        new ArrayList<>();
    for (int i = 0; i < sourceIds.length; i++) {
      //Set C as parent queue name for auto queue creation
      QueueMapping queueMapping = QueueMapping.QueueMappingBuilder.create()
          .type(MappingType.APPLICATION)
          .source(USER + sourceIds[i])
          .queue(getQueueMapping(parentQueue, USER + sourceIds[i]))
          .build();

      queueMappingsForAN.add(queueMapping);
    }

    existingMappingsForAN.addAll(queueMappingsForAN);
    conf.setQueueMappingEntities(existingMappingsForAN, QUEUE_MAPPING_NAME);
    //override with queue mappings
    conf.setOverrideWithQueueMappings(overrideWithQueueMappings);
    return conf;
  }

  @Test
  public void testUpdatePlacementRulesFactory() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    // init queue mapping for UserGroupMappingRule and AppNameMappingRule
    setupQueueMappingsForRules(conf, PARENT_QUEUE, true, new int[] {1, 2, 3});

    MockRM mockRM = null;
    try {
      mockRM = new MockRM(conf);
      CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
      cs.updatePlacementRules();
      mockRM.start();
      cs.start();

      List<PlacementRule> rules = cs.getRMContext()
          .getQueuePlacementManager().getPlacementRules();

      List<String> placementRuleNames = new ArrayList<>();
      for (PlacementRule pr : rules) {
        placementRuleNames.add(pr.getName());
      }

      // verify both placement rules were added successfully
      assertThat(placementRuleNames, hasItems(QUEUE_MAPPING_RULE_USER_GROUP));
      assertThat(placementRuleNames, hasItems(QUEUE_MAPPING_RULE_APP_NAME));
    } finally {
      if(mockRM != null) {
        mockRM.close();
      }
    }
  }

  @Test
  public void testNestedUserQueueWithStaticParentQueue() throws Exception {

    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);

    List<String> queuePlacementRules = new ArrayList<>();
    queuePlacementRules.add(QUEUE_MAPPING_RULE_USER_GROUP);
    conf.setQueuePlacementRules(queuePlacementRules);

    List<QueueMapping> existingMappingsForUG = conf.getQueueMappings();

    // set queue mapping
    List<QueueMapping> queueMappingsForUG = new ArrayList<>();

    // u:user1:b1
    QueueMapping userQueueMapping1 = QueueMappingBuilder.create()
                                        .type(QueueMapping.MappingType.USER)
                                        .source("user1")
                                        .queue("b1")
                                        .build();

    // u:%user:parentqueue.%user
    QueueMapping userQueueMapping2 = QueueMappingBuilder.create()
                                        .type(QueueMapping.MappingType.USER)
                                        .source("%user")
                                        .queue(getQueueMapping("c", "%user"))
                                        .build();

    queueMappingsForUG.add(userQueueMapping1);
    queueMappingsForUG.add(userQueueMapping2);

    existingMappingsForUG.addAll(queueMappingsForUG);
    conf.setQueueMappings(existingMappingsForUG);

    // override with queue mappings
    conf.setOverrideWithQueueMappings(true);

    MockRM mockRM = null;
    try {
      mockRM = new MockRM(conf);
      CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
      cs.updatePlacementRules();
      mockRM.start();
      cs.start();

      ApplicationSubmissionContext asc =
          Records.newRecord(ApplicationSubmissionContext.class);
      asc.setQueue("default");

      List<PlacementRule> rules =
          cs.getRMContext().getQueuePlacementManager().getPlacementRules();

      UserGroupMappingPlacementRule r =
          (UserGroupMappingPlacementRule) rules.get(0);

      ApplicationPlacementContext ctx = r.getPlacementForApp(asc, "user1");
      assertEquals("Queue", "b1", ctx.getQueue());

      ApplicationPlacementContext ctx2 = r.getPlacementForApp(asc, "user2");
      assertEquals("Queue", "user2", ctx2.getQueue());
      assertEquals("Queue", "root.c", ctx2.getParentQueue());
    } finally {
      if(mockRM != null) {
        mockRM.close();
      }
    }
  }

  @Test
  public void testNestedUserQueueWithPrimaryGroupAsDynamicParentQueue()
      throws Exception {

    /**
     * Mapping order: 1. u:%user:%primary_group.%user 2.
     * u:%user:%secondary_group.%user
     *
     * Expected parent queue is primary group of the user
     */

    // set queue mapping
    List<QueueMapping> queueMappingsForUG = new ArrayList<>();

    // u:%user:%primary_group.%user
    QueueMapping userQueueMapping1 = QueueMappingBuilder.create()
                                        .type(QueueMapping.MappingType.USER)
                                        .source("%user")
                                        .queue(
                                            getQueueMapping("%primary_group",
                                                "%user"))
                                        .build();

    // u:%user:%secondary_group.%user
    QueueMapping userQueueMapping2 = QueueMappingBuilder.create()
                                        .type(QueueMapping.MappingType.USER)
                                        .source("%user")
                                        .queue(
                                            getQueueMapping("%secondary_group",
                                                "%user"))
                                        .build();

    // u:b4:%secondary_group
    QueueMapping userQueueMapping3 = QueueMappingBuilder.create()
                                        .type(QueueMapping.MappingType.USER)
                                        .source("b4")
                                        .queue("%secondary_group")
                                        .build();
    queueMappingsForUG.add(userQueueMapping1);
    queueMappingsForUG.add(userQueueMapping2);
    queueMappingsForUG.add(userQueueMapping3);

    testNestedUserQueueWithDynamicParentQueue(queueMappingsForUG, true, "f");

    try {
      testNestedUserQueueWithDynamicParentQueue(queueMappingsForUG, true, "g");
      fail("Queue 'g' exists, but type is not Leaf Queue");
    } catch (YarnException e) {
      // Exception is expected as there is no such leaf queue
    }

    try {
      testNestedUserQueueWithDynamicParentQueue(queueMappingsForUG, true, "a1");
      fail("Actual Parent Queue of Leaf Queue 'a1' is 'a', but as per queue "
          + "mapping it returns primary queue as 'a1group'");
    } catch (YarnException e) {
      // Exception is expected as there is mismatch in expected and actual
      // parent queue
    }
  }

  @Test
  public void testNestedUserQueueWithSecondaryGroupAsDynamicParentQueue()
      throws Exception {

    /**
     * Mapping order: 1. u:%user:%secondary_group.%user 2.
     * u:%user:%primary_group.%user
     *
     * Expected parent queue is secondary group of the user
     */

    // set queue mapping
    List<QueueMapping> queueMappingsForUG = new ArrayList<>();

    // u:%user:%primary_group.%user
    QueueMapping userQueueMapping1 = QueueMappingBuilder.create()
                                          .type(QueueMapping.MappingType.USER)
                                          .source("%user")
                                          .queue(
                                              getQueueMapping("%primary_group",
                                                  "%user"))
                                          .build();

    // u:%user:%secondary_group.%user
    QueueMapping userQueueMapping2 = QueueMappingBuilder.create()
                                          .type(QueueMapping.MappingType.USER)
                                          .source("%user")
                                          .queue(
                                              getQueueMapping(
                                                  "%secondary_group", "%user")
                                              )
                                          .build();

    queueMappingsForUG.add(userQueueMapping2);
    queueMappingsForUG.add(userQueueMapping1);

    testNestedUserQueueWithDynamicParentQueue(queueMappingsForUG, false, "e");
  }

  private void testNestedUserQueueWithDynamicParentQueue(
      List<QueueMapping> mapping, boolean primary, String user)
      throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);

    List<String> queuePlacementRules = new ArrayList<>();
    queuePlacementRules.add(QUEUE_MAPPING_RULE_USER_GROUP);
    conf.setQueuePlacementRules(queuePlacementRules);

    List<QueueMapping> existingMappingsForUG = conf.getQueueMappings();

    existingMappingsForUG.addAll(mapping);
    conf.setQueueMappings(existingMappingsForUG);

    // override with queue mappings
    conf.setOverrideWithQueueMappings(true);

    MockRM mockRM = null;
    try {
      mockRM = new MockRM(conf);
      CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
      cs.updatePlacementRules();
      mockRM.start();
      cs.start();

      ApplicationSubmissionContext asc =
          Records.newRecord(ApplicationSubmissionContext.class);
      asc.setQueue("default");

      List<PlacementRule> rules =
          cs.getRMContext().getQueuePlacementManager().getPlacementRules();

      UserGroupMappingPlacementRule r =
          (UserGroupMappingPlacementRule) rules.get(0);
      ApplicationPlacementContext ctx = r.getPlacementForApp(asc, user);
      assertEquals("Queue", user, ctx.getQueue());

      if (primary) {
        assertEquals(
            "Primary Group", "root." + user + "group", ctx.getParentQueue());
      } else {
        assertEquals("Secondary Group", "root." + user + "subgroup1",
            ctx.getParentQueue());
      }
    } finally {
      if (mockRM != null) {
        mockRM.close();
      }
    }
  }

  @Test
  public void testDynamicPrimaryGroupQueue() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);

    List<String> queuePlacementRules = new ArrayList<>();
    queuePlacementRules.add(QUEUE_MAPPING_RULE_USER_GROUP);
    conf.setQueuePlacementRules(queuePlacementRules);

    List<QueueMapping> existingMappingsForUG = conf.getQueueMappings();

    // set queue mapping
    List<QueueMapping> queueMappingsForUG = new ArrayList<>();

    // u:user1:b1
    QueueMapping userQueueMapping1 = QueueMappingBuilder.create()
                                          .type(QueueMapping.MappingType.USER)
                                          .source("user1")
                                          .queue("b1")
                                          .build();

    // u:user2:%primary_group
    QueueMapping userQueueMapping2 = QueueMappingBuilder.create()
                                          .type(QueueMapping.MappingType.USER)
                                          .source("a1")
                                          .queue("%primary_group")
                                          .build();

    queueMappingsForUG.add(userQueueMapping1);
    queueMappingsForUG.add(userQueueMapping2);
    existingMappingsForUG.addAll(queueMappingsForUG);
    conf.setQueueMappings(existingMappingsForUG);

    // override with queue mappings
    conf.setOverrideWithQueueMappings(true);

    MockRM mockRM = null;
    try {
      mockRM = new MockRM(conf);
      CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
      cs.updatePlacementRules();
      mockRM.start();
      cs.start();

      ApplicationSubmissionContext asc =
          Records.newRecord(ApplicationSubmissionContext.class);
      asc.setQueue("default");

      List<PlacementRule> rules =
          cs.getRMContext().getQueuePlacementManager().getPlacementRules();
      UserGroupMappingPlacementRule r =
          (UserGroupMappingPlacementRule) rules.get(0);

      ApplicationPlacementContext ctx = r.getPlacementForApp(asc, "user1");
      assertEquals("Queue", "b1", ctx.getQueue());

      ApplicationPlacementContext ctx1 = r.getPlacementForApp(asc, "a1");
      assertEquals("Queue", "a1group", ctx1.getQueue());
    } finally {
      if (mockRM != null) {
        mockRM.close();
      }
    }
  }

  @Test
  public void testFixedUserWithDynamicGroupQueue() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);

    List<String> queuePlacementRules = new ArrayList<>();
    queuePlacementRules.add(QUEUE_MAPPING_RULE_USER_GROUP);
    conf.setQueuePlacementRules(queuePlacementRules);

    List<QueueMapping> existingMappingsForUG = conf.getQueueMappings();

    // set queue mapping
    List<QueueMapping> queueMappingsForUG = new ArrayList<>();

    // u:user1:b1
    QueueMapping userQueueMapping1 = QueueMappingBuilder.create()
                                          .type(QueueMapping.MappingType.USER)
                                          .source("user1")
                                          .queue("b1")
                                          .build();

    // u:user2:%primary_group
    QueueMapping userQueueMapping2 = QueueMappingBuilder.create()
                                          .type(QueueMapping.MappingType.USER)
                                          .source("a1")
                                          .queue("%primary_group")
                                          .build();

    // u:b4:%secondary_group
    QueueMapping userQueueMapping3 = QueueMappingBuilder.create()
                                          .type(QueueMapping.MappingType.USER)
                                          .source("e")
                                          .queue("%secondary_group")
                                          .build();

    queueMappingsForUG.add(userQueueMapping1);
    queueMappingsForUG.add(userQueueMapping2);
    queueMappingsForUG.add(userQueueMapping3);
    existingMappingsForUG.addAll(queueMappingsForUG);
    conf.setQueueMappings(existingMappingsForUG);

    //override with queue mappings
    conf.setOverrideWithQueueMappings(true);

    MockRM mockRM = null;
    try {
      mockRM = new MockRM(conf);
      CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
      cs.updatePlacementRules();
      mockRM.start();
      cs.start();

      ApplicationSubmissionContext asc =
          Records.newRecord(ApplicationSubmissionContext.class);
      asc.setQueue("default");

      List<PlacementRule> rules =
          cs.getRMContext().getQueuePlacementManager().getPlacementRules();
      UserGroupMappingPlacementRule r =
          (UserGroupMappingPlacementRule) rules.get(0);

      ApplicationPlacementContext ctx = r.getPlacementForApp(asc, "user1");
      assertEquals("Queue", "b1", ctx.getQueue());

      ApplicationPlacementContext ctx1 = r.getPlacementForApp(asc, "a1");
      assertEquals("Queue", "a1group", ctx1.getQueue());

      ApplicationPlacementContext ctx2 = r.getPlacementForApp(asc, "e");
      assertEquals("Queue", "esubgroup1", ctx2.getQueue());
    } finally {
      if (mockRM != null) {
        mockRM.close();
      }
    }
  }
}