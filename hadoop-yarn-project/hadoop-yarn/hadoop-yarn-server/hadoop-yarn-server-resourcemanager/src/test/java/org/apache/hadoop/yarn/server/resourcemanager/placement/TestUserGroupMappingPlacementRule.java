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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule.QueueMapping;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule.QueueMapping.MappingType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.PrimaryGroupMapping;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SimpleGroupsMapping;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestUserGroupMappingPlacementRule {
  YarnConfiguration conf = new YarnConfiguration();

  @Before
  public void setup() {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
  }

  private void verifyQueueMapping(QueueMapping queueMapping, String inputUser,
      String expectedQueue) throws YarnException {
    verifyQueueMapping(queueMapping, inputUser,
        YarnConfiguration.DEFAULT_QUEUE_NAME, expectedQueue, false);
  }

  private void verifyQueueMapping(QueueMapping queueMapping, String inputUser,
      String inputQueue, String expectedQueue, boolean overwrite) throws YarnException {
    verifyQueueMapping(queueMapping, inputUser, inputQueue, expectedQueue,
        overwrite, null);
  }

  private void verifyQueueMapping(QueueMapping queueMapping, String inputUser,
      String inputQueue, String expectedQueue, boolean overwrite,
      String expectedParentQueue) throws YarnException {
    Groups groups = new Groups(conf);
    UserGroupMappingPlacementRule rule = new UserGroupMappingPlacementRule(
        overwrite, Arrays.asList(queueMapping), groups);
    CapacitySchedulerQueueManager queueManager =
        mock(CapacitySchedulerQueueManager.class);
    when(queueManager.getQueue("asubgroup2")).thenReturn(mock(CSQueue.class));
    rule.setQueueManager(queueManager);
    ApplicationSubmissionContext asc = Records.newRecord(
        ApplicationSubmissionContext.class);
    asc.setQueue(inputQueue);
    ApplicationPlacementContext ctx = rule.getPlacementForApp(asc, inputUser);
    Assert.assertEquals("Queue", expectedQueue,
        ctx != null ? ctx.getQueue() : inputQueue);
    if (expectedParentQueue != null) {
      Assert.assertEquals("Parent Queue", expectedParentQueue,
          ctx.getParentQueue());
    }
  }

  @Test
  public void testSecondaryGroupMapping() throws YarnException {
    verifyQueueMapping(
        new QueueMapping(MappingType.USER, "%user", "%secondary_group"), "a",
        "asubgroup2");

    // PrimaryGroupMapping.class returns only primary group, no secondary groups
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        PrimaryGroupMapping.class, GroupMappingServiceProvider.class);

    verifyQueueMapping(
        new QueueMapping(MappingType.USER, "%user", "%secondary_group"), "a",
        "default");
  }

  @Test
  public void testMapping() throws YarnException {

    verifyQueueMapping(new QueueMapping(MappingType.USER, "a", "q1"), "a", "q1");
    verifyQueueMapping(new QueueMapping(MappingType.GROUP, "agroup", "q1"),
        "a", "q1");
    verifyQueueMapping(new QueueMapping(MappingType.USER, "%user", "q2"), "a",
        "q2");
    verifyQueueMapping(new QueueMapping(MappingType.USER, "%user", "%user"),
        "a", "a");
    verifyQueueMapping(
        new QueueMapping(MappingType.USER, "%user", "%primary_group"), "a",
        "agroup");
    verifyQueueMapping(
        new QueueMapping(MappingType.USER, "%user", "%user", "%primary_group"),
        "a", YarnConfiguration.DEFAULT_QUEUE_NAME, "a", false, "agroup");
    verifyQueueMapping(new QueueMapping(MappingType.GROUP, "asubgroup1", "q1"),
        "a", "q1");
    
    // specify overwritten, and see if user specified a queue, and it will be
    // overridden
    verifyQueueMapping(new QueueMapping(MappingType.USER, "user", "q1"), "user",
        "q2", "q1", true);
    
    // if overwritten not specified, it should be which user specified
    verifyQueueMapping(new QueueMapping(MappingType.USER, "user", "q1"), "user",
        "q2", "q2", false);

    // if overwritten not specified, it should be which user specified
    verifyQueueMapping(
        new QueueMapping(MappingType.GROUP, "usergroup", "%user", "usergroup"),
        "user", "default", "user", false);

    // if overwritten not specified, it should be which user specified
    verifyQueueMapping(
        new QueueMapping(MappingType.GROUP, "usergroup", "%user", "usergroup"),
        "user", "agroup", "user", true);

    //If user specific queue is enabled for a specified group under a given
    // parent queue
    verifyQueueMapping(
        new QueueMapping(MappingType.GROUP, "agroup", "%user", "parent1"), "a",
        "a");

    //If user specific queue is enabled for a specified group without parent
    // queue
    verifyQueueMapping(new QueueMapping(MappingType.GROUP, "agroup", "%user"),
        "a", "a");
  }
}
