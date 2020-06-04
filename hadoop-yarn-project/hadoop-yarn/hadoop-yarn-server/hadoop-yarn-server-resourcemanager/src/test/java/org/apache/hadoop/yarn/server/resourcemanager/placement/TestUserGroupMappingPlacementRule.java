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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.isNull;

import java.util.Arrays;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.NullGroupsMapping;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.MappingType;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.QueueMappingBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.placement.TestUserGroupMappingPlacementRule.QueueMappingTestData.QueueMappingTestDataBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
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

  private void verifyQueueMapping(QueueMappingTestData queueMappingTestData)
      throws YarnException {

    QueueMapping queueMapping = queueMappingTestData.queueMapping;
    String inputUser = queueMappingTestData.inputUser;
    String inputQueue = queueMappingTestData.inputQueue;
    String expectedQueue = queueMappingTestData.expectedQueue;
    boolean overwrite = queueMappingTestData.overwrite;
    String expectedParentQueue = queueMappingTestData.expectedParentQueue;

    Groups groups = new Groups(conf);
    UserGroupMappingPlacementRule rule = new UserGroupMappingPlacementRule(
        overwrite, Arrays.asList(queueMapping), groups);
    CapacitySchedulerQueueManager queueManager =
        mock(CapacitySchedulerQueueManager.class);

    ParentQueue root = mock(ParentQueue.class);
    when(root.getQueuePath()).thenReturn("root");

    ParentQueue agroup = mock(ParentQueue.class);
    when(agroup.getQueuePath()).thenReturn("root.agroup");
    ParentQueue bsubgroup2 = mock(ParentQueue.class);
    when(bsubgroup2.getQueuePath()).thenReturn("root.bsubgroup2");
    when(bsubgroup2.getParent()).thenReturn(root);

    ManagedParentQueue managedParent = mock(ManagedParentQueue.class);
    when(managedParent.getQueueName()).thenReturn("managedParent");
    when(managedParent.getQueuePath()).thenReturn("root.managedParent");

    LeafQueue a = mock(LeafQueue.class);
    when(a.getQueuePath()).thenReturn("root.agroup.a");
    when(a.getParent()).thenReturn(agroup);
    LeafQueue b = mock(LeafQueue.class);
    when(b.getQueuePath()).thenReturn("root.bsubgroup2.b");
    when(b.getParent()).thenReturn(bsubgroup2);
    LeafQueue asubgroup2 = mock(LeafQueue.class);
    when(asubgroup2.getQueuePath()).thenReturn("root.asubgroup2");
    when(asubgroup2.getParent()).thenReturn(root);

    when(queueManager.getQueue(isNull())).thenReturn(null);
    when(queueManager.getQueue("a")).thenReturn(a);
    when(a.getParent()).thenReturn(agroup);
    when(queueManager.getQueue("b")).thenReturn(b);
    when(b.getParent()).thenReturn(bsubgroup2);
    when(queueManager.getQueue("agroup")).thenReturn(agroup);
    when(agroup.getParent()).thenReturn(root);
    when(queueManager.getQueue("bsubgroup2")).thenReturn(bsubgroup2);
    when(bsubgroup2.getParent()).thenReturn(root);
    when(queueManager.getQueue("asubgroup2")).thenReturn(asubgroup2);
    when(asubgroup2.getParent()).thenReturn(root);
    when(queueManager.getQueue("managedParent")).thenReturn(managedParent);
    when(managedParent.getParent()).thenReturn(root);

    when(queueManager.getQueue("root")).thenReturn(root);
    when(queueManager.getQueue("root.agroup")).thenReturn(agroup);
    when(queueManager.getQueue("root.bsubgroup2")).thenReturn(bsubgroup2);
    when(queueManager.getQueue("root.asubgroup2")).thenReturn(asubgroup2);
    when(queueManager.getQueue("root.agroup.a")).thenReturn(a);
    when(queueManager.getQueue("root.bsubgroup2.b")).thenReturn(b);
    when(queueManager.getQueue("root.managedParent")).thenReturn(managedParent);

    when(queueManager.getQueueByFullName("root.agroup")).thenReturn(agroup);
    when(queueManager.getQueueByFullName("root.bsubgroup2"))
        .thenReturn(bsubgroup2);
    when(queueManager.getQueueByFullName("root.asubgroup2"))
        .thenReturn(asubgroup2);
    when(queueManager.getQueueByFullName("root.agroup.a")).thenReturn(a);
    when(queueManager.getQueueByFullName("root.bsubgroup2.b")).thenReturn(b);
    when(queueManager.getQueueByFullName("root.managedParent"))
        .thenReturn(managedParent);


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
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("%secondary_group").build())
                .inputUser("a")
                .expectedQueue("asubgroup2")
                .expectedParentQueue("root")
                .build());

    // PrimaryGroupMapping.class returns only primary group, no secondary groups
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        PrimaryGroupMapping.class, GroupMappingServiceProvider.class);
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("%secondary_group")
                                .build())
                .inputUser("a")
                .expectedQueue("default")
                .build());
  }

  @Test
  public void testNullGroupMapping() throws YarnException {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        NullGroupsMapping.class, GroupMappingServiceProvider.class);
    try {
      verifyQueueMapping(
          QueueMappingTestDataBuilder.create()
                  .queueMapping(QueueMappingBuilder.create()
                                  .type(MappingType.USER)
                                  .source("%user")
                                  .queue("%secondary_group")
                                  .build())
                  .inputUser("a")
                  .expectedQueue("default")
                  .build());
      fail("No Groups for user 'a'");
    } catch (YarnException e) {
      // Exception is expected as there are no groups for given user
    }
  }

  @Test
  public void testMapping() throws YarnException {
    //if a mapping rule defines no parent, we cannot expect auto creation,
    // so we must provide already existing queues
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("a")
                                .queue("a")
                                .build())
                .inputUser("a")
                .expectedQueue("a")
                .build());
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.GROUP)
                                .source("agroup")
                                .queue("a")
                                .build())
                .inputUser("a")
                .expectedQueue("a")
                .build());
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("b")
                                .build())
                .inputUser("a")
                .expectedQueue("b")
                .build());
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("%user")
                                .build())
                .inputUser("a")
                .expectedQueue("a")
                .build());
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("%primary_group")
                                .build())
                .inputUser("a")
                .expectedQueue("agroup")
                .expectedParentQueue("root")
                .build());
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("%user")
                                .parentQueue("%primary_group")
                                .build())
                .inputUser("a")
                .expectedQueue("a")
                .expectedParentQueue("root.agroup")
                .build());
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("%user")
                                .parentQueue("%secondary_group")
                                .build())
                .inputUser("b")
                .expectedQueue("b")
                .expectedParentQueue("root.bsubgroup2")
                .build());
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.GROUP)
                                .source("asubgroup1")
                                .queue("a")
                                .build())
                .inputUser("a")
                .expectedQueue("a")
                .build());

    // "agroup" queue exists
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("%primary_group")
                                .parentQueue("root")
                                .build())
                .inputUser("a")
                .expectedQueue("agroup")
                .expectedParentQueue("root")
                .build());

    // "abcgroup" queue doesn't exist, %primary_group queue, not managed parent
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("%primary_group")
                                .parentQueue("bsubgroup2")
                                .build())
                .inputUser("abc")
                .expectedQueue("default")
                .build());

    // "abcgroup" queue doesn't exist, %primary_group queue, managed parent
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("%primary_group")
                                .parentQueue("managedParent")
                                .build())
                .inputUser("abc")
                .expectedQueue("abcgroup")
                .expectedParentQueue("root.managedParent")
                .build());

    // "abcgroup" queue doesn't exist, %secondary_group queue
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("%secondary_group")
                                .parentQueue("bsubgroup2")
                                .build())
                .inputUser("abc")
                .expectedQueue("default")
                .build());

    // "asubgroup2" queue exists, %secondary_group queue
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("%user")
                                .queue("%secondary_group")
                                .parentQueue("root")
                                .build())
                .inputUser("a")
                .expectedQueue("asubgroup2")
                .expectedParentQueue("root")
                .build());

    // specify overwritten, and see if user specified a queue, and it will be
    // overridden
   verifyQueueMapping(
       QueueMappingTestDataBuilder.create()
               .queueMapping(QueueMappingBuilder.create()
                               .type(MappingType.USER)
                               .source("user")
                               .queue("a")
                               .build())
               .inputUser("user")
               .inputQueue("b")
               .expectedQueue("a")
               .overwrite(true)
               .build());

   // if overwritten not specified, it should be which user specified
   verifyQueueMapping(
       QueueMappingTestDataBuilder.create()
               .queueMapping(QueueMappingBuilder.create()
                               .type(MappingType.USER)
                               .source("user")
                               .queue("a")
                               .build())
               .inputUser("user")
               .inputQueue("b")
               .expectedQueue("b")
               .build());

   // if overwritten not specified, it should be which user specified
   verifyQueueMapping(
      QueueMappingTestDataBuilder.create()
              .queueMapping(QueueMappingBuilder.create()
                              .type(MappingType.GROUP)
                              .source("usergroup")
                              .queue("%user")
                              .parentQueue("usergroup")
                              .build())
              .inputUser("user")
              .inputQueue("a")
              .expectedQueue("a")
              .build());

   // if overwritten not specified, it should be which user specified
   verifyQueueMapping(
      QueueMappingTestDataBuilder.create()
              .queueMapping(QueueMappingBuilder.create()
                              .type(MappingType.GROUP)
                              .source("usergroup")
                              .queue("b")
                              .parentQueue("root.bsubgroup2")
                              .build())
              .inputUser("user")
              .inputQueue("a")
              .expectedQueue("b")
              .overwrite(true)
              .build());

   // If user specific queue is enabled for a specified group under a given
   // parent queue
   verifyQueueMapping(
      QueueMappingTestDataBuilder.create()
              .queueMapping(QueueMappingBuilder.create()
                              .type(MappingType.GROUP)
                              .source("agroup")
                              .queue("%user")
                              .parentQueue("root.agroup")
                              .build())
              .inputUser("a")
              .expectedQueue("a")
              .build());

   // If user specific queue is enabled for a specified group without parent
   // queue
   verifyQueueMapping(
      QueueMappingTestDataBuilder.create()
              .queueMapping(QueueMappingBuilder.create()
                              .type(MappingType.GROUP)
                              .source("agroup")
                              .queue("%user")
                              .build())
              .inputUser("a")
              .expectedQueue("a")
              .build());
  }

  /**
   * Queue Mapping test class to prepare the test data.
   *
   */
  public static final class QueueMappingTestData {

    private QueueMapping queueMapping;
    private String inputUser;
    private String inputQueue;
    private String expectedQueue;
    private boolean overwrite;
    private String expectedParentQueue;

    private QueueMappingTestData(QueueMappingTestDataBuilder builder) {
      this.queueMapping = builder.queueMapping;
      this.inputUser = builder.inputUser;
      this.inputQueue = builder.inputQueue;
      this.expectedQueue = builder.expectedQueue;
      this.overwrite = builder.overwrite;
      this.expectedParentQueue = builder.expectedParentQueue;
    }

    /**
     * Builder class to prepare the Queue Mapping test data.
     *
     */
    public static class QueueMappingTestDataBuilder {

      private QueueMapping queueMapping = null;
      private String inputUser = null;
      private String inputQueue = YarnConfiguration.DEFAULT_QUEUE_NAME;
      private String expectedQueue = null;
      private boolean overwrite = false;
      private String expectedParentQueue = null;

      public QueueMappingTestDataBuilder() {

      }

      public static QueueMappingTestDataBuilder create() {
        return new QueueMappingTestDataBuilder();
      }

      public QueueMappingTestDataBuilder queueMapping(QueueMapping mapping) {
        this.queueMapping = mapping;
        return this;
      }

      public QueueMappingTestDataBuilder inputUser(String user) {
        this.inputUser = user;
        return this;
      }

      public QueueMappingTestDataBuilder inputQueue(String queue) {
        this.inputQueue = queue;
        return this;
      }

      public QueueMappingTestDataBuilder expectedQueue(String outputQueue) {
        this.expectedQueue = outputQueue;
        return this;
      }

      public QueueMappingTestDataBuilder overwrite(boolean overwriteMappings) {
        this.overwrite = overwriteMappings;
        return this;
      }

      public QueueMappingTestDataBuilder expectedParentQueue(
          String outputParentQueue) {
        this.expectedParentQueue = outputParentQueue;
        return this;
      }

      public QueueMappingTestData build() {
        return new QueueMappingTestData(this);
      }
    }
  }
}
