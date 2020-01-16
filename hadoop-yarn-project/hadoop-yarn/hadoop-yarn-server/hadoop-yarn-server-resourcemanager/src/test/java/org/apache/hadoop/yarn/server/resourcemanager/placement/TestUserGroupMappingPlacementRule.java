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

import java.util.Arrays;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.MappingType;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.QueueMappingBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.placement.TestUserGroupMappingPlacementRule.QueueMappingTestData.QueueMappingTestDataBuilder;
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
  public void testMapping() throws YarnException {

    // simple base case for mapping user to queue
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.USER)
                                .source("a")
                                .queue("q1").build())
                .inputUser("a")
                .expectedQueue("q1")
                .build());
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                .queueMapping(QueueMappingBuilder.create()
                                .type(MappingType.GROUP)
                                .source("agroup")
                                .queue("q1").build())
                .inputUser("a")
                .expectedQueue("q1")
                .build());
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                 .queueMapping(QueueMappingBuilder.create()
                                 .type(MappingType.USER)
                                 .source("%user")
                                 .queue("q2").build())
                 .inputUser("a")
                 .expectedQueue("q2")
                 .build());

    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                  .queueMapping(QueueMappingBuilder.create()
                                   .type(MappingType.USER)
                                   .source("%user")
                                   .queue("%user").build())
                  .inputUser("a")
                  .expectedQueue("a")
                  .build());
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                  .queueMapping(QueueMappingBuilder.create()
                                    .type(MappingType.USER)
                                    .source("%user")
                                    .queue("%primary_group").build())
                  .inputUser("a")
                  .expectedQueue("agroup")
                  .build());

    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                  .queueMapping(QueueMappingBuilder.create()
                                      .type(MappingType.GROUP)
                                      .source("asubgroup1")
                                      .queue("q1").build())
                  .inputUser("a")
                  .expectedQueue("q1")
                  .build());
    
    // specify overwritten, and see if user specified a queue, and it will be
    // overridden
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                  .queueMapping(QueueMappingBuilder.create()
                                      .type(MappingType.USER)
                                      .source("user")
                                      .queue("q1").build())
                  .inputUser("user")
                  .inputQueue("q2")
                  .expectedQueue("q1")
                  .overwrite(true)
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
                      .inputQueue("default")
                      .expectedQueue("user")
                      .build());

    // if overwritten not specified, it should be which user specified
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                      .queueMapping(QueueMappingBuilder.create().
                                              type(MappingType.GROUP)
                                              .source("usergroup")
                                              .queue("%user")
                                              .parentQueue("usergroup")
                                              .build())
                      .inputUser("user")
                      .inputQueue("agroup")
                      .expectedQueue("user")
                      .overwrite(true)
                      .build());

    //If user specific queue is enabled for a specified group under a given
    // parent queue
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                      .queueMapping(QueueMappingBuilder.create()
                                            .type(MappingType.GROUP)
                                            .source("agroup")
                                            .queue("%user")
                                            .parentQueue("parent1")
                                            .build())
                      .inputUser("a")
                      .expectedQueue("a")
                      .build());

    //If user specific queue is enabled for a specified group without parent
    // queue
    verifyQueueMapping(
        QueueMappingTestDataBuilder.create()
                      .queueMapping(QueueMappingBuilder.create()
                                            .type(MappingType.GROUP)
                                            .source("agroup")
                                            .queue("%user").build())
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
