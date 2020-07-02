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

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SimpleGroupsMapping;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestAppNameMappingPlacementRule {
  private static final String ROOT_QUEUE = "root";
  private static final String Q2_QUEUE = "q2";
  private static final String Q1_QUEUE = "q1";
  private static final String USER_NAME = "user";
  private static final String DEFAULT_QUEUE = "default";
  private static final String APPLICATION_PLACEHOLDER = "%application";
  private static final String AMBIGUOUS_QUEUE = "ambiguousQueue";
  private static final String APP_NAME = "DistributedShell";
  private static final String MAPREDUCE_APP_NAME = "MAPREDUCE";

  private YarnConfiguration conf = new YarnConfiguration();

  @Before
  public void setup() {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
  }

  private void verifyQueueMapping(QueueMapping queueMapping,
      String user, String expectedQueue) throws YarnException {
    verifyQueueMapping(queueMapping, user,
        queueMapping.getQueue(), expectedQueue, false);
  }

  private void verifyQueueMapping(QueueMapping queueMapping,
      String user, String inputQueue, String expectedQueue,
      boolean overwrite) throws YarnException {
    AppNameMappingPlacementRule rule = new AppNameMappingPlacementRule(
        overwrite, Arrays.asList(queueMapping));

    CapacitySchedulerQueueManager qm =
        mock(CapacitySchedulerQueueManager.class);
    when(qm.isAmbiguous(Mockito.isA(String.class))).thenReturn(false);
    when(qm.isAmbiguous(AMBIGUOUS_QUEUE)).thenReturn(true);

    rule.queueManager = qm;

    ApplicationSubmissionContext asc = Records.newRecord(
        ApplicationSubmissionContext.class);
    if (inputQueue.equals(APPLICATION_PLACEHOLDER)) {
      inputQueue = APP_NAME;
    }
    asc.setQueue(inputQueue);
    String appName = queueMapping.getSource();
    // to create a scenario when source != appName
    if (appName.equals(APPLICATION_PLACEHOLDER)
        || appName.equals(MAPREDUCE_APP_NAME)) {
      appName = APP_NAME;
    }
    asc.setApplicationName(appName);
    ApplicationPlacementContext ctx = rule.getPlacementForApp(asc,
        user);
    Assert.assertEquals(expectedQueue,
        ctx != null ? ctx.getQueue() : inputQueue);
  }

  public QueueMapping getQueueMapping(String source, String queue) {
    return getQueueMapping(source, null, queue);
  }

  public QueueMapping getQueueMapping(String source, String parent,
      String queue) {
    return QueueMapping.QueueMappingBuilder.create()
        .type(QueueMapping.MappingType.APPLICATION)
        .source(source)
        .queue(queue)
        .parentQueue(parent)
        .build();
  }

  @Test
  public void testSpecificAppNameMappedToDefinedQueue() throws YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME, Q1_QUEUE),
        USER_NAME, Q1_QUEUE);
  }

  @Test
  public void testPlaceholderAppSourceMappedToQueue() throws YarnException {
    verifyQueueMapping(getQueueMapping(APPLICATION_PLACEHOLDER, Q2_QUEUE),
        USER_NAME, Q2_QUEUE);
  }

  @Test
  public void testPlaceHolderAppSourceAndQueueMappedToAppNameQueue()
      throws YarnException {
    verifyQueueMapping(getQueueMapping(APPLICATION_PLACEHOLDER,
        APPLICATION_PLACEHOLDER), USER_NAME, APP_NAME);
  }

  @Test
  public void testQueueInMappingOverridesSpecifiedQueue()
      throws YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME,
        Q1_QUEUE), USER_NAME, Q2_QUEUE, Q1_QUEUE, true);
  }

  @Test
  public void testQueueInMappingDoesNotOverrideSpecifiedQueue()
      throws YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME,
        Q1_QUEUE), USER_NAME, Q2_QUEUE, Q2_QUEUE, false);
  }

  @Test
  public void testDefaultQueueInMappingIsNotUsedWithoutOverride()
      throws YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME,
        DEFAULT_QUEUE), USER_NAME, Q2_QUEUE, Q2_QUEUE, false);
  }

  @Test
  public void testDefaultQueueInMappingEqualsToInputQueue()
      throws YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME,
        DEFAULT_QUEUE), USER_NAME, DEFAULT_QUEUE, DEFAULT_QUEUE, false);
  }

  @Test
  public void testMappingSourceDiffersFromInputQueue() throws YarnException {
    verifyQueueMapping(getQueueMapping(MAPREDUCE_APP_NAME,
        Q1_QUEUE), USER_NAME, DEFAULT_QUEUE, DEFAULT_QUEUE, false);
  }

  @Test(expected = YarnException.class)
  public void testMappingContainsAmbiguousLeafQueueWithoutParent()
      throws YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME, AMBIGUOUS_QUEUE),
        USER_NAME, DEFAULT_QUEUE, DEFAULT_QUEUE, false);
  }

  @Test
  public void testMappingContainsAmbiguousLeafQueueWithParent()
      throws YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME, ROOT_QUEUE, AMBIGUOUS_QUEUE),
        USER_NAME, DEFAULT_QUEUE, AMBIGUOUS_QUEUE, false);
  }

}