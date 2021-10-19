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
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SimpleGroupsMapping;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.DOT;
import static org.mockito.ArgumentMatchers.isNull;
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

  private void createQueueHierarchy(
      CapacitySchedulerQueueManager queueManager) {
    MockQueueHierarchyBuilder.create()
        .withQueueManager(queueManager)
        .withQueue(ROOT_QUEUE + DOT + Q1_QUEUE)
        .withQueue(ROOT_QUEUE + DOT + Q2_QUEUE)
        .withQueue(ROOT_QUEUE + DOT + DEFAULT_QUEUE)
        .withQueue(ROOT_QUEUE + DOT + AMBIGUOUS_QUEUE)
        .withQueue(ROOT_QUEUE + DOT + USER_NAME + DOT + AMBIGUOUS_QUEUE)
        .build();

    when(queueManager.getQueue(isNull())).thenReturn(null);
  }

  private void verifyQueueMapping(QueueMapping queueMapping,
                                  String user, String expectedQueue)
      throws IOException, YarnException {
    verifyQueueMapping(queueMapping, user,
        queueMapping.getQueue(), expectedQueue, false);
  }

  private void verifyQueueMapping(QueueMapping queueMapping,
                                  String user, String inputQueue,
                                  String expectedQueue, boolean overwrite)
      throws IOException, YarnException {
    MappingRule rule = MappingRule.createLegacyRule(
        queueMapping.getType().toString(),
        queueMapping.getSource(),
        queueMapping.getFullPath());

    CSMappingPlacementRule engine = setupEngine(rule, overwrite);

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
    ApplicationPlacementContext ctx = engine.getPlacementForApp(asc,
        user);
    Assert.assertEquals(expectedQueue,
        ctx != null ? ctx.getQueue() : inputQueue);
  }

  CSMappingPlacementRule setupEngine(MappingRule rule, boolean override)
      throws IOException {

    CapacitySchedulerConfiguration csConf =
        mock(CapacitySchedulerConfiguration.class);
    when(csConf.getMappingRules()).thenReturn(Collections.singletonList(rule));
    when(csConf.getOverrideWithQueueMappings())
        .thenReturn(override);
    CapacitySchedulerQueueManager queueManager =
        mock(CapacitySchedulerQueueManager.class);

    createQueueHierarchy(queueManager);

    CSMappingPlacementRule engine = new CSMappingPlacementRule();
    Groups groups = new Groups(conf);

    CapacityScheduler cs = mock(CapacityScheduler.class);
    when(cs.getConfiguration()).thenReturn(csConf);
    when(cs.getCapacitySchedulerQueueManager()).thenReturn(queueManager);

    engine.setGroups(groups);
    engine.setFailOnConfigError(false);
    engine.initialize(cs);

    return engine;
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
  public void testSpecificAppNameMappedToDefinedQueue()
      throws IOException, YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME, Q1_QUEUE),
        USER_NAME, Q1_QUEUE);
  }

  @Test
  public void testPlaceholderAppSourceMappedToQueue()
      throws IOException, YarnException {
    verifyQueueMapping(getQueueMapping(APPLICATION_PLACEHOLDER, Q2_QUEUE),
        USER_NAME, Q2_QUEUE);
  }

  @Test
  public void testPlaceHolderAppSourceAndQueueMappedToAppNameQueue()
      throws IOException, YarnException {
    verifyQueueMapping(getQueueMapping(APPLICATION_PLACEHOLDER,
        APPLICATION_PLACEHOLDER), USER_NAME, APP_NAME);
  }

  @Test
  public void testQueueInMappingOverridesSpecifiedQueue()
      throws IOException, YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME,
        Q1_QUEUE), USER_NAME, Q2_QUEUE, Q1_QUEUE, true);
  }

  @Test
  public void testQueueInMappingDoesNotOverrideSpecifiedQueue()
      throws IOException, YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME,
        Q1_QUEUE), USER_NAME, Q2_QUEUE, Q2_QUEUE, false);
  }

  @Test
  public void testDefaultQueueInMappingIsNotUsedWithoutOverride()
      throws IOException, YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME,
        DEFAULT_QUEUE), USER_NAME, Q2_QUEUE, Q2_QUEUE, false);
  }

  @Test
  public void testDefaultQueueInMappingEqualsToInputQueue()
      throws IOException, YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME,
        DEFAULT_QUEUE), USER_NAME, DEFAULT_QUEUE, DEFAULT_QUEUE, false);
  }

  @Test
  public void testMappingSourceDiffersFromInputQueue()
      throws IOException, YarnException {
    verifyQueueMapping(getQueueMapping(MAPREDUCE_APP_NAME,
        Q1_QUEUE), USER_NAME, DEFAULT_QUEUE, DEFAULT_QUEUE, false);
  }

  @Test
  public void testMappingContainsAmbiguousLeafQueueWithoutParent()
      throws IOException, YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME, AMBIGUOUS_QUEUE),
        USER_NAME, DEFAULT_QUEUE, DEFAULT_QUEUE, false);
  }

  @Test
  public void testMappingContainsAmbiguousLeafQueueWithParent()
      throws IOException, YarnException {
    verifyQueueMapping(getQueueMapping(APP_NAME, ROOT_QUEUE, AMBIGUOUS_QUEUE),
        USER_NAME, DEFAULT_QUEUE, AMBIGUOUS_QUEUE, false);
  }

}
