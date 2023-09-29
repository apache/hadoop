/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt
    .RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerDynamicEditException;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common
    .QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .SchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy
    .FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.security
    .ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security
    .NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security
    .RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for creation and reinitialization of auto created leaf queues
 * and capacity management under a ManagedParentQueue.
 */
public class TestCapacitySchedulerAutoQueueCreation
    extends TestCapacitySchedulerAutoCreatedQueueBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestCapacitySchedulerAutoQueueCreation.class);

  private static final String CURRENT_USER_MAPPING = "%user";

  private static final Resource TEMPLATE_MAX_RES = Resource.newInstance(16 *
          GB,
      48);
  private static final Resource TEMPLATE_MIN_RES = Resource.newInstance(1638,
      4);


  @Test(timeout = 20000)
  public void testAutoCreateLeafQueueCreation() throws Exception {

    try {
      // submit an app
      submitApp(mockRM, cs.getQueue(PARENT_QUEUE), USER0, USER0, 1, 1);

      // check preconditions
      List<ApplicationAttemptId> appsInC = cs.getAppsInQueue(PARENT_QUEUE);
      assertEquals(1, appsInC.size());
      assertNotNull(cs.getQueue(USER0));

      AutoCreatedLeafQueue autoCreatedLeafQueue =
          (AutoCreatedLeafQueue) cs.getQueue(USER0);
      ManagedParentQueue parentQueue = (ManagedParentQueue) cs.getQueue(
          PARENT_QUEUE);
      assertEquals(parentQueue, autoCreatedLeafQueue.getParent());

      Map<String, Float> expectedChildQueueAbsCapacity =
      populateExpectedAbsCapacityByLabelForParentQueue(1);
      validateInitialQueueEntitlement(parentQueue, USER0,
          expectedChildQueueAbsCapacity, accessibleNodeLabelsOnC);

      // The new queue calculation mode works from the effective resources
      // so the absoluteCapacity and the maxApplications differs a little
      // bit: 6553/16384=0.3999633789 vs 0.4
      final int maxApps = cs.getConfiguration().isLegacyQueueMode() ? 4000 : 3999;
      validateUserAndAppLimits(autoCreatedLeafQueue, maxApps, maxApps);
      validateContainerLimits(autoCreatedLeafQueue, 6, 10240);

      assertTrue(autoCreatedLeafQueue
          .getOrderingPolicy() instanceof FairOrderingPolicy);

      setupGroupQueueMappings("d", cs.getConfiguration(), "%user");
      cs.reinitialize(cs.getConfiguration(), mockRM.getRMContext());

      submitApp(mockRM, cs.getQueue("d"), TEST_GROUPUSER, TEST_GROUPUSER, 1, 1);
      autoCreatedLeafQueue =
          (AutoCreatedLeafQueue) cs.getQueue(TEST_GROUPUSER);
      parentQueue = (ManagedParentQueue) cs.getQueue("d");
      assertEquals(parentQueue, autoCreatedLeafQueue.getParent());

      expectedChildQueueAbsCapacity =
          new HashMap<String, Float>() {{
            put(NO_LABEL, 0.02f);
          }};

      validateInitialQueueEntitlement(parentQueue, TEST_GROUPUSER,
          expectedChildQueueAbsCapacity,
          new HashSet<String>() {{ add(NO_LABEL); }});

    } finally {
      cleanupQueue(USER0);
      cleanupQueue(TEST_GROUPUSER);
    }
  }

  @Test(timeout = 20000)
  public void testAutoCreateLeafQueueCreationSchedulerMaximumAllocation()
      throws Exception {
    try {
      // Check the minimum/maximum allocation settings via the
      // yarn.scheduler.minimum/maximum-allocation-mb/vcore property
      setSchedulerMinMaxAllocation(cs.getConfiguration());
      cs.getConfiguration().setAutoCreatedLeafQueueConfigMaximumAllocation(C,
          "memory-mb=18384,vcores=8");
      cs.reinitialize(cs.getConfiguration(), mockRM.getRMContext());

      // submit an app
      submitApp(mockRM, cs.getQueue(PARENT_QUEUE), USER0, USER0, 1, 1);

      // check preconditions
      List<ApplicationAttemptId> appsInC = cs.getAppsInQueue(PARENT_QUEUE);
      assertEquals(1, appsInC.size());
      assertNotNull(cs.getQueue(USER0));

      AutoCreatedLeafQueue autoCreatedLeafQueue =
          (AutoCreatedLeafQueue) cs.getQueue(USER0);

      validateContainerLimits(autoCreatedLeafQueue, 8, 18384);
    } finally {
      cleanupQueue(USER0);
      cleanupQueue(TEST_GROUPUSER);
    }
  }

  @Test(timeout = 20000)
  public void testAutoCreateLeafQueueCreationUsingFullParentPath()
      throws Exception {

    try {
      setupGroupQueueMappings("root.d", cs.getConfiguration(), "%user");
      cs.reinitialize(cs.getConfiguration(), mockRM.getRMContext());

      submitApp(mockRM, cs.getQueue("d"), TEST_GROUPUSER, TEST_GROUPUSER, 1, 1);
      AutoCreatedLeafQueue autoCreatedLeafQueue =
          (AutoCreatedLeafQueue) cs.getQueue(TEST_GROUPUSER);
      ManagedParentQueue parentQueue = (ManagedParentQueue) cs.getQueue("d");
      assertEquals(parentQueue, autoCreatedLeafQueue.getParent());

      Map<String, Float> expectedChildQueueAbsCapacity =
          new HashMap<String, Float>() {{
            put(NO_LABEL, 0.02f);
          }};

      validateInitialQueueEntitlement(parentQueue, TEST_GROUPUSER,
          expectedChildQueueAbsCapacity,
          new HashSet<String>() {{ add(NO_LABEL); }});

    } finally {
      cleanupQueue(USER0);
      cleanupQueue(TEST_GROUPUSER);
    }
  }

  @Test
  public void testReinitializeStoppedAutoCreatedLeafQueue() throws Exception {
    try {
      String host = "127.0.0.1";
      RMNode node = MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1,
          host);
      cs.handle(new NodeAddedSchedulerEvent(node));

      // submit an app

      MockRMAppSubmissionData data1 =
          MockRMAppSubmissionData.Builder.createWithMemory(GB, mockRM)
              .withAppName("test-auto-queue-creation-1")
              .withUser(USER0)
              .withAcls(null)
              .withQueue(USER0)
              .withUnmanagedAM(false)
              .build();
      RMApp app1 = MockRMAppSubmitter.submit(mockRM, data1);

      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(GB, mockRM)
              .withAppName("test-auto-queue-creation-2")
              .withUser(USER1)
              .withAcls(null)
              .withQueue(USER1)
              .withUnmanagedAM(false)
              .build();
      RMApp app2 = MockRMAppSubmitter.submit(mockRM, data);
      // check preconditions
      List<ApplicationAttemptId> appsInC = cs.getAppsInQueue(PARENT_QUEUE);
      assertEquals(2, appsInC.size());

      assertNotNull(cs.getQueue(USER0));
      assertNotNull(cs.getQueue(USER1));

      AutoCreatedLeafQueue user0Queue = (AutoCreatedLeafQueue) cs.getQueue(
          USER0);
      AutoCreatedLeafQueue user1Queue = (AutoCreatedLeafQueue) cs.getQueue(
          USER0);
      ManagedParentQueue parentQueue = (ManagedParentQueue) cs.getQueue(
          PARENT_QUEUE);

      assertEquals(parentQueue, user0Queue.getParent());
      assertEquals(parentQueue, user1Queue.getParent());

      Map<String, Float>
      expectedAbsChildQueueCapacity =
      populateExpectedAbsCapacityByLabelForParentQueue(2);
      validateInitialQueueEntitlement(parentQueue, USER0,
          expectedAbsChildQueueCapacity, accessibleNodeLabelsOnC);
      validateInitialQueueEntitlement(parentQueue, USER1,
          expectedAbsChildQueueCapacity, accessibleNodeLabelsOnC);

      ApplicationAttemptId appAttemptId = appsInC.get(0);

      Priority priority = TestUtils.createMockPriority(1);
      RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(
          null);
      ResourceRequest r1 = TestUtils.createResourceRequest(ResourceRequest.ANY,
          1 * GB, 1, true, priority,
          recordFactory);

      cs.allocate(appAttemptId, Collections.<ResourceRequest>singletonList(r1),
          null, Collections.<ContainerId>emptyList(), Collections.singletonList(host),
          null, NULL_UPDATE_REQUESTS);

      //And this will result in container assignment for app1
      CapacityScheduler.schedule(cs);

      //change state to draining
      user0Queue.stopQueue();

      cs.killAllAppsInQueue(USER0);

      mockRM.waitForState(appAttemptId, RMAppAttemptState.KILLED);

      mockRM.waitForState(appAttemptId.getApplicationId(), RMAppState.KILLED);

      //change state to stopped
      user0Queue.stopQueue();
      assertEquals(QueueState.STOPPED,
          user0Queue.getQueueInfo().getQueueState());

      cs.reinitialize(cs.getConf(), mockRM.getRMContext());

      AutoCreatedLeafQueue user0QueueReinited =
          (AutoCreatedLeafQueue) cs.getQueue(USER0);

      validateCapacities(user0QueueReinited, 0.0f, 0.0f, 1.0f, 1.0f);

      AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) cs.getQueue(
          USER1);

      expectedAbsChildQueueCapacity =
          populateExpectedAbsCapacityByLabelForParentQueue(1);

      validateInitialQueueEntitlement(parentQueue, leafQueue.getQueuePath(),
          expectedAbsChildQueueCapacity, accessibleNodeLabelsOnC);

    } finally {
      cleanupQueue(USER0);
    }
  }

  @Test
  public void testConvertAutoCreateDisabledOnManagedParentQueueFails()
      throws Exception {
    CapacityScheduler newCS = new CapacityScheduler();
    try {
      CapacitySchedulerConfiguration newConf = setupSchedulerConfiguration();
      setupQueueConfiguration(newConf);

      newConf.setAutoCreateChildQueueEnabled(C, false);

      newCS.setConf(new YarnConfiguration());
      newCS.setRMContext(mockRM.getRMContext());
      newCS.init(cs.getConf());
      newCS.start();

      newCS.reinitialize(newConf,
          new RMContextImpl(null, null, null, null, null, null,
              new RMContainerTokenSecretManager(newConf),
              new NMTokenSecretManagerInRM(newConf),
              new ClientToAMTokenSecretManagerInRM(), null));

    } catch (IOException e) {
      //expected exception
    } finally {
      newCS.stop();
    }
  }

  @Test
  public void testConvertLeafQueueToParentQueueWithAutoCreate()
      throws Exception {
    CapacityScheduler newCS = new CapacityScheduler();
    try {
      CapacitySchedulerConfiguration newConf = setupSchedulerConfiguration();
      setupQueueConfiguration(newConf);
      newConf.setAutoCreatedLeafQueueConfigCapacity(A1, A1_CAPACITY / 10);
      newConf.setAutoCreateChildQueueEnabled(A1, true);

      newCS.setConf(new YarnConfiguration());
      newCS.setRMContext(mockRM.getRMContext());
      newCS.init(cs.getConf());
      newCS.start();

      final LeafQueue a1Queue = (LeafQueue) newCS.getQueue("a1");
      a1Queue.stopQueue();

      newCS.reinitialize(newConf,
          new RMContextImpl(null, null, null, null, null, null,
              new RMContainerTokenSecretManager(newConf),
              new NMTokenSecretManagerInRM(newConf),
              new ClientToAMTokenSecretManagerInRM(), null));

    } finally {
      newCS.stop();
    }
  }

  @Test
  public void testConvertFailsFromParentQueueToManagedParentQueue()
      throws Exception {
    CapacityScheduler newCS = new CapacityScheduler();
    try {
      CapacitySchedulerConfiguration newConf = setupSchedulerConfiguration();
      setupQueueConfiguration(newConf);
      newConf.setAutoCreatedLeafQueueConfigCapacity(A, A_CAPACITY / 10);
      newConf.setAutoCreateChildQueueEnabled(A, true);

      newCS.setConf(new YarnConfiguration());
      newCS.setRMContext(mockRM.getRMContext());
      newCS.init(cs.getConf());
      newCS.start();

      final ParentQueue a1Queue = (ParentQueue) newCS.getQueue("a");
      a1Queue.stopQueue();

      newCS.reinitialize(newConf,
          new RMContextImpl(null, null, null, null, null, null,
              new RMContainerTokenSecretManager(newConf),
              new NMTokenSecretManagerInRM(newConf),
              new ClientToAMTokenSecretManagerInRM(), null));

      fail("Expected exception while converting a parent queue to"
          + " an auto create enabled parent queue");
    } catch (IOException e) {
      //expected exception
    } finally {
      newCS.stop();
    }
  }

  @Test(timeout = 10000)
  public void testAutoCreateLeafQueueFailsWithNoQueueMapping()
      throws Exception {

    final String INVALID_USER = "invalid_user";

    // submit an app under a different queue name which does not exist
    // and queue mapping does not exist for this user
    RMApp app = MockRMAppSubmitter.submit(mockRM,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, mockRM)
            .withAppName("app")
            .withUser(INVALID_USER)
            .withAcls(null)
            .withQueue(INVALID_USER)
            .withWaitForAppAcceptedState(false)
            .build());
    mockRM.drainEvents();
    mockRM.waitForState(app.getApplicationId(), RMAppState.FAILED);
    assertEquals(RMAppState.FAILED, app.getState());
  }

  @Test(timeout = 10000)
  public void testQueueMappingValidationFailsWithInvalidParentQueueInMapping()
      throws Exception {

    MockRM newMockRM = setupSchedulerInstance();
    try {
      CapacityScheduler newCS =
          (CapacityScheduler) newMockRM.getResourceScheduler();

      //"a" is not auto create enabled

      //dynamic queue mapping
      try {
        setupQueueMapping(newCS, CURRENT_USER_MAPPING, "a1",
            CURRENT_USER_MAPPING);
        newCS.updatePlacementRules();
        fail("Expected invalid parent queue mapping failure");

      } catch (IOException e) {
        //expected exception

        assertTrue(e.getMessage().contains(
            "Queue path 'a1.%user' is invalid because 'root.a.a1' " +
                "is a leaf queue"));
      }

      //"a" is not auto create enabled and app_user does not exist as a leaf
      // queue
      //static queue mapping
      try {
        setupQueueMapping(newCS, "app_user", "INVALID_PARENT_QUEUE",
            "app_user");
        newCS.updatePlacementRules();
        fail("Expected invalid parent queue mapping failure");
      } catch (IOException e) {
        //expected exception
        assertTrue(e.getMessage().contains(
            "Path root 'INVALID_PARENT_QUEUE' does not exist."));
      }
    } finally {
      if (newMockRM != null) {
        ((CapacityScheduler) newMockRM.getResourceScheduler()).stop();
        newMockRM.stop();
      }
    }
  }

  @Test(timeout = 10000)
  public void testQueueMappingUpdatesFailsOnRemovalOfParentQueueInMapping()
      throws Exception {

    MockRM newMockRM = setupSchedulerInstance();

    try {
      CapacityScheduler newCS =
          (CapacityScheduler) newMockRM.getResourceScheduler();

      setupQueueMapping(newCS, CURRENT_USER_MAPPING, "c", CURRENT_USER_MAPPING);
      newCS.updatePlacementRules();

      try {
        setupQueueMapping(newCS, CURRENT_USER_MAPPING, "nonexistent",
            CURRENT_USER_MAPPING);
        newCS.updatePlacementRules();
        fail("Expected invalid parent queue mapping failure");
      } catch (IOException e) {
        //expected exception
        assertTrue(
            e.getMessage().contains("Path root 'nonexistent' does not exist."));
      }
    } finally {
      if (newMockRM != null) {
        ((CapacityScheduler) newMockRM.getResourceScheduler()).stop();
        newMockRM.stop();
      }
    }
  }

  @Test
  public void testParentQueueUpdateInQueueMappingFailsAfterAutoCreation()
      throws Exception {

    MockRM newMockRM = setupSchedulerInstance();
    CapacityScheduler newCS =
        (CapacityScheduler) newMockRM.getResourceScheduler();

    try {
      submitApp(newCS, USER0, USER0, PARENT_QUEUE);

      assertNotNull(newCS.getQueue(USER0));

      //The new placement engine's validation is a bit more
      //strict so it would reject the original u:user_0:a.user_0 rule since
      //it checks if that paths exists or is a managed parent, but if we use
      //a.%user we can trick the engine, since it cannot validate if the actual
      //value of the %user will exist or not, it allows the rule
      setupQueueMapping(newCS, USER0, "a", "%user");
      newCS.updatePlacementRules();

      RMContext rmContext = mock(RMContext.class);
      when(rmContext.getDispatcher()).thenReturn(dispatcher);
      newCS.setRMContext(rmContext);

      ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
      //The new engine would return root.a as the parent queue for this
      // submission so creating the ApplicationPlacementContext accordingly
      SchedulerEvent addAppEvent = new AppAddedSchedulerEvent(appId, USER0,
          USER0, new ApplicationPlacementContext(USER0, "root.a"));
      newCS.handle(addAppEvent);

      RMAppEvent event = new RMAppEvent(appId, RMAppEventType.APP_REJECTED,
          "error");
      dispatcher.spyOnNextEvent(event, 10000);
    } finally {
      if (newMockRM != null) {
        ((CapacityScheduler) newMockRM.getResourceScheduler()).stop();
        newMockRM.stop();
      }
    }
  }


  @Test
  public void testAutoQueueCreationFailsForEmptyPathWithAQCAndWeightMode()
      throws Exception {
    if (mockRM != null) {
      mockRM.stop();
    }

    //We need a special configuration we only need a V2 queue auto creation
    //And weight mode, to allow dynamic auto queue creation for root
    CapacitySchedulerConfiguration conf = setupSchedulerConfiguration();
    conf.setAutoQueueCreationV2Enabled(ROOT, true);
    conf.setCapacity("root.default", "1w");
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    //Just a regular mockRM and CapacityScheduler instance
    MockRM newMockRM = new MockRM(conf);
    newMockRM.start();
    ((CapacityScheduler) newMockRM.getResourceScheduler()).start();

    CapacityScheduler newCS =
        (CapacityScheduler) newMockRM.getResourceScheduler();

    try {
      //submitting to root..user, this should fail WITHOUT crashing the RM
      submitApp(newCS, USER0, "user", "root.");

      RMContext rmContext = mock(RMContext.class);
      when(rmContext.getDispatcher()).thenReturn(dispatcher);
      newCS.setRMContext(rmContext);

      ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
      SchedulerEvent addAppEvent = new AppAddedSchedulerEvent(
          appId, "user", USER0);
      newCS.handle(addAppEvent);

      RMAppEvent event = new RMAppEvent(appId, RMAppEventType.APP_REJECTED,
          "error");
      dispatcher.spyOnNextEvent(event, 10000);
    } finally {
      ((CapacityScheduler) newMockRM.getResourceScheduler()).stop();
      newMockRM.stop();
    }
  }


  /**
   * This test case checks if a mapping rule can put an application to an auto
   * created queue even if an other queue with the same leaf name already
   * exists.
   *
   * In this scenario we use the following queues
   *   root.a.a1 - already existing queue
   *   root.c    - managed parent queue
   *
   * And the following mapping rule
   *   u:%user:root.c.%user - Any submission should go to root.c.USERNAME queue
   *
   * When user 'a1' submits a new application we expect it to go to 'root.c.a1'
   * because of the mapping rule, and the queue should be created.
   *
   * @throws Exception - When stuff go wrong, obvious reason to fail the test
   */
  @Test
  public void testAutoQueueCreationWhenQueueExistsWithSameName()
      throws Exception {

    MockRM newMockRM = setupSchedulerInstance();
    CapacityScheduler newCS =
        (CapacityScheduler) newMockRM.getResourceScheduler();

    try {
      setupQueueMapping(newCS, "%user", "root.c", "%user");
      newCS.updatePlacementRules();

      //making sure the target queue does not exist before submission
      assertNull(newCS.getQueue("root.c.a1"));
      RMApp app = MockRMAppSubmitter.submit(newMockRM,
          MockRMAppSubmissionData.Builder.createWithMemory(512, newMockRM)
              .withAppName("testAutoQueueCreationWhenQueueExistsWithSameName")
              .withUser("a1")
              .withQueue("default")
              .build());
      RMAppAttempt attempt = MockRM.waitForAttemptScheduled(app, newMockRM);
      //checking if the target queue have been created during the submission
      assertNotNull(newCS.getQueue("root.c.a1"));
      //making sure the application is indeed in the right queue
      assertEquals("root.c.a1", app.getQueue());
    } finally {
      if (newMockRM != null) {
        ((CapacityScheduler) newMockRM.getResourceScheduler()).stop();
        newMockRM.stop();
      }
    }
  }

  @Test
  public void testAutoCreationFailsWhenParentCapacityExceeded()
      throws Exception {
    MockRM newMockRM = setupSchedulerInstance();
    CapacityScheduler newCS =
        (CapacityScheduler) newMockRM.getResourceScheduler();

    try {
      CapacitySchedulerConfiguration conf = newCS.getConfiguration();
      conf.setShouldFailAutoQueueCreationWhenGuaranteedCapacityExceeded(C,
          true);

      newCS.reinitialize(conf, newMockRM.getRMContext());

      // Test add one auto created queue dynamically and manually modify
      // capacity
      ManagedParentQueue parentQueue = (ManagedParentQueue) newCS.getQueue("c");
      AutoCreatedLeafQueue c1 = new AutoCreatedLeafQueue(newCS.getQueueContext(), "c1",
          parentQueue);
      newCS.addQueue(c1);
      c1.setCapacity(0.5f);
      c1.setAbsoluteCapacity(c1.getParent().getAbsoluteCapacity() * 1f);
      c1.setMaxCapacity(1f);

      setEntitlement(c1, new QueueEntitlement(0.5f, 1f));

      AutoCreatedLeafQueue c2 = new AutoCreatedLeafQueue(newCS.getQueueContext(), "c2",
          parentQueue);
      newCS.addQueue(c2);
      setEntitlement(c2, new QueueEntitlement(0.5f, 1f));

      try {
        AutoCreatedLeafQueue c3 = new AutoCreatedLeafQueue(newCS.getQueueContext(), "c3",
            parentQueue);
        newCS.addQueue(c3);
        fail("Expected exception for auto queue creation failure");
      } catch (SchedulerDynamicEditException e) {
        //expected exception
      }
    } finally {
      if (newMockRM != null) {
        ((CapacityScheduler) newMockRM.getResourceScheduler()).stop();
        newMockRM.stop();
      }
    }
  }

  @Test
  public void testAutoCreatedQueueActivationDeactivation() throws Exception {

    try {
      CSQueue parentQueue = cs.getQueue(PARENT_QUEUE);

      //submit app1 as USER1
      ApplicationId user1AppId = submitApp(mockRM, parentQueue, USER1, USER1,
          1, 1);
      Map<String, Float> expectedAbsChildQueueCapacity =
          populateExpectedAbsCapacityByLabelForParentQueue(1);
      validateInitialQueueEntitlement(parentQueue, USER1,
          expectedAbsChildQueueCapacity, accessibleNodeLabelsOnC);

      //submit another app2 as USER2
      ApplicationId user2AppId = submitApp(mockRM, parentQueue, USER2, USER2, 2,
          1);

      expectedAbsChildQueueCapacity =
          populateExpectedAbsCapacityByLabelForParentQueue(2);
      validateInitialQueueEntitlement(parentQueue, USER2,
          expectedAbsChildQueueCapacity, accessibleNodeLabelsOnC);

      //submit another app3 as USER1
      submitApp(mockRM, parentQueue, USER1, USER1, 3, 2);

      //validate total activated abs capacity remains the same
      GuaranteedOrZeroCapacityOverTimePolicy autoCreatedQueueManagementPolicy =
          (GuaranteedOrZeroCapacityOverTimePolicy) ((ManagedParentQueue) parentQueue)
              .getAutoCreatedQueueManagementPolicy();

      for (String nodeLabel : accessibleNodeLabelsOnC) {
        assertEquals(expectedAbsChildQueueCapacity.get(nodeLabel),
            autoCreatedQueueManagementPolicy.getAbsoluteActivatedChildQueueCapacity(nodeLabel), EPSILON);
      }

      //submit user_3 app. This cant be allocated since there is no capacity
      // in NO_LABEL, SSD but can be in GPU label
      submitApp(mockRM, parentQueue, USER3, USER3, 4, 1);
      final CSQueue user3LeafQueue = cs.getQueue(USER3);
      validateCapacities((AutoCreatedLeafQueue) user3LeafQueue, 0.0f, 0.0f,
          1.0f, 1.0f);
      validateCapacitiesByLabel((ManagedParentQueue) parentQueue,
          (AutoCreatedLeafQueue)
          user3LeafQueue, NODEL_LABEL_GPU);

      assertEquals(0.2f, autoCreatedQueueManagementPolicy
          .getAbsoluteActivatedChildQueueCapacity(NO_LABEL), EPSILON);
      assertEquals(0.9f, autoCreatedQueueManagementPolicy.getAbsoluteActivatedChildQueueCapacity(NODEL_LABEL_GPU),
          EPSILON);

      //Verify that AMs can be allocated
      //Node 1 has SSD and default node label expression on C is SSD.
      //This validates that the default node label expression with SSD is set
      // on the AM attempt
      // and app attempt reaches ALLOCATED state for a dynamic queue 'USER1'
      mockRM.launchAM(mockRM.getRMContext().getRMApps().get(user1AppId),
          mockRM, nm1);

//      //deactivate USER2 queue
      cs.killAllAppsInQueue(USER2);
      mockRM.waitForState(user2AppId, RMAppState.KILLED);

      //Verify if USER_2 can be deactivated since it has no pending apps
      List<QueueManagementChange> queueManagementChanges =
          autoCreatedQueueManagementPolicy.computeQueueManagementChanges();

      ManagedParentQueue managedParentQueue = (ManagedParentQueue) parentQueue;
      managedParentQueue.
          validateAndApplyQueueManagementChanges(queueManagementChanges);

      validateDeactivatedQueueEntitlement(parentQueue, USER2,
          expectedAbsChildQueueCapacity, queueManagementChanges);

      //USER_3 should now get activated for SSD, NO_LABEL
      Set<String> expectedNodeLabelsUpdated = new HashSet<>();
      expectedNodeLabelsUpdated.add(NO_LABEL);
      expectedNodeLabelsUpdated.add(NODEL_LABEL_SSD);

      validateActivatedQueueEntitlement(parentQueue, USER3,
          expectedAbsChildQueueCapacity , queueManagementChanges, expectedNodeLabelsUpdated);

    } finally {
      cleanupQueue(USER1);
      cleanupQueue(USER2);
      cleanupQueue(USER3);
    }
  }

  @Test
  public void testClusterResourceUpdationOnAutoCreatedLeafQueues() throws
      Exception {

    MockRM newMockRM = setupSchedulerInstance();
    try {
      CapacityScheduler newCS =
          (CapacityScheduler) newMockRM.getResourceScheduler();

      CSQueue parentQueue = newCS.getQueue(PARENT_QUEUE);

      //submit app1 as USER1
      submitApp(newMockRM, parentQueue, USER1, USER1, 1, 1);
      Map<String, Float> expectedAbsChildQueueCapacity =
          populateExpectedAbsCapacityByLabelForParentQueue(1);
      validateInitialQueueEntitlement(newMockRM, newCS, parentQueue, USER1,
          expectedAbsChildQueueCapacity, accessibleNodeLabelsOnC);

      //submit another app2 as USER2
      ApplicationId user2AppId = submitApp(newMockRM, parentQueue, USER2, USER2, 2,
          1);
      expectedAbsChildQueueCapacity =
          populateExpectedAbsCapacityByLabelForParentQueue(2);
      validateInitialQueueEntitlement(newMockRM, newCS, parentQueue, USER2,
          expectedAbsChildQueueCapacity, accessibleNodeLabelsOnC);

      //validate total activated abs capacity remains the same
      GuaranteedOrZeroCapacityOverTimePolicy autoCreatedQueueManagementPolicy =
          (GuaranteedOrZeroCapacityOverTimePolicy) ((ManagedParentQueue)
              parentQueue)
              .getAutoCreatedQueueManagementPolicy();
      assertEquals(autoCreatedQueueManagementPolicy
          .getAbsoluteActivatedChildQueueCapacity(NO_LABEL), 0.2f, EPSILON);

      //submit user_3 app. This cant be scheduled since there is no capacity
      submitApp(newMockRM, parentQueue, USER3, USER3, 3, 1);
      final CSQueue user3LeafQueue = newCS.getQueue(USER3);
      validateCapacities((AutoCreatedLeafQueue) user3LeafQueue, 0.0f, 0.0f,
          1.0f, 1.0f);

      assertEquals(autoCreatedQueueManagementPolicy
          .getAbsoluteActivatedChildQueueCapacity(NO_LABEL), 0.2f, EPSILON);

      // add new NM.
      newMockRM.registerNode("127.0.0.3:1234", 125 * GB, 20);

      // There will be change in effective resource when nodes are added
      // since we deal with percentages

      Resource MAX_RES = Resources.addTo(TEMPLATE_MAX_RES, Resources.createResource(125 *
          GB, 20));

      Resource MIN_RES = Resources.createResource(14438, 6);

      Assert.assertEquals("Effective Min resource for USER3 is not correct",
          Resources.none(), user3LeafQueue.getQueueResourceQuotas()
              .getEffectiveMinResource());
      Assert.assertEquals("Effective Max resource for USER3 is not correct",
          MAX_RES, user3LeafQueue
              .getQueueResourceQuotas()
              .getEffectiveMaxResource());

      CSQueue user1LeafQueue = newCS.getQueue(USER1);
      CSQueue user2LeafQueue = newCS.getQueue(USER2);
      Assert.assertEquals("Effective Min resource for USER2 is not correct",
          MIN_RES, user1LeafQueue.getQueueResourceQuotas()
              .getEffectiveMinResource());
      Assert.assertEquals("Effective Max resource for USER2 is not correct",
          MAX_RES, user1LeafQueue.getQueueResourceQuotas().getEffectiveMaxResource());

      Assert.assertEquals("Effective Min resource for USER1 is not correct",
          MIN_RES, user2LeafQueue.getQueueResourceQuotas()
              .getEffectiveMinResource());
      Assert.assertEquals("Effective Max resource for USER1 is not correct",
          MAX_RES, user2LeafQueue.getQueueResourceQuotas()
              .getEffectiveMaxResource());

      // unregister one NM.
      newMockRM.unRegisterNode(nm3);
      Resource MIN_RES_UPDATED = Resources.createResource(12800, 2);
      Resource MAX_RES_UPDATED = Resources.createResource(128000, 20);

      // After loosing one NM, resources will reduce
      Assert.assertEquals("Effective Min resource for USER2 is not correct",
          MIN_RES_UPDATED, user1LeafQueue.getQueueResourceQuotas().getEffectiveMinResource
              ());
      Assert.assertEquals("Effective Max resource for USER2 is not correct",
          MAX_RES_UPDATED, user2LeafQueue.getQueueResourceQuotas()
              .getEffectiveMaxResource());

    } finally {
      cleanupQueue(USER1);
      cleanupQueue(USER2);
      cleanupQueue(USER3);
      if (newMockRM != null) {
        ((CapacityScheduler) newMockRM.getResourceScheduler()).stop();
        newMockRM.stop();
      }
    }
  }

  @Test
  public void testReinitializeQueuesWithAutoCreatedLeafQueues()
      throws Exception {

    MockRM newMockRM = setupSchedulerInstance();
    try {
      CapacityScheduler newCS =
          (CapacityScheduler) newMockRM.getResourceScheduler();
      CapacitySchedulerConfiguration conf = newCS.getConfiguration();

      CSQueue parentQueue = newCS.getQueue(PARENT_QUEUE);

      //submit app1 as USER1
      submitApp(newMockRM, parentQueue, USER1, USER1, 1, 1);

      Map<String, Float> expectedChildQueueAbsCapacity =
      populateExpectedAbsCapacityByLabelForParentQueue(1);
      validateInitialQueueEntitlement(newMockRM, newCS, parentQueue, USER1,
          expectedChildQueueAbsCapacity, accessibleNodeLabelsOnC);

      //submit another app2 as USER2
      ApplicationId user2AppId = submitApp(newMockRM, parentQueue, USER2,
          USER2, 2,
          1);
      expectedChildQueueAbsCapacity =
          populateExpectedAbsCapacityByLabelForParentQueue(2);
      validateInitialQueueEntitlement(newMockRM, newCS, parentQueue, USER2,
          expectedChildQueueAbsCapacity, accessibleNodeLabelsOnC);

      //update parent queue capacity
      conf.setCapacity(C, 30f);
      conf.setCapacity(D, 10f);
      conf.setMaximumCapacity(C, 50f);

      newCS.reinitialize(conf, newMockRM.getRMContext());

      // validate that leaf queues abs capacity is now changed
      AutoCreatedLeafQueue user0Queue = (AutoCreatedLeafQueue) newCS.getQueue(
          USER1);
      validateCapacities(user0Queue, 0.5f, 0.15f, 1.0f, 0.5f);
      // The new queue calculation mode works from the effective resources
      // so the absoluteCapacity and the maxApplications differs a little
      // bit: 6553/16384=0.3999633789 vs 0.4
      final int maxApps = cs.getConfiguration().isLegacyQueueMode() ? 4000 : 3999;
      validateUserAndAppLimits(user0Queue, maxApps, maxApps);

      //update leaf queue template capacities
      conf.setAutoCreatedLeafQueueConfigCapacity(C, 30f);
      conf.setAutoCreatedLeafQueueConfigMaxCapacity(C, 40f);

      newCS.reinitialize(conf, newMockRM.getRMContext());
      validateCapacities(user0Queue, 0.3f, 0.09f, 0.4f, 0.2f);
      validateUserAndAppLimits(user0Queue, maxApps, maxApps);

      //submit app1 as USER3
      submitApp(newMockRM, parentQueue, USER3, USER3, 3, 1);
      AutoCreatedLeafQueue user3Queue =
          (AutoCreatedLeafQueue) newCS.getQueue(USER1);
      validateCapacities(user3Queue, 0.3f, 0.09f, 0.4f,0.2f);

      validateUserAndAppLimits(user3Queue, maxApps, maxApps);

      //submit app1 as USER1 - is already activated. there should be no diff
      // in capacities
      submitApp(newMockRM, parentQueue, USER3, USER3, 4, 2);

      validateCapacities(user3Queue, 0.3f, 0.09f, 0.4f,0.2f);

      validateUserAndAppLimits(user3Queue, maxApps, maxApps);
      validateContainerLimits(user3Queue, 6, 10240);

      GuaranteedOrZeroCapacityOverTimePolicy autoCreatedQueueManagementPolicy =
          (GuaranteedOrZeroCapacityOverTimePolicy) ((ManagedParentQueue)
              parentQueue)
              .getAutoCreatedQueueManagementPolicy();
      assertEquals(0.27f, autoCreatedQueueManagementPolicy
          .getAbsoluteActivatedChildQueueCapacity
              (NO_LABEL), EPSILON);
    } finally {
      cleanupQueue(USER1);
      cleanupQueue(USER2);
      if (newMockRM != null) {
        ((CapacityScheduler) newMockRM.getResourceScheduler()).stop();
        newMockRM.stop();
      }
    }
  }

  @Test
  public void testDynamicAutoQueueCreationWithTags()
      throws Exception {
    // This test we will reinitialize mockRM, so stop the previous initialized
    // mockRM to avoid issues like MetricsSystem
    if (mockRM != null) {
      mockRM.stop();
    }
    mockRM = null;
    try {
      CapacitySchedulerConfiguration csConf
          = new CapacitySchedulerConfiguration();
      csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
          new String[] {"a", "b"});
      csConf.setCapacity("root.a", 90);
      csConf.setCapacity("root.b", 10);
      csConf.setAutoCreateChildQueueEnabled("root.a", true);
      csConf.setAutoCreatedLeafQueueConfigCapacity("root.a", 50);
      csConf.setAutoCreatedLeafQueueConfigMaxCapacity("root.a", 100);
      csConf.setAcl("root.a", QueueACL.ADMINISTER_QUEUE, "*");
      csConf.setAcl("root.a", QueueACL.SUBMIT_APPLICATIONS, "*");
      csConf.setBoolean(YarnConfiguration
          .APPLICATION_TAG_BASED_PLACEMENT_ENABLED, true);
      csConf.setStrings(YarnConfiguration
          .APPLICATION_TAG_BASED_PLACEMENT_USER_WHITELIST, "hadoop");
      csConf.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
          "u:%user:root.a.%user");

      RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
      mgr.init(csConf);
      mockRM = new MockRM(csConf) {
        @Override
        public RMNodeLabelsManager createNodeLabelManager() {
          return mgr;
        }
      };
      mockRM.start();
      MockNM nm = mockRM.registerNode("127.0.0.1:1234", 16 * GB);

      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(GB, mockRM)
          .withAppName("apptodynamicqueue")
          .withUser("hadoop")
          .withAcls(null)
          .withUnmanagedAM(false)
          .withApplicationTags(Sets.newHashSet("userid=testuser"))
          .build();
      RMApp app = MockRMAppSubmitter.submit(mockRM, data);
      MockRM.launchAndRegisterAM(app, mockRM, nm);
      nm.nodeHeartbeat(true);

      CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
      CSQueue queue = cs.getQueue("root.a.testuser");
      assertNotNull("Leaf queue has not been auto-created", queue);
      assertEquals("Number of running applications", 1,
          queue.getNumApplications());
    } finally {
      if (mockRM != null) {
        mockRM.close();
      }
    }
  }
}
