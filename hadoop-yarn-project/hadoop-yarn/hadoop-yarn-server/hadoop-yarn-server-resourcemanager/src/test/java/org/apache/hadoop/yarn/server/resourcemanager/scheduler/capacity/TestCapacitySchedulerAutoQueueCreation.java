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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule.CURRENT_USER_MAPPING;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for creation and reinitilization of auto created leaf queues
 * under a ManagedParentQueue.
 */
public class TestCapacitySchedulerAutoQueueCreation {

  private static final Log LOG = LogFactory.getLog(TestCapacityScheduler.class);
  private final int GB = 1024;
  private final static ContainerUpdates NULL_UPDATE_REQUESTS =
      new ContainerUpdates();

  private static final String A = CapacitySchedulerConfiguration.ROOT + ".a";
  private static final String B = CapacitySchedulerConfiguration.ROOT + ".b";
  private static final String C = CapacitySchedulerConfiguration.ROOT + ".c";
  private static final String D = CapacitySchedulerConfiguration.ROOT + ".d";
  private static final String A1 = A + ".a1";
  private static final String A2 = A + ".a2";
  private static final String B1 = B + ".b1";
  private static final String B2 = B + ".b2";
  private static final String B3 = B + ".b3";
  private static final String C1 = C + ".c1";
  private static final String C2 = C + ".c2";
  private static final String C3 = C + ".c3";
  private static float A_CAPACITY = 20f;
  private static float B_CAPACITY = 40f;
  private static float C_CAPACITY = 20f;
  private static float D_CAPACITY = 20f;
  private static float A1_CAPACITY = 30;
  private static float A2_CAPACITY = 70;
  private static float B1_CAPACITY = 60f;
  private static float B2_CAPACITY = 20f;
  private static float B3_CAPACITY = 20f;
  private static float C1_CAPACITY = 20f;
  private static float C2_CAPACITY = 20f;

  private static String USER = "user_";
  private static String USER0 = USER + 0;
  private static String USER2 = USER + 2;
  private static String PARENT_QUEUE = "c";

  private MockRM mockRM = null;

  private CapacityScheduler cs;

  private final TestCapacityScheduler tcs = new TestCapacityScheduler();

  private static SpyDispatcher dispatcher;

  private static EventHandler<Event> rmAppEventEventHandler;

  private static class SpyDispatcher extends AsyncDispatcher {

    private static BlockingQueue<Event> eventQueue =
        new LinkedBlockingQueue<>();

    private static class SpyRMAppEventHandler implements EventHandler<Event> {
      public void handle(Event event) {
        eventQueue.add(event);
      }
    }

    @Override
    protected void dispatch(Event event) {
      eventQueue.add(event);
    }

    @Override
    public EventHandler<Event> getEventHandler() {
      return rmAppEventEventHandler;
    }

    void spyOnNextEvent(Event expectedEvent, long timeout)
        throws InterruptedException {

      Event event = eventQueue.poll(timeout, TimeUnit.MILLISECONDS);
      assertEquals(expectedEvent.getType(), event.getType());
      assertEquals(expectedEvent.getClass(), event.getClass());
    }
  }

  @Before
  public void setUp() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    List<String> queuePlacementRules = new ArrayList<>();
    queuePlacementRules.add(YarnConfiguration.USER_GROUP_PLACEMENT_RULE);
    conf.setQueuePlacementRules(queuePlacementRules);

    setupQueueMappings(conf);

    mockRM = new MockRM(conf);
    cs = (CapacityScheduler) mockRM.getResourceScheduler();

    dispatcher = new SpyDispatcher();
    rmAppEventEventHandler = new SpyDispatcher.SpyRMAppEventHandler();
    dispatcher.register(RMAppEventType.class, rmAppEventEventHandler);
    cs.updatePlacementRules();
    mockRM.start();

    cs.start();
  }

  private CapacitySchedulerConfiguration setupQueueMappings(
      CapacitySchedulerConfiguration conf) {

    //set queue mapping
    List<UserGroupMappingPlacementRule.QueueMapping> queueMappings =
        new ArrayList<>();
    for (int i = 0; i <= 3; i++) {
      //Set C as parent queue name for auto queue creation
      UserGroupMappingPlacementRule.QueueMapping userQueueMapping =
          new UserGroupMappingPlacementRule.QueueMapping(
              UserGroupMappingPlacementRule.QueueMapping.MappingType.USER,
              USER + i, getQueueMapping(PARENT_QUEUE, USER + i));
      queueMappings.add(userQueueMapping);
    }

    conf.setQueueMappings(queueMappings);
    //override with queue mappings
    conf.setOverrideWithQueueMappings(true);
    return conf;
  }

  /**
   * @param conf, to be modified
   * @return, CS configuration which has C
   * as an auto creation enabled parent queue
   * <p>
   * root
   * /     \      \       \
   * a        b      c    d
   * / \    /  |  \
   * a1  a2 b1  b2  b3
   */
  private CapacitySchedulerConfiguration setupQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    //setup new queues with one of them auto enabled
    // Define top-level queues
    // Set childQueue for root
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b", "c", "d" });

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);
    conf.setCapacity(C, C_CAPACITY);
    conf.setCapacity(D, D_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1", "a2" });
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    conf.setQueues(B, new String[] { "b1", "b2", "b3" });
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    conf.setUserLimitFactor(C, 1.0f);
    conf.setAutoCreateChildQueueEnabled(C, true);

    //Setup leaf queue template configs
    conf.setAutoCreatedLeafQueueTemplateCapacity(C, 50.0f);
    conf.setAutoCreatedLeafQueueTemplateMaxCapacity(C, 100.0f);

    LOG.info("Setup " + C + " as an auto leaf creation enabled parent queue");

    conf.setUserLimitFactor(D, 1.0f);
    conf.setAutoCreateChildQueueEnabled(D, true);

    //Setup leaf queue template configs
    conf.setAutoCreatedLeafQueueTemplateCapacity(D, 10.0f);
    conf.setAutoCreatedLeafQueueTemplateMaxCapacity(D, 100.0f);

    LOG.info("Setup " + D + " as an auto leaf creation enabled parent queue");

    return conf;
  }

  @After
  public void tearDown() throws Exception {
    if (mockRM != null) {
      mockRM.stop();
    }
  }

  @Test(timeout = 10000)
  public void testAutoCreateLeafQueueCreation() throws Exception {

    try {
      // submit an app
      submitApp(cs, USER0, USER0, PARENT_QUEUE);

      // check preconditions
      List<ApplicationAttemptId> appsInC = cs.getAppsInQueue(PARENT_QUEUE);
      assertEquals(1, appsInC.size());
      assertNotNull(cs.getQueue(USER0));

      AutoCreatedLeafQueue autoCreatedLeafQueue =
          (AutoCreatedLeafQueue) cs.getQueue(USER0);
      ManagedParentQueue parentQueue = (ManagedParentQueue) cs.getQueue(
          PARENT_QUEUE);
      assertEquals(parentQueue, autoCreatedLeafQueue.getParent());
      validateCapacities(autoCreatedLeafQueue);
    } finally {
      cleanupQueue(USER0);
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

      RMApp app = mockRM.submitApp(GB, "test-auto-queue-creation-1", USER0,
          null, USER0);
      // check preconditions
      List<ApplicationAttemptId> appsInC = cs.getAppsInQueue(PARENT_QUEUE);
      assertEquals(1, appsInC.size());

      assertNotNull(cs.getQueue(USER0));

      AutoCreatedLeafQueue autoCreatedLeafQueue =
          (AutoCreatedLeafQueue) cs.getQueue(USER0);
      ManagedParentQueue parentQueue = (ManagedParentQueue) cs.getQueue(
          PARENT_QUEUE);
      assertEquals(parentQueue, autoCreatedLeafQueue.getParent());
      validateCapacities(autoCreatedLeafQueue);

      ApplicationAttemptId appAttemptId = appsInC.get(0);

      Priority priority = TestUtils.createMockPriority(1);
      RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(
          null);
      ResourceRequest r1 = TestUtils.createResourceRequest(ResourceRequest.ANY,
          1 * GB, 1, true, priority, recordFactory);

      cs.allocate(appAttemptId, Collections.<ResourceRequest>singletonList(r1),
          Collections.<ContainerId>emptyList(), Collections.singletonList(host),
          null, NULL_UPDATE_REQUESTS);

      //And this will result in container assignment for app1
      CapacityScheduler.schedule(cs);

      //change state to draining
      autoCreatedLeafQueue.stopQueue();

      cs.killAllAppsInQueue(USER0);

      mockRM.waitForState(appAttemptId, RMAppAttemptState.KILLED);

      mockRM.waitForState(appAttemptId.getApplicationId(), RMAppState.KILLED);

      //change state to stopped
      autoCreatedLeafQueue.stopQueue();
      assertEquals(QueueState.STOPPED,
          autoCreatedLeafQueue.getQueueInfo().getQueueState());

      cs.reinitialize(cs.getConf(), mockRM.getRMContext());

      AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) cs.getQueue(
          USER0);
      validateCapacities(leafQueue);

    } finally {
      cleanupQueue(USER0);
    }
  }

  @Test
  public void testRefreshQueuesWithAutoCreatedLeafQueues() throws Exception {

    MockRM newMockRM = setupSchedulerInstance();
    try {
      CapacityScheduler newCS =
          (CapacityScheduler) newMockRM.getResourceScheduler();
      CapacitySchedulerConfiguration conf = newCS.getConfiguration();

      // Test add one auto created queue dynamically and manually modify
      // capacity
      ManagedParentQueue parentQueue = (ManagedParentQueue) newCS.getQueue("c");
      AutoCreatedLeafQueue c1 = new AutoCreatedLeafQueue(newCS, "c1",
          parentQueue);
      newCS.addQueue(c1);
      c1.setEntitlement(new QueueEntitlement(C1_CAPACITY / 100, 1f));

      // Test add another auto created queue and use setEntitlement to modify
      // capacity
      AutoCreatedLeafQueue c2 = new AutoCreatedLeafQueue(newCS, "c2",
          (ManagedParentQueue) newCS.getQueue("c"));
      newCS.addQueue(c2);
      newCS.setEntitlement("c2", new QueueEntitlement(C2_CAPACITY / 100, 1f));

      // Verify all allocations match
      checkQueueCapacities(newCS, C_CAPACITY, D_CAPACITY);

      // Reinitialize and verify all dynamic queued survived

      conf.setCapacity(A, 20f);
      conf.setCapacity(B, 20f);
      conf.setCapacity(C, 40f);
      conf.setCapacity(D, 20f);
      newCS.reinitialize(conf, newMockRM.getRMContext());

      checkQueueCapacities(newCS, 40f, 20f);

      //chnage parent template configs and reinitialize
      conf.setAutoCreatedLeafQueueTemplateCapacity(C, 30.0f);
      conf.setAutoCreatedLeafQueueTemplateMaxCapacity(C, 100.0f);
      newCS.reinitialize(conf, newMockRM.getRMContext());

      ManagedParentQueue c = (ManagedParentQueue) newCS.getQueue("c");
      AutoCreatedLeafQueue c3 = new AutoCreatedLeafQueue(newCS, "c3", c);
      newCS.addQueue(c3);

      AbstractManagedParentQueue.AutoCreatedLeafQueueTemplate
          leafQueueTemplate = parentQueue.getLeafQueueTemplate();
      QueueCapacities cap = leafQueueTemplate.getQueueCapacities();
      c3.setEntitlement(
          new QueueEntitlement(cap.getCapacity(), cap.getMaximumCapacity()));
      newCS.reinitialize(conf, newMockRM.getRMContext());

      checkQueueCapacities(newCS, 40f, 20f);
    } finally {
      if (newMockRM != null) {
        ((CapacityScheduler) newMockRM.getResourceScheduler()).stop();
        newMockRM.stop();
      }
    }
  }

  @Test
  public void testConvertAutoCreateDisabledOnManagedParentQueueFails()
      throws Exception {
    CapacityScheduler newCS = new CapacityScheduler();
    try {
      CapacitySchedulerConfiguration newConf =
          new CapacitySchedulerConfiguration();
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
      CapacitySchedulerConfiguration newConf =
          new CapacitySchedulerConfiguration();
      setupQueueConfiguration(newConf);
      newConf.setAutoCreatedLeafQueueTemplateCapacity(A1, A1_CAPACITY / 10);
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
      CapacitySchedulerConfiguration newConf =
          new CapacitySchedulerConfiguration();
      setupQueueConfiguration(newConf);
      newConf.setAutoCreatedLeafQueueTemplateCapacity(A, A_CAPACITY / 10);
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
    RMApp app = mockRM.submitApp(GB, "app", INVALID_USER, null, INVALID_USER,
        false);
    mockRM.drainEvents();
    mockRM.waitForState(app.getApplicationId(), RMAppState.FAILED);
    assertEquals(RMAppState.FAILED, app.getState());
  }

  private void validateCapacities(AutoCreatedLeafQueue autoCreatedLeafQueue) {
    assertEquals(autoCreatedLeafQueue.getCapacity(), 0.0f, EPSILON);
    assertEquals(autoCreatedLeafQueue.getAbsoluteCapacity(), 0.0f, EPSILON);
    assertEquals(autoCreatedLeafQueue.getMaximumCapacity(), 0.0f, EPSILON);
    assertEquals(autoCreatedLeafQueue.getAbsoluteMaximumCapacity(), 0.0f,
        EPSILON);
    int maxAppsForAutoCreatedQueues = (int) (
        CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS
            * autoCreatedLeafQueue.getParent().getAbsoluteCapacity());
    assertEquals(autoCreatedLeafQueue.getMaxApplicationsPerUser(),
        maxAppsForAutoCreatedQueues);
    assertEquals(autoCreatedLeafQueue.getMaxApplicationsPerUser(),
        (int) (maxAppsForAutoCreatedQueues * (cs.getConfiguration()
            .getUserLimitFactor(
                autoCreatedLeafQueue.getParent().getQueuePath()))));
  }

  private void cleanupQueue(String queueName) throws YarnException {
    AutoCreatedLeafQueue queue = (AutoCreatedLeafQueue) cs.getQueue(queueName);
    if (queue != null) {
      queue.setEntitlement(new QueueEntitlement(0.0f, 0.0f));
      ((ManagedParentQueue) queue.getParent()).removeChildQueue(
          queue.getQueueName());
      cs.getCapacitySchedulerQueueManager().removeQueue(queue.getQueueName());
    } else{
      throw new YarnException("Queue does not exist " + queueName);
    }
  }

  String getQueueMapping(String parentQueue, String leafQueue) {
    return parentQueue + DOT + leafQueue;
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
        setupQueueMapping(newCS, CURRENT_USER_MAPPING, "a",
            CURRENT_USER_MAPPING);
        newCS.updatePlacementRules();
        fail("Expected invalid parent queue mapping failure");

      } catch (IOException e) {
        //expected exception
        assertTrue(e.getMessage().contains(
            "invalid parent queue which does not have auto creation of leaf "
                + "queues enabled ["
                + "a" + "]"));
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
        assertTrue(e.getMessage()
            .contains("invalid parent queue [" + "INVALID_PARENT_QUEUE" + "]"));
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
        setupQueueMapping(newCS, CURRENT_USER_MAPPING, "",
            CURRENT_USER_MAPPING);
        newCS.updatePlacementRules();
        fail("Expected invalid parent queue mapping failure");
      } catch (IOException e) {
        //expected exception
        assertTrue(e.getMessage().contains("invalid parent queue []"));
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
      newMockRM.start();
      newCS.start();

      submitApp(newCS, USER0, USER0, PARENT_QUEUE);

      assertNotNull(newCS.getQueue(USER0));

      setupQueueMapping(newCS, USER0, "d", USER0);
      newCS.updatePlacementRules();

      RMContext rmContext = mock(RMContext.class);
      when(rmContext.getDispatcher()).thenReturn(dispatcher);
      newCS.setRMContext(rmContext);

      ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
      SchedulerEvent addAppEvent = new AppAddedSchedulerEvent(appId, USER0,
          USER0, new ApplicationPlacementContext(USER0, "d"));
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
  public void testAutoCreationFailsWhenParentCapacityExceeded()
      throws IOException, SchedulerDynamicEditException {
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
      AutoCreatedLeafQueue c1 = new AutoCreatedLeafQueue(newCS, "c1",
          parentQueue);
      newCS.addQueue(c1);
      c1.setEntitlement(new QueueEntitlement(0.5f, 1f));

      AutoCreatedLeafQueue c2 = new AutoCreatedLeafQueue(newCS, "c2",
          parentQueue);
      newCS.addQueue(c2);
      c2.setEntitlement(new QueueEntitlement(0.5f, 1f));

      try {
        AutoCreatedLeafQueue c3 = new AutoCreatedLeafQueue(newCS, "c3",
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

  private List<UserGroupMappingPlacementRule.QueueMapping> setupQueueMapping(
      CapacityScheduler newCS, String user, String parentQueue, String queue) {
    List<UserGroupMappingPlacementRule.QueueMapping> queueMappings =
        new ArrayList<>();
    queueMappings.add(new UserGroupMappingPlacementRule.QueueMapping(
        UserGroupMappingPlacementRule.QueueMapping.MappingType.USER, user,
        getQueueMapping(parentQueue, queue)));
    newCS.getConfiguration().setQueueMappings(queueMappings);
    return queueMappings;
  }

  private MockRM setupSchedulerInstance() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    List<String> queuePlacementRules = new ArrayList<String>();
    queuePlacementRules.add(YarnConfiguration.USER_GROUP_PLACEMENT_RULE);
    conf.setQueuePlacementRules(queuePlacementRules);

    setupQueueMappings(conf);

    MockRM newMockRM = new MockRM(conf);
    return newMockRM;
  }

  void checkQueueCapacities(CapacityScheduler newCS, float capacityC,
      float capacityD) {
    CSQueue rootQueue = newCS.getRootQueue();
    CSQueue queueC = tcs.findQueue(rootQueue, C);
    CSQueue queueD = tcs.findQueue(rootQueue, D);
    CSQueue queueC1 = tcs.findQueue(queueC, C1);
    CSQueue queueC2 = tcs.findQueue(queueC, C2);
    CSQueue queueC3 = tcs.findQueue(queueC, C3);

    float capC = capacityC / 100.0f;
    float capD = capacityD / 100.0f;

    tcs.checkQueueCapacity(queueC, capC, capC, 1.0f, 1.0f);
    tcs.checkQueueCapacity(queueD, capD, capD, 1.0f, 1.0f);
    tcs.checkQueueCapacity(queueC1, C1_CAPACITY / 100.0f,
        (C1_CAPACITY / 100.0f) * capC, 1.0f, 1.0f);
    tcs.checkQueueCapacity(queueC2, C2_CAPACITY / 100.0f,
        (C2_CAPACITY / 100.0f) * capC, 1.0f, 1.0f);

    if (queueC3 != null) {
      ManagedParentQueue parentQueue = (ManagedParentQueue) queueC;
      QueueCapacities cap =
          parentQueue.getLeafQueueTemplate().getQueueCapacities();
      tcs.checkQueueCapacity(queueC3, cap.getCapacity(),
          (cap.getCapacity()) * capC, 1.0f, 1.0f);
    }
  }

  ApplicationAttemptId submitApp(CapacityScheduler newCS, String user,
      String queue, String parentQueue) {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    SchedulerEvent addAppEvent = new AppAddedSchedulerEvent(appId, queue, user,
        new ApplicationPlacementContext(queue, parentQueue));
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    SchedulerEvent addAttemptEvent = new AppAttemptAddedSchedulerEvent(
        appAttemptId, false);
    newCS.handle(addAppEvent);
    newCS.handle(addAttemptEvent);
    return appAttemptId;
  }
}
