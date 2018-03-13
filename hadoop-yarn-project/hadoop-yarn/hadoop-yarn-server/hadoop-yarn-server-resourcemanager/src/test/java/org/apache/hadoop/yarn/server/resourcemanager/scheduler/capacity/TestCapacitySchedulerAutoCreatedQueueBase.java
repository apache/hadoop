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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels
    .NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .UserGroupMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .ResourceScheduler;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common
    .QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.CSQueueUtils.EPSILON;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCapacitySchedulerAutoCreatedQueueBase {

  private static final Log LOG = LogFactory.getLog(
      TestCapacitySchedulerAutoCreatedQueueBase.class);
  public static final int GB = 1024;
  public final static ContainerUpdates NULL_UPDATE_REQUESTS =
      new ContainerUpdates();

  public static final String A = CapacitySchedulerConfiguration.ROOT + ".a";
  public static final String B = CapacitySchedulerConfiguration.ROOT + ".b";
  public static final String C = CapacitySchedulerConfiguration.ROOT + ".c";
  public static final String D = CapacitySchedulerConfiguration.ROOT + ".d";
  public static final String E = CapacitySchedulerConfiguration.ROOT + ".e";
  public static final String A1 = A + ".a1";
  public static final String A2 = A + ".a2";
  public static final String B1 = B + ".b1";
  public static final String B2 = B + ".b2";
  public static final String B3 = B + ".b3";
  public static final String C1 = C + ".c1";
  public static final String C2 = C + ".c2";
  public static final String C3 = C + ".c3";
  public static final float A_CAPACITY = 20f;
  public static final float B_CAPACITY = 40f;
  public static final float C_CAPACITY = 20f;
  public static final float D_CAPACITY = 20f;
  public static final float A1_CAPACITY = 30;
  public static final float A2_CAPACITY = 70;
  public static final float B1_CAPACITY = 60f;
  public static final float B2_CAPACITY = 20f;
  public static final float B3_CAPACITY = 20f;
  public static final float C1_CAPACITY = 20f;
  public static final float C2_CAPACITY = 20f;

  public static final int NODE_MEMORY = 16;

  public static final int NODE1_VCORES = 16;
  public static final int NODE2_VCORES = 32;
  public static final int NODE3_VCORES = 48;

  public static final String USER = "user_";
  public static final String USER0 = USER + 0;
  public static final String USER1 = USER + 1;
  public static final String USER2 = USER + 2;
  public static final String USER3 = USER + 3;
  public static final String PARENT_QUEUE = "c";

  public static final Set<String> accessibleNodeLabelsOnC = new HashSet<>();

  public static final String NODEL_LABEL_GPU = "GPU";
  public static final String NODEL_LABEL_SSD = "SSD";

  protected MockRM mockRM = null;
  protected MockNM nm1 = null;
  protected MockNM nm2 = null;
  protected MockNM nm3 = null;
  protected CapacityScheduler cs;
  private final TestCapacityScheduler tcs = new TestCapacityScheduler();
  protected SpyDispatcher dispatcher;
  private static EventHandler<Event> rmAppEventEventHandler;

  public static class SpyDispatcher extends AsyncDispatcher {

    public static BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();

    public static class SpyRMAppEventHandler implements EventHandler<Event> {
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

    setupQueueMappings(conf, PARENT_QUEUE, true, new int[] { 0, 1, 2, 3 });

    dispatcher = new SpyDispatcher();
    rmAppEventEventHandler = new SpyDispatcher.SpyRMAppEventHandler();
    dispatcher.register(RMAppEventType.class, rmAppEventEventHandler);

    RMNodeLabelsManager mgr = setupNodeLabelManager(conf);

    mockRM = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    cs = (CapacityScheduler) mockRM.getResourceScheduler();
    cs.updatePlacementRules();
    mockRM.start();
    cs.start();

    setupNodes(mockRM);
  }

  protected void setupNodes(MockRM newMockRM) throws Exception {
    nm1 = // label = SSD
        new MockNM("h1:1234", NODE_MEMORY * GB, NODE1_VCORES, newMockRM
            .getResourceTrackerService());
    nm1.registerNode();

    nm2 = // label = GPU
        new MockNM("h2:1234", NODE_MEMORY * GB, NODE2_VCORES, newMockRM
            .getResourceTrackerService
            ());
    nm2.registerNode();

    nm3 = // label = ""
        new MockNM("h3:1234", NODE_MEMORY * GB, NODE3_VCORES, newMockRM
            .getResourceTrackerService
                ());
    nm3.registerNode();
  }

  public static CapacitySchedulerConfiguration setupQueueMappings(
      CapacitySchedulerConfiguration conf, String parentQueue, boolean
      overrideWithQueueMappings, int[] userIds) {

    List<String> queuePlacementRules = new ArrayList<>();
    queuePlacementRules.add(YarnConfiguration.USER_GROUP_PLACEMENT_RULE);
    conf.setQueuePlacementRules(queuePlacementRules);

    List<UserGroupMappingPlacementRule.QueueMapping> existingMappings =
        conf.getQueueMappings();

    //set queue mapping
    List<UserGroupMappingPlacementRule.QueueMapping> queueMappings =
        new ArrayList<>();
    for (int i = 0; i < userIds.length; i++) {
      //Set C as parent queue name for auto queue creation
      UserGroupMappingPlacementRule.QueueMapping userQueueMapping =
          new UserGroupMappingPlacementRule.QueueMapping(
              UserGroupMappingPlacementRule.QueueMapping.MappingType.USER,
              USER + userIds[i],
              getQueueMapping(parentQueue, USER + userIds[i]));
      queueMappings.add(userQueueMapping);
    }

    existingMappings.addAll(queueMappings);
    conf.setQueueMappings(existingMappings);
    //override with queue mappings
    conf.setOverrideWithQueueMappings(overrideWithQueueMappings);
    return conf;
  }

  /**
   * @param conf, to be modified
   * @return, CS configuration which has C as an auto creation enabled parent
   * queue
   * <p>
   * root /     \      \       \ a        b      c    d / \    /  |  \ a1  a2 b1
   * b2  b3
   */
  public static CapacitySchedulerConfiguration setupQueueConfiguration(
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
    conf.setAutoCreatedLeafQueueConfigCapacity(C, 50.0f);
    conf.setAutoCreatedLeafQueueConfigMaxCapacity(C, 100.0f);
    conf.setAutoCreatedLeafQueueConfigUserLimit(C, 100);
    conf.setAutoCreatedLeafQueueConfigUserLimitFactor(C, 3.0f);

    LOG.info("Setup " + C + " as an auto leaf creation enabled parent queue");

    conf.setUserLimitFactor(D, 1.0f);
    conf.setAutoCreateChildQueueEnabled(D, true);
    conf.setUserLimit(D, 100);
    conf.setUserLimitFactor(D, 3.0f);

    //Setup leaf queue template configs
    conf.setAutoCreatedLeafQueueConfigCapacity(D, 10.0f);
    conf.setAutoCreatedLeafQueueConfigMaxCapacity(D, 100.0f);
    conf.setAutoCreatedLeafQueueConfigUserLimit(D, 3);
    conf.setAutoCreatedLeafQueueConfigUserLimitFactor(D, 100);

    conf.set(CapacitySchedulerConfiguration.PREFIX + C + DOT
            + CapacitySchedulerConfiguration
            .AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX
            + DOT + CapacitySchedulerConfiguration.ORDERING_POLICY,
        FAIR_APP_ORDERING_POLICY);

    accessibleNodeLabelsOnC.add(NODEL_LABEL_GPU);
    accessibleNodeLabelsOnC.add(NODEL_LABEL_SSD);
    accessibleNodeLabelsOnC.add(NO_LABEL);

    conf.setAccessibleNodeLabels(C, accessibleNodeLabelsOnC);
    conf.setCapacityByLabel(C, NODEL_LABEL_GPU, 50);
    conf.setCapacityByLabel(C, NODEL_LABEL_SSD, 50);

    LOG.info("Setup " + D + " as an auto leaf creation enabled parent queue");

    return conf;
  }

  public static CapacitySchedulerConfiguration
      setupQueueConfigurationForSingleAutoCreatedLeafQueue(
      CapacitySchedulerConfiguration conf) {

    //setup new queues with one of them auto enabled
    // Define top-level queues
    // Set childQueue for root
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"c"});
    conf.setCapacity(C, 100f);

    conf.setUserLimitFactor(C, 1.0f);
    conf.setAutoCreateChildQueueEnabled(C, true);

    //Setup leaf queue template configs
    conf.setAutoCreatedLeafQueueConfigCapacity(C, 100f);
    conf.setAutoCreatedLeafQueueConfigMaxCapacity(C, 100.0f);
    conf.setAutoCreatedLeafQueueConfigUserLimit(C, 100);
    conf.setAutoCreatedLeafQueueConfigUserLimitFactor(C, 3.0f);

    return conf;
  }

  @After
  public void tearDown() throws Exception {
    if (mockRM != null) {
      mockRM.stop();
    }
  }

  protected void validateCapacities(AutoCreatedLeafQueue autoCreatedLeafQueue,
      float capacity, float absCapacity, float maxCapacity,
      float absMaxCapacity) {
    assertEquals(capacity, autoCreatedLeafQueue.getCapacity(), EPSILON);
    assertEquals(absCapacity, autoCreatedLeafQueue.getAbsoluteCapacity(),
        EPSILON);
    assertEquals(maxCapacity, autoCreatedLeafQueue.getMaximumCapacity(),
        EPSILON);
    assertEquals(absMaxCapacity,
        autoCreatedLeafQueue.getAbsoluteMaximumCapacity(), EPSILON);
  }

  protected void cleanupQueue(String queueName) throws YarnException {
    AutoCreatedLeafQueue queue = (AutoCreatedLeafQueue) cs.getQueue(queueName);
    if (queue != null) {
      setEntitlement(queue, new QueueEntitlement(0.0f, 0.0f));
      ((ManagedParentQueue) queue.getParent()).removeChildQueue(
          queue.getQueueName());
      cs.getCapacitySchedulerQueueManager().removeQueue(queue.getQueueName());
    }
  }

  protected ApplicationId submitApp(MockRM rm, CSQueue parentQueue,
      String leafQueueName, String user, int expectedNumAppsInParentQueue,
      int expectedNumAppsInLeafQueue) throws Exception {

    CapacityScheduler capacityScheduler =
        (CapacityScheduler) rm.getResourceScheduler();
    // submit an app
    RMApp rmApp = rm.submitApp(GB, "test-auto-queue-activation", user, null,
        leafQueueName);

    // check preconditions
    List<ApplicationAttemptId> appsInParentQueue =
        capacityScheduler.getAppsInQueue(parentQueue.getQueueName());
    assertEquals(expectedNumAppsInParentQueue, appsInParentQueue.size());

    List<ApplicationAttemptId> appsInLeafQueue =
        capacityScheduler.getAppsInQueue(leafQueueName);
    assertEquals(expectedNumAppsInLeafQueue, appsInLeafQueue.size());

    return rmApp.getApplicationId();
  }

  protected List<UserGroupMappingPlacementRule.QueueMapping> setupQueueMapping(
      CapacityScheduler newCS, String user, String parentQueue, String queue) {
    List<UserGroupMappingPlacementRule.QueueMapping> queueMappings =
        new ArrayList<>();
    queueMappings.add(new UserGroupMappingPlacementRule.QueueMapping(
        UserGroupMappingPlacementRule.QueueMapping.MappingType.USER, user,
        getQueueMapping(parentQueue, queue)));
    newCS.getConfiguration().setQueueMappings(queueMappings);
    return queueMappings;
  }

  protected MockRM setupSchedulerInstance() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    setupQueueMappings(conf, PARENT_QUEUE, true, new int[] {0, 1, 2, 3});

    RMNodeLabelsManager mgr = setupNodeLabelManager(conf);
    MockRM newMockRM = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    newMockRM.start();
    ((CapacityScheduler) newMockRM.getResourceScheduler()).start();
    setupNodes(newMockRM);
    return newMockRM;
  }

  static String getQueueMapping(String parentQueue, String leafQueue) {
    return parentQueue + DOT + leafQueue;
  }

  protected RMNodeLabelsManager setupNodeLabelManager(
      CapacitySchedulerConfiguration conf) throws IOException {
    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(
        ImmutableSet.of(NODEL_LABEL_SSD, NODEL_LABEL_GPU));
    mgr.addLabelsToNode(ImmutableMap
        .of(NodeId.newInstance("h1", 0),
            TestUtils.toSet(NODEL_LABEL_SSD)));
    mgr.addLabelsToNode(ImmutableMap
        .of(NodeId.newInstance("h2", 0),
            TestUtils.toSet(NODEL_LABEL_GPU)));
    return mgr;
  }

  protected ApplicationAttemptId submitApp(CapacityScheduler newCS, String user,
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

  protected RMApp submitApp(String user, String queue, String nodeLabel)
      throws Exception {
    RMApp app = mockRM.submitApp(GB,
        "test-auto-queue-creation" + RandomUtils.nextInt(100), user, null,
        queue, nodeLabel);
    Assert.assertEquals(app.getAmNodeLabelExpression(), nodeLabel);
    // check preconditions
    List<ApplicationAttemptId> appsInC = cs.getAppsInQueue(PARENT_QUEUE);
    assertEquals(1, appsInC.size());
    assertNotNull(cs.getQueue(queue));

    return app;
  }

  void setEntitlement(AutoCreatedLeafQueue queue,
      QueueEntitlement entitlement) {
    queue.setCapacity(entitlement.getCapacity());
    queue.setAbsoluteCapacity(
        queue.getParent().getAbsoluteCapacity() * entitlement.getCapacity());
    // note: we currently set maxCapacity to capacity
    // this might be revised later
    queue.setMaxCapacity(entitlement.getMaxCapacity());
  }

  protected void validateUserAndAppLimits(
      AutoCreatedLeafQueue autoCreatedLeafQueue, int maxApps,
      int maxAppsPerUser) {
    assertEquals(maxApps, autoCreatedLeafQueue.getMaxApplications());
    assertEquals(maxAppsPerUser,
        autoCreatedLeafQueue.getMaxApplicationsPerUser());
  }

  protected void validateInitialQueueEntitlement(CSQueue parentQueue,
      String leafQueueName, float expectedTotalChildQueueAbsCapacity)
      throws SchedulerDynamicEditException {
    validateInitialQueueEntitlement(cs, parentQueue, leafQueueName,
        expectedTotalChildQueueAbsCapacity);
  }

  protected void validateInitialQueueEntitlement(
      CapacityScheduler capacityScheduler, CSQueue parentQueue,
      String leafQueueName, float expectedTotalChildQueueAbsCapacity)
      throws SchedulerDynamicEditException {
    ManagedParentQueue autoCreateEnabledParentQueue =
        (ManagedParentQueue) parentQueue;

    GuaranteedOrZeroCapacityOverTimePolicy policy =
        (GuaranteedOrZeroCapacityOverTimePolicy) autoCreateEnabledParentQueue
            .getAutoCreatedQueueManagementPolicy();

    assertEquals(expectedTotalChildQueueAbsCapacity,
        policy.getAbsoluteActivatedChildQueueCapacity(), EPSILON);

    AutoCreatedLeafQueue leafQueue =
        (AutoCreatedLeafQueue) capacityScheduler.getQueue(leafQueueName);

    Map<String, QueueEntitlement> expectedEntitlements = new HashMap<>();
    QueueCapacities cap = autoCreateEnabledParentQueue.getLeafQueueTemplate()
        .getQueueCapacities();

    for (String label : accessibleNodeLabelsOnC) {
      validateCapacitiesByLabel(autoCreateEnabledParentQueue, leafQueue, label);

      QueueEntitlement expectedEntitlement = new QueueEntitlement(
          cap.getCapacity(label), cap.getMaximumCapacity(label));

      expectedEntitlements.put(label, expectedEntitlement);

      validateEffectiveMinResource(leafQueue, label, expectedEntitlements);
    }

    assertEquals(true, policy.isActive(leafQueue));
  }

  protected void validateCapacitiesByLabel(
      ManagedParentQueue autoCreateEnabledParentQueue,
      AutoCreatedLeafQueue leafQueue, String label) {
    assertEquals(
        autoCreateEnabledParentQueue.getLeafQueueTemplate().getQueueCapacities()
            .getCapacity(), leafQueue.getQueueCapacities().getCapacity(label),
        EPSILON);
    assertEquals(
        autoCreateEnabledParentQueue.getLeafQueueTemplate().getQueueCapacities()
            .getMaximumCapacity(),
        leafQueue.getQueueCapacities().getMaximumCapacity(label), EPSILON);
  }

  protected void validateEffectiveMinResource(CSQueue leafQueue,
      String label, Map<String, QueueEntitlement> expectedQueueEntitlements) {
    ManagedParentQueue parentQueue = (ManagedParentQueue) leafQueue.getParent();

    Resource resourceByLabel = mockRM.getRMContext().getNodeLabelManager().
        getResourceByLabel(label, cs.getClusterResource());
    Resource effMinCapacity = Resources.multiply(resourceByLabel,
        expectedQueueEntitlements.get(label).getCapacity() * parentQueue
            .getQueueCapacities().getAbsoluteCapacity(label));
    assertEquals(effMinCapacity, Resources.multiply(resourceByLabel,
        leafQueue.getQueueCapacities().getAbsoluteCapacity(label)));
    assertEquals(effMinCapacity, leafQueue.getEffectiveCapacity(label));

    if (leafQueue.getQueueCapacities().getAbsoluteCapacity(label) > 0) {
      assertTrue(Resources
          .greaterThan(cs.getResourceCalculator(), cs.getClusterResource(),
              effMinCapacity, Resources.none()));
    } else{
      assertTrue(Resources.equals(effMinCapacity, Resources.none()));
    }
  }

  protected void validateActivatedQueueEntitlement(CSQueue parentQueue,
      String leafQueueName, float expectedTotalChildQueueAbsCapacity,
      List<QueueManagementChange> queueManagementChanges)
      throws SchedulerDynamicEditException {
    ManagedParentQueue autoCreateEnabledParentQueue =
        (ManagedParentQueue) parentQueue;

    GuaranteedOrZeroCapacityOverTimePolicy policy =
        (GuaranteedOrZeroCapacityOverTimePolicy) autoCreateEnabledParentQueue
            .getAutoCreatedQueueManagementPolicy();

    QueueCapacities cap = autoCreateEnabledParentQueue.getLeafQueueTemplate()
        .getQueueCapacities();
    QueueEntitlement expectedEntitlement = new QueueEntitlement(
        cap.getCapacity(), cap.getMaximumCapacity());

    //validate capacity
    validateQueueEntitlements(leafQueueName, expectedEntitlement,
        queueManagementChanges);

    //validate parent queue state
    assertEquals(expectedTotalChildQueueAbsCapacity,
        policy.getAbsoluteActivatedChildQueueCapacity(), EPSILON);

    AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) cs.getQueue(
        leafQueueName);

    //validate leaf queue state
    assertEquals(true, policy.isActive(leafQueue));
  }

  protected void validateDeactivatedQueueEntitlement(CSQueue parentQueue,
      String leafQueueName, float expectedTotalChildQueueAbsCapacity,
      List<QueueManagementChange> queueManagementChanges)
      throws SchedulerDynamicEditException {
    QueueEntitlement expectedEntitlement = new QueueEntitlement(0.0f, 1.0f);

    ManagedParentQueue autoCreateEnabledParentQueue =
        (ManagedParentQueue) parentQueue;

    AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) cs.getQueue(
        leafQueueName);

    GuaranteedOrZeroCapacityOverTimePolicy policy =
        (GuaranteedOrZeroCapacityOverTimePolicy) autoCreateEnabledParentQueue
            .getAutoCreatedQueueManagementPolicy();

    //validate parent queue state
    assertEquals(expectedTotalChildQueueAbsCapacity,
        policy.getAbsoluteActivatedChildQueueCapacity(), EPSILON);

    //validate leaf queue state
    assertEquals(false, policy.isActive(leafQueue));

    //validate capacity
    validateQueueEntitlements(leafQueueName, expectedEntitlement,
        queueManagementChanges);
  }

  private void validateQueueEntitlements(String leafQueueName,
      QueueEntitlement expectedEntitlement,
      List<QueueManagementChange> queueEntitlementChanges) {
    AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) cs.getQueue(
        leafQueueName);
    validateQueueEntitlementChangesForLeafQueue(leafQueue, expectedEntitlement,
        queueEntitlementChanges);
  }

  private void validateQueueEntitlementChangesForLeafQueue(CSQueue leafQueue,
      QueueEntitlement expectedQueueEntitlement,
      final List<QueueManagementChange> queueEntitlementChanges) {
    boolean found = false;

    Map<String, QueueEntitlement> expectedQueueEntitlements = new HashMap<>();
    for (QueueManagementChange entitlementChange : queueEntitlementChanges) {
      if (leafQueue.getQueueName().equals(
          entitlementChange.getQueue().getQueueName())) {

        AutoCreatedLeafQueueConfig updatedQueueTemplate =
            entitlementChange.getUpdatedQueueTemplate();

        for (String label : accessibleNodeLabelsOnC) {
          QueueEntitlement newEntitlement = new QueueEntitlement(
              updatedQueueTemplate.getQueueCapacities().getCapacity(label),
              updatedQueueTemplate.getQueueCapacities()
                  .getMaximumCapacity(label));
          assertEquals(expectedQueueEntitlement, newEntitlement);
          expectedQueueEntitlements.put(label, expectedQueueEntitlement);
          validateEffectiveMinResource(leafQueue, label,
              expectedQueueEntitlements);
        }
        found = true;
        break;
      }
    }
    if (!found) {
      fail("Could not find the specified leaf queue in entitlement changes : "
          + leafQueue.getQueueName());
    }
  }

}
