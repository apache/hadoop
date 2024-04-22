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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.MockNM.createMockNodeStatus;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.ROOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.appHelper;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.checkApplicationResourceUsage;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.checkNodeResourceUsage;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.createMockRMContext;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.createResourceManager;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.nodeUpdate;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.registerNode;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.setUpMove;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.setUpMoveAmbiguousQueue;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.stopResourceManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCapacitySchedulerApps {

  public static final int MAX_PARALLEL_APPS = 5;
  public static final String USER_0 = "user_0";
  private ResourceManager resourceManager = null;
  private RMContext mockContext;

  @Before
  public void setUp() throws Exception {
    resourceManager = createResourceManager();
    mockContext = createMockRMContext();
  }

  @After
  public void tearDown() throws Exception {
    stopResourceManager(resourceManager);
  }

  @Test
  public void testGetAppsInQueue() throws Exception {
    Application application0 = new Application("user_0", "a1", resourceManager);
    application0.submit();

    Application application1 = new Application("user_0", "a2", resourceManager);
    application1.submit();

    Application application2 = new Application("user_0", "b2", resourceManager);
    application2.submit();

    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(application0.getApplicationAttemptId()));
    assertTrue(appsInA.contains(application1.getApplicationAttemptId()));
    assertEquals(2, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(application0.getApplicationAttemptId()));
    assertTrue(appsInRoot.contains(application1.getApplicationAttemptId()));
    assertTrue(appsInRoot.contains(application2.getApplicationAttemptId()));
    assertEquals(3, appsInRoot.size());

    Assert.assertNull(scheduler.getAppsInQueue("nonexistentqueue"));
  }

  @Test
  public void testAddAndRemoveAppFromCapacityScheduler() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    @SuppressWarnings("unchecked")
    AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode> cs =
        (AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>) rm
            .getResourceScheduler();
    SchedulerApplication<SchedulerApplicationAttempt> app =
        TestSchedulerUtils.verifyAppAddedAndRemovedFromScheduler(
            cs.getSchedulerApplications(), cs, "a1");
    Assert.assertEquals("a1", app.getQueue().getQueueName());
  }

  @Test
  public void testKillAllAppsInQueue() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());
    String queue =
        scheduler.getApplicationAttempt(appsInA1.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("a1", queue);

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    // now kill the app
    scheduler.killAllAppsInQueue("a1");

    // check postconditions
    rm.waitForState(app.getApplicationId(), RMAppState.KILLED);
    rm.waitForAppRemovedFromScheduler(app.getApplicationId());
    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.isEmpty());

    appsInA1 = scheduler.getAppsInQueue("a1");
    assertTrue(appsInA1.isEmpty());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.isEmpty());

    rm.stop();
  }

  @Test
  public void testKillAllAppsInvalidSource() throws Exception {
    MockRM rm = setUpMove();
    YarnScheduler scheduler = rm.getResourceScheduler();

    // submit an app
    ApplicationAttemptId appAttemptId = submitApp(rm);

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    // now kill the app
    try {
      scheduler.killAllAppsInQueue("DOES_NOT_EXIST");
      Assert.fail();
    } catch (YarnException e) {
      // expected
    }

    // check postconditions, app should still be in a1
    appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    rm.stop();
  }

  // Test to ensure that we don't carry out reservation on nodes
  // that have no CPU available when using the DominantResourceCalculator
  @Test(timeout = 30000)
  public void testAppReservationWithDominantResourceCalculator() throws Exception {
    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setResourceComparator(DominantResourceCalculator.class);

    YarnConfiguration conf = new YarnConfiguration(csconf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    MockRM rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10 * GB, 1);

    // register extra nodes to bump up cluster resource
    MockNM nm2 = rm.registerNode("127.0.0.1:1235", 10 * GB, 4);
    rm.registerNode("127.0.0.1:1236", 10 * GB, 4);

    RMApp app1 = MockRMAppSubmitter.submitWithMemory(1024, rm);
    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    SchedulerNodeReport reportNm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());

    // check node report
    Assert.assertEquals(1 * GB, reportNm1.getUsedResource().getMemorySize());
    Assert.assertEquals(9 * GB, reportNm1.getAvailableResource().getMemorySize());

    // add request for containers
    am1.addRequests(new String[]{"127.0.0.1", "127.0.0.2"}, 1 * GB, 1, 1);
    am1.schedule(); // send the request

    // kick the scheduler, container reservation should not happen
    nm1.nodeHeartbeat(true);
    Thread.sleep(1000);
    AllocateResponse allocResponse = am1.schedule();
    ApplicationResourceUsageReport report =
        rm.getResourceScheduler().getAppResourceUsageReport(
            attempt1.getAppAttemptId());
    Assert.assertEquals(0, allocResponse.getAllocatedContainers().size());
    Assert.assertEquals(0, report.getNumReservedContainers());

    // container should get allocated on this node
    nm2.nodeHeartbeat(true);

    while (allocResponse.getAllocatedContainers().size() == 0) {
      Thread.sleep(100);
      allocResponse = am1.schedule();
    }
    report =
        rm.getResourceScheduler().getAppResourceUsageReport(
            attempt1.getAppAttemptId());
    Assert.assertEquals(1, allocResponse.getAllocatedContainers().size());
    Assert.assertEquals(0, report.getNumReservedContainers());
    rm.stop();
  }

  @Test
  public void testMoveAppAmbiguousQueue() throws Exception {
    MockRM rm = setUpMoveAmbiguousQueue();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();
    QueueMetrics metrics = scheduler.getRootQueueMetrics();
    Assert.assertEquals(0, metrics.getAppsPending());
    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("root.a.a")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    // check preconditions
    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("root.a.a");
    assertEquals(1, appsInA.size());
    String queue =
        scheduler.getApplicationAttempt(appsInA.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("a", queue);

    // now move the app
    scheduler.moveApplication(app.getApplicationId(), "a1");
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("root.a.a1");
    assertEquals(1, appsInA1.size());
    queue =
        scheduler.getApplicationAttempt(appsInA1.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("a1", queue);

    appsInA = scheduler.getAppsInQueue("root.a.a");
    assertTrue(appsInA.isEmpty());

    rm.stop();
  }

  @Test
  public void testMoveAppBasic() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();
    QueueMetrics metrics = scheduler.getRootQueueMetrics();
    Assert.assertEquals(0, metrics.getAppsPending());
    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();
    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());
    String queue =
        scheduler.getApplicationAttempt(appsInA1.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("a1", queue);

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    assertEquals(1, metrics.getAppsPending());

    List<ApplicationAttemptId> appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    List<ApplicationAttemptId> appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    // now move the app
    scheduler.moveApplication(app.getApplicationId(), "b1");

    // check postconditions
    appsInB1 = scheduler.getAppsInQueue("b1");
    assertEquals(1, appsInB1.size());
    queue =
        scheduler.getApplicationAttempt(appsInB1.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("b1", queue);

    appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.contains(appAttemptId));
    assertEquals(1, appsInB.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    assertEquals(1, metrics.getAppsPending());

    appsInA1 = scheduler.getAppsInQueue("a1");
    assertTrue(appsInA1.isEmpty());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.isEmpty());

    rm.stop();
  }

  @Test
  public void testMoveAppPendingMetrics() throws Exception {
    MockRM rm = setUpMove();
    ResourceScheduler scheduler = rm.getResourceScheduler();
    assertApps(scheduler, 0, 0, 0);

    // submit two apps in a1
    RMApp app1 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .build());
    RMApp app2 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-2")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .build());
    assertApps(scheduler, 2, 0, 2);

    // submit one app in b1
    RMApp app3 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-2")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("b1")
            .build());
    assertApps(scheduler, 2, 1, 3);

    // now move the app1 from a1 to b1
    scheduler.moveApplication(app1.getApplicationId(), "b1");
    assertApps(scheduler, 1, 2, 3);

    // now move the app2 from a1 to b1
    scheduler.moveApplication(app2.getApplicationId(), "b1");
    assertApps(scheduler, 0, 3, 3);

    // now move the app3 from b1 to a1
    scheduler.moveApplication(app3.getApplicationId(), "a1");
    assertApps(scheduler, 1, 2, 3);
    rm.stop();
  }

  private void assertApps(ResourceScheduler scheduler,
                          int a1Size,
                          int b1Size,
                          int appsPending) {
    assertAppsSize(scheduler, "a1", a1Size);
    assertAppsSize(scheduler, "b1", b1Size);
    assertEquals(appsPending, scheduler.getRootQueueMetrics().getAppsPending());
  }

  private void assertAppsSize(ResourceScheduler scheduler, String queueName, int size) {
    assertEquals(size, scheduler.getAppsInQueue(queueName).size());
  }

  @Test
  public void testMoveAppSameParent() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    assertOneAppInQueue(scheduler, "a1");
    assertApps(scheduler, "root", appAttemptId);
    assertApps(scheduler, "a", appAttemptId);
    assertApps(scheduler, "a2");

    // now move the app
    scheduler.moveApplication(app.getApplicationId(), "a2");

    // check postconditions
    assertApps(scheduler, "root", appAttemptId);
    assertApps(scheduler, "a", appAttemptId);
    assertApps(scheduler, "a1");
    assertOneAppInQueue(scheduler, "a2");

    rm.stop();
  }

  private void assertApps(ResourceScheduler scheduler,
                          String queueName,
                          ApplicationAttemptId... apps) {
    assertEquals(Lists.newArrayList(apps), scheduler.getAppsInQueue(queueName));
  }

  private void assertOneAppInQueue(AbstractYarnScheduler scheduler, String queueName) {
    List<ApplicationAttemptId> apps = scheduler.getAppsInQueue(queueName);
    assertEquals(1, apps.size());
    Assert.assertEquals(queueName,
        scheduler.getApplicationAttempt(apps.get(0)).getQueue().getQueueName());
  }

  @Test
  public void testMoveAppForMoveToQueueWithFreeCap() throws Exception {

    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host0 = "host_0";
    NodeManager nm0 =
        registerNode(resourceManager, host0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(4 * GB, 1), mockNodeStatus);

    // Register node2
    String host1 = "host_1";
    NodeManager nm1 =
        registerNode(resourceManager, host1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(2 * GB, 1), mockNodeStatus);

    // ResourceRequest priorities
    Priority priority0 = Priority.newInstance(0);
    Priority priority1 = Priority.newInstance(1);

    // Submit application_0
    Application application0 =
        new Application("user_0", "a1", resourceManager);
    application0.submit(); // app + app attempt event sent to scheduler

    application0.addNodeManager(host0, 1234, nm0);
    application0.addNodeManager(host1, 1234, nm1);

    Resource capability00 = Resources.createResource(1 * GB, 1);
    application0.addResourceRequestSpec(priority1, capability00);

    Resource capability01 = Resources.createResource(2 * GB, 1);
    application0.addResourceRequestSpec(priority0, capability01);

    Task task00 =
        new Task(application0, priority1, new String[]{host0, host1});
    application0.addTask(task00);

    // Submit application_1
    Application application1 =
        new Application("user_1", "b2", resourceManager);
    application1.submit(); // app + app attempt event sent to scheduler

    application1.addNodeManager(host0, 1234, nm0);
    application1.addNodeManager(host1, 1234, nm1);

    Resource capability10 = Resources.createResource(1 * GB, 1);
    application1.addResourceRequestSpec(priority1, capability10);

    Resource capability11 = Resources.createResource(2 * GB, 1);
    application1.addResourceRequestSpec(priority0, capability11);

    Task task10 =
        new Task(application1, priority1, new String[]{host0, host1});
    application1.addTask(task10);

    // Send resource requests to the scheduler
    application0.schedule(); // allocate
    application1.schedule(); // allocate

    // task_0_0 task_1_0 allocated, used=2G
    nodeUpdate(resourceManager, nm0);

    // nothing allocated
    nodeUpdate(resourceManager, nm1);

    // Get allocations from the scheduler
    application0.schedule(); // task_0_0
    checkApplicationResourceUsage(1 * GB, application0);

    application1.schedule(); // task_1_0
    checkApplicationResourceUsage(1 * GB, application1);

    checkNodeResourceUsage(2 * GB, nm0); // task_0_0 (1G) and task_1_0 (1G) 2G
    // available
    checkNodeResourceUsage(0 * GB, nm1); // no tasks, 2G available

    // move app from a1(30% cap of total 10.5% cap) to b1(79,2% cap of 89,5%
    // total cap)
    scheduler.moveApplication(application0.getApplicationId(), "b1");

    // 2GB 1C
    Task task11 =
        new Task(application1, priority0,
            new String[]{ResourceRequest.ANY});
    application1.addTask(task11);

    application1.schedule();

    // 2GB 1C
    Task task01 =
        new Task(application0, priority0, new String[]{host0, host1});
    application0.addTask(task01);

    application0.schedule();

    // prev 2G used free 2G
    nodeUpdate(resourceManager, nm0);

    // prev 0G used free 2G
    nodeUpdate(resourceManager, nm1);

    // Get allocations from the scheduler
    application1.schedule();
    checkApplicationResourceUsage(3 * GB, application1);

    // Get allocations from the scheduler
    application0.schedule();
    checkApplicationResourceUsage(3 * GB, application0);

    checkNodeResourceUsage(4 * GB, nm0);
    checkNodeResourceUsage(2 * GB, nm1);
  }

  @Test
  public void testMoveAppSuccess() throws Exception {

    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host0 = "host_0";
    NodeManager nm0 =
        registerNode(resourceManager, host0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1), mockNodeStatus);

    // Register node2
    String host1 = "host_1";
    NodeManager nm1 =
        registerNode(resourceManager, host1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1), mockNodeStatus);

    // ResourceRequest priorities
    Priority priority0 = Priority.newInstance(0);
    Priority priority1 = Priority.newInstance(1);

    // Submit application_0
    Application application0 =
        new Application("user_0", "a1", resourceManager);
    application0.submit(); // app + app attempt event sent to scheduler

    application0.addNodeManager(host0, 1234, nm0);
    application0.addNodeManager(host1, 1234, nm1);

    Resource capability00 = Resources.createResource(3 * GB, 1);
    application0.addResourceRequestSpec(priority1, capability00);

    Resource capability01 = Resources.createResource(2 * GB, 1);
    application0.addResourceRequestSpec(priority0, capability01);

    Task task00 =
        new Task(application0, priority1, new String[]{host0, host1});
    application0.addTask(task00);

    // Submit application_1
    Application application1 =
        new Application("user_1", "b2", resourceManager);
    application1.submit(); // app + app attempt event sent to scheduler

    application1.addNodeManager(host0, 1234, nm0);
    application1.addNodeManager(host1, 1234, nm1);

    Resource capability10 = Resources.createResource(1 * GB, 1);
    application1.addResourceRequestSpec(priority1, capability10);

    Resource capability11 = Resources.createResource(2 * GB, 1);
    application1.addResourceRequestSpec(priority0, capability11);

    Task task10 =
        new Task(application1, priority1, new String[]{host0, host1});
    application1.addTask(task10);

    // Send resource requests to the scheduler
    application0.schedule(); // allocate
    application1.schedule(); // allocate

    // b2 can only run 1 app at a time
    scheduler.moveApplication(application0.getApplicationId(), "b2");

    nodeUpdate(resourceManager, nm0);

    nodeUpdate(resourceManager, nm1);

    // Get allocations from the scheduler
    application0.schedule(); // task_0_0
    checkApplicationResourceUsage(0 * GB, application0);

    application1.schedule(); // task_1_0
    checkApplicationResourceUsage(1 * GB, application1);

    // task_1_0 (1G) application_0 moved to b2 with max running app 1 so it is
    // not scheduled
    checkNodeResourceUsage(1 * GB, nm0);
    checkNodeResourceUsage(0 * GB, nm1);

    // lets move application_0 to a queue where it can run
    scheduler.moveApplication(application0.getApplicationId(), "a2");
    application0.schedule();

    nodeUpdate(resourceManager, nm1);

    // Get allocations from the scheduler
    application0.schedule(); // task_0_0
    checkApplicationResourceUsage(3 * GB, application0);

    checkNodeResourceUsage(1 * GB, nm0);
    checkNodeResourceUsage(3 * GB, nm1);

  }

  @Test(expected = YarnException.class)
  public void testMoveAppViolateQueueState() throws Exception {
    resourceManager = new ResourceManager() {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    StringBuilder qState = new StringBuilder();
    qState.append(CapacitySchedulerConfiguration.PREFIX).append(B)
        .append(CapacitySchedulerConfiguration.DOT)
        .append(CapacitySchedulerConfiguration.STATE);
    csConf.set(qState.toString(), QueueState.STOPPED.name());
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    resourceManager.init(conf);
    resourceManager.getRMContext().getContainerTokenSecretManager()
        .rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();
    ((AsyncDispatcher) resourceManager.getRMContext().getDispatcher()).start();
    mockContext = mock(RMContext.class);
    when(mockContext.getConfigurationProvider()).thenReturn(
        new LocalConfigurationProvider());

    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host0 = "host_0";
    NodeManager nm0 =
        registerNode(resourceManager, host0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(6 * GB, 1), mockNodeStatus);

    // ResourceRequest priorities
    Priority priority0 = Priority.newInstance(0);
    Priority priority1 = Priority.newInstance(1);

    // Submit application_0
    Application application0 =
        new Application("user_0", "a1", resourceManager);
    application0.submit(); // app + app attempt event sent to scheduler

    application0.addNodeManager(host0, 1234, nm0);

    Resource capability00 = Resources.createResource(3 * GB, 1);
    application0.addResourceRequestSpec(priority1, capability00);

    Resource capability01 = Resources.createResource(2 * GB, 1);
    application0.addResourceRequestSpec(priority0, capability01);

    Task task00 =
        new Task(application0, priority1, new String[]{host0});
    application0.addTask(task00);

    // Send resource requests to the scheduler
    application0.schedule(); // allocate

    // task_0_0 allocated
    nodeUpdate(resourceManager, nm0);

    // Get allocations from the scheduler
    application0.schedule(); // task_0_0
    checkApplicationResourceUsage(3 * GB, application0);

    checkNodeResourceUsage(3 * GB, nm0);
    // b2 queue contains 3GB consumption app,
    // add another 3GB will hit max capacity limit on queue b
    scheduler.moveApplication(application0.getApplicationId(), "b1");

  }

  @Test
  public void testMoveAppQueueMetricsCheck() throws Exception {
    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host0 = "host_0";
    NodeManager nm0 =
        registerNode(resourceManager, host0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1), mockNodeStatus);

    // Register node2
    String host1 = "host_1";
    NodeManager nm1 =
        registerNode(resourceManager, host1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1), mockNodeStatus);

    // ResourceRequest priorities
    Priority priority0 = Priority.newInstance(0);
    Priority priority1 = Priority.newInstance(1);

    // Submit application_0
    Application application0 =
        new Application("user_0", "a1", resourceManager);
    application0.submit(); // app + app attempt event sent to scheduler

    application0.addNodeManager(host0, 1234, nm0);
    application0.addNodeManager(host1, 1234, nm1);

    Resource capability00 = Resources.createResource(3 * GB, 1);
    application0.addResourceRequestSpec(priority1, capability00);

    Resource capability01 = Resources.createResource(2 * GB, 1);
    application0.addResourceRequestSpec(priority0, capability01);

    Task task00 =
        new Task(application0, priority1, new String[]{host0, host1});
    application0.addTask(task00);

    // Submit application_1
    Application application1 =
        new Application("user_1", "b2", resourceManager);
    application1.submit(); // app + app attempt event sent to scheduler

    application1.addNodeManager(host0, 1234, nm0);
    application1.addNodeManager(host1, 1234, nm1);

    Resource capability10 = Resources.createResource(1 * GB, 1);
    application1.addResourceRequestSpec(priority1, capability10);

    Resource capability11 = Resources.createResource(2 * GB, 1);
    application1.addResourceRequestSpec(priority0, capability11);

    Task task10 =
        new Task(application1, priority1, new String[]{host0, host1});
    application1.addTask(task10);

    // Send resource requests to the scheduler
    application0.schedule(); // allocate
    application1.schedule(); // allocate

    nodeUpdate(resourceManager, nm0);

    nodeUpdate(resourceManager, nm1);

    CapacityScheduler cs =
        (CapacityScheduler) resourceManager.getResourceScheduler();
    CSQueue origRootQ = cs.getRootQueue();
    CapacitySchedulerInfo oldInfo =
        new CapacitySchedulerInfo(origRootQ, cs);
    int origNumAppsA = getNumAppsInQueue("a", origRootQ.getChildQueues());
    int origNumAppsRoot = origRootQ.getNumApplications();

    scheduler.moveApplication(application0.getApplicationId(), "a2");

    CSQueue newRootQ = cs.getRootQueue();
    int newNumAppsA = getNumAppsInQueue("a", newRootQ.getChildQueues());
    int newNumAppsRoot = newRootQ.getNumApplications();
    CapacitySchedulerInfo newInfo =
        new CapacitySchedulerInfo(newRootQ, cs);
    CapacitySchedulerLeafQueueInfo origOldA1 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a1", oldInfo.getQueues());
    CapacitySchedulerLeafQueueInfo origNewA1 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a1", newInfo.getQueues());
    CapacitySchedulerLeafQueueInfo targetOldA2 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a2", oldInfo.getQueues());
    CapacitySchedulerLeafQueueInfo targetNewA2 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a2", newInfo.getQueues());
    // originally submitted here
    assertEquals(1, origOldA1.getNumApplications());
    assertEquals(1, origNumAppsA);
    assertEquals(2, origNumAppsRoot);
    // after the move
    assertEquals(0, origNewA1.getNumApplications());
    assertEquals(1, newNumAppsA);
    assertEquals(2, newNumAppsRoot);
    // original consumption on a1
    assertEquals(3 * GB, origOldA1.getResourcesUsed().getMemorySize());
    assertEquals(1, origOldA1.getResourcesUsed().getvCores());
    assertEquals(0, origNewA1.getResourcesUsed().getMemorySize()); // after the move
    assertEquals(0, origNewA1.getResourcesUsed().getvCores()); // after the move
    // app moved here with live containers
    assertEquals(3 * GB, targetNewA2.getResourcesUsed().getMemorySize());
    assertEquals(1, targetNewA2.getResourcesUsed().getvCores());
    // it was empty before the move
    assertEquals(0, targetOldA2.getNumApplications());
    assertEquals(0, targetOldA2.getResourcesUsed().getMemorySize());
    assertEquals(0, targetOldA2.getResourcesUsed().getvCores());
    // after the app moved here
    assertEquals(1, targetNewA2.getNumApplications());
    // 1 container on original queue before move
    assertEquals(1, origOldA1.getNumContainers());
    // after the move the resource released
    assertEquals(0, origNewA1.getNumContainers());
    // and moved to the new queue
    assertEquals(1, targetNewA2.getNumContainers());
    // which originally didn't have any
    assertEquals(0, targetOldA2.getNumContainers());
    // 1 user with 3GB
    assertEquals(3 * GB, origOldA1.getUsers().getUsersList().get(0)
        .getResourcesUsed().getMemorySize());
    // 1 user with 1 core
    assertEquals(1, origOldA1.getUsers().getUsersList().get(0)
        .getResourcesUsed().getvCores());
    // user ha no more running app in the orig queue
    assertEquals(0, origNewA1.getUsers().getUsersList().size());
    // 1 user with 3GB
    assertEquals(3 * GB, targetNewA2.getUsers().getUsersList().get(0)
        .getResourcesUsed().getMemorySize());
    // 1 user with 1 core
    assertEquals(1, targetNewA2.getUsers().getUsersList().get(0)
        .getResourcesUsed().getvCores());

    // Get allocations from the scheduler
    application0.schedule(); // task_0_0
    checkApplicationResourceUsage(3 * GB, application0);

    application1.schedule(); // task_1_0
    checkApplicationResourceUsage(1 * GB, application1);

    // task_1_0 (1G) application_0 moved to b2 with max running app 1 so it is
    // not scheduled
    checkNodeResourceUsage(4 * GB, nm0);
    checkNodeResourceUsage(0 * GB, nm1);

  }

  @Test
  public void testMoveAllApps() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    ApplicationAttemptId appAttemptId = submitApp(rm);

    // check preconditions
    assertOneAppInQueue(scheduler, "a1");
    assertApps(scheduler, "root", appAttemptId);
    assertApps(scheduler, "a", appAttemptId);
    assertApps(scheduler, "a1", appAttemptId);
    assertApps(scheduler, "b1");
    assertApps(scheduler, "b");

    // now move the app
    scheduler.moveAllApps("a1", "b1");

    // check post conditions
    Thread.sleep(1000);
    assertOneAppInQueue(scheduler, "b1");
    assertApps(scheduler, "root", appAttemptId);
    assertApps(scheduler, "b", appAttemptId);
    assertApps(scheduler, "b1", appAttemptId);
    assertApps(scheduler, "a1");
    assertApps(scheduler, "a");

    rm.stop();
  }

  @Test
  public void testMaxParallelAppsPendingQueueMetrics() throws Exception {
    MockRM rm = setUpMove();
    ResourceScheduler scheduler = rm.getResourceScheduler();
    CapacityScheduler cs = (CapacityScheduler) scheduler;
    cs.getQueueContext().getConfiguration().setInt(QueuePrefixes.getQueuePrefix(A1)
        + CapacitySchedulerConfiguration.MAX_PARALLEL_APPLICATIONS, MAX_PARALLEL_APPS);
    cs.reinitialize(cs.getQueueContext().getConfiguration(), mockContext);
    List<ApplicationAttemptId> attemptIds = new ArrayList<>();

    for (int i = 0; i < 2 * MAX_PARALLEL_APPS; i++) {
      attemptIds.add(submitApp(rm));
    }

    // Finish first batch to allow the other batch to run
    for (int i = 0; i < MAX_PARALLEL_APPS; i++) {
      cs.handle(new AppAttemptRemovedSchedulerEvent(attemptIds.get(i),
          RMAppAttemptState.FINISHED, true));
    }

    // Finish the remaining apps
    for (int i = MAX_PARALLEL_APPS; i < 2 * MAX_PARALLEL_APPS; i++) {
      cs.handle(new AppAttemptRemovedSchedulerEvent(attemptIds.get(i),
          RMAppAttemptState.FINISHED, true));
    }

    Assert.assertEquals("No pending app should remain for root queue", 0,
        cs.getRootQueueMetrics().getAppsPending());
    Assert.assertEquals("No running application should remain for root queue", 0,
        cs.getRootQueueMetrics().getAppsRunning());

    rm.stop();
  }

  private ApplicationAttemptId submitApp(MockRM rm) throws Exception {
    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser(USER_0)
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    return rm.getApplicationReport(app.getApplicationId())
        .getCurrentApplicationAttemptId();
  }

  @Test
  public void testMoveAllAppsInvalidDestination() throws Exception {
    MockRM rm = setUpMove();
    ResourceScheduler scheduler = rm.getResourceScheduler();

    // submit an app
    ApplicationAttemptId appAttemptId = submitApp(rm);

    // check preconditions
    assertApps(scheduler, "root", appAttemptId);
    assertApps(scheduler, "a", appAttemptId);
    assertApps(scheduler, "a1", appAttemptId);
    assertApps(scheduler, "b");
    assertApps(scheduler, "b1");

    // now move the app
    try {
      scheduler.moveAllApps("a1", "DOES_NOT_EXIST");
      Assert.fail();
    } catch (YarnException e) {
      // expected
    }

    // check post conditions, app should still be in a1
    assertApps(scheduler, "root", appAttemptId);
    assertApps(scheduler, "a", appAttemptId);
    assertApps(scheduler, "a1", appAttemptId);
    assertApps(scheduler, "b");
    assertApps(scheduler, "b1");

    rm.stop();
  }

  @Test
  public void testMoveAllAppsInvalidSource() throws Exception {
    MockRM rm = setUpMove();
    ResourceScheduler scheduler = rm.getResourceScheduler();

    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    assertApps(scheduler, "root", appAttemptId);
    assertApps(scheduler, "a", appAttemptId);
    assertApps(scheduler, "a1", appAttemptId);
    assertApps(scheduler, "b");
    assertApps(scheduler, "b1");

    // now move the app
    try {
      scheduler.moveAllApps("DOES_NOT_EXIST", "b1");
      Assert.fail();
    } catch (YarnException e) {
      // expected
    }

    // check post conditions, app should still be in a1
    assertApps(scheduler, "root", appAttemptId);
    assertApps(scheduler, "a", appAttemptId);
    assertApps(scheduler, "a1", appAttemptId);
    assertApps(scheduler, "b");
    assertApps(scheduler, "b1");

    rm.stop();
  }

  @Test
  public void testMoveAppWithActiveUsersWithOnlyPendingApps() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration(conf);

    // Define top-level queues
    newConf.setQueues(ROOT,
        new String[]{"a", "b"});

    newConf.setCapacity(A, 50);
    newConf.setCapacity(B, 50);

    // Define 2nd-level queues
    newConf.setQueues(A, new String[]{"a1"});
    newConf.setCapacity(A1, 100);
    newConf.setUserLimitFactor(A1, 2.0f);
    newConf.setMaximumAMResourcePercentPerPartition(A1, "", 0.1f);

    newConf.setQueues(B, new String[]{"b1"});
    newConf.setCapacity(B1, 100);
    newConf.setUserLimitFactor(B1, 2.0f);

    MockRM rm = new MockRM(newConf);
    rm.start();

    CapacityScheduler scheduler =
        (CapacityScheduler) rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("h1:1234", 16 * GB);

    // submit an app
    MockRMAppSubmissionData data3 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("u1")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data3);
    MockAM am1 = MockRM.launchAndRegisterAM(app, rm, nm1);

    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("u2")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data2);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);

    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("u3")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm, data1);

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("u4")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app4 = MockRMAppSubmitter.submit(rm, data);

    // Each application asks 50 * 1GB containers
    am1.allocate("*", 1 * GB, 50, null);
    am2.allocate("*", 1 * GB, 50, null);

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());

    // check preconditions
    assertApps(scheduler, "root",
        app3.getCurrentAppAttempt().getAppAttemptId(),
        app4.getCurrentAppAttempt().getAppAttemptId(),
        appAttemptId,
        app2.getCurrentAppAttempt().getAppAttemptId());
    assertApps(scheduler, "a",
        app3.getCurrentAppAttempt().getAppAttemptId(),
        app4.getCurrentAppAttempt().getAppAttemptId(),
        appAttemptId,
        app2.getCurrentAppAttempt().getAppAttemptId());
    assertApps(scheduler, "a1",
        app3.getCurrentAppAttempt().getAppAttemptId(),
        app4.getCurrentAppAttempt().getAppAttemptId(),
        appAttemptId,
        app2.getCurrentAppAttempt().getAppAttemptId());
    assertApps(scheduler, "b");
    assertApps(scheduler, "b1");

    UsersManager um =
        (UsersManager) scheduler.getQueue("a1").getAbstractUsersManager();

    assertEquals(4, um.getNumActiveUsers());
    assertEquals(2, um.getNumActiveUsersWithOnlyPendingApps());

    // now move the app
    scheduler.moveAllApps("a1", "b1");

    //Triggering this event so that user limit computation can
    //happen again
    for (int i = 0; i < 10; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      Thread.sleep(500);
    }

    // check post conditions
    assertApps(scheduler, "root",
        appAttemptId,
        app2.getCurrentAppAttempt().getAppAttemptId(),
        app3.getCurrentAppAttempt().getAppAttemptId(),
        app4.getCurrentAppAttempt().getAppAttemptId());
    assertApps(scheduler, "a");
    assertApps(scheduler, "a1");
    assertApps(scheduler, "b",
        appAttemptId,
        app2.getCurrentAppAttempt().getAppAttemptId(),
        app3.getCurrentAppAttempt().getAppAttemptId(),
        app4.getCurrentAppAttempt().getAppAttemptId());
    assertApps(scheduler, "b1",
        appAttemptId,
        app2.getCurrentAppAttempt().getAppAttemptId(),
        app3.getCurrentAppAttempt().getAppAttemptId(),
        app4.getCurrentAppAttempt().getAppAttemptId());

    UsersManager umB1 =
        (UsersManager) scheduler.getQueue("b1").getAbstractUsersManager();

    assertEquals(2, umB1.getNumActiveUsers());
    assertEquals(2, umB1.getNumActiveUsersWithOnlyPendingApps());

    rm.close();
  }

  @Test(timeout = 60000)
  public void testMoveAttemptNotAdded() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(getCapacityConfiguration(conf));
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    ApplicationId appId = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);

    RMAppAttemptMetrics attemptMetric =
        new RMAppAttemptMetrics(appAttemptId, rm.getRMContext());
    RMAppImpl app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(appId);
    RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
    Container container = mock(Container.class);
    when(attempt.getMasterContainer()).thenReturn(container);
    ApplicationSubmissionContext submissionContext =
        mock(ApplicationSubmissionContext.class);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);

    rm.getRMContext().getRMApps().put(appId, app);

    SchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(appId, "a1", "user");
    try {
      cs.moveApplication(appId, "b1");
      fail("Move should throw exception app not available");
    } catch (YarnException e) {
      assertEquals("App to be moved application_100_0001 not found.",
          e.getMessage());
    }
    cs.handle(addAppEvent);
    cs.moveApplication(appId, "b1");
    SchedulerEvent addAttemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    cs.handle(addAttemptEvent);
    CSQueue rootQ = cs.getRootQueue();
    CSQueue queueB = cs.getQueue("b");
    CSQueue queueA = cs.getQueue("a");
    CSQueue queueA1 = cs.getQueue("a1");
    CSQueue queueB1 = cs.getQueue("b1");
    Assert.assertEquals(1, rootQ.getNumApplications());
    Assert.assertEquals(0, queueA.getNumApplications());
    Assert.assertEquals(1, queueB.getNumApplications());
    Assert.assertEquals(0, queueA1.getNumApplications());
    Assert.assertEquals(1, queueB1.getNumApplications());

    rm.close();
  }

  @Test
  public void testRemoveAttemptMoveAdded() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        CapacityScheduler.class);
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    // Create Mock RM
    MockRM rm = new MockRM(getCapacityConfiguration(conf));
    CapacityScheduler sch = (CapacityScheduler) rm.getResourceScheduler();
    // add node
    Resource newResource = Resource.newInstance(4 * GB, 1);
    RMNode node = MockNodes.newNodeInfo(0, newResource, 1, "127.0.0.1");
    SchedulerEvent addNode = new NodeAddedSchedulerEvent(node);
    sch.handle(addNode);

    ApplicationAttemptId appAttemptId = appHelper(rm, sch, 100, 1, "a1", "user");

    // get Queues
    CSQueue queueA1 = sch.getQueue("a1");
    CSQueue queueB = sch.getQueue("b");
    CSQueue queueB1 = sch.getQueue("b1");

    // add Running rm container and simulate live containers to a1
    ContainerId newContainerId = ContainerId.newContainerId(appAttemptId, 2);
    RMContainerImpl rmContainer = mock(RMContainerImpl.class);
    when(rmContainer.getState()).thenReturn(RMContainerState.RUNNING);
    Container container2 = mock(Container.class);
    when(rmContainer.getContainer()).thenReturn(container2);
    Resource resource = Resource.newInstance(1024, 1);
    when(container2.getResource()).thenReturn(resource);
    when(rmContainer.getExecutionType()).thenReturn(ExecutionType.GUARANTEED);
    when(container2.getNodeId()).thenReturn(node.getNodeID());
    when(container2.getId()).thenReturn(newContainerId);
    when(rmContainer.getNodeLabelExpression())
        .thenReturn(RMNodeLabelsManager.NO_LABEL);
    when(rmContainer.getContainerId()).thenReturn(newContainerId);
    sch.getApplicationAttempt(appAttemptId).getLiveContainersMap()
        .put(newContainerId, rmContainer);
    QueueMetrics queueA1M = queueA1.getMetrics();
    queueA1M.incrPendingResources(rmContainer.getNodeLabelExpression(),
        "user1", 1, resource);
    queueA1M.allocateResources(rmContainer.getNodeLabelExpression(),
        "user1", resource);
    // remove attempt
    sch.handle(new AppAttemptRemovedSchedulerEvent(appAttemptId,
        RMAppAttemptState.KILLED, true));
    // Move application to queue b1
    sch.moveApplication(appAttemptId.getApplicationId(), "b1");
    // Check queue metrics after move
    Assert.assertEquals(0, queueA1.getNumApplications());
    Assert.assertEquals(1, queueB.getNumApplications());
    Assert.assertEquals(0, queueB1.getNumApplications());

    // Release attempt add event
    ApplicationAttemptId appAttemptId2 =
        BuilderUtils.newApplicationAttemptId(appAttemptId.getApplicationId(), 2);
    SchedulerEvent addAttemptEvent2 =
        new AppAttemptAddedSchedulerEvent(appAttemptId2, true);
    sch.handle(addAttemptEvent2);

    // Check metrics after attempt added
    Assert.assertEquals(0, queueA1.getNumApplications());
    Assert.assertEquals(1, queueB.getNumApplications());
    Assert.assertEquals(1, queueB1.getNumApplications());


    QueueMetrics queueB1M = queueB1.getMetrics();
    QueueMetrics queueBM = queueB.getMetrics();
    // Verify allocation MB of current state
    Assert.assertEquals(0, queueA1M.getAllocatedMB());
    Assert.assertEquals(0, queueA1M.getAllocatedVirtualCores());
    Assert.assertEquals(1024, queueB1M.getAllocatedMB());
    Assert.assertEquals(1, queueB1M.getAllocatedVirtualCores());

    // remove attempt
    sch.handle(new AppAttemptRemovedSchedulerEvent(appAttemptId2,
        RMAppAttemptState.FINISHED, false));

    Assert.assertEquals(0, queueA1M.getAllocatedMB());
    Assert.assertEquals(0, queueA1M.getAllocatedVirtualCores());
    Assert.assertEquals(0, queueB1M.getAllocatedMB());
    Assert.assertEquals(0, queueB1M.getAllocatedVirtualCores());

    verifyQueueMetrics(queueB1M);
    verifyQueueMetrics(queueBM);
    // Verify queue A1 metrics
    verifyQueueMetrics(queueA1M);
    rm.close();
  }

  @Test
  public void testAppSubmission() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setQueues(A, new String[]{"a1", "a2", "b"});
    conf.setCapacity(A1, 20);
    conf.setCapacity(new QueuePath("root.a.b"), 10);
    MockRM rm = new MockRM(conf);
    rm.start();

    RMApp noParentQueueApp = submitAppAndWaitForState(rm, "q", RMAppState.FAILED);
    Assert.assertEquals(RMAppState.FAILED, noParentQueueApp.getState());

    RMApp ambiguousQueueApp = submitAppAndWaitForState(rm, "b", RMAppState.FAILED);
    Assert.assertEquals(RMAppState.FAILED, ambiguousQueueApp.getState());

    RMApp emptyPartQueueApp = submitAppAndWaitForState(rm, "root..a1", RMAppState.FAILED);
    Assert.assertEquals(RMAppState.FAILED, emptyPartQueueApp.getState());

    RMApp failedAutoQueue = submitAppAndWaitForState(rm, "root.a.b.c.d", RMAppState.FAILED);
    Assert.assertEquals(RMAppState.FAILED, failedAutoQueue.getState());
  }

  private RMApp submitAppAndWaitForState(MockRM rm, String b, RMAppState state) throws Exception {
    MockRMAppSubmissionData ambiguousQueueAppData =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withWaitForAppAcceptedState(false)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue(b)
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, ambiguousQueueAppData);
    rm.waitForState(app1.getApplicationId(), state);
    return app1;
  }

  private int getNumAppsInQueue(String name, List<CSQueue> queues) {
    for (CSQueue queue : queues) {
      if (queue.getQueueShortName().equals(name)) {
        return queue.getNumApplications();
      }
    }
    return -1;
  }

  private CapacitySchedulerQueueInfo getQueueInfo(String name,
                                                  CapacitySchedulerQueueInfoList info) {
    if (info != null) {
      for (CapacitySchedulerQueueInfo queueInfo : info.getQueueInfoList()) {
        if (queueInfo.getQueueName().equals(name)) {
          return queueInfo;
        } else {
          CapacitySchedulerQueueInfo result =
              getQueueInfo(name, queueInfo.getQueues());
          if (result == null) {
            continue;
          }
          return result;
        }
      }
    }
    return null;
  }

  private void verifyQueueMetrics(QueueMetrics queue) {
    Assert.assertEquals(0, queue.getPendingMB());
    Assert.assertEquals(0, queue.getActiveUsers());
    Assert.assertEquals(0, queue.getActiveApps());
    Assert.assertEquals(0, queue.getAppsPending());
    Assert.assertEquals(0, queue.getAppsRunning());
    Assert.assertEquals(0, queue.getAllocatedMB());
    Assert.assertEquals(0, queue.getAllocatedVirtualCores());
  }

  private Configuration getCapacityConfiguration(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[]{"a", "b"});
    conf.setCapacity(A, 50);
    conf.setCapacity(B, 50);
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, 50);
    conf.setCapacity(A2, 50);
    conf.setQueues(B, new String[]{"b1"});
    conf.setCapacity(B1, 100);
    return conf;
  }

}
