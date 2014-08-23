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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.base.Supplier;


@SuppressWarnings({"rawtypes", "unchecked"})
@RunWith(value = Parameterized.class)
public class TestWorkPreservingRMRestart {

  private YarnConfiguration conf;
  private Class<?> schedulerClass;
  MockRM rm1 = null;
  MockRM rm2 = null;

  @Before
  public void setup() throws UnknownHostException {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    conf = new YarnConfiguration();
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.setClass(YarnConfiguration.RM_SCHEDULER, schedulerClass,
      ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @After
  public void tearDown() {
    if (rm1 != null) {
      rm1.stop();
    }
    if (rm2 != null) {
      rm2.stop();
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][] { { CapacityScheduler.class },
        { FifoScheduler.class }, {FairScheduler.class } });
  }

  public TestWorkPreservingRMRestart(Class<?> schedulerClass) {
    this.schedulerClass = schedulerClass;
  }

  // Test common scheduler state including SchedulerAttempt, SchedulerNode,
  // AppSchedulingInfo can be reconstructed via the container recovery reports
  // on NM re-registration.
  // Also test scheduler specific changes: i.e. Queue recovery-
  // CSQueue/FSQueue/FifoQueue recovery respectively.
  // Test Strategy: send 3 container recovery reports(AMContainer, running
  // container, completed container) on NM re-registration, check the states of
  // SchedulerAttempt, SchedulerNode etc. are updated accordingly.
  @Test(timeout = 20000)
  public void testSchedulerRecovery() throws Exception {
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
      DominantResourceCalculator.class.getName());

    int containerMemory = 1024;
    Resource containerResource = Resource.newInstance(containerMemory, 1);

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // clear queue metrics
    rm1.clearQueueMetrics(app1);

    // Re-start RM
    rm2 = new MockRM(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    // recover app
    RMApp recoveredApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    RMAppAttempt loadedAttempt1 = recoveredApp1.getCurrentAppAttempt();
    NMContainerStatus amContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 1,
          ContainerState.RUNNING);
    NMContainerStatus runningContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 2,
          ContainerState.RUNNING);
    NMContainerStatus completedContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 3,
          ContainerState.COMPLETE);

    nm1.registerNode(Arrays.asList(amContainer, runningContainer,
      completedContainer), null);

    // Wait for RM to settle down on recovering containers;
    waitForNumContainersToRecover(2, rm2, am1.getApplicationAttemptId());
    Set<ContainerId> launchedContainers =
        ((RMNodeImpl) rm2.getRMContext().getRMNodes().get(nm1.getNodeId()))
          .getLaunchedContainers();
    assertTrue(launchedContainers.contains(amContainer.getContainerId()));
    assertTrue(launchedContainers.contains(runningContainer.getContainerId()));

    // check RMContainers are re-recreated and the container state is correct.
    rm2.waitForState(nm1, amContainer.getContainerId(),
      RMContainerState.RUNNING);
    rm2.waitForState(nm1, runningContainer.getContainerId(),
      RMContainerState.RUNNING);
    rm2.waitForContainerToComplete(loadedAttempt1, completedContainer);

    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm2.getResourceScheduler();
    SchedulerNode schedulerNode1 = scheduler.getSchedulerNode(nm1.getNodeId());

    // ********* check scheduler node state.*******
    // 2 running containers.
    Resource usedResources = Resources.multiply(containerResource, 2);
    Resource nmResource =
        Resource.newInstance(nm1.getMemory(), nm1.getvCores());

    assertTrue(schedulerNode1.isValidContainer(amContainer.getContainerId()));
    assertTrue(schedulerNode1.isValidContainer(runningContainer
      .getContainerId()));
    assertFalse(schedulerNode1.isValidContainer(completedContainer
      .getContainerId()));
    // 2 launched containers, 1 completed container
    assertEquals(2, schedulerNode1.getNumContainers());

    assertEquals(Resources.subtract(nmResource, usedResources),
      schedulerNode1.getAvailableResource());
    assertEquals(usedResources, schedulerNode1.getUsedResource());
    Resource availableResources = Resources.subtract(nmResource, usedResources);

    // ***** check queue state based on the underlying scheduler ********
    Map<ApplicationId, SchedulerApplication> schedulerApps =
        ((AbstractYarnScheduler) rm2.getResourceScheduler())
          .getSchedulerApplications();
    SchedulerApplication schedulerApp =
        schedulerApps.get(recoveredApp1.getApplicationId());

    if (schedulerClass.equals(CapacityScheduler.class)) {
      checkCSQueue(rm2, schedulerApp, nmResource, nmResource, usedResources, 2);
    } else if (schedulerClass.equals(FifoScheduler.class)) {
      checkFifoQueue(schedulerApp, usedResources, availableResources);
    }

    // *********** check scheduler attempt state.********
    SchedulerApplicationAttempt schedulerAttempt =
        schedulerApp.getCurrentAppAttempt();
    assertTrue(schedulerAttempt.getLiveContainers().contains(
      scheduler.getRMContainer(amContainer.getContainerId())));
    assertTrue(schedulerAttempt.getLiveContainers().contains(
      scheduler.getRMContainer(runningContainer.getContainerId())));
    assertEquals(schedulerAttempt.getCurrentConsumption(), usedResources);

    // Until YARN-1959 is resolved
    if (scheduler.getClass() != FairScheduler.class) {
      assertEquals(availableResources, schedulerAttempt.getHeadroom());
    }

    // *********** check appSchedulingInfo state ***********
    assertEquals((1 << 22) + 1, schedulerAttempt.getNewContainerId());
  }

  private void checkCSQueue(MockRM rm,
      SchedulerApplication<SchedulerApplicationAttempt> app,
      Resource clusterResource, Resource queueResource, Resource usedResource,
      int numContainers)
      throws Exception {
    checkCSLeafQueue(rm2, app, clusterResource, queueResource, usedResource,
      numContainers);

    LeafQueue queue = (LeafQueue) app.getQueue();
    Resource availableResources = Resources.subtract(queueResource, usedResource);
    // ************* check Queue metrics ************
    QueueMetrics queueMetrics = queue.getMetrics();
    asserteMetrics(queueMetrics, 1, 0, 1, 0, 2, availableResources.getMemory(),
      availableResources.getVirtualCores(), usedResource.getMemory(),
      usedResource.getVirtualCores());

    // ************ check user metrics ***********
    QueueMetrics userMetrics =
        queueMetrics.getUserMetrics(app.getUser());
    asserteMetrics(userMetrics, 1, 0, 1, 0, 2, availableResources.getMemory(),
      availableResources.getVirtualCores(), usedResource.getMemory(),
      usedResource.getVirtualCores());
  }

  private void checkCSLeafQueue(MockRM rm,
      SchedulerApplication<SchedulerApplicationAttempt> app,
      Resource clusterResource, Resource queueResource, Resource usedResource,
      int numContainers) {
    LeafQueue leafQueue = (LeafQueue) app.getQueue();
    // assert queue used resources.
    assertEquals(usedResource, leafQueue.getUsedResources());
    assertEquals(numContainers, leafQueue.getNumContainers());

    ResourceCalculator calc =
        ((CapacityScheduler) rm.getResourceScheduler()).getResourceCalculator();
    float usedCapacity =
        Resources.divide(calc, clusterResource, usedResource, queueResource);
    // assert queue used capacity
    assertEquals(usedCapacity, leafQueue.getUsedCapacity(), 1e-8);
    float absoluteUsedCapacity =
        Resources.divide(calc, clusterResource, usedResource, clusterResource);
    // assert queue absolute capacity
    assertEquals(absoluteUsedCapacity, leafQueue.getAbsoluteUsedCapacity(),
      1e-8);
    // assert user consumed resources.
    assertEquals(usedResource, leafQueue.getUser(app.getUser())
      .getConsumedResources());
  }

  private void checkFifoQueue(SchedulerApplication schedulerApp,
      Resource usedResources, Resource availableResources) throws Exception {
    FifoScheduler scheduler = (FifoScheduler) rm2.getResourceScheduler();
    // ************ check cluster used Resources ********
    assertEquals(usedResources, scheduler.getUsedResource());

    // ************ check app headroom ****************
    SchedulerApplicationAttempt schedulerAttempt =
        schedulerApp.getCurrentAppAttempt();
    assertEquals(availableResources, schedulerAttempt.getHeadroom());

    // ************ check queue metrics ****************
    QueueMetrics queueMetrics = scheduler.getRootQueueMetrics();
    asserteMetrics(queueMetrics, 1, 0, 1, 0, 2, availableResources.getMemory(),
      availableResources.getVirtualCores(), usedResources.getMemory(),
      usedResources.getVirtualCores());
  }

  // create 3 container reports for AM
  public static List<NMContainerStatus>
      createNMContainerStatusForApp(MockAM am) {
    List<NMContainerStatus> list =
        new ArrayList<NMContainerStatus>();
    NMContainerStatus amContainer =
        TestRMRestart.createNMContainerStatus(am.getApplicationAttemptId(), 1,
          ContainerState.RUNNING);
    NMContainerStatus runningContainer =
        TestRMRestart.createNMContainerStatus(am.getApplicationAttemptId(), 2,
          ContainerState.RUNNING);
    NMContainerStatus completedContainer =
        TestRMRestart.createNMContainerStatus(am.getApplicationAttemptId(), 3,
          ContainerState.COMPLETE);
    list.add(amContainer);
    list.add(runningContainer);
    list.add(completedContainer);
    return list;
  }

  private static final String R = "Default";
  private static final String A = "QueueA";
  private static final String B = "QueueB";
  private static final String USER_1 = "user1";
  private static final String USER_2 = "user2";

  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { R });
    final String Q_R = CapacitySchedulerConfiguration.ROOT + "." + R;
    conf.setCapacity(Q_R, 100);
    final String Q_A = Q_R + "." + A;
    final String Q_B = Q_R + "." + B;
    conf.setQueues(Q_R, new String[] {A, B});
    conf.setCapacity(Q_A, 50);
    conf.setCapacity(Q_B, 50);
    conf.setDouble(CapacitySchedulerConfiguration
      .MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, 0.5f);
  }

  // Test CS recovery with multi-level queues and multi-users:
  // 1. setup 2 NMs each with 8GB memory;
  // 2. setup 2 level queues: Default -> (QueueA, QueueB)
  // 3. User1 submits 2 apps on QueueA
  // 4. User2 submits 1 app  on QueueB
  // 5. AM and each container has 1GB memory
  // 6. Restart RM.
  // 7. nm1 re-syncs back containers belong to user1
  // 8. nm2 re-syncs back containers belong to user2.
  // 9. Assert the parent queue and 2 leaf queues state and the metrics.
  // 10. Assert each user's consumption inside the queue.
  @Test (timeout = 30000)
  public void testCapacitySchedulerRecovery() throws Exception {
    if (!schedulerClass.equals(CapacityScheduler.class)) {
      return;
    }
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
      DominantResourceCalculator.class.getName());
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfiguration(csConf);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(csConf);
    rm1 = new MockRM(csConf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    MockNM nm2 =
        new MockNM("127.1.1.1:4321", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm2.registerNode();
    RMApp app1_1 = rm1.submitApp(1024, "app1_1", USER_1, null, A);
    MockAM am1_1 = MockRM.launchAndRegisterAM(app1_1, rm1, nm1);
    RMApp app1_2 = rm1.submitApp(1024, "app1_2", USER_1, null, A);
    MockAM am1_2 = MockRM.launchAndRegisterAM(app1_2, rm1, nm2);

    RMApp app2 = rm1.submitApp(1024, "app2", USER_2, null, B);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    // clear queue metrics
    rm1.clearQueueMetrics(app1_1);
    rm1.clearQueueMetrics(app1_2);
    rm1.clearQueueMetrics(app2);

    // Re-start RM
    rm2 = new MockRM(csConf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm2.setResourceTrackerService(rm2.getResourceTrackerService());

    List<NMContainerStatus> am1_1Containers =
        createNMContainerStatusForApp(am1_1);
    List<NMContainerStatus> am1_2Containers =
        createNMContainerStatusForApp(am1_2);
    am1_1Containers.addAll(am1_2Containers);
    nm1.registerNode(am1_1Containers, null);

    List<NMContainerStatus> am2Containers =
        createNMContainerStatusForApp(am2);
    nm2.registerNode(am2Containers, null);

    // Wait for RM to settle down on recovering containers;
    waitForNumContainersToRecover(2, rm2, am1_1.getApplicationAttemptId());
    waitForNumContainersToRecover(2, rm2, am1_2.getApplicationAttemptId());
    waitForNumContainersToRecover(2, rm2, am1_2.getApplicationAttemptId());

    // Calculate each queue's resource usage.
    Resource containerResource = Resource.newInstance(1024, 1);
    Resource nmResource =
        Resource.newInstance(nm1.getMemory(), nm1.getvCores());
    Resource clusterResource = Resources.multiply(nmResource, 2);
    Resource q1Resource = Resources.multiply(clusterResource, 0.5);
    Resource q2Resource = Resources.multiply(clusterResource, 0.5);
    Resource q1UsedResource = Resources.multiply(containerResource, 4);
    Resource q2UsedResource = Resources.multiply(containerResource, 2);
    Resource totalUsedResource = Resources.add(q1UsedResource, q2UsedResource);
    Resource q1availableResources =
        Resources.subtract(q1Resource, q1UsedResource);
    Resource q2availableResources =
        Resources.subtract(q2Resource, q2UsedResource);
    Resource totalAvailableResource =
        Resources.add(q1availableResources, q2availableResources);

    Map<ApplicationId, SchedulerApplication> schedulerApps =
        ((AbstractYarnScheduler) rm2.getResourceScheduler())
          .getSchedulerApplications();
    SchedulerApplication schedulerApp1_1 =
        schedulerApps.get(app1_1.getApplicationId());

    // assert queue A state.
    checkCSLeafQueue(rm2, schedulerApp1_1, clusterResource, q1Resource,
      q1UsedResource, 4);
    QueueMetrics queue1Metrics = schedulerApp1_1.getQueue().getMetrics();
    asserteMetrics(queue1Metrics, 2, 0, 2, 0, 4,
      q1availableResources.getMemory(), q1availableResources.getVirtualCores(),
      q1UsedResource.getMemory(), q1UsedResource.getVirtualCores());

    // assert queue B state.
    SchedulerApplication schedulerApp2 =
        schedulerApps.get(app2.getApplicationId());
    checkCSLeafQueue(rm2, schedulerApp2, clusterResource, q2Resource,
      q2UsedResource, 2);
    QueueMetrics queue2Metrics = schedulerApp2.getQueue().getMetrics();
    asserteMetrics(queue2Metrics, 1, 0, 1, 0, 2,
      q2availableResources.getMemory(), q2availableResources.getVirtualCores(),
      q2UsedResource.getMemory(), q2UsedResource.getVirtualCores());

    // assert parent queue state.
    LeafQueue leafQueue = (LeafQueue) schedulerApp2.getQueue();
    ParentQueue parentQueue = (ParentQueue) leafQueue.getParent();
    checkParentQueue(parentQueue, 6, totalUsedResource, (float) 6 / 16,
      (float) 6 / 16);
    asserteMetrics(parentQueue.getMetrics(), 3, 0, 3, 0, 6,
      totalAvailableResource.getMemory(),
      totalAvailableResource.getVirtualCores(), totalUsedResource.getMemory(),
      totalUsedResource.getVirtualCores());
  }

  private void checkParentQueue(ParentQueue parentQueue, int numContainers,
      Resource usedResource, float UsedCapacity, float absoluteUsedCapacity) {
    assertEquals(numContainers, parentQueue.getNumContainers());
    assertEquals(usedResource, parentQueue.getUsedResources());
    assertEquals(UsedCapacity, parentQueue.getUsedCapacity(), 1e-8);
    assertEquals(absoluteUsedCapacity, parentQueue.getAbsoluteUsedCapacity(), 1e-8);
  }

  // Test RM shuts down, in the meanwhile, AM fails. Restarted RM scheduler
  // should not recover the containers that belong to the failed AM.
  @Test(timeout = 20000)
  public void testAMfailedBetweenRMRestart() throws Exception {
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    rm2 = new MockRM(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());

    NMContainerStatus amContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 1,
          ContainerState.COMPLETE);
    NMContainerStatus runningContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 2,
          ContainerState.RUNNING);
    NMContainerStatus completedContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 3,
          ContainerState.COMPLETE);
    nm1.registerNode(Arrays.asList(amContainer, runningContainer,
      completedContainer), null);
    rm2.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    // Wait for RM to settle down on recovering containers;
    Thread.sleep(3000);

    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm2.getResourceScheduler();
    // Previous AM failed, The failed AM should once again release the
    // just-recovered containers.
    assertNull(scheduler.getRMContainer(runningContainer.getContainerId()));
    assertNull(scheduler.getRMContainer(completedContainer.getContainerId()));

    rm2.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 2, nm1);

    MockNM nm2 =
        new MockNM("127.1.1.1:4321", 8192, rm2.getResourceTrackerService());
    NMContainerStatus previousAttemptContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 4,
          ContainerState.RUNNING);
    nm2.registerNode(Arrays.asList(previousAttemptContainer), null);
    // Wait for RM to settle down on recovering containers;
    Thread.sleep(3000);
    // check containers from previous failed attempt should not be recovered.
    assertNull(scheduler.getRMContainer(previousAttemptContainer.getContainerId()));
  }

  // Apps already completed before RM restart. Restarted RM scheduler should not
  // recover containers for completed apps.
  @Test(timeout = 20000)
  public void testContainersNotRecoveredForCompletedApps() throws Exception {
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    MockRM.finishAMAndVerifyAppState(app1, rm1, nm1, am1);

    rm2 = new MockRM(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    NMContainerStatus runningContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 2,
          ContainerState.RUNNING);
    NMContainerStatus completedContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 3,
          ContainerState.COMPLETE);
    nm1.registerNode(Arrays.asList(runningContainer, completedContainer), null);
    RMApp recoveredApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    assertEquals(RMAppState.FINISHED, recoveredApp1.getState());

    // Wait for RM to settle down on recovering containers;
    Thread.sleep(3000);

    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm2.getResourceScheduler();

    // scheduler should not recover containers for finished apps.
    assertNull(scheduler.getRMContainer(runningContainer.getContainerId()));
    assertNull(scheduler.getRMContainer(completedContainer.getContainerId()));
  }

  @Test (timeout = 600000)
  public void testAppReregisterOnRMWorkPreservingRestart() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    // start RM
    rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = MockRM.launchAM(app0, rm1, nm1);

    // start new RM
    rm2 = new MockRM(conf, memStore);
    rm2.start();
    rm2.waitForState(app0.getApplicationId(), RMAppState.ACCEPTED);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.LAUNCHED);

    am0.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am0.registerAppAttempt(true);

    rm2.waitForState(app0.getApplicationId(), RMAppState.RUNNING);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.RUNNING);
  }
  
  @Test (timeout = 30000)
  public void testAMContainerStatusWithRMRestart() throws Exception {  
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1_1 = rm1.submitApp(1024);
    MockAM am1_1 = MockRM.launchAndRegisterAM(app1_1, rm1, nm1);
    
    RMAppAttempt attempt0 = app1_1.getCurrentAppAttempt();
    AbstractYarnScheduler scheduler =
        ((AbstractYarnScheduler) rm1.getResourceScheduler());
    
    Assert.assertTrue(scheduler.getRMContainer(
        attempt0.getMasterContainer().getId()).isAMContainer());

    // Re-start RM
    rm2 = new MockRM(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());

    List<NMContainerStatus> am1_1Containers =
        createNMContainerStatusForApp(am1_1);
    nm1.registerNode(am1_1Containers, null);

    // Wait for RM to settle down on recovering containers;
    waitForNumContainersToRecover(2, rm2, am1_1.getApplicationAttemptId());

    scheduler = ((AbstractYarnScheduler) rm2.getResourceScheduler());
    Assert.assertTrue(scheduler.getRMContainer(
        attempt0.getMasterContainer().getId()).isAMContainer());
  }

  @Test (timeout = 20000)
  public void testRecoverSchedulerAppAndAttemptSynchronously() throws Exception {
    // start RM
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    rm2 = new MockRM(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    // scheduler app/attempt is immediately available after RM is re-started.
    Assert.assertNotNull(rm2.getResourceScheduler().getSchedulerAppInfo(
      am0.getApplicationAttemptId()));

    // getTransferredContainers should not throw NPE.
    ((AbstractYarnScheduler) rm2.getResourceScheduler())
      .getTransferredContainers(am0.getApplicationAttemptId());

    List<NMContainerStatus> containers = createNMContainerStatusForApp(am0);
    nm1.registerNode(containers, null);
    waitForNumContainersToRecover(2, rm2, am0.getApplicationAttemptId());
  }

  // Test if RM on recovery receives the container release request from AM
  // before it receives the container status reported by NM for recovery. this
  // container should not be recovered.
  @Test (timeout = 30000)
  public void testReleasedContainerNotRecovered() throws Exception {
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    rm1.start();

    RMApp app1 = rm1.submitApp(1024);
    final MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Re-start RM
    conf.setInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 8000);
    rm2 = new MockRM(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    rm2.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    am1.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am1.registerAppAttempt(true);

    // try to release a container before the container is actually recovered.
    final ContainerId runningContainer =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 2);
    am1.allocate(null, Arrays.asList(runningContainer));

    // send container statuses to recover the containers
    List<NMContainerStatus> containerStatuses =
        createNMContainerStatusForApp(am1);
    nm1.registerNode(containerStatuses, null);

    // only the am container should be recovered.
    waitForNumContainersToRecover(1, rm2, am1.getApplicationAttemptId());

    final AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm2.getResourceScheduler();
    // cached release request is cleaned.
    // assertFalse(scheduler.getPendingRelease().contains(runningContainer));

    AllocateResponse response = am1.allocate(null, null);
    // AM gets notified of the completed container.
    boolean receivedCompletedContainer = false;
    for (ContainerStatus status : response.getCompletedContainersStatuses()) {
      if (status.getContainerId().equals(runningContainer)) {
        receivedCompletedContainer = true;
      }
    }
    assertTrue(receivedCompletedContainer);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        // release cache is cleaned up and previous running container is not
        // recovered
        return scheduler.getApplicationAttempt(am1.getApplicationAttemptId())
          .getPendingRelease().isEmpty()
            && scheduler.getRMContainer(runningContainer) == null;
      }
    }, 1000, 20000);
  }

  private void asserteMetrics(QueueMetrics qm, int appsSubmitted,
      int appsPending, int appsRunning, int appsCompleted,
      int allocatedContainers, int availableMB, int availableVirtualCores,
      int allocatedMB, int allocatedVirtualCores) {
    assertEquals(appsSubmitted, qm.getAppsSubmitted());
    assertEquals(appsPending, qm.getAppsPending());
    assertEquals(appsRunning, qm.getAppsRunning());
    assertEquals(appsCompleted, qm.getAppsCompleted());
    assertEquals(allocatedContainers, qm.getAllocatedContainers());
    assertEquals(availableMB, qm.getAvailableMB());
    assertEquals(availableVirtualCores, qm.getAvailableVirtualCores());
    assertEquals(allocatedMB, qm.getAllocatedMB());
    assertEquals(allocatedVirtualCores, qm.getAllocatedVirtualCores());
  }

  public static void waitForNumContainersToRecover(int num, MockRM rm,
      ApplicationAttemptId attemptId) throws Exception {
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();
    SchedulerApplicationAttempt attempt =
        scheduler.getApplicationAttempt(attemptId);
    while (attempt == null) {
      System.out.println("Wait for scheduler attempt " + attemptId
          + " to be created");
      Thread.sleep(200);
      attempt = scheduler.getApplicationAttempt(attemptId);
    }
    while (attempt.getLiveContainers().size() < num) {
      System.out.println("Wait for " + num
          + " containers to recover. currently: "
          + attempt.getLiveContainers().size());
      Thread.sleep(200);
    }
  }
}
