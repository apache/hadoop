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

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.TestRMRestart.TestSecurityMockRM;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueInvalidException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.Records;
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

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"rawtypes", "unchecked"})
public class TestWorkPreservingRMRestart extends ParameterizedSchedulerTestBase {

  private YarnConfiguration conf;
  MockRM rm1 = null;
  MockRM rm2 = null;

  @Before
  public void setup() throws UnknownHostException {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    conf = getConf();
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.setLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS, 0);
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
    conf = null;
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
    assertTrue(
        "SchedulerNode#toString is not in expected format",
        schedulerNode1
        .toString().contains(schedulerNode1.getUnallocatedResource().toString()));
    assertTrue(
        "SchedulerNode#toString is not in expected format",
        schedulerNode1
        .toString().contains(schedulerNode1.getAllocatedResource().toString()));

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
      schedulerNode1.getUnallocatedResource());
    assertEquals(usedResources, schedulerNode1.getAllocatedResource());
    Resource availableResources = Resources.subtract(nmResource, usedResources);

    // ***** check queue state based on the underlying scheduler ********
    Map<ApplicationId, SchedulerApplication> schedulerApps =
        ((AbstractYarnScheduler) rm2.getResourceScheduler())
          .getSchedulerApplications();
    SchedulerApplication schedulerApp =
        schedulerApps.get(recoveredApp1.getApplicationId());

    if (getSchedulerType() == SchedulerType.CAPACITY) {
      checkCSQueue(rm2, schedulerApp, nmResource, nmResource, usedResources, 2);
    } else {
      checkFSQueue(rm2, schedulerApp, usedResources, availableResources);
    }

    // *********** check scheduler attempt state.********
    SchedulerApplicationAttempt schedulerAttempt =
        schedulerApp.getCurrentAppAttempt();
    assertTrue(schedulerAttempt.getLiveContainers().contains(
      scheduler.getRMContainer(amContainer.getContainerId())));
    assertTrue(schedulerAttempt.getLiveContainers().contains(
      scheduler.getRMContainer(runningContainer.getContainerId())));
    assertEquals(schedulerAttempt.getCurrentConsumption(), usedResources);

    // *********** check appSchedulingInfo state ***********
    assertEquals((1L << 40) + 1L, schedulerAttempt.getNewContainerId());
  }

  private Configuration getSchedulerDynamicConfiguration() throws IOException {
    conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    conf.setTimeDuration(
        YarnConfiguration.RM_RESERVATION_SYSTEM_PLAN_FOLLOWER_TIME_STEP, 1L,
        TimeUnit.SECONDS);
    if (getSchedulerType() == SchedulerType.CAPACITY) {
      CapacitySchedulerConfiguration schedulerConf =
          new CapacitySchedulerConfiguration(conf);
      ReservationSystemTestUtil.setupDynamicQueueConfiguration(schedulerConf);
      return schedulerConf;
    } else {
      String allocFile = new File(FairSchedulerTestBase.TEST_DIR,
          TestWorkPreservingRMRestart.class.getSimpleName() + ".xml")
              .getAbsolutePath();
      ReservationSystemTestUtil.setupFSAllocationFile(allocFile);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
          ResourceScheduler.class);
      conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, allocFile);
      return conf;
    }
  }

  // Test work preserving recovery of apps running under reservation.
  // This involves:
  // 1. Setting up a dynamic reservable queue,
  // 2. Submitting an app to it,
  // 3. Failing over RM,
  // 4. Validating that the app is recovered post failover,
  // 5. Check if all running containers are recovered,
  // 6. Verify the scheduler state like attempt info,
  // 7. Verify the queue/user metrics for the dynamic reservable queue.
  @Test(timeout = 30000)
  public void testDynamicQueueRecovery() throws Exception {
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());

    // 1. Set up dynamic reservable queue.
    Configuration schedulerConf = getSchedulerDynamicConfiguration();
    int containerMemory = 1024;
    Resource containerResource = Resource.newInstance(containerMemory, 1);

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(schedulerConf);
    rm1 = new MockRM(schedulerConf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    // 2. Run plan follower to update the added node & then submit app to
    // dynamic queue.
    rm1.getRMContext().getReservationSystem()
        .synchronizePlan(ReservationSystemTestUtil.reservationQ, true);
    RMApp app1 = rm1.submitApp(200, "dynamicQApp",
        UserGroupInformation.getCurrentUser().getShortUserName(), null,
        ReservationSystemTestUtil.getReservationQueueName());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // clear queue metrics
    rm1.clearQueueMetrics(app1);

    // 3. Fail over (restart) RM.
    rm2 = new MockRM(schedulerConf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    // 4. Validate app is recovered post failover.
    RMApp recoveredApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    RMAppAttempt loadedAttempt1 = recoveredApp1.getCurrentAppAttempt();
    NMContainerStatus amContainer = TestRMRestart.createNMContainerStatus(
        am1.getApplicationAttemptId(), 1, ContainerState.RUNNING);
    NMContainerStatus runningContainer = TestRMRestart.createNMContainerStatus(
        am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    NMContainerStatus completedContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 3,
            ContainerState.COMPLETE);

    nm1.registerNode(
        Arrays.asList(amContainer, runningContainer, completedContainer), null);

    // Wait for RM to settle down on recovering containers.
    waitForNumContainersToRecover(2, rm2, am1.getApplicationAttemptId());
    Set<ContainerId> launchedContainers =
        ((RMNodeImpl) rm2.getRMContext().getRMNodes().get(nm1.getNodeId()))
            .getLaunchedContainers();
    assertTrue(launchedContainers.contains(amContainer.getContainerId()));
    assertTrue(launchedContainers.contains(runningContainer.getContainerId()));

    // 5. Check RMContainers are re-recreated and the container state is
    // correct.
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
    assertTrue(
        schedulerNode1.isValidContainer(runningContainer.getContainerId()));
    assertFalse(
        schedulerNode1.isValidContainer(completedContainer.getContainerId()));
    // 2 launched containers, 1 completed container
    assertEquals(2, schedulerNode1.getNumContainers());

    assertEquals(Resources.subtract(nmResource, usedResources),
        schedulerNode1.getUnallocatedResource());
    assertEquals(usedResources, schedulerNode1.getAllocatedResource());
    Resource availableResources = Resources.subtract(nmResource, usedResources);

    // 6. Verify the scheduler state like attempt info.
    Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> sa =
        ((AbstractYarnScheduler) rm2.getResourceScheduler())
            .getSchedulerApplications();
    SchedulerApplication<SchedulerApplicationAttempt> schedulerApp =
        sa.get(recoveredApp1.getApplicationId());

    // 7. Verify the queue/user metrics for the dynamic reservable queue.
    if (getSchedulerType() == SchedulerType.CAPACITY) {
      checkCSQueue(rm2, schedulerApp, nmResource, nmResource, usedResources, 2);
    } else {
      checkFSQueue(rm2, schedulerApp, usedResources, availableResources);
    }

    // *********** check scheduler attempt state.********
    SchedulerApplicationAttempt schedulerAttempt =
        schedulerApp.getCurrentAppAttempt();
    assertTrue(schedulerAttempt.getLiveContainers()
        .contains(scheduler.getRMContainer(amContainer.getContainerId())));
    assertTrue(schedulerAttempt.getLiveContainers()
        .contains(scheduler.getRMContainer(runningContainer.getContainerId())));
    assertEquals(schedulerAttempt.getCurrentConsumption(), usedResources);

    // *********** check appSchedulingInfo state ***********
    assertEquals((1L << 40) + 1L, schedulerAttempt.getNewContainerId());
  }

  private void checkCSQueue(MockRM rm,
      SchedulerApplication<SchedulerApplicationAttempt> app,
      Resource clusterResource, Resource queueResource, Resource usedResource,
      int numContainers)
      throws Exception {
    checkCSLeafQueue(rm, app, clusterResource, queueResource, usedResource,
        numContainers);

    LeafQueue queue = (LeafQueue) app.getQueue();
    Resource availableResources =
        Resources.subtract(queueResource, usedResource);
    // ************ check app headroom ****************
    SchedulerApplicationAttempt schedulerAttempt = app.getCurrentAppAttempt();
    assertEquals(availableResources, schedulerAttempt.getHeadroom());

    // ************* check Queue metrics ************
    QueueMetrics queueMetrics = queue.getMetrics();
    assertMetrics(queueMetrics, 1, 0, 1, 0, 2, availableResources.getMemorySize(),
        availableResources.getVirtualCores(), usedResource.getMemorySize(),
        usedResource.getVirtualCores());

    // ************ check user metrics ***********
    QueueMetrics userMetrics =
        queueMetrics.getUserMetrics(app.getUser());
    assertMetrics(userMetrics, 1, 0, 1, 0, 2, availableResources.getMemorySize(),
        availableResources.getVirtualCores(), usedResource.getMemorySize(),
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
      .getUsed());
  }

  private void checkFSQueue(ResourceManager rm,
      SchedulerApplication  schedulerApp, Resource usedResources,
      Resource availableResources) throws Exception {
    // waiting for RM's scheduling apps
    int retry = 0;
    Resource assumedFairShare = Resource.newInstance(8192, 8);
    while (true) {
      Thread.sleep(100);
      if (assumedFairShare.equals(((FairScheduler)rm.getResourceScheduler())
          .getQueueManager().getRootQueue().getFairShare())) {
        break;
      }
      retry++;
      if (retry > 30) {
        Assert.fail("Apps are not scheduled within assumed timeout");
      }
    }

    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    FSParentQueue root = scheduler.getQueueManager().getRootQueue();
    // ************ check cluster used Resources ********
    assertTrue(root.getPolicy() instanceof DominantResourceFairnessPolicy);
    assertEquals(usedResources,root.getResourceUsage());

    // ************ check app headroom ****************
    FSAppAttempt schedulerAttempt =
        (FSAppAttempt) schedulerApp.getCurrentAppAttempt();
    assertEquals(availableResources, schedulerAttempt.getHeadroom());

    // ************ check queue metrics ****************
    QueueMetrics queueMetrics = scheduler.getRootQueueMetrics();
    assertMetrics(queueMetrics, 1, 0, 1, 0, 2, availableResources.getMemorySize(),
        availableResources.getVirtualCores(), usedResources.getMemorySize(),
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
  private static final String B1 = "QueueB1";
  private static final String B2 = "QueueB2";
  //don't ever create the below queue ;-)
  private static final String QUEUE_DOESNT_EXIST = "NoSuchQueue";
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
  
  private void setupQueueConfigurationOnlyA(
      CapacitySchedulerConfiguration conf) {
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { R });
    final String Q_R = CapacitySchedulerConfiguration.ROOT + "." + R;
    conf.setCapacity(Q_R, 100);
    final String Q_A = Q_R + "." + A;
    conf.setQueues(Q_R, new String[] {A});
    conf.setCapacity(Q_A, 100);
    conf.setDouble(CapacitySchedulerConfiguration
      .MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, 1.0f);
  }

  private void setupQueueConfigurationChildOfB(CapacitySchedulerConfiguration conf) {
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { R });
    final String Q_R = CapacitySchedulerConfiguration.ROOT + "." + R;
    conf.setCapacity(Q_R, 100);
    final String Q_A = Q_R + "." + A;
    final String Q_B = Q_R + "." + B;
    final String Q_B1 = Q_B + "." + B1;
    final String Q_B2 = Q_B + "." + B2;
    conf.setQueues(Q_R, new String[] {A, B});
    conf.setCapacity(Q_A, 50);
    conf.setCapacity(Q_B, 50);
    conf.setQueues(Q_B, new String[] {B1, B2});
    conf.setCapacity(Q_B1, 50);
    conf.setCapacity(Q_B2, 50);
    conf.setDouble(CapacitySchedulerConfiguration
        .MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, 0.5f);
  }

  // 1. submit an app to default queue and let it finish
  // 2. restart rm with no default queue
  // 3. getApplicationReport call should succeed (with no NPE)
  @Test (timeout = 30000)
  public void testRMRestartWithRemovedQueue() throws Exception{
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "");
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    final RMApp app1 = rm1.submitApp(1024, "app1", USER_1, null);
    MockAM am1 = MockRM.launchAndRegisterAM(app1,rm1, nm1);
    MockRM.finishAMAndVerifyAppState(app1, rm1, nm1, am1);

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[]{QUEUE_DOESNT_EXIST});
    final String noQueue = CapacitySchedulerConfiguration.ROOT + "." + QUEUE_DOESNT_EXIST;
    csConf.setCapacity(noQueue, 100);
    rm2 = new MockRM(csConf,memStore);

    rm2.start();
    UserGroupInformation user2 = UserGroupInformation.createRemoteUser("user2");

    ApplicationReport report =
        user2.doAs(new PrivilegedExceptionAction<ApplicationReport>() {
          @Override
          public ApplicationReport run() throws Exception {
            return rm2.getApplicationReport(app1.getApplicationId());
          }
    });
    Assert.assertNotNull(report);
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
    if (getSchedulerType() != SchedulerType.CAPACITY) {
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

    csConf.set("yarn.scheduler.capacity.root.Default.QueueB.state", "STOPPED");

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
    assertMetrics(queue1Metrics, 2, 0, 2, 0, 4,
        q1availableResources.getMemorySize(),
        q1availableResources.getVirtualCores(), q1UsedResource.getMemorySize(),
        q1UsedResource.getVirtualCores());

    // assert queue B state.
    SchedulerApplication schedulerApp2 =
        schedulerApps.get(app2.getApplicationId());
    checkCSLeafQueue(rm2, schedulerApp2, clusterResource, q2Resource,
      q2UsedResource, 2);
    QueueMetrics queue2Metrics = schedulerApp2.getQueue().getMetrics();
    assertMetrics(queue2Metrics, 1, 0, 1, 0, 2,
        q2availableResources.getMemorySize(),
        q2availableResources.getVirtualCores(), q2UsedResource.getMemorySize(),
        q2UsedResource.getVirtualCores());

    // assert parent queue state.
    LeafQueue leafQueue = (LeafQueue) schedulerApp2.getQueue();
    ParentQueue parentQueue = (ParentQueue) leafQueue.getParent();
    checkParentQueue(parentQueue, 6, totalUsedResource, (float) 6 / 16,
      (float) 6 / 16);
    assertMetrics(parentQueue.getMetrics(), 3, 0, 3, 0, 6,
        totalAvailableResource.getMemorySize(),
        totalAvailableResource.getVirtualCores(), totalUsedResource.getMemorySize(),
        totalUsedResource.getVirtualCores());
  }

  private void verifyAppRecoveryWithWrongQueueConfig(
      CapacitySchedulerConfiguration csConf, RMApp app, String diagnostics,
      MemoryRMStateStore memStore, RMState state) throws Exception {
    // Restart RM with fail-fast as false. App should be killed.
    csConf.setBoolean(YarnConfiguration.RM_FAIL_FAST, false);
    rm2 = new MockRM(csConf, memStore);
    rm2.start();
    // Wait for app to be killed.
    rm2.waitForState(app.getApplicationId(), RMAppState.KILLED);
    ApplicationReport report = rm2.getApplicationReport(app.getApplicationId());
    assertEquals(report.getFinalApplicationStatus(),
        FinalApplicationStatus.KILLED);
    assertEquals(report.getYarnApplicationState(), YarnApplicationState.KILLED);
    assertEquals(report.getDiagnostics(), diagnostics);

    // Remove updated app info(app being KILLED) from state store and reinstate
    // state store to previous state i.e. which indicates app is RUNNING.
    // This is to simulate app recovery with fail fast config as true.
    for(Map.Entry<ApplicationId, ApplicationStateData> entry :
        state.getApplicationState().entrySet()) {
      ApplicationStateData appState = mock(ApplicationStateData.class);
      ApplicationSubmissionContext ctxt =
          mock(ApplicationSubmissionContext.class);
      when(appState.getApplicationSubmissionContext()).thenReturn(ctxt);
      when(ctxt.getApplicationId()).thenReturn(entry.getKey());
      memStore.removeApplicationStateInternal(appState);
      memStore.storeApplicationStateInternal(
          entry.getKey(), entry.getValue());
    }

    // Now restart RM with fail-fast as true. QueueException should be thrown.
    csConf.setBoolean(YarnConfiguration.RM_FAIL_FAST, true);
    MockRM rm = new MockRM(csConf, memStore);
    try {
      rm.start();
      Assert.fail("QueueException must have been thrown");
    } catch (QueueInvalidException e) {
    } finally {
      rm.close();
    }
  }

  //Test behavior of an app if queue is changed from leaf to parent during
  //recovery. Test case does following:
  //1. Add an app to QueueB and start the attempt.
  //2. Add 2 subqueues(QueueB1 and QueueB2) to QueueB, restart the RM, once with
  //   fail fast config as false and once with fail fast as true.
  //3. Verify that app was killed if fail fast is false.
  //4. Verify that QueueException was thrown if fail fast is true.
  @Test (timeout = 30000)
  public void testCapacityLeafQueueBecomesParentOnRecovery() throws Exception {
    if (getSchedulerType() != SchedulerType.CAPACITY) {
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
    MockNM nm =
        new MockNM("127.1.1.1:4321", 8192, rm1.getResourceTrackerService());
    nm.registerNode();

    // Submit an app to QueueB.
    RMApp app = rm1.submitApp(1024, "app", USER_2, null, B);
    MockRM.launchAndRegisterAM(app, rm1, nm);
    assertEquals(rm1.getApplicationReport(app.getApplicationId()).
        getYarnApplicationState(), YarnApplicationState.RUNNING);

    // Take a copy of state store so that it can be reset to this state.
    RMState state = memStore.loadState();

    // Change scheduler config with child queues added to QueueB.
    csConf = new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationChildOfB(csConf);

    String diags = "Application killed on recovery as it was submitted to " +
        "queue QueueB which is no longer a leaf queue after restart.";
    verifyAppRecoveryWithWrongQueueConfig(csConf, app, diags, memStore, state);
  }

  //Test behavior of an app if queue is removed during recovery. Test case does
  //following:
  //1. Add some apps to two queues, attempt to add an app to a non-existant
  //   queue to verify that the new logic is not in effect during normal app
  //   submission
  //2. Remove one of the queues, restart the RM, once with fail fast config as
  //   false and once with fail fast as true.
  //3. Verify that app was killed if fail fast is false.
  //4. Verify that QueueException was thrown if fail fast is true.
  @Test (timeout = 30000)
  public void testCapacitySchedulerQueueRemovedRecovery() throws Exception {
    if (getSchedulerType() != SchedulerType.CAPACITY) {
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
    assertEquals(rm1.getApplicationReport(app2.getApplicationId()).
        getYarnApplicationState(), YarnApplicationState.RUNNING);

    //Submit an app with a non existant queue to make sure it does not
    //cause a fatal failure in the non-recovery case
    RMApp appNA = rm1.submitApp(1024, "app1_2", USER_1, null,
       QUEUE_DOESNT_EXIST, false);

    // clear queue metrics
    rm1.clearQueueMetrics(app1_1);
    rm1.clearQueueMetrics(app1_2);
    rm1.clearQueueMetrics(app2);

    // Take a copy of state store so that it can be reset to this state.
    RMState state = memStore.loadState();

    // Set new configuration with QueueB removed.
    csConf = new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationOnlyA(csConf);

    String diags = "Application killed on recovery as it was submitted to " +
        "queue QueueB which no longer exists after restart.";
    verifyAppRecoveryWithWrongQueueConfig(csConf, app2, diags, memStore, state);
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
    conf.setLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS, 0);
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

    YarnScheduler scheduler = rm2.getResourceScheduler();
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

    YarnScheduler scheduler = rm2.getResourceScheduler();

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
    // Issuing registerAppAttempt() before and after RM restart to confirm
    // registerApplicationMaster() is idempotent.
    am0.registerAppAttempt();

    // start new RM
    rm2 = new MockRM(conf, memStore);
    rm2.start();
    rm2.waitForState(app0.getApplicationId(), RMAppState.ACCEPTED);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.LAUNCHED);

    am0.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    // retry registerApplicationMaster() after RM restart.
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
    YarnScheduler scheduler = rm1.getResourceScheduler();

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

    scheduler = rm2.getResourceScheduler();
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
    rm2.getResourceScheduler()
      .getTransferredContainers(am0.getApplicationAttemptId());

    List<NMContainerStatus> containers = createNMContainerStatusForApp(am0);
    nm1.registerNode(containers, null);
    waitForNumContainersToRecover(2, rm2, am0.getApplicationAttemptId());
  }

  // Test if RM on recovery receives the container release request from AM
  // before it receives the container status reported by NM for recovery. this
  // container should not be recovered.
  @Test (timeout = 50000)
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
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
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

  private void assertMetrics(QueueMetrics qm, int appsSubmitted,
      int appsPending, int appsRunning, int appsCompleted,
      int allocatedContainers, long availableMB, long availableVirtualCores,
      long allocatedMB, long allocatedVirtualCores) {
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

  @Test (timeout = 20000)
  public void testNewContainersNotAllocatedDuringSchedulerRecovery()
      throws Exception {
    conf.setLong(
      YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS, 4000);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Restart RM
    rm2 = new MockRM(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm1.registerNode();
    ControlledClock clock = new ControlledClock();
    long startTime = System.currentTimeMillis();
    ((RMContextImpl)rm2.getRMContext()).setSystemClock(clock);
    am1.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am1.registerAppAttempt(true);
    rm2.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    // AM request for new containers
    am1.allocate("127.0.0.1", 1000, 1, new ArrayList<ContainerId>());

    List<Container> containers = new ArrayList<Container>();
    clock.setTime(startTime + 2000);
    nm1.nodeHeartbeat(true);

    // sleep some time as allocation happens asynchronously.
    Thread.sleep(3000);
    containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
      new ArrayList<ContainerId>()).getAllocatedContainers());
    // container is not allocated during scheduling recovery.
    Assert.assertTrue(containers.isEmpty());

    clock.setTime(startTime + 8000);
    nm1.nodeHeartbeat(true);
    // Container is created after recovery is done.
    while (containers.isEmpty()) {
      containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }
  }

  /**
   * Testing to confirm that retried finishApplicationMaster() doesn't throw
   * InvalidApplicationMasterRequest before and after RM restart.
   */
  @Test (timeout = 20000)
  public void testRetriedFinishApplicationMasterRequest()
      throws Exception {
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

    am0.registerAppAttempt();

    // Emulating following a scenario:
    // RM1 saves the app in RMStateStore and then crashes,
    // FinishApplicationMasterResponse#isRegistered still return false,
    // so AM still retry the 2nd RM
    MockRM.finishAMAndVerifyAppState(app0, rm1, nm1, am0);


    // start new RM
    rm2 = new MockRM(conf, memStore);
    rm2.start();

    am0.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am0.unregisterAppAttempt(false);
  }

  @Test (timeout = 30000)
  public void testAppFailedToRenewTokenOnRecovery() throws Exception {
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    UserGroupInformation.setConfiguration(conf);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    MockRM rm1 = new TestSecurityMockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    MockRM rm2 = new TestSecurityMockRM(conf, memStore) {
      protected DelegationTokenRenewer createDelegationTokenRenewer() {
        return new DelegationTokenRenewer() {
          @Override
          public void addApplicationSync(ApplicationId applicationId,
              Credentials ts, boolean shouldCancelAtEnd, String user)
              throws IOException {
            throw new IOException("Token renew failed !!");
          }
        };
      }
    };
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    rm2.start();
    NMContainerStatus containerStatus =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 1,
          ContainerState.RUNNING);
    nm1.registerNode(Arrays.asList(containerStatus), null);

    // am re-register
    rm2.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    am1.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am1.registerAppAttempt(true);
    rm2.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    // Because the token expired, am could crash.
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm2.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    rm2.waitForState(app1.getApplicationId(), RMAppState.FAILED);
  }

  /**
   * Test validateAndCreateResourceRequest fails on recovery, app should ignore
   * this Exception and continue
   */
  @Test (timeout = 30000)
  public void testAppFailToValidateResourceRequestOnRecovery() throws Exception{
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Change the config so that validateAndCreateResourceRequest throws
    // exception on recovery
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 50);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 100);

    rm2 = new MockRM(conf, memStore);
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    rm2.start();
  }

  @Test(timeout = 20000)
  public void testContainerCompleteMsgNotLostAfterAMFailedAndRMRestart() throws Exception {
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();

    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();

    // submit app with keepContainersAcrossApplicationAttempts true
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(200);
    RMApp app0 = rm1.submitApp(resource, "", UserGroupInformation
        .getCurrentUser().getShortUserName(), null, false, null,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS, null, null, true, true,
        false, null, 0, null, true, null);
    MockAM am0 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    am0.allocate("127.0.0.1", 1000, 2, new ArrayList<ContainerId>());
    nm1.nodeHeartbeat(true);
    List<Container> conts = am0.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() == 0) {
      nm1.nodeHeartbeat(true);
      conts.addAll(am0.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }

    // am failed,and relaunch it
    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(app0.getApplicationId(), RMAppState.ACCEPTED);
    MockAM am1 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    // rm failover
    rm2 = new MockRM(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());

    // container launched by first am completed
    NMContainerStatus amContainer =
        TestRMRestart.createNMContainerStatus(am0.getApplicationAttemptId(), 1,
          ContainerState.RUNNING);
    NMContainerStatus completedContainer=
        TestRMRestart.createNMContainerStatus(am0.getApplicationAttemptId(), 2,
          ContainerState.COMPLETE);
    NMContainerStatus runningContainer =
        TestRMRestart.createNMContainerStatus(am0.getApplicationAttemptId(), 3,
          ContainerState.RUNNING);
    nm1.registerNode(Arrays.asList(amContainer, runningContainer,
        completedContainer), null);
    Thread.sleep(200);

    // check whether current am could get containerCompleteMsg
    RMApp recoveredApp0 =
        rm2.getRMContext().getRMApps().get(app0.getApplicationId());
    RMAppAttempt loadedAttempt1 = recoveredApp0.getCurrentAppAttempt();
    assertEquals(1,loadedAttempt1.getJustFinishedContainers().size());
  }

  // Test that if application state was saved, but attempt state was not saved.
  // RM should start correctly.
  @Test (timeout = 20000)
  public void testAppStateSavedButAttemptStateNotSaved() throws Exception {
    MemoryRMStateStore memStore = new MemoryRMStateStore() {
      @Override public synchronized void updateApplicationAttemptStateInternal(
          ApplicationAttemptId appAttemptId,
          ApplicationAttemptStateData attemptState) {
        // do nothing;
        // simulate the failure that attempt final state is not saved.
      }
    };
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();

    MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    MockRM.finishAMAndVerifyAppState(app1, rm1, nm1, am1);

    ApplicationStateData appSavedState =
        memStore.getState().getApplicationState().get(app1.getApplicationId());

    // check that app state is  saved.
    assertEquals(RMAppState.FINISHED, appSavedState.getState());
    // check that attempt state is not saved.
    assertNull(appSavedState.getAttempt(am1.getApplicationAttemptId()).getState());

    rm2 = new MockRM(conf, memStore);
    rm2.start();
    RMApp recoveredApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());

    assertEquals(RMAppState.FINISHED, recoveredApp1.getState());
    // check that attempt state is recovered correctly.
    assertEquals(RMAppAttemptState.FINISHED, recoveredApp1.getCurrentAppAttempt().getState());
  }
}
