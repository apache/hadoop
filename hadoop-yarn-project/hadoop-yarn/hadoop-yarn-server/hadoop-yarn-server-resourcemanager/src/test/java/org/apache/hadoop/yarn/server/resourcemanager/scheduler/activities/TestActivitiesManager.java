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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

/**
 * Test class for {@link ActivitiesManager}.
 */
public class TestActivitiesManager {

  private final static int NUM_NODES = 5;

  private final static int NUM_APPS = 5;

  private final static int NUM_THREADS = 5;

  private RMContext rmContext;

  private TestingActivitiesManager activitiesManager;

  private List<SchedulerApplicationAttempt> apps;

  private List<SchedulerNode> nodes;

  private ThreadPoolExecutor threadPoolExecutor;

  @Before
  public void setup() {
    rmContext = Mockito.mock(RMContext.class);
    Configuration conf = new Configuration();
    Mockito.when(rmContext.getYarnConfiguration()).thenReturn(conf);
    ResourceScheduler scheduler = Mockito.mock(ResourceScheduler.class);
    Mockito.when(scheduler.getMinimumResourceCapability())
        .thenReturn(Resources.none());
    Mockito.when(rmContext.getScheduler()).thenReturn(scheduler);
    LeafQueue mockQueue = Mockito.mock(LeafQueue.class);
    Map<ApplicationId, RMApp> rmApps = new ConcurrentHashMap<>();
    Mockito.doReturn(rmApps).when(rmContext).getRMApps();
    apps = new ArrayList<>();
    for (int i = 0; i < NUM_APPS; i++) {
      ApplicationAttemptId appAttemptId =
          TestUtils.getMockApplicationAttemptId(i, 0);
      RMApp mockApp = Mockito.mock(RMApp.class);
      Mockito.doReturn(appAttemptId.getApplicationId()).when(mockApp)
          .getApplicationId();
      Mockito.doReturn(FinalApplicationStatus.UNDEFINED).when(mockApp)
          .getFinalApplicationStatus();
      rmApps.put(appAttemptId.getApplicationId(), mockApp);
      FiCaSchedulerApp app =
          new FiCaSchedulerApp(appAttemptId, "user", mockQueue,
              mock(ActiveUsersManager.class), rmContext);
      apps.add(app);
    }
    nodes = new ArrayList<>();
    for (int i = 0; i < NUM_NODES; i++) {
      nodes.add(TestUtils.getMockNode("host" + i, "rack", 1, 10240));
    }
    activitiesManager = new TestingActivitiesManager(rmContext);
    threadPoolExecutor =
        new ThreadPoolExecutor(NUM_THREADS, NUM_THREADS, 3L,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>());
  }

  /**
   * Test recording activities belong to different nodes in multiple threads,
   * these threads can run without interference and one activity
   * should be recorded by every thread.
   */
  @Test
  public void testRecordingDifferentNodeActivitiesInMultiThreads()
      throws Exception {
    Random rand = new Random();
    List<Future<Void>> futures = new ArrayList<>();
    for (SchedulerNode node : nodes) {
      Callable<Void> task = () -> {
        SchedulerApplicationAttempt randomApp =
            apps.get(rand.nextInt(NUM_APPS));
        // start recording activities for random node
        activitiesManager.recordNextNodeUpdateActivities(
            node.getNodeID().toString());
        // generate node/app activities
        ActivitiesLogger.NODE
            .startNodeUpdateRecording(activitiesManager, node.getNodeID());
        ActivitiesLogger.APP
            .recordAppActivityWithoutAllocation(activitiesManager, node,
                randomApp,
                new SchedulerRequestKey(Priority.newInstance(0), 0, null),
                ActivityDiagnosticConstant.NODE_IS_BLACKLISTED,
                ActivityState.REJECTED, ActivityLevel.NODE);
        ActivitiesLogger.NODE
            .finishNodeUpdateRecording(activitiesManager, node.getNodeID(), "");
        return null;
      };
      futures.add(threadPoolExecutor.submit(task));
    }
    for (Future<Void> future : futures) {
      future.get();
    }
    // Check activities for all nodes should be recorded and every node should
    // have only one allocation information.
    Assert.assertEquals(NUM_NODES,
        activitiesManager.historyNodeAllocations.size());
    for (List<List<NodeAllocation>> nodeAllocationsForThisNode :
        activitiesManager.historyNodeAllocations.values()) {
      Assert.assertEquals(1, nodeAllocationsForThisNode.size());
      Assert.assertEquals(1, nodeAllocationsForThisNode.get(0).size());
    }
  }

  /**
   * Test recording activities for multi-nodes assignment in multiple threads,
   * only one activity info should be recorded by one of these threads.
   */
  @Test
  public void testRecordingSchedulerActivitiesForMultiNodesInMultiThreads()
      throws Exception {
    Random rand = new Random();
    // start recording activities for multi-nodes
    activitiesManager.recordNextNodeUpdateActivities(
        ActivitiesManager.EMPTY_NODE_ID.toString());
    List<Future<Void>> futures = new ArrayList<>();
    // generate node/app activities
    for (SchedulerNode node : nodes) {
      Callable<Void> task = () -> {
        SchedulerApplicationAttempt randomApp =
            apps.get(rand.nextInt(NUM_APPS));
        ActivitiesLogger.NODE.startNodeUpdateRecording(activitiesManager,
            ActivitiesManager.EMPTY_NODE_ID);
        ActivitiesLogger.APP
            .recordAppActivityWithoutAllocation(activitiesManager, node,
                randomApp,
                new SchedulerRequestKey(Priority.newInstance(0), 0, null),
                ActivityDiagnosticConstant.NODE_IS_BLACKLISTED,
                ActivityState.REJECTED, ActivityLevel.NODE);
        ActivitiesLogger.NODE.finishNodeUpdateRecording(activitiesManager,
            ActivitiesManager.EMPTY_NODE_ID, "");
        return null;
      };
      futures.add(threadPoolExecutor.submit(task));
    }
    for (Future<Void> future : futures) {
      future.get();
    }
    // Check activities for multi-nodes should be recorded only once
    Assert.assertEquals(1, activitiesManager.historyNodeAllocations.size());
  }


  /**
   * Test recording app activities in multiple threads,
   * only one activity info should be recorded by one of these threads.
   */
  @Test
  public void testRecordingAppActivitiesInMultiThreads()
      throws Exception {
    Random rand = new Random();
    // start recording activities for a random app
    SchedulerApplicationAttempt randomApp = apps.get(rand.nextInt(NUM_APPS));
    activitiesManager
        .turnOnAppActivitiesRecording(randomApp.getApplicationId(), 3);
    List<Future<Void>> futures = new ArrayList<>();
    // generate app activities
    int nTasks = 20;
    for (int i=0; i<nTasks; i++) {
      Callable<Void> task = () -> {
        ActivitiesLogger.APP.startAppAllocationRecording(activitiesManager,
            (FiCaSchedulerNode) nodes.get(0),
            SystemClock.getInstance().getTime(), randomApp);
        for (SchedulerNode node : nodes) {
          ActivitiesLogger.APP
              .recordAppActivityWithoutAllocation(activitiesManager, node,
                  randomApp,
                  new SchedulerRequestKey(Priority.newInstance(0), 0, null),
                  ActivityDiagnosticConstant.NODE_IS_BLACKLISTED,
                  ActivityState.REJECTED, ActivityLevel.NODE);
        }
        ActivitiesLogger.APP
            .finishSkippedAppAllocationRecording(activitiesManager,
                randomApp.getApplicationId(), ActivityState.SKIPPED,
                ActivityDiagnosticConstant.EMPTY);
        return null;
      };
      futures.add(threadPoolExecutor.submit(task));
    }
    // Check activities for multi-nodes should be recorded only once
    for (Future<Void> future : futures) {
      future.get();
    }
    Queue<AppAllocation> appAllocations =
        activitiesManager.completedAppAllocations
            .get(randomApp.getApplicationId());
    Assert.assertEquals(nTasks, appAllocations.size());
    for(AppAllocation aa : appAllocations) {
      Assert.assertEquals(NUM_NODES, aa.getAllocationAttempts().size());
    }
  }

  @Test (timeout = 30000)
  public void testAppActivitiesTTL() throws Exception {
    long cleanupIntervalMs = 100;
    long appActivitiesTTL = 1000;
    rmContext.getYarnConfiguration()
        .setLong(YarnConfiguration.RM_ACTIVITIES_MANAGER_CLEANUP_INTERVAL_MS,
            cleanupIntervalMs);
    rmContext.getYarnConfiguration()
        .setLong(YarnConfiguration.RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_TTL_MS,
            appActivitiesTTL);
    ActivitiesManager newActivitiesManager = new ActivitiesManager(rmContext);
    newActivitiesManager.serviceStart();
    // start recording activities for first app and first node
    SchedulerApplicationAttempt app = apps.get(0);
    FiCaSchedulerNode node = (FiCaSchedulerNode) nodes.get(0);
    newActivitiesManager
        .turnOnAppActivitiesRecording(app.getApplicationId(), 3);
    int numActivities = 10;
    for (int i = 0; i < numActivities; i++) {
      ActivitiesLogger.APP
          .startAppAllocationRecording(newActivitiesManager, node,
              SystemClock.getInstance().getTime(), app);
      ActivitiesLogger.APP
          .recordAppActivityWithoutAllocation(newActivitiesManager, node, app,
              new SchedulerRequestKey(Priority.newInstance(0), 0, null),
              ActivityDiagnosticConstant.NODE_IS_BLACKLISTED,
              ActivityState.REJECTED, ActivityLevel.NODE);
      ActivitiesLogger.APP
          .finishSkippedAppAllocationRecording(newActivitiesManager,
              app.getApplicationId(), ActivityState.SKIPPED,
              ActivityDiagnosticConstant.EMPTY);
    }
    AppActivitiesInfo appActivitiesInfo = newActivitiesManager
        .getAppActivitiesInfo(app.getApplicationId(), null, null, null, -1,
            false, 3);
    Assert.assertEquals(numActivities,
        appActivitiesInfo.getAllocations().size());
    // sleep until all app activities expired
    Thread.sleep(cleanupIntervalMs + appActivitiesTTL);
    // there should be no remaining app activities
    appActivitiesInfo = newActivitiesManager
        .getAppActivitiesInfo(app.getApplicationId(), null, null, null, -1,
            false, 3);
    Assert.assertEquals(0,
        appActivitiesInfo.getAllocations().size());
  }

  @Test (timeout = 30000)
  public void testAppActivitiesPerformance() {
    // start recording activities for first app
    SchedulerApplicationAttempt app = apps.get(0);
    FiCaSchedulerNode node = (FiCaSchedulerNode) nodes.get(0);
    activitiesManager.turnOnAppActivitiesRecording(app.getApplicationId(), 100);
    int numActivities = 100;
    int numNodes = 10000;
    int testingTimes = 10;
    for (int ano = 0; ano < numActivities; ano++) {
      ActivitiesLogger.APP.startAppAllocationRecording(activitiesManager, node,
          SystemClock.getInstance().getTime(), app);
      for (int i = 0; i < numNodes; i++) {
        NodeId nodeId = NodeId.newInstance("host" + i, 0);
        activitiesManager
            .addSchedulingActivityForApp(app.getApplicationId(), null, 0,
                ActivityState.SKIPPED,
                ActivityDiagnosticConstant.NODE_IS_BLACKLISTED,
                ActivityLevel.NODE, nodeId, 0L);
      }
      ActivitiesLogger.APP
          .finishSkippedAppAllocationRecording(activitiesManager,
              app.getApplicationId(), ActivityState.SKIPPED,
              ActivityDiagnosticConstant.EMPTY);
    }

    // It often take a longer time for the first query, ignore this distraction
    activitiesManager
        .getAppActivitiesInfo(app.getApplicationId(), null, null, null, -1,
            true, 100);

    // Test getting normal app activities
    Supplier<Void> normalSupplier = () -> {
      AppActivitiesInfo appActivitiesInfo = activitiesManager
          .getAppActivitiesInfo(app.getApplicationId(), null, null, null, -1,
              false, 100);
      Assert.assertEquals(numActivities,
          appActivitiesInfo.getAllocations().size());
      Assert.assertEquals(1,
          appActivitiesInfo.getAllocations().get(0).getChildren()
              .size());
      Assert.assertEquals(numNodes,
          appActivitiesInfo.getAllocations().get(0).getChildren()
              .get(0).getChildren().size());
      return null;
    };
    testManyTimes("Getting normal app activities", normalSupplier,
        testingTimes);

    // Test getting aggregated app activities
    Supplier<Void> aggregatedSupplier = () -> {
      AppActivitiesInfo appActivitiesInfo = activitiesManager
          .getAppActivitiesInfo(app.getApplicationId(), null, null,
              RMWSConsts.ActivitiesGroupBy.DIAGNOSTIC, -1, false, 100);
      Assert.assertEquals(numActivities,
          appActivitiesInfo.getAllocations().size());
      Assert.assertEquals(1,
          appActivitiesInfo.getAllocations().get(0).getChildren()
              .size());
      Assert.assertEquals(1,
          appActivitiesInfo.getAllocations().get(0).getChildren()
              .get(0).getChildren().size());
      Assert.assertEquals(numNodes,
          appActivitiesInfo.getAllocations().get(0).getChildren()
              .get(0).getChildren().get(0).getNodeIds().size());
      return null;
    };
    testManyTimes("Getting aggregated app activities", aggregatedSupplier,
        testingTimes);

    // Test getting summarized app activities
    Supplier<Void> summarizedSupplier = () -> {
      AppActivitiesInfo appActivitiesInfo = activitiesManager
          .getAppActivitiesInfo(app.getApplicationId(), null, null,
              RMWSConsts.ActivitiesGroupBy.DIAGNOSTIC, -1, true, 100);
      Assert.assertEquals(1, appActivitiesInfo.getAllocations().size());
      Assert.assertEquals(1,
          appActivitiesInfo.getAllocations().get(0).getChildren()
              .size());
      Assert.assertEquals(1,
          appActivitiesInfo.getAllocations().get(0).getChildren()
              .get(0).getChildren().size());
      Assert.assertEquals(numNodes,
          appActivitiesInfo.getAllocations().get(0).getChildren()
              .get(0).getChildren().get(0).getNodeIds().size());
      return null;
    };
    testManyTimes("Getting summarized app activities", summarizedSupplier,
        testingTimes);
  }

  @Test (timeout = 10000)
  public void testAppActivitiesMaxQueueLengthUpdate()
      throws TimeoutException, InterruptedException {
    Configuration conf = new Configuration();
    int configuredAppActivitiesMaxQueueLength = 1;
    conf.setInt(YarnConfiguration.
            RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_MAX_QUEUE_LENGTH,
        configuredAppActivitiesMaxQueueLength);
    conf.setInt(YarnConfiguration.RM_ACTIVITIES_MANAGER_CLEANUP_INTERVAL_MS,
        500);
    ConcurrentMap<NodeId, RMNode> mockNodes = new ConcurrentHashMap<>();
    int numNodes = 5;
    for (int i = 0; i < numNodes; i++) {
      mockNodes.put(NodeId.newInstance("node" + i, 0), mock(RMNode.class));
    }
    CapacityScheduler cs = Mockito.mock(CapacityScheduler.class);
    RMContext mockRMContext = Mockito.mock(RMContext.class);
    Mockito.when(mockRMContext.getRMNodes()).thenReturn(mockNodes);
    Mockito.when(mockRMContext.getYarnConfiguration()).thenReturn(conf);
    Mockito.when(mockRMContext.getScheduler()).thenReturn(cs);
    /*
     * Test for async-scheduling with multi-node placement disabled
     */
    Mockito.when(cs.isMultiNodePlacementEnabled()).thenReturn(false);
    int numAsyncSchedulerThreads = 3;
    Mockito.when(cs.getNumAsyncSchedulerThreads())
        .thenReturn(numAsyncSchedulerThreads);
    ActivitiesManager newActivitiesManager =
        new ActivitiesManager(mockRMContext);
    Assert.assertEquals(1,
        newActivitiesManager.getAppActivitiesMaxQueueLength());
    newActivitiesManager.init(conf);
    newActivitiesManager.start();
    GenericTestUtils.waitFor(
        () -> newActivitiesManager.getAppActivitiesMaxQueueLength()
            == numNodes * numAsyncSchedulerThreads, 100, 3000);
    Assert.assertEquals(15,
        newActivitiesManager.getAppActivitiesMaxQueueLength());
    /*
     * Test for HB-driven scheduling with multi-node placement disabled
     */
    Mockito.when(cs.getNumAsyncSchedulerThreads()).thenReturn(0);
    GenericTestUtils.waitFor(
        () -> newActivitiesManager.getAppActivitiesMaxQueueLength()
            == numNodes * 1.2, 100, 3000);
    Assert.assertEquals(6,
        newActivitiesManager.getAppActivitiesMaxQueueLength());
    /*
     * Test for scheduling with multi-node placement enabled
     */
    Mockito.when(cs.isMultiNodePlacementEnabled()).thenReturn(true);
    GenericTestUtils.waitFor(
        () -> newActivitiesManager.getAppActivitiesMaxQueueLength()
            == configuredAppActivitiesMaxQueueLength, 100, 3000);
    Assert.assertEquals(1,
        newActivitiesManager.getAppActivitiesMaxQueueLength());
  }

  private void testManyTimes(String testingName,
      Supplier<Void> supplier, int testingTimes) {
    long totalTime = 0;
    for (int i = 0; i < testingTimes; i++) {
      long startTime = System.currentTimeMillis();
      supplier.get();
      totalTime += System.currentTimeMillis() - startTime;
    }
    System.out.println("#" + testingName + ", testing times : " + testingTimes
        + ", total cost time : " + totalTime + " ms, average cost time : "
        + (float) totalTime / testingTimes + " ms.");
  }

  /**
   * Testing activities manager which can record all history information about
   * node allocations.
   */
  public class TestingActivitiesManager extends ActivitiesManager {

    private Map<NodeId, List<List<NodeAllocation>>> historyNodeAllocations =
        new ConcurrentHashMap<>();

    public TestingActivitiesManager(RMContext rmContext) {
      super(rmContext);
      super.completedNodeAllocations = Mockito.spy(new ConcurrentHashMap<>());
      Mockito.doAnswer((invocationOnMock) -> {
        NodeId nodeId = (NodeId) invocationOnMock.getArguments()[0];
        List<NodeAllocation> nodeAllocations =
            (List<NodeAllocation>) invocationOnMock.getArguments()[1];
        List<List<NodeAllocation>> historyAllocationsForThisNode =
            historyNodeAllocations.get(nodeId);
        if (historyAllocationsForThisNode == null) {
          historyAllocationsForThisNode = new ArrayList<>();
          historyNodeAllocations.put(nodeId, historyAllocationsForThisNode);
        }
        historyAllocationsForThisNode.add(nodeAllocations);
        return null;
      }).when(completedNodeAllocations).put(any(NodeId.class),
          any(List.class));
    }
  }
}