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
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
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
                randomApp, Priority.newInstance(0),
                ActivityDiagnosticConstant.FAIL_TO_ALLOCATE,
                ActivityState.REJECTED);
        ActivitiesLogger.NODE
            .finishNodeUpdateRecording(activitiesManager, node.getNodeID());
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
                randomApp, Priority.newInstance(0),
                ActivityDiagnosticConstant.FAIL_TO_ALLOCATE,
                ActivityState.REJECTED);
        ActivitiesLogger.NODE.finishNodeUpdateRecording(activitiesManager,
            ActivitiesManager.EMPTY_NODE_ID);
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
                  randomApp, Priority.newInstance(0),
                  ActivityDiagnosticConstant.FAIL_TO_ALLOCATE,
                  ActivityState.REJECTED);
        }
        ActivitiesLogger.APP
            .finishAllocatedAppAllocationRecording(activitiesManager,
                randomApp.getApplicationId(), null, ActivityState.SKIPPED,
                ActivityDiagnosticConstant.SKIPPED_ALL_PRIORITIES);
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