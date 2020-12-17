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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.security.AppPriorityACLsManager;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.Before;
import org.junit.Test;

public class TestCSMaxRunningAppsEnforcer {
  private CapacitySchedulerQueueManager queueManager;
  private CSMaxRunningAppsEnforcer maxAppsEnforcer;
  private int appNum;
  private ControlledClock clock;
  private RMContext rmContext;
  private CapacityScheduler scheduler;
  private ActivitiesManager activitiesManager;
  private CapacitySchedulerConfiguration csConfig;

  @Before
  public void setup() throws IOException {
    csConfig = new CapacitySchedulerConfiguration();
    rmContext = mock(RMContext.class);
    when(rmContext.getYarnConfiguration()).thenReturn(csConfig);
    when(rmContext.getRMApps()).thenReturn(new ConcurrentHashMap<>());
    clock = new ControlledClock();
    scheduler = mock(CapacityScheduler.class);
    when(rmContext.getScheduler()).thenReturn(scheduler);
    when(scheduler.getConf()).thenReturn(csConfig);
    when(scheduler.getConfig()).thenReturn(csConfig);
    when(scheduler.getConfiguration()).thenReturn(csConfig);
    when(scheduler.getResourceCalculator()).thenReturn(
        new DefaultResourceCalculator());
    when(scheduler.getRMContext()).thenReturn(rmContext);
    when(scheduler.getClusterResource())
        .thenReturn(Resource.newInstance(16384, 8));
    when(scheduler.getMinimumAllocation())
        .thenReturn(Resource.newInstance(1024, 1));
    when(scheduler.getMinimumResourceCapability())
        .thenReturn(Resource.newInstance(1024, 1));
    activitiesManager = mock(ActivitiesManager.class);
    maxAppsEnforcer = new CSMaxRunningAppsEnforcer(scheduler);
    appNum = 0;
    setupQueues(csConfig);
    RMNodeLabelsManager labelManager = mock(RMNodeLabelsManager.class);
    AppPriorityACLsManager appPriorityACLManager =
        mock(AppPriorityACLsManager.class);
    when(rmContext.getNodeLabelManager()).thenReturn(labelManager);
    when(labelManager.getResourceByLabel(anyString(), any(Resource.class)))
        .thenReturn(Resource.newInstance(16384, 8));
    queueManager = new CapacitySchedulerQueueManager(csConfig, labelManager,
        appPriorityACLManager);
    queueManager.setCapacitySchedulerContext(scheduler);
    queueManager.initializeQueues(csConfig);
  }

  private void setupQueues(CapacitySchedulerConfiguration config) {
    config.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"queue1", "queue2"});
    config.setQueues("root.queue1", new String[] {"subqueue1", "subqueue2"});
    config.setQueues("root.queue1.subqueue1", new String[] {"leaf1"});
    config.setQueues("root.queue1.subqueue2", new String[] {"leaf2"});
    config.setFloat(PREFIX + "root.capacity", 100.0f);
    config.setFloat(PREFIX + "root.queue1.capacity", 50.0f);
    config.setFloat(PREFIX + "root.queue2.capacity", 50.0f);
    config.setFloat(PREFIX + "root.queue1.subqueue1.capacity", 50.0f);
    config.setFloat(PREFIX + "root.queue1.subqueue2.capacity", 50.0f);
    config.setFloat(PREFIX + "root.queue1.subqueue1.leaf1.capacity", 100.0f);
    config.setFloat(PREFIX + "root.queue1.subqueue2.leaf2.capacity", 100.0f);
  }

  private FiCaSchedulerApp addApp(LeafQueue queue, String user) {
    ApplicationId appId = ApplicationId.newInstance(0, appNum++);
    ApplicationAttemptId attId = ApplicationAttemptId.newInstance(appId, 0);

    FiCaSchedulerApp attempt = new FiCaSchedulerApp(attId,
        user, queue, queue.getAbstractUsersManager(),
        rmContext, Priority.newInstance(0), false,
        activitiesManager) {

      private final long startTime = clock.getTime();

      @Override
      public long getStartTime() {
        return startTime;
      }
    };

    maxAppsEnforcer.checkRunnabilityWithUpdate(attempt);
    maxAppsEnforcer.trackApp(attempt);

    queue.submitApplicationAttempt(attempt, attempt.getUser());

    return attempt;
  }

  private void removeApp(FiCaSchedulerApp attempt) {
    LeafQueue queue = attempt.getCSLeafQueue();
    queue.finishApplicationAttempt(attempt, queue.getQueuePath());
    maxAppsEnforcer.untrackApp(attempt);
    maxAppsEnforcer.updateRunnabilityOnAppRemoval(attempt);
  }

  @Test
  public void testRemoveDoesNotEnableAnyApp() {
    ParentQueue root =
        (ParentQueue) queueManager.getRootQueue();
    LeafQueue leaf1 = (LeafQueue) queueManager
        .getQueueByFullName("root.queue1.subqueue1.leaf1");
    LeafQueue leaf2 = (LeafQueue) queueManager
        .getQueueByFullName("root.queue1.subqueue2.leaf2");
    root.setMaxParallelApps(2);
    leaf1.setMaxParallelApps(1);
    leaf2.setMaxParallelApps(1);

    FiCaSchedulerApp app1 = addApp(leaf1, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    assertEquals(1, leaf1.getNumRunnableApps());
    assertEquals(1, leaf2.getNumRunnableApps());
    assertEquals(1, leaf2.getNumNonRunnableApps());

    removeApp(app1);
    assertEquals(0, leaf1.getNumRunnableApps());
    assertEquals(1, leaf2.getNumRunnableApps());
    assertEquals(1, leaf2.getNumNonRunnableApps());
  }

  @Test
  public void testRemoveEnablesAppOnCousinQueue() {
    LeafQueue leaf1 = (LeafQueue) queueManager
        .getQueueByFullName("root.queue1.subqueue1.leaf1");
    LeafQueue leaf2 = (LeafQueue) queueManager
        .getQueueByFullName("root.queue1.subqueue2.leaf2");
    ParentQueue queue1 = (ParentQueue) queueManager
        .getQueueByFullName("root.queue1");
    queue1.setMaxParallelApps(2);

    FiCaSchedulerApp app1 = addApp(leaf1, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    assertEquals(1, leaf1.getNumRunnableApps());
    assertEquals(1, leaf2.getNumRunnableApps());
    assertEquals(1, leaf2.getNumNonRunnableApps());

    removeApp(app1);
    assertEquals(0, leaf1.getNumRunnableApps());
    assertEquals(2, leaf2.getNumRunnableApps());
    assertEquals(0, leaf2.getNumNonRunnableApps());
  }

  @Test
  public void testRemoveEnablesOneByQueueOneByUser() {
    LeafQueue leaf1 = (LeafQueue) queueManager
        .getQueueByFullName("root.queue1.subqueue1.leaf1");
    LeafQueue leaf2 = (LeafQueue) queueManager
        .getQueueByFullName("root.queue1.subqueue2.leaf2");
    leaf1.setMaxParallelApps(2);
    //userMaxApps.put("user1", 1);
    csConfig.setInt(PREFIX + "user.user1.max-parallel-apps", 1);

    FiCaSchedulerApp app1 = addApp(leaf1, "user1");
    addApp(leaf1, "user2");
    addApp(leaf1, "user3");
    addApp(leaf2, "user1");
    assertEquals(2, leaf1.getNumRunnableApps());
    assertEquals(1, leaf1.getNumNonRunnableApps());
    assertEquals(1, leaf2.getNumNonRunnableApps());

    removeApp(app1);
    assertEquals(2, leaf1.getNumRunnableApps());
    assertEquals(1, leaf2.getNumRunnableApps());
    assertEquals(0, leaf1.getNumNonRunnableApps());
    assertEquals(0, leaf2.getNumNonRunnableApps());
  }

  @Test
  public void testRemoveEnablingOrderedByStartTime() {
    LeafQueue leaf1 = (LeafQueue) queueManager
        .getQueueByFullName("root.queue1.subqueue1.leaf1");
    LeafQueue leaf2 = (LeafQueue) queueManager
        .getQueueByFullName("root.queue1.subqueue2.leaf2");
    ParentQueue queue1 = (ParentQueue) queueManager
        .getQueueByFullName("root.queue1");
    queue1.setMaxParallelApps(2);
    FiCaSchedulerApp app1 = addApp(leaf1, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    clock.tickSec(20);
    addApp(leaf1, "user");
    assertEquals(1, leaf1.getNumRunnableApps());
    assertEquals(1, leaf2.getNumRunnableApps());
    assertEquals(1, leaf1.getNumNonRunnableApps());
    assertEquals(1, leaf2.getNumNonRunnableApps());
    removeApp(app1);
    assertEquals(0, leaf1.getNumRunnableApps());
    assertEquals(2, leaf2.getNumRunnableApps());
    assertEquals(0, leaf2.getNumNonRunnableApps());
  }

  @Test
  public void testMultipleAppsWaitingOnCousinQueue() {
    LeafQueue leaf1 = (LeafQueue) queueManager
        .getQueueByFullName("root.queue1.subqueue1.leaf1");
    LeafQueue leaf2 = (LeafQueue) queueManager
        .getQueueByFullName("root.queue1.subqueue2.leaf2");
    ParentQueue queue1 = (ParentQueue) queueManager
        .getQueueByFullName("root.queue1");
    queue1.setMaxParallelApps(2);
    FiCaSchedulerApp app1 = addApp(leaf1, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    assertEquals(1, leaf1.getNumRunnableApps());
    assertEquals(1, leaf2.getNumRunnableApps());
    assertEquals(2, leaf2.getNumNonRunnableApps());
    removeApp(app1);
    assertEquals(0, leaf1.getNumRunnableApps());
    assertEquals(2, leaf2.getNumRunnableApps());
    assertEquals(1, leaf2.getNumNonRunnableApps());
  }

  @Test
  public void testMultiListStartTimeIteratorEmptyAppLists() {
    List<List<FiCaSchedulerApp>> lists =
        new ArrayList<List<FiCaSchedulerApp>>();
    lists.add(Arrays.asList(mockAppAttempt(1)));
    lists.add(Arrays.asList(mockAppAttempt(2)));
    Iterator<FiCaSchedulerApp> iter =
        new CSMaxRunningAppsEnforcer.MultiListStartTimeIterator(lists);
    assertEquals(1, iter.next().getStartTime());
    assertEquals(2, iter.next().getStartTime());
  }

  private FiCaSchedulerApp mockAppAttempt(long startTime) {
    FiCaSchedulerApp schedApp = mock(FiCaSchedulerApp.class);
    when(schedApp.getStartTime()).thenReturn(startTime);
    return schedApp;
  }
}
