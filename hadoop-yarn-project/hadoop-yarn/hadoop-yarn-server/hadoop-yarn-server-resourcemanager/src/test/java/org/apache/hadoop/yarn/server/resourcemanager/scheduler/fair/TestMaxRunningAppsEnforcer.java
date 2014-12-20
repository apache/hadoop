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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Before;
import org.junit.Test;

public class TestMaxRunningAppsEnforcer {
  private QueueManager queueManager;
  private Map<String, Integer> queueMaxApps;
  private Map<String, Integer> userMaxApps;
  private MaxRunningAppsEnforcer maxAppsEnforcer;
  private int appNum;
  private TestFairScheduler.MockClock clock;
  private RMContext rmContext;
  private FairScheduler scheduler;
  
  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    clock = new TestFairScheduler.MockClock();
    scheduler = mock(FairScheduler.class);
    when(scheduler.getConf()).thenReturn(
        new FairSchedulerConfiguration(conf));
    when(scheduler.getClock()).thenReturn(clock);
    AllocationConfiguration allocConf = new AllocationConfiguration(
        conf);
    when(scheduler.getAllocationConfiguration()).thenReturn(allocConf);
    
    queueManager = new QueueManager(scheduler);
    queueManager.initialize(conf);
    queueMaxApps = allocConf.queueMaxApps;
    userMaxApps = allocConf.userMaxApps;
    maxAppsEnforcer = new MaxRunningAppsEnforcer(scheduler);
    appNum = 0;
    rmContext = mock(RMContext.class);
    when(rmContext.getEpoch()).thenReturn(0L);
  }
  
  private FSAppAttempt addApp(FSLeafQueue queue, String user) {
    ApplicationId appId = ApplicationId.newInstance(0l, appNum++);
    ApplicationAttemptId attId = ApplicationAttemptId.newInstance(appId, 0);
    boolean runnable = maxAppsEnforcer.canAppBeRunnable(queue, user);
    FSAppAttempt app = new FSAppAttempt(scheduler, attId, user, queue, null,
        rmContext);
    queue.addApp(app, runnable);
    if (runnable) {
      maxAppsEnforcer.trackRunnableApp(app);
    } else {
      maxAppsEnforcer.trackNonRunnableApp(app);
    }
    return app;
  }
  
  private void removeApp(FSAppAttempt app) {
    app.getQueue().removeApp(app);
    maxAppsEnforcer.untrackRunnableApp(app);
    maxAppsEnforcer.updateRunnabilityOnAppRemoval(app, app.getQueue());
  }
  
  @Test
  public void testRemoveDoesNotEnableAnyApp() {
    FSLeafQueue leaf1 = queueManager.getLeafQueue("root.queue1", true);
    FSLeafQueue leaf2 = queueManager.getLeafQueue("root.queue2", true);
    queueMaxApps.put("root", 2);
    queueMaxApps.put("root.queue1", 1);
    queueMaxApps.put("root.queue2", 1);
    FSAppAttempt app1 = addApp(leaf1, "user");
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
    FSLeafQueue leaf1 = queueManager.getLeafQueue("root.queue1.subqueue1.leaf1", true);
    FSLeafQueue leaf2 = queueManager.getLeafQueue("root.queue1.subqueue2.leaf2", true);
    queueMaxApps.put("root.queue1", 2);
    FSAppAttempt app1 = addApp(leaf1, "user");
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
    FSLeafQueue leaf1 = queueManager.getLeafQueue("root.queue1.leaf1", true);
    FSLeafQueue leaf2 = queueManager.getLeafQueue("root.queue1.leaf2", true);
    queueMaxApps.put("root.queue1.leaf1", 2);
    userMaxApps.put("user1", 1);
    FSAppAttempt app1 = addApp(leaf1, "user1");
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
    FSLeafQueue leaf1 = queueManager.getLeafQueue("root.queue1.subqueue1.leaf1", true);
    FSLeafQueue leaf2 = queueManager.getLeafQueue("root.queue1.subqueue2.leaf2", true);
    queueMaxApps.put("root.queue1", 2);
    FSAppAttempt app1 = addApp(leaf1, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    clock.tick(20);
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
    FSLeafQueue leaf1 = queueManager.getLeafQueue("root.queue1.subqueue1.leaf1", true);
    FSLeafQueue leaf2 = queueManager.getLeafQueue("root.queue1.subqueue2.leaf2", true);
    queueMaxApps.put("root.queue1", 2);
    FSAppAttempt app1 = addApp(leaf1, "user");
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
    List<List<FSAppAttempt>> lists = new ArrayList<List<FSAppAttempt>>();
    lists.add(Arrays.asList(mockAppAttempt(1)));
    lists.add(Arrays.asList(mockAppAttempt(2)));
    Iterator<FSAppAttempt> iter =
        new MaxRunningAppsEnforcer.MultiListStartTimeIterator(lists);
    assertEquals(1, iter.next().getStartTime());
    assertEquals(2, iter.next().getStartTime());
  }
  
  private FSAppAttempt mockAppAttempt(long startTime) {
    FSAppAttempt schedApp = mock(FSAppAttempt.class);
    when(schedApp.getStartTime()).thenReturn(startTime);
    return schedApp;
  }
}
