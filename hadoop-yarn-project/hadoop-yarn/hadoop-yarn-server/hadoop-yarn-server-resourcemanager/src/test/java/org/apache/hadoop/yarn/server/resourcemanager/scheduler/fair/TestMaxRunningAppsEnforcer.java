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
  
  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    clock = new TestFairScheduler.MockClock();
    FairScheduler scheduler = mock(FairScheduler.class);
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
    when(rmContext.getEpoch()).thenReturn(0);
  }
  
  private FSSchedulerApp addApp(FSLeafQueue queue, String user) {
    ApplicationId appId = ApplicationId.newInstance(0l, appNum++);
    ApplicationAttemptId attId = ApplicationAttemptId.newInstance(appId, 0);
    boolean runnable = maxAppsEnforcer.canAppBeRunnable(queue, user);
    FSSchedulerApp app = new FSSchedulerApp(attId, user, queue, null,
        rmContext);
    queue.addApp(app, runnable);
    if (runnable) {
      maxAppsEnforcer.trackRunnableApp(app);
    } else {
      maxAppsEnforcer.trackNonRunnableApp(app);
    }
    return app;
  }
  
  private void removeApp(FSSchedulerApp app) {
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
    FSSchedulerApp app1 = addApp(leaf1, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    assertEquals(1, leaf1.getRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getNonRunnableAppSchedulables().size());
    removeApp(app1);
    assertEquals(0, leaf1.getRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getNonRunnableAppSchedulables().size());
  }
  
  @Test
  public void testRemoveEnablesAppOnCousinQueue() {
    FSLeafQueue leaf1 = queueManager.getLeafQueue("root.queue1.subqueue1.leaf1", true);
    FSLeafQueue leaf2 = queueManager.getLeafQueue("root.queue1.subqueue2.leaf2", true);
    queueMaxApps.put("root.queue1", 2);
    FSSchedulerApp app1 = addApp(leaf1, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    assertEquals(1, leaf1.getRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getNonRunnableAppSchedulables().size());
    removeApp(app1);
    assertEquals(0, leaf1.getRunnableAppSchedulables().size());
    assertEquals(2, leaf2.getRunnableAppSchedulables().size());
    assertEquals(0, leaf2.getNonRunnableAppSchedulables().size());
  }
  
  @Test
  public void testRemoveEnablesOneByQueueOneByUser() {
    FSLeafQueue leaf1 = queueManager.getLeafQueue("root.queue1.leaf1", true);
    FSLeafQueue leaf2 = queueManager.getLeafQueue("root.queue1.leaf2", true);
    queueMaxApps.put("root.queue1.leaf1", 2);
    userMaxApps.put("user1", 1);
    FSSchedulerApp app1 = addApp(leaf1, "user1");
    addApp(leaf1, "user2");
    addApp(leaf1, "user3");
    addApp(leaf2, "user1");
    assertEquals(2, leaf1.getRunnableAppSchedulables().size());
    assertEquals(1, leaf1.getNonRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getNonRunnableAppSchedulables().size());
    removeApp(app1);
    assertEquals(2, leaf1.getRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getRunnableAppSchedulables().size());
    assertEquals(0, leaf1.getNonRunnableAppSchedulables().size());
    assertEquals(0, leaf2.getNonRunnableAppSchedulables().size());
  }
  
  @Test
  public void testRemoveEnablingOrderedByStartTime() {
    FSLeafQueue leaf1 = queueManager.getLeafQueue("root.queue1.subqueue1.leaf1", true);
    FSLeafQueue leaf2 = queueManager.getLeafQueue("root.queue1.subqueue2.leaf2", true);
    queueMaxApps.put("root.queue1", 2);
    FSSchedulerApp app1 = addApp(leaf1, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    clock.tick(20);
    addApp(leaf1, "user");
    assertEquals(1, leaf1.getRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getRunnableAppSchedulables().size());
    assertEquals(1, leaf1.getNonRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getNonRunnableAppSchedulables().size());
    removeApp(app1);
    assertEquals(0, leaf1.getRunnableAppSchedulables().size());
    assertEquals(2, leaf2.getRunnableAppSchedulables().size());
    assertEquals(0, leaf2.getNonRunnableAppSchedulables().size());
  }
  
  @Test
  public void testMultipleAppsWaitingOnCousinQueue() {
    FSLeafQueue leaf1 = queueManager.getLeafQueue("root.queue1.subqueue1.leaf1", true);
    FSLeafQueue leaf2 = queueManager.getLeafQueue("root.queue1.subqueue2.leaf2", true);
    queueMaxApps.put("root.queue1", 2);
    FSSchedulerApp app1 = addApp(leaf1, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    assertEquals(1, leaf1.getRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getRunnableAppSchedulables().size());
    assertEquals(2, leaf2.getNonRunnableAppSchedulables().size());
    removeApp(app1);
    assertEquals(0, leaf1.getRunnableAppSchedulables().size());
    assertEquals(2, leaf2.getRunnableAppSchedulables().size());
    assertEquals(1, leaf2.getNonRunnableAppSchedulables().size());
  }
  
  @Test
  public void testMultiListStartTimeIteratorEmptyAppLists() {
    List<List<AppSchedulable>> lists = new ArrayList<List<AppSchedulable>>();
    lists.add(Arrays.asList(mockAppSched(1)));
    lists.add(Arrays.asList(mockAppSched(2)));
    Iterator<FSSchedulerApp> iter =
        new MaxRunningAppsEnforcer.MultiListStartTimeIterator(lists);
    assertEquals(1, iter.next().getAppSchedulable().getStartTime());
    assertEquals(2, iter.next().getAppSchedulable().getStartTime());
  }
  
  private AppSchedulable mockAppSched(long startTime) {
    AppSchedulable appSched = mock(AppSchedulable.class);
    when(appSched.getStartTime()).thenReturn(startTime);
    FSSchedulerApp schedApp = mock(FSSchedulerApp.class);
    when(schedApp.getAppSchedulable()).thenReturn(appSched);
    when(appSched.getApp()).thenReturn(schedApp);
    return appSched;
  }
}
