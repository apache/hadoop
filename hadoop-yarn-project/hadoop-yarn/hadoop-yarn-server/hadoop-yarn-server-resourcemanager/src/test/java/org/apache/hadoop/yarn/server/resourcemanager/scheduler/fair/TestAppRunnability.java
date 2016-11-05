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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
 * This class is to  test the fair scheduler functionality of
 * deciding the number of runnable application under various conditions.
 */
public class TestAppRunnability extends FairSchedulerTestBase {
  private final static String ALLOC_FILE =
      new File(TEST_DIR, "test-queues").getAbsolutePath();

  @Before
  public void setUp() throws IOException {
    conf = createConfiguration();
    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
  }

  @After
  public void tearDown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.shutdown();
  }

  @Test
  public void testUserAsDefaultQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId, "default", "user1", null);
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals("root.user1", resourceManager.getRMContext().getRMApps()
        .get(appAttemptId.getApplicationId()).getQueue());
  }

  @Test
  public void testNotUserAsDefaultQueue() throws Exception {

    // Restarting resource manager since the Conf object is changed changed.
    resourceManager.stop();
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "false");
    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();

    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId, "default", "user2", null);
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("user2", true)
        .getNumRunnableApps());
  }

  @Test
  public void testAppAdditionAndRemoval() throws Exception {
    ApplicationAttemptId attemptId = createAppAttemptId(1, 1);
    AppAddedSchedulerEvent appAddedEvent =
        new AppAddedSchedulerEvent(attemptId.getApplicationId(), "default",
            "user1");
    scheduler.handle(appAddedEvent);
    AppAttemptAddedSchedulerEvent attemptAddedEvent =
        new AppAttemptAddedSchedulerEvent(createAppAttemptId(1, 1), false);
    scheduler.handle(attemptAddedEvent);

    // Scheduler should have two queues (the default and the one created for
    // user1)
    assertEquals(2, scheduler.getQueueManager().getLeafQueues().size());

    // That queue should have one app
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());

    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(createAppAttemptId(1, 1),
            RMAppAttemptState.FINISHED, false);

    // Now remove app
    scheduler.handle(appRemovedEvent1);

    // Queue should have no apps
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
  }

  @Test
  public void testPreemptionVariablesForQueueCreatedRuntime() throws Exception {

    // Set preemption variables for the root queue
    FSParentQueue root = scheduler.getQueueManager().getRootQueue();
    root.setMinSharePreemptionTimeout(10000);
    root.setFairSharePreemptionTimeout(15000);
    root.setFairSharePreemptionThreshold(.6f);

    // User1 submits one application
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId, "default", "user1", null);

    // The user1 queue should inherit the configurations from the root queue
    FSLeafQueue userQueue =
        scheduler.getQueueManager().getLeafQueue("user1", true);
    assertEquals(1, userQueue.getNumRunnableApps());
    assertEquals(10000, userQueue.getMinSharePreemptionTimeout());
    assertEquals(15000, userQueue.getFairSharePreemptionTimeout());
    assertEquals(.6f, userQueue.getFairSharePreemptionThreshold(), 0.001);
  }

  @Test
  public void testDontAllowUndeclaredPools() throws Exception {
    conf.setBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS, false);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"jerry\">");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();
    // Restarting resource manager since the file location and content is
    // changed.
    resourceManager.stop();
    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();

    QueueManager queueManager = scheduler.getQueueManager();

    FSLeafQueue jerryQueue = queueManager.getLeafQueue("jerry", false);
    FSLeafQueue defaultQueue = queueManager.getLeafQueue("default", false);

    // Should get put into jerry
    createSchedulingRequest(1024, "jerry", "someuser");
    assertEquals(1, jerryQueue.getNumRunnableApps());

    // Should get forced into default
    createSchedulingRequest(1024, "newqueue", "someuser");
    assertEquals(1, jerryQueue.getNumRunnableApps());
    assertEquals(1, defaultQueue.getNumRunnableApps());

    // Would get put into someuser because of user-as-default-queue, but should
    // be forced into default
    createSchedulingRequest(1024, "default", "someuser");
    assertEquals(1, jerryQueue.getNumRunnableApps());
    assertEquals(2, defaultQueue.getNumRunnableApps());

    // Should get put into jerry because of user-as-default-queue
    createSchedulingRequest(1024, "default", "jerry");
    assertEquals(2, jerryQueue.getNumRunnableApps());
    assertEquals(2, defaultQueue.getNumRunnableApps());
  }

  @Test
  public void testMoveRunnableApp() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    QueueManager queueMgr = scheduler.getQueueManager();
    FSLeafQueue oldQueue = queueMgr.getLeafQueue("queue1", true);
    FSLeafQueue targetQueue = queueMgr.getLeafQueue("queue2", true);

    ApplicationAttemptId appAttId =
        createSchedulingRequest(1024, 1, "queue1", "user1", 3);
    ApplicationId appId = appAttId.getApplicationId();
    RMNode node = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.handle(updateEvent);

    assertEquals(Resource.newInstance(1024, 1), oldQueue.getResourceUsage());
    scheduler.update();
    assertEquals(Resource.newInstance(3072, 3), oldQueue.getDemand());

    scheduler.moveApplication(appId, "queue2");
    FSAppAttempt app = scheduler.getSchedulerApp(appAttId);
    assertSame(targetQueue, app.getQueue());
    assertFalse(oldQueue.isRunnableApp(app));
    assertTrue(targetQueue.isRunnableApp(app));
    assertEquals(Resource.newInstance(0, 0), oldQueue.getResourceUsage());
    assertEquals(Resource.newInstance(1024, 1), targetQueue.getResourceUsage());
    assertEquals(0, oldQueue.getNumRunnableApps());
    assertEquals(1, targetQueue.getNumRunnableApps());
    assertEquals(1, queueMgr.getRootQueue().getNumRunnableApps());

    scheduler.update();
    assertEquals(Resource.newInstance(0, 0), oldQueue.getDemand());
    assertEquals(Resource.newInstance(3072, 3), targetQueue.getDemand());
  }

  @Test
  public void testMoveNonRunnableApp() throws Exception {
    QueueManager queueMgr = scheduler.getQueueManager();
    FSLeafQueue oldQueue = queueMgr.getLeafQueue("queue1", true);
    FSLeafQueue targetQueue = queueMgr.getLeafQueue("queue2", true);
    oldQueue.setMaxRunningApps(0);
    targetQueue.setMaxRunningApps(0);

    ApplicationAttemptId appAttId =
        createSchedulingRequest(1024, 1, "queue1", "user1", 3);

    assertEquals(0, oldQueue.getNumRunnableApps());
    scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
    assertEquals(0, oldQueue.getNumRunnableApps());
    assertEquals(0, targetQueue.getNumRunnableApps());
    assertEquals(0, queueMgr.getRootQueue().getNumRunnableApps());
  }

  @Test
  public void testMoveMakesAppRunnable() throws Exception {
    QueueManager queueMgr = scheduler.getQueueManager();
    FSLeafQueue oldQueue = queueMgr.getLeafQueue("queue1", true);
    FSLeafQueue targetQueue = queueMgr.getLeafQueue("queue2", true);
    oldQueue.setMaxRunningApps(0);

    ApplicationAttemptId appAttId =
        createSchedulingRequest(1024, 1, "queue1", "user1", 3);

    FSAppAttempt app = scheduler.getSchedulerApp(appAttId);
    assertTrue(oldQueue.isNonRunnableApp(app));

    scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
    assertFalse(oldQueue.isNonRunnableApp(app));
    assertFalse(targetQueue.isNonRunnableApp(app));
    assertTrue(targetQueue.isRunnableApp(app));
    assertEquals(1, targetQueue.getNumRunnableApps());
    assertEquals(1, queueMgr.getRootQueue().getNumRunnableApps());
  }
}
