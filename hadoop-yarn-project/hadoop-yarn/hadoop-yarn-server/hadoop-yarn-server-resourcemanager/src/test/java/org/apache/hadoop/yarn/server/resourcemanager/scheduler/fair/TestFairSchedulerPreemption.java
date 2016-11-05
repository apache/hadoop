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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFairSchedulerPreemption extends FairSchedulerTestBase {
  private final static String ALLOC_FILE = new File(TEST_DIR,
      TestFairSchedulerPreemption.class.getName() + ".xml").getAbsolutePath();

  private ControlledClock clock;

  private static class StubbedFairScheduler extends FairScheduler {
    public long lastPreemptMemory = -1;

    @Override
    protected void preemptResources(Resource toPreempt) {
      lastPreemptMemory = toPreempt.getMemorySize();
    }

    public void resetLastPreemptResources() {
      lastPreemptMemory = -1;
    }
  }

  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, StubbedFairScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(FairSchedulerConfiguration.PREEMPTION, true);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    return conf;
  }

  @Before
  public void setup() throws IOException {
    conf = createConfiguration();
    clock = new ControlledClock();
  }

  @After
  public void teardown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    conf = null;
  }

  private void startResourceManagerWithStubbedFairScheduler(float utilizationThreshold) {
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD,
        utilizationThreshold);
    resourceManager = new MockRM(conf);
    resourceManager.start();

    assertTrue(
        resourceManager.getResourceScheduler() instanceof StubbedFairScheduler);
    scheduler = (FairScheduler)resourceManager.getResourceScheduler();

    scheduler.setClock(clock);
    scheduler.updateInterval = 60 * 1000;
  }

  // YARN-4648: The starting code for ResourceManager mock is originated from
  // TestFairScheduler. It should be keep as it was to guarantee no changing
  // behaviour of ResourceManager preemption.
  private void startResourceManagerWithRealFairScheduler() {
    scheduler = new FairScheduler();
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
            ResourceScheduler.class);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
    conf.setInt(FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB,
            1024);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 10240);
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, false);
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD, 0f);
    conf.setFloat(
            FairSchedulerConfiguration
                    .RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE,
            TEST_RESERVATION_THRESHOLD);

    resourceManager = new MockRM(conf);

    // TODO: This test should really be using MockRM. For now starting stuff
    // that is needed at a bare minimum.
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    resourceManager.getRMContext().getStateStore().start();

    // to initialize the master key
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();

    scheduler.setRMContext(resourceManager.getRMContext());
  }

  private void stopResourceManager() {
    if (scheduler != null) {
      scheduler.stop();
      scheduler = null;
    }
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.shutdown();
  }

  private void registerNodeAndSubmitApp(
      int memory, int vcores, int appContainers, int appMemory) {
    RMNode node1 = MockNodes.newNodeInfo(
        1, Resources.createResource(memory, vcores), 1, "node1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    assertEquals("Incorrect amount of resources in the cluster",
        memory, scheduler.rootMetrics.getAvailableMB());
    assertEquals("Incorrect amount of resources in the cluster",
        vcores, scheduler.rootMetrics.getAvailableVirtualCores());

    createSchedulingRequest(appMemory, "queueA", "user1", appContainers);
    scheduler.update();
    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < 3; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);
    }
    assertEquals("app1's request is not met",
        memory - appContainers * appMemory,
        scheduler.rootMetrics.getAvailableMB());
  }

  @Test
  public void testPreemptionWithFreeResources() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.print("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.print("<fairSharePreemptionTimeout>10</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    startResourceManagerWithStubbedFairScheduler(0f);
    // Create node with 4GB memory and 4 vcores
    registerNodeAndSubmitApp(4 * 1024, 4, 2, 1024);

    // Verify submitting another request triggers preemption
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tickSec(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should have been called", 1024,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);

    resourceManager.stop();

    startResourceManagerWithStubbedFairScheduler(0.8f);
    // Create node with 4GB memory and 4 vcores
    registerNodeAndSubmitApp(4 * 1024, 4, 3, 1024);

    // Verify submitting another request doesn't trigger preemption
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tickSec(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should not have been called", -1,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);

    resourceManager.stop();

    startResourceManagerWithStubbedFairScheduler(0.7f);
    // Create node with 4GB memory and 4 vcores
    registerNodeAndSubmitApp(4 * 1024, 4, 3, 1024);

    // Verify submitting another request triggers preemption
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tickSec(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should have been called", 1024,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);
  }

  @Test (timeout = 5000)
  /**
   * Make sure containers are chosen to be preempted in the correct order.
   */
  public void testChoiceOfPreemptedContainers() throws Exception {
    startResourceManagerWithRealFairScheduler();
    conf.setLong(FairSchedulerConfiguration.PREEMPTION_INTERVAL, 5000);
    conf.setLong(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 10000);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE + ".allocation.file", ALLOC_FILE);
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "false");

    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"default\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Create two nodes
    RMNode node1 =
            MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024, 4), 1,
                    "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 =
            MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024, 4), 2,
                    "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    // Queue A and B each request two applications
    ApplicationAttemptId app1 =
            createSchedulingRequest(1 * 1024, 1, "queueA", "user1", 1, 1);
    createSchedulingRequestExistingApplication(1 * 1024, 1, 2, app1);
    ApplicationAttemptId app2 =
            createSchedulingRequest(1 * 1024, 1, "queueA", "user1", 1, 3);
    createSchedulingRequestExistingApplication(1 * 1024, 1, 4, app2);

    ApplicationAttemptId app3 =
            createSchedulingRequest(1 * 1024, 1, "queueB", "user1", 1, 1);
    createSchedulingRequestExistingApplication(1 * 1024, 1, 2, app3);
    ApplicationAttemptId app4 =
            createSchedulingRequest(1 * 1024, 1, "queueB", "user1", 1, 3);
    createSchedulingRequestExistingApplication(1 * 1024, 1, 4, app4);

    scheduler.update();

    scheduler.getQueueManager().getLeafQueue("queueA", true)
            .setPolicy(SchedulingPolicy.parse("fifo"));
    scheduler.getQueueManager().getLeafQueue("queueB", true)
            .setPolicy(SchedulingPolicy.parse("fair"));

    // Sufficient node check-ins to fully schedule containers
    NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
    NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
    for (int i = 0; i < 4; i++) {
      scheduler.handle(nodeUpdate1);
      scheduler.handle(nodeUpdate2);
    }

    assertEquals(2, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(2, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(2, scheduler.getSchedulerApp(app3).getLiveContainers().size());
    assertEquals(2, scheduler.getSchedulerApp(app4).getLiveContainers().size());

    // Now new requests arrive from queueC and default
    createSchedulingRequest(1 * 1024, 1, "queueC", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, 1, "queueC", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, 1, "default", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, 1, "default", "user1", 1, 1);
    scheduler.update();

    // We should be able to claw back one container from queueA and queueB each.
    scheduler.preemptResources(Resources.createResource(2 * 1024));
    assertEquals(2, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(2, scheduler.getSchedulerApp(app3).getLiveContainers().size());

    // First verify we are adding containers to preemption list for the app.
    // For queueA (fifo), app2 is selected.
    // For queueB (fair), app4 is selected.
    assertTrue("App2 should have container to be preempted",
            !Collections.disjoint(
                    scheduler.getSchedulerApp(app2).getLiveContainers(),
                    scheduler.getSchedulerApp(app2).getPreemptionContainers()));
    assertTrue("App4 should have container to be preempted",
            !Collections.disjoint(
                    scheduler.getSchedulerApp(app2).getLiveContainers(),
                    scheduler.getSchedulerApp(app2).getPreemptionContainers()));

    // Pretend 15 seconds have passed
    clock.tickSec(15);

    // Trigger a kill by insisting we want containers back
    scheduler.preemptResources(Resources.createResource(2 * 1024));

    // At this point the containers should have been killed (since we are not simulating AM)
    assertEquals(1, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app4).getLiveContainers().size());
    // Inside each app, containers are sorted according to their priorities.
    // Containers with priority 4 are preempted for app2 and app4.
    Set<RMContainer> set = new HashSet<RMContainer>();
    for (RMContainer container :
            scheduler.getSchedulerApp(app2).getLiveContainers()) {
      if (container.getAllocatedSchedulerKey().getPriority().getPriority() ==
          4) {
        set.add(container);
      }
    }
    for (RMContainer container :
            scheduler.getSchedulerApp(app4).getLiveContainers()) {
      if (container.getAllocatedSchedulerKey().getPriority().getPriority() ==
          4) {
        set.add(container);
      }
    }
    assertTrue("Containers with priority=4 in app2 and app4 should be " +
            "preempted.", set.isEmpty());

    // Trigger a kill by insisting we want containers back
    scheduler.preemptResources(Resources.createResource(2 * 1024));

    // Pretend 15 seconds have passed
    clock.tickSec(15);

    // We should be able to claw back another container from A and B each.
    // For queueA (fifo), continue preempting from app2.
    // For queueB (fair), even app4 has a lowest priority container with p=4, it
    // still preempts from app3 as app3 is most over fair share.
    scheduler.preemptResources(Resources.createResource(2 * 1024));

    assertEquals(2, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app3).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app4).getLiveContainers().size());

    // Now A and B are below fair share, so preemption shouldn't do anything
    scheduler.preemptResources(Resources.createResource(2 * 1024));
    assertTrue("App1 should have no container to be preempted",
            scheduler.getSchedulerApp(app1).getPreemptionContainers().isEmpty());
    assertTrue("App2 should have no container to be preempted",
            scheduler.getSchedulerApp(app2).getPreemptionContainers().isEmpty());
    assertTrue("App3 should have no container to be preempted",
            scheduler.getSchedulerApp(app3).getPreemptionContainers().isEmpty());
    assertTrue("App4 should have no container to be preempted",
            scheduler.getSchedulerApp(app4).getPreemptionContainers().isEmpty());
    stopResourceManager();
  }

  @Test
  public void testPreemptionIsNotDelayedToNextRound() throws Exception {
    startResourceManagerWithRealFairScheduler();

    conf.setLong(FairSchedulerConfiguration.PREEMPTION_INTERVAL, 5000);
    conf.setLong(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 10000);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "false");

    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>8</weight>");
    out.println("<queue name=\"queueA1\" />");
    out.println("<queue name=\"queueA2\" />");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>2</weight>");
    out.println("</queue>");
    out.println("<defaultFairSharePreemptionTimeout>10</defaultFairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>.5</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node of 8G
    RMNode node1 = MockNodes.newNodeInfo(1,
            Resources.createResource(8 * 1024, 8), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Run apps in queueA.A1 and queueB
    ApplicationAttemptId app1 = createSchedulingRequest(1 * 1024, 1,
            "queueA.queueA1", "user1", 7, 1);
    // createSchedulingRequestExistingApplication(1 * 1024, 1, 2, app1);
    ApplicationAttemptId app2 = createSchedulingRequest(1 * 1024, 1, "queueB",
            "user2", 1, 1);

    scheduler.update();

    NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
    for (int i = 0; i < 8; i++) {
      scheduler.handle(nodeUpdate1);
    }

    // verify if the apps got the containers they requested
    assertEquals(7, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app2).getLiveContainers().size());

    // Now submit an app in queueA.queueA2
    ApplicationAttemptId app3 = createSchedulingRequest(1 * 1024, 1,
            "queueA.queueA2", "user3", 7, 1);
    scheduler.update();

    // Let 11 sec pass
    clock.tickSec(11);

    scheduler.update();
    Resource toPreempt = scheduler.resourceDeficit(scheduler.getQueueManager()
            .getLeafQueue("queueA.queueA2", false), clock.getTime());
    assertEquals(3277, toPreempt.getMemorySize());

    // verify if the 3 containers required by queueA2 are preempted in the same
    // round
    scheduler.preemptResources(toPreempt);
    assertEquals(3, scheduler.getSchedulerApp(app1).getPreemptionContainers()
            .size());
    stopResourceManager();
  }

  @Test (timeout = 5000)
  /**
   * Tests the timing of decision to preempt tasks.
   */
  public void testPreemptionDecision() throws Exception {
    startResourceManagerWithRealFairScheduler();

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueD\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionTimeout>10</defaultFairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>.5</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Create four nodes
    RMNode node1 =
            MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 2), 1,
                    "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 =
            MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 2), 2,
                    "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 =
            MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 2), 3,
                    "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);

    // Queue A and B each request three containers
    ApplicationAttemptId app1 =
            createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 1);
    ApplicationAttemptId app2 =
            createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 2);
    ApplicationAttemptId app3 =
            createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 3);

    ApplicationAttemptId app4 =
            createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 1);
    ApplicationAttemptId app5 =
            createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 2);
    ApplicationAttemptId app6 =
            createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 3);

    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < 2; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);

      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);

      NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
      scheduler.handle(nodeUpdate3);
    }

    // Now new requests arrive from queues C and D
    ApplicationAttemptId app7 =
            createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 1);
    ApplicationAttemptId app8 =
            createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 2);
    ApplicationAttemptId app9 =
            createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 3);

    ApplicationAttemptId app10 =
            createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 1);
    ApplicationAttemptId app11 =
            createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 2);
    ApplicationAttemptId app12 =
            createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 3);

    scheduler.update();

    FSLeafQueue schedC =
            scheduler.getQueueManager().getLeafQueue("queueC", true);
    FSLeafQueue schedD =
            scheduler.getQueueManager().getLeafQueue("queueD", true);

    assertTrue(Resources.equals(
            Resources.none(), scheduler.resourceDeficit(schedC, clock.getTime())));
    assertTrue(Resources.equals(
            Resources.none(), scheduler.resourceDeficit(schedD, clock.getTime())));
    // After minSharePreemptionTime has passed, they should want to preempt min
    // share.
    clock.tickSec(6);
    assertEquals(
            1024, scheduler.resourceDeficit(schedC, clock.getTime()).getMemorySize());
    assertEquals(
            1024, scheduler.resourceDeficit(schedD, clock.getTime()).getMemorySize());

    // After fairSharePreemptionTime has passed, they should want to preempt
    // fair share.
    scheduler.update();
    clock.tickSec(6);
    assertEquals(
            1536 , scheduler.resourceDeficit(schedC, clock.getTime()).getMemorySize());
    assertEquals(
            1536, scheduler.resourceDeficit(schedD, clock.getTime()).getMemorySize());
    stopResourceManager();
  }

  @Test
/**
 * Tests the timing of decision to preempt tasks.
 */
  public void testPreemptionDecisionWithDRF() throws Exception {
    startResourceManagerWithRealFairScheduler();

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,1vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,2vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,3vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueD\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,2vcores</minResources>");
    out.println("</queue>");
    out.println("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionTimeout>10</defaultFairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>.5</defaultFairSharePreemptionThreshold>");
    out.println("<defaultQueueSchedulingPolicy>drf</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Create four nodes
    RMNode node1 =
            MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 4), 1,
                    "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 =
            MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 4), 2,
                    "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 =
            MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 4), 3,
                    "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);

    // Queue A and B each request three containers
    ApplicationAttemptId app1 =
            createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 1);
    ApplicationAttemptId app2 =
            createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 2);
    ApplicationAttemptId app3 =
            createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 3);

    ApplicationAttemptId app4 =
            createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 1);
    ApplicationAttemptId app5 =
            createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 2);
    ApplicationAttemptId app6 =
            createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 3);

    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < 2; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);

      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);

      NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
      scheduler.handle(nodeUpdate3);
    }

    // Now new requests arrive from queues C and D
    ApplicationAttemptId app7 =
            createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 1);
    ApplicationAttemptId app8 =
            createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 2);
    ApplicationAttemptId app9 =
            createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 3);

    ApplicationAttemptId app10 =
            createSchedulingRequest(1 * 1024, "queueD", "user1", 2, 1);
    ApplicationAttemptId app11 =
            createSchedulingRequest(1 * 1024, "queueD", "user1", 2, 2);
    ApplicationAttemptId app12 =
            createSchedulingRequest(1 * 1024, "queueD", "user1", 2, 3);

    scheduler.update();

    FSLeafQueue schedC =
            scheduler.getQueueManager().getLeafQueue("queueC", true);
    FSLeafQueue schedD =
            scheduler.getQueueManager().getLeafQueue("queueD", true);

    assertTrue(Resources.equals(
            Resources.none(), scheduler.resourceDeficit(schedC, clock.getTime())));
    assertTrue(Resources.equals(
            Resources.none(), scheduler.resourceDeficit(schedD, clock.getTime())));

    // Test :
    // 1) whether componentWise min works as expected.
    // 2) DRF calculator is used

    // After minSharePreemptionTime has passed, they should want to preempt min
    // share.
    clock.tickSec(6);
    Resource res = scheduler.resourceDeficit(schedC, clock.getTime());
    assertEquals(1024, res.getMemorySize());
    // Demand = 3
    assertEquals(3, res.getVirtualCores());

    res = scheduler.resourceDeficit(schedD, clock.getTime());
    assertEquals(1024, res.getMemorySize());
    // Demand = 6, but min share = 2
    assertEquals(2, res.getVirtualCores());

    // After fairSharePreemptionTime has passed, they should want to preempt
    // fair share.
    scheduler.update();
    clock.tickSec(6);
    res = scheduler.resourceDeficit(schedC, clock.getTime());
    assertEquals(1536, res.getMemorySize());
    assertEquals(3, res.getVirtualCores());

    res = scheduler.resourceDeficit(schedD, clock.getTime());
    assertEquals(1536, res.getMemorySize());
    // Demand = 6, but fair share = 3
    assertEquals(3, res.getVirtualCores());
    stopResourceManager();
  }

  @Test
  /**
   * Tests the various timing of decision to preempt tasks.
   */
  public void testPreemptionDecisionWithVariousTimeout() throws Exception {
    startResourceManagerWithRealFairScheduler();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>2</weight>");
    out.println("<minSharePreemptionTimeout>10</minSharePreemptionTimeout>");
    out.println("<fairSharePreemptionTimeout>25</fairSharePreemptionTimeout>");
    out.println("<queue name=\"queueB1\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("<minSharePreemptionTimeout>5</minSharePreemptionTimeout>");
    out.println("</queue>");
    out.println("<queue name=\"queueB2\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("<fairSharePreemptionTimeout>20</fairSharePreemptionTimeout>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.print("<defaultMinSharePreemptionTimeout>15</defaultMinSharePreemptionTimeout>");
    out.print("<defaultFairSharePreemptionTimeout>30</defaultFairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Check the min/fair share preemption timeout for each queue
    QueueManager queueMgr = scheduler.getQueueManager();
    assertEquals(30000, queueMgr.getQueue("root")
            .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("default")
            .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueA")
            .getFairSharePreemptionTimeout());
    assertEquals(25000, queueMgr.getQueue("queueB")
            .getFairSharePreemptionTimeout());
    assertEquals(25000, queueMgr.getQueue("queueB.queueB1")
            .getFairSharePreemptionTimeout());
    assertEquals(20000, queueMgr.getQueue("queueB.queueB2")
            .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueC")
            .getFairSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("root")
            .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("default")
            .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueA")
            .getMinSharePreemptionTimeout());
    assertEquals(10000, queueMgr.getQueue("queueB")
            .getMinSharePreemptionTimeout());
    assertEquals(5000, queueMgr.getQueue("queueB.queueB1")
            .getMinSharePreemptionTimeout());
    assertEquals(10000, queueMgr.getQueue("queueB.queueB2")
            .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueC")
            .getMinSharePreemptionTimeout());

    // Create one big node
    RMNode node1 =
            MockNodes.newNodeInfo(1, Resources.createResource(6 * 1024, 6), 1,
                    "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A takes all resources
    for (int i = 0; i < 6; i ++) {
      createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 1);
    }

    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
    for (int i = 0; i < 6; i++) {
      scheduler.handle(nodeUpdate1);
    }

    // Now new requests arrive from queues B1, B2 and C
    createSchedulingRequest(1 * 1024, "queueB.queueB1", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, "queueB.queueB1", "user1", 1, 2);
    createSchedulingRequest(1 * 1024, "queueB.queueB1", "user1", 1, 3);
    createSchedulingRequest(1 * 1024, "queueB.queueB2", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, "queueB.queueB2", "user1", 1, 2);
    createSchedulingRequest(1 * 1024, "queueB.queueB2", "user1", 1, 3);
    createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 2);
    createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 3);

    scheduler.update();

    FSLeafQueue queueB1 = queueMgr.getLeafQueue("queueB.queueB1", true);
    FSLeafQueue queueB2 = queueMgr.getLeafQueue("queueB.queueB2", true);
    FSLeafQueue queueC = queueMgr.getLeafQueue("queueC", true);

    assertTrue(Resources.equals(
            Resources.none(), scheduler.resourceDeficit(queueB1, clock.getTime())));
    assertTrue(Resources.equals(
            Resources.none(), scheduler.resourceDeficit(queueB2, clock.getTime())));
    assertTrue(Resources.equals(
            Resources.none(), scheduler.resourceDeficit(queueC, clock.getTime())));

    // After 5 seconds, queueB1 wants to preempt min share
    scheduler.update();
    clock.tickSec(6);
    assertEquals(
            1024, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemorySize());
    assertEquals(
            0, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemorySize());
    assertEquals(
            0, scheduler.resourceDeficit(queueC, clock.getTime()).getMemorySize());

    // After 10 seconds, queueB2 wants to preempt min share
    scheduler.update();
    clock.tickSec(5);
    assertEquals(
            1024, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemorySize());
    assertEquals(
            1024, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemorySize());
    assertEquals(
            0, scheduler.resourceDeficit(queueC, clock.getTime()).getMemorySize());

    // After 15 seconds, queueC wants to preempt min share
    scheduler.update();
    clock.tickSec(5);
    assertEquals(
            1024, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemorySize());
    assertEquals(
            1024, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemorySize());
    assertEquals(
            1024, scheduler.resourceDeficit(queueC, clock.getTime()).getMemorySize());

    // After 20 seconds, queueB2 should want to preempt fair share
    scheduler.update();
    clock.tickSec(5);
    assertEquals(
            1024, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemorySize());
    assertEquals(
            1536, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemorySize());
    assertEquals(
            1024, scheduler.resourceDeficit(queueC, clock.getTime()).getMemorySize());

    // After 25 seconds, queueB1 should want to preempt fair share
    scheduler.update();
    clock.tickSec(5);
    assertEquals(
            1536, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemorySize());
    assertEquals(
            1536, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemorySize());
    assertEquals(
            1024, scheduler.resourceDeficit(queueC, clock.getTime()).getMemorySize());

    // After 30 seconds, queueC should want to preempt fair share
    scheduler.update();
    clock.tickSec(5);
    assertEquals(
            1536, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemorySize());
    assertEquals(
            1536, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemorySize());
    assertEquals(
            1536, scheduler.resourceDeficit(queueC, clock.getTime()).getMemorySize());
    stopResourceManager();
  }

  @Test
  /**
   * Tests the decision to preempt tasks respect to non-preemptable queues
   * 1, Queues as follow:
   *   queueA(non-preemptable)
   *   queueB(preemptable)
   *   parentQueue(non-preemptable)
   *     --queueC(preemptable)
   *   queueD(preemptable)
   * 2, Submit request to queueA, queueB, queueC, and all of them are over MinShare
   * 3, Now all resource are occupied
   * 4, Submit request to queueD, and need to preempt resource from other queues
   * 5, Only preemptable queue(queueB) would be preempted.
   */
  public void testPreemptionDecisionWithNonPreemptableQueue() throws Exception {
    startResourceManagerWithRealFairScheduler();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("<allowPreemptionFrom>false</allowPreemptionFrom>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"parentQueue\">");
    out.println("<allowPreemptionFrom>false</allowPreemptionFrom>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("<queue name=\"queueD\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>2048mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionTimeout>10</defaultFairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>.5</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Create four nodes(3G each)
    RMNode node1 =
            MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024, 3), 1,
                    "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 =
            MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024, 3), 2,
                    "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 =
            MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024, 3), 3,
                    "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);

    RMNode node4 =
            MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024, 3), 4,
                    "127.0.0.4");
    NodeAddedSchedulerEvent nodeEvent4 = new NodeAddedSchedulerEvent(node4);
    scheduler.handle(nodeEvent4);

    // Submit apps to queueA, queueB, queueC,
    // now all resource of the cluster is occupied
    ApplicationAttemptId app1 =
            createSchedulingRequest(1 * 1024, "queueA", "user1", 4, 1);
    ApplicationAttemptId app2 =
            createSchedulingRequest(1 * 1024, "queueB", "user1", 4, 2);
    ApplicationAttemptId app3 =
            createSchedulingRequest(1 * 1024, "parentQueue.queueC", "user1", 4, 3);

    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < 3; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);

      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);

      NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
      scheduler.handle(nodeUpdate3);

      NodeUpdateSchedulerEvent nodeUpdate4 = new NodeUpdateSchedulerEvent(node4);
      scheduler.handle(nodeUpdate4);
    }

    assertEquals(4, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(4, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(4, scheduler.getSchedulerApp(app3).getLiveContainers().size());

    // Now new requests arrive from queues D
    ApplicationAttemptId app4 =
            createSchedulingRequest(1 * 1024, "queueD", "user1", 4, 1);
    scheduler.update();
    FSLeafQueue schedD =
            scheduler.getQueueManager().getLeafQueue("queueD", true);

    // After minSharePreemptionTime has passed, 2G resource should preempted from
    // queueB to queueD
    clock.tickSec(6);
    assertEquals(2048,
            scheduler.resourceDeficit(schedD, clock.getTime()).getMemorySize());

    scheduler.preemptResources(Resources.createResource(2 * 1024));
    // now only app2 is selected to be preempted
    assertTrue("App2 should have container to be preempted",
            !Collections.disjoint(
                    scheduler.getSchedulerApp(app2).getLiveContainers(),
                    scheduler.getSchedulerApp(app2).getPreemptionContainers()));
    assertTrue("App1 should not have container to be preempted",
            Collections.disjoint(
                    scheduler.getSchedulerApp(app1).getLiveContainers(),
                    scheduler.getSchedulerApp(app1).getPreemptionContainers()));
    assertTrue("App3 should not have container to be preempted",
            Collections.disjoint(
                    scheduler.getSchedulerApp(app3).getLiveContainers(),
                    scheduler.getSchedulerApp(app3).getPreemptionContainers()));
    // Pretend 20 seconds have passed
    clock.tickSec(20);
    scheduler.preemptResources(Resources.createResource(2 * 1024));
    for (int i = 0; i < 3; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);

      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);

      NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
      scheduler.handle(nodeUpdate3);

      NodeUpdateSchedulerEvent nodeUpdate4 = new NodeUpdateSchedulerEvent(node4);
      scheduler.handle(nodeUpdate4);
    }
    // after preemption
    assertEquals(4, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(2, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(4, scheduler.getSchedulerApp(app3).getLiveContainers().size());
    assertEquals(2, scheduler.getSchedulerApp(app4).getLiveContainers().size());
    stopResourceManager();
  }

  @Test
  /**
   * Tests the decision to preempt tasks when allowPreemptionFrom is set false on
   * all queues.
   * Then none of them would be preempted actually.
   * 1, Queues as follow:
   *   queueA(non-preemptable)
   *   queueB(non-preemptable)
   *   parentQueue(non-preemptable)
   *     --queueC(preemptable)
   *   parentQueue(preemptable)
   *     --queueD(non-preemptable)
   * 2, Submit request to queueB, queueC, queueD, and all of them are over MinShare
   * 3, Now all resource are occupied
   * 4, Submit request to queueA, and need to preempt resource from other queues
   * 5, None of queues would be preempted.
   */
  public void testPreemptionDecisionWhenPreemptionDisabledOnAllQueues()
          throws Exception {
    startResourceManagerWithRealFairScheduler();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>2048mb,0vcores</minResources>");
    out.println("<allowPreemptionFrom>false</allowPreemptionFrom>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("<allowPreemptionFrom>false</allowPreemptionFrom>");
    out.println("</queue>");
    out.println("<queue name=\"parentQueue1\">");
    out.println("<allowPreemptionFrom>false</allowPreemptionFrom>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("<queue name=\"parentQueue2\">");
    out.println("<queue name=\"queueD\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("<allowPreemptionFrom>false</allowPreemptionFrom>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionTimeout>10</defaultFairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>.5</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Create four nodes(3G each)
    RMNode node1 =
            MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024, 3), 1,
                    "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 =
            MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024, 3), 2,
                    "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 =
            MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024, 3), 3,
                    "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);

    RMNode node4 =
            MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024, 3), 4,
                    "127.0.0.4");
    NodeAddedSchedulerEvent nodeEvent4 = new NodeAddedSchedulerEvent(node4);
    scheduler.handle(nodeEvent4);

    // Submit apps to queueB, queueC, queueD
    // now all resource of the cluster is occupied

    ApplicationAttemptId app1 =
            createSchedulingRequest(1 * 1024, "queueB", "user1", 4, 1);
    ApplicationAttemptId app2 =
            createSchedulingRequest(1 * 1024, "parentQueue1.queueC", "user1", 4, 2);
    ApplicationAttemptId app3 =
            createSchedulingRequest(1 * 1024, "parentQueue2.queueD", "user1", 4, 3);
    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < 3; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);

      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);

      NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
      scheduler.handle(nodeUpdate3);

      NodeUpdateSchedulerEvent nodeUpdate4 = new NodeUpdateSchedulerEvent(node4);
      scheduler.handle(nodeUpdate4);
    }

    assertEquals(4, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(4, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(4, scheduler.getSchedulerApp(app3).getLiveContainers().size());

    // Now new requests arrive from queues A
    ApplicationAttemptId app4 =
            createSchedulingRequest(1 * 1024, "queueA", "user1", 4, 1);
    scheduler.update();
    FSLeafQueue schedA =
            scheduler.getQueueManager().getLeafQueue("queueA", true);

    // After minSharePreemptionTime has passed, resource deficit is 2G
    clock.tickSec(6);
    assertEquals(2048,
            scheduler.resourceDeficit(schedA, clock.getTime()).getMemorySize());

    scheduler.preemptResources(Resources.createResource(2 * 1024));
    // now none app is selected to be preempted
    assertTrue("App1 should have container to be preempted",
            Collections.disjoint(
                    scheduler.getSchedulerApp(app1).getLiveContainers(),
                    scheduler.getSchedulerApp(app1).getPreemptionContainers()));
    assertTrue("App2 should not have container to be preempted",
            Collections.disjoint(
                    scheduler.getSchedulerApp(app2).getLiveContainers(),
                    scheduler.getSchedulerApp(app2).getPreemptionContainers()));
    assertTrue("App3 should not have container to be preempted",
            Collections.disjoint(
                    scheduler.getSchedulerApp(app3).getLiveContainers(),
                    scheduler.getSchedulerApp(app3).getPreemptionContainers()));
    // Pretend 20 seconds have passed
    clock.tickSec(20);
    scheduler.preemptResources(Resources.createResource(2 * 1024));
    for (int i = 0; i < 3; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);

      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);

      NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
      scheduler.handle(nodeUpdate3);

      NodeUpdateSchedulerEvent nodeUpdate4 = new NodeUpdateSchedulerEvent(node4);
      scheduler.handle(nodeUpdate4);
    }
    // after preemption
    assertEquals(4, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(4, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(4, scheduler.getSchedulerApp(app3).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app4).getLiveContainers().size());
    stopResourceManager();
  }

  @Test
  public void testBackwardsCompatiblePreemptionConfiguration() throws Exception {
    startResourceManagerWithRealFairScheduler();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<queue name=\"queueB1\">");
    out.println("<minSharePreemptionTimeout>5</minSharePreemptionTimeout>");
    out.println("</queue>");
    out.println("<queue name=\"queueB2\">");
    out.println("</queue>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("</queue>");
    out.print("<defaultMinSharePreemptionTimeout>15</defaultMinSharePreemptionTimeout>");
    out.print("<defaultFairSharePreemptionTimeout>30</defaultFairSharePreemptionTimeout>");
    out.print("<fairSharePreemptionTimeout>40</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Check the min/fair share preemption timeout for each queue
    QueueManager queueMgr = scheduler.getQueueManager();
    assertEquals(30000, queueMgr.getQueue("root")
            .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("default")
            .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueA")
            .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueB")
            .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueB.queueB1")
            .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueB.queueB2")
            .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueC")
            .getFairSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("root")
            .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("default")
            .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueA")
            .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueB")
            .getMinSharePreemptionTimeout());
    assertEquals(5000, queueMgr.getQueue("queueB.queueB1")
            .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueB.queueB2")
            .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueC")
            .getMinSharePreemptionTimeout());

    // If both exist, we take the default one
    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<queue name=\"queueB1\">");
    out.println("<minSharePreemptionTimeout>5</minSharePreemptionTimeout>");
    out.println("</queue>");
    out.println("<queue name=\"queueB2\">");
    out.println("</queue>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("</queue>");
    out.print("<defaultMinSharePreemptionTimeout>15</defaultMinSharePreemptionTimeout>");
    out.print("<defaultFairSharePreemptionTimeout>25</defaultFairSharePreemptionTimeout>");
    out.print("<fairSharePreemptionTimeout>30</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());

    assertEquals(25000, queueMgr.getQueue("root")
            .getFairSharePreemptionTimeout());
    stopResourceManager();
  }

  @Test(timeout = 5000)
  public void testRecoverRequestAfterPreemption() throws Exception {
    startResourceManagerWithRealFairScheduler();
    conf.setLong(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 10);

    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    SchedulerRequestKey schedulerKey = TestUtils.toSchedulerKey(20);
    String host = "127.0.0.1";
    int GB = 1024;

    // Create Node and raised Node Added event
    RMNode node = MockNodes.newNodeInfo(1,
            Resources.createResource(16 * 1024, 4), 0, host);
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    // Create 3 container requests and place it in ask
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest nodeLocalRequest = createResourceRequest(GB, 1, host,
            schedulerKey.getPriority().getPriority(), 1, true);
    ResourceRequest rackLocalRequest = createResourceRequest(GB, 1,
        node.getRackName(), schedulerKey.getPriority().getPriority(), 1,
        true);
    ResourceRequest offRackRequest = createResourceRequest(GB, 1,
        ResourceRequest.ANY, schedulerKey.getPriority().getPriority(), 1, true);
    ask.add(nodeLocalRequest);
    ask.add(rackLocalRequest);
    ask.add(offRackRequest);

    // Create Request and update
    ApplicationAttemptId appAttemptId = createSchedulingRequest("queueA",
            "user1", ask);
    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeUpdate);

    assertEquals(1, scheduler.getSchedulerApp(appAttemptId).getLiveContainers()
            .size());
    SchedulerApplicationAttempt app = scheduler.getSchedulerApp(appAttemptId);

    // ResourceRequest will be empty once NodeUpdate is completed
    Assert.assertNull(app.getResourceRequest(schedulerKey, host));

    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 1);
    RMContainer rmContainer = app.getRMContainer(containerId1);

    // Create a preempt event and register for preemption
    scheduler.warnOrKillContainer(rmContainer);

    // Wait for few clock ticks
    clock.tickSec(5);

    // preempt now
    scheduler.warnOrKillContainer(rmContainer);

    // Trigger container rescheduled event
    scheduler.handle(new ContainerPreemptEvent(appAttemptId, rmContainer,
            SchedulerEventType.MARK_CONTAINER_FOR_KILLABLE));

    List<ResourceRequest> requests = rmContainer.getResourceRequests();
    // Once recovered, resource request will be present again in app
    Assert.assertEquals(3, requests.size());
    for (ResourceRequest request : requests) {
      Assert.assertEquals(1,
              app.getResourceRequest(schedulerKey, request.getResourceName())
                      .getNumContainers());
    }

    // Send node heartbeat
    scheduler.update();
    scheduler.handle(nodeUpdate);

    List<Container> containers = scheduler.allocate(appAttemptId,
            Collections.<ResourceRequest> emptyList(),
            Collections.<ContainerId> emptyList(), null, null, null, null).getContainers();

    // Now with updated ResourceRequest, a container is allocated for AM.
    Assert.assertTrue(containers.size() == 1);
    stopResourceManager();
  }
}
