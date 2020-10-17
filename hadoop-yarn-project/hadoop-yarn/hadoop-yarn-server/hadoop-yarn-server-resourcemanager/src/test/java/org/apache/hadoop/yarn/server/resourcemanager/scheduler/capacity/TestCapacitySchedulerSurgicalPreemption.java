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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitorManager;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;

import org.apache.hadoop.yarn.server.resourcemanager.nodelabels
    .RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestCapacitySchedulerSurgicalPreemption
    extends CapacitySchedulerPreemptionTestBase {

  private static final int NUM_NM = 5;
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf.setBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS,
        true);
  }

  @Test(timeout = 60000)
  public void testSimpleSurgicalPreemption()
      throws Exception {
    /**
     * Test case: Submit two application (app1/app2) to different queues, queue
     * structure:
     *
     * <pre>
     *             Root
     *            /  |  \
     *           a   b   c
     *          10   20  70
     * </pre>
     *
     * 1) Two nodes (n1/n2) in the cluster, each of them has 20G.
     *
     * 2) app1 submit to queue-a first, it asked 32 * 1G containers
     * We will allocate 16 on n1 and 16 on n2.
     *
     * 3) app2 submit to queue-c, ask for one 1G container (for AM)
     *
     * 4) app2 asks for another 6G container, it will be reserved on n1
     *
     * Now: we have:
     * n1: 17 from app1, 1 from app2, and 1 reserved from app2
     * n2: 16 from app1.
     *
     * After preemption, we should expect:
     * Preempt 4 containers from app1 on n1.
     */
    testSimpleSurgicalPreemption("a", "c", "user", "user");
  }

  protected void testSimpleSurgicalPreemption(String queue1, String queue2,
      String user1, String user2)
      throws Exception {

    MockRM rm1 = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.start();

    MockNM nm1 = rm1.registerNode("h1:1234", 20 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 20 * GB);
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser(user1)
            .withAcls(null)
            .withQueue(queue1)
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    am1.allocate("*", 1 * GB, 32, new ArrayList<ContainerId>());

    // Do allocation for node1/node2
    for (int i = 0; i < 32; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    }

    // App1 should have 33 containers now
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(33, schedulerApp1.getLiveContainers().size());
    // 17 from n1 and 16 from n2
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode1.getNodeID()),
        am1.getApplicationAttemptId(), 17);
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode2.getNodeID()),
        am1.getApplicationAttemptId(), 16);


    // Submit app2 to queue-c and asks for a 1G container for AM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser(user2)
            .withAcls(null)
            .withQueue(queue2)
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    // NM1/NM2 has available resource = 2G/4G
    Assert.assertEquals(2 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    Assert.assertEquals(4 * GB, cs.getNode(nm2.getNodeId())
        .getUnallocatedResource().getMemorySize());

    // AM asks for a 1 * GB container
    am2.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.newInstance(1), ResourceRequest.ANY,
            Resources.createResource(6 * GB), 1)), null);

    // Call allocation once on n1, we should expect the container reserved on n1
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());

    // Get edit policy and do one update
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();

    // Call edit schedule twice, and check if 4 containers from app1 at n1 killed
    editPolicy.editSchedule();
    editPolicy.editSchedule();

    waitNumberOfLiveContainersFromApp(schedulerApp1, 29);

    // 13 from n1 (4 preempted) and 16 from n2
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode1.getNodeID()),
        am1.getApplicationAttemptId(), 13);
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode2.getNodeID()),
        am1.getApplicationAttemptId(), 16);

    // Ensure preemption metrics were recored.
    Assert.assertEquals("Number of preempted containers incorrectly recorded:",
        4, cs.getQueue("root").getMetrics().getAggregatePreemptedContainers());

    Assert.assertEquals("Amount of preempted memory incorrectly recorded:",
        4 * GB,
        cs.getQueue("root").getMetrics().getAggregateMemoryMBPreempted());

    Assert.assertEquals("Number of preempted vcores incorrectly recorded:", 4,
        cs.getQueue("root").getMetrics().getAggregateVcoresPreempted());

    rm1.close();
  }

  @Test(timeout = 60000)
  public void testSurgicalPreemptionWithAvailableResource()
      throws Exception {
    /**
     * Test case: Submit two application (app1/app2) to different queues, queue
     * structure:
     *
     * <pre>
     *             Root
     *            /  |  \
     *           a   b   c
     *          10   20  70
     * </pre>
     *
     * 1) Two nodes (n1/n2) in the cluster, each of them has 20G.
     *
     * 2) app1 submit to queue-b, asks for 1G * 5
     *
     * 3) app2 submit to queue-c, ask for one 4G container (for AM)
     *
     * After preemption, we should expect:
     * Preempt 3 containers from app1 and AM of app2 successfully allocated.
     */
    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    MockNM nm1 = rm1.registerNode("h1:1234", 20 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 20 * GB);
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    am1.allocate("*", 1 * GB, 38, new ArrayList<ContainerId>());

    // Do allocation for node1/node2
    for (int i = 0; i < 38; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    }

    // App1 should have 31 containers now
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(39, schedulerApp1.getLiveContainers().size());
    // 17 from n1 and 16 from n2
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode1.getNodeID()),
        am1.getApplicationAttemptId(), 20);
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode2.getNodeID()),
        am1.getApplicationAttemptId(), 19);


    // Submit app2 to queue-c and asks for a 4G container for AM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(4 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        ApplicationAttemptId.newInstance(app2.getApplicationId(), 1));

    // Call editSchedule: containers are selected to be preemption candidate
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();
    editPolicy.editSchedule();
    Assert.assertEquals(3, editPolicy.getToPreemptContainers().size());

    // Call editSchedule again: selected containers are killed
    editPolicy.editSchedule();
    waitNumberOfLiveContainersFromApp(schedulerApp1, 36);

    // Call allocation, containers are reserved
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    waitNumberOfReservedContainersFromApp(schedulerApp2, 1);

    // Call editSchedule twice and allocation once, container should get allocated
    editPolicy.editSchedule();
    editPolicy.editSchedule();

    int tick = 0;
    while (schedulerApp2.getLiveContainers().size() != 1 && tick < 10) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
      tick++;
      Thread.sleep(100);
    }
    waitNumberOfReservedContainersFromApp(schedulerApp2, 0);

    rm1.close();
  }

  @Test(timeout = 60000)
  public void testPriorityPreemptionWhenAllQueuesAreBelowGuaranteedCapacities()
      throws Exception {
    /**
     * Test case: Submit two application (app1/app2) to different queues, queue
     * structure:
     *
     * <pre>
     *             Root
     *            /  |  \
     *           a   b   c
     *          10   20  70
     * </pre>
     *
     * 1) Two nodes (n1/n2) in the cluster, each of them has 20G.
     *
     * 2) app1 submit to queue-b first, it asked 6 * 1G containers
     * We will allocate 4 on n1 (including AM) and 3 on n2.
     *
     * 3) app2 submit to queue-c, ask for one 18G container (for AM)
     *
     * After preemption, we should expect:
     * Preempt 3 containers from app1 and AM of app2 successfully allocated.
     */
    conf.setPUOrderingPolicyUnderUtilizedPreemptionEnabled(true);
    conf.setPUOrderingPolicyUnderUtilizedPreemptionDelay(1000);
    conf.setQueueOrderingPolicy(CapacitySchedulerConfiguration.ROOT,
        CapacitySchedulerConfiguration.QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY);

    // Queue c has higher priority than a/b
    conf.setQueuePriority(CapacitySchedulerConfiguration.ROOT + ".c", 1);

    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    MockNM nm1 = rm1.registerNode("h1:1234", 20 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 20 * GB);
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    am1.allocate("*", 1 * GB, 6, new ArrayList<>());

    // Do allocation for node1/node2
    for (int i = 0; i < 3; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    }

    // App1 should have 7 containers now, so the abs-used-cap of b is
    // 7 / 40 = 17.5% < 20% (guaranteed)
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(7, schedulerApp1.getLiveContainers().size());
    // 4 from n1 and 3 from n2
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode1.getNodeID()),
        am1.getApplicationAttemptId(), 4);
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode2.getNodeID()),
        am1.getApplicationAttemptId(), 3);

    // Submit app2 to queue-c and asks for a 1G container for AM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(18 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        ApplicationAttemptId.newInstance(app2.getApplicationId(), 1));

    while (cs.getNode(rmNode1.getNodeID()).getReservedContainer() == null) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      Thread.sleep(10);
    }

    // Call editSchedule immediately: containers are not selected
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();
    editPolicy.editSchedule();
    Assert.assertEquals(0, editPolicy.getToPreemptContainers().size());

    // Sleep the timeout interval, we should be able to see containers selected
    Thread.sleep(1000);
    editPolicy.editSchedule();
    Assert.assertEquals(2, editPolicy.getToPreemptContainers().size());

    // Call editSchedule again: selected containers are killed, and new AM
    // container launched
    editPolicy.editSchedule();

    // Do allocation till reserved container allocated
    while (cs.getNode(rmNode1.getNodeID()).getReservedContainer() != null) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      Thread.sleep(10);
    }

    waitNumberOfLiveContainersFromApp(schedulerApp2, 1);

    rm1.close();
  }

  @Test(timeout = 300000)
  public void testPriorityPreemptionRequiresMoveReservation()
      throws Exception {
    /**
     * Test case: Submit two application (app1/app2) to different queues, queue
     * structure:
     *
     * <pre>
     *             Root
     *            /  |  \
     *           a   b   c
     *          10   20  70
     * </pre>
     *
     * 1) 3 nodes in the cluster, 10G for each
     *
     * 2) app1 submit to queue-b first, it asked 2G each,
     *    it can get 2G on n1 (AM), 2 * 2G on n2
     *
     * 3) app2 submit to queue-c, with 2G AM container (allocated on n3)
     *    app2 requires 9G resource, which will be reserved on n3
     *
     * We should expect container unreserved from n3 and allocated on n1/n2
     */
    conf.setPUOrderingPolicyUnderUtilizedPreemptionEnabled(true);
    conf.setPUOrderingPolicyUnderUtilizedPreemptionDelay(1000);
    conf.setQueueOrderingPolicy(CapacitySchedulerConfiguration.ROOT,
        CapacitySchedulerConfiguration.QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY);
    conf.setPUOrderingPolicyUnderUtilizedPreemptionMoveReservation(true);

    // Queue c has higher priority than a/b
    conf.setQueuePriority(CapacitySchedulerConfiguration.ROOT + ".c", 1);

    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 10 * GB);
    MockNM nm3 = rm1.registerNode("h3:1234", 10 * GB);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());
    RMNode rmNode3 = rm1.getRMContext().getRMNodes().get(nm3.getNodeId());

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    am1.allocate("*", 2 * GB, 2, new ArrayList<>());

    // Do allocation for node2 twice
    for (int i = 0; i < 2; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    }

    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(3, schedulerApp1.getLiveContainers().size());

    // 1 from n1 and 2 from n2
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode1.getNodeID()),
        am1.getApplicationAttemptId(), 1);
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode2.getNodeID()),
        am1.getApplicationAttemptId(), 2);

    // Submit app2 to queue-c and asks for a 2G container for AM, on n3
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm3);
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        ApplicationAttemptId.newInstance(app2.getApplicationId(), 1));

    // Asks 1 * 9G container
    am2.allocate("*", 9 * GB, 1, new ArrayList<>());

    // Do allocation for node3 once
    cs.handle(new NodeUpdateSchedulerEvent(rmNode3));

    // Make sure container reserved on node3
    Assert.assertNotNull(
        cs.getNode(rmNode3.getNodeID()).getReservedContainer());

    // Call editSchedule immediately: nothing happens
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();
    editPolicy.editSchedule();
    Assert.assertNotNull(
        cs.getNode(rmNode3.getNodeID()).getReservedContainer());

    // Sleep the timeout interval, we should be able to see reserved container
    // moved to n2 (n1 occupied by AM)
    Thread.sleep(1000);
    editPolicy.editSchedule();
    Assert.assertNull(
        cs.getNode(rmNode3.getNodeID()).getReservedContainer());
    Assert.assertNotNull(
        cs.getNode(rmNode2.getNodeID()).getReservedContainer());
    Assert.assertEquals(am2.getApplicationAttemptId(), cs.getNode(
        rmNode2.getNodeID()).getReservedContainer().getApplicationAttemptId());

    // Do it again, we should see containers marked to be preempt
    editPolicy.editSchedule();
    Assert.assertEquals(2, editPolicy.getToPreemptContainers().size());

    // Call editSchedule again: selected containers are killed
    editPolicy.editSchedule();

    // Do allocation till reserved container allocated
    while (schedulerApp2.getLiveContainers().size() < 2) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
      Thread.sleep(200);
    }

    waitNumberOfLiveContainersFromApp(schedulerApp1, 1);

    rm1.close();
  }

  @Test(timeout = 60000)
  public void testPriorityPreemptionOnlyTriggeredWhenDemandingQueueUnsatisfied()
      throws Exception {
    /**
     * Test case: Submit two application (app1/app2) to different queues, queue
     * structure:
     *
     * <pre>
     *             Root
     *            /  |  \
     *           a   b   c
     *          10   20  70
     * </pre>
     *
     * 1) 10 nodes (n0-n9) in the cluster, each of them has 10G.
     *
     * 2) app1 submit to queue-b first, it asked 8 * 1G containers
     * We will allocate 1 container on each of n0-n10
     *
     * 3) app2 submit to queue-c, ask for 10 * 10G containers (including AM)
     *
     * After preemption, we should expect:
     * Preempt 7 containers from app1 and usage of app2 is 70%
     */
    conf.setPUOrderingPolicyUnderUtilizedPreemptionEnabled(true);
    conf.setPUOrderingPolicyUnderUtilizedPreemptionDelay(1000);
    conf.setQueueOrderingPolicy(CapacitySchedulerConfiguration.ROOT,
        CapacitySchedulerConfiguration.QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY);

    // Queue c has higher priority than a/b
    conf.setQueuePriority(CapacitySchedulerConfiguration.ROOT + ".c", 1);

    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    MockNM[] mockNMs = new MockNM[10];
    for (int i = 0; i < 10; i++) {
      mockNMs[i] = rm1.registerNode("h" + i + ":1234", 10 * GB);
    }

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    RMNode[] rmNodes = new RMNode[10];
    for (int i = 0; i < 10; i++) {
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(mockNMs[i].getNodeId());
    }

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, mockNMs[0]);

    am1.allocate("*", 1 * GB, 8, new ArrayList<>());

    // Do allocation for nm1-nm8
    for (int i = 1; i < 9; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    // App1 should have 9 containers now, so the abs-used-cap of b is 9%
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(9, schedulerApp1.getLiveContainers().size());
    for (int i = 0; i < 9; i++) {
      waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNodes[i].getNodeID()),
          am1.getApplicationAttemptId(), 1);
    }

    // Submit app2 to queue-c and asks for a 10G container for AM
    // Launch AM in NM9
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(10 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, mockNMs[9]);
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        ApplicationAttemptId.newInstance(app2.getApplicationId(), 1));

    // Ask 10 * 10GB containers
    am2.allocate("*", 10 * GB, 10, new ArrayList<>());

    // Do allocation for all nms
    for (int i = 1; i < 10; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    // Check am2 reserved resource from nm1-nm9
    for (int i = 1; i < 9; i++) {
      Assert.assertNotNull("Should reserve on nm-" + i,
          cs.getNode(rmNodes[i].getNodeID()).getReservedContainer());
    }

    // Sleep the timeout interval, we should be able to see 6 containers selected
    // 6 (selected) + 1 (allocated) which makes target capacity to 70%
    Thread.sleep(1000);

    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();
    editPolicy.editSchedule();
    checkNumberOfPreemptionCandidateFromApp(editPolicy, 6,
        am1.getApplicationAttemptId());

    // Call editSchedule again: selected containers are killed
    editPolicy.editSchedule();
    waitNumberOfLiveContainersFromApp(schedulerApp1, 3);

    // Do allocation for all nms
    for (int i = 1; i < 10; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    waitNumberOfLiveContainersFromApp(schedulerApp2, 7);
    waitNumberOfLiveContainersFromApp(schedulerApp1, 3);

    rm1.close();
  }

  @Test(timeout = 600000)
  public void testPriorityPreemptionFromHighestPriorityQueueAndOldestContainer()
      throws Exception {
    /**
     * Test case: Submit two application (app1/app2) to different queues, queue
     * structure:
     *
     * <pre>
     *             Root
     *            /  |  \
     *           a   b   c
     *          45  45  10
     * </pre>
     *
     * Priority of queue_a = 1
     * Priority of queue_b = 2
     *
     * 1) 5 nodes (n0-n4) in the cluster, each of them has 4G.
     *
     * 2) app1 submit to queue-c first (AM=1G), it asked 4 * 1G containers
     *    We will allocate 1 container on each of n0-n4. AM on n4.
     *
     * 3) app2 submit to queue-a, AM container=0.5G, allocated on n0
     *    Ask for 2 * 3.5G containers. (Reserved on n0/n1)
     *
     * 4) app2 submit to queue-b, AM container=0.5G, allocated on n2
     *    Ask for 2 * 3.5G containers. (Reserved on n2/n3)
     *
     * First we will preempt container on n2 since it is the oldest container of
     * Highest priority queue (b)
     */

    // A/B has higher priority
    conf.setQueuePriority(CapacitySchedulerConfiguration.ROOT + ".a" , 1);
    conf.setQueuePriority(CapacitySchedulerConfiguration.ROOT + ".b", 2);
    conf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".a", 45f);
    conf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".b", 45f);
    conf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".c", 10f);

    testPriorityPreemptionFromHighestPriorityQueueAndOldestContainer(new
        String[] {"a", "b", "c"}, new String[] {"user", "user", "user"});

  }

  protected void
  testPriorityPreemptionFromHighestPriorityQueueAndOldestContainer(String[]
      queues, String[] users) throws Exception {
    // Total preemption = 1G per round, which is 5% of cluster resource (20G)
    conf.setFloat(CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
        0.05f);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
    conf.setPUOrderingPolicyUnderUtilizedPreemptionEnabled(true);
    conf.setPUOrderingPolicyUnderUtilizedPreemptionDelay(1000);
    conf.setQueueOrderingPolicy(CapacitySchedulerConfiguration.ROOT,
        CapacitySchedulerConfiguration.QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY);

    MockRM rm1 = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();

    MockNM[] mockNMs = new MockNM[5];
    for (int i = 0; i < 5; i++) {
      mockNMs[i] = rm1.registerNode("h" + i + ":1234", 4 * GB);
    }

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    RMNode[] rmNodes = new RMNode[5];
    for (int i = 0; i < 5; i++) {
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(mockNMs[i].getNodeId());
    }

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser(users[2])
            .withAcls(null)
            .withQueue(queues[2])
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data2);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, mockNMs[4]);

    am1.allocate("*", 1 * GB, 4, new ArrayList<>());

    // Do allocation for nm1-nm8
    for (int i = 0; i < 4; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    // App1 should have 5 containers now, one for each node
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(5, schedulerApp1.getLiveContainers().size());
    for (int i = 0; i < 5; i++) {
      waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNodes[i].getNodeID()),
          am1.getApplicationAttemptId(), 1);
    }

    // Submit app2 to queue-a and asks for a 0.5G container for AM (on n0)
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(512, rm1)
            .withAppName("app")
            .withUser(users[0])
            .withAcls(null)
            .withQueue(queues[0])
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, mockNMs[0]);
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        ApplicationAttemptId.newInstance(app2.getApplicationId(), 1));

    // Ask 2 * 3.5GB containers
    am2.allocate("*", 3 * GB + 512, 2, new ArrayList<>());

    // Do allocation for n0-n1
    for (int i = 0; i < 2; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    // Check am2 reserved resource from nm0-nm1
    for (int i = 0; i < 2; i++) {
      Assert.assertNotNull("Should reserve on nm-" + i,
          cs.getNode(rmNodes[i].getNodeID()).getReservedContainer());
      Assert.assertEquals(cs.getNode(rmNodes[i].getNodeID())
          .getReservedContainer()
          .getQueueName(), cs.normalizeQueueName(queues[0]));
    }

    // Submit app3 to queue-b and asks for a 0.5G container for AM (on n2)
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(512, rm1)
            .withAppName("app")
            .withUser(users[1])
            .withAcls(null)
            .withQueue(queues[1])
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, mockNMs[2]);
    FiCaSchedulerApp schedulerApp3 = cs.getApplicationAttempt(
        ApplicationAttemptId.newInstance(app3.getApplicationId(), 1));

    // Ask 2 * 3.5GB containers
    am3.allocate("*", 3 * GB + 512, 2, new ArrayList<>());

    // Do allocation for n2-n3
    for (int i = 2; i < 4; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    // Check am2 reserved resource from nm2-nm3
    for (int i = 2; i < 4; i++) {
      Assert.assertNotNull("Should reserve on nm-" + i,
          cs.getNode(rmNodes[i].getNodeID()).getReservedContainer());
      Assert.assertEquals(cs.getNode(rmNodes[i].getNodeID())
          .getReservedContainer()
          .getQueueName(), cs.normalizeQueueName(queues[1]));
    }

    // Sleep the timeout interval, we should be able to see 1 container selected
    Thread.sleep(1000);

    /* 1st container preempted is on n2 */
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();
    editPolicy.editSchedule();

    // We should have one to-preempt container, on node[2]
    Set<RMContainer> selectedToPreempt =
        editPolicy.getToPreemptContainers().keySet();
    Assert.assertEquals(1, selectedToPreempt.size());
    Assert.assertEquals(mockNMs[2].getNodeId(),
        selectedToPreempt.iterator().next().getAllocatedNode());

    // Call editSchedule again: selected containers are killed
    editPolicy.editSchedule();
    waitNumberOfLiveContainersFromApp(schedulerApp1, 4);

    // Make sure the container killed, then do allocation for all nms
    for (int i = 0; i < 4; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    waitNumberOfLiveContainersFromApp(schedulerApp1, 4);
    waitNumberOfLiveContainersFromApp(schedulerApp2, 1);
    waitNumberOfLiveContainersFromApp(schedulerApp3, 2);

    /* 2nd container preempted is on n3 */
    editPolicy.editSchedule();

    // We should have one to-preempt container, on node[3]
    selectedToPreempt =
        editPolicy.getToPreemptContainers().keySet();
    Assert.assertEquals(1, selectedToPreempt.size());
    Assert.assertEquals(mockNMs[3].getNodeId(),
        selectedToPreempt.iterator().next().getAllocatedNode());

    // Call editSchedule again: selected containers are killed
    editPolicy.editSchedule();
    waitNumberOfLiveContainersFromApp(schedulerApp1, 3);

    // Do allocation for all nms
    for (int i = 0; i < 4; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    waitNumberOfLiveContainersFromApp(schedulerApp1, 3);
    waitNumberOfLiveContainersFromApp(schedulerApp2, 1);
    waitNumberOfLiveContainersFromApp(schedulerApp3, 3);

    /* 3rd container preempted is on n0 */
    editPolicy.editSchedule();

    // We should have one to-preempt container, on node[0]
    selectedToPreempt =
        editPolicy.getToPreemptContainers().keySet();
    Assert.assertEquals(1, selectedToPreempt.size());
    Assert.assertEquals(mockNMs[0].getNodeId(),
        selectedToPreempt.iterator().next().getAllocatedNode());

    // Call editSchedule again: selected containers are killed
    editPolicy.editSchedule();
    waitNumberOfLiveContainersFromApp(schedulerApp1, 2);

    // Do allocation for all nms
    for (int i = 0; i < 4; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    waitNumberOfLiveContainersFromApp(schedulerApp1, 2);
    waitNumberOfLiveContainersFromApp(schedulerApp2, 2);
    waitNumberOfLiveContainersFromApp(schedulerApp3, 3);

    /* 4th container preempted is on n1 */
    editPolicy.editSchedule();

    // We should have one to-preempt container, on node[0]
    selectedToPreempt =
        editPolicy.getToPreemptContainers().keySet();
    Assert.assertEquals(1, selectedToPreempt.size());
    Assert.assertEquals(mockNMs[1].getNodeId(),
        selectedToPreempt.iterator().next().getAllocatedNode());

    // Call editSchedule again: selected containers are killed
    editPolicy.editSchedule();
    waitNumberOfLiveContainersFromApp(schedulerApp1, 1);

    // Do allocation for all nms
    for (int i = 0; i < 4; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    waitNumberOfLiveContainersFromApp(schedulerApp1, 1);
    waitNumberOfLiveContainersFromApp(schedulerApp2, 3);
    waitNumberOfLiveContainersFromApp(schedulerApp3, 3);

    rm1.close();
  }

  private void initializeConfProperties(CapacitySchedulerConfiguration conf)
      throws IOException {

    conf.setQueues("root", new String[] {"A", "B"});
    conf.setCapacity("root.A", 50);
    conf.setCapacity("root.B", 50);
    conf.setQueuePriority("root.A", 1);
    conf.setQueuePriority("root.B", 2);

    conf.set(PREFIX + "root.ordering-policy", "priority-utilization");
    conf.set(PREFIX + "ordering-policy.priority-utilization.underutilized-preemption.enabled", "true");
    conf.set(PREFIX + "ordering-policy.priority-utilization.underutilized-preemption.allow-move-reservation", "false");
    conf.set(PREFIX + "ordering-policy.priority-utilization.underutilized-preemption.reserved-container-delay-ms", "0");
    conf.set(PREFIX + "root.accessible-node-labels.x.capacity", "100");

    // Setup queue access to node labels
    conf.set(PREFIX + "root.A.accessible-node-labels", "x");
    conf.set(PREFIX + "root.B.accessible-node-labels", "x");
    conf.set(PREFIX + "root.A.default-node-label-expression", "x");
    conf.set(PREFIX + "root.B.default-node-label-expression", "x");
    conf.set(PREFIX + "root.A.accessible-node-labels.x.capacity", "50");
    conf.set(PREFIX + "root.B.accessible-node-labels.x.capacity", "50");
    conf.set(PREFIX + "root.A.user-limit-factor", "100");
    conf.set(PREFIX + "root.B.user-limit-factor", "100");
    conf.set(PREFIX + "maximum-am-resource-percent", "1");

    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.set(YarnConfiguration.RM_AM_MAX_ATTEMPTS, "1");
    conf.set(CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL, "1000");
    conf.set(CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL, "1000");
    conf.set(CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND, "0.5");
    conf.set(CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR, "1");

  }

  @Test
  public void testPriorityPreemptionWithNodeLabels() throws Exception {
    // set up queue priority and capacity
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();

    initializeConfProperties(conf);

    MockRM rm1 = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();

    MockNM[] mockNMs = new MockNM[NUM_NM];
    for (int i = 0; i < NUM_NM; i++) {
      mockNMs[i] = rm1.registerNode("h" + i + ":1234", 6144);
    }

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("x")));

    RMNode[] rmNodes = new RMNode[5];
    for (int i = 0; i < NUM_NM; i++) {
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(mockNMs[i].getNodeId());
      mgr.replaceLabelsOnNode(
          ImmutableMap.of(rmNodes[i].getNodeID(), ImmutableSet.of("x")));
    }

    // launch an app to queue B, AM container launched in nm4
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(4096, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("B")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data2);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, mockNMs[4]);

    am1.allocate("*", 4096, NUM_NM-1, new ArrayList<>());

    // Do allocation for nm0-nm3
    for (int i = 0; i < NUM_NM-1; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    // App1 should have 5 containers now, one for each node
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(NUM_NM, schedulerApp1.getLiveContainers().size());
    for (int i = 0; i < NUM_NM; i++) {
      waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(
          rmNodes[i].getNodeID()), am1.getApplicationAttemptId(), 1);
    }

    // Submit app2 to queue A and asks for a 750MB container for AM (on n0)
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("A")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, mockNMs[0]);
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        ApplicationAttemptId.newInstance(app2.getApplicationId(), 1));

    // Ask NUM_NM-1 * 1500MB containers
    am2.allocate("*", 2048, NUM_NM-1, new ArrayList<>());

    // Do allocation for n1-n4
    for (int i = 1; i < NUM_NM; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    // kill app1
    rm1.killApp(app1.getApplicationId());

    // Submit app3 to queue B and asks for a 5000MB container for AM (on n2)
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("B")
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, mockNMs[2]);
    FiCaSchedulerApp schedulerApp3 = cs.getApplicationAttempt(
        ApplicationAttemptId.newInstance(app3.getApplicationId(), 1));

    // Ask NUM_NM * 5000MB containers
    am3.allocate("*", 5120, NUM_NM, new ArrayList<>());

    // Do allocation for n0-n4
    for (int i = 0; i < NUM_NM; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    // Sleep the timeout interval, we should see 2 containers selected
    Thread.sleep(1000);

    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();
    editPolicy.editSchedule();

    // We should only allow to preempt 2 containers, on node1 and node2
    Set<RMContainer> selectedToPreempt =
        editPolicy.getToPreemptContainers().keySet();
    Assert.assertEquals(2, selectedToPreempt.size());
    List<NodeId> selectedToPreemptNodeIds = new ArrayList<>();
    for (RMContainer rmc : selectedToPreempt) {
      selectedToPreemptNodeIds.add(rmc.getAllocatedNode());
    }
    assertThat(selectedToPreemptNodeIds, CoreMatchers.hasItems(
        mockNMs[1].getNodeId(), mockNMs[2].getNodeId()));

    rm1.close();

  }

  @Test(timeout = 60000)
  public void testPreemptionForFragmentatedCluster() throws Exception {
    // Set additional_balance_queue_based_on_reserved_res to true to get
    // additional preemptions.
    conf.setBoolean(
        CapacitySchedulerConfiguration.ADDITIONAL_RESOURCE_BALANCE_BASED_ON_RESERVED_CONTAINERS,
        true);

    /**
     * Two queues, a/b, each of them are 50/50
     * 5 nodes in the cluster, each of them is 30G.
     *
     * Submit first app, AM = 3G, and 4 * 21G containers.
     * Submit second app, AM = 3G, and 4 * 21G containers,
     *
     * We can get one container preempted from 1st app.
     */
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        this.conf);
    conf.setLong(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        1024 * 21);
    conf.setQueues("root", new String[] { "a", "b" });
    conf.setCapacity("root.a", 50);
    conf.setUserLimitFactor("root.a", 100);
    conf.setCapacity("root.b", 50);
    conf.setUserLimitFactor("root.b", 100);
    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    List<MockNM> nms = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      nms.add(rm1.registerNode("h" + i + ":1234", 30 * GB));
    }

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(3 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms.get(0));

    am1.allocate("*", 21 * GB, 4, new ArrayList<ContainerId>());

    // Do allocation for all nodes
    for (int i = 0; i < 10; i++) {
      MockNM mockNM = nms.get(i % nms.size());
      RMNode rmNode = cs.getRMContext().getRMNodes().get(mockNM.getNodeId());
      cs.handle(new NodeUpdateSchedulerEvent(rmNode));
    }

    // App1 should have 5 containers now
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(5, schedulerApp1.getLiveContainers().size());

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(3 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nms.get(2));

    am2.allocate("*", 21 * GB, 4, new ArrayList<ContainerId>());

    // Do allocation for all nodes
    for (int i = 0; i < 10; i++) {
      MockNM mockNM = nms.get(i % nms.size());
      RMNode rmNode = cs.getRMContext().getRMNodes().get(mockNM.getNodeId());
      cs.handle(new NodeUpdateSchedulerEvent(rmNode));
    }

    // App2 should have 2 containers now
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        am2.getApplicationAttemptId());
    Assert.assertEquals(2, schedulerApp2.getLiveContainers().size());

    waitNumberOfReservedContainersFromApp(schedulerApp2, 1);

    // Call editSchedule twice and allocation once, container should get allocated
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();
    editPolicy.editSchedule();
    editPolicy.editSchedule();

    int tick = 0;
    while (schedulerApp2.getLiveContainers().size() != 4 && tick < 10) {
      // Do allocation for all nodes
      for (int i = 0; i < 10; i++) {
        MockNM mockNM = nms.get(i % nms.size());
        RMNode rmNode = cs.getRMContext().getRMNodes().get(mockNM.getNodeId());
        cs.handle(new NodeUpdateSchedulerEvent(rmNode));
      }
      tick++;
      Thread.sleep(100);
    }
    Assert.assertEquals(3, schedulerApp2.getLiveContainers().size());

    rm1.close();
  }

  @Test(timeout = 600000)
  public void testPreemptionToBalanceWithCustomTimeout() throws Exception {
    /**
     * Test case: Submit two application (app1/app2) to different queues, queue
     * structure:
     *
     * <pre>
     *             Root
     *            /  |  \
     *           a   b   c
     *          10   20  70
     * </pre>
     *
     * 1) Two nodes (n1/n2) in the cluster, each of them has 20G.
     *
     * 2) app1 submit to queue-b, asks for 1G * 5
     *
     * 3) app2 submit to queue-c, ask for one 4G container (for AM)
     *
     * After preemption, we should expect:
     * 1. Preempt 4 containers from app1
     * 2. the selected containers will be killed after configured timeout.
     * 3. AM of app2 successfully allocated.
     */
    conf.setBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED,
        true);
    conf.setLong(
        CapacitySchedulerConfiguration.MAX_WAIT_BEFORE_KILL_FOR_QUEUE_BALANCE_PREEMPTION,
        20*1000);
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        this.conf);

    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    MockNM nm1 = rm1.registerNode("h1:1234", 20 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 20 * GB);
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    am1.allocate("*", 1 * GB, 38, new ArrayList<ContainerId>());

    // Do allocation for node1/node2
    for (int i = 0; i < 38; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    }

    // App1 should have 39 containers now
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(39, schedulerApp1.getLiveContainers().size());
    // 20 from n1 and 19 from n2
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode1.getNodeID()),
        am1.getApplicationAttemptId(), 20);
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode2.getNodeID()),
        am1.getApplicationAttemptId(), 19);


    // Submit app2 to queue-c and asks for a 4G container for AM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(4 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        ApplicationAttemptId.newInstance(app2.getApplicationId(), 1));

    // Call editSchedule: containers are selected to be preemption candidate
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();
    editPolicy.editSchedule();
    Assert.assertEquals(4, editPolicy.getToPreemptContainers().size());

    // check live containers immediately, nothing happen
    Assert.assertEquals(39, schedulerApp1.getLiveContainers().size());

    Thread.sleep(20*1000);
    // Call editSchedule again: selected containers are killed
    editPolicy.editSchedule();
    waitNumberOfLiveContainersFromApp(schedulerApp1, 35);

    // Call allocation, containers are reserved
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    waitNumberOfReservedContainersFromApp(schedulerApp2, 1);

    // Call editSchedule twice and allocation once, container should get allocated
    editPolicy.editSchedule();
    editPolicy.editSchedule();

    int tick = 0;
    while (schedulerApp2.getLiveContainers().size() != 1 && tick < 10) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
      tick++;
      Thread.sleep(100);
    }
    waitNumberOfReservedContainersFromApp(schedulerApp2, 0);

    rm1.close();


  }


}
