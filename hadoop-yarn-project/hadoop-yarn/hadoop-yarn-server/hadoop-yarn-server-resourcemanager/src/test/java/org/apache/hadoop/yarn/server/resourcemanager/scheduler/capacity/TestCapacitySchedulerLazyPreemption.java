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

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitorManager;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestCapacitySchedulerLazyPreemption
    extends CapacitySchedulerPreemptionTestBase {
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf.setBoolean(CapacitySchedulerConfiguration.LAZY_PREEMPTION_ENABLED,
        true);
  }

  @Test (timeout = 60000)
  public void testSimplePreemption() throws Exception {
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
     * 1) Two nodes in the cluster, each of them has 4G.
     *
     * 2) app1 submit to queue-a first, it asked 7 * 1G containers, so there's no
     * more resource available.
     *
     * 3) app2 submit to queue-c, ask for one 1G container (for AM)
     *
     * Now the cluster is fulfilled.
     *
     * 4) app2 asks for another 1G container, system will preempt one container
     * from app1, and app2 will receive the preempted container
     */
    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    MockNM nm1 = rm1.registerNode("h1:1234", 4 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 4 * GB);
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

    am1.allocate("*", 1 * GB, 7, new ArrayList<ContainerId>());

    // Do allocation 3 times for node1/node2
    for (int i = 0; i < 3; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    }

    // App1 should have 7 containers now, and no available resource for cluster
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(7, schedulerApp1.getLiveContainers().size());

    // Submit app2 to queue-c and asks for a 1G container for AM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    // NM1/NM2 has available resource = 0G
    Assert.assertEquals(0 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    Assert.assertEquals(0 * GB, cs.getNode(nm2.getNodeId())
        .getUnallocatedResource().getMemorySize());

    // AM asks for a 1 * GB container
    am2.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.newInstance(1), ResourceRequest.ANY,
            Resources.createResource(1 * GB), 1)), null);

    // Get edit policy and do one update
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();

    // Call edit schedule twice, and check if one container from app1 marked
    // to be "killable"
    editPolicy.editSchedule();
    editPolicy.editSchedule();

    PreemptionManager pm = cs.getPreemptionManager();
    Map<ContainerId, RMContainer> killableContainers =
        waitKillableContainersSize(
            pm, "root.a", RMNodeLabelsManager.NO_LABEL, 1);
    Assert.assertEquals(1, killableContainers.size());
    Assert.assertEquals(killableContainers.entrySet().iterator().next().getKey()
        .getApplicationAttemptId(), am1.getApplicationAttemptId());

    // Call CS.handle once to see if container preempted
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));

    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        am2.getApplicationAttemptId());

    // App1 has 6 containers, and app2 has 2 containers
    Assert.assertEquals(6, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(2, schedulerApp2.getLiveContainers().size());

    // Ensure preemption metrics were recored.
    Assert.assertEquals(
        "Number of preempted containers incorrectly recorded:", 1,
        cs.getQueue("a").getMetrics().getAggregatePreemptedContainers());
    Assert.assertEquals(
        "Number of preempted containers incorrectly recorded:", 1,
        cs.getRootQueue().getMetrics().getAggregatePreemptedContainers());

    rm1.close();
  }

  @Test (timeout = 60000)
  public void testPreemptionConsidersNodeLocalityDelay()
      throws Exception {
    /**
     * Test case: same as testSimplePreemption steps 1-3.
     *
     * Step 4: app2 asks for 1G container with locality specified, so it needs
     * to wait for missed-opportunity before get scheduled.
     * Check if system waits missed-opportunity before finish killable container
     */
    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 4 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 4 * GB);
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

    am1.allocate("*", 1 * GB, 6, new ArrayList<ContainerId>());

    // Do allocation 3 times for node1/node2
    for (int i = 0; i < 3; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    }

    // App1 should have 7 containers now, and no available resource for cluster
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(7, schedulerApp1.getLiveContainers().size());

    // Submit app2 to queue-c and asks for a 1G container for AM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    // NM1/NM2 has available resource = 0G
    Assert.assertEquals(0 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    Assert.assertEquals(0 * GB, cs.getNode(nm2.getNodeId())
        .getUnallocatedResource().getMemorySize());

    // AM asks for a 1 * GB container with unknown host and unknown rack
    am2.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.newInstance(1), ResourceRequest.ANY,
            Resources.createResource(1 * GB), 1), ResourceRequest
        .newInstance(Priority.newInstance(1), "unknownhost",
            Resources.createResource(1 * GB), 1), ResourceRequest
        .newInstance(Priority.newInstance(1), "/default-rack",
            Resources.createResource(1 * GB), 1)), null);

    // Get edit policy and do one update
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();

    // Call edit schedule twice, and check if one container from app1 marked
    // to be "killable"
    editPolicy.editSchedule();
    editPolicy.editSchedule();

    PreemptionManager pm = cs.getPreemptionManager();
    Map<ContainerId, RMContainer> killableContainers =
        waitKillableContainersSize(
            pm, "root.a", RMNodeLabelsManager.NO_LABEL, 1);
    Assert.assertEquals(killableContainers.entrySet().iterator().next().getKey()
        .getApplicationAttemptId(), am1.getApplicationAttemptId());

    // Call CS.handle once to see if container preempted
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));

    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        am2.getApplicationAttemptId());

    // App1 has 7 containers, and app2 has 1 containers (no container preempted)
    Assert.assertEquals(7, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());

    // Do allocation again, one container will be preempted
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));

    // App1 has 6 containers, and app2 has 2 containers (new container allocated)
    Assert.assertEquals(6, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(2, schedulerApp2.getLiveContainers().size());

    rm1.close();
  }

  @Test (timeout = 60000)
  public void testPreemptionConsidersHardNodeLocality()
      throws Exception {
    /**
     * Test case: same as testSimplePreemption steps 1-3.
     *
     * Step 4: app2 asks for 1G container with hard locality specified, and
     *         asked host is not existed
     * Confirm system doesn't preempt any container.
     */
    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 4 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 4 * GB);
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

    am1.allocate("*", 1 * GB, 6, new ArrayList<ContainerId>());

    // Do allocation 3 times for node1/node2
    for (int i = 0; i < 3; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }
    for (int i = 0; i < 3; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    }

    // App1 should have 7 containers now, and no available resource for cluster
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(7, schedulerApp1.getLiveContainers().size());

    // Submit app2 to queue-c and asks for a 1G container for AM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    // NM1/NM2 has available resource = 0G
    Assert.assertEquals(0 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    Assert.assertEquals(0 * GB, cs.getNode(nm2.getNodeId())
        .getUnallocatedResource().getMemorySize());

    // AM asks for a 1 * GB container for h3 with hard locality,
    // h3 doesn't exist in the cluster
    am2.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.newInstance(1), ResourceRequest.ANY,
            Resources.createResource(1 * GB), 1, true), ResourceRequest
        .newInstance(Priority.newInstance(1), "h3",
            Resources.createResource(1 * GB), 1, false), ResourceRequest
        .newInstance(Priority.newInstance(1), "/default-rack",
            Resources.createResource(1 * GB), 1, false)), null);

    // Get edit policy and do one update
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();

    // Call edit schedule twice, and check if one container from app1 marked
    // to be "killable"
    editPolicy.editSchedule();
    editPolicy.editSchedule();

    PreemptionManager pm = cs.getPreemptionManager();
    Map<ContainerId, RMContainer> killableContainers =
        waitKillableContainersSize(
            pm, "root.a", RMNodeLabelsManager.NO_LABEL, 1);
    Assert.assertEquals(killableContainers.entrySet().iterator().next().getKey()
        .getApplicationAttemptId(), am1.getApplicationAttemptId());

    // Call CS.handle once to see if container preempted
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));

    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        am2.getApplicationAttemptId());

    // App1 has 7 containers, and app2 has 1 containers (no container preempted)
    Assert.assertEquals(7, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());

    // Do allocation again, nothing will be preempted
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));

    // App1 has 7 containers, and app2 has 1 containers (no container allocated)
    Assert.assertEquals(7, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());

    rm1.close();
  }

  @Test (timeout = 60000)
  public void testPreemptionPolicyShouldRespectAlreadyMarkedKillableContainers()
      throws Exception {
    /**
     * Test case:
     * <pre>
     *             Root
     *            /  |  \
     *           a   b   c
     *          10   20  70
     * </pre>
     * Submit applications to two queues, one uses more than the other, so
     * preemption will happen.
     *
     * Check:
     * 1) Killable containers resources will be excluded from PCPP (no duplicated
     *    container added to killable list)
     * 2) When more resources need to be preempted, new containers will be selected
     *    and killable containers will be considered
     */
    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

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

    am1.allocate("*", 1 * GB, 6, new ArrayList<ContainerId>());

    // Do allocation 6 times for node1
    for (int i = 0; i < 6; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }

    // App1 should have 7 containers now, and no available resource for cluster
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(7, schedulerApp1.getLiveContainers().size());

    // Submit app2 to queue-c and asks for a 1G container for AM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    // NM1 has available resource = 0G
    Assert.assertEquals(0 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>());

    // Get edit policy and do one update
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();

    // Call edit schedule twice, and check if one container from app1 marked
    // to be "killable"
    editPolicy.editSchedule();
    editPolicy.editSchedule();

    PreemptionManager pm = cs.getPreemptionManager();
    waitKillableContainersSize(pm, "root.a", RMNodeLabelsManager.NO_LABEL, 1);

    // Check killable containers and to-be-preempted containers in edit policy
    Assert.assertEquals(0, editPolicy.getToPreemptContainers().size());

    // Run edit schedule again, confirm status doesn't changed
    editPolicy.editSchedule();
    Assert.assertEquals(0, editPolicy.getToPreemptContainers().size());

    // Save current to kill containers
    Set<ContainerId> previousKillableContainers = new HashSet<>(
        pm.getKillableContainersMap("a", RMNodeLabelsManager.NO_LABEL)
            .keySet());

    // Update request resource of c from 1 to 2, so we need to preempt
    // one more container
    am2.allocate("*", 1 * GB, 2, new ArrayList<ContainerId>());

    // Call editPolicy.editSchedule() once, we should have 1 container in to-preempt map
    // and 1 container in killable map
    editPolicy.editSchedule();
    Assert.assertEquals(1, editPolicy.getToPreemptContainers().size());

    // Call editPolicy.editSchedule() once more, we should have 2 containers killable map
    editPolicy.editSchedule();
    Assert.assertEquals(0, editPolicy.getToPreemptContainers().size());

    // Check if previous killable containers included by new killable containers
    Map<ContainerId, RMContainer> killableContainers =
        waitKillableContainersSize(
            pm, "root.a", RMNodeLabelsManager.NO_LABEL, 2);
    Assert.assertTrue(
        Sets.difference(previousKillableContainers, killableContainers.keySet())
            .isEmpty());
  }

  /*
   * Ignore this test now because it could be a premature optimization
   */
  @Ignore
  @Test (timeout = 60000)
  public void testPreemptionPolicyCleanupKillableContainersWhenNoPreemptionNeeded()
      throws Exception {
    /**
     * Test case:
     * <pre>
     *             Root
     *            /  |  \
     *           a   b   c
     *          10   20  70
     * </pre>
     * Submit applications to two queues, one uses more than the other, so
     * preemption will happen.
     *
     * Check:
     * 1) Containers will be marked to killable
     * 2) Cancel resource request
     * 3) Killable containers will be cancelled from policy and scheduler
     */
    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

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

    am1.allocate("*", 1 * GB, 6, new ArrayList<ContainerId>());

    // Do allocation 6 times for node1
    for (int i = 0; i < 6; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }

    // App1 should have 7 containers now, and no available resource for cluster
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(7, schedulerApp1.getLiveContainers().size());

    // Submit app2 to queue-c and asks for a 1G container for AM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    // NM1 has available resource = 0G
    Assert.assertEquals(0 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    am2.allocate("*", 3 * GB, 1, new ArrayList<ContainerId>());

    // Get edit policy and do one update
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();

    // Call edit schedule twice, and check if 3 container from app1 marked
    // to be "killable"
    editPolicy.editSchedule();
    editPolicy.editSchedule();

    PreemptionManager pm = cs.getPreemptionManager();
    waitKillableContainersSize(pm, "a", RMNodeLabelsManager.NO_LABEL, 3);

    // Change reqeust from 3G to 2G, now we can preempt one less container. (3->2)
    am2.allocate("*", 2 * GB, 1, new ArrayList<ContainerId>());
    editPolicy.editSchedule();
    Assert.assertEquals(0, editPolicy.getToPreemptContainers().size());
    waitKillableContainersSize(pm, "a", RMNodeLabelsManager.NO_LABEL, 2);

    // Call editSchedule once more to make sure still nothing happens
    editPolicy.editSchedule();
    Assert.assertEquals(0, editPolicy.getToPreemptContainers().size());
    waitKillableContainersSize(pm, "a", RMNodeLabelsManager.NO_LABEL, 2);
  }

  @Test (timeout = 60000)
  public void testPreemptionConsidersUserLimit()
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
     * Queue-c's user-limit-factor = 0.1, so single user cannot allocate >1 containers in queue-c
     *
     * 1) Two nodes in the cluster, each of them has 4G.
     *
     * 2) app1 submit to queue-a first, it asked 7 * 1G containers, so there's no
     * more resource available.
     *
     * 3) app2 submit to queue-c, ask for one 1G container (for AM)
     *
     * Now the cluster is fulfilled.
     *
     * 4) app2 asks for another 1G container, system will preempt one container
     * from app1, and app2 will receive the preempted container
     */
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
    csConf.setUserLimitFactor(CapacitySchedulerConfiguration.ROOT + ".c", 0.1f);
    MockRM rm1 = new MockRM(csConf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 4 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 4 * GB);
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

    am1.allocate("*", 1 * GB, 6, new ArrayList<ContainerId>());

    // Do allocation 3 times for node1/node2
    for (int i = 0; i < 3; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    }

    // App1 should have 7 containers now, and no available resource for cluster
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(7, schedulerApp1.getLiveContainers().size());

    // Submit app2 to queue-c and asks for a 1G container for AM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    // NM1/NM2 has available resource = 0G
    Assert.assertEquals(0 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    Assert.assertEquals(0 * GB, cs.getNode(nm2.getNodeId())
        .getUnallocatedResource().getMemorySize());

    // AM asks for a 1 * GB container
    am2.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.newInstance(1), ResourceRequest.ANY,
            Resources.createResource(1 * GB), 1)), null);

    // Get edit policy and do one update
    SchedulingMonitorManager smm = ((CapacityScheduler) rm1.
        getResourceScheduler()).getSchedulingMonitorManager();
    SchedulingMonitor smon = smm.getAvailableSchedulingMonitor();
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) smon.getSchedulingEditPolicy();

    // Call edit schedule twice, and check if no container from app1 marked
    // to be "killable"
    editPolicy.editSchedule();
    editPolicy.editSchedule();

    // No preemption happens
    PreemptionManager pm = cs.getPreemptionManager();
    Map<ContainerId, RMContainer> killableContainers =
        waitKillableContainersSize(pm, "a", RMNodeLabelsManager.NO_LABEL, 0);
    Assert.assertEquals(0, killableContainers.size());

    // Call CS.handle once to see if container preempted
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));

    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        am2.getApplicationAttemptId());

    // App1 has 7 containers, and app2 has 1 containers (nothing preempted)
    Assert.assertEquals(7, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());

    rm1.close();
  }

  private Map<ContainerId, RMContainer> waitKillableContainersSize(
      PreemptionManager pm, String queueName, String partition,
      int expectedSize) throws InterruptedException {
    Map<ContainerId, RMContainer> killableContainers =
        pm.getKillableContainersMap(queueName, partition);

    int wait = 0;
    // Wait for at most 5 sec (it should be super fast actually)
    while (expectedSize != killableContainers.size() && wait < 500) {
      killableContainers = pm.getKillableContainersMap(queueName, partition);
      Thread.sleep(10);
      wait++;
    }

    Assert.assertEquals(expectedSize, killableContainers.size());
    return killableContainers;
  }
}