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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class TestCapacitySchedulerSurgicalPreemption
    extends CapacitySchedulerPreemptionTestBase {

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
    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    MockNM nm1 = rm1.registerNode("h1:1234", 20 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 20 * GB);
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());

    // launch an app to queue, AM container should be launched in nm1
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "a");
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
    RMApp app2 = rm1.submitApp(1 * GB, "app", "user", null, "c");
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
    SchedulingEditPolicy editPolicy = getSchedulingEditPolicy(rm1);

    // Call edit schedule twice, and check if 4 containers from app1 at n1 killed
    editPolicy.editSchedule();
    editPolicy.editSchedule();

    waitNumberOfLiveContainersFromApp(schedulerApp1, 29);

    // 13 from n1 (4 preempted) and 16 from n2
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode1.getNodeID()),
        am1.getApplicationAttemptId(), 13);
    waitNumberOfLiveContainersOnNodeFromApp(cs.getNode(rmNode2.getNodeID()),
        am1.getApplicationAttemptId(), 16);

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
     * 2) app1 submit to queue-a first, it asked 38 * 1G containers
     * We will allocate 20 on n1 and 19 on n2.
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
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "a");
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
    RMApp app2 = rm1.submitApp(4 * GB, "app", "user", null, "c");
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        ApplicationAttemptId.newInstance(app2.getApplicationId(), 1));

    // Call editSchedule: containers are selected to be preemption candidate
    ProportionalCapacityPreemptionPolicy editPolicy =
        (ProportionalCapacityPreemptionPolicy) getSchedulingEditPolicy(rm1);
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
}
