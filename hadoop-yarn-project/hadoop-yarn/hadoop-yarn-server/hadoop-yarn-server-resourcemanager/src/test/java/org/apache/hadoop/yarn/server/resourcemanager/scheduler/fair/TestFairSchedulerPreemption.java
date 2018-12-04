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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Tests to verify fairshare and minshare preemption, using parameterization.
 */
@RunWith(Parameterized.class)
public class TestFairSchedulerPreemption extends FairSchedulerTestBase {
  private static final File ALLOC_FILE = new File(TEST_DIR, "test-queues");
  private static final int GB = 1024;

  // Scheduler clock
  private final ControlledClock clock = new ControlledClock();

  // Node Capacity = NODE_CAPACITY_MULTIPLE * (1 GB or 1 vcore)
  private static final int NODE_CAPACITY_MULTIPLE = 4;

  private final boolean fairsharePreemption;
  private final boolean drf;

  // App that takes up the entire cluster
  private FSAppAttempt greedyApp;

  // Starving app that is expected to instigate preemption
  private FSAppAttempt starvingApp;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> getParameters() {
    return Arrays.asList(new Object[][] {
        {"MinSharePreemption", 0},
        {"MinSharePreemptionWithDRF", 1},
        {"FairSharePreemption", 2},
        {"FairSharePreemptionWithDRF", 3}
    });
  }

  public TestFairSchedulerPreemption(String name, int mode)
      throws IOException {
    fairsharePreemption = (mode > 1); // 2 and 3
    drf = (mode % 2 == 1); // 1 and 3
    writeAllocFile();
  }

  @Before
  public void setup() throws IOException {
    createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        ALLOC_FILE.getAbsolutePath());
    conf.setBoolean(FairSchedulerConfiguration.PREEMPTION, true);
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD, 0f);
    conf.setInt(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 0);
    setupCluster();
  }

  @After
  public void teardown() {
    ALLOC_FILE.delete();
    conf = null;
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
  }

  private void writeAllocFile() throws IOException {
    /*
     * Queue hierarchy:
     * root
     * |--- preemptable
     *      |--- child-1
     *      |--- child-2
     * |--- preemptable-sibling
     * |--- nonpreemptible
     *      |--- child-1
     *      |--- child-2
     */
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");

    out.println("<queue name=\"preemptable\">");
    writePreemptionParams(out);

    // Child-1
    out.println("<queue name=\"child-1\">");
    writeResourceParams(out);
    out.println("</queue>");

    // Child-2
    out.println("<queue name=\"child-2\">");
    writeResourceParams(out);
    out.println("</queue>");

    out.println("</queue>"); // end of preemptable queue

    out.println("<queue name=\"preemptable-sibling\">");
    writePreemptionParams(out);
    out.println("</queue>");

    // Queue with preemption disallowed
    out.println("<queue name=\"nonpreemptable\">");
    out.println("<allowPreemptionFrom>false" +
        "</allowPreemptionFrom>");
    writePreemptionParams(out);

    // Child-1
    out.println("<queue name=\"child-1\">");
    writeResourceParams(out);
    out.println("</queue>");

    // Child-2
    out.println("<queue name=\"child-2\">");
    writeResourceParams(out);
    out.println("</queue>");

    out.println("</queue>"); // end of nonpreemptable queue

    if (drf) {
      out.println("<defaultQueueSchedulingPolicy>drf" +
          "</defaultQueueSchedulingPolicy>");
    }
    out.println("</allocations>");
    out.close();

    assertTrue("Allocation file does not exist, not running the test",
        ALLOC_FILE.exists());
  }

  private void writePreemptionParams(PrintWriter out) {
    if (fairsharePreemption) {
      out.println("<fairSharePreemptionThreshold>1" +
          "</fairSharePreemptionThreshold>");
      out.println("<fairSharePreemptionTimeout>0" +
          "</fairSharePreemptionTimeout>");
    } else {
      out.println("<minSharePreemptionTimeout>0" +
          "</minSharePreemptionTimeout>");
    }
  }

  private void writeResourceParams(PrintWriter out) {
    if (!fairsharePreemption) {
      out.println("<minResources>4096mb,4vcores</minResources>");
    }
  }

  private void setupCluster() throws IOException {
    resourceManager = new MockRM(conf);
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
    // YARN-6249, FSLeafQueue#lastTimeAtMinShare is initialized to the time in
    // the real world, so we should keep the clock up with it.
    clock.setTime(SystemClock.getInstance().getTime());
    scheduler.setClock(clock);
    resourceManager.start();

    // Create and add two nodes to the cluster, with capacities
    // disproportional to the container requests.
    addNode(NODE_CAPACITY_MULTIPLE * GB, 3 * NODE_CAPACITY_MULTIPLE);
    addNode(NODE_CAPACITY_MULTIPLE * GB, 3 * NODE_CAPACITY_MULTIPLE);

    // Reinitialize the scheduler so DRF policy picks up cluster capacity
    // TODO (YARN-6194): One shouldn't need to call this
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Verify if child-1 and child-2 are preemptable
    FSQueue child1 =
        scheduler.getQueueManager().getQueue("nonpreemptable.child-1");
    assertFalse(child1.isPreemptable());
    FSQueue child2 =
        scheduler.getQueueManager().getQueue("nonpreemptable.child-2");
    assertFalse(child2.isPreemptable());
  }

  private void sendEnoughNodeUpdatesToAssignFully() {
    for (RMNode node : rmNodes) {
      NodeUpdateSchedulerEvent nodeUpdateSchedulerEvent =
          new NodeUpdateSchedulerEvent(node);
      for (int i = 0; i < NODE_CAPACITY_MULTIPLE; i++) {
        scheduler.handle(nodeUpdateSchedulerEvent);
      }
    }
  }

  /**
   * Submit an application to a given queue and take over the entire cluster.
   *
   * @param queueName queue name
   */
  private void takeAllResources(String queueName) {
    // Create an app that takes up all the resources on the cluster
    ApplicationAttemptId appAttemptId
        = createSchedulingRequest(GB, 1, queueName, "default",
        NODE_CAPACITY_MULTIPLE * rmNodes.size());
    greedyApp = scheduler.getSchedulerApp(appAttemptId);
    scheduler.update();
    sendEnoughNodeUpdatesToAssignFully();
    assertEquals(8, greedyApp.getLiveContainers().size());
    // Verify preemptable for queue and app attempt
    assertTrue(
        scheduler.getQueueManager().getQueue(queueName).isPreemptable()
            == greedyApp.isPreemptable());
  }

  /**
   * Submit an application to a given queue and preempt half resources of the
   * cluster.
   *
   * @param queueName queue name
   * @throws InterruptedException
   *         if any thread has interrupted the current thread.
   */
  private void preemptHalfResources(String queueName)
      throws InterruptedException {
    ApplicationAttemptId appAttemptId
        = createSchedulingRequest(2 * GB, 2, queueName, "default",
        NODE_CAPACITY_MULTIPLE * rmNodes.size() / 2);
    starvingApp = scheduler.getSchedulerApp(appAttemptId);

    // Move clock enough to identify starvation
    clock.tickSec(1);
    scheduler.update();
  }

  /**
   * Submit application to {@code queue1} and take over the entire cluster.
   * Submit application with larger containers to {@code queue2} that
   * requires preemption from the first application.
   *
   * @param queue1 first queue
   * @param queue2 second queue
   * @throws InterruptedException if interrupted while waiting
   */
  private void submitApps(String queue1, String queue2)
      throws InterruptedException {
    takeAllResources(queue1);
    preemptHalfResources(queue2);
  }

  private void verifyPreemption(int numStarvedAppContainers,
                                int numGreedyAppContainers)
      throws InterruptedException {
    // Sleep long enough for four containers to be preempted.
    for (int i = 0; i < 1000; i++) {
      if (greedyApp.getLiveContainers().size() == numGreedyAppContainers) {
        break;
      }
      Thread.sleep(10);
    }

    // Post preemption, verify the greedyApp has the correct # of containers.
    assertEquals("Incorrect # of containers on the greedy app",
            numGreedyAppContainers, greedyApp.getLiveContainers().size());

    // Verify the queue metrics are set appropriately. The greedyApp started
    // with 8 1GB, 1vcore containers.
    assertEquals("Incorrect # of preempted containers in QueueMetrics",
        8 - numGreedyAppContainers,
        greedyApp.getQueue().getMetrics().getAggregatePreemptedContainers());

    // Verify the node is reserved for the starvingApp
    for (RMNode rmNode : rmNodes) {
      FSSchedulerNode node = (FSSchedulerNode)
          scheduler.getNodeTracker().getNode(rmNode.getNodeID());
      if (node.getContainersForPreemption().size() > 0) {
        assertTrue("node should be reserved for the starvingApp",
            node.getPreemptionList().keySet().contains(starvingApp));
      }
    }

    sendEnoughNodeUpdatesToAssignFully();

    // Verify the preempted containers are assigned to starvingApp
    assertEquals("Starved app is not assigned the right # of containers",
        numStarvedAppContainers, starvingApp.getLiveContainers().size());

    // Verify the node is not reserved for the starvingApp anymore
    for (RMNode rmNode : rmNodes) {
      FSSchedulerNode node = (FSSchedulerNode)
          scheduler.getNodeTracker().getNode(rmNode.getNodeID());
      if (node.getContainersForPreemption().size() > 0) {
        assertFalse(node.getPreemptionList().keySet().contains(starvingApp));
      }
    }
  }

  private void verifyNoPreemption() throws InterruptedException {
    // Sleep long enough to ensure not even one container is preempted.
    for (int i = 0; i < 100; i++) {
      if (greedyApp.getLiveContainers().size() != 8) {
        break;
      }
      Thread.sleep(10);
    }
    assertEquals(8, greedyApp.getLiveContainers().size());
  }

  @Test
  public void testPreemptionWithinSameLeafQueue() throws Exception {
    String queue = "root.preemptable.child-1";
    submitApps(queue, queue);
    if (fairsharePreemption) {
      verifyPreemption(2, 4);
    } else {
      verifyNoPreemption();
    }
  }

  @Test
  public void testPreemptionBetweenTwoSiblingLeafQueues() throws Exception {
    submitApps("root.preemptable.child-1", "root.preemptable.child-2");
    verifyPreemption(2, 4);
  }

  @Test
  public void testPreemptionBetweenNonSiblingQueues() throws Exception {
    submitApps("root.preemptable.child-1", "root.nonpreemptable.child-1");
    verifyPreemption(2, 4);
  }

  @Test
  public void testNoPreemptionFromDisallowedQueue() throws Exception {
    submitApps("root.nonpreemptable.child-1", "root.preemptable.child-1");
    verifyNoPreemption();
  }

  /**
   * Set the number of AM containers for each node.
   *
   * @param numAMContainersPerNode number of AM containers per node
   */
  private void setNumAMContainersPerNode(int numAMContainersPerNode) {
    List<FSSchedulerNode> potentialNodes =
        scheduler.getNodeTracker().getNodesByResourceName("*");
    for (FSSchedulerNode node: potentialNodes) {
      List<RMContainer> containers=
          node.getCopiedListOfRunningContainers();
      // Change the first numAMContainersPerNode out of 4 containers to
      // AM containers
      for (int i = 0; i < numAMContainersPerNode; i++) {
        ((RMContainerImpl) containers.get(i)).setAMContainer(true);
      }
    }
  }

  @Test
  public void testPreemptionSelectNonAMContainer() throws Exception {
    takeAllResources("root.preemptable.child-1");
    setNumAMContainersPerNode(2);
    preemptHalfResources("root.preemptable.child-2");

    verifyPreemption(2, 4);

    ArrayList<RMContainer> containers =
        (ArrayList<RMContainer>) starvingApp.getLiveContainers();
    String host0 = containers.get(0).getNodeId().getHost();
    String host1 = containers.get(1).getNodeId().getHost();
    // Each node provides two and only two non-AM containers to be preempted, so
    // the preemption happens on both nodes.
    assertTrue("Preempted containers should come from two different "
        + "nodes.", !host0.equals(host1));
  }

  @Test
  public void testAppNotPreemptedBelowFairShare() throws Exception {
    takeAllResources("root.preemptable.child-1");
    tryPreemptMoreThanFairShare("root.preemptable.child-2");
  }

  private void tryPreemptMoreThanFairShare(String queueName)
          throws InterruptedException {
    ApplicationAttemptId appAttemptId
            = createSchedulingRequest(3 * GB, 3, queueName, "default",
            NODE_CAPACITY_MULTIPLE * rmNodes.size() / 2);
    starvingApp = scheduler.getSchedulerApp(appAttemptId);

    verifyPreemption(1, 5);
  }

  @Test
  public void testPreemptionBetweenSiblingQueuesWithParentAtFairShare()
      throws InterruptedException {
    // Run this test only for fairshare preemption
    if (!fairsharePreemption) {
      return;
    }

    // Let one of the child queues take over the entire cluster
    takeAllResources("root.preemptable.child-1");

    // Submit a job so half the resources go to parent's sibling
    preemptHalfResources("root.preemptable-sibling");
    verifyPreemption(2, 4);

    // Submit a job to the child's sibling to force preemption from the child
    preemptHalfResources("root.preemptable.child-2");
    verifyPreemption(1, 2);
  }

  /* It tests the case that there is less-AM-container solution in the
   * remaining nodes.
   */
  @Test
  public void testRelaxLocalityPreemptionWithLessAMInRemainingNodes()
      throws Exception {
    takeAllResources("root.preemptable.child-1");
    RMNode node1 = rmNodes.get(0);
    setAllAMContainersOnNode(node1.getNodeID());
    ApplicationAttemptId greedyAppAttemptId =
        getGreedyAppAttemptIdOnNode(node1.getNodeID());
    updateRelaxLocalityRequestSchedule(node1, GB, 4);
    verifyRelaxLocalityPreemption(node1.getNodeID(), greedyAppAttemptId, 4);
  }

  /* It tests the case that there is no less-AM-container solution in the
   * remaining nodes.
   */
  @Test
  public void testRelaxLocalityPreemptionWithNoLessAMInRemainingNodes()
      throws Exception {
    takeAllResources("root.preemptable.child-1");
    RMNode node1 = rmNodes.get(0);
    setNumAMContainersOnNode(3, node1.getNodeID());
    RMNode node2 = rmNodes.get(1);
    setAllAMContainersOnNode(node2.getNodeID());
    ApplicationAttemptId greedyAppAttemptId =
        getGreedyAppAttemptIdOnNode(node2.getNodeID());
    updateRelaxLocalityRequestSchedule(node1, GB * 2, 1);
    verifyRelaxLocalityPreemption(node2.getNodeID(), greedyAppAttemptId, 6);
  }

  private void setAllAMContainersOnNode(NodeId nodeId) {
    setNumAMContainersOnNode(Integer.MAX_VALUE, nodeId);
  }

  private void setNumAMContainersOnNode(int num, NodeId nodeId) {
    int count = 0;
    SchedulerNode node = scheduler.getNodeTracker().getNode(nodeId);
    for (RMContainer container: node.getCopiedListOfRunningContainers()) {
      count++;
      if (count <= num) {
        ((RMContainerImpl) container).setAMContainer(true);
      } else {
        break;
      }
    }
  }

  private ApplicationAttemptId getGreedyAppAttemptIdOnNode(NodeId nodeId) {
    SchedulerNode node = scheduler.getNodeTracker().getNode(nodeId);
    return node.getCopiedListOfRunningContainers().get(0)
        .getApplicationAttemptId();
  }

  /*
   * Send the resource requests allowed relax locality to scheduler. The
   * params node/nodeMemory/numNodeContainers used for NODE_LOCAL request.
   */
  private void updateRelaxLocalityRequestSchedule(RMNode node, int nodeMemory,
      int numNodeContainers) {
    // Make the RACK_LOCAL and OFF_SWITCH requests big enough that they can't be
    // satisfied. This forces the RR that we consider for preemption to be the
    // NODE_LOCAL one.
    ResourceRequest nodeRequest = createResourceRequest(nodeMemory,
        node.getHostName(), 1, numNodeContainers, true);
    ResourceRequest rackRequest =
        createResourceRequest(GB * 10, node.getRackName(), 1, 1, true);
    ResourceRequest anyRequest =
        createResourceRequest(GB * 10, ResourceRequest.ANY, 1, 1, true);

    List<ResourceRequest> resourceRequests =
        Arrays.asList(nodeRequest, rackRequest, anyRequest);

    ApplicationAttemptId starvedAppAttemptId = createSchedulingRequest(
        "root.preemptable.child-2", "default", resourceRequests);
    starvingApp = scheduler.getSchedulerApp(starvedAppAttemptId);

    // Move clock enough to identify starvation
    clock.tickSec(1);
    scheduler.update();
  }

  private void verifyRelaxLocalityPreemption(NodeId notBePreemptedNodeId,
      ApplicationAttemptId greedyAttemptId, int numGreedyAppContainers)
      throws Exception {
    // Make sure 4 containers were preempted from the greedy app, but also that
    // none were preempted on our all-AM node, even though the NODE_LOCAL RR
    // asked for resources on it.

    // TODO (YARN-7655) The starved app should be allocated 4 containers.
    // It should be possible to modify the RRs such that this is true
    // after YARN-7903.
    verifyPreemption(0, numGreedyAppContainers);
    SchedulerNode node = scheduler.getNodeTracker()
        .getNode(notBePreemptedNodeId);
    for (RMContainer container : node.getCopiedListOfRunningContainers()) {
      assert(container.isAMContainer());
      assert(container.getApplicationAttemptId().equals(greedyAttemptId));
    }
  }

}
