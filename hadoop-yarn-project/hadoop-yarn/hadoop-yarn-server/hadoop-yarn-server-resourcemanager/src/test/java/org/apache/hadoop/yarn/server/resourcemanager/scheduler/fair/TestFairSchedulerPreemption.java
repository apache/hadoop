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
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;

/**
 * Tests to verify fairshare and minshare preemption, using parameterization.
 */
@RunWith(Parameterized.class)
public class TestFairSchedulerPreemption extends FairSchedulerTestBase {
  private static final File ALLOC_FILE = new File(TEST_DIR, "test-queues");

  // Node Capacity = NODE_CAPACITY_MULTIPLE * (1 GB or 1 vcore)
  private static final int NODE_CAPACITY_MULTIPLE = 4;

  private final boolean fairsharePreemption;

  // App that takes up the entire cluster
  private FSAppAttempt greedyApp;

  // Starving app that is expected to instigate preemption
  private FSAppAttempt starvingApp;

  @Parameterized.Parameters
  public static Collection<Boolean[]> getParameters() {
    return Arrays.asList(new Boolean[][] {
        {true}, {false}});
  }

  public TestFairSchedulerPreemption(Boolean fairshare) throws IOException {
    fairsharePreemption = fairshare;
    writeAllocFile();
  }

  @Before
  public void setup() {
    createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        ALLOC_FILE.getAbsolutePath());
    conf.setBoolean(FairSchedulerConfiguration.PREEMPTION, true);
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD, 0f);
    conf.setInt(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 0);
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
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();

    // Create and add two nodes to the cluster
    addNode(NODE_CAPACITY_MULTIPLE * 1024, NODE_CAPACITY_MULTIPLE);
    addNode(NODE_CAPACITY_MULTIPLE * 1024, NODE_CAPACITY_MULTIPLE);
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
    // Create an app that takes up all the resources on the cluster
    ApplicationAttemptId appAttemptId1
        = createSchedulingRequest(1024, 1, queue1, "default",
        NODE_CAPACITY_MULTIPLE * rmNodes.size());
    greedyApp = scheduler.getSchedulerApp(appAttemptId1);
    scheduler.update();
    sendEnoughNodeUpdatesToAssignFully();
    assertEquals(8, greedyApp.getLiveContainers().size());

    // Create an app that takes up all the resources on the cluster
    ApplicationAttemptId appAttemptId2
        = createSchedulingRequest(2048, 2, queue2, "default",
        NODE_CAPACITY_MULTIPLE * rmNodes.size() / 2);
    starvingApp = scheduler.getSchedulerApp(appAttemptId2);

    // Sleep long enough to pass
    Thread.sleep(10);

    scheduler.update();
  }

  private void verifyPreemption() throws InterruptedException {
    // Sleep long enough for four containers to be preempted. Note that the
    // starved app must be queued four times for containers to be preempted.
    for (int i = 0; i < 10000; i++) {
      if (greedyApp.getLiveContainers().size() == 4) {
        break;
      }
      Thread.sleep(10);
    }

    // Verify the right amount of containers are preempted from greedyApp
    assertEquals(4, greedyApp.getLiveContainers().size());

    sendEnoughNodeUpdatesToAssignFully();

    // Verify the preempted containers are assigned to starvingApp
    assertEquals(2, starvingApp.getLiveContainers().size());
  }

  private void verifyNoPreemption() throws InterruptedException {
    // Sleep long enough to ensure not even one container is preempted.
    for (int i = 0; i < 600; i++) {
      if (greedyApp.getLiveContainers().size() != 8) {
        break;
      }
      Thread.sleep(10);
    }
    assertEquals(8, greedyApp.getLiveContainers().size());
  }

  @Test
  public void testPreemptionWithinSameLeafQueue() throws Exception {
    setupCluster();
    String queue = "root.preemptable.child-1";
    submitApps(queue, queue);
    if (fairsharePreemption) {
      verifyPreemption();
    } else {
      verifyNoPreemption();
    }
  }

  @Test
  public void testPreemptionBetweenTwoSiblingLeafQueues() throws Exception {
    setupCluster();
    submitApps("root.preemptable.child-1", "root.preemptable.child-2");
    verifyPreemption();
  }

  @Test
  public void testPreemptionBetweenNonSiblingQueues() throws Exception {
    setupCluster();
    submitApps("root.preemptable.child-1", "root.nonpreemptable.child-1");
    verifyPreemption();
  }

  @Test
  public void testNoPreemptionFromDisallowedQueue() throws Exception {
    setupCluster();
    submitApps("root.nonpreemptable.child-1", "root.preemptable.child-1");
    verifyNoPreemption();
  }
}
