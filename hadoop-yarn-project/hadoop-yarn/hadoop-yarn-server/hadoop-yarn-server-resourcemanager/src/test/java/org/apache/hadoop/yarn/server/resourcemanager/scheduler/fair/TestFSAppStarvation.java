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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileWriter;
import org.apache.hadoop.yarn.util.ControlledClock;

import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Test class to verify identification of app starvation.
 */
public class TestFSAppStarvation extends FairSchedulerTestBase {

  private static final File ALLOC_FILE = new File(TEST_DIR, "test-QUEUES");

  private final ControlledClock clock = new ControlledClock();

  // Node Capacity = NODE_CAPACITY_MULTIPLE * (1 GB or 1 vcore)
  private static final int NODE_CAPACITY_MULTIPLE = 4;
  private static final String[] QUEUES =
      {"no-preemption", "minshare", "fairshare.child", "drf.child"};

  private FairSchedulerWithMockPreemption.MockPreemptionThread preemptionThread;

  @Before
  public void setup() {
    createConfiguration();
    conf.set(YarnConfiguration.RM_SCHEDULER,
        FairSchedulerWithMockPreemption.class.getCanonicalName());
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        ALLOC_FILE.getAbsolutePath());
    conf.setBoolean(FairSchedulerConfiguration.PREEMPTION, true);
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD, 0f);
    // This effectively disables the update thread since we call update
    // explicitly on the main thread
    conf.setLong(FairSchedulerConfiguration.UPDATE_INTERVAL_MS, Long.MAX_VALUE);
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

  /*
   * Test to verify application starvation is computed only when preemption
   * is enabled.
   */
  @Test
  public void testPreemptionDisabled() throws Exception {
    conf.setBoolean(FairSchedulerConfiguration.PREEMPTION, false);

    setupClusterAndSubmitJobs();

    assertNull("Found starved apps even when preemption is turned off",
        scheduler.getContext().getStarvedApps());
  }

  /*
   * Test to verify application starvation is computed correctly when
   * preemption is turned on.
   */
  @Test
  public void testPreemptionEnabled() throws Exception {
    setupClusterAndSubmitJobs();

    // Wait for apps to be processed by MockPreemptionThread
    for (int i = 0; i < 6000; ++i) {
      if (preemptionThread.uniqueAppsAdded() >= 3) {
        break;
      }
      Thread.sleep(10);
    }

    assertNotNull("FSContext does not have an FSStarvedApps instance",
        scheduler.getContext().getStarvedApps());
    assertEquals("Expecting 3 starved applications, one each for the "
            + "minshare and fairshare queues",
        3, preemptionThread.uniqueAppsAdded());

    // Verify apps are added again only after the set delay for starvation has
    // passed.
    clock.tickSec(1);
    scheduler.update();
    assertEquals("Apps re-added even before starvation delay passed",
        preemptionThread.totalAppsAdded(), preemptionThread.uniqueAppsAdded());
    verifyLeafQueueStarvation();

    clock.tickMsec(
        FairSchedulerWithMockPreemption.DELAY_FOR_NEXT_STARVATION_CHECK_MS);
    scheduler.update();

    // Wait for apps to be processed by MockPreemptionThread
    for (int i = 0; i < 6000; ++i) {
      if(preemptionThread.totalAppsAdded() >=
          preemptionThread.uniqueAppsAdded() * 2) {
        break;
      }
      Thread.sleep(10);
    }

    assertEquals("Each app should be marked as starved once" +
            " at each scheduler update above",
        preemptionThread.totalAppsAdded(),
        preemptionThread.uniqueAppsAdded() * 2);
  }

  /*
   * Test to verify app starvation is computed only when the cluster
   * utilization threshold is over the preemption threshold.
   */
  @Test
  public void testClusterUtilizationThreshold() throws Exception {
    // Set preemption threshold to 1.1, so the utilization is always lower
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD, 1.1f);

    setupClusterAndSubmitJobs();

    assertNotNull("FSContext does not have an FSStarvedApps instance",
        scheduler.getContext().getStarvedApps());
    assertEquals("Found starved apps when preemption threshold is over 100%", 0,
        preemptionThread.totalAppsAdded());
  }

  private void verifyLeafQueueStarvation() {
    for (String q : QUEUES) {
      if (!q.equals("no-preemption")) {
        boolean isStarved =
            scheduler.getQueueManager().getLeafQueue(q, false).isStarved();
        assertTrue(isStarved);
      }
    }
  }

  private void setupClusterAndSubmitJobs() throws Exception {
    setupStarvedCluster();
    submitAppsToEachLeafQueue();
    sendEnoughNodeUpdatesToAssignFully();

    // Sleep to hit the preemption timeouts
    clock.tickMsec(10);

    // Scheduler update to populate starved apps
    scheduler.update();
  }

  /**
   * Setup the cluster for starvation testing:
   * 1. Create FS allocation file
   * 2. Create and start MockRM
   * 3. Add two nodes to the cluster
   * 4. Submit an app that uses up all resources on the cluster
   */
  private void setupStarvedCluster() {
    AllocationFileWriter.create()
        .drfDefaultQueueSchedulingPolicy()
        // Default queue
        .addQueue(new AllocationFileQueue.Builder("default").build())
        // Queue with preemption disabled
        .addQueue(new AllocationFileQueue.Builder("no-preemption")
            .fairSharePreemptionThreshold(0).build())
        // Queue with minshare preemption enabled
        .addQueue(new AllocationFileQueue.Builder("minshare")
            .fairSharePreemptionThreshold(0)
            .minSharePreemptionTimeout(0)
            .minResources("2048mb,2vcores")
            .build())
        // FAIR queue with fairshare preemption enabled
        .addQueue(new AllocationFileQueue.Builder("fairshare")
            .fairSharePreemptionThreshold(1)
            .fairSharePreemptionTimeout(0)
            .schedulingPolicy("fair")
            .subQueue(new AllocationFileQueue.Builder("child")
                .fairSharePreemptionThreshold(1)
                .fairSharePreemptionTimeout(0)
                .schedulingPolicy("fair").build())
            .build())
        // DRF queue with fairshare preemption enabled
        .addQueue(new AllocationFileQueue.Builder("drf")
            .fairSharePreemptionThreshold(1)
            .fairSharePreemptionTimeout(0)
            .schedulingPolicy("drf")
            .subQueue(new AllocationFileQueue.Builder("child")
                .fairSharePreemptionThreshold(1)
                .fairSharePreemptionTimeout(0)
                .schedulingPolicy("drf").build())
            .build())
        .writeToFile(ALLOC_FILE.getAbsolutePath());

    assertTrue("Allocation file does not exist, not running the test",
        ALLOC_FILE.exists());

    resourceManager = new MockRM(conf);
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
    scheduler.setClock(clock);
    resourceManager.start();
    preemptionThread = (FairSchedulerWithMockPreemption.MockPreemptionThread)
        scheduler.preemptionThread;

    // Create and add two nodes to the cluster
    addNode(NODE_CAPACITY_MULTIPLE * 1024, NODE_CAPACITY_MULTIPLE);
    addNode(NODE_CAPACITY_MULTIPLE * 1024, NODE_CAPACITY_MULTIPLE);

    // Create an app that takes up all the resources on the cluster
    ApplicationAttemptId app
        = createSchedulingRequest(1024, 1, "root.default", "default", 8);

    scheduler.update();
    sendEnoughNodeUpdatesToAssignFully();

    assertEquals(8, scheduler.getSchedulerApp(app).getLiveContainers().size());
  }

  private void submitAppsToEachLeafQueue() {
    for (String queue : QUEUES) {
      createSchedulingRequest(1024, 1, "root." + queue, "user", 1);
    }
    scheduler.update();
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
}
