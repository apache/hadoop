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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.NodeAttributeStore;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.TestResourceTrackerService.NullNodeAttributeStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generic tests for overcommitting resources. This needs to be instantiated
 * with a scheduler ({@link YarnConfiguration#RM_SCHEDULER}).
 *
 * If reducing the amount of resources leads to overcommitting (negative
 * available resources), the scheduler will select containers to make room.
 * <ul>
 * <li>If there is no timeout (&lt;0), it doesn't kill or preempt surplus
 * containers.</li>
 * <li>If the timeout is 0, it kills the surplus containers immediately.</li>
 * <li>If the timeout is larger than 0, it first asks the application to
 * preempt those containers and after the timeout passes, it kills the surplus
 * containers.</li>
 * </ul>
 */
public abstract class TestSchedulerOvercommit {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSchedulerOvercommit.class);

  /** 1 GB in MB. */
  protected final static int GB = 1024;

  /** We do scheduling and heart beat every 200ms. */
  protected static final int INTERVAL = 200;


  /** Mock Resource Manager. */
  private MockRM rm;
  /** Scheduler for the Mock Resource Manager.*/
  private ResourceScheduler scheduler;

  /** Node Manager running containers. */
  private MockNM nm;
  private NodeId nmId;

  /** Application to allocate containers. */
  private RMAppAttempt attempt;
  private MockAM am;

  /**
   * Setup the cluster with: an RM, a NM and an application for test.
   * @throws Exception If it cannot set up the cluster.
   */
  @Before
  public void setup() throws Exception {
    LOG.info("Setting up the test cluster...");

    // Start the Resource Manager
    Configuration conf = getConfiguration();
    rm = new MockRM(conf);
    rm.start();
    scheduler = rm.getResourceScheduler();

    // Add a Node Manager with 4GB
    nm = rm.registerNode("127.0.0.1:1234", 4 * GB);
    nmId = nm.getNodeId();

    // Start an AM with 2GB
    RMApp app = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm).build());
    nm.nodeHeartbeat(true);
    attempt = app.getCurrentAppAttempt();
    am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();

    // After allocation, used 2GB and remaining 2GB on the NM
    assertMemory(scheduler, nmId, 2 * GB, 2 * GB);
    nm.nodeHeartbeat(true);
  }

  /**
   * Get the configuration for the scheduler. This is used when setting up the
   * Resource Manager and should setup the scheduler (e.g., Capacity Scheduler
   * or Fair Scheduler). It needs to set the configuration with
   * {@link YarnConfiguration#RM_SCHEDULER}.
   * @return Configuration for the scheduler.
   */
  protected Configuration getConfiguration() {
    Configuration conf = new YarnConfiguration();

    // Prevent loading node attributes
    conf.setClass(YarnConfiguration.FS_NODE_ATTRIBUTE_STORE_IMPL_CLASS,
        NullNodeAttributeStore.class, NodeAttributeStore.class);

    return conf;
  }

  /**
   * Stops the default application and the RM (with the scheduler).
   * @throws Exception If it cannot stop the cluster.
   */
  @After
  public void cleanup() throws Exception {
    LOG.info("Cleaning up the test cluster...");

    if (am != null) {
      am.unregisterAppAttempt();
      am = null;
    }
    if (rm != null) {
      rm.drainEvents();
      rm.stop();
      rm = null;
    }
  }


  /**
   * Reducing the resources with no timeout should prevent new containers
   * but wait for the current ones without killing.
   */
  @Test
  public void testReduceNoTimeout() throws Exception {

    // New 2GB container should give 4 GB used (2+2) and 0 GB available
    Container c1 = createContainer(am, 2 * GB);
    assertMemory(scheduler, nmId, 4 * GB, 0);

    // Update node resource to 2 GB, so resource is over-consumed
    updateNodeResource(rm, nmId, 2 * GB, 2, -1);
    // The used resource should still be 4 GB and negative available resource
    waitMemory(scheduler, nmId, 4 * GB, -2 * GB, INTERVAL, 2 * 1000);
    // Check that the NM got the updated resources
    nm.nodeHeartbeat(true);
    assertEquals(2 * GB, nm.getCapability().getMemorySize());

    // Check that we did not get a preemption request
    assertNoPreemption(am.schedule().getPreemptionMessage());

    // Check container can complete successfully with resource over-commitment
    ContainerStatus containerStatus = BuilderUtils.newContainerStatus(
        c1.getId(), ContainerState.COMPLETE, "", 0, c1.getResource());
    nm.containerStatus(containerStatus);

    LOG.info("Waiting for container to be finished for app...");
    GenericTestUtils.waitFor(
        () -> attempt.getJustFinishedContainers().size() == 1,
        INTERVAL, 2 * 1000);
    assertEquals(1, am.schedule().getCompletedContainersStatuses().size());
    assertMemory(scheduler, nmId, 2 * GB, 0);

    // Verify no NPE is trigger in schedule after resource is updated
    am.addRequests(new String[] {"127.0.0.1", "127.0.0.2"}, 3 * GB, 1, 1);
    AllocateResponse allocResponse2 = am.schedule();
    assertTrue("Shouldn't have enough resource to allocate containers",
        allocResponse2.getAllocatedContainers().isEmpty());
    // Try 10 times as scheduling is an async process
    for (int i = 0; i < 10; i++) {
      Thread.sleep(INTERVAL);
      allocResponse2 = am.schedule();
      assertTrue("Shouldn't have enough resource to allocate containers",
          allocResponse2.getAllocatedContainers().isEmpty());
    }
  }

  /**
   * Changing resources multiples times without waiting for the
   * timeout.
   */
  @Test
  public void testChangeResourcesNoTimeout() throws Exception {
    waitMemory(scheduler, nmId, 2 * GB, 2 * GB, 100, 2 * 1000);

    updateNodeResource(rm, nmId, 5 * GB, 2, -1);
    waitMemory(scheduler, nmId, 2 * GB, 3 * GB, 100, 2 * 1000);

    updateNodeResource(rm, nmId, 0 * GB, 2, -1);
    waitMemory(scheduler, nmId, 2 * GB, -2 * GB, 100, 2 * 1000);

    updateNodeResource(rm, nmId, 4 * GB, 2, -1);
    waitMemory(scheduler, nmId, 2 * GB, 2 * GB, 100, 2 * 1000);

    // The application should still be running without issues.
    assertEquals(RMAppAttemptState.RUNNING, attempt.getState());
  }

  /**
   * Reducing the resources with 0 time out kills the container right away.
   */
  @Test
  public void testReduceKill() throws Exception {

    Container container = createContainer(am, 2 * GB);
    assertMemory(scheduler, nmId, 4 * GB, 0);

    // Reducing to 2GB should kill the container
    long t0 = Time.now();
    updateNodeResource(rm, nmId, 2 * GB, 2, 0);
    waitMemory(scheduler, nm, 2 * GB, 0 * GB, INTERVAL, 2 * INTERVAL);

    // Check that the new container was killed
    List<ContainerStatus> completedContainers =
        am.schedule().getCompletedContainersStatuses();
    assertEquals(1, completedContainers.size());
    ContainerStatus containerStatus = completedContainers.get(0);
    assertContainerKilled(container.getId(), containerStatus);

    // It should kill the containers right away
    assertTime(0, Time.now() - t0);
  }

  /**
   * Reducing the resources with a time out should first preempt and then kill.
   */
  @Test
  public void testReducePreemptAndKill() throws Exception {

    Container container = createContainer(am, 2 * GB);
    assertMemory(scheduler, nmId, 4 * GB, 0);

    // We give an overcommit time out of 2 seconds
    final int timeout = (int)TimeUnit.SECONDS.toMillis(2);

    // Reducing to 2GB should first preempt the container
    long t0 = Time.now();
    updateNodeResource(rm, nmId, 2 * GB, 2, timeout);
    waitMemory(scheduler, nm, 4 * GB, -2 * GB, INTERVAL, timeout);

    // wait until MARK_CONTAINER_FOR_PREEMPTION is handled
    rm.drainEvents();

    // We should receive a notification to preempt the container
    PreemptionMessage preemptMsg = am.schedule().getPreemptionMessage();
    assertPreemption(container.getId(), preemptMsg);

    // Wait until the container is killed
    waitMemory(scheduler, nm, 2 * GB, 0, INTERVAL, timeout + 2 * INTERVAL);

    // Check that the container was killed
    List<ContainerStatus> completedContainers =
        am.schedule().getCompletedContainersStatuses();
    assertEquals(1, completedContainers.size());
    ContainerStatus containerStatus = completedContainers.get(0);
    assertContainerKilled(container.getId(), containerStatus);

    // Check how long it took to kill the container
    assertTime(timeout, Time.now() - t0);
  }

  /**
   * Reducing the resources (with a time out) triggers a preemption message to
   * the AM right away. Then, increasing them again should prevent the killing
   * when the time out would have happened.
   */
  @Test
  public void testReducePreemptAndCancel() throws Exception {

    Container container = createContainer(am, 2 * GB);
    assertMemory(scheduler, nmId, 4 * GB, 0);

    // We give an overcommit time out of 1 seconds
    final int timeout = (int)TimeUnit.SECONDS.toMillis(1);

    // Reducing to 2GB should first preempt the container
    updateNodeResource(rm, nmId, 2 * GB, 2, timeout);
    waitMemory(scheduler, nm, 4 * GB, -2 * GB, INTERVAL, timeout);

    // wait until MARK_CONTAINER_FOR_PREEMPTION is handled
    rm.drainEvents();

    // We should receive a notification to preempt the container
    PreemptionMessage preemptMsg = am.schedule().getPreemptionMessage();
    assertPreemption(container.getId(), preemptMsg);

    // Increase the resources again
    updateNodeResource(rm, nmId, 4 * GB, 2, timeout);
    waitMemory(scheduler, nm, 4 * GB, 0, INTERVAL, timeout);

    long t0 = Time.now();
    while (Time.now() - t0 < TimeUnit.SECONDS.toMillis(2)) {
      nm.nodeHeartbeat(true);
      AllocateResponse allocation = am.schedule();
      assertNoPreemption(allocation.getPreemptionMessage());
      assertTrue(allocation.getCompletedContainersStatuses().isEmpty());
      Thread.sleep(INTERVAL);
    }

    // Check that the containers are still running
    assertMemory(scheduler, nmId, 4 * GB, 0);
    assertEquals(2, scheduler.getNodeReport(nmId).getNumContainers());
  }

  /**
   * Test the order we kill multiple containers.
   * It initially has: AM(2GB), C1(1GB), C2(1GB), AM2(2GB), and C3(2GB).
   * It should kill in this order: C3, C2, C1, AM2, and AM1.
   */
  @Test
  public void testKillMultipleContainers() throws Exception {

    updateNodeResource(rm, nmId, 8 * GB, 6, -1);
    waitMemory(scheduler, nmId, 2 * GB, 6 * GB, 200, 5 * 1000);

    // Start 2 containers with 1 GB each
    Container c1 = createContainer(am, 1 * GB);
    Container c2 = createContainer(am, 1 * GB);
    waitMemory(scheduler, nmId, 4 * GB, 4 * GB, 200, 5 * 1000);

    // Start an AM with 2GB
    RMApp app2 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm)
            .withAppName("app2")
            .withUser("user2")
            .build());
    nm.nodeHeartbeat(true);
    RMAppAttempt attempt2 = app2.getCurrentAppAttempt();
    MockAM am2 = rm.sendAMLaunched(attempt2.getAppAttemptId());
    am2.registerAppAttempt();
    waitMemory(scheduler, nm, 6 * GB, 2 * GB, 200, 5 * 1000);
    assertEquals(RMAppAttemptState.RUNNING, attempt2.getState());

    Container c3 = createContainer(am2, 2 * GB);
    waitMemory(scheduler, nm, 8 * GB, 0 * GB, 200, 5 * 1000);
    assertEquals(5, scheduler.getNodeReport(nmId).getNumContainers());

    // Reduce the resources to kill C3 and C2 (not AM2)
    updateNodeResource(rm, nmId, 5 * GB, 6, 0);
    waitMemory(scheduler, nm, 5 * GB, 0 * GB, 200, 5 * 1000);
    assertEquals(3, scheduler.getNodeReport(nmId).getNumContainers());

    List<ContainerStatus> completedContainers =
        am2.schedule().getCompletedContainersStatuses();
    assertEquals(1, completedContainers.size());
    ContainerStatus container3Status = completedContainers.get(0);
    assertContainerKilled(c3.getId(), container3Status);

    completedContainers = am.schedule().getCompletedContainersStatuses();
    assertEquals(1, completedContainers.size());
    ContainerStatus container2Status = completedContainers.get(0);
    assertContainerKilled(c2.getId(), container2Status);
    assertEquals(RMAppAttemptState.RUNNING, attempt.getState());
    assertEquals(RMAppAttemptState.RUNNING, attempt2.getState());

    // Reduce the resources to kill C1 (not AM2)
    updateNodeResource(rm, nmId, 4 * GB, 6, 0);
    waitMemory(scheduler, nm, 4 * GB, 0 * GB, 200, 5 * 1000);
    assertEquals(2, scheduler.getNodeReport(nmId).getNumContainers());
    completedContainers = am.schedule().getCompletedContainersStatuses();
    assertEquals(1, completedContainers.size());
    ContainerStatus container1Status = completedContainers.get(0);
    assertContainerKilled(c1.getId(), container1Status);
    assertEquals(RMAppAttemptState.RUNNING, attempt.getState());
    assertEquals(RMAppAttemptState.RUNNING, attempt2.getState());

    // Reduce the resources to kill AM2
    updateNodeResource(rm, nmId, 2 * GB, 6, 0);
    waitMemory(scheduler, nm, 2 * GB, 0 * GB, 200, 5 * 1000);
    assertEquals(1, scheduler.getNodeReport(nmId).getNumContainers());
    assertEquals(RMAppAttemptState.FAILED, attempt2.getState());

    // The first application should be fine and still running
    assertEquals(RMAppAttemptState.RUNNING, attempt.getState());
  }

  @Test
  public void testEndToEnd() throws Exception {

    Container c1 = createContainer(am, 2 * GB);
    assertMemory(scheduler, nmId, 4 * GB, 0);

    // check node report, 4 GB used and 0 GB available
    assertMemory(scheduler, nmId, 4 * GB, 0);
    nm.nodeHeartbeat(true);
    assertEquals(4 * GB, nm.getCapability().getMemorySize());

    // update node resource to 2 GB, so resource is over-consumed
    updateNodeResource(rm, nmId, 2 * GB, 2, -1);
    // the used resource should still 4 GB and negative available resource
    waitMemory(scheduler, nmId, 4 * GB, -2 * GB, 200, 5 * 1000);
    // check that we did not get a preemption requests
    assertNoPreemption(am.schedule().getPreemptionMessage());

    // check that the NM got the updated resources
    nm.nodeHeartbeat(true);
    assertEquals(2 * GB, nm.getCapability().getMemorySize());

    // check container can complete successfully with resource over-commitment
    ContainerStatus containerStatus = BuilderUtils.newContainerStatus(
        c1.getId(), ContainerState.COMPLETE, "", 0, c1.getResource());
    nm.containerStatus(containerStatus);

    LOG.info("Waiting for containers to be finished for app 1...");
    GenericTestUtils.waitFor(
        () -> attempt.getJustFinishedContainers().size() == 1, 100, 2000);
    assertEquals(1, am.schedule().getCompletedContainersStatuses().size());
    assertMemory(scheduler, nmId, 2 * GB, 0);

    // verify no NPE is trigger in schedule after resource is updated
    am.addRequests(new String[] {"127.0.0.1", "127.0.0.2"}, 3 * GB, 1, 1);
    AllocateResponse allocResponse2 = am.schedule();
    assertTrue("Shouldn't have enough resource to allocate containers",
        allocResponse2.getAllocatedContainers().isEmpty());
    // try 10 times as scheduling is an async process
    for (int i = 0; i < 10; i++) {
      Thread.sleep(100);
      allocResponse2 = am.schedule();
      assertTrue("Shouldn't have enough resource to allocate containers",
          allocResponse2.getAllocatedContainers().isEmpty());
    }

    // increase the resources again to 5 GB to schedule the 3GB container
    updateNodeResource(rm, nmId, 5 * GB, 2, -1);
    waitMemory(scheduler, nmId, 2 * GB, 3 * GB, 100, 5 * 1000);

    // kick the scheduling and check it took effect
    nm.nodeHeartbeat(true);
    while (allocResponse2.getAllocatedContainers().isEmpty()) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(100);
      allocResponse2 = am.schedule();
    }
    assertEquals(1, allocResponse2.getAllocatedContainers().size());
    Container c2 = allocResponse2.getAllocatedContainers().get(0);
    assertEquals(3 * GB, c2.getResource().getMemorySize());
    assertEquals(nmId, c2.getNodeId());
    assertMemory(scheduler, nmId, 5 * GB, 0);

    // reduce the resources and trigger a preempt request to the AM for c2
    updateNodeResource(rm, nmId, 3 * GB, 2, 2 * 1000);
    waitMemory(scheduler, nmId, 5 * GB, -2 * GB, 200, 5 * 1000);

    // wait until MARK_CONTAINER_FOR_PREEMPTION is handled
    rm.drainEvents();

    PreemptionMessage preemptMsg = am.schedule().getPreemptionMessage();
    assertPreemption(c2.getId(), preemptMsg);

    // increasing the resources again, should stop killing the containers
    updateNodeResource(rm, nmId, 5 * GB, 2, -1);
    waitMemory(scheduler, nmId, 5 * GB, 0, 200, 5 * 1000);
    Thread.sleep(3 * 1000);
    assertMemory(scheduler, nmId, 5 * GB, 0);

    // reduce the resources again to trigger a preempt request to the AM for c2
    long t0 = Time.now();
    updateNodeResource(rm, nmId, 3 * GB, 2, 2 * 1000);
    waitMemory(scheduler, nmId, 5 * GB, -2 * GB, 200, 5 * 1000);

    // wait until MARK_CONTAINER_FOR_PREEMPTION is handled
    rm.drainEvents();

    preemptMsg = am.schedule().getPreemptionMessage();
    assertPreemption(c2.getId(), preemptMsg);

    // wait until the scheduler kills the container
    GenericTestUtils.waitFor(() -> {
      try {
        nm.nodeHeartbeat(true); // trigger preemption in the NM
      } catch (Exception e) {
        LOG.error("Cannot heartbeat", e);
      }
      SchedulerNodeReport report = scheduler.getNodeReport(nmId);
      return report.getAvailableResource().getMemorySize() > 0;
    }, 200, 5 * 1000);
    assertMemory(scheduler, nmId, 2 * GB, 1 * GB);

    List<ContainerStatus> completedContainers =
        am.schedule().getCompletedContainersStatuses();
    assertEquals(1, completedContainers.size());
    ContainerStatus c2status = completedContainers.get(0);
    assertContainerKilled(c2.getId(), c2status);

    assertTime(2000, Time.now() - t0);
  }

  /**
   * Create a container with a particular size and make sure it succeeds.
   * @param app Application Master to add the container to.
   * @param memory Memory of the container.
   * @return Newly created container.
   * @throws Exception If there are issues creating the container.
   */
  protected Container createContainer(
      final MockAM app, final int memory) throws Exception {

    ResourceRequest req = ResourceRequest.newBuilder()
        .capability(Resource.newInstance(memory, 1))
        .numContainers(1)
        .build();
    AllocateResponse response = app.allocate(singletonList(req), emptyList());
    List<Container> allocated = response.getAllocatedContainers();
    nm.nodeHeartbeat(true);
    for (int i = 0; allocated.isEmpty() && i < 10; i++) {
      LOG.info("Waiting for containers to be created for app...");
      Thread.sleep(INTERVAL);
      response = app.schedule();
      allocated = response.getAllocatedContainers();
      nm.nodeHeartbeat(true);
    }
    assertFalse("Cannot create the container", allocated.isEmpty());

    assertEquals(1, allocated.size());
    final Container c = allocated.get(0);
    assertEquals(memory, c.getResource().getMemorySize());
    assertEquals(nmId, c.getNodeId());
    return c;
  }

  /**
   * Update the resources on a Node Manager.
   * @param rm Resource Manager to contact.
   * @param nmId Identifier of the Node Manager.
   * @param memory Memory in MB.
   * @param vCores Number of virtual cores.
   * @param overcommitTimeout Timeout for overcommit.
   * @throws Exception If the update cannot be completed.
   */
  public static void updateNodeResource(MockRM rm, NodeId nmId,
      int memory, int vCores, int overcommitTimeout) throws Exception {
    AdminService admin = rm.getAdminService();
    ResourceOption resourceOption = ResourceOption.newInstance(
        Resource.newInstance(memory, vCores), overcommitTimeout);
    UpdateNodeResourceRequest req = UpdateNodeResourceRequest.newInstance(
        singletonMap(nmId, resourceOption));
    admin.updateNodeResource(req);
  }

  /**
   * Make sure that the container was killed.
   * @param containerId Expected container identifier.
   * @param status Container status to check.
   */
  public static void assertContainerKilled(
      final ContainerId containerId, final ContainerStatus status) {
    assertEquals(containerId, status.getContainerId());
    assertEquals(ContainerState.COMPLETE, status.getState());
    assertEquals(ContainerExitStatus.PREEMPTED, status.getExitStatus());
    assertEquals(SchedulerUtils.PREEMPTED_CONTAINER, status.getDiagnostics());
  }

  /**
   * Check that an elapsed time is at least the expected time and no more than
   * two heart beats/scheduling rounds.
   * @param expectedTime Time expected in milliseconds.
   * @param time Actual time to check.
   */
  public static void assertTime(final long expectedTime, final long time) {
    assertTrue("Too short: " + time + "ms", time > expectedTime);
    assertTrue("Too long: " + time + "ms",
        time < (expectedTime + 2 * INTERVAL));
  }

  /**
   * Check that the scheduler didn't ask to preempt anything.
   * @param msg Preemption message from the scheduler.
   */
  public static void assertNoPreemption(final PreemptionMessage msg) {
    if (msg != null &&
        msg.getContract() != null &&
        !msg.getContract().getContainers().isEmpty()) {
      fail("We shouldn't preempt containers: " + msg);
    }
  }

  /**
   * Check that the scheduler ask to preempt a particular container.
   * @param containerId Expected container to preempt.
   * @param msg Preemption message from the scheduler.
   */
  public static void assertPreemption(
      final ContainerId containerId, final PreemptionMessage msg) {
    assertNotNull("Expected a preemption message", msg);
    Set<ContainerId> preemptContainers = new HashSet<>();
    if (msg.getContract() != null) {
      for (PreemptionContainer c : msg.getContract().getContainers()) {
        preemptContainers.add(c.getId());
      }
    }
    if (msg.getStrictContract() != null) {
      for (PreemptionContainer c : msg.getStrictContract().getContainers()) {
        preemptContainers.add(c.getId());
      }
    }
    assertEquals(Collections.singleton(containerId), preemptContainers);
  }

  /**
   * Check if a node report has the expected memory values.
   * @param scheduler Scheduler with the data.
   * @param nmId Identifier of the node to check.
   * @param expectedUsed The expected used memory in MB.
   * @param expectedAvailable The expected available memory in MB.
   */
  public static void assertMemory(ResourceScheduler scheduler, NodeId nmId,
      long expectedUsed, long expectedAvailable) {
    SchedulerNodeReport nmReport = scheduler.getNodeReport(nmId);
    assertNotNull(nmReport);
    Resource used = nmReport.getUsedResource();
    assertEquals("Used memory", expectedUsed, used.getMemorySize());
    Resource available = nmReport.getAvailableResource();
    assertEquals("Available memory",
        expectedAvailable, available.getMemorySize());
  }

  /**
   * Wait until the memory of a NM is at a given point.
   * It does not trigger NM heart beat.
   * @param scheduler Scheduler with the data.
   * @param nmId Identifier of the node to check.
   * @param expectedUsed The expected used memory in MB.
   * @param expectedAvailable The expected available memory in MB.
   * @param checkEveryMillis How often to perform the test in ms.
   * @param waitForMillis The maximum time to wait in ms.
   * @throws Exception If we don't get to the expected memory.
   */
  public static void waitMemory(ResourceScheduler scheduler,
      NodeId nmId, int expectedUsed, int expectedAvailable,
      int checkEveryMillis, int waitForMillis) throws Exception {
    waitMemory(scheduler, nmId, null, expectedUsed, expectedAvailable,
        checkEveryMillis, waitForMillis);
  }

  /**
   * Wait until the memory of a NM is at a given point.
   * It triggers NM heart beat.
   * @param scheduler Scheduler with the data.
   * @param nm Node Manager to check.
   * @param expectedUsed The expected used memory in MB.
   * @param expectedAvailable The expected available memory in MB.
   * @param checkEveryMillis How often to perform the test in ms.
   * @param waitForMillis The maximum time to wait in ms.
   * @throws Exception If we don't get to the expected memory.
   */
  public static void waitMemory(ResourceScheduler scheduler, MockNM nm,
      int expectedUsed, int expectedAvailable,
      int checkEveryMillis, int waitForMillis) throws Exception {
    waitMemory(scheduler, nm.getNodeId(), nm, expectedUsed, expectedAvailable,
        checkEveryMillis, waitForMillis);
  }

  /**
   * Wait until the memory of a NM is at a given point.
   * If the NM is specified, it does heart beat.
   * @param scheduler Scheduler with the data.
   * @param nmId Identifier of the node to check.
   * @param nm Node Manager to check.
   * @param expectedUsed The expected used memory in MB.
   * @param expectedAvailable The expected available memory in MB.
   * @param checkEveryMillis How often to perform the test in ms.
   * @param waitForMillis The maximum time to wait in ms.
   * @throws Exception If we don't get to the expected memory.
   */
  public static void waitMemory(ResourceScheduler scheduler,
      NodeId nmId, MockNM nm,
      int expectedUsed, int expectedAvailable,
      int checkEveryMillis, int waitForMillis) throws Exception {

    long start = Time.monotonicNow();
    while (Time.monotonicNow() - start < waitForMillis) {
      try {
        if (nm != null) {
          nm.nodeHeartbeat(true);
        }
        assertMemory(scheduler, nmId, expectedUsed, expectedAvailable);
        return;
      } catch (AssertionError e) {
        Thread.sleep(checkEveryMillis);
      }
    }

    // No success, notify time out
    SchedulerNodeReport nmReport = scheduler.getNodeReport(nmId);
    Resource used = nmReport.getUsedResource();
    Resource available = nmReport.getAvailableResource();
    throw new TimeoutException("Took longer than " + waitForMillis +
        "ms to get to " + expectedUsed + "," + expectedAvailable +
        " actual=" + used + "," + available);
  }
}
