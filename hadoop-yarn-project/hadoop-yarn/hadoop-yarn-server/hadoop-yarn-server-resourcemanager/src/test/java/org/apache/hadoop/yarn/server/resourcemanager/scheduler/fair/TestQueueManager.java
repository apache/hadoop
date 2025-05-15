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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

/**
 * Test the {@link FairScheduler} queue manager correct queue hierarchies
 * management (create, delete and type changes).
 */
public class TestQueueManager {
  private QueueManager queueManager;
  private FairScheduler scheduler;

  @BeforeEach
  public void setUp() {
    PlacementManager placementManager = new PlacementManager();
    FairSchedulerConfiguration conf = new FairSchedulerConfiguration();
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getQueuePlacementManager()).thenReturn(placementManager);
    SystemClock clock = SystemClock.getInstance();

    scheduler = mock(FairScheduler.class);
    when(scheduler.getConf()).thenReturn(conf);
    when(scheduler.getConfig()).thenReturn(conf);
    when(scheduler.getRMContext()).thenReturn(rmContext);
    when(scheduler.getResourceCalculator()).thenReturn(
        new DefaultResourceCalculator());
    when(scheduler.getClock()).thenReturn(clock);

    AllocationConfiguration allocConf =
        new AllocationConfiguration(scheduler);
    when(scheduler.getAllocationConfiguration()).thenReturn(allocConf);

    // Set up some queues to test default child max resource inheritance
    allocConf.configuredQueues.get(FSQueueType.PARENT).add("root.test");
    allocConf.configuredQueues.get(FSQueueType.LEAF).add("root.test.childA");
    allocConf.configuredQueues.get(FSQueueType.PARENT).add("root.test.childB");

    queueManager = new QueueManager(scheduler);

    FSQueueMetrics.forQueue("root", null, true, conf);
    queueManager.initialize();
    queueManager.updateAllocationConfiguration(allocConf);
  }

  /**
   * Test the leaf to parent queue conversion, excluding the default queue.
   */
  @Test
  public void testReloadTurnsLeafQueueIntoParent() {
    updateConfiguredLeafQueues(queueManager, "queue1");
    
    // When no apps are running in the leaf queue, should be fine turning it
    // into a parent.
    updateConfiguredLeafQueues(queueManager, "queue1.queue2");
    assertNull(queueManager.getLeafQueue("queue1", false));
    assertNotNull(queueManager.getLeafQueue("queue1.queue2", false));
    
    // When leaf queues are empty, should be ok deleting them and
    // turning parent into a leaf.
    updateConfiguredLeafQueues(queueManager, "queue1");
    assertNull(queueManager.getLeafQueue("queue1.queue2", false));
    assertNotNull(queueManager.getLeafQueue("queue1", false));
    
    // When apps exist in leaf queue, we shouldn't be able to create
    // children under it, but things should work otherwise.
    FSLeafQueue q1 = queueManager.getLeafQueue("queue1", false);
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    q1.addAssignedApp(appId);
    updateConfiguredLeafQueues(queueManager, "queue1.queue2");
    assertNull(queueManager.getLeafQueue("queue1.queue2", false));
    assertNotNull(queueManager.getLeafQueue("queue1", false));
    
    // When apps exist in leaf queues under a parent queue, shouldn't be
    // able to turn it into a leaf queue, but things should work otherwise.
    q1.removeAssignedApp(appId);
    updateConfiguredLeafQueues(queueManager, "queue1.queue2");
    FSLeafQueue q2 = queueManager.getLeafQueue("queue1.queue2", false);
    q2.addAssignedApp(appId);
    updateConfiguredLeafQueues(queueManager, "queue1");
    assertNotNull(queueManager.getLeafQueue("queue1.queue2", false));
    assertNull(queueManager.getLeafQueue("queue1", false));
    
    // Since YARN-7769 FS doesn't create the default queue during init, so
    // it should be possible to create a queue under the root.default queue
    updateConfiguredLeafQueues(queueManager, "default.queue3");
    assertNotNull(queueManager.getLeafQueue("default.queue3", false));
    assertNull(queueManager.getLeafQueue("default", false));
  }

  /**
   * Test the postponed leaf to parent queue conversion (app running).
   */
  @Test
  public void testReloadTurnsLeafToParentWithNoLeaf() {
    AllocationConfiguration allocConf =
        new AllocationConfiguration(scheduler);
    // Create a leaf queue1
    allocConf.configuredQueues.get(FSQueueType.LEAF).add("root.queue1");
    queueManager.updateAllocationConfiguration(allocConf);
    assertNotNull(queueManager.getLeafQueue("root.queue1", false));

    // Lets say later on admin makes queue1 a parent queue by
    // specifying "type=parent" in the alloc xml and lets say apps running in
    // queue1
    FSLeafQueue q1 = queueManager.getLeafQueue("queue1", false);
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    q1.addAssignedApp(appId);
    allocConf = new AllocationConfiguration(scheduler);
    allocConf.configuredQueues.get(FSQueueType.PARENT)
        .add("root.queue1");

    // When allocs are reloaded queue1 shouldn't be converter to parent
    queueManager.updateAllocationConfiguration(allocConf);
    assertNotNull(queueManager.getLeafQueue("root.queue1", false));
    assertNull(queueManager.getParentQueue("root.queue1", false));

    // Now lets assume apps completed and there are no apps in queue1
    q1.removeAssignedApp(appId);
    // We should see queue1 transform from leaf queue to parent queue.
    queueManager.updateAllocationConfiguration(allocConf);
    assertNull(queueManager.getLeafQueue("root.queue1", false));
    assertNotNull(queueManager.getParentQueue("root.queue1", false));
    // this parent should not have any children
    assertTrue(queueManager.getParentQueue("root.queue1", false)
        .getChildQueues().isEmpty());
  }

  /**
   * Check the queue name parsing (blank space in all forms).
   */
  @Test
  public void testCheckQueueNodeName() {
    assertFalse(queueManager.isQueueNameValid(""));
    assertFalse(queueManager.isQueueNameValid("  "));
    assertFalse(queueManager.isQueueNameValid(" a"));
    assertFalse(queueManager.isQueueNameValid("a "));
    assertFalse(queueManager.isQueueNameValid(" a "));
    assertFalse(queueManager.isQueueNameValid("\u00a0"));
    assertFalse(queueManager.isQueueNameValid("a\u00a0"));
    assertFalse(queueManager.isQueueNameValid("\u00a0a\u00a0"));
    assertTrue(queueManager.isQueueNameValid("a b"));
    assertTrue(queueManager.isQueueNameValid("a"));
  }

  private void updateConfiguredLeafQueues(QueueManager queueMgr,
                                          String... confLeafQueues) {
    AllocationConfiguration allocConf =
        new AllocationConfiguration(scheduler);
    allocConf.configuredQueues.get(FSQueueType.LEAF)
        .addAll(Sets.newHashSet(confLeafQueues));
    queueMgr.updateAllocationConfiguration(allocConf);
  }

  /**
   * Test simple leaf queue creation.
   */
  @Test
  public void testCreateLeafQueue() {
    FSQueue q1 = queueManager.createQueue("root.queue1", FSQueueType.LEAF);

    assertNotNull(queueManager.getLeafQueue("root.queue1", false),
        "Leaf queue root.queue1 was not created");
    assertEquals("root.queue1", q1.getName(), "createQueue() returned wrong queue");
  }

  /**
   * Test creation of a leaf queue and its parent.
   */
  @Test
  public void testCreateLeafQueueAndParent() {
    FSQueue q2 = queueManager.createQueue("root.queue1.queue2",
        FSQueueType.LEAF);

    assertNotNull(queueManager.getParentQueue("root.queue1", false),
        "Parent queue root.queue1 was not created");
    assertNotNull(queueManager.getLeafQueue("root.queue1.queue2", false),
        "Leaf queue root.queue1.queue2 was not created");
    assertEquals("root.queue1.queue2", q2.getName(),
        "createQueue() returned wrong queue");
  }

  /**
   * Test creation of leaf and parent child queues when the parent queue has
   * child defaults set. In this test we rely on the root.test,
   * root.test.childA and root.test.childB queues that are created in the
   * {@link #setUp()} method.
   */
  @Test
  public void testCreateQueueWithChildDefaults() {
    queueManager.getQueue("root.test").setMaxChildQueueResource(
        new ConfigurableResource(Resources.createResource(8192, 256)));

    FSQueue q1 = queueManager.createQueue("root.test.childC", FSQueueType.LEAF);
    assertNotNull(queueManager.getLeafQueue("root.test.childC", false),
        "Leaf queue root.test.childC was not created");
    assertEquals(q1.getName(), "root.test.childC", "createQueue() returned wrong queue");
    assertEquals(Resources.createResource(8192, 256),
        q1.getMaxShare(), "Max resources for root.queue1 were not inherited from "
        + "parent's max child resources");

    FSQueue q2 = queueManager.createQueue("root.test.childD",
        FSQueueType.PARENT);

    assertNotNull(queueManager.getParentQueue("root.test.childD", false),
        "Leaf queue root.test.childD was not created");
    assertEquals("root.test.childD", q2.getName(),
        "createQueue() returned wrong queue");
    assertEquals(Resources.createResource(8192, 256),
        q2.getMaxShare(), "Max resources for root.test.childD were not inherited "
        + "from parent's max child resources");

    // Check that the childA and childB queues weren't impacted
    // by the child defaults
    assertNotNull(queueManager.getLeafQueue("root.test.childA", false),
        "Leaf queue root.test.childA was not created during setup");
    assertEquals(Resources.unbounded(),
        queueManager.getLeafQueue("root.test.childA", false).getMaxShare(),
        "Max resources for root.test.childA were inherited from "
        + "parent's max child resources");
    assertNotNull(queueManager.getParentQueue("root.test.childB", false),
        "Leaf queue root.test.childB was not created during setup");
    assertEquals(Resources.unbounded(),
        queueManager.getParentQueue("root.test.childB", false).getMaxShare(),
        "Max resources for root.test.childB were inherited from "
        + "parent's max child resources");
  }

  /**
   * Test creation of a leaf queue with no resource limits.
   */
  @Test
  public void testCreateLeafQueueWithDefaults() {
    FSQueue q1 = queueManager.createQueue("root.queue1", FSQueueType.LEAF);

    assertNotNull(queueManager.getLeafQueue("root.queue1", false),
        "Leaf queue root.queue1 was not created");
    assertEquals("root.queue1", q1.getName(), "createQueue() returned wrong queue");

    // Min default is 0,0
    assertEquals(Resources.createResource(0, 0),
        q1.getMinShare(), "Min resources were not set to default");

    // Max default is unbounded
    assertEquals(Resources.unbounded(),
        q1.getMaxShare(), "Max resources were not set to default");
  }

  /**
   * Test creation of a simple parent queue.
   */
  @Test
  public void testCreateParentQueue() {
    FSQueue q1 = queueManager.createQueue("root.queue1", FSQueueType.PARENT);

    assertNotNull(queueManager.getParentQueue("root.queue1", false),
        "Parent queue root.queue1 was not created");
    assertEquals("root.queue1", q1.getName(),
        "createQueue() returned wrong queue");
  }

  /**
   * Test creation of a parent queue and its parent.
   */
  @Test
  public void testCreateParentQueueAndParent() {
    FSQueue q2 = queueManager.createQueue("root.queue1.queue2",
        FSQueueType.PARENT);

    assertNotNull(queueManager.getParentQueue("root.queue1", false),
        "Parent queue root.queue1 was not created");
    assertNotNull(queueManager.getParentQueue("root.queue1.queue2", false),
        "Leaf queue root.queue1.queue2 was not created");
    assertEquals("root.queue1.queue2", q2.getName(),
        "createQueue() returned wrong queue");
  }

  /**
   * Test the removal of a dynamic leaf under a hierarchy of static parents.
   */
  @Test
  public void testRemovalOfDynamicLeafQueue() {
    FSLeafQueue q1 = queueManager.getLeafQueue("root.test.childB.dynamic1",
        true);

    assertNotNull(q1, "Queue root.test.childB.dynamic1 was not created");
    assertEquals("root.test.childB.dynamic1", q1.getName(),
        "createQueue() returned wrong queue");
    assertTrue(q1.isDynamic(), "root.test.childB.dynamic1 is not a dynamic queue");

    // an application is submitted to root.test.childB.dynamic1
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    q1.addAssignedApp(appId);

    // root.test.childB.dynamic1 is not empty and should not be removed
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.test.childB.dynamic1", false);
    assertNotNull(q1, "Queue root.test.childB.dynamic1 was deleted");

    // the application finishes, the next removeEmptyDynamicQueues() should
    // clean root.test.childB.dynamic1 up, but keep its static parent
    q1.removeAssignedApp(appId);

    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.test.childB.dynamic1", false);
    assertNull(q1, "Queue root.test.childB.dynamic1 was not deleted");
    assertNotNull(queueManager.getParentQueue("root.test.childB", false),
        "The static parent of root.test.childB.dynamic1 was deleted");
  }

  /**
   * Test the removal of a dynamic parent and its child in one cleanup action.
   */
  @Test
  public void testRemovalOfDynamicParentQueue() {
    FSQueue q1 = queueManager.getLeafQueue("root.parent1.dynamic1", true);

    assertNotNull(q1, "Queue root.parent1.dynamic1 was not created");
    assertEquals("root.parent1.dynamic1", q1.getName(),
        "createQueue() returned wrong queue");
    assertTrue(q1.isDynamic(), "root.parent1.dynamic1 is not a dynamic queue");

    FSQueue p1 = queueManager.getParentQueue("root.parent1", false);
    assertNotNull(p1, "Queue root.parent1 was not created");
    assertTrue(p1.isDynamic(), "root.parent1 is not a dynamic queue");

    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.parent1.dynamic1", false);
    p1 = queueManager.getParentQueue("root.parent1", false);

    assertNull(q1, "Queue root.parent1.dynamic1 was not deleted");
    assertNull(p1, "Queue root.parent1 was not deleted");
  }

  /**
   * Test the change from dynamic to static for a leaf queue.
   */
  @Test
  public void testNonEmptyDynamicQueueBecomingStaticQueue() {
    FSLeafQueue q1 = queueManager.getLeafQueue("root.leaf1", true);

    assertNotNull(q1, "Queue root.leaf1 was not created");
    assertEquals("root.leaf1", q1.getName(),
        "createQueue() returned wrong queue");
    assertTrue(q1.isDynamic(), "root.leaf1 is not a dynamic queue");

    // pretend that we submitted an app to the queue
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    q1.addAssignedApp(appId);

    // non-empty queues should not be deleted
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.leaf1", false);
    assertNotNull(q1, "Queue root.leaf1 was deleted");

    // next we add leaf1 under root in the allocation config
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    allocConf.configuredQueues.get(FSQueueType.LEAF).add("root.leaf1");
    queueManager.updateAllocationConfiguration(allocConf);

    // updateAllocationConfiguration() should make root.leaf1 a dynamic queue
    assertFalse(q1.isDynamic(), "root.leaf1 is not a static queue");

    // application finished now and the queue is empty, but since leaf1 is a
    // static queue at this point, hence not affected by
    // removeEmptyDynamicQueues()
    q1.removeAssignedApp(appId);
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.leaf1", false);
    assertNotNull(q1, "Queue root.leaf1 was deleted");
    assertFalse(q1.isDynamic(), "root.leaf1 is not a static queue");
  }

  /**
   * Test the change from static to dynamic for a leaf queue.
   */
  @Test
  public void testNonEmptyStaticQueueBecomingDynamicQueue() {
    FSLeafQueue q1 = queueManager.getLeafQueue("root.test.childA", false);

    assertNotNull(q1, "Queue root.test.childA does not exist");
    assertEquals("root.test.childA", q1.getName(),
        "createQueue() returned wrong queue");
    assertFalse(q1.isDynamic(), "root.test.childA is not a static queue");

    // we submitted an app to the queue
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    q1.addAssignedApp(appId);

    // the next removeEmptyDynamicQueues() call should not modify
    // root.test.childA
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.test.childA", false);
    assertNotNull(q1, "Queue root.test.childA was deleted");
    assertFalse(q1.isDynamic(), "root.test.childA is not a dynamic queue");

    // next we remove all queues from the allocation config,
    // this causes all queues to change to dynamic
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    for (Set<String> queueNames : allocConf.configuredQueues.values()) {
      queueManager.setQueuesToDynamic(queueNames);
      queueNames.clear();
    }
    queueManager.updateAllocationConfiguration(allocConf);

    q1 = queueManager.getLeafQueue("root.test.childA", false);
    assertNotNull(q1, "Queue root.test.childA was deleted");
    assertTrue(q1.isDynamic(), "root.test.childA is not a dynamic queue");

    // application finished - the queue does not have runnable app
    // the next removeEmptyDynamicQueues() call should remove the queues
    q1.removeAssignedApp(appId);

    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();

    q1 = queueManager.getLeafQueue("root.test.childA", false);
    assertNull(q1, "Queue root.test.childA was not deleted");

    FSParentQueue p1 = queueManager.getParentQueue("root.test", false);
    assertNull(p1, "Queue root.test was not deleted");
  }

  /**
   * Testing the removal of a dynamic parent queue without a child.
   */
  @Test
  public void testRemovalOfChildlessParentQueue() {
    FSParentQueue q1 = queueManager.getParentQueue("root.test.childB", false);

    assertNotNull(q1, "Queue root.test.childB was not created");
    assertEquals("root.test.childB", q1.getName(),
        "createQueue() returned wrong queue");
    assertFalse(q1.isDynamic(), "root.test.childB is a dynamic queue");

    // static queues should not be deleted
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getParentQueue("root.test.childB", false);
    assertNotNull(q1, "Queue root.test.childB was deleted");

    // next we remove root.test.childB from the allocation config
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    allocConf.configuredQueues.get(FSQueueType.PARENT)
        .remove("root.test.childB");
    queueManager.updateAllocationConfiguration(allocConf);
    queueManager.setQueuesToDynamic(Collections.singleton("root.test.childB"));

    // the next removeEmptyDynamicQueues() call should clean
    // root.test.childB up
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getParentQueue("root.leaf1", false);
    assertNull(q1, "Queue root.leaf1 was not deleted");
  }

  /**
   * Test if a queue is correctly changed from dynamic to static and vice
   * versa.
   */
  @Test
  public void testQueueTypeChange() {
    FSQueue q1 = queueManager.getLeafQueue("root.parent1.leaf1", true);
    assertNotNull(q1, "Queue root.parent1.leaf1 was not created");
    assertEquals("root.parent1.leaf1", q1.getName(),
        "createQueue() returned wrong queue");
    assertTrue(q1.isDynamic(), "root.parent1.leaf1 is not a dynamic queue");

    FSQueue p1 = queueManager.getParentQueue("root.parent1", false);
    assertNotNull(p1, "Queue root.parent1 was not created");
    assertTrue(p1.isDynamic(), "root.parent1 is not a dynamic queue");

    // adding root.parent1.leaf1 and root.parent1 to the allocation config
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    allocConf.configuredQueues.get(FSQueueType.PARENT).add("root.parent1");
    allocConf.configuredQueues.get(FSQueueType.LEAF)
        .add("root.parent1.leaf1");

    // updateAllocationConfiguration() should change both queues over to static
    queueManager.updateAllocationConfiguration(allocConf);
    q1 = queueManager.getLeafQueue("root.parent1.leaf1", false);
    assertFalse(q1.isDynamic(), "root.parent1.leaf1 is not a static queue");
    p1 = queueManager.getParentQueue("root.parent1", false);
    assertFalse(p1.isDynamic(), "root.parent1 is not a static queue");

    // removing root.parent1.leaf1 and root.parent1 from the allocation
    // config
    allocConf.configuredQueues.get(FSQueueType.PARENT).remove("root.parent1");
    allocConf.configuredQueues.get(FSQueueType.LEAF)
        .remove("root.parent1.leaf1");

    // updateAllocationConfiguration() should change both queues
    // to dynamic
    queueManager.updateAllocationConfiguration(allocConf);
    queueManager.setQueuesToDynamic(
        ImmutableSet.of("root.parent1", "root.parent1.leaf1"));
    q1 = queueManager.getLeafQueue("root.parent1.leaf1", false);
    assertTrue(q1.isDynamic(), "root.parent1.leaf1 is not a dynamic queue");
    p1 = queueManager.getParentQueue("root.parent1", false);
    assertTrue(p1.isDynamic(), "root.parent1 is not a dynamic queue");
  }

  /**
   * Test that an assigned app flags a queue as being not empty.
   */
  @Test
  public void testApplicationAssignmentPreventsRemovalOfDynamicQueue() {
    FSLeafQueue q = queueManager.getLeafQueue("root.leaf1", true);
    assertNotNull(q, "root.leaf1 does not exist");
    assertTrue(q.isEmpty(), "root.leaf1 is not empty");

    // assigning an application (without an appAttempt so far) to the queue
    // removeEmptyDynamicQueues() should not remove the queue
    ApplicationId applicationId = ApplicationId.newInstance(1L, 0);
    q.addAssignedApp(applicationId);
    q = queueManager.getLeafQueue("root.leaf1", false);
    assertFalse(q.isEmpty(), "root.leaf1 is empty");

    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q = queueManager.getLeafQueue("root.leaf1", false);
    assertNotNull(q, "root.leaf1 has been removed");
    assertFalse(q.isEmpty(), "root.leaf1 is empty");

    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 0);
    ActiveUsersManager activeUsersManager =
        mock(ActiveUsersManager.class);
    RMContext rmContext = mock(RMContext.class);
    doReturn(scheduler.getConfig()).when(rmContext).getYarnConfiguration();

    // the appAttempt is created
    // removeEmptyDynamicQueues() should not remove the queue
    FSAppAttempt appAttempt = new FSAppAttempt(scheduler, applicationAttemptId,
        "a_user", q, activeUsersManager, rmContext);
    q.addApp(appAttempt, true);
    queueManager.removeEmptyDynamicQueues();
    q = queueManager.getLeafQueue("root.leaf1", false);
    assertNotNull(q, "root.leaf1 has been removed");
    assertFalse(q.isEmpty(), "root.leaf1 is empty");

    // the appAttempt finished, the queue should be empty
    q.removeApp(appAttempt);
    q = queueManager.getLeafQueue("root.leaf1", false);
    assertTrue(q.isEmpty(), "root.leaf1 is not empty");

    // removeEmptyDynamicQueues() should remove the queue
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q = queueManager.getLeafQueue("root.leaf1", false);
    assertNull(q, "root.leaf1 has not been removed");
  }

  /**
   * Test changing a leaf into a parent queue and auto create of the leaf queue
   * under the newly created parent.
   */
  @Test
  public void testRemovalOfIncompatibleNonEmptyQueue() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    allocConf.configuredQueues.get(FSQueueType.LEAF).add("root.a");
    scheduler.allocConf = allocConf;
    queueManager.updateAllocationConfiguration(allocConf);

    FSLeafQueue q = queueManager.getLeafQueue("root.a", true);
    assertNotNull(q, "root.a does not exist");
    assertTrue(q.isEmpty(), "root.a is not empty");

    // we start to run an application on root.a
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    q.addAssignedApp(appId);
    assertFalse(q.isEmpty(), "root.a is empty");

    // root.a should not be removed by removeEmptyDynamicQueues or by
    // removePendingIncompatibleQueues
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q = queueManager.getLeafQueue("root.a", false);
    assertNotNull(q, "root.a does not exist");

    // let's introduce queue incompatibility
    allocConf.configuredQueues.get(FSQueueType.LEAF).remove("root.a");
    allocConf.configuredQueues.get(FSQueueType.PARENT).add("root.a");
    allocConf.configuredQueues.get(FSQueueType.LEAF).add("root.a.b");
    queueManager.updateAllocationConfiguration(allocConf);

    // since root.a has running applications, it should be still a leaf queue
    q = queueManager.getLeafQueue("root.a", false);
    assertNotNull(q, "root.a has been removed");
    assertFalse(q.isEmpty(), "root.a is empty");

    // removePendingIncompatibleQueues should still keep root.a as a leaf queue
    queueManager.removePendingIncompatibleQueues();
    q = queueManager.getLeafQueue("root.a", false);
    assertNotNull(q, "root.a has been removed");
    assertFalse(q.isEmpty(), "root.a is empty");

    // when the application finishes, root.a will become a parent queue on next
    // config cleanup. The leaf queue will be created below it on reload of the
    // config.
    q.removeAssignedApp(appId);
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    FSParentQueue p = queueManager.getParentQueue("root.a", false);
    assertNotNull(p, "root.a does not exist");
    queueManager.updateAllocationConfiguration(allocConf);
    q = queueManager.getLeafQueue("root.a.b", false);
    assertNotNull(q, "root.a.b was not created");
  }

  /**
   * Test to check multiple levels of parent queue removal.
   */
  @Test
  public void testRemoveDeepHierarchy() {
    // create a deeper queue hierarchy
    FSLeafQueue q = queueManager.getLeafQueue("root.p1.p2.p3.leaf", true);
    assertNotNull(q, "root.p1.p2.p3.leaf does not exist");
    assertTrue(q.isEmpty(), "root.p1.p2.p3.leaf is not empty");

    // Add an application to make the queue not empty
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    q.addAssignedApp(appId);

    // remove should not remove the queues
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q = queueManager.getLeafQueue("root.p1.p2.p3.leaf", false);
    assertNotNull(q, "root.p1.p2.p3.leaf does not exist");

    // Remove the application
    q.removeAssignedApp(appId);
    // Cleanup should remove the whole tree
    queueManager.removeEmptyDynamicQueues();
    q = queueManager.getLeafQueue("root.p1.p2.p3.leaf", false);
    assertNull(q, "root.p1.p2.p3.leaf does exist");
    FSParentQueue p = queueManager.getParentQueue("root.p1", false);
    assertNull(p, "root.p1 does exist");
  }

  /**
   * Test the removal of queues when a parent is shared in the tree. First
   * remove one branch then the second branch of the tree.
   */
  @Test
  public void testRemoveSplitHierarchy()  {
    // create a deeper queue hierarchy
    FSLeafQueue leaf1 = queueManager.getLeafQueue("root.p1.p2-1.leaf-1", true);
    assertNotNull(leaf1, "root.p1.p2-1.leaf-1 does not exist");
    assertTrue(leaf1.isEmpty(), "root.p1.p2-1.leaf1 is not empty");
    // Create a split below the first level
    FSLeafQueue leaf2 = queueManager.getLeafQueue("root.p1.p2-2.leaf-2", true);
    assertNotNull(leaf2, "root.p1.p2-2.leaf2 does not exist");
    assertTrue(leaf2.isEmpty(), "root.p1.p2-2.leaf2 is not empty");

    // Add an application to make one of the queues not empty
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    leaf1.addAssignedApp(appId);

    // remove should not remove the non empty split
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    leaf1 = queueManager.getLeafQueue("root.p1.p2-1.leaf-1", false);
    assertNotNull(leaf1, "root.p1.p2-1.leaf-1 does not exist");
    leaf2 = queueManager.getLeafQueue("root.p1.p2-2.leaf-2", false);
    assertNull(leaf2, "root.p1.p2-2.leaf2 does exist");
    FSParentQueue p = queueManager.getParentQueue("root.p1.p2-2", false);
    assertNull(p, "root.p1.p2-2 does exist");

    // Remove the application
    leaf1.removeAssignedApp(appId);
    // Cleanup should remove the whole tree
    queueManager.removeEmptyDynamicQueues();
    leaf1 = queueManager.getLeafQueue("root.p1.p2-1.leaf-1", false);
    assertNull(leaf1, "root.p1.p2-1.leaf-1 does exist");
    p = queueManager.getParentQueue("root.p1", false);
    assertNull(p, "root.p1 does exist");
  }
}
