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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestQueueManager {
  private FairSchedulerConfiguration conf;
  private QueueManager queueManager;
  private Set<FSQueue> notEmptyQueues;
  private FairScheduler scheduler;
  
  @Before
  public void setUp() throws Exception {
    conf = new FairSchedulerConfiguration();
    scheduler = mock(FairScheduler.class);

    AllocationConfiguration allocConf = new AllocationConfiguration(conf);

    // Set up some queues to test default child max resource inheritance
    allocConf.configuredQueues.get(FSQueueType.PARENT).add("root.test");
    allocConf.configuredQueues.get(FSQueueType.LEAF).add("root.test.childA");
    allocConf.configuredQueues.get(FSQueueType.PARENT).add("root.test.childB");

    when(scheduler.getAllocationConfiguration()).thenReturn(allocConf);
    when(scheduler.getConf()).thenReturn(conf);
    when(scheduler.getResourceCalculator()).thenReturn(
        new DefaultResourceCalculator());

    SystemClock clock = SystemClock.getInstance();

    when(scheduler.getClock()).thenReturn(clock);
    notEmptyQueues = new HashSet<>();
    queueManager = new QueueManager(scheduler) {
      @Override
      public boolean isEmpty(FSQueue queue) {
        return !notEmptyQueues.contains(queue);
      }
    };

    FSQueueMetrics.forQueue("root", null, true, conf);

    queueManager.initialize(conf);
  }

  @Test
  public void testReloadTurnsLeafQueueIntoParent() throws Exception {
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
    notEmptyQueues.add(queueManager.getLeafQueue("queue1", false));
    updateConfiguredLeafQueues(queueManager, "queue1.queue2");
    assertNull(queueManager.getLeafQueue("queue1.queue2", false));
    assertNotNull(queueManager.getLeafQueue("queue1", false));
    
    // When apps exist in leaf queues under a parent queue, shouldn't be
    // able to turn it into a leaf queue, but things should work otherwise.
    notEmptyQueues.clear();
    updateConfiguredLeafQueues(queueManager, "queue1.queue2");
    notEmptyQueues.add(queueManager.getQueue("root.queue1"));
    updateConfiguredLeafQueues(queueManager, "queue1");
    assertNotNull(queueManager.getLeafQueue("queue1.queue2", false));
    assertNull(queueManager.getLeafQueue("queue1", false));
    
    // Should never to be able to create a queue under the default queue
    updateConfiguredLeafQueues(queueManager, "default.queue3");
    assertNull(queueManager.getLeafQueue("default.queue3", false));
    assertNotNull(queueManager.getLeafQueue("default", false));
  }
  
  @Test
  public void testReloadTurnsLeafToParentWithNoLeaf() {
    AllocationConfiguration allocConf = new AllocationConfiguration(conf);
    // Create a leaf queue1
    allocConf.configuredQueues.get(FSQueueType.LEAF).add("root.queue1");
    queueManager.updateAllocationConfiguration(allocConf);
    assertNotNull(queueManager.getLeafQueue("root.queue1", false));

    // Lets say later on admin makes queue1 a parent queue by
    // specifying "type=parent" in the alloc xml and lets say apps running in
    // queue1
    notEmptyQueues.add(queueManager.getLeafQueue("root.queue1", false));
    allocConf = new AllocationConfiguration(conf);
    allocConf.configuredQueues.get(FSQueueType.PARENT)
        .add("root.queue1");

    // When allocs are reloaded queue1 shouldn't be converter to parent
    queueManager.updateAllocationConfiguration(allocConf);
    assertNotNull(queueManager.getLeafQueue("root.queue1", false));
    assertNull(queueManager.getParentQueue("root.queue1", false));

    // Now lets assume apps completed and there are no apps in queue1
    notEmptyQueues.clear();
    // We should see queue1 transform from leaf queue to parent queue.
    queueManager.updateAllocationConfiguration(allocConf);
    assertNull(queueManager.getLeafQueue("root.queue1", false));
    assertNotNull(queueManager.getParentQueue("root.queue1", false));
    // this parent should not have any children
    assertTrue(queueManager.getParentQueue("root.queue1", false)
        .getChildQueues().isEmpty());
  }

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

  private void updateConfiguredLeafQueues(QueueManager queueMgr, String... confLeafQueues) {
    AllocationConfiguration allocConf = new AllocationConfiguration(conf);
    allocConf.configuredQueues.get(FSQueueType.LEAF).addAll(Sets.newHashSet(confLeafQueues));
    queueMgr.updateAllocationConfiguration(allocConf);
  }

  /**
   * Test simple leaf queue creation.
   */
  @Test
  public void testCreateLeafQueue() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();

    queueManager.updateAllocationConfiguration(allocConf);

    FSQueue q1 = queueManager.createQueue("root.queue1", FSQueueType.LEAF);

    assertNotNull("Leaf queue root.queue1 was not created",
        queueManager.getLeafQueue("root.queue1", false));
    assertEquals("createQueue() returned wrong queue",
        "root.queue1", q1.getName());
  }

  /**
   * Test creation of a leaf queue and its parent.
   */
  @Test
  public void testCreateLeafQueueAndParent() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();

    queueManager.updateAllocationConfiguration(allocConf);

    FSQueue q2 = queueManager.createQueue("root.queue1.queue2",
        FSQueueType.LEAF);

    assertNotNull("Parent queue root.queue1 was not created",
        queueManager.getParentQueue("root.queue1", false));
    assertNotNull("Leaf queue root.queue1.queue2 was not created",
        queueManager.getLeafQueue("root.queue1.queue2", false));
    assertEquals("createQueue() returned wrong queue",
        "root.queue1.queue2", q2.getName());
  }

  /**
   * Test creation of leaf and parent child queues when the parent queue has
   * child defaults set. In this test we rely on the root.test,
   * root.test.childA and root.test.childB queues that are created in the
   * {@link #setUp()} method.
   */
  @Test
  public void testCreateQueueWithChildDefaults() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();

    queueManager.updateAllocationConfiguration(allocConf);
    queueManager.getQueue("root.test").setMaxChildQueueResource(
        new ConfigurableResource(Resources.createResource(8192, 256)));

    FSQueue q1 = queueManager.createQueue("root.test.childC", FSQueueType.LEAF);
    assertNotNull("Leaf queue root.test.childC was not created",
        queueManager.getLeafQueue("root.test.childC", false));
    assertEquals("createQueue() returned wrong queue",
        "root.test.childC", q1.getName());
    assertEquals("Max resources for root.queue1 were not inherited from "
        + "parent's max child resources", Resources.createResource(8192, 256),
        q1.getMaxShare());

    FSQueue q2 = queueManager.createQueue("root.test.childD",
        FSQueueType.PARENT);

    assertNotNull("Leaf queue root.test.childD was not created",
        queueManager.getParentQueue("root.test.childD", false));
    assertEquals("createQueue() returned wrong queue",
        "root.test.childD", q2.getName());
    assertEquals("Max resources for root.test.childD were not inherited "
        + "from parent's max child resources",
        Resources.createResource(8192, 256),
        q2.getMaxShare());

    // Check that the childA and childB queues weren't impacted
    // by the child defaults
    assertNotNull("Leaf queue root.test.childA was not created during setup",
        queueManager.getLeafQueue("root.test.childA", false));
    assertEquals("Max resources for root.test.childA were inherited from "
        + "parent's max child resources", Resources.unbounded(),
        queueManager.getLeafQueue("root.test.childA", false).getMaxShare());
    assertNotNull("Leaf queue root.test.childB was not created during setup",
        queueManager.getParentQueue("root.test.childB", false));
    assertEquals("Max resources for root.test.childB were inherited from "
        + "parent's max child resources", Resources.unbounded(),
        queueManager.getParentQueue("root.test.childB", false).getMaxShare());
  }

  /**
   * Test creation of a leaf queue with no resource limits.
   */
  @Test
  public void testCreateLeafQueueWithDefaults() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    FSQueue q1 = queueManager.createQueue("root.queue1", FSQueueType.LEAF);

    assertNotNull("Leaf queue root.queue1 was not created",
        queueManager.getLeafQueue("root.queue1", false));
    assertEquals("createQueue() returned wrong queue",
        "root.queue1", q1.getName());

    // Min default is 0,0
    assertEquals("Min resources were not set to default",
        Resources.createResource(0, 0),
        q1.getMinShare());

    // Max default is unbounded
    assertEquals("Max resources were not set to default", Resources.unbounded(),
        q1.getMaxShare());
  }

  /**
   * Test creation of a simple parent queue.
   */
  @Test
  public void testCreateParentQueue() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();

    queueManager.updateAllocationConfiguration(allocConf);

    FSQueue q1 = queueManager.createQueue("root.queue1", FSQueueType.PARENT);

    assertNotNull("Parent queue root.queue1 was not created",
        queueManager.getParentQueue("root.queue1", false));
    assertEquals("createQueue() returned wrong queue",
        "root.queue1", q1.getName());
  }

  /**
   * Test creation of a parent queue and its parent.
   */
  @Test
  public void testCreateParentQueueAndParent() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();

    queueManager.updateAllocationConfiguration(allocConf);

    FSQueue q2 = queueManager.createQueue("root.queue1.queue2",
        FSQueueType.PARENT);

    assertNotNull("Parent queue root.queue1 was not created",
        queueManager.getParentQueue("root.queue1", false));
    assertNotNull("Leaf queue root.queue1.queue2 was not created",
        queueManager.getParentQueue("root.queue1.queue2", false));
    assertEquals("createQueue() returned wrong queue",
        "root.queue1.queue2", q2.getName());
  }

  @Test
  public void testRemovalOfDynamicLeafQueue() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();

    queueManager.updateAllocationConfiguration(allocConf);

    FSQueue q1 = queueManager.getLeafQueue("root.test.childB.dynamic1", true);

    assertNotNull("Queue root.test.childB.dynamic1 was not created", q1);
    assertEquals("createQueue() returned wrong queue",
        "root.test.childB.dynamic1", q1.getName());
    assertTrue("root.test.childB.dynamic1 is not a dynamic queue",
        q1.isDynamic());

    // an application is submitted to root.test.childB.dynamic1
    notEmptyQueues.add(q1);

    // root.test.childB.dynamic1 is not empty and should not be removed
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.test.childB.dynamic1", false);
    assertNotNull("Queue root.test.childB.dynamic1 was deleted", q1);

    // the application finishes, the next removeEmptyDynamicQueues() should
    // clean root.test.childB.dynamic1 up, but keep its static parent
    notEmptyQueues.remove(q1);

    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.test.childB.dynamic1", false);
    assertNull("Queue root.test.childB.dynamic1 was not deleted", q1);
    assertNotNull("The static parent of root.test.childB.dynamic1 was deleted",
        queueManager.getParentQueue("root.test.childB", false));
  }

  @Test
  public void testRemovalOfDynamicParentQueue() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();

    queueManager.updateAllocationConfiguration(allocConf);

    FSQueue q1 = queueManager.getLeafQueue("root.parent1.dynamic1", true);

    assertNotNull("Queue root.parent1.dynamic1 was not created", q1);
    assertEquals("createQueue() returned wrong queue",
        "root.parent1.dynamic1", q1.getName());
    assertTrue("root.parent1.dynamic1 is not a dynamic queue", q1.isDynamic());

    FSQueue p1 = queueManager.getParentQueue("root.parent1", false);
    assertNotNull("Queue root.parent1 was not created", p1);
    assertTrue("root.parent1 is not a dynamic queue", p1.isDynamic());

    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.parent1.dynamic1", false);
    p1 = queueManager.getParentQueue("root.parent1", false);

    assertNull("Queue root.parent1.dynamic1 was not deleted", q1);
    assertNull("Queue root.parent1 was not deleted", p1);
  }

  @Test
  public void testNonEmptyDynamicQueueBecomingStaticQueue() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();

    queueManager.updateAllocationConfiguration(allocConf);

    FSLeafQueue q1 = queueManager.getLeafQueue("root.leaf1", true);

    assertNotNull("Queue root.leaf1 was not created", q1);
    assertEquals("createQueue() returned wrong queue",
        "root.leaf1", q1.getName());
    assertTrue("root.leaf1 is not a dynamic queue", q1.isDynamic());

    // pretend that we submitted an app to the queue
    notEmptyQueues.add(q1);

    // non-empty queues should not be deleted
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.leaf1", false);
    assertNotNull("Queue root.leaf1 was deleted", q1);

    // next we add leaf1 under root in the allocation config
    allocConf.configuredQueues.get(FSQueueType.LEAF).add("root.leaf1");
    queueManager.updateAllocationConfiguration(allocConf);

    // updateAllocationConfiguration() should make root.leaf1 a dynamic queue
    assertFalse("root.leaf1 is not a static queue", q1.isDynamic());

    // application finished now and the queue is empty, but since leaf1 is a
    // static queue at this point, hence not affected by
    // removeEmptyDynamicQueues()
    notEmptyQueues.clear();
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.leaf1", false);
    assertNotNull("Queue root.leaf1 was deleted", q1);
    assertFalse("root.leaf1 is not a static queue", q1.isDynamic());
  }

  @Test
  public void testNonEmptyStaticQueueBecomingDynamicQueue() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    queueManager.updateAllocationConfiguration(allocConf);

    FSLeafQueue q1 = queueManager.getLeafQueue("root.test.childA", false);

    assertNotNull("Queue root.test.childA does not exist", q1);
    assertEquals("createQueue() returned wrong queue",
        "root.test.childA", q1.getName());
    assertFalse("root.test.childA is not a static queue", q1.isDynamic());

    // we submitted an app to the queue
    notEmptyQueues.add(q1);

    // the next removeEmptyDynamicQueues() call should not modify
    // root.test.childA
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getLeafQueue("root.test.childA", false);
    assertNotNull("Queue root.test.childA was deleted", q1);
    assertFalse("root.test.childA is not a dynamic queue", q1.isDynamic());

    // next we remove all queues from the allocation config,
    // this causes all queues to change to dynamic
    for (Set<String> queueNames : allocConf.configuredQueues.values()) {
      queueManager.setQueuesToDynamic(queueNames);
      queueNames.clear();
    }
    queueManager.updateAllocationConfiguration(allocConf);

    q1 = queueManager.getLeafQueue("root.test.childA", false);
    assertNotNull("Queue root.test.childA was deleted", q1);
    assertTrue("root.test.childA is not a dynamic queue", q1.isDynamic());

    // application finished - the queue does not have runnable app
    // the next removeEmptyDynamicQueues() call should remove the queues
    notEmptyQueues.remove(q1);

    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();

    q1 = queueManager.getLeafQueue("root.test.childA", false);
    assertNull("Queue root.test.childA was not deleted", q1);

    FSParentQueue p1 = queueManager.getParentQueue("root.test", false);
    assertNull("Queue root.test was not deleted", p1);
  }

  @Test
  public void testRemovalOfChildlessParentQueue() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    queueManager.updateAllocationConfiguration(allocConf);

    FSParentQueue q1 = queueManager.getParentQueue("root.test.childB", false);

    assertNotNull("Queue root.test.childB was not created", q1);
    assertEquals("createQueue() returned wrong queue",
        "root.test.childB", q1.getName());
    assertFalse("root.test.childB is a dynamic queue", q1.isDynamic());

    // static queues should not be deleted
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getParentQueue("root.test.childB", false);
    assertNotNull("Queue root.test.childB was deleted", q1);

    // next we remove root.test.childB from the allocation config
    allocConf.configuredQueues.get(FSQueueType.PARENT)
        .remove("root.test.childB");
    queueManager.updateAllocationConfiguration(allocConf);
    queueManager.setQueuesToDynamic(Collections.singleton("root.test.childB"));

    // the next removeEmptyDynamicQueues() call should clean
    // root.test.childB up
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q1 = queueManager.getParentQueue("root.leaf1", false);
    assertNull("Queue root.leaf1 was not deleted", q1);
  }

  @Test
  public void testQueueTypeChange() {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    queueManager.updateAllocationConfiguration(allocConf);

    FSQueue q1 = queueManager.getLeafQueue("root.parent1.leaf1", true);
    assertNotNull("Queue root.parent1.leaf1 was not created", q1);
    assertEquals("createQueue() returned wrong queue",
        "root.parent1.leaf1", q1.getName());
    assertTrue("root.parent1.leaf1 is not a dynamic queue", q1.isDynamic());

    FSQueue p1 = queueManager.getParentQueue("root.parent1", false);
    assertNotNull("Queue root.parent1 was not created", p1);
    assertTrue("root.parent1 is not a dynamic queue", p1.isDynamic());

    // adding root.parent1.leaf1 and root.parent1 to the allocation config
    allocConf.configuredQueues.get(FSQueueType.PARENT).add("root.parent1");
    allocConf.configuredQueues.get(FSQueueType.LEAF)
        .add("root.parent1.leaf1");

    // updateAllocationConfiguration() should change both queues over to static
    queueManager.updateAllocationConfiguration(allocConf);
    q1 = queueManager.getLeafQueue("root.parent1.leaf1", false);
    assertFalse("root.parent1.leaf1 is not a static queue", q1.isDynamic());
    p1 = queueManager.getParentQueue("root.parent1", false);
    assertFalse("root.parent1 is not a static queue", p1.isDynamic());

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
    assertTrue("root.parent1.leaf1 is not a dynamic queue", q1.isDynamic());
    p1 = queueManager.getParentQueue("root.parent1", false);
    assertTrue("root.parent1 is not a dynamic queue", p1.isDynamic());
  }

  @Test
  public void testApplicationAssignmentPreventsRemovalOfDynamicQueue()
      throws Exception {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    queueManager = new QueueManager(scheduler);
    queueManager.initialize(conf);
    queueManager.updateAllocationConfiguration(allocConf);

    FSLeafQueue q = queueManager.getLeafQueue("root.leaf1", true);
    assertNotNull("root.leaf1 does not exist", q);
    assertTrue("root.leaf1 is not empty", queueManager.isEmpty(q));

    // assigning an application (without an appAttempt so far) to the queue
    // removeEmptyDynamicQueues() should not remove the queue
    ApplicationId applicationId = ApplicationId.newInstance(1L, 0);
    q.addAssignedApp(applicationId);
    q = queueManager.getLeafQueue("root.leaf1", false);
    assertFalse("root.leaf1 is empty", queueManager.isEmpty(q));

    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q = queueManager.getLeafQueue("root.leaf1", false);
    assertNotNull("root.leaf1 has been removed", q);
    assertFalse("root.leaf1 is empty", queueManager.isEmpty(q));

    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 0);
    ActiveUsersManager activeUsersManager =
        Mockito.mock(ActiveUsersManager.class);
    RMContext rmContext = Mockito.mock(RMContext.class);

    // the appAttempt is created
    // removeEmptyDynamicQueues() should not remove the queue
    FSAppAttempt appAttempt = new FSAppAttempt(scheduler, applicationAttemptId,
        "a_user", q, activeUsersManager, rmContext);
    q.addApp(appAttempt, true);
    queueManager.removeEmptyDynamicQueues();
    q = queueManager.getLeafQueue("root.leaf1", false);
    assertNotNull("root.leaf1 has been removed", q);
    assertFalse("root.leaf1 is empty", queueManager.isEmpty(q));

    // the appAttempt finished, the queue should be empty
    q.removeApp(appAttempt);
    q = queueManager.getLeafQueue("root.leaf1", false);
    assertTrue("root.leaf1 is not empty", queueManager.isEmpty(q));

    // removeEmptyDynamicQueues() should remove the queue
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q = queueManager.getLeafQueue("root.leaf1", false);
    assertNull("root.leaf1 has not been removed", q);
  }

  @Test
  public void testRemovalOfIncompatibleNonEmptyQueue()
      throws Exception {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    allocConf.configuredQueues.get(FSQueueType.LEAF).add("root.a");
    scheduler.allocConf = allocConf;
    queueManager.updateAllocationConfiguration(allocConf);

    FSLeafQueue q = queueManager.getLeafQueue("root.a", true);
    assertNotNull("root.a does not exist", q);
    assertTrue("root.a is not empty", queueManager.isEmpty(q));

    // we start to run an application on root.a
    notEmptyQueues.add(q);
    q = queueManager.getLeafQueue("root.a", false);
    assertNotNull("root.a does not exist", q);
    assertFalse("root.a is empty", queueManager.isEmpty(q));

    // root.a should not be removed by removeEmptyDynamicQueues or by
    // removePendingIncompatibleQueues
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    q = queueManager.getLeafQueue("root.a", false);
    assertNotNull("root.a does not exist", q);

    // let's introduce queue incompatibility
    allocConf.configuredQueues.get(FSQueueType.LEAF).remove("root.a");
    allocConf.configuredQueues.get(FSQueueType.PARENT).add("root.a");
    allocConf.configuredQueues.get(FSQueueType.LEAF).add("root.a.b");
    queueManager.updateAllocationConfiguration(allocConf);

    // since root.a has running applications, it should be still a leaf queue
    q = queueManager.getLeafQueue("root.a", false);
    assertNotNull("root.a has been removed", q);
    assertFalse("root.a is empty", queueManager.isEmpty(q));

    // removePendingIncompatibleQueues should still keep root.a as a leaf queue
    queueManager.removePendingIncompatibleQueues();
    q = queueManager.getLeafQueue("root.a", false);
    assertNotNull("root.a has been removed", q);
    assertFalse("root.a is empty", queueManager.isEmpty(q));

    // when the application finishes, root.a should be a parent queue
    notEmptyQueues.clear();
    queueManager.removePendingIncompatibleQueues();
    queueManager.removeEmptyDynamicQueues();
    FSParentQueue p = queueManager.getParentQueue("root.a", false);
    assertNotNull("root.a does not exist", p);
  }

}
