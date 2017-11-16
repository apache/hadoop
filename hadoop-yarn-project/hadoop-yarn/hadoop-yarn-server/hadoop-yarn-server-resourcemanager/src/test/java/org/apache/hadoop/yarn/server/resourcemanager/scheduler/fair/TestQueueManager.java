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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;

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
}
