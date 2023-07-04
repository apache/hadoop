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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.setMaxAllocMb;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.setMaxAllocVcores;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.setMaxAllocation;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.unsetMaxAllocation;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B1_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B2_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B3;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B3_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.checkQueueStructureCapacities;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.ExpectedCapacities;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.findQueue;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.getDefaultCapacities;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfWithoutChildrenOfB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfigurationWithB1AsParentQueue;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfigurationWithoutB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfigurationWithoutB1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class TestCapacitySchedulerQueues {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestCapacitySchedulerQueues.class);
  private MockRM rm;
  private NullRMNodeLabelsManager mgr;
  private CapacitySchedulerConfiguration conf;

  @Before
  public void setUp() throws Exception {
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    rm = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());

    Resource clusterResource = Resource.newInstance(128 * GB, 128);
    mgr.setResourceForLabel(CommonNodeLabelsManager.NO_LABEL, clusterResource);
    cs.getRootQueue().updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));
  }

  @After
  public void tearDown() throws Exception {
    if (rm != null) {
      rm.stop();
    }
    if (mgr != null) {
      mgr.close();
    }
  }

  /**
   * Test that parseQueue throws an exception when two leaf queues have the
   * same name.
   *
   * @throws IOException
   */
  @Test(expected = IOException.class)
  public void testParseQueue() throws IOException {
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    cs.init(conf);
    cs.start();

    conf.setQueues(CapacitySchedulerConfiguration.ROOT + ".a.a1", new String[]{"b1"});
    conf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".a.a1.b1", 100.0f);
    conf.setUserLimitFactor(CapacitySchedulerConfiguration.ROOT + ".a.a1.b1", 100.0f);

    cs.reinitialize(conf, new RMContextImpl(null, null, null, null, null,
        null, new RMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInRM(conf),
        new ClientToAMTokenSecretManagerInRM(), null));
    cs.stop();
  }

  @Test
  public void testRefreshQueues() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs);

    conf.setCapacity(A, 80f);
    conf.setCapacity(B, 20f);
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs, getDefaultCapacities(80f / 100.0f, 20f / 100.0f));
    cs.stop();
  }

  @Test
  public void testRefreshQueuesWithNewQueue() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs);

    // Add a new queue b4
    final String b4 = B + ".b4";
    final float b4Capacity = 10;
    final float modifiedB3Capacity = B3_CAPACITY - b4Capacity;

    try {
      conf.setCapacity(A, 80f);
      conf.setCapacity(B, 20f);
      conf.setQueues(B, new String[]{"b1", "b2", "b3", "b4"});
      conf.setCapacity(B1, B1_CAPACITY);
      conf.setCapacity(B2, B2_CAPACITY);
      conf.setCapacity(B3, modifiedB3Capacity);
      conf.setCapacity(b4, b4Capacity);
      cs.reinitialize(conf, rm.getRMContext());

      final float capA = 80f / 100.0f;
      final float capB = 20f / 100.0f;
      Map<String, ExpectedCapacities> expectedCapacities =
          getDefaultCapacities(capA, capB);
      expectedCapacities.put(B3,
          new ExpectedCapacities(modifiedB3Capacity / 100.0f, capB));
      expectedCapacities.put(b4, new ExpectedCapacities(b4Capacity / 100.0f, capB));
      checkQueueStructureCapacities(cs, expectedCapacities);

      // Verify parent for B4
      CSQueue rootQueue = cs.getRootQueue();
      CSQueue queueB = findQueue(rootQueue, B);
      CSQueue queueB4 = findQueue(queueB, b4);

      assertEquals(queueB, queueB4.getParent());
    } finally {
      cs.stop();
    }
  }

  @Test
  public void testRefreshQueuesMaxAllocationRefresh() throws Exception {
    // queue refresh should not allow changing the maximum allocation setting
    // per queue to be smaller than previous setting
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs);

    assertEquals("max allocation in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max allocation for A1",
        Resources.none(),
        conf.getQueueMaximumAllocation(A1));
    assertEquals("max allocation",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        ResourceUtils.fetchMaximumAllocationFromConfig(conf).getMemorySize());

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueA1 = findQueue(queueA, A1);
    assertEquals("queue max allocation", ((LeafQueue) queueA1)
        .getMaximumAllocation().getMemorySize(), 8192);

    setMaxAllocMb(conf, A1, 4096);

    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("max allocation exception",
          e.getCause().toString().contains("not be decreased"));
    }

    setMaxAllocMb(conf, A1, 8192);
    cs.reinitialize(conf, rm.getRMContext());

    setMaxAllocVcores(conf, A1,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES - 1);
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("max allocation exception",
          e.getCause().toString().contains("not be decreased"));
    }
    cs.stop();
  }

  @Test
  public void testRefreshQueuesMaxAllocationPerQueueLarge() throws Exception {
    // verify we can't set the allocation per queue larger then cluster setting
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    cs.init(conf);
    cs.start();
    // change max allocation for B3 queue to be larger then cluster max
    setMaxAllocMb(conf, B3,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB + 2048);
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("maximum allocation exception",
          e.getCause().getMessage().contains("maximum allocation"));
    }

    setMaxAllocMb(conf, B3,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    cs.reinitialize(conf, rm.getRMContext());

    setMaxAllocVcores(conf, B3,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES + 1);
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("maximum allocation exception",
          e.getCause().getMessage().contains("maximum allocation"));
    }
    cs.stop();
  }

  @Test
  public void testRefreshQueuesMaxAllocationRefreshLarger() throws Exception {
    // queue refresh should allow max allocation per queue to go larger
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    setMaxAllocMb(conf,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    setMaxAllocVcores(conf,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    setMaxAllocMb(conf, A1, 4096);
    setMaxAllocVcores(conf, A1, 2);
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs);

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueA1 = findQueue(queueA, A1);

    assertEquals("max capability MB in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max capability vcores in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores());
    assertEquals("max allocation MB A1",
        4096,
        queueA1.getMaximumAllocation().getMemorySize());
    assertEquals("max allocation vcores A1",
        2,
        queueA1.getMaximumAllocation().getVirtualCores());
    assertEquals("cluster max allocation MB",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        ResourceUtils.fetchMaximumAllocationFromConfig(conf).getMemorySize());
    assertEquals("cluster max allocation vcores",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        ResourceUtils.fetchMaximumAllocationFromConfig(conf).getVirtualCores());

    assertEquals("queue max allocation", 4096,
        queueA1.getMaximumAllocation().getMemorySize());

    setMaxAllocMb(conf, A1, 6144);
    setMaxAllocVcores(conf, A1, 3);
    cs.reinitialize(conf, null);
    // conf will have changed but we shouldn't be able to change max allocation
    // for the actual queue
    assertEquals("max allocation MB A1", 6144,
        queueA1.getMaximumAllocation().getMemorySize());
    assertEquals("max allocation vcores A1", 3,
        queueA1.getMaximumAllocation().getVirtualCores());
    assertEquals("max allocation MB cluster",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        ResourceUtils.fetchMaximumAllocationFromConfig(conf).getMemorySize());
    assertEquals("max allocation vcores cluster",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        ResourceUtils.fetchMaximumAllocationFromConfig(conf).getVirtualCores());
    assertEquals("queue max allocation MB", 6144,
        queueA1.getMaximumAllocation().getMemorySize());
    assertEquals("queue max allocation vcores", 3,
        queueA1.getMaximumAllocation().getVirtualCores());
    assertEquals("max capability MB cluster",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("cluster max capability vcores",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores());
    cs.stop();
  }

  @Test
  public void testRefreshQueuesMaxAllocationCSError() throws Exception {
    // Try to refresh the cluster level max allocation size to be smaller
    // and it should error out
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    setMaxAllocMb(conf, 10240);
    setMaxAllocVcores(conf, 10);
    setMaxAllocMb(conf, A1, 4096);
    setMaxAllocVcores(conf, A1, 4);
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());

    checkQueueStructureCapacities(cs);

    assertEquals("max allocation MB in CS", 10240,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max allocation vcores in CS", 10,
        cs.getMaximumResourceCapability().getVirtualCores());

    setMaxAllocMb(conf, 6144);
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("max allocation exception",
          e.getCause().toString().contains("not be decreased"));
    }

    setMaxAllocMb(conf, 10240);
    cs.reinitialize(conf, rm.getRMContext());

    setMaxAllocVcores(conf, 8);
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("max allocation exception",
          e.getCause().toString().contains("not be decreased"));
    }
    cs.stop();
  }

  @Test
  public void testRefreshQueuesMaxAllocationCSLarger() throws Exception {
    // Try to refresh the cluster level max allocation size to be larger
    // and verify that if there is no setting per queue it uses the
    // cluster level setting.
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    setMaxAllocMb(conf, 10240);
    setMaxAllocVcores(conf, 10);
    setMaxAllocMb(conf, A1, 4096);
    setMaxAllocVcores(conf, A1, 4);
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs);

    assertEquals("max allocation MB in CS", 10240,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max allocation vcores in CS", 10,
        cs.getMaximumResourceCapability().getVirtualCores());

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueB = findQueue(rootQueue, B);
    CSQueue queueA1 = findQueue(queueA, A1);
    CSQueue queueA2 = findQueue(queueA, A2);
    CSQueue queueB2 = findQueue(queueB, B2);

    assertEquals("queue A1 max allocation MB", 4096,
        queueA1.getMaximumAllocation().getMemorySize());
    assertEquals("queue A1 max allocation vcores", 4,
        queueA1.getMaximumAllocation().getVirtualCores());
    assertEquals("queue A2 max allocation MB", 10240,
        queueA2.getMaximumAllocation().getMemorySize());
    assertEquals("queue A2 max allocation vcores", 10,
        queueA2.getMaximumAllocation().getVirtualCores());
    assertEquals("queue B2 max allocation MB", 10240,
        queueB2.getMaximumAllocation().getMemorySize());
    assertEquals("queue B2 max allocation vcores", 10,
        queueB2.getMaximumAllocation().getVirtualCores());

    setMaxAllocMb(conf, 12288);
    setMaxAllocVcores(conf, 12);
    cs.reinitialize(conf, null);
    // cluster level setting should change and any queues without
    // per queue setting
    assertEquals("max allocation MB in CS", 12288,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max allocation vcores in CS", 12,
        cs.getMaximumResourceCapability().getVirtualCores());
    assertEquals("queue A1 max MB allocation", 4096,
        queueA1.getMaximumAllocation().getMemorySize());
    assertEquals("queue A1 max vcores allocation", 4,
        queueA1.getMaximumAllocation().getVirtualCores());
    assertEquals("queue A2 max MB allocation", 12288,
        queueA2.getMaximumAllocation().getMemorySize());
    assertEquals("queue A2 max vcores allocation", 12,
        queueA2.getMaximumAllocation().getVirtualCores());
    assertEquals("queue B2 max MB allocation", 12288,
        queueB2.getMaximumAllocation().getMemorySize());
    assertEquals("queue B2 max vcores allocation", 12,
        queueB2.getMaximumAllocation().getVirtualCores());
    cs.stop();
  }

  /**
   * Test for queue deletion.
   *
   * @throws Exception
   */
  @Test
  public void testRefreshQueuesWithQueueDelete() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs);

    // test delete leaf queue when there is application running.
    Map<String, CSQueue> queues =
        cs.getCapacitySchedulerQueueManager().getShortNameQueues();
    String b1QTobeDeleted = "b1";
    LeafQueue csB1Queue = Mockito.spy((LeafQueue) queues.get(b1QTobeDeleted));
    when(csB1Queue.getState()).thenReturn(QueueState.DRAINING)
        .thenReturn(QueueState.STOPPED);
    cs.getCapacitySchedulerQueueManager().addQueue(b1QTobeDeleted, csB1Queue);
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutB1(conf);
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("Expected to throw exception when refresh queue tries to delete a"
          + " queue with running apps");
    } catch (IOException e) {
      // ignore
    }

    // test delete leaf queue(root.b.b1) when there is no application running.
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutB1(conf);
    try {
      cs.reinitialize(conf, rm.getRMContext());
    } catch (IOException e) {
      LOG.error(
          "Expected to NOT throw exception when refresh queue tries to delete"
              + " a queue WITHOUT running apps",
          e);
      fail("Expected to NOT throw exception when refresh queue tries to delete"
          + " a queue WITHOUT running apps");
    }
    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueB = findQueue(rootQueue, B);
    CSQueue queueB3 = findQueue(queueB, B1);
    assertNull("Refresh needs to support delete of leaf queue ", queueB3);

    // reset back to default configuration for testing parent queue delete
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs);

    // set the configurations such that it fails once but should be successfull
    // next time
    queues = cs.getCapacitySchedulerQueueManager().getShortNameQueues();
    CSQueue bQueue = Mockito.spy((ParentQueue) queues.get("b"));
    when(bQueue.getState()).thenReturn(QueueState.DRAINING)
        .thenReturn(QueueState.STOPPED);
    cs.getCapacitySchedulerQueueManager().addQueue("b", bQueue);

    bQueue = Mockito.spy((LeafQueue) queues.get("b1"));
    when(bQueue.getState()).thenReturn(QueueState.STOPPED);
    cs.getCapacitySchedulerQueueManager().addQueue("b1", bQueue);

    bQueue = Mockito.spy((LeafQueue) queues.get("b2"));
    when(bQueue.getState()).thenReturn(QueueState.STOPPED);
    cs.getCapacitySchedulerQueueManager().addQueue("b2", bQueue);

    bQueue = Mockito.spy((LeafQueue) queues.get("b3"));
    when(bQueue.getState()).thenReturn(QueueState.STOPPED);
    cs.getCapacitySchedulerQueueManager().addQueue("b3", bQueue);

    // test delete Parent queue when there is application running.
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutB(conf);
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("Expected to throw exception when refresh queue tries to delete a"
          + " parent queue with running apps in children queue");
    } catch (IOException e) {
      // ignore
    }

    // test delete Parent queue when there is no application running.
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutB(conf);
    try {
      cs.reinitialize(conf, rm.getRMContext());
    } catch (IOException e) {
      fail("Expected to not throw exception when refresh queue tries to delete"
          + " a queue without running apps");
    }
    rootQueue = cs.getRootQueue();
    queueB = findQueue(rootQueue, B);
    String message =
        "Refresh needs to support delete of Parent queue and its children.";
    assertNull(message, queueB);
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b"));
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b1"));
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b2"));

    cs.stop();
  }

  /**
   * Test for all child queue deletion and thus making parent queue a child.
   *
   * @throws Exception
   */
  @Test
  public void testRefreshQueuesWithAllChildQueuesDeleted() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs);

    // test delete all leaf queues when there is no application running.
    Map<String, CSQueue> queues =
        cs.getCapacitySchedulerQueueManager().getShortNameQueues();

    CSQueue bQueue = Mockito.spy((LeafQueue) queues.get("b1"));
    when(bQueue.getState()).thenReturn(QueueState.RUNNING)
        .thenReturn(QueueState.STOPPED);
    cs.getCapacitySchedulerQueueManager().addQueue("b1", bQueue);

    bQueue = Mockito.spy((LeafQueue) queues.get("b2"));
    when(bQueue.getState()).thenReturn(QueueState.STOPPED);
    cs.getCapacitySchedulerQueueManager().addQueue("b2", bQueue);

    bQueue = Mockito.spy((LeafQueue) queues.get("b3"));
    when(bQueue.getState()).thenReturn(QueueState.STOPPED);
    cs.getCapacitySchedulerQueueManager().addQueue("b3", bQueue);

    conf = new CapacitySchedulerConfiguration();
    setupQueueConfWithoutChildrenOfB(conf);

    // test convert parent queue to leaf queue(root.b) when there is no
    // application running.
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("Expected to throw exception when refresh queue tries to make parent"
          + " queue a child queue when one of its children is still running.");
    } catch (IOException e) {
      //do not do anything, expected exception
    }

    // test delete leaf queues(root.b.b1,b2,b3) when there is no application
    // running.
    try {
      cs.reinitialize(conf, rm.getRMContext());
    } catch (IOException e) {
      e.printStackTrace();
      fail("Expected to NOT throw exception when refresh queue tries to delete"
          + " all children of a parent queue(without running apps).");
    }
    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueB = findQueue(rootQueue, B);
    assertNotNull("Parent Queue B should not be deleted", queueB);
    Assert.assertTrue("As Queue'B children are not deleted",
        queueB instanceof LeafQueue);

    String message =
        "Refresh needs to support delete of all children of Parent queue.";
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b3"));
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b1"));
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b2"));

    cs.stop();
  }

  /**
   * Test if we can convert a leaf queue to a parent queue.
   *
   * @throws Exception
   */
  @Test(timeout = 10000)
  public void testConvertLeafQueueToParentQueue() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs);

    String targetQueue = "b1";
    CSQueue b1 = cs.getQueue(targetQueue);
    Assert.assertEquals(QueueState.RUNNING, b1.getState());

    // test if we can convert a leaf queue which is in RUNNING state
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithB1AsParentQueue(conf);
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("Expected to throw exception when refresh queue tries to convert"
          + " a child queue to a parent queue.");
    } catch (IOException e) {
      // ignore
    }

    // now set queue state for b1 to STOPPED
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.set("yarn.scheduler.capacity.root.b.b1.state", "STOPPED");
    cs.reinitialize(conf, rm.getRMContext());
    Assert.assertEquals(QueueState.STOPPED, b1.getState());

    // test if we can convert a leaf queue which is in STOPPED state
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithB1AsParentQueue(conf);
    try {
      cs.reinitialize(conf, rm.getRMContext());
    } catch (IOException e) {
      fail("Expected to NOT throw exception when refresh queue tries"
          + " to convert a leaf queue WITHOUT running apps");
    }
    b1 = cs.getQueue(targetQueue);
    Assert.assertTrue(b1 instanceof AbstractParentQueue);
    Assert.assertEquals(QueueState.RUNNING, b1.getState());
    Assert.assertTrue(!b1.getChildQueues().isEmpty());
    cs.stop();
  }

  @Test
  public void testQueuesMaxAllocationInheritance() throws Exception {
    // queue level max allocation is set by the queue configuration explicitly
    // or inherits from the parent.

    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    setMaxAllocMb(conf,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    setMaxAllocVcores(conf,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    // Test the child queue overrides
    setMaxAllocation(conf, CapacitySchedulerConfiguration.ROOT,
        "memory-mb=4096,vcores=2");
    setMaxAllocation(conf, A1, "memory-mb=6144,vcores=2");
    setMaxAllocation(conf, B, "memory-mb=5120, vcores=2");
    setMaxAllocation(conf, B2, "memory-mb=1024, vcores=2");

    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs);

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueB = findQueue(rootQueue, B);
    CSQueue queueA1 = findQueue(queueA, A1);
    CSQueue queueA2 = findQueue(queueA, A2);
    CSQueue queueB1 = findQueue(queueB, B1);
    CSQueue queueB2 = findQueue(queueB, B2);

    assertEquals("max capability MB in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max capability vcores in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores());
    assertEquals("max allocation MB A1",
        6144,
        queueA1.getMaximumAllocation().getMemorySize());
    assertEquals("max allocation vcores A1",
        2,
        queueA1.getMaximumAllocation().getVirtualCores());
    assertEquals("max allocation MB A2", 4096,
        queueA2.getMaximumAllocation().getMemorySize());
    assertEquals("max allocation vcores A2",
        2,
        queueA2.getMaximumAllocation().getVirtualCores());
    assertEquals("max allocation MB B", 5120,
        queueB.getMaximumAllocation().getMemorySize());
    assertEquals("max allocation MB B1", 5120,
        queueB1.getMaximumAllocation().getMemorySize());
    assertEquals("max allocation MB B2", 1024,
        queueB2.getMaximumAllocation().getMemorySize());

    // Test get the max-allocation from different parent
    unsetMaxAllocation(conf, A1);
    unsetMaxAllocation(conf, B);
    unsetMaxAllocation(conf, B1);
    setMaxAllocation(conf, CapacitySchedulerConfiguration.ROOT,
        "memory-mb=6144,vcores=2");
    setMaxAllocation(conf, A, "memory-mb=8192,vcores=2");

    cs.reinitialize(conf, rm.getRMContext());

    assertEquals("max capability MB in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max capability vcores in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores());
    assertEquals("max allocation MB A1",
        8192,
        queueA1.getMaximumAllocation().getMemorySize());
    assertEquals("max allocation vcores A1",
        2,
        queueA1.getMaximumAllocation().getVirtualCores());
    assertEquals("max allocation MB B1",
        6144,
        queueB1.getMaximumAllocation().getMemorySize());
    assertEquals("max allocation vcores B1",
        2,
        queueB1.getMaximumAllocation().getVirtualCores());

    // Test the default
    unsetMaxAllocation(conf, CapacitySchedulerConfiguration.ROOT);
    unsetMaxAllocation(conf, A);
    unsetMaxAllocation(conf, A1);
    cs.reinitialize(conf, rm.getRMContext());

    assertEquals("max capability MB in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max capability vcores in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores());
    assertEquals("max allocation MB A1",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        queueA1.getMaximumAllocation().getMemorySize());
    assertEquals("max allocation vcores A1",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        queueA1.getMaximumAllocation().getVirtualCores());
    assertEquals("max allocation MB A2",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        queueA2.getMaximumAllocation().getMemorySize());
    assertEquals("max allocation vcores A2",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        queueA2.getMaximumAllocation().getVirtualCores());
    cs.stop();
  }

  @Test
  public void testVerifyQueuesMaxAllocationConf() throws Exception {
    // queue level max allocation can't exceed the cluster setting

    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(rm.getRMContext());
    setMaxAllocMb(conf,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    setMaxAllocVcores(conf,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    long largerMem =
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB + 1024;
    long largerVcores =
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES + 10;

    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rm.getRMContext());
    checkQueueStructureCapacities(cs);

    setMaxAllocation(conf, CapacitySchedulerConfiguration.ROOT,
        "memory-mb=" + largerMem + ",vcores=2");
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("Queue Root maximum allocation can't exceed the cluster setting");
    } catch (Exception e) {
      assertTrue("maximum allocation exception",
          e.getCause().getMessage().contains("maximum allocation"));
    }

    setMaxAllocation(conf, CapacitySchedulerConfiguration.ROOT,
        "memory-mb=4096,vcores=2");
    setMaxAllocation(conf, A, "memory-mb=6144,vcores=2");
    setMaxAllocation(conf, A1, "memory-mb=" + largerMem + ",vcores=2");
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("Queue A1 maximum allocation can't exceed the cluster setting");
    } catch (Exception e) {
      assertTrue("maximum allocation exception",
          e.getCause().getMessage().contains("maximum allocation"));
    }
    setMaxAllocation(conf, A1, "memory-mb=8192" + ",vcores=" + largerVcores);
    try {
      cs.reinitialize(conf, rm.getRMContext());
      fail("Queue A1 maximum allocation can't exceed the cluster setting");
    } catch (Exception e) {
      assertTrue("maximum allocation exception",
          e.getCause().getMessage().contains("maximum allocation"));
    }
    cs.stop();
  }
}
