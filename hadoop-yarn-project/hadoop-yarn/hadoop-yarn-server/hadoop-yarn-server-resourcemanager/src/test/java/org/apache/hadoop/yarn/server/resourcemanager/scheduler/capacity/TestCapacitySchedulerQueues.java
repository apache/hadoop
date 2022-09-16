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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assertions;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.createMockRMContext;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.createResourceManager;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.stopResourceManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

public class TestCapacitySchedulerQueues {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestCapacitySchedulerQueues.class);
  private ResourceManager resourceManager = null;
  private RMContext mockContext;

  @BeforeEach
  public void setUp() throws Exception {
    resourceManager = createResourceManager();
    mockContext = createMockRMContext();
  }

  @AfterEach
  public void tearDown() throws Exception {
    stopResourceManager(resourceManager);
  }

  /**
   * Test that parseQueue throws an exception when two leaf queues have the
   * same name.
   *
   * @throws IOException
   */
  @Test
  void testParseQueue() throws IOException {
    Assertions.assertThrows(IOException.class, () -> {
      CapacityScheduler cs = new CapacityScheduler();
      cs.setConf(new YarnConfiguration());
      cs.setRMContext(resourceManager.getRMContext());
      CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
      setupQueueConfiguration(conf);
      cs.init(conf);
      cs.start();

      conf.setQueues(CapacitySchedulerConfiguration.ROOT + ".a.a1", new String[]{"b1"});
      conf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".a.a1.b1", 100.0f);
      conf.setUserLimitFactor(CapacitySchedulerConfiguration.ROOT + ".a.a1.b1", 100.0f);

      cs.reinitialize(conf, new RMContextImpl(null, null, null, null, null,
          null, new RMContainerTokenSecretManager(conf),
          new NMTokenSecretManagerInRM(conf),
          new ClientToAMTokenSecretManagerInRM(), null));
    });
  }

  @Test
  void testRefreshQueues() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null,
        null, new RMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInRM(conf),
        new ClientToAMTokenSecretManagerInRM(), null);
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rmContext);
    checkQueueStructureCapacities(cs);

    conf.setCapacity(A, 80f);
    conf.setCapacity(B, 20f);
    cs.reinitialize(conf, mockContext);
    checkQueueStructureCapacities(cs, getDefaultCapacities(80f / 100.0f, 20f / 100.0f));
    cs.stop();
  }

  @Test
  void testRefreshQueuesWithNewQueue() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, new RMContextImpl(null, null, null, null, null,
        null, new RMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInRM(conf),
        new ClientToAMTokenSecretManagerInRM(), null));
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
      cs.reinitialize(conf, mockContext);

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
  void testRefreshQueuesMaxAllocationRefresh() throws Exception {
    // queue refresh should not allow changing the maximum allocation setting
    // per queue to be smaller than previous setting
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, mockContext);
    checkQueueStructureCapacities(cs);

    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize(),
        "max allocation in CS");
    assertEquals(Resources.none(),
        conf.getQueueMaximumAllocation(A1),
        "max allocation for A1");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        ResourceUtils.fetchMaximumAllocationFromConfig(conf).getMemorySize(),
        "max allocation");

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueA1 = findQueue(queueA, A1);
    assertEquals(((LeafQueue) queueA1)
        .getMaximumAllocation().getMemorySize(), 8192, "queue max allocation");

    setMaxAllocMb(conf, A1, 4096);

    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue(e.getCause().toString().contains("not be decreased"),
          "max allocation exception");
    }

    setMaxAllocMb(conf, A1, 8192);
    cs.reinitialize(conf, mockContext);

    setMaxAllocVcores(conf, A1,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES - 1);
    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue(e.getCause().toString().contains("not be decreased"),
          "max allocation exception");
    }
  }

  @Test
  void testRefreshQueuesMaxAllocationPerQueueLarge() throws Exception {
    // verify we can't set the allocation per queue larger then cluster setting
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.init(conf);
    cs.start();
    // change max allocation for B3 queue to be larger then cluster max
    setMaxAllocMb(conf, B3,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB + 2048);
    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage().contains("maximum allocation"),
          "maximum allocation exception");
    }

    setMaxAllocMb(conf, B3,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    cs.reinitialize(conf, mockContext);

    setMaxAllocVcores(conf, B3,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES + 1);
    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage().contains("maximum allocation"),
          "maximum allocation exception");
    }
  }

  @Test
  void testRefreshQueuesMaxAllocationRefreshLarger() throws Exception {
    // queue refresh should allow max allocation per queue to go larger
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    setMaxAllocMb(conf,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    setMaxAllocVcores(conf,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    setMaxAllocMb(conf, A1, 4096);
    setMaxAllocVcores(conf, A1, 2);
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, mockContext);
    checkQueueStructureCapacities(cs);

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueA1 = findQueue(queueA, A1);

    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize(),
        "max capability MB in CS");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores(),
        "max capability vcores in CS");
    assertEquals(4096,
        queueA1.getMaximumAllocation().getMemorySize(),
        "max allocation MB A1");
    assertEquals(2,
        queueA1.getMaximumAllocation().getVirtualCores(),
        "max allocation vcores A1");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        ResourceUtils.fetchMaximumAllocationFromConfig(conf).getMemorySize(),
        "cluster max allocation MB");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        ResourceUtils.fetchMaximumAllocationFromConfig(conf).getVirtualCores(),
        "cluster max allocation vcores");

    assertEquals(4096, queueA1.getMaximumAllocation().getMemorySize(),
        "queue max allocation");

    setMaxAllocMb(conf, A1, 6144);
    setMaxAllocVcores(conf, A1, 3);
    cs.reinitialize(conf, null);
    // conf will have changed but we shouldn't be able to change max allocation
    // for the actual queue
    assertEquals(6144, queueA1.getMaximumAllocation().getMemorySize(),
        "max allocation MB A1");
    assertEquals(3, queueA1.getMaximumAllocation().getVirtualCores(),
        "max allocation vcores A1");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        ResourceUtils.fetchMaximumAllocationFromConfig(conf).getMemorySize(),
        "max allocation MB cluster");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        ResourceUtils.fetchMaximumAllocationFromConfig(conf).getVirtualCores(),
        "max allocation vcores cluster");
    assertEquals(6144, queueA1.getMaximumAllocation().getMemorySize(),
        "queue max allocation MB");
    assertEquals(3, queueA1.getMaximumAllocation().getVirtualCores(),
        "queue max allocation vcores");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize(),
        "max capability MB cluster");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores(),
        "cluster max capability vcores");
  }

  @Test
  void testRefreshQueuesMaxAllocationCSError() throws Exception {
    // Try to refresh the cluster level max allocation size to be smaller
    // and it should error out
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    setMaxAllocMb(conf, 10240);
    setMaxAllocVcores(conf, 10);
    setMaxAllocMb(conf, A1, 4096);
    setMaxAllocVcores(conf, A1, 4);
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, mockContext);
    checkQueueStructureCapacities(cs);

    assertEquals(10240, cs.getMaximumResourceCapability().getMemorySize(),
        "max allocation MB in CS");
    assertEquals(10, cs.getMaximumResourceCapability().getVirtualCores(),
        "max allocation vcores in CS");

    setMaxAllocMb(conf, 6144);
    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue(e.getCause().toString().contains("not be decreased"),
          "max allocation exception");
    }

    setMaxAllocMb(conf, 10240);
    cs.reinitialize(conf, mockContext);

    setMaxAllocVcores(conf, 8);
    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue(e.getCause().toString().contains("not be decreased"),
          "max allocation exception");
    }
  }

  @Test
  void testRefreshQueuesMaxAllocationCSLarger() throws Exception {
    // Try to refresh the cluster level max allocation size to be larger
    // and verify that if there is no setting per queue it uses the
    // cluster level setting.
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    setMaxAllocMb(conf, 10240);
    setMaxAllocVcores(conf, 10);
    setMaxAllocMb(conf, A1, 4096);
    setMaxAllocVcores(conf, A1, 4);
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, mockContext);
    checkQueueStructureCapacities(cs);

    assertEquals(10240, cs.getMaximumResourceCapability().getMemorySize(),
        "max allocation MB in CS");
    assertEquals(10, cs.getMaximumResourceCapability().getVirtualCores(),
        "max allocation vcores in CS");

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueB = findQueue(rootQueue, B);
    CSQueue queueA1 = findQueue(queueA, A1);
    CSQueue queueA2 = findQueue(queueA, A2);
    CSQueue queueB2 = findQueue(queueB, B2);

    assertEquals(4096, queueA1.getMaximumAllocation().getMemorySize(),
        "queue A1 max allocation MB");
    assertEquals(4, queueA1.getMaximumAllocation().getVirtualCores(),
        "queue A1 max allocation vcores");
    assertEquals(10240, queueA2.getMaximumAllocation().getMemorySize(),
        "queue A2 max allocation MB");
    assertEquals(10, queueA2.getMaximumAllocation().getVirtualCores(),
        "queue A2 max allocation vcores");
    assertEquals(10240, queueB2.getMaximumAllocation().getMemorySize(),
        "queue B2 max allocation MB");
    assertEquals(10, queueB2.getMaximumAllocation().getVirtualCores(),
        "queue B2 max allocation vcores");

    setMaxAllocMb(conf, 12288);
    setMaxAllocVcores(conf, 12);
    cs.reinitialize(conf, null);
    // cluster level setting should change and any queues without
    // per queue setting
    assertEquals(12288, cs.getMaximumResourceCapability().getMemorySize(),
        "max allocation MB in CS");
    assertEquals(12, cs.getMaximumResourceCapability().getVirtualCores(),
        "max allocation vcores in CS");
    assertEquals(4096, queueA1.getMaximumAllocation().getMemorySize(),
        "queue A1 max MB allocation");
    assertEquals(4, queueA1.getMaximumAllocation().getVirtualCores(),
        "queue A1 max vcores allocation");
    assertEquals(12288, queueA2.getMaximumAllocation().getMemorySize(),
        "queue A2 max MB allocation");
    assertEquals(12, queueA2.getMaximumAllocation().getVirtualCores(),
        "queue A2 max vcores allocation");
    assertEquals(12288, queueB2.getMaximumAllocation().getMemorySize(),
        "queue B2 max MB allocation");
    assertEquals(12, queueB2.getMaximumAllocation().getVirtualCores(),
        "queue B2 max vcores allocation");
  }

  /**
   * Test for queue deletion.
   *
   * @throws Exception
   */
  @Test
  void testRefreshQueuesWithQueueDelete() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null,
        null, new RMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInRM(conf),
        new ClientToAMTokenSecretManagerInRM(), null);
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rmContext);
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
      cs.reinitialize(conf, mockContext);
      fail("Expected to throw exception when refresh queue tries to delete a"
          + " queue with running apps");
    } catch (IOException e) {
      // ignore
    }

    // test delete leaf queue(root.b.b1) when there is no application running.
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutB1(conf);
    try {
      cs.reinitialize(conf, mockContext);
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
    assertNull(queueB3, "Refresh needs to support delete of leaf queue ");

    // reset back to default configuration for testing parent queue delete
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.reinitialize(conf, rmContext);
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
      cs.reinitialize(conf, mockContext);
      fail("Expected to throw exception when refresh queue tries to delete a"
          + " parent queue with running apps in children queue");
    } catch (IOException e) {
      // ignore
    }

    // test delete Parent queue when there is no application running.
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutB(conf);
    try {
      cs.reinitialize(conf, mockContext);
    } catch (IOException e) {
      fail("Expected to not throw exception when refresh queue tries to delete"
          + " a queue without running apps");
    }
    rootQueue = cs.getRootQueue();
    queueB = findQueue(rootQueue, B);
    String message =
        "Refresh needs to support delete of Parent queue and its children.";
    assertNull(queueB, message);
    assertNull(cs.getCapacitySchedulerQueueManager().getQueues().get("b"),
        message);
    assertNull(cs.getCapacitySchedulerQueueManager().getQueues().get("b1"),
        message);
    assertNull(cs.getCapacitySchedulerQueueManager().getQueues().get("b2"),
        message);

    cs.stop();
  }

  /**
   * Test for all child queue deletion and thus making parent queue a child.
   *
   * @throws Exception
   */
  @Test
  void testRefreshQueuesWithAllChildQueuesDeleted() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null,
        null, new RMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInRM(conf),
        new ClientToAMTokenSecretManagerInRM(), null);
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rmContext);
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
      cs.reinitialize(conf, mockContext);
      fail("Expected to throw exception when refresh queue tries to make parent"
          + " queue a child queue when one of its children is still running.");
    } catch (IOException e) {
      //do not do anything, expected exception
    }

    // test delete leaf queues(root.b.b1,b2,b3) when there is no application
    // running.
    try {
      cs.reinitialize(conf, mockContext);
    } catch (IOException e) {
      e.printStackTrace();
      fail("Expected to NOT throw exception when refresh queue tries to delete"
          + " all children of a parent queue(without running apps).");
    }
    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueB = findQueue(rootQueue, B);
    assertNotNull(queueB, "Parent Queue B should not be deleted");
    Assertions.assertTrue(queueB instanceof LeafQueue,
        "As Queue'B children are not deleted");

    String message =
        "Refresh needs to support delete of all children of Parent queue.";
    assertNull(cs.getCapacitySchedulerQueueManager().getQueues().get("b3"),
        message);
    assertNull(cs.getCapacitySchedulerQueueManager().getQueues().get("b1"),
        message);
    assertNull(cs.getCapacitySchedulerQueueManager().getQueues().get("b2"),
        message);

    cs.stop();
  }

  /**
   * Test if we can convert a leaf queue to a parent queue.
   *
   * @throws Exception
   */
  @Timeout(10000)  @Test
  void testConvertLeafQueueToParentQueue() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null,
        null, new RMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInRM(conf),
        new ClientToAMTokenSecretManagerInRM(), null);
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rmContext);
    checkQueueStructureCapacities(cs);

    String targetQueue = "b1";
    CSQueue b1 = cs.getQueue(targetQueue);
    Assertions.assertEquals(QueueState.RUNNING, b1.getState());

    // test if we can convert a leaf queue which is in RUNNING state
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithB1AsParentQueue(conf);
    try {
      cs.reinitialize(conf, mockContext);
      fail("Expected to throw exception when refresh queue tries to convert"
          + " a child queue to a parent queue.");
    } catch (IOException e) {
      // ignore
    }

    // now set queue state for b1 to STOPPED
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.set("yarn.scheduler.capacity.root.b.b1.state", "STOPPED");
    cs.reinitialize(conf, mockContext);
    Assertions.assertEquals(QueueState.STOPPED, b1.getState());

    // test if we can convert a leaf queue which is in STOPPED state
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithB1AsParentQueue(conf);
    try {
      cs.reinitialize(conf, mockContext);
    } catch (IOException e) {
      fail("Expected to NOT throw exception when refresh queue tries"
          + " to convert a leaf queue WITHOUT running apps");
    }
    b1 = cs.getQueue(targetQueue);
    Assertions.assertTrue(b1 instanceof ParentQueue);
    Assertions.assertEquals(QueueState.RUNNING, b1.getState());
    Assertions.assertTrue(!b1.getChildQueues().isEmpty());
  }

  @Test
  void testQueuesMaxAllocationInheritance() throws Exception {
    // queue level max allocation is set by the queue configuration explicitly
    // or inherits from the parent.

    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
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
    cs.reinitialize(conf, mockContext);
    checkQueueStructureCapacities(cs);

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueB = findQueue(rootQueue, B);
    CSQueue queueA1 = findQueue(queueA, A1);
    CSQueue queueA2 = findQueue(queueA, A2);
    CSQueue queueB1 = findQueue(queueB, B1);
    CSQueue queueB2 = findQueue(queueB, B2);

    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize(),
        "max capability MB in CS");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores(),
        "max capability vcores in CS");
    assertEquals(6144,
        queueA1.getMaximumAllocation().getMemorySize(),
        "max allocation MB A1");
    assertEquals(2,
        queueA1.getMaximumAllocation().getVirtualCores(),
        "max allocation vcores A1");
    assertEquals(4096, queueA2.getMaximumAllocation().getMemorySize(),
        "max allocation MB A2");
    assertEquals(2,
        queueA2.getMaximumAllocation().getVirtualCores(),
        "max allocation vcores A2");
    assertEquals(5120, queueB.getMaximumAllocation().getMemorySize(),
        "max allocation MB B");
    assertEquals(5120, queueB1.getMaximumAllocation().getMemorySize(),
        "max allocation MB B1");
    assertEquals(1024, queueB2.getMaximumAllocation().getMemorySize(),
        "max allocation MB B2");

    // Test get the max-allocation from different parent
    unsetMaxAllocation(conf, A1);
    unsetMaxAllocation(conf, B);
    unsetMaxAllocation(conf, B1);
    setMaxAllocation(conf, CapacitySchedulerConfiguration.ROOT,
        "memory-mb=6144,vcores=2");
    setMaxAllocation(conf, A, "memory-mb=8192,vcores=2");

    cs.reinitialize(conf, mockContext);

    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize(),
        "max capability MB in CS");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores(),
        "max capability vcores in CS");
    assertEquals(8192,
        queueA1.getMaximumAllocation().getMemorySize(),
        "max allocation MB A1");
    assertEquals(2,
        queueA1.getMaximumAllocation().getVirtualCores(),
        "max allocation vcores A1");
    assertEquals(6144,
        queueB1.getMaximumAllocation().getMemorySize(),
        "max allocation MB B1");
    assertEquals(2,
        queueB1.getMaximumAllocation().getVirtualCores(),
        "max allocation vcores B1");

    // Test the default
    unsetMaxAllocation(conf, CapacitySchedulerConfiguration.ROOT);
    unsetMaxAllocation(conf, A);
    unsetMaxAllocation(conf, A1);
    cs.reinitialize(conf, mockContext);

    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize(),
        "max capability MB in CS");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores(),
        "max capability vcores in CS");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        queueA1.getMaximumAllocation().getMemorySize(),
        "max allocation MB A1");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        queueA1.getMaximumAllocation().getVirtualCores(),
        "max allocation vcores A1");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        queueA2.getMaximumAllocation().getMemorySize(),
        "max allocation MB A2");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        queueA2.getMaximumAllocation().getVirtualCores(),
        "max allocation vcores A2");
  }

  @Test
  void testVerifyQueuesMaxAllocationConf() throws Exception {
    // queue level max allocation can't exceed the cluster setting

    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
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
    cs.reinitialize(conf, mockContext);
    checkQueueStructureCapacities(cs);

    setMaxAllocation(conf, CapacitySchedulerConfiguration.ROOT,
        "memory-mb=" + largerMem + ",vcores=2");
    try {
      cs.reinitialize(conf, mockContext);
      fail("Queue Root maximum allocation can't exceed the cluster setting");
    } catch (Exception e) {
      assertTrue(e.getCause().getMessage().contains("maximum allocation"),
          "maximum allocation exception");
    }

    setMaxAllocation(conf, CapacitySchedulerConfiguration.ROOT,
        "memory-mb=4096,vcores=2");
    setMaxAllocation(conf, A, "memory-mb=6144,vcores=2");
    setMaxAllocation(conf, A1, "memory-mb=" + largerMem + ",vcores=2");
    try {
      cs.reinitialize(conf, mockContext);
      fail("Queue A1 maximum allocation can't exceed the cluster setting");
    } catch (Exception e) {
      assertTrue(e.getCause().getMessage().contains("maximum allocation"),
          "maximum allocation exception");
    }
    setMaxAllocation(conf, A1, "memory-mb=8192" + ",vcores=" + largerVcores);
    try {
      cs.reinitialize(conf, mockContext);
      fail("Queue A1 maximum allocation can't exceed the cluster setting");
    } catch (Exception e) {
      assertTrue(e.getCause().getMessage().contains("maximum allocation"),
          "maximum allocation exception");
    }
  }
}
