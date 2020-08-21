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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.security.AppPriorityACLsManager;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestParentQueue {

  private static final Resource QUEUE_B_RESOURCE = Resource
      .newInstance(14 * 1024, 22);
  private static final Resource QUEUE_A_RESOURCE = Resource
      .newInstance(6 * 1024, 10);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestParentQueue.class);
  
  RMContext rmContext;
  YarnConfiguration conf;
  CapacitySchedulerConfiguration csConf;
  CapacitySchedulerContext csContext;
  
  final static int GB = 1024;
  final static String DEFAULT_RACK = "/default";

  private final ResourceCalculator resourceComparator =
      new DefaultResourceCalculator();
  
  @Before
  public void setUp() throws Exception {
    rmContext = TestUtils.getMockRMContext();
    conf = new YarnConfiguration();
    csConf = new CapacitySchedulerConfiguration();
    
    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(16*GB, 32));
    when(csContext.getClusterResource()).
        thenReturn(Resources.createResource(100 * 16 * GB, 100 * 32));
    when(csContext.getPreemptionManager()).thenReturn(new PreemptionManager());
    when(csContext.getResourceCalculator()).
        thenReturn(resourceComparator);
    when(csContext.getRMContext()).thenReturn(rmContext);
  }
  
  private static final String A = "a";
  private static final String B = "b";
  private static final String Q_A =
      CapacitySchedulerConfiguration.ROOT + "." + A;
  private static final String Q_B =
      CapacitySchedulerConfiguration.ROOT + "." + B;
  private void setupSingleLevelQueues(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {A, B});
    
    conf.setCapacity(Q_A, 30);
    
    conf.setCapacity(Q_B, 70);
    
    LOG.info("Setup top-level queues a and b");
  }

  private void setupSingleLevelQueuesWithAbsoluteResource(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[]{A, B});

    conf.setMinimumResourceRequirement("", Q_A,
        QUEUE_A_RESOURCE);

    conf.setMinimumResourceRequirement("", Q_B,
        QUEUE_B_RESOURCE);

    LOG.info("Setup top-level queues a and b with absolute resource");
  }

  private FiCaSchedulerApp getMockApplication(int appId, String user) {
    FiCaSchedulerApp application = mock(FiCaSchedulerApp.class);
    doReturn(user).when(application).getUser();
    doReturn(Resources.createResource(0, 0)).when(application).getHeadroom();
    return application;
  }

  private void applyAllocationToQueue(Resource clusterResource,
      int allocatedMem,
      CSQueue queue) {
    // Call accept & apply for queue
    ResourceCommitRequest request = mock(ResourceCommitRequest.class);
    when(request.anythingAllocatedOrReserved()).thenReturn(true);
    ContainerAllocationProposal allocation = mock(
        ContainerAllocationProposal.class);
    when(request.getTotalReleasedResource()).thenReturn(Resources.none());
    when(request.getFirstAllocatedOrReservedContainer()).thenReturn(allocation);
    SchedulerContainer scontainer = mock(SchedulerContainer.class);
    when(allocation.getAllocatedOrReservedContainer()).thenReturn(scontainer);
    when(allocation.getAllocatedOrReservedResource()).thenReturn(
        Resources.createResource(allocatedMem));
    when(scontainer.getNodePartition()).thenReturn("");

    if (queue.accept(clusterResource, request)) {
      queue.apply(clusterResource, request);
    }
  }

  private void stubQueueAllocation(final CSQueue queue, 
      final Resource clusterResource, final FiCaSchedulerNode node, 
      final int allocation) {
    stubQueueAllocation(queue, clusterResource, node, allocation, 
        NodeType.NODE_LOCAL);
  }
  
  private void stubQueueAllocation(final CSQueue queue, 
      final Resource clusterResource, final FiCaSchedulerNode node, 
      final int allocation, final NodeType type) {
    
    // Simulate the queue allocation
    doAnswer(new Answer<CSAssignment>() {
      @Override
      public CSAssignment answer(InvocationOnMock invocation) throws Throwable {
        try {
          throw new Exception();
        } catch (Exception e) {
          LOG.info("FOOBAR q.assignContainers q=" + queue.getQueuePath() +
              " alloc=" + allocation + " node=" + node.getNodeName());
        }
        final Resource allocatedResource = Resources.createResource(allocation);
        if (queue instanceof ParentQueue) {
          ((ParentQueue)queue).allocateResource(clusterResource, 
              allocatedResource, RMNodeLabelsManager.NO_LABEL);
        } else {
          FiCaSchedulerApp app1 = getMockApplication(0, "");
          ((LeafQueue)queue).allocateResource(clusterResource, app1, 
              allocatedResource, null, null);
        }
        
        // Next call - nothing
        if (allocation > 0) {
          doReturn(new CSAssignment(Resources.none(), type)).when(queue)
              .assignContainers(eq(clusterResource),
                  any(CandidateNodeSet.class), any(ResourceLimits.class),
                  any(SchedulingMode.class));

          // Mock the node's resource availability
          Resource available = node.getUnallocatedResource();
          doReturn(Resources.subtractFrom(available, allocatedResource)).
          when(node).getUnallocatedResource();
        }

        return new CSAssignment(allocatedResource, type);
      }
    }).when(queue).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), any(ResourceLimits.class),
        any(SchedulingMode.class));
  }
  
  private float computeQueueAbsoluteUsedCapacity(CSQueue queue, 
      int expectedMemory, Resource clusterResource) {
    return (
        ((float)expectedMemory / (float)clusterResource.getMemorySize())
      );
  }
  
  private float computeQueueUsedCapacity(CSQueue queue,
      int expectedMemory, Resource clusterResource) {
    return (expectedMemory / 
        (clusterResource.getMemorySize() * queue.getAbsoluteCapacity()));
  }
  
  final static float DELTA = 0.0001f;
  private void verifyQueueMetrics(CSQueue queue, 
      int expectedMemory, Resource clusterResource) {
    assertEquals(
        computeQueueAbsoluteUsedCapacity(queue, expectedMemory, clusterResource), 
        queue.getAbsoluteUsedCapacity(), 
        DELTA);
    assertEquals(
        computeQueueUsedCapacity(queue, expectedMemory, clusterResource), 
        queue.getUsedCapacity(), 
        DELTA);

  }
  
  @Test
  public void testSingleLevelQueues() throws Exception {
    // Setup queue configs
    setupSingleLevelQueues(csConf);

    CSQueueStore queues = new CSQueueStore();
    CSQueue root =
        CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
            CapacitySchedulerConfiguration.ROOT, queues, queues, 
            TestUtils.spyHook);

    // Setup some nodes
    final int memoryPerNode = 10;
    final int coresPerNode = 16;
    final int numNodes = 2;
    
    FiCaSchedulerNode node_0 = 
        TestUtils.getMockNode("host_0", DEFAULT_RACK, 0, memoryPerNode*GB);
    FiCaSchedulerNode node_1 = 
        TestUtils.getMockNode("host_1", DEFAULT_RACK, 0, memoryPerNode*GB);
    
    final Resource clusterResource = 
        Resources.createResource(numNodes * (memoryPerNode*GB),
            numNodes * coresPerNode);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Start testing
    LeafQueue a = (LeafQueue)queues.get(A);
    LeafQueue b = (LeafQueue)queues.get(B);
    
    a.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    b.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    queues.get(CapacitySchedulerConfiguration.ROOT).getQueueResourceUsage()
    .incPending(Resources.createResource(1 * GB));
    
    // Simulate B returning a container on node_0
    stubQueueAllocation(a, clusterResource, node_0, 0*GB);
    stubQueueAllocation(b, clusterResource, node_0, 1*GB);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(a, 0*GB, clusterResource);
    verifyQueueMetrics(b, 1*GB, clusterResource);
    
    // Now, A should get the scheduling opportunity since A=0G/6G, B=1G/14G
    stubQueueAllocation(a, clusterResource, node_1, 2*GB);
    stubQueueAllocation(b, clusterResource, node_1, 1*GB);
    root.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    InOrder allocationOrder = inOrder(a, b);
    allocationOrder.verify(a).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    root.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder.verify(b).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    verifyQueueMetrics(a, 2*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);

    // Now, B should get the scheduling opportunity 
    // since A has 2/6G while B has 2/14G
    stubQueueAllocation(a, clusterResource, node_0, 1*GB);
    stubQueueAllocation(b, clusterResource, node_0, 2*GB);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    root.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(b, a);
    allocationOrder.verify(b).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    allocationOrder.verify(a).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 4*GB, clusterResource);

    // Now, B should still get the scheduling opportunity 
    // since A has 3/6G while B has 4/14G
    stubQueueAllocation(a, clusterResource, node_0, 0*GB);
    stubQueueAllocation(b, clusterResource, node_0, 4*GB);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(b, a);
    allocationOrder.verify(b).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    allocationOrder.verify(a).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 8*GB, clusterResource);

    // Now, A should get the scheduling opportunity 
    // since A has 3/6G while B has 8/14G
    stubQueueAllocation(a, clusterResource, node_1, 1*GB);
    stubQueueAllocation(b, clusterResource, node_1, 1*GB);
    root.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    root.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(a, b);
    allocationOrder.verify(b).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    allocationOrder.verify(a).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    verifyQueueMetrics(a, 4*GB, clusterResource);
    verifyQueueMetrics(b, 9*GB, clusterResource);
  }

  @Test
  public void testSingleLevelQueuesPrecision() throws Exception {
    // Setup queue configs
    setupSingleLevelQueues(csConf);
    csConf.setCapacity(Q_A, 30);
    csConf.setCapacity(Q_B, 70.5F);

    CSQueueStore queues = new CSQueueStore();
    boolean exceptionOccurred = false;
    try {
      CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
          CapacitySchedulerConfiguration.ROOT, queues, queues,
          TestUtils.spyHook);
    } catch (IllegalArgumentException ie) {
      exceptionOccurred = true;
    }
    if (!exceptionOccurred) {
      Assert.fail("Capacity is more then 100% so should be failed.");
    }
    csConf.setCapacity(Q_A, 30);
    csConf.setCapacity(Q_B, 70);
    exceptionOccurred = false;
    queues.clear();
    try {
      CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
          CapacitySchedulerConfiguration.ROOT, queues, queues,
          TestUtils.spyHook);
    } catch (IllegalArgumentException ie) {
      exceptionOccurred = true;
    }
    if (exceptionOccurred) {
      Assert.fail("Capacity is 100% so should not be failed.");
    }
    csConf.setCapacity(Q_A, 30);
    csConf.setCapacity(Q_B, 70.005F);
    exceptionOccurred = false;
    queues.clear();
    try {
      CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
          CapacitySchedulerConfiguration.ROOT, queues, queues,
          TestUtils.spyHook);
    } catch (IllegalArgumentException ie) {
      exceptionOccurred = true;
    }
    if (exceptionOccurred) {
      Assert
          .fail("Capacity is under PRECISION which is .05% so should not be failed.");
    }
  }
  
  private static final String C = "c";
  private static final String C1 = "c1";
  private static final String C11 = "c11";
  private static final String C111 = "c111";
  private static final String C1111 = "c1111";

  private static final String D = "d";
  private static final String A1 = "a1";
  private static final String A2 = "a2";
  private static final String B1 = "b1";
  private static final String B2 = "b2";
  private static final String B3 = "b3";
  
  private void setupMultiLevelQueues(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {A, B, C, D});
    
    conf.setCapacity(Q_A, 10);
    
    conf.setCapacity(Q_B, 50);
    
    final String Q_C = CapacitySchedulerConfiguration.ROOT + "." + C;
    conf.setCapacity(Q_C, 19.5f);
    
    final String Q_D = CapacitySchedulerConfiguration.ROOT + "." + D;
    conf.setCapacity(Q_D, 20.5f);
    
    // Define 2-nd level queues
    conf.setQueues(Q_A, new String[] {A1, A2});
    conf.setCapacity(Q_A + "." + A1, 50);
    conf.setCapacity(Q_A + "." + A2, 50);
    
    conf.setQueues(Q_B, new String[] {B1, B2, B3});
    conf.setCapacity(Q_B + "." + B1, 10);
    conf.setCapacity(Q_B + "." + B2, 20);
    conf.setCapacity(Q_B + "." + B3, 70);

    conf.setQueues(Q_C, new String[] {C1});

    final String Q_C1= Q_C + "." + C1;
    conf.setCapacity(Q_C1, 100);
    conf.setQueues(Q_C1, new String[] {C11});

    final String Q_C11= Q_C1 + "." + C11;
    conf.setCapacity(Q_C11, 100);
    conf.setQueues(Q_C11, new String[] {C111});

    final String Q_C111= Q_C11 + "." + C111;
    conf.setCapacity(Q_C111, 100);
    //Leaf Queue
    conf.setQueues(Q_C111, new String[] {C1111});
    final String Q_C1111= Q_C111 + "." + C1111;
    conf.setCapacity(Q_C1111, 100);
  }

  @Test
  public void testMultiLevelQueues() throws Exception {
    /*
     * Structure of queue:
     *            Root
     *           ____________
     *          /    |   \   \
     *         A     B    C   D
     *       / |   / | \   \
     *      A1 A2 B1 B2 B3  C1
     *                        \
     *                         C11
     *                           \
     *                           C111
     *                             \
     *                              C1111
     */
    
    // Setup queue configs
    setupMultiLevelQueues(csConf);

    CSQueueStore queues = new CSQueueStore();
    CSQueue root =
        CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
            CapacitySchedulerConfiguration.ROOT, queues, queues,
            TestUtils.spyHook);
    
    // Setup some nodes
    final int memoryPerNode = 10;
    final int coresPerNode = 16;
    final int numNodes = 3;
    
    FiCaSchedulerNode node_0 = 
        TestUtils.getMockNode("host_0", DEFAULT_RACK, 0, memoryPerNode*GB);
    FiCaSchedulerNode node_1 = 
        TestUtils.getMockNode("host_1", DEFAULT_RACK, 0, memoryPerNode*GB);
    FiCaSchedulerNode node_2 = 
        TestUtils.getMockNode("host_2", DEFAULT_RACK, 0, memoryPerNode*GB);
    
    final Resource clusterResource = 
        Resources.createResource(numNodes * (memoryPerNode*GB), 
            numNodes * coresPerNode);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Start testing
    CSQueue a = queues.get(A);
    a.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue b = queues.get(B);
    b.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue c = queues.get(C);
    c.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue d = queues.get(D);
    d.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));

    CSQueue a1 = queues.get(A1);
    a1.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue a2 = queues.get(A2);
    a2.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));

    CSQueue b1 = queues.get(B1);
    b1.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue b2 = queues.get(B2);
    b2.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue b3 = queues.get(B3);
    b3.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    queues.get(CapacitySchedulerConfiguration.ROOT).getQueueResourceUsage()
    .incPending(Resources.createResource(1 * GB));

    // Simulate C returning a container on node_0
    stubQueueAllocation(a, clusterResource, node_0, 0*GB);
    stubQueueAllocation(b, clusterResource, node_0, 0*GB);
    stubQueueAllocation(c, clusterResource, node_0, 1*GB);
    stubQueueAllocation(d, clusterResource, node_0, 0*GB);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(a, 0*GB, clusterResource);
    verifyQueueMetrics(b, 0*GB, clusterResource);
    verifyQueueMetrics(c, 1*GB, clusterResource);
    verifyQueueMetrics(d, 0*GB, clusterResource);
    reset(a); reset(b); reset(c);

    // Now get B2 to allocate
    // A = 0/3, B = 0/15, C = 1/6, D=0/6
    stubQueueAllocation(a, clusterResource, node_1, 0*GB);
    stubQueueAllocation(b2, clusterResource, node_1, 4*GB);
    stubQueueAllocation(c, clusterResource, node_1, 0*GB);
    root.assignContainers(clusterResource, node_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyAllocationToQueue(clusterResource, 4*GB,
        b);
    verifyQueueMetrics(a, 0*GB, clusterResource);
    verifyQueueMetrics(b, 4*GB, clusterResource);
    verifyQueueMetrics(c, 1*GB, clusterResource);
    reset(a); reset(b); reset(c);
    
    // Now get both A1, C & B3 to allocate in right order
    // A = 0/3, B = 4/15, C = 1/6, D=0/6
    stubQueueAllocation(a1, clusterResource, node_0, 1*GB);
    stubQueueAllocation(b3, clusterResource, node_0, 2*GB);
    stubQueueAllocation(c, clusterResource, node_0, 2*GB);

    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    InOrder allocationOrder = inOrder(a, c, b);
    allocationOrder.verify(a).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    applyAllocationToQueue(clusterResource, 1 * GB, a);

    root.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder.verify(c).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    applyAllocationToQueue(clusterResource, 2 * GB, root);

    root.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder.verify(b).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    applyAllocationToQueue(clusterResource, 2*GB, b);
    verifyQueueMetrics(a, 1*GB, clusterResource);
    verifyQueueMetrics(b, 6*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    reset(a); reset(b); reset(c);
    
    // Now verify max-capacity
    // A = 1/3, B = 6/15, C = 3/6, D=0/6
    // Ensure a1 won't alloc above max-cap although it should get 
    // scheduling opportunity now, right after a2
    LOG.info("here");
    ((ParentQueue)a).setMaxCapacity(.1f);  // a should be capped at 3/30
    stubQueueAllocation(a1, clusterResource, node_2, 1*GB); // shouldn't be 
                                                            // allocated due 
                                                            // to max-cap
    stubQueueAllocation(a2, clusterResource, node_2, 2*GB);
    stubQueueAllocation(b3, clusterResource, node_2, 1*GB);
    stubQueueAllocation(b1, clusterResource, node_2, 1*GB);
    stubQueueAllocation(c, clusterResource, node_2, 1*GB);
    root.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(a, a2, a1, b, c);
    allocationOrder.verify(a).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    allocationOrder.verify(a2).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    applyAllocationToQueue(clusterResource, 2*GB, a);

    root.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder.verify(b).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    applyAllocationToQueue(clusterResource, 2*GB, b);

    root.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder.verify(c).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 8*GB, clusterResource);
    verifyQueueMetrics(c, 4*GB, clusterResource);
    reset(a); reset(b); reset(c);
  }
  
  @Test (expected=IllegalArgumentException.class)
  public void testQueueCapacitySettingChildZero() throws Exception {
    // Setup queue configs
    setupMultiLevelQueues(csConf);
    
    // set child queues capacity to 0 when parents not 0
    csConf.setCapacity(Q_B + "." + B1, 0);
    csConf.setCapacity(Q_B + "." + B2, 0);
    csConf.setCapacity(Q_B + "." + B3, 0);

    CSQueueStore queues = new CSQueueStore();
    CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
        CapacitySchedulerConfiguration.ROOT, queues, queues,
        TestUtils.spyHook);
  }
  
  @Test (expected=IllegalArgumentException.class)
  public void testQueueCapacitySettingParentZero() throws Exception {
    // Setup queue configs
    setupMultiLevelQueues(csConf);
    
    // set parent capacity to 0 when child not 0
    csConf.setCapacity(Q_B, 0);
    csConf.setCapacity(Q_A, 60);

    CSQueueStore queues = new CSQueueStore();
    CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
        CapacitySchedulerConfiguration.ROOT, queues, queues,
        TestUtils.spyHook);
  }
  
  @Test
  public void testQueueCapacityZero() throws Exception {
    // Setup queue configs
    setupMultiLevelQueues(csConf);
    
    // set parent and child capacity to 0
    csConf.setCapacity(Q_B, 0);
    csConf.setCapacity(Q_B + "." + B1, 0);
    csConf.setCapacity(Q_B + "." + B2, 0);
    csConf.setCapacity(Q_B + "." + B3, 0);
    
    csConf.setCapacity(Q_A, 60);

    CSQueueStore queues = new CSQueueStore();
    try {
      CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
          CapacitySchedulerConfiguration.ROOT, queues, queues,
          TestUtils.spyHook);
    } catch (IllegalArgumentException e) {
      fail("Failed to create queues with 0 capacity: " + e);
    }
    assertTrue("Failed to create queues with 0 capacity", true);
  }

  @Test
  public void testOffSwitchScheduling() throws Exception {
    // Setup queue configs
    setupSingleLevelQueues(csConf);

    CSQueueStore queues = new CSQueueStore();
    CSQueue root =
        CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
            CapacitySchedulerConfiguration.ROOT, queues, queues,
            TestUtils.spyHook);

    // Setup some nodes
    final int memoryPerNode = 10;
    final int coresPerNode = 16;
    final int numNodes = 2;

    FiCaSchedulerNode node_0 =
        TestUtils.getMockNode("host_0", DEFAULT_RACK, 0, memoryPerNode*GB);
    FiCaSchedulerNode node_1 =
        TestUtils.getMockNode("host_1", DEFAULT_RACK, 0, memoryPerNode*GB);

    final Resource clusterResource =
        Resources.createResource(numNodes * (memoryPerNode*GB),
            numNodes * coresPerNode);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Start testing
    LeafQueue a = (LeafQueue)queues.get(A);
    LeafQueue b = (LeafQueue)queues.get(B);
    a.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    b.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    queues.get(CapacitySchedulerConfiguration.ROOT).getQueueResourceUsage()
        .incPending(Resources.createResource(1 * GB));

    // Simulate B returning a container on node_0
    stubQueueAllocation(a, clusterResource, node_0, 0*GB, NodeType.OFF_SWITCH);
    stubQueueAllocation(b, clusterResource, node_0, 1*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(a, 0*GB, clusterResource);
    verifyQueueMetrics(b, 1*GB, clusterResource);

    // Now, A should get the scheduling opportunity since A=0G/6G, B=1G/14G
    // also, B gets a scheduling opportunity since A allocates RACK_LOCAL
    stubQueueAllocation(a, clusterResource, node_1, 2*GB, NodeType.RACK_LOCAL);
    stubQueueAllocation(b, clusterResource, node_1, 1*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    InOrder allocationOrder = inOrder(a);
    allocationOrder.verify(a).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    root.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(b);
    allocationOrder.verify(b).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    verifyQueueMetrics(a, 2*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);

    // Now, B should get the scheduling opportunity
    // since A has 2/6G while B has 2/14G,
    // However, since B returns off-switch, A won't get an opportunity
    stubQueueAllocation(a, clusterResource, node_0, 1*GB, NodeType.NODE_LOCAL);
    stubQueueAllocation(b, clusterResource, node_0, 2*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(b, a);
    allocationOrder.verify(b).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    allocationOrder.verify(a).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    verifyQueueMetrics(a, 2*GB, clusterResource);
    verifyQueueMetrics(b, 4*GB, clusterResource);

  }
  
  @Test
  public void testOffSwitchSchedulingMultiLevelQueues() throws Exception {
    // Setup queue configs
    setupMultiLevelQueues(csConf);
    //B3
    CSQueueStore queues = new CSQueueStore();
    CSQueue root = 
        CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
            CapacitySchedulerConfiguration.ROOT, queues, queues,
            TestUtils.spyHook);

    // Setup some nodes
    final int memoryPerNode = 10;
    final int coresPerNode = 10;
    final int numNodes = 2;
    
    FiCaSchedulerNode node_0 = 
        TestUtils.getMockNode("host_0", DEFAULT_RACK, 0, memoryPerNode*GB);
    FiCaSchedulerNode node_1 = 
        TestUtils.getMockNode("host_1", DEFAULT_RACK, 0, memoryPerNode*GB);
    
    final Resource clusterResource = 
        Resources.createResource(numNodes * (memoryPerNode*GB),
            numNodes * coresPerNode);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Start testing
    LeafQueue b3 = (LeafQueue)queues.get(B3);
    LeafQueue b2 = (LeafQueue)queues.get(B2);
    b2.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    b3.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    queues.get(CapacitySchedulerConfiguration.ROOT).getQueueResourceUsage()
    .incPending(Resources.createResource(1 * GB));
    
    CSQueue b = queues.get(B);
    b.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    
    // Simulate B3 returning a container on node_0
    stubQueueAllocation(b2, clusterResource, node_0, 0*GB, NodeType.OFF_SWITCH);
    stubQueueAllocation(b3, clusterResource, node_0, 1*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(b2, 0*GB, clusterResource);
    verifyQueueMetrics(b3, 1*GB, clusterResource);
    
    // Now, B2 should get the scheduling opportunity since B2=0G/2G, B3=1G/7G
    // also, B3 gets a scheduling opportunity since B2 allocates RACK_LOCAL
    stubQueueAllocation(b2, clusterResource, node_1, 1*GB, NodeType.RACK_LOCAL);
    stubQueueAllocation(b3, clusterResource, node_1, 1*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    root.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    InOrder allocationOrder = inOrder(b2, b3);
    allocationOrder.verify(b2).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    allocationOrder.verify(b3).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    verifyQueueMetrics(b2, 1*GB, clusterResource);
    verifyQueueMetrics(b3, 2*GB, clusterResource);
    
    // Now, B3 should get the scheduling opportunity 
    // since B2 has 1/2G while B3 has 2/7G, 
    // However, since B3 returns off-switch, B2 won't get an opportunity
    stubQueueAllocation(b2, clusterResource, node_0, 1*GB, NodeType.NODE_LOCAL);
    stubQueueAllocation(b3, clusterResource, node_0, 1*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(b3, b2);
    allocationOrder.verify(b3).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    allocationOrder.verify(b2).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), anyResourceLimits(),
        any(SchedulingMode.class));
    verifyQueueMetrics(b2, 1*GB, clusterResource);
    verifyQueueMetrics(b3, 3*GB, clusterResource);

  }

  public boolean hasQueueACL(List<QueueUserACLInfo> aclInfos, QueueACL acl, String qName) {
    for (QueueUserACLInfo aclInfo : aclInfos) {
      if (aclInfo.getQueueName().equals(qName)) {
        if (aclInfo.getUserAcls().contains(acl)) {
          return true;
        }
      }
    }    
    return false;
  }

  @Test
  public void testQueueAcl() throws Exception {
 
    setupMultiLevelQueues(csConf);
    csConf.setAcl(CapacitySchedulerConfiguration.ROOT, QueueACL.SUBMIT_APPLICATIONS, " ");
    csConf.setAcl(CapacitySchedulerConfiguration.ROOT, QueueACL.ADMINISTER_QUEUE, " ");

    final String Q_C = CapacitySchedulerConfiguration.ROOT + "." + C;
    csConf.setAcl(Q_C, QueueACL.ADMINISTER_QUEUE, "*");
    final String Q_C11= Q_C + "." + C1 +  "." + C11;
    csConf.setAcl(Q_C11, QueueACL.SUBMIT_APPLICATIONS, "*");

    CSQueueStore queues = new CSQueueStore();
    CSQueue root = 
        CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
            CapacitySchedulerConfiguration.ROOT, queues, queues,
            TestUtils.spyHook);
    YarnAuthorizationProvider authorizer =
        YarnAuthorizationProvider.getInstance(conf);
    AppPriorityACLsManager appPriorityACLManager = new AppPriorityACLsManager(
        conf);
    CapacitySchedulerQueueManager.setQueueAcls(authorizer,
        appPriorityACLManager, queues);

    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    // Setup queue configs
    ParentQueue c = (ParentQueue)queues.get(C);
    ParentQueue c1 = (ParentQueue)queues.get(C1);
    ParentQueue c11 = (ParentQueue)queues.get(C11);
    ParentQueue c111 = (ParentQueue)queues.get(C111);

    assertFalse(root.hasAccess(QueueACL.ADMINISTER_QUEUE, user));
    List<QueueUserACLInfo> aclInfos = root.getQueueUserAclInfo(user);
    assertFalse(hasQueueACL(aclInfos, QueueACL.ADMINISTER_QUEUE, "root"));
    
    assertFalse(root.hasAccess(QueueACL.SUBMIT_APPLICATIONS, user));
    assertFalse(hasQueueACL(aclInfos, QueueACL.SUBMIT_APPLICATIONS, "root"));

    // c has no SA, but QA
    assertTrue(c.hasAccess(QueueACL.ADMINISTER_QUEUE, user));
    assertTrue(hasQueueACL(aclInfos,  QueueACL.ADMINISTER_QUEUE, "root.c"));
    assertFalse(c.hasAccess(QueueACL.SUBMIT_APPLICATIONS, user));
    assertFalse(hasQueueACL(aclInfos, QueueACL.SUBMIT_APPLICATIONS, "root.c"));

    //Queue c1 has QA, no SA (gotten perm from parent)
    assertTrue(c1.hasAccess(QueueACL.ADMINISTER_QUEUE, user)); 
    assertTrue(hasQueueACL(aclInfos,  QueueACL.ADMINISTER_QUEUE, "root.c.c1"));
    assertFalse(c1.hasAccess(QueueACL.SUBMIT_APPLICATIONS, user)); 
    assertFalse(hasQueueACL(
        aclInfos, QueueACL.SUBMIT_APPLICATIONS, "root.c.c1"));

    //Queue c11 has permissions from parent queue and SA
    assertTrue(c11.hasAccess(QueueACL.ADMINISTER_QUEUE, user));
    assertTrue(hasQueueACL(
        aclInfos,  QueueACL.ADMINISTER_QUEUE, "root.c.c1.c11"));
    assertTrue(c11.hasAccess(QueueACL.SUBMIT_APPLICATIONS, user));
    assertTrue(
        hasQueueACL(aclInfos, QueueACL.SUBMIT_APPLICATIONS, "root.c.c1.c11"));

    //Queue c111 has SA and AQ, both from parent
    assertTrue(c111.hasAccess(QueueACL.ADMINISTER_QUEUE, user));
    assertTrue(hasQueueACL(
        aclInfos,  QueueACL.ADMINISTER_QUEUE, "root.c.c1.c11.c111"));
    assertTrue(c111.hasAccess(QueueACL.SUBMIT_APPLICATIONS, user));
    assertTrue(hasQueueACL(
        aclInfos, QueueACL.SUBMIT_APPLICATIONS, "root.c.c1.c11.c111"));

    reset(c);
  }

  @Test
  public void testAbsoluteResourceWithChangeInClusterResource()
      throws Exception {
    // Setup queue configs
    setupSingleLevelQueuesWithAbsoluteResource(csConf);

    CSQueueStore queues = new CSQueueStore();
    CSQueue root = CapacitySchedulerQueueManager.parseQueue(csContext, csConf,
        null, CapacitySchedulerConfiguration.ROOT, queues, queues,
        TestUtils.spyHook);

    // Setup some nodes
    final int memoryPerNode = 10;
    int coresPerNode = 16;
    int numNodes = 2;

    Resource clusterResource = Resources.createResource(
        numNodes * (memoryPerNode * GB), numNodes * coresPerNode);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Start testing
    LeafQueue a = (LeafQueue) queues.get(A);
    LeafQueue b = (LeafQueue) queues.get(B);

    assertEquals(a.getQueueResourceQuotas().getConfiguredMinResource(),
        QUEUE_A_RESOURCE);
    assertEquals(b.getQueueResourceQuotas().getConfiguredMinResource(),
        QUEUE_B_RESOURCE);
    assertEquals(a.getQueueResourceQuotas().getEffectiveMinResource(),
        QUEUE_A_RESOURCE);
    assertEquals(b.getQueueResourceQuotas().getEffectiveMinResource(),
        QUEUE_B_RESOURCE);

    numNodes = 1;
    clusterResource = Resources.createResource(numNodes * (memoryPerNode * GB),
        numNodes * coresPerNode);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    Resource QUEUE_B_RESOURCE_HALF = Resource.newInstance(7 * 1024, 11);
    Resource QUEUE_A_RESOURCE_HALF = Resource.newInstance(3 * 1024, 5);
    assertEquals(a.getQueueResourceQuotas().getConfiguredMinResource(),
        QUEUE_A_RESOURCE);
    assertEquals(b.getQueueResourceQuotas().getConfiguredMinResource(),
        QUEUE_B_RESOURCE);
    assertEquals(a.getQueueResourceQuotas().getEffectiveMinResource(),
        QUEUE_A_RESOURCE_HALF);
    assertEquals(b.getQueueResourceQuotas().getEffectiveMinResource(),
        QUEUE_B_RESOURCE_HALF);

    coresPerNode = 40;
    clusterResource = Resources.createResource(numNodes * (memoryPerNode * GB),
        numNodes * coresPerNode);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    Resource QUEUE_B_RESOURCE_70PERC = Resource.newInstance(7 * 1024, 27);
    Resource QUEUE_A_RESOURCE_30PERC = Resource.newInstance(3 * 1024, 12);
    assertEquals(a.getQueueResourceQuotas().getConfiguredMinResource(),
        QUEUE_A_RESOURCE);
    assertEquals(b.getQueueResourceQuotas().getConfiguredMinResource(),
        QUEUE_B_RESOURCE);
    assertEquals(a.getQueueResourceQuotas().getEffectiveMinResource(),
        QUEUE_A_RESOURCE_30PERC);
    assertEquals(b.getQueueResourceQuotas().getEffectiveMinResource(),
        QUEUE_B_RESOURCE_70PERC);
  }

  @Test
  public void testDeriveCapacityFromAbsoluteConfigurations() throws Exception {
    // Setup queue configs
    setupSingleLevelQueuesWithAbsoluteResource(csConf);

    CSQueueStore queues = new CSQueueStore();
    CSQueue root = CapacitySchedulerQueueManager.parseQueue(csContext, csConf,
            null, CapacitySchedulerConfiguration.ROOT, queues, queues,
            TestUtils.spyHook);

    // Setup some nodes
    int numNodes = 2;
    final long memoryPerNode = (QUEUE_A_RESOURCE.getMemorySize() +
            QUEUE_B_RESOURCE.getMemorySize()) / numNodes;
    int coresPerNode = (QUEUE_A_RESOURCE.getVirtualCores() +
            QUEUE_B_RESOURCE.getVirtualCores()) / numNodes;

    Resource clusterResource = Resources.createResource(
            numNodes * memoryPerNode, numNodes * coresPerNode);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
            new ResourceLimits(clusterResource));

    // Start testing
    // Only MaximumSystemApplications is set in csConf
    LeafQueue a = (LeafQueue) queues.get(A);
    LeafQueue b = (LeafQueue) queues.get(B);

    float queueAScale = (float) QUEUE_A_RESOURCE.getMemorySize() /
            (float) clusterResource.getMemorySize();
    float queueBScale = (float) QUEUE_B_RESOURCE.getMemorySize() /
            (float) clusterResource.getMemorySize();

    assertEquals(queueAScale, a.getQueueCapacities().getCapacity(),
        DELTA);
    assertEquals(1f, a.getQueueCapacities().getMaximumCapacity(),
        DELTA);
    assertEquals(queueAScale, a.getQueueCapacities().getAbsoluteCapacity(),
        DELTA);
    assertEquals(1f,
        a.getQueueCapacities().getAbsoluteMaximumCapacity(), DELTA);
    assertEquals((int) (csConf.getMaximumSystemApplications() * queueAScale),
            a.getMaxApplications());
    assertEquals(a.getMaxApplications(), a.getMaxApplicationsPerUser());

    assertEquals(queueBScale,
        b.getQueueCapacities().getCapacity(), DELTA);
    assertEquals(1f,
        b.getQueueCapacities().getMaximumCapacity(), DELTA);
    assertEquals(queueBScale,
        b.getQueueCapacities().getAbsoluteCapacity(), DELTA);
    assertEquals(1f,
        b.getQueueCapacities().getAbsoluteMaximumCapacity(), DELTA);
    assertEquals((int) (csConf.getMaximumSystemApplications() * queueBScale),
            b.getMaxApplications());
    assertEquals(b.getMaxApplications(), b.getMaxApplicationsPerUser());

    // Set GlobalMaximumApplicationsPerQueue in csConf
    csConf.setGlobalMaximumApplicationsPerQueue(20000);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    assertEquals((int) (csConf.getGlobalMaximumApplicationsPerQueue() *
            queueAScale), a.getMaxApplications());
    assertEquals(a.getMaxApplications(), a.getMaxApplicationsPerUser());
    assertEquals((int) (csConf.getGlobalMaximumApplicationsPerQueue() *
            queueBScale), b.getMaxApplications());
    assertEquals(b.getMaxApplications(), b.getMaxApplicationsPerUser());

    // Set MaximumApplicationsPerQueue in csConf
    int queueAMaxApplications = 30000;
    int queueBMaxApplications = 30000;
    csConf.set("yarn.scheduler.capacity." + Q_A + ".maximum-applications",
            Integer.toString(queueAMaxApplications));
    csConf.set("yarn.scheduler.capacity." + Q_B + ".maximum-applications",
            Integer.toString(queueBMaxApplications));
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    assertEquals(queueAMaxApplications, a.getMaxApplications());
    assertEquals(a.getMaxApplications(), a.getMaxApplicationsPerUser());
    assertEquals(queueBMaxApplications, b.getMaxApplications());
    assertEquals(b.getMaxApplications(), b.getMaxApplicationsPerUser());

    // Extra cases for testing maxApplicationsPerUser
    int halfPercent = 50;
    int oneAndQuarterPercent = 125;
    a.getUsersManager().setUserLimit(halfPercent);
    b.getUsersManager().setUserLimit(oneAndQuarterPercent);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    assertEquals(a.getMaxApplications() * halfPercent / 100,
            a.getMaxApplicationsPerUser());
    // Q_B's limit per user shouldn't be greater
    // than the whole queue's application limit
    assertEquals(b.getMaxApplications(), b.getMaxApplicationsPerUser());

    float userLimitFactorQueueA = 0.9f;
    float userLimitFactorQueueB = 1.1f;
    a.getUsersManager().setUserLimit(halfPercent);
    a.getUsersManager().setUserLimitFactor(userLimitFactorQueueA);
    b.getUsersManager().setUserLimit(100);
    b.getUsersManager().setUserLimitFactor(userLimitFactorQueueB);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    assertEquals((int) (a.getMaxApplications() * halfPercent *
            userLimitFactorQueueA / 100), a.getMaxApplicationsPerUser());
    // Q_B's limit per user shouldn't be greater
    // than the whole queue's application limit
    assertEquals(b.getMaxApplications(), b.getMaxApplicationsPerUser());

  }

  @After
  public void tearDown() throws Exception {
  }

  private ResourceLimits anyResourceLimits() {
    return any(ResourceLimits.class);
  }
}
