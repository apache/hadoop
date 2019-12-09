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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;

import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestChildQueueOrder {

  private static final Log LOG = LogFactory.getLog(TestChildQueueOrder.class);

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
    when(csContext.getResourceCalculator()).
        thenReturn(resourceComparator);
    when(csContext.getRMContext()).thenReturn(rmContext);
    when(csContext.getPreemptionManager()).thenReturn(new PreemptionManager());
  }

  private FiCaSchedulerApp getMockApplication(int appId, String user) {
    FiCaSchedulerApp application = mock(FiCaSchedulerApp.class);
    doReturn(user).when(application).getUser();
    doReturn(Resources.createResource(0, 0)).when(application).getHeadroom();
    return application;
  }

  private void stubQueueAllocation(final CSQueue queue,
      final Resource clusterResource, final FiCaSchedulerNode node,
      final int allocation) {
    stubQueueAllocation(queue, clusterResource, node, allocation,
        NodeType.NODE_LOCAL);
  }

  @SuppressWarnings("unchecked")
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
          LOG.info("FOOBAR q.assignContainers q=" + queue.getQueueName() + 
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
          doReturn(new CSAssignment(Resources.none(), type)).
              when(queue).assignContainers(eq(clusterResource),
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
    doNothing().when(node).releaseContainer(any(ContainerId.class),
        anyBoolean());
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

  private static final String A = "a";
  private static final String B = "b";
  private static final String C = "c";
  private static final String D = "d";

  private void setupSortedQueues(CapacitySchedulerConfiguration conf) {

    // Define queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {A, B, C, D});

    final String Q_A = CapacitySchedulerConfiguration.ROOT + "." + A;
    conf.setCapacity(Q_A, 25);

    final String Q_B = CapacitySchedulerConfiguration.ROOT + "." + B;
    conf.setCapacity(Q_B, 25);

    final String Q_C = CapacitySchedulerConfiguration.ROOT + "." + C;
    conf.setCapacity(Q_C, 25);

    final String Q_D = CapacitySchedulerConfiguration.ROOT + "." + D;
    conf.setCapacity(Q_D, 25);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSortedQueues() throws Exception {
    // Setup queue configs
    setupSortedQueues(csConf);
    Map<String, CSQueue> queues = new HashMap<String, CSQueue>();
    CSQueue root = 
        CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
          CapacitySchedulerConfiguration.ROOT, queues, queues, 
          TestUtils.spyHook);

    // Setup some nodes
    final int memoryPerNode = 10;
    final int coresPerNode = 16;
    final int numNodes = 1;

    FiCaSchedulerNode node_0 = 
      TestUtils.getMockNode("host_0", DEFAULT_RACK, 0, memoryPerNode*GB);
    doNothing().when(node_0).releaseContainer(any(ContainerId.class),
        anyBoolean());
    
    final Resource clusterResource = 
      Resources.createResource(numNodes * (memoryPerNode*GB), 
          numNodes * coresPerNode);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Start testing
    CSQueue a = queues.get(A);
    CSQueue b = queues.get(B);
    CSQueue c = queues.get(C);
    CSQueue d = queues.get(D);
    
    // Make a/b/c/d has >0 pending resource, so that allocation will continue.
    queues.get(CapacitySchedulerConfiguration.ROOT).getQueueResourceUsage()
        .incPending(Resources.createResource(1 * GB));
    a.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    b.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    c.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    d.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));

    final String user_0 = "user_0";

    // Stub an App and its containerCompleted
    FiCaSchedulerApp app_0 = getMockApplication(0,user_0);
    doReturn(true).when(app_0).containerCompleted(any(RMContainer.class),
        any(ContainerStatus.class), any(RMContainerEventType.class),
        any(String.class));

    Priority priority = TestUtils.createMockPriority(1); 
    ContainerAllocationExpirer expirer = 
      mock(ContainerAllocationExpirer.class);
    DrainDispatcher drainDispatcher = new DrainDispatcher();
    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getContainerAllocationExpirer()).thenReturn(expirer);
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        app_0.getApplicationId(), 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    Container container=TestUtils.getMockContainer(containerId, 
        node_0.getNodeID(), Resources.createResource(1*GB), priority);
    RMContainer rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), appAttemptId,
        node_0.getNodeID(), "user", rmContext);

    // Assign {1,2,3,4} 1GB containers respectively to queues
    stubQueueAllocation(a, clusterResource, node_0, 1*GB);
    stubQueueAllocation(b, clusterResource, node_0, 0*GB);
    stubQueueAllocation(c, clusterResource, node_0, 0*GB);
    stubQueueAllocation(d, clusterResource, node_0, 0*GB);
    root.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    for(int i=0; i < 2; i++)
    {
      stubQueueAllocation(a, clusterResource, node_0, 0*GB);
      stubQueueAllocation(b, clusterResource, node_0, 1*GB);
      stubQueueAllocation(c, clusterResource, node_0, 0*GB);
      stubQueueAllocation(d, clusterResource, node_0, 0*GB);
      root.assignContainers(clusterResource, node_0, new ResourceLimits(
          clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    } 
    for(int i=0; i < 3; i++)
    {
      stubQueueAllocation(a, clusterResource, node_0, 0*GB);
      stubQueueAllocation(b, clusterResource, node_0, 0*GB);
      stubQueueAllocation(c, clusterResource, node_0, 1*GB);
      stubQueueAllocation(d, clusterResource, node_0, 0*GB);
      root.assignContainers(clusterResource, node_0, new ResourceLimits(
          clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    }  
    for(int i=0; i < 4; i++)
    {
      stubQueueAllocation(a, clusterResource, node_0, 0*GB);
      stubQueueAllocation(b, clusterResource, node_0, 0*GB);
      stubQueueAllocation(c, clusterResource, node_0, 0*GB);
      stubQueueAllocation(d, clusterResource, node_0, 1*GB);
      root.assignContainers(clusterResource, node_0, new ResourceLimits(
          clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    }    
    verifyQueueMetrics(a, 1*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    verifyQueueMetrics(d, 4*GB, clusterResource);
    LOG.info("status child-queues: " + ((ParentQueue)root).
        getChildQueuesToPrint());

    //Release 3 x 1GB containers from D
    for(int i=0; i < 3;i++)
    {
      d.completedContainer(clusterResource, app_0, node_0,
          rmContainer, null, RMContainerEventType.KILL, null, true);
    }
    verifyQueueMetrics(a, 1*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    verifyQueueMetrics(d, 1*GB, clusterResource);
    //reset manually resources on node
    node_0 = TestUtils.getMockNode("host_0", DEFAULT_RACK, 0,
        (memoryPerNode-1-2-3-1)*GB);
    LOG.info("status child-queues: " + 
        ((ParentQueue)root).getChildQueuesToPrint());


    // Assign 2 x 1GB Containers to A 
    for(int i=0; i < 2; i++)
    {
      stubQueueAllocation(a, clusterResource, node_0, 1*GB);
      stubQueueAllocation(b, clusterResource, node_0, 0*GB);
      stubQueueAllocation(c, clusterResource, node_0, 0*GB);
      stubQueueAllocation(d, clusterResource, node_0, 0*GB);
      root.assignContainers(clusterResource, node_0, new ResourceLimits(
          clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    }
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    verifyQueueMetrics(d, 1*GB, clusterResource);
    LOG.info("status child-queues: " + 
        ((ParentQueue)root).getChildQueuesToPrint());

    //Release 1GB Container from A
    a.completedContainer(clusterResource, app_0, node_0, 
        rmContainer, null, RMContainerEventType.KILL, null, true);
    verifyQueueMetrics(a, 2*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    verifyQueueMetrics(d, 1*GB, clusterResource);
    //reset manually resources on node
    node_0 = TestUtils.getMockNode("host_0", DEFAULT_RACK, 0,
        (memoryPerNode-2-2-3-1)*GB);
    LOG.info("status child-queues: " + 
        ((ParentQueue)root).getChildQueuesToPrint());

    // Assign 1GB container to B 
    stubQueueAllocation(a, clusterResource, node_0, 0*GB);
    stubQueueAllocation(b, clusterResource, node_0, 1*GB);
    stubQueueAllocation(c, clusterResource, node_0, 0*GB);
    stubQueueAllocation(d, clusterResource, node_0, 0*GB);
    root.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(a, 2*GB, clusterResource);
    verifyQueueMetrics(b, 3*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    verifyQueueMetrics(d, 1*GB, clusterResource);
    LOG.info("status child-queues: " + 
        ((ParentQueue)root).getChildQueuesToPrint());

    //Release 1GB container resources from B
    b.completedContainer(clusterResource, app_0, node_0, 
        rmContainer, null, RMContainerEventType.KILL, null, true);
    verifyQueueMetrics(a, 2*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    verifyQueueMetrics(d, 1*GB, clusterResource);
    //reset manually resources on node
    node_0 = TestUtils.getMockNode("host_0", DEFAULT_RACK, 0, 
        (memoryPerNode-2-2-3-1)*GB);
    LOG.info("status child-queues: " + 
        ((ParentQueue)root).getChildQueuesToPrint());

    // Assign 1GB container to A
    stubQueueAllocation(a, clusterResource, node_0, 1*GB);
    stubQueueAllocation(b, clusterResource, node_0, 0*GB);
    stubQueueAllocation(c, clusterResource, node_0, 0*GB);
    stubQueueAllocation(d, clusterResource, node_0, 0*GB);
    root.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    verifyQueueMetrics(d, 1*GB, clusterResource);
    LOG.info("status child-queues: " + 
        ((ParentQueue)root).getChildQueuesToPrint());

    // Now do the real test, where B and D request a 1GB container
    // D should should get the next container if the order is correct
    stubQueueAllocation(a, clusterResource, node_0, 0*GB);
    stubQueueAllocation(b, clusterResource, node_0, 1*GB);
    stubQueueAllocation(c, clusterResource, node_0, 0*GB);
    stubQueueAllocation(d, clusterResource, node_0, 1*GB);
    root.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    InOrder allocationOrder = inOrder(d,b);
    allocationOrder.verify(d).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), any(ResourceLimits.class),
        any(SchedulingMode.class));
    allocationOrder.verify(b).assignContainers(eq(clusterResource),
        any(CandidateNodeSet.class), any(ResourceLimits.class),
        any(SchedulingMode.class));
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    verifyQueueMetrics(d, 2*GB, clusterResource); //D got the container
    LOG.info("status child-queues: " + 
        ((ParentQueue)root).getChildQueuesToPrint());
  }

  @After
  public void tearDown() throws Exception {
  }
}
