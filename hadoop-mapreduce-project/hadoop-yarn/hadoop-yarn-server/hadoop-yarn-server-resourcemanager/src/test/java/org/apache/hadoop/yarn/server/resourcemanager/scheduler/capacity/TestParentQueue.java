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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestParentQueue {

  private static final Log LOG = LogFactory.getLog(TestParentQueue.class);
  
  RMContext rmContext;
  CapacitySchedulerConfiguration csConf;
  CapacitySchedulerContext csContext;
  
  final static int GB = 1024;
  final static String DEFAULT_RACK = "/default";

  @Before
  public void setUp() throws Exception {
    rmContext = TestUtils.getMockRMContext();
    csConf = new CapacitySchedulerConfiguration();
    
    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(GB));
    when(csContext.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(16*GB));
    when(csContext.getClusterResources()).
        thenReturn(Resources.createResource(100 * 16 * GB));
  }
  
  private static final String A = "a";
  private static final String B = "b";
  private void setupSingleLevelQueues(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(CapacityScheduler.ROOT, new String[] {A, B});
    conf.setCapacity(CapacityScheduler.ROOT, 100);
    
    final String Q_A = CapacityScheduler.ROOT + "." + A;
    conf.setCapacity(Q_A, 30);
    
    final String Q_B = CapacityScheduler.ROOT + "." + B;
    conf.setCapacity(Q_B, 70);
    
    LOG.info("Setup top-level queues a and b");
  }

  private SchedulerApp getMockApplication(int appId, String user) {
    SchedulerApp application = mock(SchedulerApp.class);
    doReturn(user).when(application).getUser();
    doReturn(null).when(application).getHeadroom();
    return application;
  }

  private void stubQueueAllocation(final CSQueue queue, 
      final Resource clusterResource, final SchedulerNode node, 
      final int allocation) {
    
    // Simulate the queue allocation
    doAnswer(new Answer<Resource>() {
      @Override
      public Resource answer(InvocationOnMock invocation) throws Throwable {
        try {
          throw new Exception();
        } catch (Exception e) {
          LOG.info("FOOBAR q.assignContainers q=" + queue.getQueueName() + 
              " alloc=" + allocation + " node=" + node.getHostName());
        }
        final Resource allocatedResource = Resources.createResource(allocation);
        if (queue instanceof ParentQueue) {
          ((ParentQueue)queue).allocateResource(clusterResource, 
              allocatedResource);
        } else {
          SchedulerApp app1 = getMockApplication(0, "");
          ((LeafQueue)queue).allocateResource(clusterResource, app1, 
              allocatedResource);
        }
        
        // Next call - nothing
        if (allocation > 0) {
          doReturn(Resources.none()).when(queue).assignContainers(
              eq(clusterResource), eq(node));

          // Mock the node's resource availability
          Resource available = node.getAvailableResource();
          doReturn(Resources.subtractFrom(available, allocatedResource)).
          when(node).getAvailableResource();
        }

        return allocatedResource;
      }
    }).
    when(queue).assignContainers(eq(clusterResource), eq(node));
  }
  
  private float computeQueueUtilization(CSQueue queue, 
      int expectedMemory, Resource clusterResource) {
    return (expectedMemory / 
        (clusterResource.getMemory() * queue.getAbsoluteCapacity()));
  }
  
  @Test
  public void testSingleLevelQueues() throws Exception {
    // Setup queue configs
    setupSingleLevelQueues(csConf);
    
    Map<String, CSQueue> queues = new HashMap<String, CSQueue>();
    CSQueue root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, 
            CapacityScheduler.ROOT, queues, queues, 
            CapacityScheduler.queueComparator, 
            CapacityScheduler.applicationComparator,
            TestUtils.spyHook);

    // Setup some nodes
    final int memoryPerNode = 10;
    final int numNodes = 2;
    
    SchedulerNode node_0 = 
        TestUtils.getMockNode("host_0", DEFAULT_RACK, 0, memoryPerNode*GB);
    SchedulerNode node_1 = 
        TestUtils.getMockNode("host_1", DEFAULT_RACK, 0, memoryPerNode*GB);
    
    final Resource clusterResource = 
        Resources.createResource(numNodes * (memoryPerNode*GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Start testing
    LeafQueue a = (LeafQueue)queues.get(A);
    LeafQueue b = (LeafQueue)queues.get(B);
    final float delta = 0.0001f;
    
    // Simulate B returning a container on node_0
    stubQueueAllocation(a, clusterResource, node_0, 0*GB);
    stubQueueAllocation(b, clusterResource, node_0, 1*GB);
    root.assignContainers(clusterResource, node_0);
    assertEquals(0.0f, a.getUtilization(), delta);
    assertEquals(computeQueueUtilization(b, 1*GB, clusterResource), 
        b.getUtilization(), delta);
    
    // Now, A should get the scheduling opportunity since A=0G/6G, B=1G/14G
    stubQueueAllocation(a, clusterResource, node_1, 2*GB);
    stubQueueAllocation(b, clusterResource, node_1, 1*GB);
    root.assignContainers(clusterResource, node_1);
    InOrder allocationOrder = inOrder(a, b);
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    assertEquals(computeQueueUtilization(a, 2*GB, clusterResource), 
        a.getUtilization(), delta);
    assertEquals(computeQueueUtilization(b, 2*GB, clusterResource), 
        b.getUtilization(), delta);

    // Now, B should get the scheduling opportunity 
    // since A has 2/6G while B has 2/14G
    stubQueueAllocation(a, clusterResource, node_0, 1*GB);
    stubQueueAllocation(b, clusterResource, node_0, 2*GB);
    root.assignContainers(clusterResource, node_0);
    allocationOrder = inOrder(b, a);
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    assertEquals(computeQueueUtilization(a, 3*GB, clusterResource), 
        a.getUtilization(), delta);
    assertEquals(computeQueueUtilization(b, 4*GB, clusterResource), 
        b.getUtilization(), delta);

    // Now, B should still get the scheduling opportunity 
    // since A has 3/6G while B has 4/14G
    stubQueueAllocation(a, clusterResource, node_0, 0*GB);
    stubQueueAllocation(b, clusterResource, node_0, 4*GB);
    root.assignContainers(clusterResource, node_0);
    allocationOrder = inOrder(b, a);
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    assertEquals(computeQueueUtilization(a, 3*GB, clusterResource), 
        a.getUtilization(), delta);
    assertEquals(computeQueueUtilization(b, 8*GB, clusterResource), 
        b.getUtilization(), delta);

    // Now, A should get the scheduling opportunity 
    // since A has 3/6G while B has 8/14G
    stubQueueAllocation(a, clusterResource, node_1, 1*GB);
    stubQueueAllocation(b, clusterResource, node_1, 1*GB);
    root.assignContainers(clusterResource, node_1);
    allocationOrder = inOrder(a, b);
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    assertEquals(computeQueueUtilization(a, 4*GB, clusterResource), 
        a.getUtilization(), delta);
    assertEquals(computeQueueUtilization(b, 9*GB, clusterResource), 
        b.getUtilization(), delta);
  }

  private static final String C = "c";
  private static final String D = "d";
  private static final String A1 = "a1";
  private static final String A2 = "a2";
  private static final String B1 = "b1";
  private static final String B2 = "b2";
  private static final String B3 = "b3";
  
  private void setupMultiLevelQueues(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(CapacityScheduler.ROOT, new String[] {A, B, C, D});
    conf.setCapacity(CapacityScheduler.ROOT, 100);
    
    final String Q_A = CapacityScheduler.ROOT + "." + A;
    conf.setCapacity(Q_A, 10);
    
    final String Q_B = CapacityScheduler.ROOT + "." + B;
    conf.setCapacity(Q_B, 50);
    
    final String Q_C = CapacityScheduler.ROOT + "." + C;
    conf.setCapacity(Q_C, 20);
    
    final String Q_D = CapacityScheduler.ROOT + "." + D;
    conf.setCapacity(Q_D, 20);
    
    // Define 2-nd level queues
    conf.setQueues(Q_A, new String[] {A1, A2});
    conf.setCapacity(Q_A + "." + A1, 50);
    conf.setCapacity(Q_A + "." + A2, 50);
    
    conf.setQueues(Q_B, new String[] {B1, B2, B3});
    conf.setCapacity(Q_B + "." + B1, 10);
    conf.setCapacity(Q_B + "." + B2, 20);
    conf.setCapacity(Q_B + "." + B3, 70);
  }



  @Test
  public void testMultiLevelQueues() throws Exception {
    // Setup queue configs
    setupMultiLevelQueues(csConf);
    
    Map<String, CSQueue> queues = new HashMap<String, CSQueue>();
    CSQueue root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, 
            CapacityScheduler.ROOT, queues, queues, 
            CapacityScheduler.queueComparator, 
            CapacityScheduler.applicationComparator,
            TestUtils.spyHook);
    
    // Setup some nodes
    final int memoryPerNode = 10;
    final int numNodes = 3;
    
    SchedulerNode node_0 = 
        TestUtils.getMockNode("host_0", DEFAULT_RACK, 0, memoryPerNode*GB);
    SchedulerNode node_1 = 
        TestUtils.getMockNode("host_1", DEFAULT_RACK, 0, memoryPerNode*GB);
    SchedulerNode node_2 = 
        TestUtils.getMockNode("host_2", DEFAULT_RACK, 0, memoryPerNode*GB);
    
    final Resource clusterResource = 
        Resources.createResource(numNodes * (memoryPerNode*GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Start testing
    CSQueue a = queues.get(A);
    CSQueue b = queues.get(B);
    CSQueue c = queues.get(C);
    CSQueue d = queues.get(D);

    CSQueue a1 = queues.get(A1);
    CSQueue a2 = queues.get(A2);

    CSQueue b1 = queues.get(B1);
    CSQueue b2 = queues.get(B2);
    CSQueue b3 = queues.get(B3);

    final float delta = 0.0001f;
    
    // Simulate C returning a container on node_0
    stubQueueAllocation(a, clusterResource, node_0, 0*GB);
    stubQueueAllocation(b, clusterResource, node_0, 0*GB);
    stubQueueAllocation(c, clusterResource, node_0, 1*GB);
    stubQueueAllocation(d, clusterResource, node_0, 0*GB);
    root.assignContainers(clusterResource, node_0);
    assertEquals(computeQueueUtilization(a, 0*GB, clusterResource), 
        a.getUtilization(), delta);
    assertEquals(computeQueueUtilization(b, 0*GB, clusterResource), 
        b.getUtilization(), delta);
    assertEquals(computeQueueUtilization(c, 1*GB, clusterResource), 
        c.getUtilization(), delta);
    assertEquals(computeQueueUtilization(d, 0*GB, clusterResource), 
        d.getUtilization(), delta);
    reset(a); reset(b); reset(c);

    // Now get B2 to allocate
    // A = 0/3, B = 0/15, C = 1/6, D=0/6
    stubQueueAllocation(a, clusterResource, node_1, 0*GB);
    stubQueueAllocation(b2, clusterResource, node_1, 4*GB);
    stubQueueAllocation(c, clusterResource, node_1, 0*GB);
    root.assignContainers(clusterResource, node_1);
    assertEquals(computeQueueUtilization(a, 0*GB, clusterResource), 
        a.getUtilization(), delta);
    assertEquals(computeQueueUtilization(b, 4*GB, clusterResource), 
        b.getUtilization(), delta);
    assertEquals(computeQueueUtilization(c, 1*GB, clusterResource), 
        c.getUtilization(), delta);
    reset(a); reset(b); reset(c);
    
    // Now get both A1, C & B3 to allocate in right order
    // A = 0/3, B = 4/15, C = 1/6, D=0/6
    stubQueueAllocation(a1, clusterResource, node_0, 1*GB);
    stubQueueAllocation(b3, clusterResource, node_0, 2*GB);
    stubQueueAllocation(c, clusterResource, node_0, 2*GB);
    root.assignContainers(clusterResource, node_0);
    InOrder allocationOrder = inOrder(a, c, b);
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    allocationOrder.verify(c).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    assertEquals(computeQueueUtilization(a, 1*GB, clusterResource), 
        a.getUtilization(), delta);
    assertEquals(computeQueueUtilization(b, 6*GB, clusterResource), 
        b.getUtilization(), delta);
    assertEquals(computeQueueUtilization(c, 3*GB, clusterResource), 
        c.getUtilization(), delta);
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
    root.assignContainers(clusterResource, node_2);
    allocationOrder = inOrder(a, a2, a1, b, c);
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    allocationOrder.verify(a2).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    allocationOrder.verify(c).assignContainers(eq(clusterResource), 
        any(SchedulerNode.class));
    assertEquals(computeQueueUtilization(a, 3*GB, clusterResource), 
        a.getUtilization(), delta);
    assertEquals(computeQueueUtilization(b, 8*GB, clusterResource), 
        b.getUtilization(), delta);
    assertEquals(computeQueueUtilization(c, 4*GB, clusterResource), 
        c.getUtilization(), delta);
    reset(a); reset(b); reset(c);
    
  }
  
  @After
  public void tearDown() throws Exception {
  }
}
