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

import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.junit.After;
import org.junit.Test;

public class TestSchedulerApplicationAttempt {

  private static final NodeId nodeId = NodeId.newInstance("somehost", 5);

  private Configuration conf = new Configuration();
  
  @After
  public void tearDown() {
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.shutdown();
  }
  
  @Test
  public void testMove() {
    final String user = "user1";
    Queue parentQueue = createQueue("parent", null);
    Queue oldQueue = createQueue("old", parentQueue);
    Queue newQueue = createQueue("new", parentQueue);
    QueueMetrics parentMetrics = parentQueue.getMetrics();
    QueueMetrics oldMetrics = oldQueue.getMetrics();
    QueueMetrics newMetrics = newQueue.getMetrics();

    ApplicationAttemptId appAttId = createAppAttemptId(0, 0);
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getEpoch()).thenReturn(3L);
    SchedulerApplicationAttempt app = new SchedulerApplicationAttempt(appAttId,
        user, oldQueue, oldQueue.getActiveUsersManager(), rmContext);
    oldMetrics.submitApp(user);
    
    // confirm that containerId is calculated based on epoch.
    assertEquals(0x30000000001L, app.getNewContainerId());
    
    // Resource request
    Resource requestedResource = Resource.newInstance(1536, 2);
    Priority requestedPriority = Priority.newInstance(2);
    ResourceRequest request = ResourceRequest.newInstance(requestedPriority,
        ResourceRequest.ANY, requestedResource, 3);
    app.updateResourceRequests(Arrays.asList(request));

    // Allocated container
    RMContainer container1 = createRMContainer(appAttId, 1, requestedResource);
    app.liveContainers.put(container1.getContainerId(), container1);
    SchedulerNode node = createNode();
    app.appSchedulingInfo.allocate(NodeType.OFF_SWITCH, node, requestedPriority,
        request, container1.getContainer());
    
    // Reserved container
    Priority prio1 = Priority.newInstance(1);
    Resource reservedResource = Resource.newInstance(2048, 3);
    RMContainer container2 = createReservedRMContainer(appAttId, 1, reservedResource,
        node.getNodeID(), prio1);
    Map<NodeId, RMContainer> reservations = new HashMap<NodeId, RMContainer>();
    reservations.put(node.getNodeID(), container2);
    app.reservedContainers.put(prio1, reservations);
    oldMetrics.reserveResource(user, reservedResource);
    
    checkQueueMetrics(oldMetrics, 1, 1, 1536, 2, 2048, 3, 3072, 4);
    checkQueueMetrics(newMetrics, 0, 0, 0, 0, 0, 0, 0, 0);
    checkQueueMetrics(parentMetrics, 1, 1, 1536, 2, 2048, 3, 3072, 4);
    
    app.move(newQueue);
    
    checkQueueMetrics(oldMetrics, 0, 0, 0, 0, 0, 0, 0, 0);
    checkQueueMetrics(newMetrics, 1, 1, 1536, 2, 2048, 3, 3072, 4);
    checkQueueMetrics(parentMetrics, 1, 1, 1536, 2, 2048, 3, 3072, 4);
  }
  
  private void checkQueueMetrics(QueueMetrics metrics, int activeApps,
      int runningApps, int allocMb, int allocVcores, int reservedMb,
      int reservedVcores, int pendingMb, int pendingVcores) {
    assertEquals(activeApps, metrics.getActiveApps());
    assertEquals(runningApps, metrics.getAppsRunning());
    assertEquals(allocMb, metrics.getAllocatedMB());
    assertEquals(allocVcores, metrics.getAllocatedVirtualCores());
    assertEquals(reservedMb, metrics.getReservedMB());
    assertEquals(reservedVcores, metrics.getReservedVirtualCores());
    assertEquals(pendingMb, metrics.getPendingMB());
    assertEquals(pendingVcores, metrics.getPendingVirtualCores());
  }
  
  private SchedulerNode createNode() {
    SchedulerNode node = mock(SchedulerNode.class);
    when(node.getNodeName()).thenReturn("somehost");
    when(node.getRackName()).thenReturn("somerack");
    when(node.getNodeID()).thenReturn(nodeId);
    return node;
  }
  
  private RMContainer createReservedRMContainer(ApplicationAttemptId appAttId,
      int id, Resource resource, NodeId nodeId, Priority reservedPriority) {
    RMContainer container = createRMContainer(appAttId, id, resource);
    when(container.getReservedResource()).thenReturn(resource);
    when(container.getReservedPriority()).thenReturn(reservedPriority);
    when(container.getReservedNode()).thenReturn(nodeId);
    return container;
  }
  
  private RMContainer createRMContainer(ApplicationAttemptId appAttId, int id,
      Resource resource) {
    ContainerId containerId = ContainerId.newInstance(appAttId, id);
    RMContainer rmContainer = mock(RMContainer.class);
    Container container = mock(Container.class);
    when(container.getResource()).thenReturn(resource);
    when(container.getNodeId()).thenReturn(nodeId);
    when(rmContainer.getContainer()).thenReturn(container);
    when(rmContainer.getContainerId()).thenReturn(containerId);
    return rmContainer;
  }
  
  private Queue createQueue(String name, Queue parent) {
    QueueMetrics metrics = QueueMetrics.forQueue(name, parent, false, conf);
    ActiveUsersManager activeUsersManager = new ActiveUsersManager(metrics);
    Queue queue = mock(Queue.class);
    when(queue.getMetrics()).thenReturn(metrics);
    when(queue.getActiveUsersManager()).thenReturn(activeUsersManager);
    return queue;
  }
  
  private ApplicationAttemptId createAppAttemptId(int appId, int attemptId) {
    ApplicationId appIdImpl = ApplicationId.newInstance(0, appId);
    ApplicationAttemptId attId =
        ApplicationAttemptId.newInstance(appIdImpl, attemptId);
    return attId;
  }
}
