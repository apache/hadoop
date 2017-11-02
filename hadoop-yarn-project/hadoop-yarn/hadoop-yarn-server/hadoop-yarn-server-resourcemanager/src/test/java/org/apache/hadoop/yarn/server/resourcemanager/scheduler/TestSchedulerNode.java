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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.NoOpSystemMetricPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for SchedulerNode.
 */
public class TestSchedulerNode {
  private final Resource nodeCapacity = Resource.newInstance(1024*10, 4);

  @Test
  public void testAllocateAndReleaseGuaranteedContainer() {
    SchedulerNode schedulerNode = createSchedulerNode(nodeCapacity);
    Resource resource = Resource.newInstance(4096, 1);
    RMContainer container = createRMContainer(0, resource,
        ExecutionType.GUARANTEED, schedulerNode.getRMNode());
    ContainerId containerId = container.getContainerId();

    // allocate a container on the node
    schedulerNode.allocateContainer(container);

    Assert.assertEquals("The container should have been allocated",
        resource, schedulerNode.getAllocatedResource());
    Assert.assertEquals("Incorrect remaining resource accounted.",
        Resources.subtract(nodeCapacity, resource),
        schedulerNode.getUnallocatedResource());
    Assert.assertEquals("The container should have been allocated" +
        " but not launched", resource,
        schedulerNode.getResourceAllocatedPendingLaunch());
    Assert.assertEquals("The container should have been allocated",
        1, schedulerNode.getNumGuaranteedContainers());
    Assert.assertTrue(
        schedulerNode.isValidGuaranteedContainer(containerId));

    // launch the container on the node
    schedulerNode.containerLaunched(containerId);

    Assert.assertEquals("The container should have been launched",
        Resources.none(), schedulerNode.getResourceAllocatedPendingLaunch());

    // release the container
    schedulerNode.releaseContainer(containerId, true);
    Assert.assertEquals("The container should have been released",
        0, schedulerNode.getNumGuaranteedContainers());
    Assert.assertEquals("The container should have been released",
        Resources.none(), schedulerNode.getAllocatedResource());
  }

  @Test
  public void testAllocateAndReleaseOpportunisticContainer() {
    SchedulerNode schedulerNode = createSchedulerNode(nodeCapacity);
    Resource resource = Resource.newInstance(4096, 1);
    RMContainer container = createRMContainer(0, resource,
        ExecutionType.OPPORTUNISTIC, schedulerNode.getRMNode());
    ContainerId containerId = container.getContainerId();

    // allocate a container on the node
    schedulerNode.allocateContainer(container);

    Assert.assertEquals("The container should have been allocated",
        resource, schedulerNode.getOpportunisticResourceAllocated());
    Assert.assertEquals("Incorrect remaining resource accounted.",
        nodeCapacity, schedulerNode.getUnallocatedResource());
    Assert.assertEquals("The container should have been allocated" +
        " but not launched", resource,
        schedulerNode.getResourceAllocatedPendingLaunch());
    Assert.assertEquals("The container should have been allocated",
        1, schedulerNode.getNumOpportunisticContainers());
    Assert.assertTrue(
        schedulerNode.isValidOpportunisticContainer(containerId));

    // launch the container on the node
    schedulerNode.containerLaunched(containerId);

    Assert.assertEquals("The container should have been launched",
        Resources.none(), schedulerNode.getResourceAllocatedPendingLaunch());

    // release the container
    schedulerNode.releaseContainer(containerId, true);
    Assert.assertEquals("The container should have been released",
        0, schedulerNode.getNumOpportunisticContainers());
    Assert.assertEquals("The container should have been released",
        Resources.none(), schedulerNode.getOpportunisticResourceAllocated());
    Assert.assertFalse("The container should have been released",
        schedulerNode.isValidOpportunisticContainer(containerId));
  }

  @Test
  public void testAllocateAndReleaseContainers() {
    SchedulerNode schedulerNode = createSchedulerNode(nodeCapacity);

    Resource guaranteedResource = Resource.newInstance(4096, 1);
    RMContainer guaranteedContainer =
        createRMContainer(0, guaranteedResource,
            ExecutionType.GUARANTEED, schedulerNode.getRMNode());
    ContainerId guaranteedContainerId = guaranteedContainer.getContainerId();

    // allocate a guaranteed container on the node
    schedulerNode.allocateContainer(guaranteedContainer);

    Assert.assertEquals("The guaranteed container should have been allocated",
        guaranteedResource, schedulerNode.getAllocatedResource());
    Assert.assertEquals("Incorrect remaining resource accounted.",
        Resources.subtract(nodeCapacity, guaranteedResource),
        schedulerNode.getUnallocatedResource());
    Assert.assertEquals("The guaranteed container should have been allocated" +
            " but not launched", guaranteedResource,
        schedulerNode.getResourceAllocatedPendingLaunch());
    Assert.assertEquals("The container should have been allocated",
        1, schedulerNode.getNumGuaranteedContainers());
    Assert.assertTrue(
        schedulerNode.isValidGuaranteedContainer(guaranteedContainerId));

    Resource opportunisticResource = Resource.newInstance(8192, 4);
    RMContainer opportunisticContainer =
        createRMContainer(1, opportunisticResource,
            ExecutionType.OPPORTUNISTIC, schedulerNode.getRMNode());
    ContainerId opportunisticContainerId =
        opportunisticContainer.getContainerId();

    // allocate an opportunistic container on the node
    schedulerNode.allocateContainer(opportunisticContainer);

    Assert.assertEquals("The opportunistic container should have been" +
        " allocated", opportunisticResource,
        schedulerNode.getOpportunisticResourceAllocated());
    Assert.assertEquals("Incorrect remaining resource accounted.",
        Resources.subtract(nodeCapacity, guaranteedResource),
        schedulerNode.getUnallocatedResource());
    Assert.assertEquals("The opportunistic container should also have been" +
        " allocated but not launched",
        Resources.add(guaranteedResource, opportunisticResource),
        schedulerNode.getResourceAllocatedPendingLaunch());
    Assert.assertEquals("The container should have been allocated",
        1, schedulerNode.getNumOpportunisticContainers());
    Assert.assertTrue(
        schedulerNode.isValidOpportunisticContainer(opportunisticContainerId));

    // launch both containers on the node
    schedulerNode.containerLaunched(guaranteedContainerId);
    schedulerNode.containerLaunched(opportunisticContainerId);

    Assert.assertEquals("Both containers should have been launched",
        Resources.none(), schedulerNode.getResourceAllocatedPendingLaunch());

    // release both containers
    schedulerNode.releaseContainer(guaranteedContainerId, true);
    schedulerNode.releaseContainer(opportunisticContainerId, true);

    Assert.assertEquals("The guaranteed container should have been released",
        0, schedulerNode.getNumGuaranteedContainers());
    Assert.assertEquals("The opportunistic container should have been released",
        0, schedulerNode.getNumOpportunisticContainers());
    Assert.assertEquals("The guaranteed container should have been released",
        Resources.none(), schedulerNode.getAllocatedResource());
    Assert.assertEquals("The opportunistic container should have been released",
        Resources.none(), schedulerNode.getOpportunisticResourceAllocated());
    Assert.assertFalse("The guaranteed container should have been released",
        schedulerNode.isValidGuaranteedContainer(guaranteedContainerId));
    Assert.assertFalse("The opportunistic container should have been released",
        schedulerNode.isValidOpportunisticContainer(opportunisticContainerId));
  }

  @Test
  public void testReleaseLaunchedContainerNotAsNode() {
    SchedulerNode schedulerNode = createSchedulerNode(nodeCapacity);
    Resource resource = Resource.newInstance(4096, 1);
    RMContainer container = createRMContainer(0, resource,
        ExecutionType.GUARANTEED, schedulerNode.getRMNode());
    ContainerId containerId = container.getContainerId();

    // allocate a container on the node
    schedulerNode.allocateContainer(container);

    Assert.assertEquals("The container should have been allocated",
        resource, schedulerNode.getAllocatedResource());
    Assert.assertEquals("Incorrect remaining resource accounted.",
        Resources.subtract(nodeCapacity, resource),
        schedulerNode.getUnallocatedResource());
    Assert.assertEquals("The container should have been allocated" +
        " but not launched", resource,
        schedulerNode.getResourceAllocatedPendingLaunch());
    Assert.assertEquals("The container should have been allocated",
        1, schedulerNode.getNumGuaranteedContainers());
    Assert.assertTrue(
        schedulerNode.isValidGuaranteedContainer(containerId));

    // launch the container on the node
    schedulerNode.containerLaunched(containerId);

    Assert.assertEquals("The container should have been launched",
        Resources.none(), schedulerNode.getResourceAllocatedPendingLaunch());

    // release the container
    schedulerNode.releaseContainer(containerId, false);
    Assert.assertEquals("The container should not have been released",
        1, schedulerNode.getNumGuaranteedContainers());
    Assert.assertEquals("The container should not have been released",
        resource, schedulerNode.getAllocatedResource());
    Assert.assertTrue("The container should not have been released",
        schedulerNode.isValidGuaranteedContainer(containerId));
  }

  @Test
  public void testReleaseUnlaunchedContainerAsNode() {
    SchedulerNode schedulerNode = createSchedulerNode(nodeCapacity);
    Resource resource = Resource.newInstance(4096, 1);
    RMContainer container = createRMContainer(0, resource,
        ExecutionType.GUARANTEED, schedulerNode.getRMNode());
    ContainerId containerId = container.getContainerId();

    // allocate a container on the node
    schedulerNode.allocateContainer(container);

    Assert.assertEquals("The container should have been allocated",
        resource, schedulerNode.getAllocatedResource());
    Assert.assertEquals("Incorrect remaining resource accounted.",
        Resources.subtract(nodeCapacity, resource),
        schedulerNode.getUnallocatedResource());
    Assert.assertEquals("The container should have been allocated" +
        " but not launched",
        resource, schedulerNode.getResourceAllocatedPendingLaunch());
    Assert.assertEquals("The container should have been allocated",
        1, schedulerNode.getNumGuaranteedContainers());
    Assert.assertTrue(
        schedulerNode.isValidGuaranteedContainer(containerId));

    // make sure the container is not launched yet
    Assert.assertEquals("The container should not be launched already",
        resource, schedulerNode.getResourceAllocatedPendingLaunch());

    // release the container
    schedulerNode.releaseContainer(containerId, true);
    Assert.assertEquals("The container should have been released",
        0, schedulerNode.getNumGuaranteedContainers());
    Assert.assertEquals("The container should have been released",
        Resources.none(), schedulerNode.getAllocatedResource());
    Assert.assertFalse("The container should have been released",
        schedulerNode.isValidGuaranteedContainer(containerId));
    Assert.assertEquals("The container should have been released",
        Resources.none(), schedulerNode.getResourceAllocatedPendingLaunch());
  }

  @Test
  public void testReleaseUnlaunchedContainerNotAsNode() {
    SchedulerNode schedulerNode = createSchedulerNode(nodeCapacity);
    Resource resource = Resource.newInstance(4096, 1);
    RMContainer container = createRMContainer(0, resource,
        ExecutionType.GUARANTEED, schedulerNode.getRMNode());
    ContainerId containerId = container.getContainerId();

    // allocate a container on the node
    schedulerNode.allocateContainer(container);

    Assert.assertEquals("The container should have been allocated",
        resource, schedulerNode.getAllocatedResource());
    Assert.assertEquals("Incorrect remaining resource accounted.",
        Resources.subtract(nodeCapacity, resource),
        schedulerNode.getUnallocatedResource());
    Assert.assertEquals("The container should have been allocated" +
        " but not launched", resource,
        schedulerNode.getResourceAllocatedPendingLaunch());
    Assert.assertEquals("The container should have been allocated",
        1, schedulerNode.getNumGuaranteedContainers());
    Assert.assertTrue(
        schedulerNode.isValidGuaranteedContainer(containerId));

    // make sure the container is not launched yet
    Assert.assertEquals("The container should not have been launched",
        resource, schedulerNode.getResourceAllocatedPendingLaunch());

    // release the container
    schedulerNode.releaseContainer(containerId, false);
    Assert.assertEquals("The container should have been released",
        0, schedulerNode.getNumGuaranteedContainers());
    Assert.assertEquals("The container should have been released",
        Resources.none(), schedulerNode.getAllocatedResource());
    Assert.assertFalse("The container should have been released",
        schedulerNode.isValidGuaranteedContainer(containerId));
    Assert.assertEquals("The container should have been released",
        Resources.none(), schedulerNode.getResourceAllocatedPendingLaunch());
  }

  private SchedulerNode createSchedulerNode(Resource capacity) {
    NodeId nodeId = NodeId.newInstance("localhost", 0);

    RMNode rmNode = mock(RMNode.class);
    when(rmNode.getNodeID()).thenReturn(nodeId);
    when(rmNode.getHostName()).thenReturn(nodeId.getHost());
    when(rmNode.getTotalCapability()).thenReturn(capacity);
    when(rmNode.getRackName()).thenReturn("/default");
    when(rmNode.getHttpAddress()).thenReturn(nodeId.getHost());
    when(rmNode.getNodeAddress()).thenReturn(nodeId.getHost());

    return new SchedulerNodeForTest(rmNode);
  }

  private static RMContainerImpl createRMContainer(long containerId,
      Resource resource, ExecutionType executionType, RMNode node) {
    Container container =
        createContainer(containerId, resource, executionType, node);

    Dispatcher dispatcher = new AsyncDispatcher();
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getDispatcher()).thenReturn(dispatcher);
    when(rmContext.getSystemMetricsPublisher()).
        thenReturn(new NoOpSystemMetricPublisher());
    when(rmContext.getYarnConfiguration()).
        thenReturn(new YarnConfiguration());
    when(rmContext.getContainerAllocationExpirer()).
        thenReturn(new ContainerAllocationExpirer(dispatcher));
    when(rmContext.getRMApplicationHistoryWriter()).
        thenReturn(new RMApplicationHistoryWriter());

    return new RMContainerImpl(container, null,
        container.getId().getApplicationAttemptId(),
        node.getNodeID(), "test", rmContext);
  }

  private static Container createContainer(long containerId, Resource resource,
      ExecutionType executionType, RMNode node) {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.
        newInstance(ApplicationId.newInstance(0, 0), 0);
    ContainerId cId =
        ContainerId.newContainerId(appAttemptId, containerId);
    Container container = Container.newInstance(
        cId, node.getNodeID(), node.getNodeAddress(), resource,
        Priority.newInstance(0), null, executionType);
    return container;
  }


  /**
   * A test implementation of SchedulerNode for the purpose of testing
   * SchedulerNode only. Resource reservation is scheduler-dependent,
   * and therefore not covered here.
   */
  private static final class SchedulerNodeForTest extends SchedulerNode {
    SchedulerNodeForTest(RMNode node) {
      super(node, false);
    }

    @Override
    public void reserveResource(SchedulerApplicationAttempt attempt,
        SchedulerRequestKey schedulerKey, RMContainer container) {
    }

    @Override
    public void unreserveResource(SchedulerApplicationAttempt attempt) {
    }
  }
}
