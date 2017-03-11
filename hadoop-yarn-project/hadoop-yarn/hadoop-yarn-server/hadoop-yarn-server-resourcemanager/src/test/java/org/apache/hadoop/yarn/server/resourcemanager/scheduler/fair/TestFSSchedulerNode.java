package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test scheduler node, especially preemption reservations.
 */
public class TestFSSchedulerNode {
  long containerNum = 0;
  ArrayList<RMContainer> containers = new ArrayList<>();

  private RMNode createNode() {
    RMNode node = mock(RMNode.class);
    when(node.getTotalCapability()).thenReturn(Resource.newInstance(8192, 8));
    when(node.getHostName()).thenReturn("host.domain.com");
    return node;
  }

  private RMContainer createContainer() {
    RMContainer container = mock(RMContainer.class);
    Container containerInner = mock(Container.class);
    ContainerId id = mock(ContainerId.class);
    when(id.getContainerId()).thenReturn(containerNum);
    when(containerInner.getResource()).
        thenReturn(Resource.newInstance(1024, 1));
    when(containerInner.getId()).thenReturn(id);
    when(containerInner.getExecutionType()).
        thenReturn(ExecutionType.GUARANTEED);
    when(container.getContainerId()).thenReturn(id);
    when(container.getContainer()).thenReturn(containerInner);
    when(container.getExecutionType()).thenReturn(ExecutionType.GUARANTEED);
    when(container.getAllocatedResource()).
        thenReturn(Resource.newInstance(1024, 1));
    containers.add(container);
    containerNum++;
    return container;
  }

  private void saturateCluster(FSSchedulerNode schedulerNode) {
    while (!Resources.isNone(schedulerNode.getUnallocatedResource())) {
      createContainer();
      schedulerNode.allocateContainer(containers.get((int)containerNum - 1));
      schedulerNode.containerStarted(containers.get((int)containerNum - 1).
          getContainerId());
    }
  }

  private FSAppAttempt createStarvingApp(FSSchedulerNode schedulerNode,
                                         Resource request) {
    FSAppAttempt starvingApp = mock(FSAppAttempt.class);
    when(starvingApp.assignContainer(schedulerNode)).thenAnswer(
        new Answer<Resource>() {
          @Override
          public Resource answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            Resource response = Resource.newInstance(0, 0);
            while (!Resources.isNone(request) &&
                !Resources.isNone(schedulerNode.getUnallocatedResource())) {
              RMContainer container = createContainer();
              schedulerNode.allocateContainer(container);
              Resources.addTo(response, container.getAllocatedResource());
              Resources.subtractFrom(request, container.getAllocatedResource());
            }
            return response;
          }
        });
    return starvingApp;
  }

  private void finalValidation(FSSchedulerNode schedulerNode) {
    assertEquals("Everything should have been released",
        Resources.none(), schedulerNode.getAllocatedResource());
    assertTrue("No containers should be reserved for preemption",
        schedulerNode.containersForPreemption.isEmpty());
    assertTrue("No resources should be reserved for preemptees",
        schedulerNode.reservedApp.isEmpty());
  }

  /**
   * Allocate and release a single container.
   */
  @Test
  public void testSimpleAllocation() {
    RMNode node = createNode();
    FSSchedulerNode schedulerNode = new FSSchedulerNode(node, false);

    createContainer();
    assertEquals("Nothing should have been allocated, yet",
        Resources.none(), schedulerNode.getAllocatedResource());
    schedulerNode.allocateContainer(containers.get(0));
    assertEquals("Container should be allocated",
        containers.get(0).getContainer().getResource(),
        schedulerNode.getAllocatedResource());
    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);
    assertEquals("Everything should have been released",
        Resources.none(), schedulerNode.getAllocatedResource());

    // Check that we are error prone
    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);
    finalValidation(schedulerNode);
  }

  /**
   * Allocate and release three containers with launch.
   */
  @Test
  public void testMultipleAllocations() {
    RMNode node = createNode();
    FSSchedulerNode schedulerNode = new FSSchedulerNode(node, false);

    createContainer();
    createContainer();
    createContainer();
    assertEquals("Nothing should have been allocated, yet",
        Resources.none(), schedulerNode.getAllocatedResource());
    schedulerNode.allocateContainer(containers.get(0));
    schedulerNode.containerStarted(containers.get(0).getContainerId());
    schedulerNode.allocateContainer(containers.get(1));
    schedulerNode.containerStarted(containers.get(1).getContainerId());
    schedulerNode.allocateContainer(containers.get(2));
    assertEquals("Container should be allocated",
        Resources.multiply(containers.get(0).getContainer().getResource(), 3.0),
        schedulerNode.getAllocatedResource());
    schedulerNode.releaseContainer(containers.get(1).getContainerId(), true);
    schedulerNode.releaseContainer(containers.get(2).getContainerId(), true);
    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);
    finalValidation(schedulerNode);
  }

  /**
   * Allocate and release a single container.
   */
  @Test
  public void testSimplePreemption() {
    RMNode node = createNode();
    FSSchedulerNode schedulerNode = new FSSchedulerNode(node, false);

    // Launch containers and saturate the cluster
    saturateCluster(schedulerNode);
    assertEquals("Container should be allocated",
        Resources.multiply(containers.get(0).getContainer().getResource(),
            containerNum),
        schedulerNode.getAllocatedResource());

    // Preempt a container
    FSAppAttempt starvingApp = createStarvingApp(schedulerNode,
        Resource.newInstance(1024, 1));
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(0)), starvingApp);

    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);
    schedulerNode.assignContainersToPreemptionReservees();
    assertEquals("Container should be allocated",
        schedulerNode.getTotalResource(),
        schedulerNode.getAllocatedResource());

    // Release all containers
    for (int i = 1; i < containerNum; ++i) {
      schedulerNode.releaseContainer(containers.get(i).getContainerId(), true);
    }
    finalValidation(schedulerNode);
  }

  /**
   * Allocate and release three containers requested by two apps.
   */
  @Test
  public void testComplexPreemption() {
    RMNode node = createNode();
    FSSchedulerNode schedulerNode = new FSSchedulerNode(node, false);

    // Launch containers and saturate the cluster
    saturateCluster(schedulerNode);
    assertEquals("Container should be allocated",
        Resources.multiply(containers.get(0).getContainer().getResource(),
            containerNum),
        schedulerNode.getAllocatedResource());

    // Preempt a container
    FSAppAttempt starvingApp1 = createStarvingApp(schedulerNode,
        Resource.newInstance(2048, 2));
    FSAppAttempt starvingApp2 = createStarvingApp(schedulerNode,
        Resource.newInstance(1024, 1));

    // Preemption thread kicks in
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(0)), starvingApp1);
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(1)), starvingApp1);
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(2)), starvingApp2);

    // Preemption happens
    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);
    schedulerNode.releaseContainer(containers.get(2).getContainerId(), true);
    schedulerNode.releaseContainer(containers.get(1).getContainerId(), true);

    schedulerNode.assignContainersToPreemptionReservees();
    assertEquals("Container should be allocated",
        schedulerNode.getTotalResource(),
        schedulerNode.getAllocatedResource());

    // Release all containers
    for (int i = 3; i < containerNum; ++i) {
      schedulerNode.releaseContainer(containers.get(i).getContainerId(), true);
    }
    finalValidation(schedulerNode);
  }

  /**
   * Allocate and release three containers requested by two apps
   * in two rounds.
   */
  @Test
  public void testMultiplePreemptionEvents() {
    RMNode node = createNode();
    FSSchedulerNode schedulerNode = new FSSchedulerNode(node, false);

    // Launch containers and saturate the cluster
    saturateCluster(schedulerNode);
    assertEquals("Container should be allocated",
        Resources.multiply(containers.get(0).getContainer().getResource(),
            containerNum),
        schedulerNode.getAllocatedResource());

    // Preempt a container
    FSAppAttempt starvingApp1 = createStarvingApp(schedulerNode, Resource.newInstance(2048, 2));
    FSAppAttempt starvingApp2 = createStarvingApp(schedulerNode, Resource.newInstance(1024, 1));

    // Preemption thread kicks in
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(0)), starvingApp1);
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(1)), starvingApp1);
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(2)), starvingApp2);

    // Preemption happens
    schedulerNode.releaseContainer(containers.get(1).getContainerId(), true);
    schedulerNode.assignContainersToPreemptionReservees();

    schedulerNode.releaseContainer(containers.get(2).getContainerId(), true);
    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);
    schedulerNode.assignContainersToPreemptionReservees();

    assertEquals("Container should be allocated",
        schedulerNode.getTotalResource(),
        schedulerNode.getAllocatedResource());

    // Release all containers
    for (int i = 3; i < containerNum; ++i) {
      schedulerNode.releaseContainer(containers.get(i).getContainerId(), true);
    }
    finalValidation(schedulerNode);
  }
}

