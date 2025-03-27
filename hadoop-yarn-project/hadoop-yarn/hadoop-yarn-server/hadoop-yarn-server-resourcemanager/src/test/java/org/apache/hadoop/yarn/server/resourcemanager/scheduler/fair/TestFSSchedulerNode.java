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

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test scheduler node, especially preemption reservations.
 */
public class TestFSSchedulerNode {
  private final ArrayList<RMContainer> containers = new ArrayList<>();

  private RMNode createNode() {
    RMNode node = mock(RMNode.class);
    when(node.getTotalCapability()).thenReturn(Resource.newInstance(8192, 8));
    when(node.getHostName()).thenReturn("host.domain.com");
    return node;
  }

  private void createDefaultContainer() {
    createContainer(Resource.newInstance(1024, 1), null);
  }

  private RMContainer createContainer(
      Resource request, ApplicationAttemptId appAttemptId) {
    RMContainer container = mock(RMContainer.class);
    Container containerInner = mock(Container.class);
    ContainerId id = mock(ContainerId.class);
    when(id.getContainerId()).thenReturn((long)containers.size());
    when(containerInner.getResource()).
        thenReturn(Resources.clone(request));
    when(containerInner.getId()).thenReturn(id);
    when(containerInner.getExecutionType()).
        thenReturn(ExecutionType.GUARANTEED);
    when(container.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(container.getContainerId()).thenReturn(id);
    when(container.getContainer()).thenReturn(containerInner);
    when(container.getExecutionType()).thenReturn(ExecutionType.GUARANTEED);
    when(container.getAllocatedResource()).
        thenReturn(Resources.clone(request));
    when(container.compareTo(any())).thenAnswer(new Answer<Integer>() {
      public Integer answer(InvocationOnMock invocation) {
        return
            Long.compare(
            ((RMContainer)invocation.getMock()).getContainerId()
                .getContainerId(),
            ((RMContainer)invocation.getArguments()[0]).getContainerId()
                .getContainerId());
      }
    });
    containers.add(container);
    return container;
  }

  private void saturateCluster(FSSchedulerNode schedulerNode) {
    while (!Resources.isNone(schedulerNode.getUnallocatedResource())) {
      createDefaultContainer();
      schedulerNode.allocateContainer(containers.get(containers.size() - 1));
      schedulerNode.containerStarted(containers.get(containers.size() - 1).
          getContainerId());
    }
  }

  private FSAppAttempt createStarvingApp(FSSchedulerNode schedulerNode,
                                         Resource request) {
    FSAppAttempt starvingApp = mock(FSAppAttempt.class);
    ApplicationAttemptId appAttemptId =
        mock(ApplicationAttemptId.class);
    when(starvingApp.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(starvingApp.assignContainer(schedulerNode)).thenAnswer(
        new Answer<Resource>() {
          @Override
          public Resource answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            Resource response = Resource.newInstance(0, 0);
            while (!Resources.isNone(request) &&
                !Resources.isNone(schedulerNode.getUnallocatedResource())) {
              RMContainer container = createContainer(request, appAttemptId);
              schedulerNode.allocateContainer(container);
              Resources.addTo(response, container.getAllocatedResource());
              Resources.subtractFrom(request,
                  container.getAllocatedResource());
            }
            return response;
          }
        });
    when(starvingApp.isStarved()).thenAnswer(
        new Answer<Boolean>() {
          @Override
          public Boolean answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            return !Resources.isNone(request);
          }
        }
    );
    when(starvingApp.getPendingDemand()).thenReturn(request);
    return starvingApp;
  }

  private void finalValidation(FSSchedulerNode schedulerNode) {
    assertEquals(Resources.none(), schedulerNode.getAllocatedResource(),
        "Everything should have been released");
    assertTrue(schedulerNode.containersForPreemption.isEmpty(),
        "No containers should be reserved for preemption");
    assertTrue(schedulerNode.resourcesPreemptedForApp.isEmpty(),
        "No resources should be reserved for preemptors");
    assertEquals(Resources.none(), schedulerNode.getTotalReserved(),
        "No amount of resource should be reserved for preemptees");
  }

  private void allocateContainers(FSSchedulerNode schedulerNode) {
    FairScheduler.assignPreemptedContainers(schedulerNode);
  }

  /**
   * Allocate and release a single container.
   */
  @Test
  public void testSimpleAllocation() {
    RMNode node = createNode();
    FSSchedulerNode schedulerNode = new FSSchedulerNode(node, false);

    createDefaultContainer();
    assertEquals(Resources.none(), schedulerNode.getAllocatedResource(),
        "Nothing should have been allocated, yet");
    schedulerNode.allocateContainer(containers.get(0));
    assertEquals(containers.get(0).getContainer().getResource(),
        schedulerNode.getAllocatedResource(), "Container should be allocated");
    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);
    assertEquals(Resources.none(), schedulerNode.getAllocatedResource(),
        "Everything should have been released");

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

    createDefaultContainer();
    createDefaultContainer();
    createDefaultContainer();
    assertEquals(Resources.none(), schedulerNode.getAllocatedResource(),
        "Nothing should have been allocated, yet");
    schedulerNode.allocateContainer(containers.get(0));
    schedulerNode.containerStarted(containers.get(0).getContainerId());
    schedulerNode.allocateContainer(containers.get(1));
    schedulerNode.containerStarted(containers.get(1).getContainerId());
    schedulerNode.allocateContainer(containers.get(2));
    assertEquals(Resources.multiply(containers.get(0).getContainer().getResource(), 3.0),
        schedulerNode.getAllocatedResource(), "Container should be allocated");
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
    assertEquals(Resources.multiply(containers.get(0).getContainer().getResource(),
        containers.size()), schedulerNode.getAllocatedResource(),
        "Container should be allocated");

    // Request preemption
    FSAppAttempt starvingApp = createStarvingApp(schedulerNode,
        Resource.newInstance(1024, 1));
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(0)), starvingApp);
    assertEquals(containers.get(0).getAllocatedResource(),
        schedulerNode.getTotalReserved(),
        "No resource amount should be reserved for preemptees");

    // Preemption occurs release one container
    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);
    allocateContainers(schedulerNode);
    assertEquals(schedulerNode.getTotalResource(),
        schedulerNode.getAllocatedResource(),
        "Container should be allocated");

    // Release all remaining containers
    for (int i = 1; i < containers.size(); ++i) {
      schedulerNode.releaseContainer(containers.get(i).getContainerId(), true);
    }
    finalValidation(schedulerNode);
  }

  /**
   * Allocate a single container twice and release.
   */
  @Test
  public void testDuplicatePreemption() {
    RMNode node = createNode();
    FSSchedulerNode schedulerNode = new FSSchedulerNode(node, false);

    // Launch containers and saturate the cluster
    saturateCluster(schedulerNode);
    assertEquals(Resources.multiply(containers.get(0).getContainer().getResource(),
        containers.size()), schedulerNode.getAllocatedResource(),
        "Container should be allocated");

    // Request preemption twice
    FSAppAttempt starvingApp = createStarvingApp(schedulerNode,
        Resource.newInstance(1024, 1));
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(0)), starvingApp);
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(0)), starvingApp);
    assertEquals(containers.get(0).getAllocatedResource(),
        schedulerNode.getTotalReserved(),
        "No resource amount should be reserved for preemptees");

    // Preemption occurs release one container
    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);
    allocateContainers(schedulerNode);
    assertEquals(schedulerNode.getTotalResource(),
        schedulerNode.getAllocatedResource(), "Container should be allocated");

    // Release all remaining containers
    for (int i = 1; i < containers.size(); ++i) {
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
    assertEquals(Resources.multiply(containers.get(0).getContainer().getResource(),
        containers.size()), schedulerNode.getAllocatedResource(),
        "Container should be allocated");

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

    allocateContainers(schedulerNode);
    assertEquals(schedulerNode.getTotalResource(),
        schedulerNode.getAllocatedResource(), "Container should be allocated");

    // Release all containers
    for (int i = 3; i < containers.size(); ++i) {
      schedulerNode.releaseContainer(containers.get(i).getContainerId(), true);
    }
    finalValidation(schedulerNode);
  }

  /**
   * Allocate and release three containers requested by two apps in two rounds.
   */
  @Test
  public void testMultiplePreemptionEvents() {
    RMNode node = createNode();
    FSSchedulerNode schedulerNode = new FSSchedulerNode(node, false);

    // Launch containers and saturate the cluster
    saturateCluster(schedulerNode);
    assertEquals(Resources.multiply(containers.get(0).getContainer().getResource(),
        containers.size()), schedulerNode.getAllocatedResource(),
        "Container should be allocated");

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
    schedulerNode.releaseContainer(containers.get(1).getContainerId(), true);
    allocateContainers(schedulerNode);

    schedulerNode.releaseContainer(containers.get(2).getContainerId(), true);
    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);
    allocateContainers(schedulerNode);

    assertEquals(schedulerNode.getTotalResource(),
        schedulerNode.getAllocatedResource(), "Container should be allocated");

    // Release all containers
    for (int i = 3; i < containers.size(); ++i) {
      schedulerNode.releaseContainer(containers.get(i).getContainerId(), true);
    }
    finalValidation(schedulerNode);
  }

  /**
   * Allocate and release a single container and delete the app in between.
   */
  @Test
  public void testPreemptionToCompletedApp() {
    RMNode node = createNode();
    FSSchedulerNode schedulerNode = new FSSchedulerNode(node, false);

    // Launch containers and saturate the cluster
    saturateCluster(schedulerNode);
    assertEquals(Resources.multiply(containers.get(0).getContainer().getResource(),
        containers.size()), schedulerNode.getAllocatedResource(),
        "Container should be allocated");

    // Preempt a container
    FSAppAttempt starvingApp = createStarvingApp(schedulerNode,
        Resource.newInstance(1024, 1));
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(0)), starvingApp);

    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);

    // Stop the application then try to satisfy the reservation
    // and observe that there are still free resources not allocated to
    // the deleted app
    when(starvingApp.isStopped()).thenReturn(true);
    allocateContainers(schedulerNode);
    assertNotEquals(schedulerNode.getTotalResource(),
        schedulerNode.getAllocatedResource(), "Container should be allocated");

    // Release all containers
    for (int i = 1; i < containers.size(); ++i) {
      schedulerNode.releaseContainer(containers.get(i).getContainerId(), true);
    }
    finalValidation(schedulerNode);
  }

  /**
   * Preempt a bigger container than the preemption request.
   */
  @Test
  public void testPartialReservedPreemption() {
    RMNode node = createNode();
    FSSchedulerNode schedulerNode = new FSSchedulerNode(node, false);

    // Launch containers and saturate the cluster
    saturateCluster(schedulerNode);
    assertEquals(Resources.multiply(containers.get(0).getContainer().getResource(),
        containers.size()), schedulerNode.getAllocatedResource(),
        "Container should be allocated");

    // Preempt a container
    Resource originalStarvingAppDemand = Resource.newInstance(512, 1);
    FSAppAttempt starvingApp = createStarvingApp(schedulerNode,
        originalStarvingAppDemand);
    schedulerNode.addContainersForPreemption(
        Collections.singletonList(containers.get(0)), starvingApp);

    // Preemption occurs
    schedulerNode.releaseContainer(containers.get(0).getContainerId(), true);

    // Container partially reassigned
    allocateContainers(schedulerNode);
    assertEquals(Resources.subtract(schedulerNode.getTotalResource(),
        Resource.newInstance(512, 0)),
        schedulerNode.getAllocatedResource(), "Container should be allocated");

    // Cleanup simulating node update
    schedulerNode.getPreemptionList();

    // Release all containers
    for (int i = 1; i < containers.size(); ++i) {
      schedulerNode.releaseContainer(containers.get(i).getContainerId(), true);
    }
    finalValidation(schedulerNode);
  }

}

