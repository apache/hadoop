/*
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

package org.apache.slider.server.appmaster.model.appstate;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockRMOperationHandler;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.CancelSingleRequest;
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.ContainerAssignment;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.slider.server.appmaster.state.ContainerPriority.buildPriority;
import static org.apache.slider.server.appmaster.state.ContainerPriority.extractRole;

/**
 * Test app state RM operations.
 */
public class TestMockAppStateRMOperations extends BaseMockAppStateTest
    implements MockRoles {
  private static final Logger LOG =
      LoggerFactory.getLogger(BaseMockAppStateTest.class);

  @Override
  public String getTestName() {
    return "TestMockAppStateRMOperations";
  }

  @Test
  public void testPriorityOnly() throws Throwable {
    assertEquals(5, extractRole(buildPriority(5, false)));
  }

  @Test
  public void testPriorityRoundTrip() throws Throwable {
    assertEquals(5, extractRole(buildPriority(5, false)));
  }

  @Test
  public void testPriorityRoundTripWithRequest() throws Throwable {
    int priority = buildPriority(5, false);
    assertEquals(5, extractRole(priority));
  }

  @Test
  public void testMockAddOp() throws Throwable {
    getRole0Status().setDesired(1);
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    assertListLength(ops, 1);
    ContainerRequestOperation operation = (ContainerRequestOperation)ops.get(0);
    int priority = operation.getRequest().getPriority().getPriority();
    assertEquals(extractRole(priority), getRole0Status().getKey());
    MockRMOperationHandler handler = new MockRMOperationHandler();
    handler.execute(ops);

    AbstractRMOperation op = handler.getFirstOp();
    assertTrue(op instanceof ContainerRequestOperation);
  }

  /**
   * Test of a flex up and down op which verifies that outstanding
   * requests are cancelled first.
   * <ol>
   *   <li>request 5 nodes, assert 5 request made</li>
   *   <li>allocate 1 of them</li>
   *   <li>flex cluster size to 3</li>
   *   <li>assert this generates 2 cancel requests</li>
   * </ol>
   */
  @Test
  public void testRequestThenCancelOps() throws Throwable {
    RoleStatus role0 = getRole0Status();
    role0.setDesired(5);
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    assertListLength(ops, 5);
    // now 5 outstanding requests.
    assertEquals(5, role0.getRequested());

    // allocate one
    List<AbstractRMOperation> processed = new ArrayList<>();
    processed.add(ops.get(0));
    List<ContainerId> released = new ArrayList<>();
    List<AppState.NodeCompletionResult> completionResults = new ArrayList<>();
    submitOperations(processed, released);
    List<RoleInstance> instances = createAndSubmitNodes(released);
    processSubmissionOperations(instances, completionResults, released);


    // four outstanding
    assertEquals(4, role0.getRequested());

    // flex cluster to 3
    role0.setDesired(3);
    ops = appState.reviewRequestAndReleaseNodes();

    // expect two cancel operation from review
    assertListLength(ops, 2);
    for (AbstractRMOperation op : ops) {
      assertTrue(op instanceof CancelSingleRequest);
    }

    MockRMOperationHandler handler = new MockRMOperationHandler();
    handler.setAvailableToCancel(4);
    handler.execute(ops);
    assertEquals(2, handler.getAvailableToCancel());
    assertEquals(2, role0.getRequested());

    // flex down one more
    role0.setDesired(2);
    ops = appState.reviewRequestAndReleaseNodes();
    assertListLength(ops, 1);
    for (AbstractRMOperation op : ops) {
      assertTrue(op instanceof CancelSingleRequest);
    }
    handler.execute(ops);
    assertEquals(1, handler.getAvailableToCancel());
    assertEquals(1, role0.getRequested());
  }

  @Test
  public void testCancelNoActualContainers() throws Throwable {
    RoleStatus role0 = getRole0Status();
    role0.setDesired(5);
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    assertListLength(ops, 5);
    // now 5 outstanding requests.
    assertEquals(5, role0.getRequested());
    role0.setDesired(0);
    ops = appState.reviewRequestAndReleaseNodes();
    assertListLength(ops, 5);

  }


  @Test
  public void testFlexDownOutstandingRequests() throws Throwable {
    // engine only has two nodes, so > 2 will be outstanding
    engine = new MockYarnEngine(1, 2);
    List<AbstractRMOperation> ops;
    // role: desired = 2, requested = 1, actual=1
    RoleStatus role0 = getRole0Status();
    role0.setDesired(4);
    createAndSubmitNodes();

    assertEquals(2, role0.getRequested());
    assertEquals(2, role0.getRunning());
    // there are now two outstanding, two actual
    // Release 3 and verify that the two
    // cancellations were combined with a release
    role0.setDesired(1);
    assertEquals(-3, role0.getDelta());
    ops = appState.reviewRequestAndReleaseNodes();
    assertListLength(ops, 3);
    int numCancel = 0;
    int numRelease = 0;
    for (AbstractRMOperation op : ops) {
      if (op instanceof CancelSingleRequest) {
        numCancel++;
      }
      if (op instanceof ContainerReleaseOperation) {
        numRelease++;
      }
    }
    assertEquals(2, numCancel);
    assertEquals(1, numRelease);
    assertEquals(0, role0.getRequested());
    // TODO releasing?
//    assertEquals(1, role0.getReleasing());
  }

  @Test
  public void testCancelAllOutstandingRequests() throws Throwable {

    // role: desired = 2, requested = 1, actual=1
    RoleStatus role0 = getRole0Status();
    role0.setDesired(2);
    List<AbstractRMOperation> ops;
    ops = appState.reviewRequestAndReleaseNodes();
    int count = 0;
    for (AbstractRMOperation op : ops) {
      if (op instanceof ContainerRequestOperation) {
        count++;
      }
    }
    assertEquals(2, count);

    // there are now two outstanding, two actual
    // Release 3 and verify that the two
    // cancellations were combined with a release
    role0.setDesired(0);
    ops = appState.reviewRequestAndReleaseNodes();
    assertEquals(2, ops.size());

    for (AbstractRMOperation op : ops) {
      assertTrue(op instanceof CancelSingleRequest);
    }
  }


  @Test
  public void testFlexUpOutstandingRequests() throws Throwable {

    List<AbstractRMOperation> ops;
    // role: desired = 2, requested = 1, actual=1
    RoleStatus role0 = getRole0Status();
    role0.setDesired(2);
    appState.incRunningContainers(role0);
    appState.incRequestedContainers(role0);

    // flex up 2 nodes, yet expect only one node to be requested,
    // as the  outstanding request is taken into account
    role0.setDesired(4);
    appState.incRequestedContainers(role0);

    assertEquals(1, role0.getRunning());
    assertEquals(2, role0.getRequested());
    assertEquals(3, role0.getActualAndRequested());
    assertEquals(1, role0.getDelta());
    ops = appState.reviewRequestAndReleaseNodes();
    assertListLength(ops, 1);
    assertTrue(ops.get(0) instanceof ContainerRequestOperation);
    assertEquals(3, role0.getRequested());
  }

  @Test
  public void testFlexUpNoSpace() throws Throwable {
    // engine only has two nodes, so > 2 will be outstanding
    engine = new MockYarnEngine(1, 2);
    // role: desired = 2, requested = 1, actual=1
    RoleStatus role0 = getRole0Status();
    role0.setDesired(4);
    createAndSubmitNodes();

    assertEquals(2, role0.getRequested());
    assertEquals(2, role0.getRunning());
    role0.setDesired(8);
    assertEquals(4, role0.getDelta());
    createAndSubmitNodes();
    assertEquals(6, role0.getRequested());
  }


  @Test
  public void testAllocateReleaseOp() throws Throwable {
    getRole0Status().setDesired(1);

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    ContainerRequestOperation operation = (ContainerRequestOperation)ops.get(0);
    AMRMClient.ContainerRequest request = operation.getRequest();
    Container cont = engine.allocateContainer(request);
    List<Container> allocated = new ArrayList<>();
    allocated.add(cont);
    List<ContainerAssignment> assignments = new ArrayList<>();
    List<AbstractRMOperation> operations = new ArrayList<>();
    appState.onContainersAllocated(allocated, assignments, operations);

    assertListLength(ops, 1);
    assertListLength(assignments, 1);
    ContainerAssignment assigned = assignments.get(0);
    Container target = assigned.container;
    assertEquals(target.getId(), cont.getId());
    int roleId = assigned.role.getPriority();
    assertEquals(roleId, extractRole(request.getPriority()));
    assertEquals(assigned.role.getName(), ROLE0);
    RoleInstance ri = roleInstance(assigned);
    //tell the app it arrived
    appState.containerStartSubmitted(target, ri);
    appState.innerOnNodeManagerContainerStarted(target.getId());
    assertEquals(1, getRole0Status().getRunning());

    //now release it by changing the role status
    getRole0Status().setDesired(0);
    ops = appState.reviewRequestAndReleaseNodes();
    assertListLength(ops, 1);

    assertTrue(ops.get(0) instanceof ContainerReleaseOperation);
    ContainerReleaseOperation release = (ContainerReleaseOperation) ops.get(0);
    assertEquals(release.getContainerId(), cont.getId());
  }

  @Test
  public void testComplexAllocation() throws Throwable {
    getRole0Status().setDesired(1);
    getRole1Status().setDesired(3);

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    List<Container> allocations = engine.execute(ops);
    List<ContainerAssignment> assignments = new ArrayList<>();
    List<AbstractRMOperation> releases = new ArrayList<>();
    appState.onContainersAllocated(allocations, assignments, releases);
    // we expect four release requests here for all the allocated containers
    assertListLength(releases, 4);
    for (AbstractRMOperation op : releases) {
      assertTrue(op instanceof CancelSingleRequest);
    }
    assertListLength(assignments, 4);
    for (ContainerAssignment assigned : assignments) {
      Container target = assigned.container;
      RoleInstance ri = roleInstance(assigned);
      appState.containerStartSubmitted(target, ri);
    }
    //insert some async operation here
    for (ContainerAssignment assigned : assignments) {
      Container target = assigned.container;
      appState.innerOnNodeManagerContainerStarted(target.getId());
    }
    assertEquals(4, engine.containerCount());
    getRole1Status().setDesired(0);
    ops = appState.reviewRequestAndReleaseNodes();
    assertListLength(ops, 3);
    allocations = engine.execute(ops);
    assertEquals(1, engine.containerCount());

    appState.onContainersAllocated(allocations, assignments, releases);
    assertTrue(assignments.isEmpty());
    assertTrue(releases.isEmpty());
  }

  @Test
  public void testDoubleNodeManagerStartEvent() throws Throwable {
    getRole0Status().setDesired(1);

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    List<Container> allocations = engine.execute(ops);
    List<ContainerAssignment> assignments = new ArrayList<>();
    List<AbstractRMOperation> releases = new ArrayList<>();
    appState.onContainersAllocated(allocations, assignments, releases);
    assertListLength(assignments, 1);
    ContainerAssignment assigned = assignments.get(0);
    Container target = assigned.container;
    RoleInstance ri = roleInstance(assigned);
    appState.containerStartSubmitted(target, ri);
    RoleInstance ri2 = appState.innerOnNodeManagerContainerStarted(target
        .getId());
    assertEquals(ri2, ri);
    //try a second time, expect an error
    try {
      appState.innerOnNodeManagerContainerStarted(target.getId());
      fail("Expected an exception");
    } catch (RuntimeException expected) {
      // expected
    }
    //and non-faulter should not downgrade to a null
    LOG.warn("Ignore any exception/stack trace that appears below");
    LOG.warn("===============================================================");
    RoleInstance ri3 = appState.onNodeManagerContainerStarted(target.getId());
    LOG.warn("===============================================================");
    LOG.warn("Ignore any exception/stack trace that appeared above");
    assertNull(ri3);
  }

}
