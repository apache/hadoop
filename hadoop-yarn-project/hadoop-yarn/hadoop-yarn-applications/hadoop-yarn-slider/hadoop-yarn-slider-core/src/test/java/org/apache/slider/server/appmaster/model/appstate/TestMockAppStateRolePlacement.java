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
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.CancelSingleRequest;
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.apache.slider.server.appmaster.state.ContainerAssignment;
import org.apache.slider.server.appmaster.state.RoleHistoryUtils;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.slider.server.appmaster.state.ContainerPriority.extractRole;

/**
 * Test that the app state lets you ask for nodes, get a specific host,
 * release it and then get that one back again.
 */
public class TestMockAppStateRolePlacement extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  public String getTestName() {
    return "TestMockAppStateRolePlacement";
  }


  @Test
  public void testAllocateReleaseRealloc() throws Throwable {
    getRole0Status().setDesired(1);

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    ContainerRequestOperation operation = (ContainerRequestOperation)ops
        .get(0);
    AMRMClient.ContainerRequest request = operation.getRequest();
    assertTrue(request.getRelaxLocality());
    assertNull(request.getNodes());
    assertNull(request.getRacks());
    assertNotNull(request.getCapability());

    Container allocated = engine.allocateContainer(request);
    List<ContainerAssignment> assignments = new ArrayList<>();
    List<AbstractRMOperation> releaseOperations = new ArrayList<>();
    appState.onContainersAllocated(Arrays.asList((Container)allocated),
        assignments, releaseOperations);
    // verify the release matches the allocation
    assertEquals(releaseOperations.size(), 1);
    CancelSingleRequest cancelOp = (CancelSingleRequest)releaseOperations
        .get(0);
    assertNotNull(cancelOp.getRequest());
    assertNotNull(cancelOp.getRequest().getCapability());
    assertEquals(cancelOp.getRequest().getCapability(), allocated
        .getResource());
    // now the assignment
    assertEquals(assignments.size(), 1);
    ContainerAssignment assigned = assignments.get(0);
    Container container = assigned.container;
    assertEquals(container.getId(), allocated.getId());
    int roleId = assigned.role.getPriority();
    assertEquals(roleId, extractRole(request.getPriority()));
    assertEquals(assigned.role.getName(), ROLE0);
    String containerHostname = RoleHistoryUtils.hostnameOf(container);
    RoleInstance ri = roleInstance(assigned);
    //tell the app it arrived
    appState.containerStartSubmitted(container, ri);
    assertNotNull(appState.onNodeManagerContainerStarted(container.getId()));
    assertEquals(getRole0Status().getRunning(), 1);
    ops = appState.reviewRequestAndReleaseNodes();
    assertEquals(ops.size(), 0);

    //now it is surplus
    getRole0Status().setDesired(0);
    ops = appState.reviewRequestAndReleaseNodes();
    ContainerReleaseOperation release = (ContainerReleaseOperation) ops.get(0);

    assertEquals(release.getContainerId(), container.getId());
    engine.execute(ops);
    assertNotNull(appState.onCompletedContainer(containerStatus(container
        .getId())).roleInstance);

    //view the world
    appState.getRoleHistory().dump();

    //now ask for a new one
    getRole0Status().setDesired(1);
    ops = appState.reviewRequestAndReleaseNodes();
    assertEquals(ops.size(), 1);
    operation = (ContainerRequestOperation) ops.get(0);
    AMRMClient.ContainerRequest request2 = operation.getRequest();
    assertNotNull(request2);
    assertEquals(request2.getNodes().get(0), containerHostname);
    assertFalse(request2.getRelaxLocality());
    engine.execute(ops);

  }

}
