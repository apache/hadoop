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
import org.apache.hadoop.yarn.service.compinstance.ComponentInstance;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.state.ContainerAssignment;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.servicemonitor.ProbeStatus;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test for postponing container requests until dependencies are ready.
 */
public class TestMockAppStateDependencies extends BaseMockAppStateTest
    implements MockRoles {

  private org.apache.slider.server.servicemonitor.Probe successProbe =
      new org.apache.slider.server.servicemonitor.Probe("success", null) {
        @Override
        public ProbeStatus ping(ComponentInstance roleInstance) {
          ProbeStatus status = new ProbeStatus();
          status.succeed(this);
          return status;
        }
      };

  private org.apache.slider.server.servicemonitor.Probe failureProbe =
      new org.apache.slider.server.servicemonitor.Probe("failure", null) {
        @Override
        public ProbeStatus ping(ComponentInstance roleInstance) {
          ProbeStatus status = new ProbeStatus();
          status.fail(this, new Exception());
          return status;
        }
      };

  @Override
  public String getTestName() {
    return "TestMockAppStateDependencies";
  }

  //@Test
  public void testDependencies() throws Throwable {
    RoleStatus role0Status = getRole0Status();
    RoleStatus role1Status = getRole1Status();

    // set desired instances for role0 to 1
    role0Status.setDesired(1);
    // set probe for role0 to use a ping that will always succeed
    role0Status.getProviderRole().probe = successProbe;

    // set desired instances for role1 to 1
    role1Status.setDesired(1);
    // set role0 as a dependency of role1
    role1Status.getProviderRole().component.setDependencies(Collections
        .singletonList(ROLE0));

    // role0 has no dependencies, so its dependencies are ready
    assertTrue(appState.areDependenciesReady(role0Status));
    // role1 dependency (role0) is not ready yet
    assertFalse(appState.areDependenciesReady(role1Status));
    // start the single requested instance for role0
    review(ROLE0, 2);

    // role0 is still not ready because a ping has not been issued
    assertFalse(appState.areDependenciesReady(role1Status));
    // issue pings
    appState.monitorComponentInstances();
    // now role0 is ready
    assertTrue(appState.areDependenciesReady(role1Status));
    // increase the desired containers for role0
    role0Status.setDesired(2);
    // role0 is no longer ready
    assertFalse(appState.areDependenciesReady(role1Status));
    // start a second instance for role0
    review(ROLE0, 2);

    // role0 is not ready because ping has not been issued for the new instance
    assertFalse(appState.areDependenciesReady(role1Status));
    // issue pings
    appState.monitorComponentInstances();
    // role0 is ready
    assertTrue(appState.areDependenciesReady(role1Status));

    // set probe for role0 to use a ping that will always fail
    role0Status.getProviderRole().probe = failureProbe;
    // issue pings
    appState.monitorComponentInstances();
    // role0 is not ready (failure probe works)
    assertFalse(appState.areDependenciesReady(role1Status));
    // set probe for role0 to use a ping that will always succeed
    role0Status.getProviderRole().probe = successProbe;
    // issue pings
    appState.monitorComponentInstances();
    // role0 is ready
    assertTrue(appState.areDependenciesReady(role1Status));

    // now role1 instances can be started
    review(ROLE1, 1);
  }

  public void review(String expectedRole, int outstanding) throws Exception {
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();

    // expect one request in the list
    assertEquals(1, ops.size());
    // and in a liveness check, expected outstanding
    ApplicationLivenessInformation liveness =
        appState.getApplicationLivenessInformation();
    assertEquals(outstanding, liveness.requestsOutstanding);
    assertFalse(liveness.allRequestsSatisfied);

    // record container allocated and verify it has the expected role
    List<Container> allocations = engine.execute(ops);
    List<ContainerAssignment> assignments = new ArrayList<>();
    List<AbstractRMOperation> releases = new ArrayList<>();
    appState.onContainersAllocated(allocations, assignments, releases);
    assertEquals(1, assignments.size());
    ContainerAssignment assigned = assignments.get(0);
    Container target = assigned.container;
    RoleInstance ri = roleInstance(assigned);
    assertEquals(expectedRole, ri.role);

    // one fewer request outstanding
    liveness = appState.getApplicationLivenessInformation();
    assertEquals(outstanding - 1, liveness.requestsOutstanding);

    // record container start submitted
    appState.containerStartSubmitted(target, ri);

    // additional review results in no additional requests
    ops = appState.reviewRequestAndReleaseNodes();
    assertTrue(ops.isEmpty());

    // record container start
    appState.innerOnNodeManagerContainerStarted(target.getId());
  }
}
