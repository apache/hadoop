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
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.NodeMap;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test Anti-affine placement with a cluster of size 1.
 */
public class TestMockAppStateAAOvercapacity extends BaseMockAppStateAATest
    implements MockRoles {

  private static final int NODES = 1;

  @Override
  public MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(NODES, 1);
  }

  void assertAllContainersAA() {
    assertAllContainersAA(getAaRole().getKey());
  }

  /**
   *
   * @throws Throwable
   */
  @Test
  public void testOvercapacityRecovery() throws Throwable {
    RoleStatus aaRole = getAaRole();

    describe("Ask for 1 more than the no of available nodes;" +
             "verify the state. kill the allocated container and review");
    //more than expected
    int desired = 3;
    aaRole.setDesired(desired);
    assertTrue(appState.getRoleHistory().canPlaceAANodes());

    //first request
    List<AbstractRMOperation> operations =
        appState.reviewRequestAndReleaseNodes();
    assertTrue(aaRole.isAARequestOutstanding());
    assertEquals(1, aaRole.getRequested());
    assertEquals(desired - 1, aaRole.getAAPending());
    List<AbstractRMOperation> operationsOut = new ArrayList<>();
    // allocate and re-submit
    List<RoleInstance> instances = submitOperations(operations,
        EMPTY_ID_LIST, operationsOut);
    assertEquals(1, instances.size());
    assertAllContainersAA();

    // expect an outstanding AA request to be unsatisfied
    assertTrue(aaRole.getRunning() < aaRole.getDesired());
    assertEquals(0, aaRole.getRequested());
    assertFalse(aaRole.isAARequestOutstanding());
    assertEquals(desired - 1, aaRole.getAAPending());
    List<Container> allocatedContainers = engine.execute(operations,
        EMPTY_ID_LIST);
    assertEquals(0, allocatedContainers.size());

    // now lets trigger a failure
    NodeMap nodemap = cloneNodemap();
    assertEquals(1, nodemap.size());

    RoleInstance instance = instances.get(0);
    ContainerId cid = instance.getContainerId();

    AppState.NodeCompletionResult result = appState.onCompletedContainer(
        containerStatus(cid, LauncherExitCodes.EXIT_TASK_LAUNCH_FAILURE));
    assertTrue(result.containerFailed);

    assertEquals(1, aaRole.getFailed());
    assertEquals(0, aaRole.getRunning());
    List<NodeInstance> availablePlacements = appState.getRoleHistory()
        .findNodeForNewAAInstance(aaRole);
    assertEquals(1, availablePlacements.size());
    describe("expecting a successful review with available placements of " +
            availablePlacements);
    operations = appState.reviewRequestAndReleaseNodes();
    assertEquals(1, operations.size());
  }
}
