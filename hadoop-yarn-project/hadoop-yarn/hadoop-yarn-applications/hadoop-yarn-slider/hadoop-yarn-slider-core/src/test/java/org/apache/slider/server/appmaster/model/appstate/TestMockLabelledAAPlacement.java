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
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.AppState.NodeUpdatedOutcome;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Test Anti-affine placement.
 */
public class TestMockLabelledAAPlacement extends BaseMockAppStateAATest
    implements MockRoles {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMockLabelledAAPlacement.class);

  private static final int NODES = 3;
  private static final int GPU_NODES = 2;
  private static final String HOST0 = "00000000";
  private static final String HOST1 = "00000001";

  @Override
  public void setup() throws Exception {
    super.setup();

    updateNodes(MockFactory.INSTANCE.newNodeReport(HOST0, NodeState.RUNNING,
        LABEL_GPU));
    updateNodes(MockFactory.INSTANCE.newNodeReport(HOST1, NodeState.RUNNING,
        LABEL_GPU));
  }

  @Override
  public MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(NODES, 8);
  }

  void assertAllContainersAA() {
    assertAllContainersAA(getGpuRole().getKey());
  }

  /**
   *
   * @throws Throwable
   */
  //@Test
  public void testAskForTooMany() throws Throwable {
    RoleStatus gpuRole = getGpuRole();

    describe("Ask for 1 more than the no of available nodes;" +
        " expect the final request to be unsatisfied until the cluster " +
        "changes size");
    //more than expected
    int size = GPU_NODES;
    gpuRole.setDesired(size + 1);

    List<AbstractRMOperation > operations = appState
        .reviewRequestAndReleaseNodes();
    assertTrue(gpuRole.isAARequestOutstanding());

    assertEquals(gpuRole.getAAPending(), size);
    for (int i = 0; i < size; i++) {
      String iter = "Iteration " + i + " role = " + getAaRole();
      describe(iter);
      List<AbstractRMOperation > operationsOut = new ArrayList<>();

      List<RoleInstance> roleInstances = submitOperations(operations,
          EMPTY_ID_LIST, operationsOut);
      // one instance per request
      assertEquals(1, roleInstances.size());
      appState.onNodeManagerContainerStarted(roleInstances.get(0)
          .getContainerId());
      assertAllContainersAA();
      // there should be none left
      LOG.debug(nodeInformationSnapshotAsString());
      operations = operationsOut;
      if (i + 1 < size) {
        assertEquals(2, operations.size());
      } else {
        assertEquals(1, operations.size());
      }
    }
    // expect an outstanding AA request to be unsatisfied
    assertTrue(gpuRole.getRunning() < gpuRole.getDesired());
    assertEquals(0, gpuRole.getRequested());
    assertFalse(gpuRole.isAARequestOutstanding());
    List<Container> allocatedContainers = engine.execute(operations,
        EMPTY_ID_LIST);
    assertEquals(0, allocatedContainers.size());
    // in a review now, no more requests can be generated, as there is no
    // space for AA placements, even though there is cluster capacity
    assertEquals(0, appState.reviewRequestAndReleaseNodes().size());

    // switch node 2 into being labelled
    NodeUpdatedOutcome outcome = updateNodes(MockFactory.INSTANCE.
        newNodeReport("00000002", NodeState.RUNNING, "gpu"));

    assertEquals(NODES, cloneNodemap().size());
    assertTrue(outcome.clusterChanged);
    // no active calls to empty
    assertTrue(outcome.operations.isEmpty());
    assertEquals(1, appState.reviewRequestAndReleaseNodes().size());
  }

  protected AppState.NodeUpdatedOutcome addNewNode() {
    return updateNodes(MockFactory.INSTANCE.newNodeReport("00000004",
        NodeState.RUNNING, "gpu"));
  }

  //@Test
  public void testClusterSizeChangesDuringRequestSequence() throws Throwable {
    RoleStatus gpuRole = getGpuRole();
    describe("Change the cluster size where the cluster size changes during " +
        "a test sequence.");
    gpuRole.setDesired(GPU_NODES + 1);
    List<AbstractRMOperation> operations = appState
        .reviewRequestAndReleaseNodes();
    assertTrue(gpuRole.isAARequestOutstanding());
    assertEquals(GPU_NODES, gpuRole.getAAPending());
    NodeUpdatedOutcome outcome = addNewNode();
    assertTrue(outcome.clusterChanged);
    // one call to cancel
    assertEquals(1, outcome.operations.size());
    // and on a review, one more to rebuild
    assertEquals(1, appState.reviewRequestAndReleaseNodes().size());
  }

}
