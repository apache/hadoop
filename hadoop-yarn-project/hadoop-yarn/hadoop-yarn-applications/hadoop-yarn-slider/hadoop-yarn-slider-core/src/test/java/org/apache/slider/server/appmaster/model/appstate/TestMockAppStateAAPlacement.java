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
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.server.appmaster.model.mock.MockAppState;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.AppState.NodeUpdatedOutcome;
import org.apache.slider.server.appmaster.state.AppStateBindingInfo;
import org.apache.slider.server.appmaster.state.ContainerAssignment;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.slider.api.ResourceKeys.COMPONENT_PLACEMENT_POLICY;
import static org.apache.slider.server.appmaster.model.mock.MockFactory.AAROLE_2;

/**
 * Test Anti-affine placement.
 */
public class TestMockAppStateAAPlacement extends BaseMockAppStateAATest
    implements MockRoles {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMockAppStateAAPlacement.class);

  private static final int NODES = 3;

  /**
   * The YARN engine has a cluster with very few nodes (3) and lots of
   * containers, so if AA placement isn't working, there will be affine
   * placements surfacing.
   * @return
   */
  @Override
  public MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(NODES, 8);
  }

  /**
   * This is the simplest AA allocation: no labels, so allocate anywhere.
   * @throws Throwable
   */
  @Test
  public void testAllocateAANoLabel() throws Throwable {
    RoleStatus aaRole = getAaRole();

    assertTrue(cloneNodemap().size() > 0);

    // want multiple instances, so there will be iterations
    aaRole.setDesired(2);

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    AMRMClient.ContainerRequest request = getSingleRequest(ops);
    assertFalse(request.getRelaxLocality());
    assertEquals(request.getNodes().size(), engine.getCluster()
        .getClusterSize());
    assertNull(request.getRacks());
    assertNotNull(request.getCapability());

    Container allocated = engine.allocateContainer(request);

    // notify the container ane expect
    List<ContainerAssignment> assignments = new ArrayList<>();
    List<AbstractRMOperation> operations = new ArrayList<>();
    appState.onContainersAllocated(Arrays.asList(allocated), assignments,
        operations);

    String host = allocated.getNodeId().getHost();
    NodeInstance hostInstance = cloneNodemap().get(host);
    assertEquals(1, hostInstance.get(aaRole.getKey()).getStarting());
    assertFalse(hostInstance.canHost(aaRole.getKey(), ""));
    assertFalse(hostInstance.canHost(aaRole.getKey(), null));

    // assignment
    assertEquals(1, assignments.size());

    // verify the release matches the allocation
    assertEquals(2, operations.size());
    assertNotNull(getCancel(operations, 0).getCapability().equals(allocated
            .getResource()));

    // we also expect a new allocation request to have been issued

    ContainerRequest req2 = getRequest(operations, 1);
    assertEquals(req2.getNodes().size(), engine.getCluster()
        .getClusterSize() - 1);

    assertFalse(req2.getNodes().contains(host));
    assertFalse(request.getRelaxLocality());

    // verify the pending couner is down
    assertEquals(0L, aaRole.getAAPending());
    Container allocated2 = engine.allocateContainer(req2);

    // placement must be on a different host
    assertNotEquals(allocated2.getNodeId(), allocated.getNodeId());

    ContainerAssignment assigned = assignments.get(0);
    Container container = assigned.container;
    RoleInstance ri = roleInstance(assigned);
    //tell the app it arrived
    appState.containerStartSubmitted(container, ri);
    assertNotNull(appState.onNodeManagerContainerStarted(container.getId()));
    ops = appState.reviewRequestAndReleaseNodes();
    assertEquals(0, ops.size());
    assertAllContainersAA();

    // identify those hosts with an aa role on
    Map<Integer, String> naming = appState.buildNamingMap();
    assertEquals(3, naming.size());

    String name = aaRole.getName();
    assertEquals(name, naming.get(aaRole.getKey()));
    Map<String, NodeInformation> info =
        appState.getRoleHistory().getNodeInformationSnapshot(naming);
    assertTrue(SliderUtils.isNotEmpty(info));

    NodeInformation nodeInformation = info.get(host);
    assertNotNull(nodeInformation);
    assertTrue(SliderUtils.isNotEmpty(nodeInformation.entries));
    assertNotNull(nodeInformation.entries.get(name));
    assertEquals(1, nodeInformation.entries.get(name).live);
  }

  @Test
  public void testAllocateFlexUp() throws Throwable {
    RoleStatus aaRole = getAaRole();

    // want multiple instances, so there will be iterations
    aaRole.setDesired(2);
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    getSingleRequest(ops);
    assertEquals(1, aaRole.getRequested());
    assertEquals(1, aaRole.getAAPending());
    assertEquals(aaRole.getActualAndRequested() + aaRole
            .getAAPending(), aaRole.getDesired());

    // now trigger that flex up
    aaRole.setDesired(3);

    // expect: no new reqests, pending count ++
    List<AbstractRMOperation> ops2 = appState.reviewRequestAndReleaseNodes();
    assertTrue(ops2.isEmpty());
    assertEquals(aaRole.getRunning() + aaRole.getAAPending() +
            aaRole.getOutstandingAARequestCount(), aaRole.getDesired());

    // 1 outstanding
    assertEquals(0, aaRole.getRunning());
    assertTrue(aaRole.isAARequestOutstanding());
    // and one AA
    assertEquals(2, aaRole.getAAPending());
    assertAllContainersAA();

    // next iter
    assertEquals(1, submitOperations(ops, EMPTY_ID_LIST, ops2).size());
    assertEquals(2, ops2.size());
    assertEquals(1, aaRole.getAAPending());
    assertAllContainersAA();

    assertEquals(0, appState.reviewRequestAndReleaseNodes().size());
    // now trigger the next execution cycle
    List<AbstractRMOperation> ops3 = new ArrayList<>();
    assertEquals(1, submitOperations(ops2, EMPTY_ID_LIST, ops3).size());
    assertEquals(2, ops3.size());
    assertEquals(0, aaRole.getAAPending());
    assertAllContainersAA();

  }

  @Test
  public void testAllocateFlexDownDecrementsPending() throws Throwable {
    RoleStatus aaRole = getAaRole();

    // want multiple instances, so there will be iterations
    aaRole.setDesired(2);
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    getSingleRequest(ops);
    assertEquals(1, aaRole.getAAPending());
    assertTrue(aaRole.isAARequestOutstanding());

    // flex down so that the next request should be cancelled
    aaRole.setDesired(1);

    // expect: no new requests, pending count --
    List<AbstractRMOperation> ops2 = appState.reviewRequestAndReleaseNodes();
    assertTrue(ops2.isEmpty());
    assertTrue(aaRole.isAARequestOutstanding());
    assertEquals(0, aaRole.getAAPending());
    assertAllContainersAA();

    // next iter
    submitOperations(ops, EMPTY_ID_LIST, ops2).size();
    assertEquals(1, ops2.size());
    assertAllContainersAA();
  }

  /**
   * Here flex down while there is only one outstanding request.
   * The outstanding flex should be cancelled
   * @throws Throwable
   */
  @Test
  public void testAllocateFlexDownForcesCancel() throws Throwable {
    RoleStatus aaRole = getAaRole();

    // want multiple instances, so there will be iterations
    aaRole.setDesired(1);
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    getSingleRequest(ops);
    assertEquals(1, aaRole.getRequested());
    assertEquals(0, aaRole.getAAPending());
    assertTrue(aaRole.isAARequestOutstanding());

    // flex down so that the next request should be cancelled
    aaRole.setDesired(0);
    // expect: no new requests, pending count --
    List<AbstractRMOperation> ops2 = appState.reviewRequestAndReleaseNodes();
    assertEquals(0, aaRole.getRequested());
    assertEquals(0, aaRole.getAAPending());
    assertFalse(aaRole.isAARequestOutstanding());
    assertEquals(1, ops2.size());
    getSingleCancel(ops2);

    // next iter
    submitOperations(ops, EMPTY_ID_LIST, ops2).size();
    getSingleRelease(ops2);
  }

  void assertAllContainersAA() {
    assertAllContainersAA(getAaRole().getKey());
  }

  /**
   *
   * @throws Throwable
   */
  @Test
  public void testAskForTooMany() throws Throwable {
    RoleStatus aaRole = getAaRole();

    describe("Ask for 1 more than the no of available nodes;" +
        " expect the final request to be unsatisfied until the cluster " +
        "changes size");
    //more than expected
    aaRole.setDesired(NODES + 1);
    List<AbstractRMOperation > operations = appState
        .reviewRequestAndReleaseNodes();
    assertTrue(aaRole.isAARequestOutstanding());
    assertEquals(NODES, aaRole.getAAPending());
    for (int i = 0; i < NODES; i++) {
      String iter = "Iteration " + i + " role = " + aaRole;
      LOG.info(iter);
      List<AbstractRMOperation > operationsOut = new ArrayList<>();
      assertEquals(1, submitOperations(operations, EMPTY_ID_LIST,
          operationsOut).size());
      operations = operationsOut;
      if (i + 1 < NODES) {
        assertEquals(2, operations.size());
      } else {
        assertEquals(1, operations.size());
      }
      assertAllContainersAA();
    }
    // expect an outstanding AA request to be unsatisfied
    assertTrue(aaRole.getRunning() < aaRole.getDesired());
    assertEquals(0, aaRole.getRequested());
    assertFalse(aaRole.isAARequestOutstanding());
    List<Container> allocatedContainers = engine.execute(operations,
        EMPTY_ID_LIST);
    assertEquals(0, allocatedContainers.size());
    // in a review now, no more requests can be generated, as there is no
    // space for AA placements, even though there is cluster capacity
    assertEquals(0, appState.reviewRequestAndReleaseNodes().size());

    // now do a node update (this doesn't touch the YARN engine; the node
    // isn't really there)
    NodeUpdatedOutcome outcome = addNewNode();
    assertEquals(cloneNodemap().size(), NODES + 1);
    assertTrue(outcome.clusterChanged);
    // no active calls to empty
    assertTrue(outcome.operations.isEmpty());
    assertEquals(1, appState.reviewRequestAndReleaseNodes().size());
  }

  protected AppState.NodeUpdatedOutcome addNewNode() {
    return updateNodes(MockFactory.INSTANCE.newNodeReport("4", NodeState
        .RUNNING, "gpu"));
  }

  @Test
  public void testClusterSizeChangesDuringRequestSequence() throws Throwable {
    RoleStatus aaRole = getAaRole();
    describe("Change the cluster size where the cluster size changes during " +
        "a test sequence.");
    aaRole.setDesired(NODES + 1);
    appState.reviewRequestAndReleaseNodes();
    assertTrue(aaRole.isAARequestOutstanding());
    assertEquals(NODES, aaRole.getAAPending());
    NodeUpdatedOutcome outcome = addNewNode();
    assertTrue(outcome.clusterChanged);
    // one call to cancel
    assertEquals(1, outcome.operations.size());
    // and on a review, one more to rebuild
    assertEquals(1, appState.reviewRequestAndReleaseNodes().size());
  }

  @Test
  public void testBindingInfoMustHaveNodeMap() throws Throwable {
    AppStateBindingInfo bindingInfo = buildBindingInfo();
    bindingInfo.nodeReports = null;
    try {
      MockAppState state = new MockAppState(bindingInfo);
      fail("Expected an exception, got " + state);
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testAMRestart() throws Throwable {
    int desiredAA = 3;
    getAaRole().setDesired(desiredAA);
    List<RoleInstance> instances = createAndStartNodes();
    List<Container> containers = new ArrayList<>();
    for (RoleInstance instance : instances) {
      containers.add(instance.container);
    }

    // now destroy the app state
    AppStateBindingInfo bindingInfo = buildBindingInfo();
    bindingInfo.application = factory.newApplication(0, 0, desiredAA).name(
        getValidTestName());
    bindingInfo.application.getComponent(ROLE2)
        .getConfiguration().setProperty(COMPONENT_PLACEMENT_POLICY,
        Integer.toString(PlacementPolicy.ANTI_AFFINITY_REQUIRED));
    bindingInfo.liveContainers = containers;
    appState = new MockAppState(bindingInfo);

    RoleStatus aaRole = lookupRole(AAROLE_2.name);
    RoleStatus gpuRole = lookupRole(MockFactory.AAROLE_1_GPU.name);
    appState.reviewRequestAndReleaseNodes();
    assertTrue(aaRole.isAntiAffinePlacement());
    assertTrue(aaRole.isAARequestOutstanding());

  }

}
