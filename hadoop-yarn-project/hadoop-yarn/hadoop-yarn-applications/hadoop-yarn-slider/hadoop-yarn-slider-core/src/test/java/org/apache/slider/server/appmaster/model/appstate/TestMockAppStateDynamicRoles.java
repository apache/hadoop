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

import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.apache.slider.server.appmaster.state.AppState.NodeCompletionResult;
import org.apache.slider.server.appmaster.state.ContainerPriority;
import org.apache.slider.server.appmaster.state.RoleHistoryUtils;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.slider.server.appmaster.model.mock.MockFactory.NODE_FAILURE_THRESHOLD;

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
public class TestMockAppStateDynamicRoles extends BaseMockAppStateTest
    implements MockRoles {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMockAppStateDynamicRoles.class);
  private static final String ROLE4 = "4";
  private static final String ROLE5 = "5";

  @Override
  public String getTestName() {
    return "TestMockAppStateDynamicRoles";
  }

  /**
   * Small cluster with multiple containers per node,
   * to guarantee many container allocations on each node.
   * @return
   */
  @Override
  public MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(8, 2);
  }

  @Override
  public Application buildApplication() {
    Application application = super.buildApplication();

    Component component = new Component().name(ROLE4).numberOfContainers(1L);
    component.getConfiguration().setProperty(ResourceKeys
        .NODE_FAILURE_THRESHOLD, Integer.toString(3));
    application.getComponents().add(component);

    component = new Component().name(ROLE5).numberOfContainers(1L);
    component.getConfiguration().setProperty(ResourceKeys
        .COMPONENT_PLACEMENT_POLICY, Integer.toString(PlacementPolicy.STRICT));
    application.getComponents().add(component);

    return application;
  }

  @Test
  public void testAllocateReleaseRealloc() throws Throwable {

    createAndStartNodes();
    appState.reviewRequestAndReleaseNodes();
    appState.getRoleHistory().dump();
  }

  /**
   * Find all allocations for a specific role.
   * @param role role Id/priority
   * @param actions source list
   * @return found list
   */
  List<ContainerRequestOperation> findAllocationsForRole(int role,
      List<AbstractRMOperation> actions) {
    List<ContainerRequestOperation> ops = new ArrayList<>();
    for (AbstractRMOperation op : actions) {
      if (op instanceof ContainerRequestOperation && role ==
          ContainerPriority.extractRole(((ContainerRequestOperation) op)
              .getRequest().getPriority())) {
        ops.add((ContainerRequestOperation) op);
      }
    }
    return ops;
  }

  @Test
  public void testStrictPlacementInitialRequest() throws Throwable {
    LOG.info("Initial engine state = {}", engine);
    List<AbstractRMOperation> actions = appState.reviewRequestAndReleaseNodes();
    assertEquals(2, actions.size());

    // neither have locality at this point
    assertRelaxLocalityFlag(appState.lookupRoleStatus(ROLE4).getKey(), null,
        true, actions);
    assertRelaxLocalityFlag(appState.lookupRoleStatus(ROLE5).getKey(), null,
        true, actions);
  }

  @Test
  public void testPolicyPropagation() throws Throwable {
    assertEquals(0, (appState.lookupRoleStatus(ROLE4).getPlacementPolicy() &
        PlacementPolicy.STRICT));
    assertNotEquals(0, (appState.lookupRoleStatus(ROLE5).getPlacementPolicy() &
        PlacementPolicy.STRICT));

  }

  @Test
  public void testNodeFailureThresholdPropagation() throws Throwable {
    assertEquals(3, appState.lookupRoleStatus(ROLE4).getNodeFailureThreshold());
    assertEquals(NODE_FAILURE_THRESHOLD, appState.lookupRoleStatus(ROLE5)
        .getNodeFailureThreshold());
  }

  @Test
  public void testLaxPlacementSecondRequestRole4() throws Throwable {
    LOG.info("Initial engine state = {}", engine);
    RoleStatus role4 = appState.lookupRoleStatus(ROLE4);
    RoleStatus role5 = appState.lookupRoleStatus(ROLE5);
    role4.setDesired(1);
    role5.setDesired(0);

    List<RoleInstance> instances = createStartAndStopNodes(new ArrayList<>());
    assertEquals(1, instances.size());

    int id = appState.lookupRoleStatus(ROLE4).getKey();
    RoleInstance instanceA = null;
    for (RoleInstance instance : instances) {
      if (instance.roleId == id) {
        instanceA = instance;
      }
    }
    assertNotNull(instanceA);
    String hostname = RoleHistoryUtils.hostnameOf(instanceA.container);

    LOG.info("Allocated engine state = {}", engine);
    assertEquals(1, engine.containerCount());

    assertEquals(1, role4.getRunning());
    // shrinking cluster

    role4.setDesired(0);
    appState.lookupRoleStatus(ROLE4).setDesired(0);
    List<NodeCompletionResult> completionResults = new ArrayList<>();
    createStartAndStopNodes(completionResults);
    assertEquals(0, engine.containerCount());
    assertEquals(1, completionResults.size());

    // expanding: expect hostnames  now
    role4.setDesired(1);
    List<AbstractRMOperation> actions = appState.reviewRequestAndReleaseNodes();
    assertEquals(1, actions.size());

    ContainerRequestOperation cro = (ContainerRequestOperation) actions.get(0);
    List<String> nodes = cro.getRequest().getNodes();
    assertEquals(1, nodes.size());
    assertEquals(hostname, nodes.get(0));
  }

  @Test
  public void testStrictPlacementSecondRequestRole5() throws Throwable {
    LOG.info("Initial engine state = {}", engine);
    RoleStatus role4 = appState.lookupRoleStatus(ROLE4);
    RoleStatus role5 = appState.lookupRoleStatus(ROLE5);
    role4.setDesired(0);
    role5.setDesired(1);

    List<RoleInstance> instances = createStartAndStopNodes(new ArrayList<>());
    assertEquals(1, instances.size());

    int id = appState.lookupRoleStatus(ROLE5).getKey();
    RoleInstance instanceA = null;
    for (RoleInstance instance : instances) {
      if (instance.roleId == id) {
        instanceA = instance;
      }
    }
    assertNotNull(instanceA);
    String hostname = RoleHistoryUtils.hostnameOf(instanceA.container);

    LOG.info("Allocated engine state = {}", engine);
    assertEquals(1, engine.containerCount());

    assertEquals(1, role5.getRunning());

    // shrinking cluster
    role5.setDesired(0);
    List<NodeCompletionResult> completionResults = new ArrayList<>();
    createStartAndStopNodes(completionResults);
    assertEquals(0, engine.containerCount());
    assertEquals(1, completionResults.size());
    assertEquals(0, role5.getRunning());

    role5.setDesired(1);
    List<AbstractRMOperation> actions = appState.reviewRequestAndReleaseNodes();
    assertEquals(1, actions.size());
    assertRelaxLocalityFlag(id, "", false, actions);
    ContainerRequestOperation cro = (ContainerRequestOperation) actions.get(0);
    List<String> nodes = cro.getRequest().getNodes();
    assertEquals(1, nodes.size());
    assertEquals(hostname, nodes.get(0));
  }

  public void assertRelaxLocalityFlag(
      int role,
      String expectedHost,
      boolean expectedRelaxFlag,
      List<AbstractRMOperation> actions) {
    List<ContainerRequestOperation> requests = findAllocationsForRole(
        role, actions);
    assertEquals(1, requests.size());
    ContainerRequestOperation req = requests.get(0);
    assertEquals(expectedRelaxFlag, req.getRequest().getRelaxLocality());
  }

}
