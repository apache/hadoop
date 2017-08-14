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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockRoleHistory;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.NodeEntry;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
public class TestMockAppStateDynamicHistory extends BaseMockAppStateTest
    implements MockRoles {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMockAppStateDynamicHistory.class);

  /**
   * Small cluster with multiple containers per node,
   * to guarantee many container allocations on each node.
   * @return
   */
  @Override
  public MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(8, 1);
  }

  // TODO does not support adding new components dynamically
  public void testDynamicRoleHistory() throws Throwable {

    String dynamic = "dynamicRole";
    long desired = 1;
    int placementPolicy = PlacementPolicy.DEFAULT;
    // snapshot and patch existing spec
    Application application = appState.getClusterStatus();
    Component component = new Component().name(dynamic).numberOfContainers(
        desired);
    component.getConfiguration().setProperty(ResourceKeys
        .COMPONENT_PLACEMENT_POLICY, "" + placementPolicy);
    application.getComponents().add(component);

    appState.updateComponents(
        Collections.singletonMap(dynamic, desired));

    // now look at the role map
    assertNotNull(appState.getRoleMap().get(dynamic));
    ProviderRole mappedRole = appState.getRoleMap().get(dynamic);
    int rolePriority = mappedRole.id;

    Map<Integer, ProviderRole> priorityMap = appState.getRolePriorityMap();
    assertEquals(priorityMap.size(), 4);
    ProviderRole dynamicProviderRole = priorityMap.get(rolePriority);
    assertNotNull(dynamicProviderRole);
    assertEquals(dynamicProviderRole.id, rolePriority);

    assertNotNull(appState.getRoleStatusMap().get(rolePriority));
    RoleStatus dynamicRoleStatus =
        appState.getRoleStatusMap().get(rolePriority);
    assertEquals(dynamicRoleStatus.getDesired(), desired);


    // before allocating the nodes, fill up the capacity of some of the
    // hosts
    engine.getAllocator().nextIndex();

    int targetNode = 2;
    assertEquals(targetNode, engine.getAllocator().nextIndex());
    String targetHostname = engine.getCluster().nodeAt(targetNode)
        .getHostname();

    // clock is set to a small value
    appState.setTime(100000);

    // allocate the nodes
    List<AbstractRMOperation> actions = appState.reviewRequestAndReleaseNodes();
    assertEquals(1, actions.size());
    ContainerRequestOperation action0 = (ContainerRequestOperation)actions
        .get(0);

    ContainerRequest request = action0.getRequest();
    assertTrue(SliderUtils.isEmpty(request.getNodes()));

    List<ContainerId> released = new ArrayList<>();
    List<RoleInstance> allocations = submitOperations(actions, released);
    processSubmissionOperations(allocations, new ArrayList<>(), released);
    assertEquals(1, allocations.size());
    RoleInstance ri = allocations.get(0);

    assertEquals(ri.role, dynamic);
    assertEquals(ri.roleId, rolePriority);
    assertEquals(ri.host, targetHostname);

    // now look at the role history

    RoleHistory roleHistory = appState.getRoleHistory();
    List<NodeInstance> activeNodes = roleHistory.listActiveNodes(
        rolePriority);
    assertEquals(activeNodes.size(), 1);
    NodeInstance activeNode = activeNodes.get(0);
    assertNotNull(activeNode.get(rolePriority));
    NodeEntry entry8 = activeNode.get(rolePriority);
    assertEquals(entry8.getActive(), 1);

    assertEquals(activeNode.hostname, targetHostname);

    NodeInstance activeNodeInstance =
        roleHistory.getOrCreateNodeInstance(ri.container);

    assertEquals(activeNode, activeNodeInstance);
    NodeEntry entry = activeNodeInstance.get(rolePriority);
    assertNotNull(entry);
    assertTrue(entry.getActive() > 0);
    assertTrue(entry.getLive() > 0);


    // now trigger a termination event on that role

    // increment time for a long-lived failure event
    appState.incTime(100000);

    LOG.debug("Triggering failure");
    ContainerId cid = ri.getContainerId();
    AppState.NodeCompletionResult result = appState.onCompletedContainer(
        containerStatus(cid, 1));
    assertEquals(result.roleInstance, ri);
    assertTrue(result.containerFailed);

    roleHistory.dump();
    // values should have changed
    assertEquals(1, entry.getFailed());
    assertEquals(0, entry.getStartFailed());
    assertEquals(0, entry.getActive());
    assertEquals(0, entry.getLive());


    List<NodeInstance> nodesForRoleId =
        roleHistory.getRecentNodesForRoleId(rolePriority);
    assertNotNull(nodesForRoleId);

    // make sure new nodes will default to a different host in the engine
    assertTrue(targetNode < engine.getAllocator().nextIndex());

    actions = appState.reviewRequestAndReleaseNodes();
    assertEquals(1, actions.size());
    ContainerRequestOperation action1 = (ContainerRequestOperation) actions
        .get(0);
    ContainerRequest request1 = action1.getRequest();
    assertTrue(SliderUtils.isNotEmpty(request1.getNodes()));
  }

  //@Test(expected = BadConfigException.class)
  public void testRoleHistoryRoleAdditions() throws Throwable {
    MockRoleHistory roleHistory = new MockRoleHistory(new ArrayList<>());
    roleHistory.addNewRole(new RoleStatus(new ProviderRole("one", 1)));
    roleHistory.addNewRole(new RoleStatus(new ProviderRole("two", 1)));
    roleHistory.dump();
  }

  //@Test(expected = BadConfigException.class)
  public void testRoleHistoryRoleStartupConflict() throws Throwable {
    MockRoleHistory roleHistory = new MockRoleHistory(Arrays.asList(
        new ProviderRole("one", 1), new ProviderRole("two", 1)
    ));
    roleHistory.dump();
  }
}
