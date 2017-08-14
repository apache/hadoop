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
import org.apache.slider.api.resource.Application;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockAppState;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.state.AppStateBindingInfo;
import org.apache.slider.server.appmaster.state.NodeEntry;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.NodeMap;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test that app state is rebuilt on a restart.
 */
public class TestMockAppStateRebuildOnAMRestart extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  public String getTestName() {
    return "TestMockAppStateRebuildOnAMRestart";
  }

  //@Test
  public void testRebuild() throws Throwable {

    int r0 = 1;
    int r1 = 2;
    int r2 = 3;
    getRole0Status().setDesired(r0);
    getRole1Status().setDesired(r1);
    getRole2Status().setDesired(r2);
    List<RoleInstance> instances = createAndStartNodes();

    int clusterSize = r0 + r1 + r2;
    assertEquals(instances.size(), clusterSize);

    //clone the list
    List<Container> containers = new ArrayList<>();
    for (RoleInstance ri : instances) {
      containers.add(ri.container);
    }
    NodeMap nodemap = appState.getRoleHistory().cloneNodemap();

    //and rebuild

    AppStateBindingInfo bindingInfo = buildBindingInfo();
    bindingInfo.application = factory.newApplication(r0, r1, r2)
        .name(getValidTestName());
    bindingInfo.liveContainers = containers;
    appState = new MockAppState(bindingInfo);

    assertEquals(appState.getLiveContainers().size(), clusterSize);

    appState.getRoleHistory().dump();

    //check that the app state direct structures match
    List<RoleInstance> r0live = appState.enumLiveNodesInRole(ROLE0);
    List<RoleInstance> r1live = appState.enumLiveNodesInRole(ROLE1);
    List<RoleInstance> r2live = appState.enumLiveNodesInRole(ROLE2);

    assertEquals(r0, r0live.size());
    assertEquals(r1, r1live.size());
    assertEquals(r2, r2live.size());

    //now examine the role history
    NodeMap newNodemap = appState.getRoleHistory().cloneNodemap();

    for (NodeInstance nodeInstance : newNodemap.values()) {
      String hostname = nodeInstance.hostname;
      NodeInstance orig = nodemap.get(hostname);
      assertNotNull("Null entry in original nodemap for " + hostname, orig);

      for (int i : Arrays.asList(getRole0Status().getKey(), getRole1Status()
          .getKey(), getRole2Status().getKey())) {
        assertEquals(nodeInstance.getActiveRoleInstances(i), orig
            .getActiveRoleInstances(i));
        NodeEntry origRE = orig.getOrCreate(i);
        NodeEntry newRE = nodeInstance.getOrCreate(i);
        assertEquals(origRE.getLive(), newRE.getLive());
        assertEquals(0, newRE.getStarting());
      }
    }
    assertEquals(0, appState.reviewRequestAndReleaseNodes().size());

    Application application = appState.getClusterStatus();
    // verify the AM restart container count was set
    Long restarted = application.getNumberOfRunningContainers();
    assertNotNull(restarted);
    //and that the count == 1 master + the region servers
    assertEquals(restarted.longValue(), (long)containers.size());
  }
}
