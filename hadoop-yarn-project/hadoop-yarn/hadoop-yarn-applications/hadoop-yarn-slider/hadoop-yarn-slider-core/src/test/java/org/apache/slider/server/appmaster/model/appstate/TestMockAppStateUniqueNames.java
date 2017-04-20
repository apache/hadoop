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

import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.api.resource.Resource;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine;
import org.apache.slider.server.appmaster.state.AppStateBindingInfo;
import org.apache.slider.server.appmaster.state.MostRecentContainerReleaseSelector;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;

import java.util.Collections;

/**
 * Test that if you have more than one role, the right roles are chosen for
 * release.
 */
public class TestMockAppStateUniqueNames extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  public String getTestName() {
    return "TestMockAppStateUniqueNames";
  }

  /**
   * Small cluster with multiple containers per node,
   * to guarantee many container allocations on each node.
   * @return
   */
  @Override
  public MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(4, 4);
  }

  @Override
  public AppStateBindingInfo buildBindingInfo() {
    AppStateBindingInfo bindingInfo = super.buildBindingInfo();
    bindingInfo.releaseSelector = new MostRecentContainerReleaseSelector();
    return bindingInfo;
  }

  @Override
  public Application buildApplication() {
    Application application = super.buildApplication();

    Component component = new Component().name("group1").numberOfContainers(2L)
        .resource(new Resource().memory("1024").cpus(2))
        .uniqueComponentSupport(true);

    application.getComponents().add(component);
    return application;
  }

  @Test
  public void testDynamicFlexDown() throws Throwable {
    createAndStartNodes();
    appState.updateComponents(Collections.singletonMap("group1", 0L));
    createAndStartNodes();
    RoleStatus roleStatus = appState.lookupRoleStatus("group11");
    assertEquals(0, roleStatus.getDesired());
    assertEquals(1024L, roleStatus.getResourceRequirements().getMemorySize());
    assertEquals(2, roleStatus.getResourceRequirements().getVirtualCores());
    assertEquals("group1", roleStatus.getGroup());
  }

  @Test
  public void testDynamicFlexUp() throws Throwable {
    createAndStartNodes();
    appState.updateComponents(Collections.singletonMap("group1", 3L));
    createAndStartNodes();
    RoleStatus group11 = appState.lookupRoleStatus("group11");
    RoleStatus group12 = appState.lookupRoleStatus("group12");
    RoleStatus group13 = appState.lookupRoleStatus("group13");
    assertEquals(1, group11.getDesired());
    assertEquals(1, group12.getDesired());
    assertEquals(1, group13.getDesired());
    assertEquals(1024L, group11.getResourceRequirements().getMemorySize());
    assertEquals(1024L, group12.getResourceRequirements().getMemorySize());
    assertEquals(1024L, group13.getResourceRequirements().getMemorySize());
    assertEquals(2, group11.getResourceRequirements().getVirtualCores());
    assertEquals(2, group12.getResourceRequirements().getVirtualCores());
    assertEquals(2, group13.getResourceRequirements().getVirtualCores());
    assertEquals("group1", group11.getGroup());
    assertEquals("group1", group12.getGroup());
    assertEquals("group1", group13.getGroup());

    appState.refreshClusterStatus();
  }

}
