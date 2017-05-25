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
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

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
  public AppStateBindingInfo buildBindingInfo() throws IOException {
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

  public static Map<String, RoleInstance> organize(List<RoleInstance>
      instances) {
    Map<String, RoleInstance> map = new TreeMap<>();
    for (RoleInstance instance : instances) {
      assertFalse("Multiple role instances for unique name " + instance
              .compInstanceName, map.containsKey(instance.compInstanceName));
      System.out.println("Adding to map " + instance.compInstanceName + " for" +
          instance.role);
      map.put(instance.compInstanceName, instance);
    }
    return map;
  }

  public static void verifyInstances(List<RoleInstance> instances, String
      group, String... roles) {
    assertEquals(roles.length, instances.size());
    Map<String, RoleInstance> map = organize(instances);
    int i = 0;
    for (Entry<String, RoleInstance> entry : map.entrySet()) {
      assertEquals(roles[i], entry.getKey());
      RoleInstance instance = entry.getValue();
      assertEquals(roles[i], instance.compInstanceName);
      assertEquals(i, instance.componentId);
      assertEquals(group, instance.role);
      assertEquals(group, instance.providerRole.name);
      assertEquals(group, instance.providerRole.group);
      // TODO remove group from provider role if it continues to be unused
      i++;
    }
  }

  @Test
  public void testDynamicFlexDown() throws Throwable {
    createAndStartNodes();
    List<RoleInstance> instances = appState.cloneOwnedContainerList();
    verifyInstances(instances, "group1", "group10", "group11");

    appState.updateComponents(Collections.singletonMap("group1", 0L));
    createAndStartNodes();
    instances = appState.cloneOwnedContainerList();
    assertEquals(0, instances.size());

    RoleStatus roleStatus = appState.lookupRoleStatus("group1");
    assertEquals(0, roleStatus.getDesired());
    assertEquals(1024L, roleStatus.getResourceRequirements().getMemorySize());
    assertEquals(2, roleStatus.getResourceRequirements().getVirtualCores());
    assertEquals("group1", roleStatus.getGroup());

    // now flex back up
    appState.updateComponents(Collections.singletonMap("group1", 3L));
    createAndStartNodes();
    instances = appState.cloneOwnedContainerList();
    verifyInstances(instances, "group1", "group10", "group11", "group12");
  }

  @Test
  public void testDynamicFlexUp() throws Throwable {
    createAndStartNodes();
    List<RoleInstance> instances = appState.cloneOwnedContainerList();
    verifyInstances(instances, "group1", "group10", "group11");

    appState.updateComponents(Collections.singletonMap("group1", 3L));
    createAndStartNodes();
    instances = appState.cloneOwnedContainerList();
    verifyInstances(instances, "group1", "group10", "group11", "group12");

    RoleStatus group1 = appState.lookupRoleStatus("group1");
    assertEquals(3, group1.getDesired());
    assertEquals(1024L, group1.getResourceRequirements().getMemorySize());
    assertEquals("group1", group1.getGroup());
  }

}
