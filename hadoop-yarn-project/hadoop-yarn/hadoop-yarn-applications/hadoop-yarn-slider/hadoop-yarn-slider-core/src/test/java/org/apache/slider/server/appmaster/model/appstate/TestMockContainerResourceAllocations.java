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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockAppState;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Test the container resource allocation logic.
 */
public class TestMockContainerResourceAllocations extends BaseMockAppStateTest {

  @Override
  public Application buildApplication() {
    return factory.newApplication(1, 0, 0).name(getValidTestName());
  }

  @Test
  public void testNormalAllocations() throws Throwable {
    Component role0 = appState.getClusterStatus().getComponent(MockRoles.ROLE0);
    role0.resource(new org.apache.slider.api.resource.Resource().memory("512")
        .cpus(2));
    // hack - because role0 is created before the test run
    RoleStatus role0Status =
        appState.getRoleStatusMap().get(appState.getRoleMap().get(ROLE0).id);
    role0Status.setResourceRequirements(
        appState.buildResourceRequirements(role0Status));
    appState.updateComponents(Collections.singletonMap(role0.getName(),
        role0.getNumberOfContainers()));
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    assertEquals(1, ops.size());
    ContainerRequestOperation operation = (ContainerRequestOperation) ops
        .get(0);
    Resource requirements = operation.getRequest().getCapability();
    assertEquals(512L, requirements.getMemorySize());
    assertEquals(2, requirements.getVirtualCores());
  }

  //TODO replace with resource profile feature in yarn
  @Test
  public void testMaxMemAllocations() throws Throwable {
    // max core allocations no longer supported
    Component role0 = appState.getClusterStatus().getComponent(MockRoles.ROLE0);
    role0.resource(new org.apache.slider.api.resource.Resource()
        .memory(ResourceKeys.YARN_RESOURCE_MAX).cpus(2));
    RoleStatus role0Status =
        appState.getRoleStatusMap().get(appState.getRoleMap().get(ROLE0).id);
    role0Status.setResourceRequirements(
        appState.buildResourceRequirements(role0Status));
    appState.updateComponents(Collections.singletonMap(role0.getName(),
        role0.getNumberOfContainers()));
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    assertEquals(1, ops.size());
    ContainerRequestOperation operation = (ContainerRequestOperation) ops
        .get(0);
    Resource requirements = operation.getRequest().getCapability();
    assertEquals(MockAppState.RM_MAX_RAM, requirements.getMemorySize());
    assertEquals(2, requirements.getVirtualCores());
  }

  @Test
  public void testMaxDefaultAllocations() throws Throwable {
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    assertEquals(ops.size(), 1);
    ContainerRequestOperation operation = (ContainerRequestOperation) ops
        .get(0);
    Resource requirements = operation.getRequest().getCapability();
    assertEquals(ResourceKeys.DEF_YARN_MEMORY, requirements.getMemorySize());
    assertEquals(ResourceKeys.DEF_YARN_CORES, requirements.getVirtualCores());
  }

}
