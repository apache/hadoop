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
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.state.RoleStatus;

import static org.apache.slider.api.ResourceKeys.COMPONENT_PLACEMENT_POLICY;

/**
 * Class for basis of Anti-affine placement tests; sets up role2
 * for anti-affinity.
 */
public class BaseMockAppStateAATest extends BaseMockAppStateTest
    implements MockRoles {

  /** Role status for the base AA role. */
  private RoleStatus aaRole;

  /** Role status for the AA role requiring a node with the gpu label. */
  private RoleStatus gpuRole;

  @Override
  public Application buildApplication() {
    Application application = factory.newApplication(0, 0, 0)
        .name(getTestName());
    application.getComponent(ROLE1).getConfiguration().setProperty(
        COMPONENT_PLACEMENT_POLICY, Integer.toString(PlacementPolicy
            .ANTI_AFFINITY_REQUIRED));
    application.getComponent(ROLE1).getConfiguration().setProperty(
        ResourceKeys.YARN_LABEL_EXPRESSION, LABEL_GPU);
    application.getComponent(ROLE2).getConfiguration().setProperty(
        COMPONENT_PLACEMENT_POLICY, Integer.toString(PlacementPolicy
            .ANTI_AFFINITY_REQUIRED));
    return application;
  }


  @Override
  public void setup() throws Exception {
    super.setup();
    aaRole = lookupRole(ROLE2);
    gpuRole = lookupRole(ROLE1);
  }

  protected RoleStatus getAaRole() {
    return aaRole;
  }

  protected RoleStatus getGpuRole() {
    return gpuRole;
  }
}
