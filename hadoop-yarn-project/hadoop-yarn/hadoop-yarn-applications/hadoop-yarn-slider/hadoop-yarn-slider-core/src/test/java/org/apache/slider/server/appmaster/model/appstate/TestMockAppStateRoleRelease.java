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
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
public class TestMockAppStateRoleRelease extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  public String getTestName() {
    return "TestMockAppStateRoleRelease";
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

  //@Test
  public void testAllocateReleaseRealloc() throws Throwable {
    /**
     * Allocate to all nodes
     */
    getRole0Status().setDesired(6);
    getRole1Status().setDesired(5);
    getRole2Status().setDesired(4);
    List<RoleInstance> instances = createAndStartNodes();
    assertEquals(instances.size(), 15);

    //now it is surplus
    getRole0Status().setDesired(0);
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();

    List<ContainerId> released = new ArrayList<>();
    engine.execute(ops, released);
    List<ContainerId> ids = extractContainerIds(instances, ROLE0);
    for (ContainerId cid : released) {
      assertNotNull(appState.onCompletedContainer(containerStatus(cid))
          .roleInstance);
      assertTrue(ids.contains(cid));
    }

    //view the world
    appState.getRoleHistory().dump();

  }

}
