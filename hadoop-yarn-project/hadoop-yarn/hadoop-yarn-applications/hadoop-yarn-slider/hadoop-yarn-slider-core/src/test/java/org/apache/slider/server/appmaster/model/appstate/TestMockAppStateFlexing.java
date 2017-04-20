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
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.core.exceptions.TriggerClusterTeardownException;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.CancelSingleRequest;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.ContainerAssignment;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Test app state flexing.
 */
public class TestMockAppStateFlexing extends BaseMockAppStateTest implements
    MockRoles {
  private static final Logger LOG =
      LoggerFactory.getLogger(BaseMockAppStateTest.class);

  @Override
  public String getTestName() {
    return "TestMockAppStateFlexing";
  }

  @Test
  public void testFlexDuringLaunchPhase() throws Throwable {

    // ask for one instance of role0
    getRole0Status().setDesired(1);

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();

    // at this point there's now one request in the list
    assertEquals(1, ops.size());
    // and in a liveness check, one outstanding
    ApplicationLivenessInformation liveness =
        appState.getApplicationLivenessInformation();
    assertEquals(1, liveness.requestsOutstanding);
    assertFalse(liveness.allRequestsSatisfied);

    List<Container> allocations = engine.execute(ops);
    List<ContainerAssignment> assignments = new ArrayList<>();
    List<AbstractRMOperation> releases = new ArrayList<>();
    appState.onContainersAllocated(allocations, assignments, releases);
    assertEquals(1, assignments.size());
    ContainerAssignment assigned = assignments.get(0);
    Container target = assigned.container;
    RoleInstance ri = roleInstance(assigned);

    ops = appState.reviewRequestAndReleaseNodes();
    assertTrue(ops.isEmpty());

    liveness = appState.getApplicationLivenessInformation();
    assertEquals(0, liveness.requestsOutstanding);
    assertTrue(liveness.allRequestsSatisfied);

    //now this is the start point.
    appState.containerStartSubmitted(target, ri);

    ops = appState.reviewRequestAndReleaseNodes();
    assertTrue(ops.isEmpty());

    appState.innerOnNodeManagerContainerStarted(target.getId());
  }

  @Test
  public void testFlexBeforeAllocationPhase() throws Throwable {
    getRole0Status().setDesired(1);

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    assertFalse(ops.isEmpty());

    // second scan will find the first run outstanding, so not re-issue
    // any more container requests
    List<AbstractRMOperation> ops2 = appState.reviewRequestAndReleaseNodes();
    assertTrue(ops2.isEmpty());

    // and in a liveness check, one outstanding
    ApplicationLivenessInformation liveness = appState
        .getApplicationLivenessInformation();
    assertEquals(1, liveness.requestsOutstanding);
    assertFalse(liveness.allRequestsSatisfied);

    appState.refreshClusterStatus();
    Application application = appState.getClusterStatus();
    // TODO cluster status returns liveness info
//    assertEquals(1, cd.liveness.requestsOutstanding);

  }


  @Test
  public void testFlexDownTwice() throws Throwable {
    int r0 = 6;
    int r1 = 0;
    int r2 = 0;
    getRole0Status().setDesired(r0);
    getRole1Status().setDesired(r1);
    getRole2Status().setDesired(r2);
    List<RoleInstance> instances = createAndStartNodes();

    int clusterSize = r0 + r1 + r2;
    assertEquals(instances.size(), clusterSize);
    LOG.info("shrinking cluster");
    r0 = 4;
    getRole0Status().setDesired(r0);
    List<AppState.NodeCompletionResult> completionResults = new ArrayList<>();
    instances = createStartAndStopNodes(completionResults);
    assertEquals(0, instances.size());
    // assert two nodes were released
    assertEquals(2, completionResults.size());

    // no-op review
    completionResults = new ArrayList<>();
    instances = createStartAndStopNodes(completionResults);
    assertEquals(0, instances.size());
    // assert two nodes were released
    assertEquals(0, completionResults.size());


    // now shrink again
    getRole0Status().setDesired(1);
    completionResults = new ArrayList<>();
    instances = createStartAndStopNodes(completionResults);
    assertEquals(0, instances.size());
    // assert two nodes were released
    assertEquals(3, completionResults.size());

  }

  @Test
  public void testFlexNegative() throws Throwable {
    int r0 = 6;
    int r1 = 0;
    int r2 = 0;
    getRole0Status().setDesired(r0);
    getRole1Status().setDesired(r1);
    getRole2Status().setDesired(r2);
    List<RoleInstance> instances = createAndStartNodes();

    int clusterSize = r0 + r1 + r2;
    assertEquals(instances.size(), clusterSize);
    LOG.info("shrinking cluster");
    getRole0Status().setDesired(-2);
    List<AppState.NodeCompletionResult> completionResults = new ArrayList<>();
    try {
      createStartAndStopNodes(completionResults);
      fail("expected an exception");
    } catch (TriggerClusterTeardownException e) {
    }

  }

  @Test
  public void testCancelWithRequestsOutstanding() throws Throwable {
    // flex cluster size before the original set were allocated


    getRole0Status().setDesired(6);
    // build the ops
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    // here the data structures exist

    // go down
    getRole0Status().setDesired(3);
    List<AbstractRMOperation> ops2 = appState.reviewRequestAndReleaseNodes();
    assertEquals(3, ops2.size());
    for (AbstractRMOperation op : ops2) {
      assertTrue(op instanceof CancelSingleRequest);
    }

  }

}
