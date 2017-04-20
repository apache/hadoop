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
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.resource.Application;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.TriggerClusterTeardownException;
import org.apache.slider.server.appmaster.actions.ResetFailureWindow;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockAM;
import org.apache.slider.server.appmaster.model.mock.MockAppState;
import org.apache.slider.server.appmaster.model.mock.MockRMOperationHandler;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.AppStateBindingInfo;
import org.apache.slider.server.appmaster.state.ContainerOutcome;
import org.apache.slider.server.appmaster.state.NodeEntry;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
public class TestMockAppStateContainerFailure extends BaseMockAppStateTest
    implements MockRoles {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMockAppStateContainerFailure.class);

  private MockRMOperationHandler operationHandler = new
      MockRMOperationHandler();
  private MockAM mockAM = new MockAM();

  @Override
  public String getTestName() {
    return "TestMockAppStateContainerFailure";
  }

  /**
   * Small cluster with multiple containers per node,
   * to guarantee many container allocations on each node.
   * @return
   */
  @Override
  public MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(4, 8000);
  }

  @Override
  public Application buildApplication() {
    Application application = super.buildApplication();
    application.getConfiguration().setProperty(
        ResourceKeys.CONTAINER_FAILURE_THRESHOLD, "10");
    return application;
  }

  @Test
  public void testShortLivedFail() throws Throwable {

    getRole0Status().setDesired(1);
    List<RoleInstance> instances = createAndStartNodes();
    assertEquals(1, instances.size());

    RoleInstance instance = instances.get(0);
    long created = instance.createTime;
    long started = instance.startTime;
    assertTrue(created > 0);
    assertTrue(started >= created);
    List<ContainerId> ids = extractContainerIds(instances, ROLE0);

    ContainerId cid = ids.get(0);
    assertTrue(appState.isShortLived(instance));
    AppState.NodeCompletionResult result = appState.onCompletedContainer(
        containerStatus(cid, 1));
    assertNotNull(result.roleInstance);
    assertTrue(result.containerFailed);
    RoleStatus status = getRole0Status();
    assertEquals(1, status.getFailed());
//    assertEquals(1, status.getStartFailed());

    //view the world
    appState.getRoleHistory().dump();
    List<NodeInstance> queue = appState.getRoleHistory().cloneRecentNodeList(
        getRole0Status().getKey());
    assertEquals(0, queue.size());

  }

  @Test
  public void testLongLivedFail() throws Throwable {

    getRole0Status().setDesired(1);
    List<RoleInstance> instances = createAndStartNodes();
    assertEquals(1, instances.size());

    RoleInstance instance = instances.get(0);
    instance.startTime = System.currentTimeMillis() - 60 * 60 * 1000;
    assertFalse(appState.isShortLived(instance));
    List<ContainerId> ids = extractContainerIds(instances, ROLE0);

    ContainerId cid = ids.get(0);
    AppState.NodeCompletionResult result = appState.onCompletedContainer(
        containerStatus(cid, 1));
    assertNotNull(result.roleInstance);
    assertTrue(result.containerFailed);
    RoleStatus status = getRole0Status();
    assertEquals(1, status.getFailed());
//    assertEquals(0, status.getStartFailed());

    //view the world
    appState.getRoleHistory().dump();
    List<NodeInstance> queue = appState.getRoleHistory().cloneRecentNodeList(
        getRole0Status().getKey());
    assertEquals(1, queue.size());

  }

  @Test
  public void testNodeStartFailure() throws Throwable {

    getRole0Status().setDesired(1);
    List<RoleInstance> instances = createAndSubmitNodes();
    assertEquals(1, instances.size());

    RoleInstance instance = instances.get(0);

    List<ContainerId> ids = extractContainerIds(instances, ROLE0);

    ContainerId cid = ids.get(0);
    appState.onNodeManagerContainerStartFailed(cid, new SliderException(
        "oops"));
    RoleStatus status = getRole0Status();
    assertEquals(1, status.getFailed());
//    assertEquals(1, status.getStartFailed());


    RoleHistory history = appState.getRoleHistory();
    history.dump();
    List<NodeInstance> queue = history.cloneRecentNodeList(getRole0Status()
        .getKey());
    assertEquals(0, queue.size());

    NodeInstance ni = history.getOrCreateNodeInstance(instance.container);
    NodeEntry re = ni.get(getRole0Status().getKey());
    assertEquals(1, re.getFailed());
    assertEquals(1, re.getStartFailed());
  }

  @Test
  public void testRecurrentStartupFailure() throws Throwable {

    getRole0Status().setDesired(1);
    try {
      for (int i = 0; i< 100; i++) {
        List<RoleInstance> instances = createAndSubmitNodes();
        assertEquals(1, instances.size());

        List<ContainerId> ids = extractContainerIds(instances, ROLE0);

        ContainerId cid = ids.get(0);
        LOG.info("{} instance {} {}", i, instances.get(0), cid);
        assertNotNull(cid);
        appState.onNodeManagerContainerStartFailed(cid,
            new SliderException("failure #" + i));
        AppState.NodeCompletionResult result = appState.onCompletedContainer(
            containerStatus(cid));
        assertTrue(result.containerFailed);
      }
      fail("Cluster did not fail from too many startup failures");
    } catch (TriggerClusterTeardownException teardown) {
      LOG.info("Exception {} : {}", teardown.getExitCode(), teardown);
    }
  }

  @Test
  public void testRecurrentStartupFailureWithUnlimitedFailures() throws
      Throwable {
    // Update instance definition to allow containers to fail any number of
    // times
    AppStateBindingInfo bindingInfo = buildBindingInfo();
    bindingInfo.application.getConfiguration().setProperty(
        ResourceKeys.CONTAINER_FAILURE_THRESHOLD, "0");
    appState = new MockAppState(bindingInfo);

    getRole0Status().setDesired(1);
    try {
      for (int i = 0; i < 100; i++) {
        List<RoleInstance> instances = createAndSubmitNodes();
        assertEquals(1, instances.size());

        List<ContainerId> ids = extractContainerIds(instances, ROLE0);

        ContainerId cid = ids.get(0);
        LOG.info("{} instance {} {}", i, instances.get(0), cid);
        assertNotNull(cid);
        appState.onNodeManagerContainerStartFailed(cid,
            new SliderException("failure #" + i));
        AppState.NodeCompletionResult result = appState.onCompletedContainer(
            containerStatus(cid));
        assertTrue(result.containerFailed);
      }
    } catch (TriggerClusterTeardownException teardown) {
      LOG.info("Exception {} : {}", teardown.getExitCode(), teardown);
      fail("Cluster failed despite " + ResourceKeys
          .CONTAINER_FAILURE_THRESHOLD + " = 0");
    }
  }

  @Test
  public void testRoleStatusFailureWindow() throws Throwable {

    ResetFailureWindow resetter = new ResetFailureWindow(operationHandler);

    // initial reset
    resetter.execute(mockAM, null, appState);

    getRole0Status().setDesired(1);
    for (int i = 0; i < 100; i++) {
      resetter.execute(mockAM, null, appState);
      List<RoleInstance> instances = createAndSubmitNodes();
      assertEquals(1, instances.size());

      List<ContainerId> ids = extractContainerIds(instances, ROLE0);

      ContainerId cid = ids.get(0);
      LOG.info("{} instance {} {}", i, instances.get(0), cid);
      assertNotNull(cid);
      appState.onNodeManagerContainerStartFailed(
          cid,
          new SliderException("failure #" + i));
      AppState.NodeCompletionResult result = appState.onCompletedContainer(
          containerStatus(cid));
      assertTrue(result.containerFailed);
    }
  }

  @Test
  public void testRoleStatusFailed() throws Throwable {
    RoleStatus status = getRole0Status();
    // limits exceeded
    appState.incFailedContainers(status, ContainerOutcome.Failed);
    assertEquals(1, status.getFailed());
    assertEquals(1L, status.getFailedRecently());
    assertEquals(0L, status.getLimitsExceeded());
    assertEquals(0L, status.getPreempted());
    assertEquals(0L, status.getDiskFailed());

    ResetFailureWindow resetter = new ResetFailureWindow(operationHandler);
    resetter.execute(mockAM, null, appState);
    assertEquals(1, status.getFailed());
    assertEquals(0L, status.getFailedRecently());
  }

  @Test
  public void testRoleStatusFailedLimitsExceeded() throws Throwable {
    RoleStatus status = getRole0Status();
    // limits exceeded
    appState.incFailedContainers(status, ContainerOutcome
        .Failed_limits_exceeded);
    assertEquals(1, status.getFailed());
    assertEquals(1L, status.getFailedRecently());
    assertEquals(1L, status.getLimitsExceeded());
    assertEquals(0L, status.getPreempted());
    assertEquals(0L, status.getDiskFailed());

    ResetFailureWindow resetter = new ResetFailureWindow(operationHandler);
    resetter.execute(mockAM, null, appState);
    assertEquals(1, status.getFailed());
    assertEquals(0L, status.getFailedRecently());
    assertEquals(1L, status.getLimitsExceeded());
  }


  @Test
  public void testRoleStatusFailedPrempted() throws Throwable {
    RoleStatus status = getRole0Status();
    // limits exceeded
    appState.incFailedContainers(status, ContainerOutcome.Preempted);
    assertEquals(0, status.getFailed());
    assertEquals(1L, status.getPreempted());
    assertEquals(0L, status.getFailedRecently());
    assertEquals(0L, status.getDiskFailed());

    ResetFailureWindow resetter = new ResetFailureWindow(operationHandler);
    resetter.execute(mockAM, null, appState);
    assertEquals(1L, status.getPreempted());
  }


  @Test
  public void testRoleStatusFailedNode() throws Throwable {
    RoleStatus status = getRole0Status();
    // limits exceeded
    appState.incFailedContainers(status, ContainerOutcome.Disk_failure);
    assertEquals(1, status.getFailed());
    assertEquals(0L, status.getFailedRecently());
    assertEquals(0L, status.getLimitsExceeded());
    assertEquals(0L, status.getPreempted());
    assertEquals(1L, status.getDiskFailed());
  }

  @Test
  public void testNodeEntryCompleted() throws Throwable {
    NodeEntry nodeEntry = new NodeEntry(1);
    nodeEntry.containerCompleted(true, ContainerOutcome.Completed);
    assertEquals(0, nodeEntry.getFailed());
    assertEquals(0, nodeEntry.getFailedRecently());
    assertEquals(0, nodeEntry.getStartFailed());
    assertEquals(0, nodeEntry.getPreempted());
    assertEquals(0, nodeEntry.getActive());
    assertTrue(nodeEntry.isAvailable());
  }

  @Test
  public void testNodeEntryFailed() throws Throwable {
    NodeEntry nodeEntry = new NodeEntry(1);
    nodeEntry.containerCompleted(false, ContainerOutcome.Failed);
    assertEquals(1, nodeEntry.getFailed());
    assertEquals(1, nodeEntry.getFailedRecently());
    assertEquals(0, nodeEntry.getStartFailed());
    assertEquals(0, nodeEntry.getPreempted());
    assertEquals(0, nodeEntry.getActive());
    assertTrue(nodeEntry.isAvailable());
    nodeEntry.resetFailedRecently();
    assertEquals(1, nodeEntry.getFailed());
    assertEquals(0, nodeEntry.getFailedRecently());
  }

  @Test
  public void testNodeEntryLimitsExceeded() throws Throwable {
    NodeEntry nodeEntry = new NodeEntry(1);
    nodeEntry.containerCompleted(false, ContainerOutcome
        .Failed_limits_exceeded);
    assertEquals(0, nodeEntry.getFailed());
    assertEquals(0, nodeEntry.getFailedRecently());
    assertEquals(0, nodeEntry.getStartFailed());
    assertEquals(0, nodeEntry.getPreempted());
  }

  @Test
  public void testNodeEntryPreempted() throws Throwable {
    NodeEntry nodeEntry = new NodeEntry(1);
    nodeEntry.containerCompleted(false, ContainerOutcome.Preempted);
    assertEquals(0, nodeEntry.getFailed());
    assertEquals(0, nodeEntry.getFailedRecently());
    assertEquals(0, nodeEntry.getStartFailed());
    assertEquals(1, nodeEntry.getPreempted());
  }

  @Test
  public void testNodeEntryNodeFailure() throws Throwable {
    NodeEntry nodeEntry = new NodeEntry(1);
    nodeEntry.containerCompleted(false, ContainerOutcome.Disk_failure);
    assertEquals(1, nodeEntry.getFailed());
    assertEquals(1, nodeEntry.getFailedRecently());
    assertEquals(0, nodeEntry.getStartFailed());
    assertEquals(0, nodeEntry.getPreempted());
  }



}
