/**
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

package org.apache.hadoop.yarn.client.api.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.ContainerStateTransitionListener;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;

import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestNMClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestNMClient.class);
  private static final int MAX_EARLY_FINISH = 3;
  private Configuration conf;
  private MiniYARNCluster yarnCluster;
  private YarnClientImpl yarnClient;
  private AMRMClientImpl<ContainerRequest> rmClient;
  private NMClientImpl nmClient;
  private List<NodeReport> nodeReports;
  private NMTokenCache nmTokenCache;
  private RMAppAttempt appAttempt;

  /**
   * Container State transition listener to track the number of times
   * a container has transitioned into a state.
   */
  public static class DebugSumContainerStateListener implements ContainerStateTransitionListener {

    private static final Logger LOG =
        LoggerFactory.getLogger(DebugSumContainerStateListener.class);
    private static final Map<ContainerId,
        Map<org.apache.hadoop.yarn.server.nodemanager.containermanager
            .container.ContainerState, Long>>
        TRANSITION_COUNTER = new HashMap<>();

    public void init(Context context) {
    }

    public void preTransition(ContainerImpl op,
                              org.apache.hadoop.yarn.server.nodemanager
                                  .containermanager.container.ContainerState
                                  beforeState,
                              ContainerEvent eventToBeProcessed) {
    }

    public void postTransition(
        ContainerImpl op,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container
            .ContainerState beforeState,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container
            .ContainerState afterState,
        ContainerEvent processedEvent) {
      synchronized (TRANSITION_COUNTER) {
        if (beforeState != afterState) {
          ContainerId id = op.getContainerId();
          TRANSITION_COUNTER.putIfAbsent(id, new HashMap<>());
          long sum = TRANSITION_COUNTER.get(id)
              .compute(afterState, (state, count) -> count == null ? 1 : count + 1);
          LOG.info("***** " + id +
              " Transition from " + beforeState +
              " to " + afterState +
              " sum:" + sum);
        }
      }
    }

    /**
     * Get the current number of state transitions.
     * This is useful to check, if an event has occurred in unit tests.
     * @param id Container id to check
     * @return Number of transitions to the state specified
     */
    static long getTransitionCounter(ContainerId id) {
      return TRANSITION_COUNTER
          .getOrDefault(id, new HashMap<>())
          .getOrDefault(org.apache.hadoop.yarn.server.nodemanager
              .containermanager.container
              .ContainerState.RUNNING, 0L);
    }
  }

  public void setup() throws YarnException, IOException, InterruptedException, TimeoutException {
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_CONTAINER_STATE_TRANSITION_LISTENERS,
        DebugSumContainerStateListener.class.getName());
    startYarnCluster();
    startYarnClient();
    UserGroupInformation.setLoginUser(UserGroupInformation
      .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
    nmTokenCache = new NMTokenCache();
    startRMClient();
    startNMClient();
  }


  private void startYarnCluster() {
    yarnCluster = new MiniYARNCluster(TestNMClient.class.getName(), 3, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();
    assertEquals(STATE.STARTED, yarnCluster.getServiceState());
  }

  private void startYarnClient()
      throws IOException, YarnException, InterruptedException, TimeoutException {
    yarnClient = (YarnClientImpl) YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    assertEquals(STATE.STARTED, yarnClient.getServiceState());
    nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    ApplicationSubmissionContext appContext =
        yarnClient.createApplication().getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    appContext.setApplicationName("Test");
    Priority pri = Priority.newInstance(0);
    appContext.setPriority(pri);
    appContext.setQueue("default");
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    appContext.setAMContainerSpec(amContainer);
    appContext.setUnmanagedAM(true);
    SubmitApplicationRequest appRequest = Records.newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    yarnClient.submitApplication(appContext);
    GenericTestUtils.waitFor(() -> yarnCluster.getResourceManager().getRMContext().getRMApps()
        .get(appId).getCurrentAppAttempt().getAppAttemptState() == RMAppAttemptState.LAUNCHED,
        100, 30_000, "Failed to start app");
    appAttempt = yarnCluster.getResourceManager().getRMContext().getRMApps()
        .get(appId).getCurrentAppAttempt();
  }

  private void startRMClient() {
    rmClient = (AMRMClientImpl<ContainerRequest>) AMRMClient.createAMRMClient();
    rmClient.setNMTokenCache(nmTokenCache);
    rmClient.init(conf);
    rmClient.start();
    assertEquals(STATE.STARTED, rmClient.getServiceState());
  }

  private void startNMClient() {
    nmClient = (NMClientImpl) NMClient.createNMClient();
    nmClient.setNMTokenCache(rmClient.getNMTokenCache());
    nmClient.init(conf);
    nmClient.start();
    assertEquals(STATE.STARTED, nmClient.getServiceState());
  }

  public void tearDown() throws InterruptedException {
    rmClient.stop();
    yarnClient.stop();
    yarnCluster.stop();
  }

  @Test (timeout = 180_000 * MAX_EARLY_FINISH)
  public void testNMClientNoCleanupOnStop()
      throws YarnException, IOException, InterruptedException, TimeoutException {
    int earlyFinishCounter = MAX_EARLY_FINISH;
    while (0 < earlyFinishCounter) {
      try {
        setup();
        rmClient.registerApplicationMaster("Host", 10_000, "");
        testContainerManagement(nmClient, allocateContainers(rmClient, 5));
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
        stopNmClient();
        assertFalse(nmClient.startedContainers.isEmpty());
        nmClient.cleanupRunningContainers();
        assertEquals(0, nmClient.startedContainers.size());
        return;
      } catch (EarlyFinishException e) {
        --earlyFinishCounter;
      } finally {
        tearDown();
      }
    }
    if (earlyFinishCounter == 0) {
      fail("Too many early finish exception happened");
    }
  }

  @Test (timeout = 200_000 * MAX_EARLY_FINISH)
  public void testNMClient()
      throws YarnException, IOException, InterruptedException, TimeoutException {
    int earlyFinishCounter = MAX_EARLY_FINISH;
    while (0 < earlyFinishCounter) {
      try {
        setup();
        rmClient.registerApplicationMaster("Host", 10_000, "");
        testContainerManagement(nmClient, allocateContainers(rmClient, 5));
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
        // stop the running containers on close
        assertFalse(nmClient.startedContainers.isEmpty());
        nmClient.cleanupRunningContainersOnStop(true);
        assertTrue(nmClient.getCleanupRunningContainers().get());
        nmClient.stop();
        return;
      } catch (EarlyFinishException e) {
        --earlyFinishCounter;
      } finally {
        tearDown();
      }
    }
    if (earlyFinishCounter == 0) {
      fail("Too many early finish exception happened");
    }
  }

  private void stopNmClient() {
    assertNotNull("Null nmClient", nmClient);
    // leave one unclosed
    assertEquals(1, nmClient.startedContainers.size());
    // default true
    assertTrue(nmClient.getCleanupRunningContainers().get());
    nmClient.cleanupRunningContainersOnStop(false);
    assertFalse(nmClient.getCleanupRunningContainers().get());
    nmClient.stop();
  }

  private Set<Container> allocateContainers(
      AMRMClientImpl<ContainerRequest> rmClient, int num
  ) throws YarnException, IOException {
    for (int i = 0; i < num; ++i) {
      rmClient.addContainerRequest(new ContainerRequest(
          Resource.newInstance(256, 0),
          new String[] {nodeReports.get(0).getNodeId().getHost()},
          new String[] {nodeReports.get(0).getRackName()},
          Priority.newInstance(0)
      ));
    }
    Set<Container> allocateContainers = new TreeSet<>();
    while (allocateContainers.size() < num) {
      AllocateResponse allocResponse = rmClient.allocate(0.1f);
      allocateContainers.addAll(allocResponse.getAllocatedContainers());
      for (NMToken token : allocResponse.getNMTokens()) {
        rmClient.getNMTokenCache().setToken(token.getNodeId().toString(), token.getToken());
      }
      if (allocateContainers.size() < num) {
        sleep(100);
      }
    }
    return allocateContainers;
  }

  private void testContainerManagement(
      NMClientImpl client, Set<Container> containers
  ) throws YarnException, IOException, EarlyFinishException {
    int size = containers.size();
    int i = 0;
    for (Container container : containers) {
      // getContainerStatus shouldn't be called before startContainer,
      // otherwise, NodeManager cannot find the container
      assertYarnException(
          () -> client.getContainerStatus(container.getId(), container.getNodeId()),
          "is not handled by this NodeManager");
      // upadateContainerResource shouldn't be called before startContainer,
      // otherwise, NodeManager cannot find the container
      assertYarnException(
          () -> client.updateContainerResource(container),
          "is not handled by this NodeManager");
      // restart shouldn't be called before startContainer,
      // otherwise, NodeManager cannot find the container
      assertYarnException(
          () -> client.restartContainer(container.getId()),
          "Unknown container");
      // rollback shouldn't be called before startContainer,
      // otherwise, NodeManager cannot find the container
      assertYarnException(
          () -> client.rollbackLastReInitialization(container.getId()),
          "Unknown container");
      // commit shouldn't be called before startContainer,
      // otherwise, NodeManager cannot find the container
      assertYarnException(
          () -> client.commitLastReInitialization(container.getId()),
          "Unknown container");
      // stopContainer shouldn't be called before startContainer,
      // otherwise, an exception will be thrown
      assertYarnException(
          () -> client.stopContainer(container.getId(), container.getNodeId()),
          "is not handled by this NodeManager");

      Credentials ts = new Credentials();
      DataOutputBuffer dob = new DataOutputBuffer();
      ts.writeTokenStorageToStream(dob);
      ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);
      clc.setCommands(Shell.WINDOWS
          ? Arrays.asList("ping", "-n", "10000000", "127.0.0.1", ">nul")
          : Arrays.asList("sleep", "1000000")
      );
      clc.setTokens(securityTokens);
      client.startContainer(container, clc);
      // leave one container unclosed
      if (++i < size) {
        testContainer(client, i, container, clc);
      }
    }
  }

  private void testContainer(
      NMClientImpl client, int i, Container container, ContainerLaunchContext clc
  ) throws YarnException, IOException, EarlyFinishException {
    testContainerStatusRunning(container);
    waitForContainerTransitionCount(container, 1);
    testIncreaseContainerResource(container);
    testRestartContainer(container);
    testContainerStatusRunning(container, "will be Restarted");
    waitForContainerTransitionCount(container, 2);
    if (i % 2 == 0) {
      testReInitializeContainer(container, clc, false);
      testContainerStatusRunning(container,  "will be Re-initialized");
      waitForContainerTransitionCount(container, 3);
      testContainerRollback(container, true);
      testContainerStatusRunning(container, "will be Rolled-back");
      waitForContainerTransitionCount(container, 4);
      testContainerCommit(container, false);
      testReInitializeContainer(container, clc, false);
      testContainerStatusRunning(container, "will be Re-initialized");
      waitForContainerTransitionCount(container, 5);
      testContainerCommit(container, true);
    } else {
      testReInitializeContainer(container, clc, true);
      testContainerStatusRunning(container, "will be Re-initialized");
      waitForContainerTransitionCount(container, 3);
      testContainerRollback(container, false);
      testContainerCommit(container, false);
    }
    client.stopContainer(container.getId(), container.getNodeId());
    testContainerStatusCompleted(container, "killed by the ApplicationMaster");
  }

  /**
   * Wait until the container reaches a state N times.
   * @param container container to watch
   * @param transitions the number N above
   */
  private void waitForContainerTransitionCount(Container container, long transitions) {
    while (DebugSumContainerStateListener.getTransitionCounter(container.getId()) != transitions) {
      sleep(1000);
    }
  }

  private void testContainerStatusRunning(
      Container container, String... diagnostics
  ) throws YarnException, IOException, EarlyFinishException {
    ContainerStatus actualStatus = nmClient.getContainerStatus(container.getId(), container.getNodeId());
    while (ContainerState.NEW == actualStatus.getState()) {
      sleep(100);
      actualStatus = nmClient.getContainerStatus(container.getId(), container.getNodeId());
    }
    if (ContainerState.COMPLETE == actualStatus.getState()) {
      LOG.warn("The container finished earlier than expected, EXIT_CODE[{}], DIAGNOSTIC[{}]",
          actualStatus.getExitStatus(), actualStatus.getDiagnostics());
      throw new EarlyFinishException();
    }
    assertEquals(container.getId(), actualStatus.getContainerId());
    assertEquals(actualStatus.getExitStatus(), ContainerExitStatus.INVALID);
    for (String diagnostic : diagnostics) {
      assertTrue(actualStatus.getDiagnostics().contains(diagnostic));
    }
  }

  private void testContainerStatusCompleted(
      Container container, String... diagnostics
  ) throws YarnException, IOException {
    ContainerStatus actualStatus = nmClient.getContainerStatus(container.getId(), container.getNodeId());
    while (ContainerState.COMPLETE != actualStatus.getState()) {
      sleep(100);
      actualStatus = nmClient.getContainerStatus(container.getId(), container.getNodeId());
    }
    assertEquals(container.getId(), actualStatus.getContainerId());
    assertTrue(Arrays.asList(
        ContainerExitStatus.KILLED_BY_APPMASTER,
        ContainerExitStatus.SUCCESS
    ).contains(actualStatus.getExitStatus()));
    for (String diagnostic : diagnostics) {
      assertTrue(actualStatus.getDiagnostics().contains(diagnostic));
    }
  }

  @SuppressWarnings("deprecation")
  private void testIncreaseContainerResource(Container container) {
    assertYarnException(
        () -> nmClient.increaseContainerResource(container),
        container.getId() + " has update version ");
  }

  private void testRestartContainer(Container container) throws IOException, YarnException {
    nmClient.restartContainer(container.getId());
  }

  private void testContainerRollback(Container container, boolean enabled)
      throws IOException, YarnException {
    if (enabled) {
      nmClient.rollbackLastReInitialization(container.getId());
    } else {
      assertYarnException(
          () -> nmClient.rollbackLastReInitialization(container.getId()),
          "Nothing to rollback to");
    }
  }

  private void testContainerCommit(Container container, boolean enabled)
      throws IOException, YarnException {
    if (enabled) {
      nmClient.commitLastReInitialization(container.getId());
    } else {
      assertYarnException(
          () -> nmClient.commitLastReInitialization(container.getId()),
          "Nothing to Commit");
    }
  }

  private void testReInitializeContainer(
      Container container, ContainerLaunchContext clc, boolean autoCommit
  ) throws IOException, YarnException {
    nmClient.reInitializeContainer(container.getId(), clc, autoCommit);
  }

  private void assertYarnException(ThrowingRunnable runnable, String text) {
    YarnException e = assertThrows(YarnException.class, runnable);
    assertTrue(String.format("The thrown exception is not expected cause it has text [%s]"
        + ", what not contains text [%s]", e.getMessage(), text), e.getMessage().contains(text));
  }

  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  private static class EarlyFinishException extends Exception {}
}
