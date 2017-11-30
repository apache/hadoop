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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ProfileCapability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestNMClient {
  Configuration conf = null;
  MiniYARNCluster yarnCluster = null;
  YarnClientImpl yarnClient = null;
  AMRMClientImpl<ContainerRequest> rmClient = null;
  NMClientImpl nmClient = null;
  List<NodeReport> nodeReports = null;
  ApplicationAttemptId attemptId = null;
  int nodeCount = 3;
  NMTokenCache nmTokenCache = null;

  /**
   * Container State transition listener to track the number of times
   * a container has transitioned into a state.
   */
  public static class DebugSumContainerStateListener
      implements ContainerStateTransitionListener {

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
          TRANSITION_COUNTER
              .putIfAbsent(id, new HashMap<>());
          long sum = TRANSITION_COUNTER.get(id)
              .compute(afterState,
                  (state, count) -> count == null ? 1 : count + 1);
          LOG.info("***** " + id +
              " Transition from " + beforeState +
              " to " + afterState +
              "sum:" + sum);
        }
      }
    }

    /**
     * Get the current number of state transitions.
     * This is useful to check, if an event has occurred in unit tests.
     * @param id Container id to check
     * @param state Return the overall number of transitions to this state
     * @return Number of transitions to the state specified
     */
    static long getTransitionCounter(ContainerId id,
                                     org.apache.hadoop.yarn.server.nodemanager
                                         .containermanager.container
                                         .ContainerState state) {
      Long ret = TRANSITION_COUNTER.getOrDefault(id, new HashMap<>())
          .get(state);
      return ret != null ? ret : 0;
    }
  }

  @Before
  public void setup() throws YarnException, IOException {
    // start minicluster
    conf = new YarnConfiguration();
    // Turn on state tracking
    conf.set(YarnConfiguration.NM_CONTAINER_STATE_TRANSITION_LISTENERS,
        DebugSumContainerStateListener.class.getName());
    yarnCluster =
        new MiniYARNCluster(TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();
    assertNotNull(yarnCluster);
    assertEquals(STATE.STARTED, yarnCluster.getServiceState());

    // start rm client
    yarnClient = (YarnClientImpl) YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    assertNotNull(yarnClient);
    assertEquals(STATE.STARTED, yarnClient.getServiceState());

    // get node info
    nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);

    // submit new app
    ApplicationSubmissionContext appContext = 
        yarnClient.createApplication().getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    // set the application name
    appContext.setApplicationName("Test");
    // Set the priority for the application master
    Priority pri = Priority.newInstance(0);
    appContext.setPriority(pri);
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records
        .newRecord(ContainerLaunchContext.class);
    appContext.setAMContainerSpec(amContainer);
    // unmanaged AM
    appContext.setUnmanagedAM(true);
    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    // Submit the application to the applications manager
    yarnClient.submitApplication(appContext);

    // wait for app to start
    int iterationsLeft = 30;
    RMAppAttempt appAttempt = null;
    while (iterationsLeft > 0) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState() ==
          YarnApplicationState.ACCEPTED) {
        attemptId = appReport.getCurrentApplicationAttemptId();
        appAttempt =
            yarnCluster.getResourceManager().getRMContext().getRMApps()
              .get(attemptId.getApplicationId()).getCurrentAppAttempt();
        while (true) {
          if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
            break;
          }
        }
        break;
      }
      sleep(1000);
      --iterationsLeft;
    }
    if (iterationsLeft == 0) {
      fail("Application hasn't bee started");
    }

    // Just dig into the ResourceManager and get the AMRMToken just for the sake
    // of testing.
    UserGroupInformation.setLoginUser(UserGroupInformation
      .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());

    //creating an instance NMTokenCase
    nmTokenCache = new NMTokenCache();
    
    // start am rm client
    rmClient =
        (AMRMClientImpl<ContainerRequest>) AMRMClient
          .<ContainerRequest> createAMRMClient();

    //setting an instance NMTokenCase
    rmClient.setNMTokenCache(nmTokenCache);
    rmClient.init(conf);
    rmClient.start();
    assertNotNull(rmClient);
    assertEquals(STATE.STARTED, rmClient.getServiceState());

    // start am nm client
    nmClient = (NMClientImpl) NMClient.createNMClient();
    
    //propagating the AMRMClient NMTokenCache instance
    nmClient.setNMTokenCache(rmClient.getNMTokenCache());
    nmClient.init(conf);
    nmClient.start();
    assertNotNull(nmClient);
    assertEquals(STATE.STARTED, nmClient.getServiceState());
  }

  @After
  public void tearDown() {
    rmClient.stop();
    yarnClient.stop();
    yarnCluster.stop();
  }

  private void stopNmClient(boolean stopContainers) {
    assertNotNull("Null nmClient", nmClient);
    // leave one unclosed
    assertEquals(1, nmClient.startedContainers.size());
    // default true
    assertTrue(nmClient.getCleanupRunningContainers().get());
    nmClient.cleanupRunningContainersOnStop(stopContainers);
    assertEquals(stopContainers, nmClient.getCleanupRunningContainers().get());
    nmClient.stop();
  }

  @Test (timeout = 180000)
  public void testNMClientNoCleanupOnStop()
      throws YarnException, IOException {

    rmClient.registerApplicationMaster("Host", 10000, "");

    testContainerManagement(nmClient, allocateContainers(rmClient, 5));

    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
        null, null);
    // don't stop the running containers
    stopNmClient(false);
    assertFalse(nmClient.startedContainers.isEmpty());
    //now cleanup
    nmClient.cleanupRunningContainers();
    assertEquals(0, nmClient.startedContainers.size());
  }

  @Test (timeout = 200000)
  public void testNMClient()
      throws YarnException, IOException {
    rmClient.registerApplicationMaster("Host", 10000, "");

    testContainerManagement(nmClient, allocateContainers(rmClient, 5));
    
    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
        null, null);
    // stop the running containers on close
    assertFalse(nmClient.startedContainers.isEmpty());
    nmClient.cleanupRunningContainersOnStop(true);
    assertTrue(nmClient.getCleanupRunningContainers().get());
    nmClient.stop();
  }

  private Set<Container> allocateContainers(
      AMRMClientImpl<ContainerRequest> rmClient, int num)
      throws YarnException, IOException {
    // setup container request
    Resource capability = Resource.newInstance(1024, 0);
    Priority priority = Priority.newInstance(0);
    String node = nodeReports.get(0).getNodeId().getHost();
    String rack = nodeReports.get(0).getRackName();
    String[] nodes = new String[] {node};
    String[] racks = new String[] {rack};

    for (int i = 0; i < num; ++i) {
      rmClient.addContainerRequest(new ContainerRequest(capability, nodes,
          racks, priority));
    }

    ProfileCapability profileCapability =
        ProfileCapability.newInstance(capability);
    int containersRequestedAny = rmClient.getTable(0)
        .get(priority, ResourceRequest.ANY, ExecutionType.GUARANTEED,
            profileCapability).remoteRequest.getNumContainers();

    // RM should allocate container within 2 calls to allocate()
    int allocatedContainerCount = 0;
    int iterationsLeft = 2;
    Set<Container> containers = new TreeSet<Container>();
    while (allocatedContainerCount < containersRequestedAny
        && iterationsLeft > 0) {
      AllocateResponse allocResponse = rmClient.allocate(0.1f);

      allocatedContainerCount += allocResponse.getAllocatedContainers().size();
      for(Container container : allocResponse.getAllocatedContainers()) {
        containers.add(container);
      }
      if (!allocResponse.getNMTokens().isEmpty()) {
        for (NMToken token : allocResponse.getNMTokens()) {
          rmClient.getNMTokenCache().setToken(token.getNodeId().toString(),
              token.getToken());
        }
      }
      if(allocatedContainerCount < containersRequestedAny) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(1000);
      }

      --iterationsLeft;
    }
    return containers;
  }

  private void testContainerManagement(NMClientImpl client,
      Set<Container> containers) throws YarnException, IOException {
    int size = containers.size();
    int i = 0;
    for (Container container : containers) {
      // getContainerStatus shouldn't be called before startContainer,
      // otherwise, NodeManager cannot find the container
      try {
        client.getContainerStatus(container.getId(), container.getNodeId());
        fail("Exception is expected");
      } catch (YarnException e) {
        assertTrue("The thrown exception is not expected",
            e.getMessage().contains("is not handled by this NodeManager"));
      }
      // upadateContainerResource shouldn't be called before startContainer,
      // otherwise, NodeManager cannot find the container
      try {
        client.updateContainerResource(container);
        fail("Exception is expected");
      } catch (YarnException e) {
        assertTrue("The thrown exception is not expected",
            e.getMessage().contains("is not handled by this NodeManager"));
      }

      // restart shouldn't be called before startContainer,
      // otherwise, NodeManager cannot find the container
      try {
        client.restartContainer(container.getId());
        fail("Exception is expected");
      } catch (YarnException e) {
        assertTrue("The thrown exception is not expected",
            e.getMessage().contains("Unknown container"));
      }

      // rollback shouldn't be called before startContainer,
      // otherwise, NodeManager cannot find the container
      try {
        client.rollbackLastReInitialization(container.getId());
        fail("Exception is expected");
      } catch (YarnException e) {
        assertTrue("The thrown exception is not expected",
            e.getMessage().contains("Unknown container"));
      }

      // commit shouldn't be called before startContainer,
      // otherwise, NodeManager cannot find the container
      try {
        client.commitLastReInitialization(container.getId());
        fail("Exception is expected");
      } catch (YarnException e) {
        assertTrue("The thrown exception is not expected",
            e.getMessage().contains("Unknown container"));
      }

      // stopContainer shouldn't be called before startContainer,
      // otherwise, an exception will be thrown
      try {
        client.stopContainer(container.getId(), container.getNodeId());
        fail("Exception is expected");
      } catch (YarnException e) {
        if (!e.getMessage()
              .contains("is not handled by this NodeManager")) {
          throw new AssertionError("Exception is not expected: ", e);
        }
      }

      Credentials ts = new Credentials();
      DataOutputBuffer dob = new DataOutputBuffer();
      ts.writeTokenStorageToStream(dob);
      ByteBuffer securityTokens =
          ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      ContainerLaunchContext clc =
          Records.newRecord(ContainerLaunchContext.class);
      if (Shell.WINDOWS) {
        clc.setCommands(
            Arrays.asList("ping", "-n", "10000000", "127.0.0.1", ">nul"));
      } else {
        clc.setCommands(Arrays.asList("sleep", "1000000"));
      }
      clc.setTokens(securityTokens);
      try {
        client.startContainer(container, clc);
      } catch (YarnException e) {
        throw new AssertionError("Exception is not expected ", e);
      }
      List<Integer> exitStatuses = Collections.singletonList(-1000);

      // leave one container unclosed
      if (++i < size) {
        testContainer(client, i, container, clc, exitStatuses);

      }
    }
  }

  private void testContainer(NMClientImpl client, int i, Container container,
                             ContainerLaunchContext clc, List<Integer> exitCode)
      throws YarnException, IOException {
    // NodeManager may still need some time to make the container started
    testGetContainerStatus(container, i, ContainerState.RUNNING, "",
        exitCode);
    waitForContainerTransitionCount(container,
        org.apache.hadoop.yarn.server.nodemanager.
            containermanager.container.ContainerState.RUNNING, 1);
    // Test increase container API and make sure requests can reach NM
    testIncreaseContainerResource(container);

    testRestartContainer(container.getId());
    testGetContainerStatus(container, i, ContainerState.RUNNING,
        "will be Restarted", exitCode);
    waitForContainerTransitionCount(container,
        org.apache.hadoop.yarn.server.nodemanager.
            containermanager.container.ContainerState.RUNNING, 2);

    if (i % 2 == 0) {
      testReInitializeContainer(container.getId(), clc, false);
      testGetContainerStatus(container, i, ContainerState.RUNNING,
          "will be Re-initialized", exitCode);
      waitForContainerTransitionCount(container,
          org.apache.hadoop.yarn.server.nodemanager.
              containermanager.container.ContainerState.RUNNING, 3);

      testRollbackContainer(container.getId(), false);
      testGetContainerStatus(container, i, ContainerState.RUNNING,
          "will be Rolled-back", exitCode);
      waitForContainerTransitionCount(container,
          org.apache.hadoop.yarn.server.nodemanager.
              containermanager.container.ContainerState.RUNNING, 4);

      testCommitContainer(container.getId(), true);
      testReInitializeContainer(container.getId(), clc, false);
      testGetContainerStatus(container, i, ContainerState.RUNNING,
          "will be Re-initialized", exitCode);
      waitForContainerTransitionCount(container,
          org.apache.hadoop.yarn.server.nodemanager.
              containermanager.container.ContainerState.RUNNING, 5);
      testCommitContainer(container.getId(), false);
    } else {
      testReInitializeContainer(container.getId(), clc, true);
      testGetContainerStatus(container, i, ContainerState.RUNNING,
          "will be Re-initialized", exitCode);
      waitForContainerTransitionCount(container,
          org.apache.hadoop.yarn.server.nodemanager.
              containermanager.container.ContainerState.RUNNING, 3);
      testRollbackContainer(container.getId(), true);
      testCommitContainer(container.getId(), true);
    }

    try {
      client.stopContainer(container.getId(), container.getNodeId());
    } catch (YarnException e) {
      throw (AssertionError)
        (new AssertionError("Exception is not expected: " + e, e));
    }

    // getContainerStatus can be called after stopContainer
    try {
      // O is possible if CLEANUP_CONTAINER is executed too late
      // -105 is possible if the container is not terminated but killed
      testGetContainerStatus(container, i, ContainerState.COMPLETE,
          "Container killed by the ApplicationMaster.",
          Arrays.asList(
              ContainerExitStatus.KILLED_BY_APPMASTER,
              ContainerExitStatus.SUCCESS));
    } catch (YarnException e) {
      // The exception is possible because, after the container is stopped,
      // it may be removed from NM's context.
      if (!e.getMessage()
            .contains("was recently stopped on node manager")) {
        throw (AssertionError)
          (new AssertionError("Exception is not expected: ", e));
      }
    }
  }

  /**
   * Wait until the container reaches a state N times.
   * @param container container to watch
   * @param state state to test
   * @param transitions the number N above
   * @throws YarnException This happens if the test times out while waiting
   */
  private void waitForContainerTransitionCount(
      Container container,
      org.apache.hadoop.yarn.server.nodemanager.
          containermanager.container.ContainerState state, long transitions)
      throws YarnException {
    long transitionCount = -1;
    do {
      if (transitionCount != -1) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          throw new YarnException(
              "Timeout at transition count:" + transitionCount, e);
        }
      }
      transitionCount = DebugSumContainerStateListener
          .getTransitionCounter(container.getId(), state);
    } while (transitionCount != transitions);
  }

  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void testGetContainerStatus(Container container, int index,
      ContainerState state, String diagnostics, List<Integer> exitStatuses)
          throws YarnException, IOException {
    while (true) {
      sleep(250);
      ContainerStatus status = nmClient.getContainerStatus(
          container.getId(), container.getNodeId());
      // NodeManager may still need some time to get the stable
      // container status
      if (status.getState() == state) {
        assertEquals(container.getId(), status.getContainerId());
        assertTrue("" + index + ": " + status.getDiagnostics(),
            status.getDiagnostics().contains(diagnostics));

        assertTrue("Exit Statuses are supposed to be in: " + exitStatuses +
                ", but the actual exit status code is: " +
                status.getExitStatus(),
            exitStatuses.contains(status.getExitStatus()));
        break;
      }
    }
  }

  @SuppressWarnings("deprecation")
  private void testIncreaseContainerResource(Container container)
    throws YarnException, IOException {
    try {
      nmClient.increaseContainerResource(container);
    } catch (YarnException e) {
      // NM container increase container resource should fail without a version
      // increase action to fail.
      if (!e.getMessage().contains(
          container.getId() + " has update version ")) {
        throw (AssertionError)
            (new AssertionError("Exception is not expected: " + e)
                .initCause(e));
      }
    }
  }

  private void testRestartContainer(ContainerId containerId)
      throws YarnException, IOException {
    try {
      sleep(250);
      nmClient.restartContainer(containerId);
      sleep(250);
    } catch (YarnException e) {
      // NM container will only be in SCHEDULED state, so expect the increase
      // action to fail.
      if (!e.getMessage().contains(
          "can only be changed when a container is in RUNNING state")) {
        throw (AssertionError)
            (new AssertionError("Exception is not expected: " + e)
                .initCause(e));
      }
    }
  }

  private void testRollbackContainer(ContainerId containerId,
      boolean notRollbackable) throws YarnException, IOException {
    try {
      sleep(250);
      nmClient.rollbackLastReInitialization(containerId);
      if (notRollbackable) {
        fail("Should not be able to rollback..");
      }
      sleep(250);
    } catch (YarnException e) {
      // NM container will only be in SCHEDULED state, so expect the increase
      // action to fail.
      if (notRollbackable) {
        Assert.assertTrue(e.getMessage().contains(
            "Nothing to rollback to"));
      } else {
        if (!e.getMessage().contains(
            "can only be changed when a container is in RUNNING state")) {
          throw (AssertionError)
              (new AssertionError("Exception is not expected: " + e)
                  .initCause(e));
        }
      }
    }
  }

  private void testCommitContainer(ContainerId containerId,
      boolean notCommittable) throws YarnException, IOException {
    try {
      nmClient.commitLastReInitialization(containerId);
      if (notCommittable) {
        fail("Should not be able to commit..");
      }
    } catch (YarnException e) {
      // NM container will only be in SCHEDULED state, so expect the increase
      // action to fail.
      if (notCommittable) {
        Assert.assertTrue(e.getMessage().contains(
            "Nothing to Commit"));
      } else {
        if (!e.getMessage().contains(
            "can only be changed when a container is in RUNNING state")) {
          throw (AssertionError)
              (new AssertionError("Exception is not expected: " + e)
                  .initCause(e));
        }
      }
    }
  }

  private void testReInitializeContainer(ContainerId containerId,
      ContainerLaunchContext clc, boolean autoCommit)
      throws YarnException, IOException {
    try {
      nmClient.reInitializeContainer(containerId, clc, autoCommit);
    } catch (YarnException e) {
      // NM container will only be in SCHEDULED state, so expect the increase
      // action to fail.
      if (!e.getMessage().contains(
          "can only be changed when a container is in RUNNING state")) {
        throw (AssertionError)
            (new AssertionError("Exception is not expected: " + e)
                .initCause(e));
      }
    }
  }
}
