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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerSubState;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerSchedulerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * Tests the behavior of {@link ContainerScheduler} when its queueing policy
 * is set to {@link OpportunisticContainersQueuePolicy#BY_RESOURCES}
 * such that the NM only queues containers if there's enough resources
 * on the node to start all queued containers.
 */
public class TestContainerSchedulerOppContainersByResources
    extends BaseContainerSchedulerTest {
  public TestContainerSchedulerOppContainersByResources()
      throws UnsupportedFileSystemException {
  }

  @Override
  public void setup() throws IOException {
    conf.set(YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_QUEUE_POLICY,
        OpportunisticContainersQueuePolicy.BY_RESOURCES.name());
    super.setup();
    containerManager.start();
  }

  /**
   * Checks if a container is in a running or successfully run state.
   * @param containerStatus the container status
   * @return true if the container is running or completed
   * with a successful state, false if the container has not started or failed
   */
  private static boolean isContainerInSuccessfulState(
      final ContainerStatus containerStatus) {
    final org.apache.hadoop.yarn.api.records.ContainerState state =
        containerStatus.getState();
    final ContainerSubState subState = containerStatus.getContainerSubState();
    switch (subState) {
    case RUNNING:
    case COMPLETING:
      return true;
    case DONE:
      // If the state is not COMPLETE, then the
      // container is a failed container
      return state ==
          org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE;
    default:
      return false;
    }
  }

  private void verifyRunAndKilledContainers(
      final List<ContainerId> statList,
      final int numExpectedContainers, final Set<ContainerId> runContainers,
      final Set<ContainerId> killedContainers)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> {
          GetContainerStatusesRequest statRequest =
              GetContainerStatusesRequest.newInstance(statList);
          final List<ContainerStatus> containerStatuses;
          try {
            containerStatuses = containerManager
                .getContainerStatuses(statRequest).getContainerStatuses();
          } catch (final Exception e) {
            return false;
          }

          if (numExpectedContainers != containerStatuses.size()) {
            return false;
          }

          for (final ContainerStatus status : containerStatuses) {
            if (runContainers.contains(status.getContainerId())) {
              if (!isContainerInSuccessfulState(status)) {
                return false;
              }
            } else if (killedContainers.contains(status.getContainerId())) {
              if (!status.getDiagnostics()
                  .contains("Opportunistic container queue is full")) {
                return false;
              }
            } else {
              return false;
            }
          }

          return true;
        }, 1000, 10000);
  }

  /**
   * Verifies that nothing is queued at the container scheduler.
   */
  private void verifyNothingQueued() {
    // Check that nothing is queued
    ContainerScheduler containerScheduler =
        containerManager.getContainerScheduler();
    Assert.assertEquals(0,
        containerScheduler.getNumQueuedContainers());
    Assert.assertEquals(0,
        containerScheduler.getNumQueuedGuaranteedContainers());
    Assert.assertEquals(0,
        containerScheduler.getNumQueuedOpportunisticContainers());
    Assert.assertEquals(0,
        metrics.getQueuedOpportunisticContainers());
    Assert.assertEquals(0, metrics.getQueuedGuaranteedContainers());
  }

  /**
   * Tests that newly arrived containers after the resources are filled up
   * get killed and never gets run.
   */
  @Test
  public void testKillOpportunisticWhenNoResourcesAvailable() throws Exception {
    List<StartContainerRequest> startContainerRequests = new ArrayList<>();

    // GContainer that takes up the whole node
    startContainerRequests.add(StartContainerRequest.newInstance(
        recordFactory.newRecordInstance(ContainerLaunchContext.class),
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, Resources.createResource(2048),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.GUARANTEED)));

    // OContainer that should be killed
    startContainerRequests.add(StartContainerRequest.newInstance(
        recordFactory.newRecordInstance(ContainerLaunchContext.class),
        createContainerToken(createContainerId(1), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, Resources.createResource(2048),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(startContainerRequests);
    containerManager.startContainers(allRequests);

    BaseContainerManagerTest.waitForNMContainerState(containerManager,
        createContainerId(0), ContainerState.RUNNING, 40);

    // Wait for the OContainer to get killed
    BaseContainerManagerTest.waitForNMContainerState(containerManager,
        createContainerId(1), ContainerState.DONE, 40);

    // Get container statuses.
    // Container 0 should be running and container 1 should be killed
    List<ContainerId> statList = ImmutableList.of(createContainerId(0),
        createContainerId(1));

    verifyRunAndKilledContainers(
        statList, 2,
        Collections.singleton(createContainerId(0)),
        Collections.singleton(createContainerId(1))
    );

    verifyNothingQueued();
  }

  /**
   * Tests that newly arrived containers after the resources are filled up
   * get killed and never gets run.
   * This scenario is more granular and runs more small container compared to
   * {@link #testKillOpportunisticWhenNoResourcesAvailable()}.
   */
  @Test
  public void testOpportunisticRunsWhenResourcesAvailable() throws Exception {
    List<StartContainerRequest> startContainerRequests = new ArrayList<>();
    final int numContainers = 8;
    final int numContainersQueued = 4;
    final Set<ContainerId> runContainers = new HashSet<>();
    final Set<ContainerId> killedContainers = new HashSet<>();

    for (int i = 0; i < numContainers; i++) {
      // OContainers that should be run
      startContainerRequests.add(StartContainerRequest.newInstance(
          recordFactory.newRecordInstance(ContainerLaunchContext.class),
          createContainerToken(createContainerId(i), DUMMY_RM_IDENTIFIER,
              context.getNodeId(),
              user, Resources.createResource(512),
              context.getContainerTokenSecretManager(), null,
              ExecutionType.OPPORTUNISTIC)));
    }

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(startContainerRequests);
    containerManager.startContainers(allRequests);

    // Wait for containers to start
    for (int i = 0; i < numContainersQueued; i++) {
      final ContainerId containerId = createContainerId(i);
      BaseContainerManagerTest
          .waitForNMContainerState(containerManager, containerId,
              ContainerState.RUNNING, 40);
      runContainers.add(containerId);
    }

    // Wait for containers to be killed
    for (int i = numContainersQueued; i < numContainers; i++) {
      final ContainerId containerId = createContainerId(i);
      BaseContainerManagerTest
          .waitForNMContainerState(containerManager, createContainerId(i),
              ContainerState.DONE, 40);
      killedContainers.add(containerId);
    }

    Thread.sleep(5000);

    // Get container statuses.
    List<ContainerId> statList = new ArrayList<>();
    for (int i = 0; i < numContainers; i++) {
      statList.add(createContainerId(i));
    }


    verifyRunAndKilledContainers(
        statList, numContainers, runContainers, killedContainers);

    verifyNothingQueued();
  }
}
