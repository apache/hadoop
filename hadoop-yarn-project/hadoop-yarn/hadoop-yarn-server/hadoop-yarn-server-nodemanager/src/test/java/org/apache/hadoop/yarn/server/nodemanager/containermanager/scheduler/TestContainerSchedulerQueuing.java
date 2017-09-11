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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.spy;

/**
 * Tests to verify that the {@link ContainerScheduler} is able to queue and
 * make room for containers.
 */
public class TestContainerSchedulerQueuing extends BaseContainerManagerTest {
  public TestContainerSchedulerQueuing() throws UnsupportedFileSystemException {
    super();
  }

  static {
    LOG = LoggerFactory.getLogger(TestContainerSchedulerQueuing.class);
  }

  private boolean delayContainers = true;

  @Override
  protected ContainerManagerImpl createContainerManager(
      DeletionService delSrvc) {
    return new ContainerManagerImpl(context, exec, delSrvc,
        nodeStatusUpdater, metrics, dirsHandler) {

      @Override
      protected UserGroupInformation getRemoteUgi() throws YarnException {
        ApplicationId appId = ApplicationId.newInstance(0, 0);
        ApplicationAttemptId appAttemptId =
            ApplicationAttemptId.newInstance(appId, 1);
        UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(appAttemptId.toString());
        ugi.addTokenIdentifier(new NMTokenIdentifier(appAttemptId, context
            .getNodeId(), user, context.getNMTokenSecretManager().getCurrentKey()
            .getKeyId()));
        return ugi;
      }

      @Override
      protected ContainersMonitor createContainersMonitor(
          ContainerExecutor exec) {
        return new ContainersMonitorImpl(exec, dispatcher, this.context) {
          // Define resources available for containers to be executed.
          @Override
          public long getPmemAllocatedForContainers() {
            return 2048 * 1024 * 1024L;
          }

          @Override
          public long getVmemAllocatedForContainers() {
            float pmemRatio = getConfig().getFloat(
                YarnConfiguration.NM_VMEM_PMEM_RATIO,
                YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
            return (long) (pmemRatio * getPmemAllocatedForContainers());
          }

          @Override
          public long getVCoresAllocatedForContainers() {
            return 4;
          }
        };
      }
    };
  }

  @Override
  protected ContainerExecutor createContainerExecutor() {
    DefaultContainerExecutor exec = new DefaultContainerExecutor() {
      @Override
      public int launchContainer(ContainerStartContext ctx)
          throws IOException, ConfigurationException {
        if (delayContainers) {
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
            // Nothing..
          }
        }
        return super.launchContainer(ctx);
      }
    };
    exec.setConf(conf);
    return spy(exec);
  }

  @Override
  public void setup() throws IOException {
    conf.setInt(
        YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH, 10);
    super.setup();
  }

  /**
   * Starting one GUARANTEED and one OPPORTUNISTIC container.
   * @throws Exception
   */
  @Test
  public void testStartMultipleContainers() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    List<StartContainerRequest> list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(1024, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.GUARANTEED)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(1), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(1024, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    BaseContainerManagerTest.waitForContainerState(containerManager,
        createContainerId(0),
        org.apache.hadoop.yarn.api.records.ContainerState.RUNNING);
    BaseContainerManagerTest.waitForContainerState(containerManager,
        createContainerId(1),
        org.apache.hadoop.yarn.api.records.ContainerState.RUNNING);

    // Ensure all containers are running.
    List<ContainerId> statList = new ArrayList<ContainerId>();
    for (int i = 0; i < 2; i++) {
      statList.add(createContainerId(i));
    }
    GetContainerStatusesRequest statRequest =
        GetContainerStatusesRequest.newInstance(statList);
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      Assert.assertEquals(
          org.apache.hadoop.yarn.api.records.ContainerState.RUNNING,
          status.getState());
    }
  }

  /**
   * Submit both a GUARANTEED and an OPPORTUNISTIC container, each of which
   * requires more resources than available at the node, and make sure they
   * are both queued.
   * @throws Exception
   */
  @Test
  public void testQueueMultipleContainers() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    List<StartContainerRequest> list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(3072, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.GUARANTEED)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(1), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(3072, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    Thread.sleep(5000);

    // Ensure both containers are queued.
    List<ContainerId> statList = new ArrayList<ContainerId>();
    for (int i = 0; i < 2; i++) {
      statList.add(createContainerId(i));
    }
    GetContainerStatusesRequest statRequest =
        GetContainerStatusesRequest.newInstance(statList);
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      Assert.assertEquals(
          org.apache.hadoop.yarn.api.records.ContainerState.SCHEDULED,
          status.getState());
    }

    ContainerScheduler containerScheduler =
        containerManager.getContainerScheduler();
    // Ensure both containers are properly queued.
    Assert.assertEquals(2, containerScheduler.getNumQueuedContainers());
    Assert.assertEquals(1,
        containerScheduler.getNumQueuedGuaranteedContainers());
    Assert.assertEquals(1,
        containerScheduler.getNumQueuedOpportunisticContainers());
  }

  /**
   * Starts one OPPORTUNISTIC container that takes up the whole node's
   * resources, and submit two more that will be queued.
   * @throws Exception
   */
  @Test
  public void testStartAndQueueMultipleContainers() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    List<StartContainerRequest> list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(2048, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(1), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(1024, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(2), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(1024, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    Thread.sleep(5000);

    // Ensure first container is running and others are queued.
    List<ContainerId> statList = new ArrayList<ContainerId>();
    for (int i = 0; i < 3; i++) {
      statList.add(createContainerId(i));
    }
    GetContainerStatusesRequest statRequest = GetContainerStatusesRequest
        .newInstance(Arrays.asList(createContainerId(0)));
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      if (status.getContainerId().equals(createContainerId(0))) {
        Assert.assertEquals(
            org.apache.hadoop.yarn.api.records.ContainerState.RUNNING,
            status.getState());
      } else {
        Assert.assertEquals(
            org.apache.hadoop.yarn.api.records.ContainerState.SCHEDULED,
            status.getState());
      }
    }

    ContainerScheduler containerScheduler =
        containerManager.getContainerScheduler();
    // Ensure two containers are properly queued.
    Assert.assertEquals(2, containerScheduler.getNumQueuedContainers());
    Assert.assertEquals(0,
        containerScheduler.getNumQueuedGuaranteedContainers());
    Assert.assertEquals(2,
        containerScheduler.getNumQueuedOpportunisticContainers());
  }

  /**
   * Starts one GUARANTEED container that takes us the whole node's resources.
   * and submit more OPPORTUNISTIC containers than the opportunistic container
   * queue can hold. OPPORTUNISTIC containers that cannot be queue should be
   * killed.
   * @throws Exception
   */
  @Test
  public void testStartOpportunistcsWhenOppQueueIsFull() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    List<StartContainerRequest> list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(2048, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.GUARANTEED)));

    final int maxOppQueueLength = conf.getInt(
        YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH,
        YarnConfiguration.DEFAULT_NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH);
    for (int i = 1; i < maxOppQueueLength + 2; i++) {
      list.add(StartContainerRequest.newInstance(
          containerLaunchContext,
          createContainerToken(createContainerId(i), DUMMY_RM_IDENTIFIER,
              context.getNodeId(),
              user, BuilderUtils.newResource(2048, 1),
              context.getContainerTokenSecretManager(), null,
              ExecutionType.OPPORTUNISTIC)));
    }

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    BaseContainerManagerTest.waitForNMContainerState(containerManager,
        createContainerId(0), ContainerState.RUNNING, 40);
    BaseContainerManagerTest.waitForNMContainerState(containerManager,
        createContainerId(maxOppQueueLength + 1), ContainerState.DONE,
        40);
    Thread.sleep(5000);

    // Get container statuses. Container 0 should be running and container
    // 1 to maxOppQueueLength should be queued and the last container should
    // be killed
    List<ContainerId> statList = new ArrayList<>();
    for (int i = 0; i < maxOppQueueLength + 2; i++) {
      statList.add(createContainerId(i));
    }
    GetContainerStatusesRequest statRequest =
        GetContainerStatusesRequest.newInstance(statList);
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      if (status.getContainerId().equals(createContainerId(0))) {
        Assert.assertEquals(
            org.apache.hadoop.yarn.api.records.ContainerState.RUNNING,
            status.getState());
      } else if (status.getContainerId().equals(createContainerId(
          maxOppQueueLength + 1))) {
        Assert.assertTrue(status.getDiagnostics().contains(
            "Opportunistic container queue is full"));
      } else {
        Assert.assertEquals(
            org.apache.hadoop.yarn.api.records.ContainerState.SCHEDULED,
            status.getState());
      }
      System.out.println("\nStatus : [" + status + "]\n");
    }

    ContainerScheduler containerScheduler =
        containerManager.getContainerScheduler();
    Assert.assertEquals(maxOppQueueLength,
        containerScheduler.getNumQueuedContainers());
    Assert.assertEquals(0,
        containerScheduler.getNumQueuedGuaranteedContainers());
    Assert.assertEquals(maxOppQueueLength,
        containerScheduler.getNumQueuedOpportunisticContainers());
  }

  /**
   * Submit two OPPORTUNISTIC and one GUARANTEED containers. The resources
   * requests by each container as such that only one can run in parallel.
   * Thus, the OPPORTUNISTIC container that started running, will be
   * killed for the GUARANTEED container to start.
   * Once the GUARANTEED container finishes its execution, the remaining
   * OPPORTUNISTIC container will be executed.
   * @throws Exception
   */
  @Test
  public void testKillOpportunisticForGuaranteedContainer() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    List<StartContainerRequest> list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(2048, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(1), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(2048, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(2), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(2048, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.GUARANTEED)));

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    BaseContainerManagerTest.waitForNMContainerState(containerManager,
        createContainerId(0), ContainerState.DONE, 40);
    Thread.sleep(5000);

    // Get container statuses. Container 0 should be killed, container 1
    // should be queued and container 2 should be running.
    List<ContainerId> statList = new ArrayList<ContainerId>();
    for (int i = 0; i < 3; i++) {
      statList.add(createContainerId(i));
    }
    GetContainerStatusesRequest statRequest =
        GetContainerStatusesRequest.newInstance(statList);
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      if (status.getContainerId().equals(createContainerId(0))) {
        Assert.assertTrue(status.getDiagnostics().contains(
            "Container Killed to make room for Guaranteed Container"));
      } else if (status.getContainerId().equals(createContainerId(1))) {
        Assert.assertEquals(
            org.apache.hadoop.yarn.api.records.ContainerState.SCHEDULED,
            status.getState());
      } else if (status.getContainerId().equals(createContainerId(2))) {
        Assert.assertEquals(
            org.apache.hadoop.yarn.api.records.ContainerState.RUNNING,
            status.getState());
      }
      System.out.println("\nStatus : [" + status + "]\n");
    }

    // Make sure the remaining OPPORTUNISTIC container starts its execution.
    BaseContainerManagerTest.waitForNMContainerState(containerManager,
        createContainerId(2), ContainerState.DONE, 40);
    Thread.sleep(5000);
    statRequest = GetContainerStatusesRequest.newInstance(Arrays.asList(
        createContainerId(1)));
    ContainerStatus contStatus1 = containerManager.getContainerStatuses(
        statRequest).getContainerStatuses().get(0);
    Assert.assertEquals(
        org.apache.hadoop.yarn.api.records.ContainerState.RUNNING,
        contStatus1.getState());
  }

  /**
   * 1. Submit a long running GUARANTEED container to hog all NM resources.
   * 2. Submit 6 OPPORTUNISTIC containers, all of which will be queued.
   * 3. Update the Queue Limit to 2.
   * 4. Ensure only 2 containers remain in the Queue, and 4 are de-Queued.
   * @throws Exception
   */
  @Test
  public void testQueueShedding() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    containerLaunchContext.setCommands(Arrays.asList("sleep 100"));

    List<StartContainerRequest> list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(2048, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.GUARANTEED)));

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(1), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(2), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(3), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(4), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(5), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(6), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));

    allRequests = StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    ContainerScheduler containerScheduler =
        containerManager.getContainerScheduler();
    // Ensure all containers are properly queued.
    int numTries = 30;
    while ((containerScheduler.getNumQueuedContainers() < 6) &&
        (numTries-- > 0)) {
      Thread.sleep(100);
    }
    Assert.assertEquals(6, containerScheduler.getNumQueuedContainers());

    ContainerQueuingLimit containerQueuingLimit = ContainerQueuingLimit
        .newInstance();
    containerQueuingLimit.setMaxQueueLength(2);
    containerScheduler.updateQueuingLimit(containerQueuingLimit);
    numTries = 30;
    while ((containerScheduler.getNumQueuedContainers() > 2) &&
        (numTries-- > 0)) {
      Thread.sleep(100);
    }
    Assert.assertEquals(2, containerScheduler.getNumQueuedContainers());

    List<ContainerId> statList = new ArrayList<ContainerId>();
    for (int i = 1; i < 7; i++) {
      statList.add(createContainerId(i));
    }
    GetContainerStatusesRequest statRequest =
        GetContainerStatusesRequest.newInstance(statList);
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();

    int deQueuedContainers = 0;
    int numQueuedOppContainers = 0;
    for (ContainerStatus status : containerStatuses) {
      if (status.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
        if (status.getDiagnostics().contains(
            "Container De-queued to meet NM queuing limits")) {
          deQueuedContainers++;
        }
        if (status.getState() ==
            org.apache.hadoop.yarn.api.records.ContainerState.SCHEDULED) {
          numQueuedOppContainers++;
        }
      }
    }
    Assert.assertEquals(4, deQueuedContainers);
    Assert.assertEquals(2, numQueuedOppContainers);
  }

  /**
   * 1. Submit a long running GUARANTEED container to hog all NM resources.
   * 2. Submit 2 OPPORTUNISTIC containers, both of which will be queued.
   * 3. Send Stop Container to one of the queued containers.
   * 4. Ensure container is removed from the queue.
   * @throws Exception
   */
  @Test
  public void testContainerDeQueuedAfterAMKill() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    containerLaunchContext.setCommands(Arrays.asList("sleep 100"));

    List<StartContainerRequest> list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(2048, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.GUARANTEED)));

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(1), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(2), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));

    allRequests = StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    ContainerScheduler containerScheduler =
        containerManager.getContainerScheduler();
    // Ensure both containers are properly queued.
    int numTries = 30;
    while ((containerScheduler.getNumQueuedContainers() < 2) &&
        (numTries-- > 0)) {
      Thread.sleep(100);
    }
    Assert.assertEquals(2, containerScheduler.getNumQueuedContainers());

    containerManager.stopContainers(
        StopContainersRequest.newInstance(Arrays.asList(createContainerId(2))));

    numTries = 30;
    while ((containerScheduler.getNumQueuedContainers() > 1) &&
        (numTries-- > 0)) {
      Thread.sleep(100);
    }
    Assert.assertEquals(1, containerScheduler.getNumQueuedContainers());
  }

  /**
   * Submit three OPPORTUNISTIC containers that can run concurrently, and one
   * GUARANTEED that needs to kill two of the OPPORTUNISTIC for it to run.
   * @throws Exception
   */
  @Test
  public void testKillMultipleOpportunisticContainers() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    List<StartContainerRequest> list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(1), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(2), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(3), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(1500, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.GUARANTEED)));

    allRequests = StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    BaseContainerManagerTest.waitForNMContainerState(
        containerManager, createContainerId(0),
            Arrays.asList(ContainerState.DONE,
                ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL), 40);
    Thread.sleep(5000);

    // Get container statuses. Container 0 should be killed, container 1
    // should be queued and container 2 should be running.
    int killedContainers = 0;
    List<ContainerId> statList = new ArrayList<ContainerId>();
    for (int i = 0; i < 4; i++) {
      statList.add(createContainerId(i));
    }
    GetContainerStatusesRequest statRequest =
        GetContainerStatusesRequest.newInstance(statList);
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      if (status.getDiagnostics().contains(
          "Container Killed to make room for Guaranteed Container")) {
        killedContainers++;
      }
      System.out.println("\nStatus : [" + status + "]\n");
    }

    Assert.assertEquals(2, killedContainers);
  }

  /**
   * Submit four OPPORTUNISTIC containers that can run concurrently, and then
   * two GUARANTEED that needs to kill Exactly two of the OPPORTUNISTIC for
   * it to run. Make sure only 2 are killed.
   * @throws Exception
   */
  @Test
  public void testKillOnlyRequiredOpportunisticContainers() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    List<StartContainerRequest> list = new ArrayList<>();
    // Fill NM with Opportunistic containers
    for (int i = 0; i < 4; i++) {
      list.add(StartContainerRequest.newInstance(
          containerLaunchContext,
          createContainerToken(createContainerId(i), DUMMY_RM_IDENTIFIER,
              context.getNodeId(),
              user, BuilderUtils.newResource(512, 1),
              context.getContainerTokenSecretManager(), null,
              ExecutionType.OPPORTUNISTIC)));
    }

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    list = new ArrayList<>();
    // Now ask for two Guaranteed containers
    for (int i = 4; i < 6; i++) {
      list.add(StartContainerRequest.newInstance(
          containerLaunchContext,
          createContainerToken(createContainerId(i), DUMMY_RM_IDENTIFIER,
              context.getNodeId(),
              user, BuilderUtils.newResource(512, 1),
              context.getContainerTokenSecretManager(), null,
              ExecutionType.GUARANTEED)));
    }

    allRequests = StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    BaseContainerManagerTest.waitForNMContainerState(containerManager,
        createContainerId(0), ContainerState.DONE, 40);
    Thread.sleep(5000);

    // Get container statuses. Container 0 should be killed, container 1
    // should be queued and container 2 should be running.
    int killedContainers = 0;
    List<ContainerId> statList = new ArrayList<ContainerId>();
    for (int i = 0; i < 6; i++) {
      statList.add(createContainerId(i));
    }
    GetContainerStatusesRequest statRequest =
        GetContainerStatusesRequest.newInstance(statList);
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      if (status.getDiagnostics().contains(
          "Container Killed to make room for Guaranteed Container")) {
        killedContainers++;
      }
      System.out.println("\nStatus : [" + status + "]\n");
    }

    Assert.assertEquals(2, killedContainers);
  }

  /**
   * Start running one GUARANTEED container and queue two OPPORTUNISTIC ones.
   * Try killing one of the two queued containers.
   * @throws Exception
   */
  @Test
  public void testStopQueuedContainer() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    List<StartContainerRequest> list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(2048, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.GUARANTEED)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(1), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(2), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(512, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    Thread.sleep(2000);

    // Assert there is initially one container running and two queued.
    int runningContainersNo = 0;
    int queuedContainersNo = 0;
    List<ContainerId> statList = new ArrayList<ContainerId>();
    for (int i = 0; i < 3; i++) {
      statList.add(createContainerId(i));
    }
    GetContainerStatusesRequest statRequest = GetContainerStatusesRequest
        .newInstance(statList);
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      if (status.getState() ==
          org.apache.hadoop.yarn.api.records.ContainerState.RUNNING) {
        runningContainersNo++;
      } else if (status.getState() ==
          org.apache.hadoop.yarn.api.records.ContainerState.SCHEDULED) {
        queuedContainersNo++;
      }
      System.out.println("\nStatus : [" + status + "]\n");
    }

    Assert.assertEquals(1, runningContainersNo);
    Assert.assertEquals(2, queuedContainersNo);

    // Stop one of the two queued containers.
    StopContainersRequest stopRequest = StopContainersRequest.
        newInstance(Arrays.asList(createContainerId(1)));
    containerManager.stopContainers(stopRequest);

    Thread.sleep(2000);

    // Assert queued container got properly stopped.
    statList.clear();
    for (int i = 0; i < 3; i++) {
      statList.add(createContainerId(i));
    }

    statRequest = GetContainerStatusesRequest.newInstance(statList);
    HashMap<org.apache.hadoop.yarn.api.records.ContainerState, ContainerStatus>
        map = new HashMap<>();
    for (int i=0; i < 10; i++) {
      containerStatuses = containerManager.getContainerStatuses(statRequest)
          .getContainerStatuses();
      for (ContainerStatus status : containerStatuses) {
        System.out.println("\nStatus : [" + status + "]\n");
        map.put(status.getState(), status);
        if (map.containsKey(
                org.apache.hadoop.yarn.api.records.ContainerState.RUNNING) &&
            map.containsKey(
                org.apache.hadoop.yarn.api.records.ContainerState.SCHEDULED) &&
            map.containsKey(
                org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE)) {
          break;
        }
        Thread.sleep(1000);
      }
    }
    Assert.assertEquals(createContainerId(0),
        map.get(org.apache.hadoop.yarn.api.records.ContainerState.RUNNING)
            .getContainerId());
    Assert.assertEquals(createContainerId(1),
        map.get(org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE)
            .getContainerId());
    Assert.assertEquals(createContainerId(2),
        map.get(org.apache.hadoop.yarn.api.records.ContainerState.SCHEDULED)
            .getContainerId());
  }

  /**
   * Starts one OPPORTUNISTIC container that takes up the whole node's
   * resources, and submit one more that will be queued. Now promote the
   * queued OPPORTUNISTIC container, which should kill the current running
   * OPPORTUNISTIC container to make room for the promoted request.
   * @throws Exception
   */
  @Test
  public void testPromotionOfOpportunisticContainers() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    List<StartContainerRequest> list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(2048, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(1), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(1024, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    Thread.sleep(5000);

    // Ensure first container is running and others are queued.
    List<ContainerId> statList = new ArrayList<ContainerId>();
    for (int i = 0; i < 3; i++) {
      statList.add(createContainerId(i));
    }
    GetContainerStatusesRequest statRequest = GetContainerStatusesRequest
        .newInstance(Arrays.asList(createContainerId(0)));
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      if (status.getContainerId().equals(createContainerId(0))) {
        Assert.assertEquals(
            org.apache.hadoop.yarn.api.records.ContainerState.RUNNING,
            status.getState());
      } else {
        Assert.assertEquals(
            org.apache.hadoop.yarn.api.records.ContainerState.SCHEDULED,
            status.getState());
      }
    }

    ContainerScheduler containerScheduler =
        containerManager.getContainerScheduler();
    // Ensure two containers are properly queued.
    Assert.assertEquals(1, containerScheduler.getNumQueuedContainers());
    Assert.assertEquals(0,
        containerScheduler.getNumQueuedGuaranteedContainers());
    Assert.assertEquals(1,
        containerScheduler.getNumQueuedOpportunisticContainers());

    // Promote Queued Opportunistic Container
    Token updateToken =
        createContainerToken(createContainerId(1), 1, DUMMY_RM_IDENTIFIER,
            context.getNodeId(), user, BuilderUtils.newResource(1024, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.GUARANTEED);
    List<Token> updateTokens = new ArrayList<Token>();
    updateTokens.add(updateToken);
    ContainerUpdateRequest updateRequest =
        ContainerUpdateRequest.newInstance(updateTokens);
    ContainerUpdateResponse updateResponse =
        containerManager.updateContainer(updateRequest);

    Assert.assertEquals(1,
        updateResponse.getSuccessfullyUpdatedContainers().size());
    Assert.assertEquals(0, updateResponse.getFailedRequests().size());

    waitForContainerState(containerManager, createContainerId(0),
        org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE);

    waitForContainerState(containerManager, createContainerId(1),
        org.apache.hadoop.yarn.api.records.ContainerState.RUNNING);

    // Ensure no containers are queued.
    Assert.assertEquals(0, containerScheduler.getNumQueuedContainers());
  }
}
