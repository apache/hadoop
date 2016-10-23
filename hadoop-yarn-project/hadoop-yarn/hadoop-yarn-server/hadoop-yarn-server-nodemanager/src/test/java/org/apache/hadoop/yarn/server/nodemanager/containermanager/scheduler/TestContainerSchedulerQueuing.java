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

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
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

import static org.mockito.Mockito.spy;

public class TestContainerSchedulerQueuing extends BaseContainerManagerTest {
  public TestContainerSchedulerQueuing() throws UnsupportedFileSystemException {
    super();
  }

  static {
    LOG = LogFactory.getLog(TestContainerSchedulerQueuing.class);
  }

  boolean delayContainers = true;

  @Override
  protected ContainerManagerImpl createContainerManager(
      DeletionService delSrvc) {
    return new ContainerManagerImpl(context, exec, delSrvc,
        nodeStatusUpdater, metrics, dirsHandler) {
      @Override
      public void
      setBlockNewContainerRequests(boolean blockNewContainerRequests) {
        // do nothing
      }

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
      public int launchContainer(ContainerStartContext ctx) throws IOException {
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

    allRequests = StartContainersRequest.newInstance(list);
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

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
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
}
