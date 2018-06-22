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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerSubState;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Test ContainerScheduler behaviors when NM overallocation is turned on.
 */
public class TestContainerSchedulerWithOverAllocation
    extends BaseContainerManagerTest {
  private static final int NM_OPPORTUNISTIC_QUEUE_LIMIT = 3;
  private static final int NM_CONTAINERS_VCORES = 4;
  private static final int NM_CONTAINERS_MEMORY_MB = 2048;

  static {
    LOG = LoggerFactory.getLogger(TestContainerSchedulerQueuing.class);
  }

  public TestContainerSchedulerWithOverAllocation()
      throws UnsupportedFileSystemException {
  }

  @Override
  protected ContainerExecutor createContainerExecutor() {
    DefaultContainerExecutor exec =
        new LongRunningContainerSimulatingContainerExecutor();
    exec.setConf(conf);
    return exec;
  }

  @Override
  protected ContainerManagerImpl createContainerManager(
      DeletionService delSrvc) {
    return new LongRunningContainerSimulatingContainersManager(
        context, exec, delSrvc, nodeStatusUpdater, metrics, dirsHandler, user);
  }

  @Override
  public void setup() throws IOException {
    conf.setInt(
        YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH,
        NM_OPPORTUNISTIC_QUEUE_LIMIT);
    conf.setFloat(
        YarnConfiguration.NM_OVERALLOCATION_CPU_UTILIZATION_THRESHOLD,
        0.75f);
    conf.setFloat(
        YarnConfiguration.NM_OVERALLOCATION_MEMORY_UTILIZATION_THRESHOLD,
        0.75f);
    // disable the monitor thread in ContainersMonitor to allow control over
    // when opportunistic containers are launched with over-allocation
    conf.setBoolean(YarnConfiguration.NM_CONTAINER_MONITOR_ENABLED, false);
    super.setup();
  }

  /**
   * Start one GUARANTEED and one OPPORTUNISTIC container, which in aggregate do
   * not exceed the capacity of the node. Both containers are expected to start
   * running immediately.
   */
  @Test
  public void testStartMultipleContainersWithoutOverallocation()
      throws Exception {
    containerManager.start();

    StartContainersRequest allRequests = StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() { {
          add(createStartContainerRequest(0,
              BuilderUtils.newResource(1024, 1), false));
          add(createStartContainerRequest(1,
              BuilderUtils.newResource(1024, 1), true));
        } }
    );
    containerManager.startContainers(allRequests);

    BaseContainerManagerTest.waitForContainerSubState(
        containerManager, createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(
        containerManager, createContainerId(1), ContainerSubState.RUNNING);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
      }
    });
  }

  /**
   * Start one GUARANTEED and one OPPORTUNISTIC containers whose utilization
   * is very low relative to their resource request, resulting in a low node
   * utilization. Then start another OPPORTUNISTIC containers which requests
   * more than what's left unallocated on the node. Due to overallocation
   * being turned on and node utilization being low, the second OPPORTUNISTIC
   * container is also expected to be launched immediately.
   */
  @Test
  public void testStartOppContainersWithPartialOverallocationLowUtilization()
      throws Exception {
    containerManager.start();

    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(1024, 1), true));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(824, 1), true));
          }
        }
    ));
    BaseContainerManagerTest.waitForContainerSubState(
        containerManager, createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(
        containerManager, createContainerId(1), ContainerSubState.RUNNING);

    // the current containers utilization is low
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(512, 0, 1.0f/8));

    // start a container that requests more than what's left unallocated
    // 512 + 1024 + 824 > 2048
    containerManager.startContainers(StartContainersRequest.newInstance(
        Collections.singletonList(
            createStartContainerRequest(2,
                BuilderUtils.newResource(512, 1), false))
    ));

    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .drainAsyncEvents();

    // this container is not expected to be started immediately because
    // opportunistic containers cannot be started if the node would be
    // over-allocated
    BaseContainerManagerTest.waitForContainerSubState(
        containerManager, createContainerId(2), ContainerSubState.SCHEDULED);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.SCHEDULED);
      }
    });

    // try to start opportunistic containers out of band.
    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .startContainersOutOfBandUponLowUtilization();

    // this container is expected to be started immediately because there
    // are (memory: 1024, vcore: 0.625) available based on over-allocation
    BaseContainerManagerTest.waitForContainerSubState(
        containerManager, createContainerId(2), ContainerSubState.RUNNING);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.RUNNING);
      }
    });
  }

  /**
   * Start one GUARANTEED and one OPPORTUNISTIC containers which utilizes most
   * of the resources they requested, resulting in a high node utilization.
   * Then start another OPPORTUNISTIC containers which requests more than what's
   * left unallocated on the node. Because of the high resource utilization on
   * the node, the projected utilization, if we were to start the second
   * OPPORTUNISTIC container immediately, will go over the NM overallocation
   * threshold, so the second OPPORTUNISTIC container is expected to be queued.
   */
  @Test
  public void testQueueOppContainerWithPartialOverallocationHighUtilization()
      throws Exception {
    containerManager.start();

    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(1024, 1), true));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(824, 1), true));
          }
        }
    ));
    BaseContainerManagerTest.waitForContainerSubState(
        containerManager, createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(
        containerManager, createContainerId(1), ContainerSubState.RUNNING);

    // the containers utilization is high
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(1500, 0, 1.0f/8));

    // start a container that requests more than what's left unallocated
    // 512 + 1024 + 824 > 2048
    containerManager.startContainers(StartContainersRequest.newInstance(
        Collections.singletonList(
            createStartContainerRequest(2,
                BuilderUtils.newResource(512, 1), false))
    ));

    // try to start opportunistic containers out of band because they can
    // not be launched at container scheduler event if the node would be
    // over-allocated.
    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .startContainersOutOfBandUponLowUtilization();

    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .drainAsyncEvents();

    // this container will not start immediately because there is not
    // enough resource available at the moment either in terms of
    // resources unallocated or in terms of the actual utilization
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.SCHEDULED);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.SCHEDULED);
      }
    });
  }

  /**
   * Start two GUARANTEED containers which in aggregate takes up the whole node
   * capacity, yet whose utilization is low relative to their resource request,
   * resulting in a low node resource utilization. Then try to start another
   * OPPORTUNISTIC containers. Because the resource utilization across the node
   * is low and overallocation being turned on, the OPPORTUNISTIC container is
   * expected to be launched immediately even though there is no resources left
   * unallocated.
   */
  @Test
  public void testStartOppContainersWithOverallocationLowUtilization()
      throws Exception {
    containerManager.start();

    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(1024, 1), true));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(1024, 1), true));
          }
        }
    ));
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);

    // the current containers utilization is low
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(800, 0, 1.0f/8));

    // start a container when there is no resources left unallocated.
    containerManager.startContainers(StartContainersRequest.newInstance(
        Collections.singletonList(
            createStartContainerRequest(2,
                BuilderUtils.newResource(512, 1), false))
    ));

    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .drainAsyncEvents();

    // this container is not expected to be started immediately because
    // opportunistic containers cannot be started if the node would be
    // over-allocated
    BaseContainerManagerTest.waitForContainerSubState(
        containerManager, createContainerId(2), ContainerSubState.SCHEDULED);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.SCHEDULED);
      }
    });

    // try to start opportunistic containers out of band.
    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .startContainersOutOfBandUponLowUtilization();

    // this container is expected to be started because there is resources
    // available because the actual utilization is very low
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.RUNNING);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.RUNNING);
      }
    });
  }


  /**
   * Start two GUARANTEED containers which in aggregate take up the whole node
   * capacity and fully utilize the resources they requested. Then try to start
   * four OPPORTUNISTIC containers of which three will be queued and one will be
   * killed because of the max queue length is 3.
   */
  @Test
  public void testQueueOppContainersWithFullUtilization() throws Exception {
    containerManager.start();

    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(1024, 1), true));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(1024, 1), true));
          }
        }
    ));
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);

    // the containers are fully utilizing their resources
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(2048, 0, 1.0f/8));

    // start more OPPORTUNISTIC containers than what the OPPORTUNISTIC container
    // queue can hold when there is no unallocated resource left.
    List<StartContainerRequest> moreContainerRequests =
        new ArrayList<>(NM_OPPORTUNISTIC_QUEUE_LIMIT + 1);
    for (int a = 0; a < NM_OPPORTUNISTIC_QUEUE_LIMIT + 1; a++) {
      moreContainerRequests.add(
          createStartContainerRequest(2 + a,
              BuilderUtils.newResource(512, 1), false));
    }
    containerManager.startContainers(
        StartContainersRequest.newInstance(moreContainerRequests));

    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .drainAsyncEvents();

    // All OPPORTUNISTIC containers but the last one should be queued.
    // The last OPPORTUNISTIC container to launch should be killed.
    BaseContainerManagerTest.waitForContainerState(
        containerManager, createContainerId(NM_OPPORTUNISTIC_QUEUE_LIMIT + 2),
        ContainerState.COMPLETE);

    HashMap<ContainerId, ContainerSubState> expectedContainerStatus =
        new HashMap<>();
    expectedContainerStatus.put(
        createContainerId(0), ContainerSubState.RUNNING);
    expectedContainerStatus.put(
        createContainerId(1), ContainerSubState.RUNNING);
    expectedContainerStatus.put(
        createContainerId(NM_OPPORTUNISTIC_QUEUE_LIMIT),
        ContainerSubState.DONE);
    for (int i = 0; i < NM_OPPORTUNISTIC_QUEUE_LIMIT; i++) {
      expectedContainerStatus.put(
          createContainerId(i + 2), ContainerSubState.SCHEDULED);
    }
    verifyContainerStatuses(expectedContainerStatus);
  }

  /**
   * Start two GUARANTEED containers that together does not take up the
   * whole node. Then try to start one OPPORTUNISTIC container that will
   * fit into the remaining unallocated space on the node.
   * The OPPORTUNISTIC container is expected to start even though the
   * current node utilization is above the NM overallocation threshold,
   * because it's always safe to launch containers as long as the node
   * has not been fully allocated.
   */
  @Test
  public void testStartOppContainerWithHighUtilizationNoOverallocation()
      throws Exception {
    containerManager.start();

    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(1200, 1), true));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(400, 1), true));
          }
        }
    ));
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);

    //  containers utilization is above the over-allocation threshold
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(1600, 0, 1.0f/2));

    // start a container that can just fit in the remaining unallocated space
    containerManager.startContainers(StartContainersRequest.newInstance(
        Collections.singletonList(
            createStartContainerRequest(2,
                BuilderUtils.newResource(400, 1), false))
    ));

    // the OPPORTUNISTIC container can be safely launched even though
    // the container utilization is above the NM overallocation threshold
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.RUNNING);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.RUNNING);
      }
    });
  }

  /**
   * Start two OPPORTUNISTIC containers first whose utilization is low relative
   * to the resources they requested, resulting in a low node utilization. Then
   * try to start a GUARANTEED container which requests more than what's left
   * unallocated on the node. Because the node utilization is low and NM
   * overallocation is turned on, the GUARANTEED container is expected to be
   * started immediately without killing any running OPPORTUNISTIC containers.
   */
  @Test
  public void testKillNoOppContainersWithPartialOverallocationLowUtilization()
      throws Exception {
    containerManager.start();

    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(1024, 1), false));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(824, 1), false));
          }
        }
    ));
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);

    // containers utilization is low
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(512, 0, 1.0f/8));

    // start a GUARANTEED container that requests more than what's left
    // unallocated on the node: (512  + 1024 + 824) > 2048
    containerManager.startContainers(StartContainersRequest.newInstance(
        Collections.singletonList(
            createStartContainerRequest(2,
                BuilderUtils.newResource(512, 1), true))
    ));

    // the GUARANTEED container is expected be launched immediately without
    // killing any OPPORTUNISTIC containers.
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.RUNNING);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.RUNNING);
      }
    });
  }

  /**
   * Start two OPPORTUNISTIC containers whose utilization will be high relative
   * to the resources they requested, resulting in a high node utilization.
   * Then try to start a GUARANTEED container which requests more than what's
   * left unallocated on the node. Because the node is under high utilization,
   * the second OPPORTUNISTIC container is expected to be killed in order to
   * make room for the GUARANTEED container.
   */
  @Test
  public void testKillOppContainersWithPartialOverallocationHighUtilization()
      throws Exception {
    containerManager.start();

    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(1024, 1), false));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(824, 1), false));
          }
        }
    ));
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);

    // the containers utilization is very high
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(1800, 0, 1.0f/8));

    // start a GUARANTEED container that requests more than what's left
    // unallocated on the node 512 + 1024 + 824 > 2048
    containerManager.startContainers(StartContainersRequest.newInstance(
        Collections.singletonList(
            createStartContainerRequest(2,
                BuilderUtils.newResource(512, 1), true))
    ));

    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.RUNNING);
    // the last launched OPPORTUNISTIC container is expected to be killed
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.DONE);

    GetContainerStatusesRequest statRequest = GetContainerStatusesRequest.
        newInstance(new ArrayList<ContainerId>() {
          {
            add(createContainerId(0));
            add(createContainerId(1));
            add(createContainerId(2));
          }
        });
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      if (status.getContainerId().equals(createContainerId(1))) {
        Assert.assertTrue(status.getDiagnostics().contains(
            "Container Killed to make room for Guaranteed Container"));
      } else {
        Assert.assertEquals(status.getContainerId() + " is not RUNNING",
            ContainerSubState.RUNNING, status.getContainerSubState());
      }
      System.out.println("\nStatus : [" + status + "]\n");
    }
  }


  /**
   * Start three OPPORTUNISTIC containers which in aggregates exceeds the
   * capacity of the node, yet whose utilization is low relative
   * to the resources they requested, resulting in a low node utilization.
   * Then try to start a GUARANTEED container. Even though the node has
   * nothing left unallocated, it is expected to start immediately
   * without killing any running OPPORTUNISTIC containers because the node
   * utilization is very low and overallocation is turned on.
   */
  @Test
  public void testKillNoOppContainersWithOverallocationLowUtilization()
      throws Exception {
    containerManager.start();

    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(1024, 1), false));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(1024, 1), false));
            add(createStartContainerRequest(2,
                BuilderUtils.newResource(1024, 1), false));
          }
        }
    ));
    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .drainAsyncEvents();

    // Two OPPORTUNISTIC containers are expected to start with the
    // unallocated resources, but one will be queued because no
    // over-allocation is allowed at container scheduler events.
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.SCHEDULED);

    // try to start the opportunistic container out of band because it can
    // not be launched at container scheduler event if the node would be
    // over-allocated.
    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .startContainersOutOfBandUponLowUtilization();

    // now the queued opportunistic container should also start
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.RUNNING);

    // the containers utilization is low
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(1024, 0, 1.0f/8));

    // start a GUARANTEED container that requests more than what's left
    // unallocated on the node: (512  + 1024 + 824) > 2048
    containerManager.startContainers(StartContainersRequest.newInstance(
        Collections.singletonList(
            createStartContainerRequest(3,
                BuilderUtils.newResource(512, 1), true))
    ));

    // the GUARANTEED container is expected be launched immediately without
    // killing any OPPORTUNISTIC containers
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(3), ContainerSubState.RUNNING);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.RUNNING);
        put(createContainerId(3), ContainerSubState.RUNNING);
      }
    });
  }

  /**
   * Start four OPPORTUNISTIC containers which in aggregates exceeds the
   * capacity of the node. The real resource utilization of the first two
   * OPPORTUNISTIC containers are high whereas that of the latter two are
   * almost zero. Then try to start a GUARANTEED container. The GUARANTEED
   * container will eventually start running after preempting the third
   * and fourth OPPORTUNISTIC container (which releases no resources) and
   * then the second OPPORTUNISTIC container.
   */
  public void
      testKillOppContainersConservativelyWithOverallocationHighUtilization()
          throws Exception {
    containerManager.start();

    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(1024, 1), false));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(1024, 1), false));
            add(createStartContainerRequest(2,
                BuilderUtils.newResource(512, 1), false));
            add(createStartContainerRequest(3,
                BuilderUtils.newResource(1024, 1), false));
          }
        }
    ));
    // All four GUARANTEED containers are all expected to start
    // because the containers utilization is low (0 at the point)
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(3), ContainerSubState.RUNNING);

    // the containers utilization is at the overallocation threshold
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(1536, 0, 1.0f/2));

    // try to start a GUARANTEED container when there's nothing left unallocated
    containerManager.startContainers(StartContainersRequest.newInstance(
        Collections.singletonList(
            createStartContainerRequest(4,
                BuilderUtils.newResource(1024, 1), true))
    ));

    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(4), ContainerSubState.RUNNING);
    GetContainerStatusesRequest statRequest = GetContainerStatusesRequest.
        newInstance(new ArrayList<ContainerId>() {
          {
            add(createContainerId(0));
            add(createContainerId(1));
            add(createContainerId(2));
            add(createContainerId(3));
            add(createContainerId(4));
          }
        });
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      if (status.getContainerId().equals(createContainerId(0)) ||
          status.getContainerId().equals(createContainerId(4))) {
        Assert.assertEquals(
            ContainerSubState.RUNNING, status.getContainerSubState());
      } else {
        Assert.assertTrue(status.getDiagnostics().contains(
            "Container Killed to make room for Guaranteed Container"));
      }
      System.out.println("\nStatus : [" + status + "]\n");
    }
  }

  /**
   * Start two OPPORTUNISTIC containers followed by one GUARANTEED container,
   * which in aggregate exceeds the capacity of the node. The first two
   * OPPORTUNISTIC containers use almost no resources whereas the GUARANTEED
   * one utilizes nearly all of its resource requested. Then try to start two
   * more OPPORTUNISTIC containers. The two OPPORTUNISTIC containers are
   * expected to be queued immediately. Upon the completion of the
   * resource-usage-heavy GUARANTEED container, both OPPORTUNISTIC containers
   * are expected to start.
   */
  @Test
  public void testStartOppContainersUponContainerCompletion() throws Exception {
    containerManager.start();

    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(512, 1), false));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(512, 1), false));
            add(createStartContainerRequest(2,
                BuilderUtils.newResource(1024, 1), true));
          }
        }
    ));

    // All three containers are all expected to start immediately
    // because the node utilization is low (0 at the point)
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.RUNNING);

    // the container utilization is at the overallocation threshold
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(1536, 0, 1.0f/2));

    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(3,
                BuilderUtils.newResource(512, 1), false));
            add(createStartContainerRequest(4,
                BuilderUtils.newResource(800, 1), false));
          }
        }
    ));
    // the two new OPPORTUNISTIC containers are expected to be queued
    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(3), ContainerSubState.SCHEDULED);
        put(createContainerId(4), ContainerSubState.SCHEDULED);
      }
    });

    // the GUARANTEED container is completed releasing resources
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(100, 0, 1.0f/5));
    allowContainerToSucceed(2);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.DONE);

    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .drainAsyncEvents();

    // only one OPPORTUNISTIC container is start because no over-allocation
    // is allowed to start OPPORTUNISTIC containers at container finish event.
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(3), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(4), ContainerSubState.SCHEDULED);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.DONE);
        put(createContainerId(3), ContainerSubState.RUNNING);
        put(createContainerId(4), ContainerSubState.SCHEDULED);
      }
    });

    // now try to start the OPPORTUNISTIC container that was queued because
    // we don't start OPPORTUNISTIC containers at container finish event if
    // the node would be over-allocated
    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .startContainersOutOfBandUponLowUtilization();
    ((LongRunningContainerSimulatingContainersManager) containerManager)
        .drainAsyncEvents();
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(4), ContainerSubState.RUNNING);
    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.DONE);
        put(createContainerId(3), ContainerSubState.RUNNING);
        put(createContainerId(4), ContainerSubState.RUNNING);
      }
    });
  }

  /**
   * Start one GUARANTEED container that consumes all the resources on the
   * node and keeps running, followed by two OPPORTUNISTIC containers that
   * will be queued forever because there is no containers starting or
   * finishing. Then try to start OPPORTUNISTIC containers out of band.
   */
  @Test
  public void testStartOpportunisticContainersOutOfBand() throws Exception {
    containerManager.start();

    containerManager.startContainers(StartContainersRequest.newInstance(
        Collections.singletonList(
            createStartContainerRequest(0,
                BuilderUtils.newResource(2048, 4), true))));
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);

    // the container is fully utilizing its resources
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(2048, 0, 1.0f));

    // send two OPPORTUNISTIC container requests that are expected to be queued
    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(512, 1), false));
            add(createStartContainerRequest(2,
                BuilderUtils.newResource(512, 1), false));
          }
        }
    ));
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.SCHEDULED);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.SCHEDULED);

    // the containers utilization dropped to the overallocation threshold
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(1536, 0, 1.0f/2));

    // try to start opportunistic containers out of band.
    ((LongRunningContainerSimulatingContainersManager)containerManager)
        .startContainersOutOfBandUponLowUtilization();

    // no containers in queue are expected to be launched because the
    // containers utilization is not below the over-allocation threshold
    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.SCHEDULED);
        put(createContainerId(2), ContainerSubState.SCHEDULED);
      }
    });

    // the GUARANTEED container is completed releasing resources
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(100, 0, 1.0f/5));

    // the containers utilization dropped way below the overallocation threshold
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(512, 0, 1.0f/8));

    ((LongRunningContainerSimulatingContainersManager)containerManager)
        .startContainersOutOfBandUponLowUtilization();

    // the two OPPORTUNISTIC containers are expected to be launched
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.RUNNING);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.RUNNING);
      }
    });
  }


  private void setContainerResourceUtilization(ResourceUtilization usage) {
    ((ContainerMonitorForOverallocationTest)
        containerManager.getContainersMonitor())
            .setContainerResourceUsage(usage);
  }

  private void allowContainerToSucceed(int containerId) {
    ((LongRunningContainerSimulatingContainerExecutor) this.exec)
        .containerSucceeded(createContainerId(containerId));
  }


  protected StartContainerRequest createStartContainerRequest(int containerId,
      Resource resource, boolean isGuaranteed) throws IOException {
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    ExecutionType executionType = isGuaranteed ? ExecutionType.GUARANTEED :
        ExecutionType.OPPORTUNISTIC;
    Token containerToken = createContainerToken(
        createContainerId(containerId),
        DUMMY_RM_IDENTIFIER, context.getNodeId(), user, resource,
        context.getContainerTokenSecretManager(),
        null, executionType);

    return StartContainerRequest.newInstance(
        containerLaunchContext, containerToken);
  }

  protected void verifyContainerStatuses(
      Map<ContainerId, ContainerSubState> expected)
      throws IOException, YarnException {
    List<ContainerId> statList = new ArrayList<>(expected.keySet());
    GetContainerStatusesRequest statRequest =
        GetContainerStatusesRequest.newInstance(statList);
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();

    for (ContainerStatus status : containerStatuses) {
      ContainerId containerId = status.getContainerId();
      Assert.assertEquals(containerId + " is in unexpected state",
          expected.get(containerId), status.getContainerSubState());
    }
  }

  /**
   * A container manager that sends a dummy container pid while it's cleaning
   * up running containers. Used along with
   * LongRunningContainerSimulatingContainerExecutor to simulate long running
   * container processes for testing purposes.
   */
  private static class LongRunningContainerSimulatingContainersManager
      extends ContainerManagerImpl {

    private final String user;

    LongRunningContainerSimulatingContainersManager(
        Context context, ContainerExecutor exec,
        DeletionService deletionContext,
        NodeStatusUpdater nodeStatusUpdater,
        NodeManagerMetrics metrics,
        LocalDirsHandlerService dirsHandler, String user) {
      super(context, exec, deletionContext,
          nodeStatusUpdater, metrics, dirsHandler);
      this.user = user;
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
    protected AsyncDispatcher createDispatcher() {
      return new DrainDispatcher();
    }

    /**
     * Create a container launcher that signals container processes
     * with a dummy pid. The container processes are simulated in
     * LongRunningContainerSimulatingContainerExecutor which does
     * not write a pid file on behalf of containers to launch, so
     * the pid does not matter.
     */
    @Override
    protected ContainersLauncher createContainersLauncher(
        Context context, ContainerExecutor exec) {
      ContainerManagerImpl containerManager = this;
      return new ContainersLauncher(context, dispatcher, exec, dirsHandler,
          this) {
        @Override
        protected ContainerLaunch createContainerLaunch(
            Application app, Container container) {
          return new ContainerLaunch(context, getConfig(), dispatcher,
              exec, app, container, dirsHandler, containerManager) {
            @Override
            protected String getContainerPid(Path pidFilePath)
                throws Exception {
              return "123";
            }

          };
        }
      };
    }

    @Override
    protected ContainersMonitor createContainersMonitor(
        ContainerExecutor exec) {
      return new ContainerMonitorForOverallocationTest(exec,
          dispatcher, context);
    }

    public void startContainersOutOfBandUponLowUtilization() {
      ((ContainerMonitorForOverallocationTest) getContainersMonitor())
          .attemptToStartContainersUponLowUtilization();
    }

    public void drainAsyncEvents() {
      ((DrainDispatcher) dispatcher).await();
    }
  }

  /**
   * A container executor that simulates long running container processes
   * by having container launch threads sleep infinitely until it's given
   * a signal to finish with either a success or failure exit code.
   */
  private static class LongRunningContainerSimulatingContainerExecutor
      extends DefaultContainerExecutor {
    private ConcurrentHashMap<ContainerId, ContainerFinishLatch> containers =
        new ConcurrentHashMap<>();

    public void containerSucceeded(ContainerId containerId) {
      ContainerFinishLatch containerFinishLatch = containers.get(containerId);
      if (containerFinishLatch != null) {
        containerFinishLatch.toSucceed();
      }
    }

    public void containerFailed(ContainerId containerId) {
      ContainerFinishLatch containerFinishLatch = containers.get(containerId);
      if (containerFinishLatch != null) {
        containerFinishLatch.toFail();
      }
    }

    /**
     * Simulate long running container processes by having container launcher
     * threads wait infinitely for a signal to finish.
     */
    @Override
    public int launchContainer(ContainerStartContext ctx)
        throws IOException, ConfigurationException {
      ContainerId container = ctx.getContainer().getContainerId();
      containers.putIfAbsent(container, new ContainerFinishLatch(container));

      // simulate a long running container process by having the
      // container launch thread sleep forever until it's given a
      // signal to finish with a exit code.
      while (!containers.get(container).toProceed) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          return -1;
        }
      }

      return containers.get(container).getContainerExitCode();
    }

    /**
     * Override signalContainer() so that simulated container processes
     * are properly cleaned up.
     */
    @Override
    public boolean signalContainer(ContainerSignalContext ctx)
        throws IOException {
      containerSucceeded(ctx.getContainer().getContainerId());
      return true;
    }

    /**
     * A signal that container launch threads wait for before exiting
     * in order to simulate long running container processes.
     */
    private static final class ContainerFinishLatch {
      volatile boolean toProceed;
      int exitCode;
      ContainerId container;

      ContainerFinishLatch(ContainerId containerId) {
        exitCode = 0;
        toProceed = false;
        container = containerId;
      }

      void toSucceed() {
        exitCode = 0;
        toProceed = true;
      }

      void toFail() {
        exitCode = -101;
        toProceed = true;
      }

      int getContainerExitCode() {
        // read barrier of toProceed to make sure the exit code is not stale
        if (toProceed) {
          LOG.debug(container + " finished with exit code: " + exitCode);
        }
        return exitCode;
      }
    }
  }

  /**
   * A test implementation of container monitor that allows control of
   * current resource utilization.
   */
  private static class ContainerMonitorForOverallocationTest
      extends ContainersMonitorImpl {

    private ResourceUtilization containerResourceUsage =
        ResourceUtilization.newInstance(0, 0, 0.0f);

    ContainerMonitorForOverallocationTest(ContainerExecutor exec,
        AsyncDispatcher dispatcher, Context context) {
      super(exec, dispatcher, context);
    }

    @Override
    public long getPmemAllocatedForContainers() {
      return NM_CONTAINERS_MEMORY_MB * 1024 * 1024L;
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
      return NM_CONTAINERS_VCORES;
    }

    @Override
    public ContainersResourceUtilization getContainersUtilization(
        boolean latest) {
      return new ContainersMonitor.ContainersResourceUtilization(
          containerResourceUsage, System.currentTimeMillis());
    }

    @Override
    protected void checkOverAllocationPrerequisites() {
      // do not check
    }


    public void setContainerResourceUsage(
        ResourceUtilization containerResourceUsage) {
      this.containerResourceUsage = containerResourceUsage;
    }
  }
}
