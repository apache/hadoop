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
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
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
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.exceptions.YarnException;
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
    conf.setFloat(YarnConfiguration.NM_OVERALLOCATION_CPU_PREEMPTION_THRESHOLD,
        0.8f);
    conf.setFloat(
        YarnConfiguration.NM_OVERALLOCATION_MEMORY_PREEMPTION_THRESHOLD, 0.8f);
    conf.setInt(YarnConfiguration.NM_OVERALLOCATION_PREEMPTION_CPU_COUNT, 2);
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
              BuilderUtils.newResource(1024, 1), ExecutionType.OPPORTUNISTIC));
          add(createStartContainerRequest(1,
              BuilderUtils.newResource(1024, 1), ExecutionType.GUARANTEED));
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
                BuilderUtils.newResource(1024, 1), ExecutionType.GUARANTEED));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(824, 1), ExecutionType.GUARANTEED));
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
                BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC))
    ));
    ((ContainerManagerForTest) containerManager).drainAsyncEvents();

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
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

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
                BuilderUtils.newResource(1024, 1), ExecutionType.GUARANTEED));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(824, 1), ExecutionType.GUARANTEED));
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
                BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC))
    ));

    // try to start opportunistic containers out of band because they can
    // not be launched at container scheduler event if the node would be
    // over-allocated.
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

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
                BuilderUtils.newResource(1024, 1), ExecutionType.GUARANTEED));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(1024, 1), ExecutionType.GUARANTEED));
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
                BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC))
    ));
    ((ContainerManagerForTest) containerManager).drainAsyncEvents();

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
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

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
                BuilderUtils.newResource(1024, 1), ExecutionType.GUARANTEED));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(1024, 1), ExecutionType.GUARANTEED));
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
              BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC));
    }
    containerManager.startContainers(
        StartContainersRequest.newInstance(moreContainerRequests));
    ((ContainerManagerForTest) containerManager).drainAsyncEvents();

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
                BuilderUtils.newResource(1200, 1), ExecutionType.GUARANTEED));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(400, 1), ExecutionType.GUARANTEED));
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
                BuilderUtils.newResource(400, 1), ExecutionType.OPPORTUNISTIC))
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
                BuilderUtils.newResource(1024, 1),
                ExecutionType.OPPORTUNISTIC));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(824, 1),
                ExecutionType.OPPORTUNISTIC));
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
                BuilderUtils.newResource(512, 1), ExecutionType.GUARANTEED))
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
                BuilderUtils.newResource(1024, 1),
                ExecutionType.OPPORTUNISTIC));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(824, 1),
                ExecutionType.OPPORTUNISTIC));
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
                BuilderUtils.newResource(512, 1), ExecutionType.GUARANTEED))
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
                BuilderUtils.newResource(1024, 1),
                ExecutionType.OPPORTUNISTIC));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(1024, 1),
                ExecutionType.OPPORTUNISTIC));
            add(createStartContainerRequest(2,
                BuilderUtils.newResource(1024, 1),
                ExecutionType.OPPORTUNISTIC));
          }
        }
    ));
    ((ContainerManagerForTest) containerManager).drainAsyncEvents();

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
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

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
                BuilderUtils.newResource(512, 1), ExecutionType.GUARANTEED))
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
                BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC));
            add(createStartContainerRequest(2,
                BuilderUtils.newResource(1024, 1), ExecutionType.GUARANTEED));
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
                BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC));
            add(createStartContainerRequest(4,
                BuilderUtils.newResource(800, 1), ExecutionType.OPPORTUNISTIC));
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

    ((ContainerManagerForTest) containerManager).drainAsyncEvents();

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
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();
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
                BuilderUtils.newResource(2048, 4), ExecutionType.GUARANTEED))));
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
                BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC));
            add(createStartContainerRequest(2,
                BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC));
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
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

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

    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

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

  /**
   * Start a GUARANTEED container, an OPPORTUNISTIC container, a GUARANTEED
   * container and another OPPORTUNISTIC container in order. When the node
   * memory utilization is over its preemption threshold, the two OPPORTUNISTIC
   * containers should be killed.
   */
  @Test
  public void testPreemptOpportunisticContainersUponHighMemoryUtilization()
      throws Exception {
    containerManager.start();

    // try to start four containers at once. the first GUARANTEED container
    // that requests (1024 MB, 1 vcore) can be launched because there is
    // (2048 MB, 4 vcores) unallocated. The second container, which is
    // OPPORTUNISTIC, can also be launched because it asks for 512 MB, 1 vcore
    // which is less than what is left unallocated after launching the first
    // one GUARANTEED container, (1024 MB, 3 vcores).
    // The 3rd container, which is GUARANTEED, can also be launched because
    // the node resource utilization utilization is zero such that
    // over-allocation kicks in. The 4th one, an OPPORTUNISTIC container,
    // will be queued because OPPORTUNISTIC containers can only be
    // launched when node resource utilization is checked, if launching them
    // would cause node over-allocation.
    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(1024, 1), ExecutionType.GUARANTEED));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC));
            add(createStartContainerRequest(2,
                BuilderUtils.newResource(1024, 1), ExecutionType.GUARANTEED));
            add(createStartContainerRequest(3,
                BuilderUtils.newResource(300, 1), ExecutionType.OPPORTUNISTIC));
          }
        }
    ));
    ((ContainerManagerForTest) containerManager).drainAsyncEvents();

    // the first three containers are all expected to start
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(3), ContainerSubState.SCHEDULED);

    // try to check node resource utilization and start the second
    // opportunistic containers out of band. Because the node resource
    // utilization is zero at the moment, over-allocation will kick in
    // and the container will be launched.
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(3), ContainerSubState.RUNNING);

    // the containers memory utilization is over the preemption threshold
    // (2048 > 2048 * 0.8 = 1638.4)
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(2048, 0, 0.5f));
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

    // (2048 - 2048 * 0.8) = 409.6 MB of memory needs to be reclaimed,
    // which shall result in both OPPORTUNISTIC containers to be preempted.
    // (Preempting the most recently launched OPPORTUNISTIC container, that
    // is the 4th container, would only release at most 300 MB of memory)
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.DONE);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(3), ContainerSubState.DONE);

    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.DONE);
        put(createContainerId(2), ContainerSubState.RUNNING);
        put(createContainerId(3), ContainerSubState.DONE);
      }
    });

  }

  /**
   * Start a GUARANTEED container followed by an OPPORTUNISTIC container, which
   * in aggregates does not take more than the capacity of the node.
   * When the node memory utilization is above the preemption threshold, the
   * OPPORTUNISTIC container should not be killed because the node is not being
   * over-allocated.
   */
  @Test
  public void testNoPreemptionUponHighMemoryUtilizationButNoOverallocation()
      throws Exception {
    containerManager.start();

    // start two containers, one GUARANTEED and one OPPORTUNISTIC, that together
    // take up all the allocations (2048 MB of memory and 4 vcores available on
    // the node). They can be both launched immediately because there are enough
    // allocations to do so. When the two containers fully utilize their
    // resource requests, that is, the node is being 100% utilized, the
    // OPPORTUNISTIC container shall continue to run because the node is
    // not be over-allocated.
    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(1024, 2),
                ExecutionType.GUARANTEED));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(1024, 2),
                ExecutionType.OPPORTUNISTIC));
          }
        }
    ));
    // both containers shall be launched immediately because there are
    // enough allocations to do so
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);

    // the node is being fully utilized, which is above the preemption
    // threshold (2048 * 0.75 = 1536 MB, 1.0f)
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(2048, 0, 1.0f));
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

    // no containers shall be preempted because the node is not being
    // over-allocated so it is safe to allow the node to be fully utilized
    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
      }
    });
  }

  /**
   * Start a GUARANTEED container, an OPPORTUNISTIC container, a GUARANTEED
   * container and another OPPORTUNISTIC container in order. When the node
   * cpu utilization is over its preemption threshold a few times in a row,
   * the two OPPORTUNISTIC containers should be killed one by one.
   */
  @Test
  public void testPreemptionUponHighCPUUtilization() throws Exception {
    containerManager.start();

    // try to start 4 containers at once. The first container, can be
    // safely launched immediately (2048 MB, 4 vcores left unallocated).
    // The second container, can also be launched immediately, because
    // there is enough resources unallocated after launching the first
    // container (2048 - 512 = 1536 MB, 4 - 2 = 2 vcores). After launching
    // the first two containers, there are 1024 MBs of memory and 1 vcore
    // left unallocated, so there is not enough allocation to launch the
    // third container. But because the third container is GUARANTEED and
    // the node resource utilization is zero, we can launch it based on
    // over-allocation (the projected resource utilization will be 512 MB
    // of memory and 2 vcores, below the over-allocation threshold)
    // The fourth container, which is OPPORTUNISTIC, will be queued because
    // OPPORTUNISTIC containers can not be launched based on over-allocation
    // upon container start requests (they can only be launched when node
    // resource utilization is checked in ContainersMonitor)
    containerManager.startContainers(StartContainersRequest.newInstance(
        new ArrayList<StartContainerRequest>() {
          {
            add(createStartContainerRequest(0,
                BuilderUtils.newResource(512, 2), ExecutionType.GUARANTEED));
            add(createStartContainerRequest(1,
                BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC));
            add(createStartContainerRequest(2,
                BuilderUtils.newResource(512, 2), ExecutionType.GUARANTEED));
            add(createStartContainerRequest(3,
                BuilderUtils.newResource(512, 1), ExecutionType.OPPORTUNISTIC));
          }
        }
    ));
    ((ContainerManagerForTest) containerManager).drainAsyncEvents();
    // the first three containers are expected to start. The first two
    // can be launched based on free allocation, the third can be
    // launched based on over-allocation
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(0), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.RUNNING);
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(2), ContainerSubState.RUNNING);

    // try to start second opportunistic containers out of band.
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

    // the second opportunistic container is expected to start because
    // the node resource utilization is at zero, the projected utilization
    // is 512 MBs of memory and 1 vcore
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(3), ContainerSubState.RUNNING);

    final float fullCpuUtilization = 1.0f;

    // the containers CPU utilization is over its preemption threshold (0.8f)
    // for the first time
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(2048, 0, fullCpuUtilization));
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

    // all containers should continue to be running because we don't
    // preempt OPPORTUNISTIC containers right away
    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.RUNNING);
        put(createContainerId(3), ContainerSubState.RUNNING);
      }
    });

    // the containers CPU utilization is over its preemption threshold (0.8f)
    // for the second time
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(2048, 0, fullCpuUtilization));
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

    // all containers should continue to be running because we don't preempt
    // OPPORTUNISTIC containers when the cpu is over the preemption threshold
    // (0.8f) the second time
    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.RUNNING);
        put(createContainerId(3), ContainerSubState.RUNNING);
      }
    });

    // the containers CPU utilization is over the preemption threshold (0.8f)
    // for the third time
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(2048, 0, fullCpuUtilization));
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

    // because CPU utilization is over its preemption threshold three times
    // consecutively, the amount of cpu utilization over the preemption
    // threshold, that is, 1.0 - 0.8 = 0.2f CPU needs to be reclaimed and
    // as a result, the most recently launched OPPORTUNISTIC container should
    // be killed
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(3), ContainerSubState.DONE);
    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.RUNNING);
        put(createContainerId(3), ContainerSubState.DONE);
      }
    });

    // again, the containers CPU utilization is over the preemption threshold
    // (0.8f) for the first time (the cpu over-limit count is reset every time
    // a preemption is triggered)
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(2048, 0, fullCpuUtilization));
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

    // no CPU resource is expected to be reclaimed when the CPU utilization
    // goes over the preemption threshold the first time
    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.RUNNING);
        put(createContainerId(3), ContainerSubState.DONE);
      }
    });

    // the containers CPU utilization is over the preemption threshold (0.9f)
    // for the second time
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(2048, 0, fullCpuUtilization));
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

    // still no CPU resource is expected to be reclaimed when the CPU
    // utilization goes over the preemption threshold the second time
    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.RUNNING);
        put(createContainerId(2), ContainerSubState.RUNNING);
        put(createContainerId(3), ContainerSubState.DONE);
      }
    });

    // the containers CPU utilization is over the preemption threshold
    // for the third time
    setContainerResourceUtilization(
        ResourceUtilization.newInstance(2048, 0, fullCpuUtilization));
    ((ContainerManagerForTest) containerManager)
        .checkNodeResourceUtilization();

    // because CPU utilization is over its preemption threshold three times
    // consecutively, the amount of cpu utilization over the preemption
    // threshold, that is, 1.0 - 0.8 = 0.2f CPU needs to be reclaimed and
    // as a result, the other OPPORTUNISTIC container should be killed
    BaseContainerManagerTest.waitForContainerSubState(containerManager,
        createContainerId(1), ContainerSubState.DONE);
    verifyContainerStatuses(new HashMap<ContainerId, ContainerSubState>() {
      {
        put(createContainerId(0), ContainerSubState.RUNNING);
        put(createContainerId(1), ContainerSubState.DONE);
        put(createContainerId(2), ContainerSubState.RUNNING);
        put(createContainerId(3), ContainerSubState.DONE);
      }
    });
  }


  private void setContainerResourceUtilization(ResourceUtilization usage) {
    ((ContainerMonitorForTest) containerManager.getContainersMonitor())
        .setContainerResourceUsage(usage);
  }

  private void allowContainerToSucceed(int containerId) {
    ((LongRunningContainerSimulatingContainerExecutor) this.exec)
        .containerSucceeded(createContainerId(containerId));
  }


  protected StartContainerRequest createStartContainerRequest(
      int containerId, Resource resource, ExecutionType executionType)
      throws IOException {
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
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
      extends ContainerManagerForTest {

    LongRunningContainerSimulatingContainersManager(
        Context context, ContainerExecutor exec,
        DeletionService deletionContext,
        NodeStatusUpdater nodeStatusUpdater,
        NodeManagerMetrics metrics,
        LocalDirsHandlerService dirsHandler, String user) {
        super(context, exec, deletionContext,
          nodeStatusUpdater, metrics, dirsHandler, user);
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
}
