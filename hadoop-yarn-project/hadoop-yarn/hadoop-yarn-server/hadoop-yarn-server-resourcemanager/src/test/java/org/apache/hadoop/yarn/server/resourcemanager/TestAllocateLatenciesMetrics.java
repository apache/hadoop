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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class TestAllocateLatenciesMetrics {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestAllocateLatenciesMetrics.class);
  private static final int GB = 1024;

  private MockRM rm;
  private DrainDispatcher dispatcher;

  private OpportunisticContainersStatus oppContainersStatus =
      getOpportunisticStatus();

  @Before
  public void createAndStartRM() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    startRM(conf);
  }

  private void startRM(final YarnConfiguration conf) {
    dispatcher = new DrainDispatcher();
    rm = new MockRM(conf) {
      @Override protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.start();
  }

  @After
  public void stopRM() {
    if (rm != null) {
      rm.stop();
    }
  }

  @Test
  public void testGuaranteedLatenciesMetrics() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    nm1.registerNode();
    nm2.registerNode();

    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app").withUser("user").withAcls(null)
            .withQueue("default").build();

    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();

    // All nodes 1 to 2 will be applicable for scheduling.
    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    ResourceRequest rr1 = ResourceRequest
        .newInstance(Priority.newInstance(1), "*",
            Resources.createResource(1 * GB), 1, true, null,
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true));
    ResourceRequest rr2 = ResourceRequest
        .newInstance(Priority.newInstance(1), "*",
            Resources.createResource(1 * GB), 1, true, null,
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true));
    rr1.setAllocationRequestId(1);
    rr2.setAllocationRequestId(2);
    AllocateResponse allocateResponse =
        am1.allocate(Arrays.asList(rr1, rr2), null);

    // make sure the containers get allocated
    nm1.nodeHeartbeat(true);
    List<Container> allocatedContainers =
        am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
    GenericTestUtils.waitFor(() -> {
      try {
        nm1.nodeHeartbeat(true);
        allocatedContainers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers());
        if (allocatedContainers.size() < 2) {
          return false;
        }
        return true;
      } catch (Exception e) {
        LOG.error("Error in allocating container " + e);
        return false;
      }
    }, 100, 2000);

    Container container = allocatedContainers.get(0);
    MockNM allocNode = nodes.get(container.getNodeId());

    // Start Container in NM
    allocNode.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.GUARANTEED,
            ContainerState.RUNNING, "", 0)), true);
    rm.drainEvents();

    // Verify that container is actually running wrt the RM..
    RMContainer rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(container.getId().getApplicationAttemptId())
        .getRMContainer(container.getId());
    Assert.assertEquals(RMContainerState.RUNNING, rmContainer.getState());

    // Container Completed in the NM
    allocNode.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.GUARANTEED,
            ContainerState.COMPLETE, "", 0)), true);
    Assert.assertTrue(
        ClusterMetrics.getMetrics().allocateLatencyGuarQuantiles.changed());
    rm.drainEvents();

    // Verify that container has been removed..
    rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(container.getId().getApplicationAttemptId())
        .getRMContainer(container.getId());
    Assert.assertNull(rmContainer);
  }

  @Test
  public void testOpportunisticLatenciesMetrics() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    nm1.registerNode();
    nm2.registerNode();

    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app").withUser("user").withAcls(null)
            .withQueue("default").build();

    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();

    // All nodes 1 to 2 will be applicable for scheduling.
    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    GenericTestUtils
        .waitFor(() -> amservice.getLeastLoadedNodes().size() == 2, 10,
            10 * 100);

    AllocateResponse allocateResponse = am1.allocate(Arrays.asList(
        ResourceRequest.newInstance(Priority.newInstance(1), "*",
            Resources.createResource(1 * GB), 2, true, null,
            ExecutionTypeRequest
                .newInstance(ExecutionType.OPPORTUNISTIC, true))), null);

    List<Container> allocatedContainers =
        allocateResponse.getAllocatedContainers();
    Assert.assertEquals(2, allocatedContainers.size());

    Container container = allocatedContainers.get(0);
    MockNM allocNode = nodes.get(container.getNodeId());

    // Start Container in NM
    allocNode.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.OPPORTUNISTIC,
            ContainerState.RUNNING, "", 0)), true);
    rm.drainEvents();

    // Verify that container is actually running wrt the RM..
    RMContainer rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(container.getId().getApplicationAttemptId())
        .getRMContainer(container.getId());
    Assert.assertEquals(RMContainerState.RUNNING, rmContainer.getState());

    // Container Completed in the NM
    allocNode.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.OPPORTUNISTIC,
            ContainerState.COMPLETE, "", 0)), true);
    Assert.assertTrue(
        ClusterMetrics.getMetrics().allocateLatencyOppQuantiles.changed());
    rm.drainEvents();

    // Verify that container has been removed..
    rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(container.getId().getApplicationAttemptId())
        .getRMContainer(container.getId());
    Assert.assertNull(rmContainer);
  }

  private OpportunisticContainersStatus getOpportunisticStatus() {
    return getOpportunisticStatus(-1, 100, 1000);
  }

  private OpportunisticContainersStatus getOpportunisticStatus(int waitTime,
      int queueLength, int queueCapacity) {
    OpportunisticContainersStatus status =
        OpportunisticContainersStatus.newInstance();
    status.setEstimatedQueueWaitTime(waitTime);
    status.setOpportQueueCapacity(queueCapacity);
    status.setWaitQueueLength(queueLength);
    return status;
  }
}