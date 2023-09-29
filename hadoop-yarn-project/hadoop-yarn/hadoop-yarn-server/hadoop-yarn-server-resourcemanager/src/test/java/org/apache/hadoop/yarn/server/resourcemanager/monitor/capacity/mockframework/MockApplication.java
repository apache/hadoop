/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MockApplication {
  private static final Logger LOG = LoggerFactory.getLogger(MockApplication.class);
  private List<RMContainer> liveContainers = new ArrayList<>();
  private List<RMContainer> reservedContainers = new ArrayList<>();

  private ApplicationId appId;
  final String containersConfig;
  final String queueName;
  ApplicationAttemptId appAttemptId;
  FiCaSchedulerApp app;

  MockApplication(int id, String containersConfig, String queueName) {
    this.appId = ApplicationId.newInstance(0L, id);
    this.containersConfig = containersConfig;
    this.queueName = queueName;

    //dynamic fields
    this.appAttemptId = ApplicationAttemptId
        .newInstance(appId, 1);
    //this must be the last step
    setupInitialMocking(queueName);
  }

  private void setupInitialMocking(String queueName) {
    this.app = mock(FiCaSchedulerApp.class);
    when(app.getAMResource(anyString()))
        .thenReturn(Resources.createResource(0, 0));
    when(app.getLiveContainers()).thenReturn(liveContainers);
    when(app.getReservedContainers()).thenReturn(reservedContainers);
    when(app.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(app.getApplicationId()).thenReturn(appId);
    when(app.getQueueName()).thenReturn(queueName);
  }

  private void addLiveContainer(RMContainer c) {
    this.liveContainers.add(c);
  }

  private void addReservedContainer(RMContainer c) {
    this.reservedContainers.add(c);
  }

  void addMockContainer(MockContainer mockContainer,
      FiCaSchedulerNode schedulerNode, LeafQueue queue) {
    int containerId = mockContainer.containerId;
    ContainerSpecification containerSpec = mockContainer.containerSpec;

    if (containerId == 1) {
      when(app.getAMResource(containerSpec.label)).thenReturn(containerSpec.resource);
      when(app.getAppAMNodePartitionName()).thenReturn(containerSpec.label);
    }

    if (containerSpec.reserved) {
      addReservedContainer(mockContainer.rmContainerMock);
    } else {
      addLiveContainer(mockContainer.rmContainerMock);
    }

    // Add container to scheduler-node
    addContainerToSchedulerNode(schedulerNode, mockContainer.rmContainerMock, containerSpec.reserved);

    // If this is a non-exclusive allocation
    String partition = null;
    if (containerSpec.label.isEmpty()
        && !(partition = schedulerNode.getPartition())
        .isEmpty()) {
      Map<String, TreeSet<RMContainer>> ignoreExclusivityContainers = queue
          .getIgnoreExclusivityRMContainers();
      if (!ignoreExclusivityContainers.containsKey(partition)) {
        ignoreExclusivityContainers.put(partition, new TreeSet<>());
      }
      ignoreExclusivityContainers.get(partition).add(mockContainer.rmContainerMock);
      LOG.info("Added an ignore-exclusivity container to partition {}, new size is: {}", partition, ignoreExclusivityContainers.get(partition).size());

    }
    LOG.debug("add container to app=" + appAttemptId + " res=" + containerSpec.resource + " node="
        + containerSpec.nodeId + " nodeLabelExpression=" + containerSpec.label + " partition="
        + partition);
  }

  void addAggregatedContainerData(ContainerSpecification containerSpec,
      Resource usedResources) {
    // If app has 0 container, and it has only pending, still make sure to
    // update label.
    if (containerSpec.repeat == 0) {
      when(app.getAppAMNodePartitionName()).thenReturn(containerSpec.label);
    }

    // Some more app specific aggregated data can be better filled here.
    when(app.getPriority()).thenReturn(containerSpec.priority);
    when(app.getUser()).thenReturn(containerSpec.username);
    when(app.getCurrentConsumption()).thenReturn(usedResources);
    when(app.getCurrentReservation())
        .thenReturn(Resources.createResource(0, 0));

    Map<String, Resource> pendingForDefaultPartition =
        new HashMap<>();
    // Add for default partition for now.
    pendingForDefaultPartition.put(containerSpec.label, containerSpec.pendingResource);
    when(app.getTotalPendingRequestsPerPartition())
        .thenReturn(pendingForDefaultPartition);

    // need to set pending resource in resource usage as well
    ResourceUsage ru = Mockito.spy(new ResourceUsage());
    ru.setUsed(containerSpec.label, usedResources);
    when(ru.getCachedUsed(anyString())).thenReturn(usedResources);
    when(app.getAppAttemptResourceUsage()).thenReturn(ru);
    when(app.getSchedulingResourceUsage()).thenReturn(ru);
  }

  private void addContainerToSchedulerNode(SchedulerNode node, RMContainer container,
      boolean isReserved) {
    assert node != null;

    if (isReserved) {
      when(node.getReservedContainer()).thenReturn(container);
    } else {
      node.getCopiedListOfRunningContainers().add(container);
      Resources.subtractFrom(node.getUnallocatedResource(),
          container.getAllocatedResource());
    }
  }
}
