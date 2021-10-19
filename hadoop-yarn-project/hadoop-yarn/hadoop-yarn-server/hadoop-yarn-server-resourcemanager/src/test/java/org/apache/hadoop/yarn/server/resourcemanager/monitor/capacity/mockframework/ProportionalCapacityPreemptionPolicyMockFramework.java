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

import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.TestProportionalCapacityPreemptionPolicyForNodePartitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.ArgumentMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.event.Event;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProportionalCapacityPreemptionPolicyMockFramework {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestProportionalCapacityPreemptionPolicyForNodePartitions.class);
  private static final double ALLOWED_CAPACITY_DELTA = 1e-3;

  private Map<String, CSQueue> nameToCSQueues;
  private Map<String, Resource> partitionToResource;
  private Map<NodeId, FiCaSchedulerNode> nodeIdToSchedulerNodes;
  private RMNodeLabelsManager nodeLabelsManager;
  public RMContext rmContext;

  public ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
  public Clock mClock;
  public CapacitySchedulerConfiguration conf;
  public CapacityScheduler cs;
  @SuppressWarnings("rawtypes")
  public EventHandler<Event> eventHandler;
  public ProportionalCapacityPreemptionPolicy policy;
  private Resource clusterResource;
  // Initialize resource map
  public Map<String, ResourceInformation> riMap = new HashMap<>();

  private void resetResourceInformationMap() {
    // Initialize mandatory resources
    ResourceInformation memory = ResourceInformation.newInstance(
        ResourceInformation.MEMORY_MB.getName(),
        ResourceInformation.MEMORY_MB.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    ResourceInformation vcores = ResourceInformation.newInstance(
        ResourceInformation.VCORES.getName(),
        ResourceInformation.VCORES.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    riMap.put(ResourceInformation.MEMORY_URI, memory);
    riMap.put(ResourceInformation.VCORES_URI, vcores);

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);
  }

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    resetResourceInformationMap();

    org.apache.log4j.Logger.getRootLogger().setLevel(
        org.apache.log4j.Level.DEBUG);

    conf = new CapacitySchedulerConfiguration(new Configuration(false));
    conf.setLong(
        CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL, 10000);
    conf.setLong(CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
        3000);
    // report "ideal" preempt
    conf.setFloat(CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
        (float) 1.0);
    conf.setFloat(
        CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR,
        (float) 1.0);

    mClock = mock(Clock.class);
    cs = mock(CapacityScheduler.class);
    when(cs.getResourceCalculator()).thenReturn(resourceCalculator);
    when(cs.getPreemptionManager()).thenReturn(new PreemptionManager());
    when(cs.getConfiguration()).thenReturn(conf);

    nodeLabelsManager = mock(RMNodeLabelsManager.class);
    eventHandler = mock(EventHandler.class);

    rmContext = mock(RMContext.class);
    when(rmContext.getNodeLabelManager()).thenReturn(nodeLabelsManager);
    Dispatcher dispatcher = mock(Dispatcher.class);
    when(rmContext.getDispatcher()).thenReturn(dispatcher);
    when(dispatcher.getEventHandler()).thenReturn(eventHandler);
    when(cs.getRMContext()).thenReturn(rmContext);

    partitionToResource = new HashMap<>();
    nodeIdToSchedulerNodes = new HashMap<>();
    nameToCSQueues = new HashMap<>();
    clusterResource = Resource.newInstance(0, 0);
  }

  @After
  public void cleanup() {
    resetResourceInformationMap();
  }

  public void buildEnv(String labelsConfig, String nodesConfig,
      String queuesConfig, String appsConfig) throws IOException {
    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig, false);
  }

  public void buildEnv(String labelsConfig, String nodesConfig,
      String queuesConfig, String appsConfig,
      boolean useDominantResourceCalculator) throws IOException {
    if (useDominantResourceCalculator) {
      when(cs.getResourceCalculator()).thenReturn(
          new DominantResourceCalculator());
    }

    MockNodeLabelsManager mockNodeLabelsManager =
        new MockNodeLabelsManager(labelsConfig,
            nodeLabelsManager, partitionToResource);
    clusterResource = mockNodeLabelsManager.getClusterResource();

    MockSchedulerNodes mockSchedulerNodes =
        new MockSchedulerNodes(nodesConfig);
    nodeIdToSchedulerNodes = mockSchedulerNodes.getNodeIdToSchedulerNodes();
    addNodeIdDataToScheduler();

    ParentQueue root = parseQueueConfig(queuesConfig);

    when(cs.getRootQueue()).thenReturn(root);
    when(cs.getClusterResource()).thenReturn(clusterResource);
    new MockApplications(appsConfig, resourceCalculator, nameToCSQueues,
        partitionToResource, nodeIdToSchedulerNodes);

    policy = new ProportionalCapacityPreemptionPolicy(rmContext, cs, mClock);
  }

  private ParentQueue parseQueueConfig(String queuesConfig) {
    MockQueueHierarchy mockQueueHierarchy =
        new MockQueueHierarchy(queuesConfig, cs, conf, resourceCalculator,
            partitionToResource);
    this.nameToCSQueues = mockQueueHierarchy.getNameToCSQueues();
    return mockQueueHierarchy.getRootQueue();
  }

  private void addNodeIdDataToScheduler() {
    for (NodeId nodeId : nodeIdToSchedulerNodes.keySet()) {
      when(cs.getSchedulerNode(nodeId)).thenReturn(
          nodeIdToSchedulerNodes.get(nodeId));
    }
    List<FiCaSchedulerNode> allNodes = new ArrayList<>(
        nodeIdToSchedulerNodes.values());
    when(cs.getAllNodes()).thenReturn(allNodes);
  }

  protected void updateQueueConfig(String queuesConfig) {
    ParentQueue root = parseQueueConfig(queuesConfig);
    when(cs.getRootQueue()).thenReturn(root);
  }

  //TODO this can probably be replaced with some parser logic already implemented somewhere
  static Resource parseResourceFromString(String resString) {
    String[] resource = resString.split(":");
    Resource res;
    if (resource.length == 1) {
      res = Resources.createResource(Integer.valueOf(resource[0]));
    } else {
      res = Resources.createResource(Integer.valueOf(resource[0]),
          Integer.valueOf(resource[1]));
      if (resource.length > 2) {
        // Using the same order of resources from ResourceUtils, set resource
        // information.
        ResourceInformation[] storedResourceInfo = ResourceUtils
            .getResourceTypesArray();
        for (int i = 2; i < resource.length; i++) {
          res.setResourceInformation(storedResourceInfo[i].getName(),
              ResourceInformation.newInstance(storedResourceInfo[i].getName(),
                  storedResourceInfo[i].getUnits(),
                  Integer.valueOf(resource[i])));
        }
      }
    }
    return res;
  }

  public ApplicationAttemptId getAppAttemptId(int id) {
    ApplicationId appId = ApplicationId.newInstance(0L, id);
    return ApplicationAttemptId.newInstance(appId, 1);
  }

  protected void checkContainerNodesInApp(FiCaSchedulerApp app,
      int expectedContainersNumber, String host) {
    NodeId nodeId = NodeId.newInstance(host, 1);
    int num = 0;
    for (RMContainer c : app.getLiveContainers()) {
      if (c.getAllocatedNode().equals(nodeId)) {
        num++;
      }
    }
    for (RMContainer c : app.getReservedContainers()) {
      if (c.getAllocatedNode().equals(nodeId)) {
        num++;
      }
    }
    Assert.assertEquals(expectedContainersNumber, num);
  }

  public FiCaSchedulerApp getApp(String queueName, int appId) {
    for (FiCaSchedulerApp app : ((LeafQueue) cs.getQueue(queueName))
        .getApplications()) {
      if (app.getApplicationId().getId() == appId) {
        return app;
      }
    }
    return null;
  }

  protected void checkAbsCapacities(CSQueue queue, String partition,
      float guaranteed, float max, float used) {
    QueueCapacities qc = queue.getQueueCapacities();
    Assert.assertEquals(guaranteed, qc.getAbsoluteCapacity(partition),
        ALLOWED_CAPACITY_DELTA);
    Assert.assertEquals(max, qc.getAbsoluteMaximumCapacity(partition),
        ALLOWED_CAPACITY_DELTA);
    Assert.assertEquals(used, qc.getAbsoluteUsedCapacity(partition),
        ALLOWED_CAPACITY_DELTA);
  }

  protected void checkPendingResource(CSQueue queue, String partition,
      int pending) {
    ResourceUsage ru = queue.getQueueResourceUsage();
    Assert.assertEquals(pending, ru.getPending(partition).getMemorySize());
  }

  protected void checkPriority(CSQueue queue, int expectedPriority) {
    Assert.assertEquals(expectedPriority, queue.getPriority().getPriority());
  }

  protected void checkReservedResource(CSQueue queue, String partition,
      int reserved) {
    ResourceUsage ru = queue.getQueueResourceUsage();
    Assert.assertEquals(reserved, ru.getReserved(partition).getMemorySize());
  }

  public static class IsPreemptionRequestForQueueAndNode
      implements ArgumentMatcher<ContainerPreemptEvent> {
    private final ApplicationAttemptId appAttId;
    private final String queueName;
    private final NodeId nodeId;

    public IsPreemptionRequestForQueueAndNode(ApplicationAttemptId appAttId,
        String queueName, NodeId nodeId) {
      this.appAttId = appAttId;
      this.queueName = queueName;
      this.nodeId = nodeId;
    }
    @Override
    public boolean matches(ContainerPreemptEvent cpe) {
      return appAttId.equals(cpe.getAppId())
          && queueName.equals(cpe.getContainer().getQueueName())
          && nodeId.equals(cpe.getContainer().getAllocatedNode());
    }
    @Override
    public String toString() {
      return appAttId.toString();
    }
  }
}
