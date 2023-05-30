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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.server.resourcemanager.security.AppPriorityACLsManager;
import org.junit.Assert;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfAmbiguousQueue;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfiguration;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class CapacitySchedulerTestUtilities {
  public static final int GB = 1024;

  private CapacitySchedulerTestUtilities() {
  }

  public static CapacitySchedulerQueueManager setQueueHandler(CapacitySchedulerContext cs) {
    CapacitySchedulerQueueManager queueManager = new CapacitySchedulerQueueManager(
        cs.getConfiguration(), cs.getRMContext().getNodeLabelManager(),
        new AppPriorityACLsManager(cs.getConfiguration()));
    when(cs.getCapacitySchedulerQueueManager()).thenReturn(queueManager);
    return queueManager;
  }

  @SuppressWarnings("unchecked")
  public static <E> Set<E> toSet(E... elements) {
    return Sets.newHashSet(elements);
  }

  public static void checkPendingResource(MockRM rm, String queueName, int memory,
      String label) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertEquals(
        memory,
        queue.getQueueResourceUsage()
            .getPending(label == null ? RMNodeLabelsManager.NO_LABEL : label)
            .getMemorySize());
  }


  public static void checkPendingResourceGreaterThanZero(MockRM rm, String queueName,
      String label) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertTrue(queue.getQueueResourceUsage()
        .getPending(label == null ? RMNodeLabelsManager.NO_LABEL : label)
        .getMemorySize() > 0);
  }

  public static void waitforNMRegistered(ResourceScheduler scheduler, int nodecount,
      int timesec) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timesec * 1000) {
      if (scheduler.getNumClusterNodes() < nodecount) {
        Thread.sleep(100);
      } else {
        break;
      }
    }
  }

  public static ResourceManager createResourceManager() throws Exception {
    ResourceUtils.resetResourceTypes(new Configuration());
    DefaultMetricsSystem.setMiniClusterMode(true);
    ResourceManager resourceManager = new ResourceManager() {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };
    CapacitySchedulerConfiguration csConf
        = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class, ResourceScheduler.class);
    resourceManager.init(conf);
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();
    ((AsyncDispatcher) resourceManager.getRMContext().getDispatcher()).start();
    return resourceManager;
  }

  public static RMContext createMockRMContext() {
    RMContext mockContext = mock(RMContext.class);
    when(mockContext.getConfigurationProvider()).thenReturn(
        new LocalConfigurationProvider());
    return mockContext;
  }

  public static void stopResourceManager(ResourceManager resourceManager) throws Exception {
    if (resourceManager != null) {
      QueueMetrics.clearQueueMetrics();
      DefaultMetricsSystem.shutdown();
      resourceManager.stop();
    }
  }

  public static ApplicationAttemptId appHelper(MockRM rm, CapacityScheduler cs,
                                         int clusterTs, int appId, String queue,
                                         String user) {
    ApplicationId appId1 = BuilderUtils.newApplicationId(clusterTs, appId);
    ApplicationAttemptId appAttemptId1 = BuilderUtils.newApplicationAttemptId(
        appId1, appId);

    RMAppAttemptMetrics attemptMetric1 =
        new RMAppAttemptMetrics(appAttemptId1, rm.getRMContext());
    RMAppImpl app1 = mock(RMAppImpl.class);
    when(app1.getApplicationId()).thenReturn(appId1);
    RMAppAttemptImpl attempt1 = mock(RMAppAttemptImpl.class);
    Container container = mock(Container.class);
    when(attempt1.getMasterContainer()).thenReturn(container);
    ApplicationSubmissionContext submissionContext = mock(
        ApplicationSubmissionContext.class);
    when(attempt1.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt1.getAppAttemptId()).thenReturn(appAttemptId1);
    when(attempt1.getRMAppAttemptMetrics()).thenReturn(attemptMetric1);
    when(app1.getCurrentAppAttempt()).thenReturn(attempt1);
    rm.getRMContext().getRMApps().put(appId1, app1);

    SchedulerEvent addAppEvent1 =
        new AppAddedSchedulerEvent(appId1, queue, user);
    cs.handle(addAppEvent1);
    SchedulerEvent addAttemptEvent1 =
        new AppAttemptAddedSchedulerEvent(appAttemptId1, false);
    cs.handle(addAttemptEvent1);
    return appAttemptId1;
  }

  public static MockRM setUpMoveAmbiguousQueue() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfAmbiguousQueue(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    return rm;
  }

  public static MockRM setUpMove() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    return setUpMove(conf);
  }

  public static MockRM setUpMove(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    return rm;
  }

  public static void nodeUpdate(ResourceManager rm, NodeManager nm) {
    RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    rm.getResourceScheduler().handle(nodeUpdate);
  }

  public static NodeManager registerNode(ResourceManager rm, String hostName,
                                   int containerManagerPort, int httpPort, String rackName,
                                   Resource capability, NodeStatus nodeStatus)
      throws IOException, YarnException {
    NodeManager nm = new NodeManager(hostName,
        containerManagerPort, httpPort, rackName, capability, rm, nodeStatus);
    NodeAddedSchedulerEvent nodeAddEvent1 =
        new NodeAddedSchedulerEvent(rm.getRMContext().getRMNodes()
            .get(nm.getNodeId()));
    rm.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }

  public static void checkApplicationResourceUsage(int expected, Application application) {
    Assert.assertEquals(expected, application.getUsedResources().getMemorySize());
  }

  public static void checkNodeResourceUsage(int expected, NodeManager node) {
    Assert.assertEquals(expected, node.getUsed().getMemorySize());
    node.checkResourceUsage();
  }

  public static CapacityScheduler setupCapacityScheduler(MockRM rm, int memoryGb) {
    return setupCapacityScheduler(rm, memoryGb, 16);
  }

  public static CapacityScheduler setupCapacityScheduler(MockRM rm, int memoryGb, int cores) {
    return setupCapacityScheduler(
        rm,
        Resource.newInstance(memoryGb * GB, cores),
        Collections.emptyMap()
    );
  }

  public static CapacityScheduler setupCapacityScheduler(MockRM rm, Resource resource) {
    return setupCapacityScheduler(rm, resource, Collections.emptyMap());
  }

  public static CapacityScheduler setupCapacityScheduler(
      MockRM rm, Resource resource, Map<String, String> nameToValues
  ) {
    if(!(rm.getResourceScheduler() instanceof CapacityScheduler)) {
      return null;
    }
    RMNodeLabelsManager mgr = rm.getRMContext().getNodeLabelManager();
    CapacitySchedulerQueueCapacityHandler queueController =
        new CapacitySchedulerQueueCapacityHandler(mgr);
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    Map<String, Long> others = new HashMap<>(nameToValues.size());
    for (Map.Entry<String, String> nameToValue : nameToValues.entrySet()) {
      others.put(nameToValue.getKey(), Long.valueOf(nameToValue.getValue()));
    }
    Resource clusterResource = Resource.newInstance(
        resource.getMemorySize(), resource.getVirtualCores(), others);
    if (mgr instanceof NullRMNodeLabelsManager) {
      ((NullRMNodeLabelsManager) mgr).setResourceForLabel(CommonNodeLabelsManager.NO_LABEL, clusterResource);
    }
    queueController.updateRoot(cs.getQueue("root"), clusterResource);
    updateChildren(queueController, clusterResource, cs.getQueue("root"));
    return cs;
  }

  public static void updateCSQueues(
      RMNodeLabelsManager nodeLabelsManager, CSQueue root, Resource clusterResource
  ) {
    CapacitySchedulerQueueCapacityHandler queueController =
        new CapacitySchedulerQueueCapacityHandler(nodeLabelsManager);
    queueController.updateRoot(root, clusterResource);
    updateChildren(queueController, clusterResource, root);
  }


  private static void updateChildren(
      CapacitySchedulerQueueCapacityHandler queueController,
      Resource clusterResource,
      CSQueue queue
  ) {
    queueController.updateChildren(clusterResource, queue);
    List<CSQueue> childs = queue.getChildQueues();
    if (childs == null) {
      return;
    }
    for (CSQueue child : childs) {
      updateChildren(queueController, clusterResource, child);
    }
  }
}
