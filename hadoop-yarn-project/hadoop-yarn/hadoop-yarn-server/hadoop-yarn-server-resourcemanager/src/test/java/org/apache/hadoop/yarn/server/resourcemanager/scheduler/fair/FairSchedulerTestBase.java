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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;


import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.junit.Assert;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FairSchedulerTestBase {
  public final static String TEST_DIR =
      new File(System.getProperty("test.build.data", "/tmp")).getAbsolutePath();

  private static RecordFactory
      recordFactory = RecordFactoryProvider.getRecordFactory(null);

  protected int APP_ID = 1; // Incrementing counter for scheduling apps
  protected int ATTEMPT_ID = 1; // Incrementing counter for scheduling attempts

  protected Configuration conf;
  protected FairScheduler scheduler;
  protected ResourceManager resourceManager;
  public static final float TEST_RESERVATION_THRESHOLD = 0.09f;
  private static final int SLEEP_DURATION = 10;
  private static final int SLEEP_RETRIES = 1000;
  protected static final int RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE = 10240;
  final static ContainerUpdates NULL_UPDATE_REQUESTS =
      new ContainerUpdates();

  /**
   * The list of nodes added to the cluster using the {@link #addNode} method.
   */
  protected final List<RMNode> rmNodes = new ArrayList<>();

  // Helper methods
  public Configuration createConfiguration() {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
    conf.setInt(FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB,
        1024);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE);
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, false);
    conf.setLong(FairSchedulerConfiguration.UPDATE_INTERVAL_MS, 10);
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD, 0f);

    conf.setFloat(
        FairSchedulerConfiguration
           .RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE,
        TEST_RESERVATION_THRESHOLD);
    return conf;
  }

  protected ApplicationAttemptId createAppAttemptId(int appId, int attemptId) {
    ApplicationId appIdImpl = ApplicationId.newInstance(0, appId);
    return ApplicationAttemptId.newInstance(appIdImpl, attemptId);
  }

  protected ResourceRequest createResourceRequest(
      int memory, String host, int priority, int numContainers,
      boolean relaxLocality) {
    return createResourceRequest(memory, 1, host, priority, numContainers,
        relaxLocality);
  }

  protected ResourceRequest createResourceRequest(
      int memory, int vcores, String host, int priority, int numContainers,
      boolean relaxLocality) {
    ResourceRequest request = recordFactory.newRecordInstance(ResourceRequest.class);
    request.setCapability(BuilderUtils.newResource(memory, vcores));
    request.setResourceName(host);
    request.setNumContainers(numContainers);
    Priority prio = recordFactory.newRecordInstance(Priority.class);
    prio.setPriority(priority);
    request.setPriority(prio);
    request.setRelaxLocality(relaxLocality);
    request.setNodeLabelExpression(RMNodeLabelsManager.NO_LABEL);
    return request;
  }

  /**
   * Creates a single container priority-1 request and submits to
   * scheduler.
   */
  protected ApplicationAttemptId createSchedulingRequest(
      int memory, String queueId, String userId) {
    return createSchedulingRequest(memory, queueId, userId, 1);
  }

  protected ApplicationAttemptId createSchedulingRequest(
      int memory, int vcores, String queueId, String userId) {
    return createSchedulingRequest(memory, vcores, queueId, userId, 1);
  }

  protected ApplicationAttemptId createSchedulingRequest(
      int memory, String queueId, String userId, int numContainers) {
    return createSchedulingRequest(memory, queueId, userId, numContainers, 1);
  }

  protected ApplicationAttemptId createSchedulingRequest(
      int memory, int vcores, String queueId, String userId, int numContainers) {
    return createSchedulingRequest(memory, vcores, queueId, userId, numContainers, 1);
  }

  protected ApplicationAttemptId createSchedulingRequest(
      int memory, String queueId, String userId, int numContainers, int priority) {
    return createSchedulingRequest(memory, 1, queueId, userId, numContainers,
        priority);
  }

  protected ApplicationAttemptId createSchedulingRequest(
      int memory, int vcores, String queueId, String userId, int numContainers,
      int priority) {
    ResourceRequest request = createResourceRequest(memory, vcores,
            ResourceRequest.ANY, priority, numContainers, true);
    return createSchedulingRequest(Lists.newArrayList(request), queueId,
            userId);
  }

  protected ApplicationAttemptId createSchedulingRequest(
      Collection<ResourceRequest> requests, String queueId, String userId) {
    ApplicationAttemptId id =
        createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    // This fakes the placement which is not part of the scheduler anymore
    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext(queueId);
    scheduler.addApplication(id.getApplicationId(), queueId, userId, false,
        placementCtx);
    // This conditional is for testAclSubmitApplication where app is rejected
    // and no app is added.
    if (scheduler.getSchedulerApplications()
        .containsKey(id.getApplicationId())) {
      scheduler.addApplicationAttempt(id, false, false);
    }

    List<ResourceRequest> ask = new ArrayList<>(requests);

    RMApp rmApp = mock(RMApp.class);
    RMAppAttempt rmAppAttempt = mock(RMAppAttempt.class);
    when(rmApp.getCurrentAppAttempt()).thenReturn(rmAppAttempt);
    when(rmAppAttempt.getRMAppAttemptMetrics()).thenReturn(
            new RMAppAttemptMetrics(id, resourceManager.getRMContext()));
    ApplicationSubmissionContext submissionContext =
            mock(ApplicationSubmissionContext.class);
    when(submissionContext.getUnmanagedAM()).thenReturn(false);
    when(rmAppAttempt.getSubmissionContext()).thenReturn(submissionContext);
    when(rmApp.getApplicationSubmissionContext()).thenReturn(submissionContext);
    Container container = mock(Container.class);
    when(rmAppAttempt.getMasterContainer()).thenReturn(container);
    resourceManager.getRMContext().getRMApps()
            .put(id.getApplicationId(), rmApp);

    scheduler.allocate(id, ask, null, new ArrayList<>(),
            null, null, NULL_UPDATE_REQUESTS);
    scheduler.update();
    return id;
  }
  
  protected ApplicationAttemptId createSchedulingRequest(String queueId,
      String userId, List<ResourceRequest> ask) {
    ApplicationAttemptId id = createAppAttemptId(this.APP_ID++,
        this.ATTEMPT_ID++);
    // This fakes the placement which is not part of the scheduler anymore
    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext(queueId);
    scheduler.addApplication(id.getApplicationId(), queueId, userId, false,
        placementCtx);
    // This conditional is for testAclSubmitApplication where app is rejected
    // and no app is added.
    if (scheduler.getSchedulerApplications().containsKey(
        id.getApplicationId())) {
      scheduler.addApplicationAttempt(id, false, false);
    }

    RMApp rmApp = mock(RMApp.class);
    RMAppAttempt rmAppAttempt = mock(RMAppAttempt.class);
    when(rmApp.getCurrentAppAttempt()).thenReturn(rmAppAttempt);
    when(rmAppAttempt.getRMAppAttemptMetrics()).thenReturn(
        new RMAppAttemptMetrics(id,resourceManager.getRMContext()));
    ApplicationSubmissionContext submissionContext =
        mock(ApplicationSubmissionContext.class);
    when(submissionContext.getUnmanagedAM()).thenReturn(false);
    when(rmAppAttempt.getSubmissionContext()).thenReturn(submissionContext);
    when(rmApp.getApplicationSubmissionContext()).thenReturn(submissionContext);
    resourceManager.getRMContext().getRMApps()
        .put(id.getApplicationId(), rmApp);

    scheduler.allocate(id, ask, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);
    return id;
  }

  protected void createSchedulingRequestExistingApplication(
       int memory, int priority, ApplicationAttemptId attId) {
    ResourceRequest request = createResourceRequest(memory, ResourceRequest.ANY,
        priority, 1, true);
    createSchedulingRequestExistingApplication(request, attId);
  }

  protected void createSchedulingRequestExistingApplication(
      int memory, int vcores, int priority, ApplicationAttemptId attId) {
    ResourceRequest request = createResourceRequest(memory, vcores, ResourceRequest.ANY,
        priority, 1, true);
    createSchedulingRequestExistingApplication(request, attId);
  }

  protected void createSchedulingRequestExistingApplication(
      ResourceRequest request, ApplicationAttemptId attId) {
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ask.add(request);
    scheduler.allocate(attId, ask, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);
    scheduler.update();
  }

  protected ApplicationAttemptId createRecoveringApplication(
      Resource amResource, String queueId, String userId) {
    ApplicationAttemptId id =
        createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);

    // On restore the app is already created but we need to check the AM
    // resource, make sure it is set for test
    ResourceRequest amRequest = createResourceRequest(
        // cast to int as we're not testing large values so it is safe
        (int)amResource.getMemorySize(), amResource.getVirtualCores(),
        ResourceRequest.ANY, 1, 1, true);
    List<ResourceRequest> amReqs = new ArrayList<>();
    amReqs.add(amRequest);
    createApplicationWithAMResourceInternal(id, queueId, userId, amResource,
        amReqs);

    // This fakes the placement which is not part of the scheduler anymore
    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext(queueId);
    scheduler.addApplication(id.getApplicationId(), queueId, userId, true,
        placementCtx);
    return id;
  }

  protected void createApplicationWithAMResource(ApplicationAttemptId attId,
      String queue, String user, Resource amResource) {
    createApplicationWithAMResourceInternal(attId, queue, user, amResource,
        null);
    ApplicationId appId = attId.getApplicationId();
    addApplication(queue, user, appId);
    addAppAttempt(attId);
  }

  protected void createApplicationWithAMResource(ApplicationAttemptId attId,
      String queue, String user, Resource amResource,
      List<ResourceRequest> amReqs) {
    createApplicationWithAMResourceInternal(attId, queue, user, amResource,
        amReqs);
    ApplicationId appId = attId.getApplicationId();
    addApplication(queue, user, appId);
  }

  private void createApplicationWithAMResourceInternal(
      ApplicationAttemptId attId, String queue, String user,
      Resource amResource, List<ResourceRequest> amReqs) {
    RMContext rmContext = resourceManager.getRMContext();
    ApplicationId appId = attId.getApplicationId();
    // This fakes the placement which is not part of the scheduler anymore
    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext(queue);
    // Set the placement in the app and not just in the event in the next call
    // otherwise with out of order event processing we might remove the app.
    RMApp rmApp = new RMAppImpl(appId, rmContext, conf, null, user, null,
        ApplicationSubmissionContext.newInstance(appId, null, queue, null,
            mock(ContainerLaunchContext.class), false, false, 0, amResource,
            null),
        scheduler, null, 0, null, null, amReqs, placementCtx, -1);
    rmContext.getRMApps().put(appId, rmApp);
  }

  private void addApplication(String queue, String user, ApplicationId appId) {
    RMAppEvent event = new RMAppEvent(appId, RMAppEventType.START);
    resourceManager.getRMContext().getRMApps().get(appId).handle(event);
    event = new RMAppEvent(appId, RMAppEventType.APP_NEW_SAVED);
    resourceManager.getRMContext().getRMApps().get(appId).handle(event);
    event = new RMAppEvent(appId, RMAppEventType.APP_ACCEPTED);
    resourceManager.getRMContext().getRMApps().get(appId).handle(event);
    // This fakes the placement which is not part of the scheduler anymore
    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext(queue);
    AppAddedSchedulerEvent appAddedEvent = new AppAddedSchedulerEvent(
        appId, queue, user, placementCtx);
    scheduler.handle(appAddedEvent);
  }

  private void addAppAttempt(ApplicationAttemptId attId) {
    AppAttemptAddedSchedulerEvent attempAddedEvent =
            new AppAttemptAddedSchedulerEvent(attId, false);
    scheduler.handle(attempAddedEvent);
  }

  protected RMApp createMockRMApp(ApplicationAttemptId attemptId) {
    RMApp app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(attemptId.getApplicationId());
    RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
    when(attempt.getAppAttemptId()).thenReturn(attemptId);
    RMAppAttemptMetrics attemptMetric = mock(RMAppAttemptMetrics.class);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);
    ApplicationSubmissionContext submissionContext =
        mock(ApplicationSubmissionContext.class);
    when(submissionContext.getUnmanagedAM()).thenReturn(false);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    when(app.getApplicationSubmissionContext()).thenReturn(submissionContext);
    resourceManager.getRMContext().getRMApps()
        .put(attemptId.getApplicationId(), app);
    return app;
  }

  protected void checkAppConsumption(FSAppAttempt app, Resource resource)
      throws InterruptedException {
    for (int i = 0; i < SLEEP_RETRIES; i++) {
      if (Resources.equals(resource, app.getCurrentConsumption())) {
        break;
      } else {
        Thread.sleep(SLEEP_DURATION);
      }
    }

    // available resource
    Assert.assertEquals(resource.getMemorySize(),
        app.getCurrentConsumption().getMemorySize());
    Assert.assertEquals(resource.getVirtualCores(),
        app.getCurrentConsumption().getVirtualCores());
  }

  /**
   * Add a node to the cluster and track the nodes in {@link #rmNodes}.
   * @param memory memory capacity of the node
   * @param cores cpu capacity of the node
   */
  protected void addNode(int memory, int cores) {
    int id = rmNodes.size() + 1;
    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(memory, cores), id,
            "127.0.0." + id);
    scheduler.handle(new NodeAddedSchedulerEvent(node));
    rmNodes.add(node);
  }
}
