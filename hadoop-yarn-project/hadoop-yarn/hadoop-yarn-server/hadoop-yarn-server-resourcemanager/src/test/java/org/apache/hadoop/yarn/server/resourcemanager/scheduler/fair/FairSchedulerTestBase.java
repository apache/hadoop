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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Clock;

public class FairSchedulerTestBase {
  protected static class MockClock implements Clock {
    private long time = 0;
    @Override
    public long getTime() {
      return time;
    }

    public void tick(int seconds) {
      time = time + seconds * 1000;
    }
  }

  public final static String TEST_DIR =
      new File(System.getProperty("test.build.data", "/tmp")).getAbsolutePath();

  private static RecordFactory
      recordFactory = RecordFactoryProvider.getRecordFactory(null);

  protected int APP_ID = 1; // Incrementing counter for scheduling apps
  protected int ATTEMPT_ID = 1; // Incrementing counter for scheduling attempts

  protected Configuration conf;
  protected FairScheduler scheduler;
  protected ResourceManager resourceManager;

  // Helper methods
  public Configuration createConfiguration() {
    Configuration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
    conf.setInt(FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB,
        1024);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 10240);
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, false);
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD, 0f);
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
    ApplicationAttemptId id = createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    scheduler.addApplication(id.getApplicationId(), queueId, userId, false);
    // This conditional is for testAclSubmitApplication where app is rejected
    // and no app is added.
    if (scheduler.getSchedulerApplications().containsKey(id.getApplicationId())) {
      scheduler.addApplicationAttempt(id, false, false);
    }
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest request = createResourceRequest(memory, vcores, ResourceRequest.ANY,
        priority, numContainers, true);
    ask.add(request);

    RMApp rmApp = mock(RMApp.class);
    RMAppAttempt rmAppAttempt = mock(RMAppAttempt.class);
    when(rmApp.getCurrentAppAttempt()).thenReturn(rmAppAttempt);
    when(rmAppAttempt.getRMAppAttemptMetrics()).thenReturn(
        new RMAppAttemptMetrics(id, resourceManager.getRMContext()));
    resourceManager.getRMContext().getRMApps()
        .put(id.getApplicationId(), rmApp);

    scheduler.allocate(id, ask, new ArrayList<ContainerId>(), null, null);
    return id;
  }
  
  protected ApplicationAttemptId createSchedulingRequest(String queueId,
      String userId, List<ResourceRequest> ask) {
    ApplicationAttemptId id = createAppAttemptId(this.APP_ID++,
        this.ATTEMPT_ID++);
    scheduler.addApplication(id.getApplicationId(), queueId, userId, false);
    // This conditional is for testAclSubmitApplication where app is rejected
    // and no app is added.
    if (scheduler.getSchedulerApplications().containsKey(id.getApplicationId())) {
      scheduler.addApplicationAttempt(id, false, false);
    }

    RMApp rmApp = mock(RMApp.class);
    RMAppAttempt rmAppAttempt = mock(RMAppAttempt.class);
    when(rmApp.getCurrentAppAttempt()).thenReturn(rmAppAttempt);
    when(rmAppAttempt.getRMAppAttemptMetrics()).thenReturn(
        new RMAppAttemptMetrics(id,resourceManager.getRMContext()));
    resourceManager.getRMContext().getRMApps()
        .put(id.getApplicationId(), rmApp);

    scheduler.allocate(id, ask, new ArrayList<ContainerId>(), null, null);
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
    scheduler.allocate(attId, ask,  new ArrayList<ContainerId>(), null, null);
  }

  protected void createApplicationWithAMResource(ApplicationAttemptId attId,
      String queue, String user, Resource amResource) {
    RMContext rmContext = resourceManager.getRMContext();
    RMApp rmApp = new RMAppImpl(attId.getApplicationId(), rmContext, conf,
        null, null, null, ApplicationSubmissionContext.newInstance(null, null,
        null, null, null, false, false, 0, amResource, null), null, null,
        0, null, null, null);
    rmContext.getRMApps().put(attId.getApplicationId(), rmApp);
    AppAddedSchedulerEvent appAddedEvent = new AppAddedSchedulerEvent(
        attId.getApplicationId(), queue, user);
    scheduler.handle(appAddedEvent);
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
    resourceManager.getRMContext().getRMApps()
        .put(attemptId.getApplicationId(), app);
    return app;
  }
}