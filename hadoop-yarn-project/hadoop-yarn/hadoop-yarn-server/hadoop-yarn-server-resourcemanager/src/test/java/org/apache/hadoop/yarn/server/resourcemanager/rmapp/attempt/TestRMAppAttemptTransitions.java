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
package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import static org.apache.hadoop.yarn.util.StringHelper.pjoin;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.InlineDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFailedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAllocatedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRMAppAttemptTransitions {

  private static final Log LOG = 
      LogFactory.getLog(TestRMAppAttemptTransitions.class);
  
  private static final String EMPTY_DIAGNOSTICS = "";
  private static final String RM_WEBAPP_ADDR =
      YarnConfiguration.getRMWebAppHostAndPort(new Configuration());
  
  private RMContext rmContext;
  private YarnScheduler scheduler;
  private ApplicationMasterService masterService;
  private ApplicationMasterLauncher applicationMasterLauncher;
  
  private RMApp application;
  private RMAppAttempt applicationAttempt;
  
  private final class TestApplicationAttemptEventDispatcher implements
      EventHandler<RMAppAttemptEvent> {

    @Override
    public void handle(RMAppAttemptEvent event) {
      ApplicationAttemptId appID = event.getApplicationAttemptId();
      assertEquals(applicationAttempt.getAppAttemptId(), appID);
      try {
        applicationAttempt.handle(event);
      } catch (Throwable t) {
        LOG.error("Error in handling event type " + event.getType()
            + " for application " + appID, t);
      }
    }
  }

  // handle all the RM application events - same as in ResourceManager.java
  private final class TestApplicationEventDispatcher implements
      EventHandler<RMAppEvent> {
    @Override
    public void handle(RMAppEvent event) {
      assertEquals(application.getApplicationId(), event.getApplicationId());
      try {
        application.handle(event);
      } catch (Throwable t) {
        LOG.error("Error in handling event type " + event.getType()
            + " for application " + application.getApplicationId(), t);
      }
    }
  }

  private final class TestSchedulerEventDispatcher implements
  EventHandler<SchedulerEvent> {
    @Override
    public void handle(SchedulerEvent event) {
      scheduler.handle(event);
    }
  }
  
  private final class TestAMLauncherEventDispatcher implements
  EventHandler<AMLauncherEvent> {
    @Override
    public void handle(AMLauncherEvent event) {
      applicationMasterLauncher.handle(event);
    }
  }
  
  private static int appId = 1;

  @Before
  public void setUp() throws Exception {
    InlineDispatcher rmDispatcher = new InlineDispatcher();
  
    ContainerAllocationExpirer containerAllocationExpirer =
        mock(ContainerAllocationExpirer.class);
    AMLivelinessMonitor amLivelinessMonitor = mock(AMLivelinessMonitor.class);
    Configuration conf = new Configuration();
    rmContext =
        new RMContextImpl(new MemStore(), rmDispatcher,
          containerAllocationExpirer, amLivelinessMonitor, null,
          new ApplicationTokenSecretManager(conf),
          new RMContainerTokenSecretManager(conf));
    
    scheduler = mock(YarnScheduler.class);
    masterService = mock(ApplicationMasterService.class);
    applicationMasterLauncher = mock(ApplicationMasterLauncher.class);
    
    rmDispatcher.register(RMAppAttemptEventType.class,
        new TestApplicationAttemptEventDispatcher());
  
    rmDispatcher.register(RMAppEventType.class,
        new TestApplicationEventDispatcher());
    
    rmDispatcher.register(SchedulerEventType.class, 
        new TestSchedulerEventDispatcher());
    
    rmDispatcher.register(AMLauncherEventType.class, 
        new TestAMLauncherEventDispatcher());

    rmDispatcher.init(conf);
    rmDispatcher.start();
    

    ApplicationId applicationId = MockApps.newAppID(appId++);
    ApplicationAttemptId applicationAttemptId = 
        MockApps.newAppAttemptID(applicationId, 0);
    
    final String user = MockApps.newUserName();
    final String queue = MockApps.newQueue();
    ApplicationSubmissionContext submissionContext = 
        mock(ApplicationSubmissionContext.class);
    when(submissionContext.getUser()).thenReturn(user);
    when(submissionContext.getQueue()).thenReturn(queue);
    ContainerLaunchContext amContainerSpec = mock(ContainerLaunchContext.class);
    Resource resource = mock(Resource.class);
    when(amContainerSpec.getResource()).thenReturn(resource);
    when(submissionContext.getAMContainerSpec()).thenReturn(amContainerSpec);
    
    application = mock(RMApp.class);
    applicationAttempt = 
        new RMAppAttemptImpl(applicationAttemptId, null, rmContext, scheduler, 
            masterService, submissionContext, new Configuration());
    when(application.getCurrentAppAttempt()).thenReturn(applicationAttempt);
    when(application.getApplicationId()).thenReturn(applicationId);
    
    testAppAttemptNewState();
  }

  @After
  public void tearDown() throws Exception {
    ((AsyncDispatcher)this.rmContext.getDispatcher()).stop();
  }
  

  private String getProxyUrl(RMAppAttempt appAttempt) {
    return pjoin(RM_WEBAPP_ADDR, "proxy",
        appAttempt.getAppAttemptId().getApplicationId(), "");
  }

  /**
   * {@link RMAppAttemptState#NEW}
   */
  private void testAppAttemptNewState() {
    assertEquals(RMAppAttemptState.NEW, 
        applicationAttempt.getAppAttemptState());
    assertEquals(0, applicationAttempt.getDiagnostics().length());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertNull(applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, applicationAttempt.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
    assertNotNull(applicationAttempt.getTrackingUrl());
    assertFalse("N/A".equals(applicationAttempt.getTrackingUrl()));
  }

  /**
   * {@link RMAppAttemptState#SUBMITTED}
   */
  private void testAppAttemptSubmittedState() {
    assertEquals(RMAppAttemptState.SUBMITTED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(0, applicationAttempt.getDiagnostics().length());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertNull(applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, applicationAttempt.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
    
    // Check events
    verify(masterService).
        registerAppAttempt(applicationAttempt.getAppAttemptId());
    verify(scheduler).handle(any(AppAddedSchedulerEvent.class));
  }

  /**
   * {@link RMAppAttemptState#SUBMITTED} -> {@link RMAppAttemptState#FAILED}
   */
  private void testAppAttemptSubmittedToFailedState(String diagnostics) {
    assertEquals(RMAppAttemptState.FAILED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertNull(applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, applicationAttempt.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
    
    // Check events
    verify(masterService).
        unregisterAttempt(applicationAttempt.getAppAttemptId());
    
    verify(application).handle(any(RMAppRejectedEvent.class));
  }

  /**
   * {@link RMAppAttemptState#KILLED}
   */
  private void testAppAttemptKilledState(Container amContainer, 
      String diagnostics) {
    assertEquals(RMAppAttemptState.KILLED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, applicationAttempt.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
  }

  /**
   * {@link RMAppAttemptState#SCHEDULED}
   */
  private void testAppAttemptScheduledState() {
    assertEquals(RMAppAttemptState.SCHEDULED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertNull(applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, applicationAttempt.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
    
    // Check events
    verify(application).handle(any(RMAppEvent.class));
    verify(scheduler).
        allocate(any(ApplicationAttemptId.class), 
            any(List.class), any(List.class));
  }

  /**
   * {@link RMAppAttemptState#ALLOCATED}
   */
  private void testAppAttemptAllocatedState(Container amContainer) {
    assertEquals(RMAppAttemptState.ALLOCATED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    
    // Check events
    verify(applicationMasterLauncher).handle(any(AMLauncherEvent.class));
    verify(scheduler, times(2)).
        allocate(
            any(ApplicationAttemptId.class), any(List.class), any(List.class));
  }
  
  /**
   * {@link RMAppAttemptState#FAILED}
   */
  private void testAppAttemptFailedState(Container container, 
      String diagnostics) {
    assertEquals(RMAppAttemptState.FAILED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertEquals(container, applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, applicationAttempt.getRanNodes().size());
    
    // Check events
    verify(application, times(2)).handle(any(RMAppFailedAttemptEvent.class));
  }

  /**
   * {@link RMAppAttemptState#LAUNCH}
   */
  private void testAppAttemptLaunchedState(Container container) {
    assertEquals(RMAppAttemptState.LAUNCHED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(container, applicationAttempt.getMasterContainer());
    
    // TODO - need to add more checks relevant to this state
  }

  /**
   * {@link RMAppAttemptState#RUNNING}
   */
  private void testAppAttemptRunningState(Container container,
      String host, int rpcPort, String trackingUrl) {
    assertEquals(RMAppAttemptState.RUNNING, 
        applicationAttempt.getAppAttemptState());
    assertEquals(container, applicationAttempt.getMasterContainer());
    assertEquals(host, applicationAttempt.getHost());
    assertEquals(rpcPort, applicationAttempt.getRpcPort());
    assertEquals(trackingUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(getProxyUrl(applicationAttempt),
        applicationAttempt.getTrackingUrl());
    
    // TODO - need to add more checks relevant to this state
  }

  /**
   * {@link RMAppAttemptState#FINISHED}
   */
  private void testAppAttemptFinishedState(Container container,
      FinalApplicationStatus finalStatus, 
      String trackingUrl, 
      String diagnostics) {
    assertEquals(RMAppAttemptState.FINISHED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    assertEquals(trackingUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(getProxyUrl(applicationAttempt),
        applicationAttempt.getTrackingUrl());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertEquals(container, applicationAttempt.getMasterContainer());
    assertEquals(finalStatus, applicationAttempt.getFinalApplicationStatus());
  }
  
  
  private void submitApplicationAttempt() {
    ApplicationAttemptId appAttemptId = applicationAttempt.getAppAttemptId();
    applicationAttempt.handle(
        new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.START));
    testAppAttemptSubmittedState();
  }

  private void scheduleApplicationAttempt() {
    submitApplicationAttempt();
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.APP_ACCEPTED));
    testAppAttemptScheduledState();
  }

  private Container allocateApplicationAttempt() {
    scheduleApplicationAttempt();
    
    // Mock the allocation of AM container 
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(
        BuilderUtils.newContainerId(applicationAttempt.getAppAttemptId(), 1));
    Allocation allocation = mock(Allocation.class);
    when(allocation.getContainers()).
        thenReturn(Collections.singletonList(container));
    when(
        scheduler.allocate(
            any(ApplicationAttemptId.class), 
            any(List.class), 
            any(List.class))).
    thenReturn(allocation);
    
    applicationAttempt.handle(
        new RMAppAttemptContainerAllocatedEvent(
            applicationAttempt.getAppAttemptId(), 
            container));
    
    testAppAttemptAllocatedState(container);
    
    return container;
  }
  
  private void launchApplicationAttempt(Container container) {
    applicationAttempt.handle(
        new RMAppAttemptEvent(applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.LAUNCHED));

    testAppAttemptLaunchedState(container);    
  }
  
  private void runApplicationAttempt(Container container,
      String host, 
      int rpcPort, 
      String trackingUrl) {
    applicationAttempt.handle(
        new RMAppAttemptRegistrationEvent(
            applicationAttempt.getAppAttemptId(),
            host, rpcPort, trackingUrl));
    
    testAppAttemptRunningState(container, host, rpcPort, trackingUrl);
  }
    
  @Test
  public void testNewToKilled() {
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    testAppAttemptKilledState(null, EMPTY_DIAGNOSTICS);
  } 
  
  @Test
  public void testSubmittedToFailed() {
    submitApplicationAttempt();
    String message = "Rejected";
    applicationAttempt.handle(
        new RMAppAttemptRejectedEvent(
            applicationAttempt.getAppAttemptId(), message));
    testAppAttemptSubmittedToFailedState(message);
  }

  @Test
  public void testSubmittedToKilled() {
    submitApplicationAttempt();
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    testAppAttemptKilledState(null, EMPTY_DIAGNOSTICS);
  }

  @Test
  public void testScheduledToKilled() {
    scheduleApplicationAttempt();
    applicationAttempt.handle(        
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    testAppAttemptKilledState(null, EMPTY_DIAGNOSTICS);
  }

  @Test
  public void testAllocatedToKilled() {
    Container amContainer = allocateApplicationAttempt();
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    testAppAttemptKilledState(amContainer, EMPTY_DIAGNOSTICS);
  }

  @Test
  public void testAllocatedToFailed() {
    Container amContainer = allocateApplicationAttempt();
    String diagnostics = "Launch Failed";
    applicationAttempt.handle(
        new RMAppAttemptLaunchFailedEvent(
            applicationAttempt.getAppAttemptId(), 
            diagnostics));
    testAppAttemptFailedState(amContainer, diagnostics);
  }


  @Test
  public void testNoTrackingUrl() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "");
  }

  @Test
  public void testRunningToFailed() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl");
    String containerDiagMsg = "some error";
    int exitCode = 123;
    ContainerStatus cs = BuilderUtils.newContainerStatus(amContainer.getId(),
        ContainerState.COMPLETE, containerDiagMsg, exitCode);
    ApplicationAttemptId appAttemptId = applicationAttempt.getAppAttemptId();
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        appAttemptId, cs));
    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    assertEquals(0, applicationAttempt.getRanNodes().size());
    String rmAppPageUrl = pjoin(RM_WEBAPP_ADDR, "cluster", "app",
        applicationAttempt.getAppAttemptId().getApplicationId());
    assertEquals(rmAppPageUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(rmAppPageUrl, applicationAttempt.getTrackingUrl());
  }

  @Test 
  public void testUnregisterToKilledFinish() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl");
    String trackingUrl = "newtrackingurl";
    String diagnostics = "Killed by user";
    FinalApplicationStatus finalStatus = FinalApplicationStatus.KILLED;
    applicationAttempt.handle(
        new RMAppAttemptUnregistrationEvent(
            applicationAttempt.getAppAttemptId(), 
            trackingUrl, finalStatus, diagnostics));
    testAppAttemptFinishedState(amContainer, finalStatus,
        trackingUrl, diagnostics);
  }
  
  
  @Test 
  public void testUnregisterToSuccessfulFinish() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl");
    String trackingUrl = "mytrackingurl";
    String diagnostics = "Successful";
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    applicationAttempt.handle(
        new RMAppAttemptUnregistrationEvent(
            applicationAttempt.getAppAttemptId(), 
            trackingUrl, finalStatus, diagnostics));
    testAppAttemptFinishedState(amContainer, finalStatus,
        trackingUrl, diagnostics);
  }
  
}
