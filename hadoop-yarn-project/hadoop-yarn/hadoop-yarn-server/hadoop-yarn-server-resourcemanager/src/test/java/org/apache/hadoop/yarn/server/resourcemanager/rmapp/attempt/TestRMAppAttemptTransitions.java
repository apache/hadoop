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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFailedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAcquiredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAllocatedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptNewSavedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUpdateSavedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

@RunWith(value = Parameterized.class)
public class TestRMAppAttemptTransitions {

  private static final Log LOG = 
      LogFactory.getLog(TestRMAppAttemptTransitions.class);
  
  private static final String EMPTY_DIAGNOSTICS = "";
  private static final String RM_WEBAPP_ADDR =
      WebAppUtils.getResolvedRMWebAppURLWithoutScheme(new Configuration());
  
  private boolean isSecurityEnabled;
  private RMContext rmContext;
  private YarnScheduler scheduler;
  private ApplicationMasterService masterService;
  private ApplicationMasterLauncher applicationMasterLauncher;
  private AMLivelinessMonitor amLivelinessMonitor;
  private AMLivelinessMonitor amFinishingMonitor;
  private RMApplicationHistoryWriter writer;

  private RMStateStore store;

  private RMAppImpl application;
  private RMAppAttempt applicationAttempt;

  private Configuration conf = new Configuration();
  private AMRMTokenSecretManager amRMTokenManager = spy(new AMRMTokenSecretManager(conf));
  private ClientToAMTokenSecretManagerInRM clientToAMTokenManager =
      spy(new ClientToAMTokenSecretManagerInRM());
  private NMTokenSecretManagerInRM nmTokenManager =
      spy(new NMTokenSecretManagerInRM(conf));
  private boolean transferStateFromPreviousAttempt = false;

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
      if (event instanceof RMAppFailedAttemptEvent) {
        transferStateFromPreviousAttempt =
            ((RMAppFailedAttemptEvent) event)
              .getTransferStateFromPreviousAttempt();
      }
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
  
  private ApplicationSubmissionContext submissionContext = null;
  private boolean unmanagedAM;

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][] {
        { Boolean.FALSE },
        { Boolean.TRUE }
    });
  }

  public TestRMAppAttemptTransitions(Boolean isSecurityEnabled) {
    this.isSecurityEnabled = isSecurityEnabled;
  }

  @Before
  public void setUp() throws Exception {
    AuthenticationMethod authMethod = AuthenticationMethod.SIMPLE;
    if (isSecurityEnabled) {
      authMethod = AuthenticationMethod.KERBEROS;
    }
    SecurityUtil.setAuthenticationMethod(authMethod, conf);
    UserGroupInformation.setConfiguration(conf);
    InlineDispatcher rmDispatcher = new InlineDispatcher();
  
    ContainerAllocationExpirer containerAllocationExpirer =
        mock(ContainerAllocationExpirer.class);
    amLivelinessMonitor = mock(AMLivelinessMonitor.class);
    amFinishingMonitor = mock(AMLivelinessMonitor.class);
    writer = mock(RMApplicationHistoryWriter.class);
    rmContext =
        new RMContextImpl(rmDispatcher,
          containerAllocationExpirer, amLivelinessMonitor, amFinishingMonitor,
          null, amRMTokenManager,
          new RMContainerTokenSecretManager(conf),
          nmTokenManager,
          clientToAMTokenManager,
          writer);
    
    store = mock(RMStateStore.class);
    ((RMContextImpl) rmContext).setStateStore(store);
    
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
        ApplicationAttemptId.newInstance(applicationId, 0);
    
    final String user = MockApps.newUserName();
    final String queue = MockApps.newQueue();
    submissionContext = mock(ApplicationSubmissionContext.class);
    when(submissionContext.getQueue()).thenReturn(queue);
    Resource resource = BuilderUtils.newResource(1536, 1);
    ContainerLaunchContext amContainerSpec =
        BuilderUtils.newContainerLaunchContext(null, null,
            null, null, null, null);
    when(submissionContext.getAMContainerSpec()).thenReturn(amContainerSpec);
    when(submissionContext.getResource()).thenReturn(resource);

    unmanagedAM = false;
    
    application = mock(RMAppImpl.class);
    applicationAttempt =
        new RMAppAttemptImpl(applicationAttemptId, rmContext, scheduler,
          masterService, submissionContext, new Configuration(), false);
    when(application.getCurrentAppAttempt()).thenReturn(applicationAttempt);
    when(application.getApplicationId()).thenReturn(applicationId);
    
    testAppAttemptNewState();
  }

  @After
  public void tearDown() throws Exception {
    ((AsyncDispatcher)this.rmContext.getDispatcher()).stop();
  }
  

  private String getProxyUrl(RMAppAttempt appAttempt) {
    String url = null;
    final String scheme = WebAppUtils.getHttpSchemePrefix(conf);
    try {
      URI trackingUri =
          StringUtils.isEmpty(appAttempt.getOriginalTrackingUrl()) ? null :
              ProxyUriUtils
                  .getUriFromAMUrl(scheme, appAttempt.getOriginalTrackingUrl());
      String proxy = WebAppUtils.getProxyHostAndPort(conf);
      URI proxyUri = ProxyUriUtils.getUriFromAMUrl(scheme, proxy);
      URI result = ProxyUriUtils.getProxyUri(trackingUri, proxyUri,
          appAttempt.getAppAttemptId().getApplicationId());
      url = result.toASCIIString();
    } catch (URISyntaxException ex) {
      Assert.fail();
    }
    return url;
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
    if (UserGroupInformation.isSecurityEnabled()) {
      verify(clientToAMTokenManager).createMasterKey(
          applicationAttempt.getAppAttemptId());
      // can't create ClientToken as at this time ClientTokenMasterKey has
      // not been registered in the SecretManager
      assertNull(applicationAttempt.createClientToken("some client"));
    }
    assertNull(applicationAttempt.createClientToken(null));
    assertNotNull(applicationAttempt.getAMRMToken());
    // Check events
    verify(masterService).
        registerAppAttempt(applicationAttempt.getAppAttemptId());
    verify(scheduler).handle(any(AppAttemptAddedSchedulerEvent.class));
  }

  /**
   * {@link RMAppAttemptState#SUBMITTED} -> {@link RMAppAttemptState#FAILED}
   */
  private void testAppAttemptSubmittedToFailedState(String diagnostics) {
    sendAttemptUpdateSavedEvent(applicationAttempt);
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
    
    // this works for unmanaged and managed AM's because this is actually doing
    // verify(application).handle(anyObject());
    verify(application).handle(any(RMAppRejectedEvent.class));
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  /**
   * {@link RMAppAttemptState#KILLED}
   */
  private void testAppAttemptKilledState(Container amContainer, 
      String diagnostics) {
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.KILLED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, applicationAttempt.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyAttemptFinalStateSaved();
    assertFalse(transferStateFromPreviousAttempt);
    verifyApplicationAttemptFinished(RMAppAttemptState.KILLED);
  }
  
  /**
   * {@link RMAppAttemptState#LAUNCHED}
   */
  private void testAppAttemptRecoveredState() {
    assertEquals(RMAppAttemptState.LAUNCHED, 
        applicationAttempt.getAppAttemptState());
  }

  /**
   * {@link RMAppAttemptState#SCHEDULED}
   */
  @SuppressWarnings("unchecked")
  private void testAppAttemptScheduledState() {
    RMAppAttemptState expectedState;
    int expectedAllocateCount;
    if(unmanagedAM) {
      expectedState = RMAppAttemptState.LAUNCHED;
      expectedAllocateCount = 0;
    } else {
      expectedState = RMAppAttemptState.SCHEDULED;
      expectedAllocateCount = 1;
    }

    assertEquals(expectedState, 
        applicationAttempt.getAppAttemptState());
    verify(scheduler, times(expectedAllocateCount)).
    allocate(any(ApplicationAttemptId.class), 
        any(List.class), any(List.class), any(List.class), any(List.class));

    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertNull(applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, applicationAttempt.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
  }

  /**
   * {@link RMAppAttemptState#ALLOCATED}
   */
  @SuppressWarnings("unchecked")
  private void testAppAttemptAllocatedState(Container amContainer) {
    assertEquals(RMAppAttemptState.ALLOCATED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    
    // Check events
    verify(applicationMasterLauncher).handle(any(AMLauncherEvent.class));
    verify(scheduler, times(2)).
        allocate(
            any(
                ApplicationAttemptId.class), any(List.class), any(List.class), 
                any(List.class), any(List.class));
    verify(nmTokenManager).clearNodeSetForAttempt(
      applicationAttempt.getAppAttemptId());
  }
  
  /**
   * {@link RMAppAttemptState#FAILED}
   */
  private void testAppAttemptFailedState(Container container, 
      String diagnostics) {
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertEquals(container, applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, applicationAttempt.getRanNodes().size());
    
    // Check events
    verify(application, times(1)).handle(any(RMAppFailedAttemptEvent.class));
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyAttemptFinalStateSaved();
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  /**
   * {@link RMAppAttemptState#LAUNCH}
   */
  private void testAppAttemptLaunchedState(Container container) {
    assertEquals(RMAppAttemptState.LAUNCHED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(container, applicationAttempt.getMasterContainer());
    if (UserGroupInformation.isSecurityEnabled()) {
      // ClientTokenMasterKey has been registered in SecretManager, it's able to
      // create ClientToken now
      assertNotNull(applicationAttempt.createClientToken("some client"));
    }
    // TODO - need to add more checks relevant to this state
  }

  /**
   * {@link RMAppAttemptState#RUNNING}
   */
  private void testAppAttemptRunningState(Container container,
      String host, int rpcPort, String trackingUrl, boolean unmanagedAM) {
    assertEquals(RMAppAttemptState.RUNNING, 
        applicationAttempt.getAppAttemptState());
    assertEquals(container, applicationAttempt.getMasterContainer());
    assertEquals(host, applicationAttempt.getHost());
    assertEquals(rpcPort, applicationAttempt.getRpcPort());
    verifyUrl(trackingUrl, applicationAttempt.getOriginalTrackingUrl());
    if (unmanagedAM) {
      verifyUrl(trackingUrl, applicationAttempt.getTrackingUrl());
    } else {
      assertEquals(getProxyUrl(applicationAttempt), 
          applicationAttempt.getTrackingUrl());
    }
    // TODO - need to add more checks relevant to this state
  }

  /**
   * {@link RMAppAttemptState#FINISHING}
   */
  private void testAppAttemptFinishingState(Container container,
      FinalApplicationStatus finalStatus,
      String trackingUrl,
      String diagnostics) {
    assertEquals(RMAppAttemptState.FINISHING,
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    verifyUrl(trackingUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(getProxyUrl(applicationAttempt),
        applicationAttempt.getTrackingUrl());
    assertEquals(container, applicationAttempt.getMasterContainer());
    assertEquals(finalStatus, applicationAttempt.getFinalApplicationStatus());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 0);
    verifyAttemptFinalStateSaved();
  }

  /**
   * {@link RMAppAttemptState#FINISHED}
   */
  private void testAppAttemptFinishedState(Container container,
      FinalApplicationStatus finalStatus, 
      String trackingUrl, 
      String diagnostics,
      int finishedContainerCount, boolean unmanagedAM) {
    assertEquals(RMAppAttemptState.FINISHED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    verifyUrl(trackingUrl, applicationAttempt.getOriginalTrackingUrl());
    if (unmanagedAM) {
      verifyUrl(trackingUrl, applicationAttempt.getTrackingUrl()); 
    } else {
      assertEquals(getProxyUrl(applicationAttempt),
          applicationAttempt.getTrackingUrl());
      verifyAttemptFinalStateSaved();
    }
    assertEquals(finishedContainerCount, applicationAttempt
        .getJustFinishedContainers().size());
    assertEquals(container, applicationAttempt.getMasterContainer());
    assertEquals(finalStatus, applicationAttempt.getFinalApplicationStatus());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    assertFalse(transferStateFromPreviousAttempt);
    verifyApplicationAttemptFinished(RMAppAttemptState.FINISHED);
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
            RMAppAttemptEventType.ATTEMPT_ADDED));
    
    if(unmanagedAM){
      assertEquals(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING, 
          applicationAttempt.getAppAttemptState());
      applicationAttempt.handle(
          new RMAppAttemptNewSavedEvent(
              applicationAttempt.getAppAttemptId(), null));
    }
    
    testAppAttemptScheduledState();
  }

  @SuppressWarnings("unchecked")
  private Container allocateApplicationAttempt() {
    scheduleApplicationAttempt();
    
    // Mock the allocation of AM container 
    Container container = mock(Container.class);
    Resource resource = BuilderUtils.newResource(2048, 1);
    when(container.getId()).thenReturn(
        BuilderUtils.newContainerId(applicationAttempt.getAppAttemptId(), 1));
    when(container.getResource()).thenReturn(resource);
    Allocation allocation = mock(Allocation.class);
    when(allocation.getContainers()).
        thenReturn(Collections.singletonList(container));
    when(
        scheduler.allocate(
            any(ApplicationAttemptId.class), 
            any(List.class), 
            any(List.class), 
            any(List.class), 
            any(List.class))).
    thenReturn(allocation);
    
    applicationAttempt.handle(
        new RMAppAttemptContainerAllocatedEvent(
            applicationAttempt.getAppAttemptId()));
    
    assertEquals(RMAppAttemptState.ALLOCATED_SAVING, 
        applicationAttempt.getAppAttemptState());
    applicationAttempt.handle(
        new RMAppAttemptNewSavedEvent(
            applicationAttempt.getAppAttemptId(), null));
    
    testAppAttemptAllocatedState(container);
    
    return container;
  }
  
  private void launchApplicationAttempt(Container container) {
    if (UserGroupInformation.isSecurityEnabled()) {
      // Before LAUNCHED state, can't create ClientToken as at this time
      // ClientTokenMasterKey has not been registered in the SecretManager
      assertNull(applicationAttempt.createClientToken("some client"));
    }
    applicationAttempt.handle(
        new RMAppAttemptEvent(applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.LAUNCHED));

    testAppAttemptLaunchedState(container);    
  }
  
  private void runApplicationAttempt(Container container,
      String host, 
      int rpcPort, 
      String trackingUrl, boolean unmanagedAM) {
    applicationAttempt.handle(
        new RMAppAttemptRegistrationEvent(
            applicationAttempt.getAppAttemptId(),
            host, rpcPort, trackingUrl));
    
    testAppAttemptRunningState(container, host, rpcPort, trackingUrl, 
        unmanagedAM);
  }

  private void unregisterApplicationAttempt(Container container,
      FinalApplicationStatus finalStatus, String trackingUrl,
      String diagnostics) {
    applicationAttempt.handle(
        new RMAppAttemptUnregistrationEvent(
            applicationAttempt.getAppAttemptId(),
            trackingUrl, finalStatus, diagnostics));
    sendAttemptUpdateSavedEvent(applicationAttempt);
    testAppAttemptFinishingState(container, finalStatus,
        trackingUrl, diagnostics);
  }

  private void testUnmanagedAMSuccess(String url) {
    unmanagedAM = true;
    when(submissionContext.getUnmanagedAM()).thenReturn(true);
    // submit AM and check it goes to LAUNCHED state
    scheduleApplicationAttempt();
    testAppAttemptLaunchedState(null);
    verify(amLivelinessMonitor, times(1)).register(
        applicationAttempt.getAppAttemptId());

    // launch AM
    runApplicationAttempt(null, "host", 8042, url, true);

    // complete a container
    applicationAttempt.handle(new RMAppAttemptContainerAcquiredEvent(
        applicationAttempt.getAppAttemptId(), mock(Container.class)));
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        applicationAttempt.getAppAttemptId(), mock(ContainerStatus.class)));
    // complete AM
    String diagnostics = "Successful";
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    applicationAttempt.handle(new RMAppAttemptUnregistrationEvent(
        applicationAttempt.getAppAttemptId(), url, finalStatus,
        diagnostics));
    testAppAttemptFinishedState(null, finalStatus, url, diagnostics, 1,
        true);
    assertFalse(transferStateFromPreviousAttempt);
  }

  private void sendAttemptUpdateSavedEvent(RMAppAttempt applicationAttempt) {
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    applicationAttempt.handle(
      new RMAppAttemptUpdateSavedEvent(
          applicationAttempt.getAppAttemptId(), null));
  }

  @Test
  public void testUnmanagedAMUnexpectedRegistration() {
    unmanagedAM = true;
    when(submissionContext.getUnmanagedAM()).thenReturn(true);

    // submit AM and check it goes to SUBMITTED state
    submitApplicationAttempt();
    assertEquals(RMAppAttemptState.SUBMITTED,
        applicationAttempt.getAppAttemptState());

    // launch AM and verify attempt failed
    applicationAttempt.handle(new RMAppAttemptRegistrationEvent(
        applicationAttempt.getAppAttemptId(), "host", 8042, "oldtrackingurl"));
    assertEquals(YarnApplicationAttemptState.SUBMITTED,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptSubmittedToFailedState(
        "Unmanaged AM must register after AM attempt reaches LAUNCHED state.");
  }

  @Test
  public void testUnmanagedAMContainersCleanup() {
    unmanagedAM = true;
    when(submissionContext.getUnmanagedAM()).thenReturn(true);
    when(submissionContext.getKeepContainersAcrossApplicationAttempts())
      .thenReturn(true);
    // submit AM and check it goes to SUBMITTED state
    submitApplicationAttempt();
    // launch AM and verify attempt failed
    applicationAttempt.handle(new RMAppAttemptRegistrationEvent(
      applicationAttempt.getAppAttemptId(), "host", 8042, "oldtrackingurl"));
    assertEquals(YarnApplicationAttemptState.SUBMITTED,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertFalse(transferStateFromPreviousAttempt);
  }

  @Test
  public void testNewToKilled() {
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    assertEquals(YarnApplicationAttemptState.NEW,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptKilledState(null, EMPTY_DIAGNOSTICS);
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
  } 
  
  @Test
  public void testNewToRecovered() {
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.RECOVER));
    testAppAttemptRecoveredState();
  }

  @Test
  public void testSubmittedToKilled() {
    submitApplicationAttempt();
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    assertEquals(YarnApplicationAttemptState.SUBMITTED,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptKilledState(null, EMPTY_DIAGNOSTICS);
  }

  @Test
  public void testScheduledToKilled() {
    scheduleApplicationAttempt();
    applicationAttempt.handle(        
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    assertEquals(YarnApplicationAttemptState.SCHEDULED,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptKilledState(null, EMPTY_DIAGNOSTICS);
  }

  @Test
  public void testAllocatedToKilled() {
    Container amContainer = allocateApplicationAttempt();
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    assertEquals(YarnApplicationAttemptState.ALLOCATED,
        applicationAttempt.createApplicationAttemptState());
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
    assertEquals(YarnApplicationAttemptState.ALLOCATED,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptFailedState(amContainer, diagnostics);
  }
  
  @Test
  public void testAMCrashAtAllocated() {
    Container amContainer = allocateApplicationAttempt();
    String containerDiagMsg = "some error";
    int exitCode = 123;
    ContainerStatus cs =
        BuilderUtils.newContainerStatus(amContainer.getId(),
          ContainerState.COMPLETE, containerDiagMsg, exitCode);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      applicationAttempt.getAppAttemptId(), cs));
    assertEquals(YarnApplicationAttemptState.ALLOCATED,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
      applicationAttempt.getAppAttemptState());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }
  
  @Test
  public void testRunningToFailed() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    String containerDiagMsg = "some error";
    int exitCode = 123;
    ContainerStatus cs = BuilderUtils.newContainerStatus(amContainer.getId(),
        ContainerState.COMPLETE, containerDiagMsg, exitCode);
    ApplicationAttemptId appAttemptId = applicationAttempt.getAppAttemptId();
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        appAttemptId, cs));

    // ignored ContainerFinished and Expire at FinalSaving if we were supposed
    // to Failed state.
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState()); 
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      applicationAttempt.getAppAttemptId(), BuilderUtils.newContainerStatus(
        amContainer.getId(), ContainerState.COMPLETE, "", 0)));
    applicationAttempt.handle(new RMAppAttemptEvent(
      applicationAttempt.getAppAttemptId(), RMAppAttemptEventType.EXPIRE));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState()); 
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    assertEquals(0, applicationAttempt.getRanNodes().size());
    String rmAppPageUrl = pjoin(RM_WEBAPP_ADDR, "cluster", "app",
        applicationAttempt.getAppAttemptId().getApplicationId());
    assertEquals(rmAppPageUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(rmAppPageUrl, applicationAttempt.getTrackingUrl());
    verifyAMHostAndPortInvalidated();
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  @Test
  public void testRunningToKilled() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.KILL));

    // ignored ContainerFinished and Expire at FinalSaving if we were supposed
    // to Killed state.
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState()); 
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      applicationAttempt.getAppAttemptId(), BuilderUtils.newContainerStatus(
        amContainer.getId(), ContainerState.COMPLETE, "", 0)));
    applicationAttempt.handle(new RMAppAttemptEvent(
      applicationAttempt.getAppAttemptId(), RMAppAttemptEventType.EXPIRE));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState()); 
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.KILLED,
        applicationAttempt.getAppAttemptState());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    assertEquals(0, applicationAttempt.getRanNodes().size());
    String rmAppPageUrl = pjoin(RM_WEBAPP_ADDR, "cluster", "app",
        applicationAttempt.getAppAttemptId().getApplicationId());
    assertEquals(rmAppPageUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(rmAppPageUrl, applicationAttempt.getTrackingUrl());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyAMHostAndPortInvalidated();
    verifyApplicationAttemptFinished(RMAppAttemptState.KILLED);
  }

  @Test(timeout=10000)
  public void testLaunchedExpire() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    applicationAttempt.handle(new RMAppAttemptEvent(
        applicationAttempt.getAppAttemptId(), RMAppAttemptEventType.EXPIRE));
    assertEquals(YarnApplicationAttemptState.LAUNCHED,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());
    assertTrue("expire diagnostics missing",
        applicationAttempt.getDiagnostics().contains("timed out"));
    String rmAppPageUrl = pjoin(RM_WEBAPP_ADDR, "cluster", "app",
        applicationAttempt.getAppAttemptId().getApplicationId());
    assertEquals(rmAppPageUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(rmAppPageUrl, applicationAttempt.getTrackingUrl());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  @Test(timeout=20000)
  public void testRunningExpire() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    applicationAttempt.handle(new RMAppAttemptEvent(
        applicationAttempt.getAppAttemptId(), RMAppAttemptEventType.EXPIRE));
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());
    assertTrue("expire diagnostics missing",
        applicationAttempt.getDiagnostics().contains("timed out"));
    String rmAppPageUrl = pjoin(RM_WEBAPP_ADDR, "cluster", "app",
        applicationAttempt.getAppAttemptId().getApplicationId());
    assertEquals(rmAppPageUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(rmAppPageUrl, applicationAttempt.getTrackingUrl());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyAMHostAndPortInvalidated();
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  @Test 
  public void testUnregisterToKilledFinishing() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    unregisterApplicationAttempt(amContainer,
        FinalApplicationStatus.KILLED, "newtrackingurl",
        "Killed by user");
  }

  @Test
  public void testTrackingUrlUnmanagedAM() {
    testUnmanagedAMSuccess("oldTrackingUrl");
  }

  @Test
  public void testEmptyTrackingUrlUnmanagedAM() {
    testUnmanagedAMSuccess("");
  }

  @Test
  public void testNullTrackingUrlUnmanagedAM() {
    testUnmanagedAMSuccess(null);
  }

  @Test
  public void testManagedAMWithTrackingUrl() {
    testTrackingUrlManagedAM("theTrackingUrl");
  }

  @Test
  public void testManagedAMWithEmptyTrackingUrl() {
    testTrackingUrlManagedAM("");
  }

  @Test
  public void testManagedAMWithNullTrackingUrl() {
    testTrackingUrlManagedAM(null);
  }

  private void testTrackingUrlManagedAM(String url) {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, url, false);
    unregisterApplicationAttempt(amContainer,
        FinalApplicationStatus.SUCCEEDED, url, "Successful");
  }

  @Test
  public void testUnregisterToSuccessfulFinishing() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    unregisterApplicationAttempt(amContainer,
        FinalApplicationStatus.SUCCEEDED, "mytrackingurl", "Successful");
  }

  @Test
  public void testFinishingKill() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.FAILED;
    String trackingUrl = "newtrackingurl";
    String diagnostics = "Job failed";
    unregisterApplicationAttempt(amContainer, finalStatus, trackingUrl,
        diagnostics);
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    testAppAttemptFinishingState(amContainer, finalStatus, trackingUrl,
        diagnostics);
  }

  @Test
  public void testFinishingExpire() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    String trackingUrl = "mytrackingurl";
    String diagnostics = "Successful";
    unregisterApplicationAttempt(amContainer, finalStatus, trackingUrl,
        diagnostics);
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.EXPIRE));
    testAppAttemptFinishedState(amContainer, finalStatus, trackingUrl,
        diagnostics, 0, false);
  }

  @Test
  public void testFinishingToFinishing() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    String trackingUrl = "mytrackingurl";
    String diagnostics = "Successful";
    unregisterApplicationAttempt(amContainer, finalStatus, trackingUrl,
        diagnostics);
    // container must be AM container to move from FINISHING to FINISHED
    applicationAttempt.handle(
        new RMAppAttemptContainerFinishedEvent(
            applicationAttempt.getAppAttemptId(),
            BuilderUtils.newContainerStatus(
                BuilderUtils.newContainerId(
                    applicationAttempt.getAppAttemptId(), 42),
                ContainerState.COMPLETE, "", 0)));
    testAppAttemptFinishingState(amContainer, finalStatus, trackingUrl,
        diagnostics);
  }

  @Test
  public void testSuccessfulFinishingToFinished() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    String trackingUrl = "mytrackingurl";
    String diagnostics = "Successful";
    unregisterApplicationAttempt(amContainer, finalStatus, trackingUrl,
        diagnostics);
    applicationAttempt.handle(
        new RMAppAttemptContainerFinishedEvent(
            applicationAttempt.getAppAttemptId(),
            BuilderUtils.newContainerStatus(amContainer.getId(),
                ContainerState.COMPLETE, "", 0)));
    testAppAttemptFinishedState(amContainer, finalStatus, trackingUrl,
        diagnostics, 0, false);
  }

  // While attempt is at FINAL_SAVING, Contaienr_Finished event may come before
  // Attempt_Saved event, we stay on FINAL_SAVING on Container_Finished event
  // and then directly jump from FINAL_SAVING to FINISHED state on Attempt_Saved
  // event
  @Test
  public void
      testFinalSavingToFinishedWithContainerFinished() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    String trackingUrl = "mytrackingurl";
    String diagnostics = "Successful";
    applicationAttempt.handle(new RMAppAttemptUnregistrationEvent(
      applicationAttempt.getAppAttemptId(), trackingUrl, finalStatus,
      diagnostics));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    // Container_finished event comes before Attempt_Saved event.
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      applicationAttempt.getAppAttemptId(), BuilderUtils.newContainerStatus(
        amContainer.getId(), ContainerState.COMPLETE, "", 0)));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    // send attempt_saved
    sendAttemptUpdateSavedEvent(applicationAttempt);
    testAppAttemptFinishedState(amContainer, finalStatus, trackingUrl,
      diagnostics, 0, false);
  }

  // While attempt is at FINAL_SAVING, Expire event may come before
  // Attempt_Saved event, we stay on FINAL_SAVING on Expire event and then
  // directly jump from FINAL_SAVING to FINISHED state on Attempt_Saved event.
  @Test
  public void testFinalSavingToFinishedWithExpire() {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    String trackingUrl = "mytrackingurl";
    String diagnostics = "Successssseeeful";
    applicationAttempt.handle(new RMAppAttemptUnregistrationEvent(
      applicationAttempt.getAppAttemptId(), trackingUrl, finalStatus,
      diagnostics));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    // Expire event comes before Attempt_saved event.
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
      .getAppAttemptId(), RMAppAttemptEventType.EXPIRE));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    // send attempt_saved
    sendAttemptUpdateSavedEvent(applicationAttempt);
    testAppAttemptFinishedState(amContainer, finalStatus, trackingUrl,
      diagnostics, 0, false);
  }

  // this is to test user can get client tokens only after the client token
  // master key is saved in the state store and also registered in
  // ClientTokenSecretManager
  @Test
  public void testGetClientToken() throws Exception {
    assumeTrue(isSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();

    // before attempt is launched, can not get ClientToken
    Token<ClientToAMTokenIdentifier> token =
        applicationAttempt.createClientToken(null);
    Assert.assertNull(token);
    token = applicationAttempt.createClientToken("clientuser");
    Assert.assertNull(token);

    launchApplicationAttempt(amContainer);
    // after attempt is launched , can get ClientToken
    token = applicationAttempt.createClientToken(null);
    Assert.assertNull(token);
    token = applicationAttempt.createClientToken("clientuser");
    Assert.assertNotNull(token);

    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
      .getAppAttemptId(), RMAppAttemptEventType.KILL));
    assertEquals(YarnApplicationAttemptState.LAUNCHED,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    // after attempt is killed, can not get Client Token
    token = applicationAttempt.createClientToken(null);
    Assert.assertNull(token);
    token = applicationAttempt.createClientToken("clientuser");
    Assert.assertNull(token);
  }

  @Test
  public void testFailedToFailed() {
    // create a failed attempt.
    when(submissionContext.getKeepContainersAcrossApplicationAttempts())
      .thenReturn(true);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    ContainerStatus cs1 =
        ContainerStatus.newInstance(amContainer.getId(),
          ContainerState.COMPLETE, "some error", 123);
    ApplicationAttemptId appAttemptId = applicationAttempt.getAppAttemptId();
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      appAttemptId, cs1));
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
      applicationAttempt.getAppAttemptState());
    // should not kill containers when attempt fails.
    assertTrue(transferStateFromPreviousAttempt);
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);

    // failed attempt captured the container finished event.
    assertEquals(0, applicationAttempt.getJustFinishedContainers().size());
    ContainerStatus cs2 =
        ContainerStatus.newInstance(ContainerId.newInstance(appAttemptId, 2),
          ContainerState.COMPLETE, "", 0);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      appAttemptId, cs2));
    assertEquals(1, applicationAttempt.getJustFinishedContainers().size());
    assertEquals(cs2.getContainerId(), applicationAttempt
      .getJustFinishedContainers().get(0).getContainerId());
  }


  @Test
  public void testContainersCleanupForLastAttempt() {
    // create a failed attempt.
    applicationAttempt =
        new RMAppAttemptImpl(applicationAttempt.getAppAttemptId(), rmContext,
          scheduler, masterService, submissionContext, new Configuration(),
          true);
    when(submissionContext.getKeepContainersAcrossApplicationAttempts())
      .thenReturn(true);
    when(submissionContext.getMaxAppAttempts()).thenReturn(1);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    ContainerStatus cs1 =
        ContainerStatus.newInstance(amContainer.getId(),
          ContainerState.COMPLETE, "some error", 123);
    ApplicationAttemptId appAttemptId = applicationAttempt.getAppAttemptId();
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      appAttemptId, cs1));
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
      applicationAttempt.getAppAttemptState());
    assertFalse(transferStateFromPreviousAttempt);
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  private void verifyTokenCount(ApplicationAttemptId appAttemptId, int count) {
    verify(amRMTokenManager, times(count)).applicationMasterFinished(appAttemptId);
    if (UserGroupInformation.isSecurityEnabled()) {
      verify(clientToAMTokenManager, times(count)).unRegisterApplication(appAttemptId);
      if (count > 0) {
        assertNull(applicationAttempt.createClientToken("client"));
      }
    }
  }

  private void verifyUrl(String url1, String url2) {
    if (url1 == null || url1.trim().isEmpty()) {
      assertEquals("N/A", url2);
    } else {
      assertEquals(url1, url2);
    }
  }

  private void verifyAttemptFinalStateSaved() {
    verify(store, times(1)).updateApplicationAttemptState(
      any(ApplicationAttemptState.class));
  }

  private void verifyAMHostAndPortInvalidated() {
    assertEquals("N/A", applicationAttempt.getHost());
    assertEquals(-1, applicationAttempt.getRpcPort());
  }

  private void verifyApplicationAttemptFinished(RMAppAttemptState state) {
    ArgumentCaptor<RMAppAttemptState> finalState =
        ArgumentCaptor.forClass(RMAppAttemptState.class);
    verify(writer).applicationAttemptFinished(
        any(RMAppAttempt.class), finalState.capture());
    Assert.assertEquals(state, finalState.getValue());
  }

}
