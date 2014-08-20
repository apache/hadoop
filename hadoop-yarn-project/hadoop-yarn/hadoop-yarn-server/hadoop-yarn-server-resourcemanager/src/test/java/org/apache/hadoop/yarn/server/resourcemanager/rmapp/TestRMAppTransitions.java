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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;


@RunWith(value = Parameterized.class)
public class TestRMAppTransitions {
  static final Log LOG = LogFactory.getLog(TestRMAppTransitions.class);

  private boolean isSecurityEnabled;
  private Configuration conf;
  private RMContext rmContext;
  private static int maxAppAttempts =
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS;
  private static int appId = 1;
  private DrainDispatcher rmDispatcher;
  private RMStateStore store;
  private RMApplicationHistoryWriter writer;
  private YarnScheduler scheduler;
  private TestSchedulerEventDispatcher schedulerDispatcher;

  // ignore all the RM application attempt events
  private static final class TestApplicationAttemptEventDispatcher implements
  EventHandler<RMAppAttemptEvent> {

    private final RMContext rmContext;
    public  TestApplicationAttemptEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppAttemptEvent event) {
      ApplicationId appId = event.getApplicationAttemptId().getApplicationId();
      RMApp rmApp = this.rmContext.getRMApps().get(appId);
      if (rmApp != null) {
        try {
          rmApp.getRMAppAttempt(event.getApplicationAttemptId()).handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for application " + appId, t);
        }    
      }
    }
  }

  // handle all the RM application events - same as in ResourceManager.java
  private static final class TestApplicationEventDispatcher implements
  EventHandler<RMAppEvent> {

    private final RMContext rmContext;
    public TestApplicationEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppEvent event) {
      ApplicationId appID = event.getApplicationId();
      RMApp rmApp = this.rmContext.getRMApps().get(appID);
      if (rmApp != null) {
        try {
          rmApp.handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for application " + appID, t);
        }
      }
    }
  }

  // handle all the RM application manager events - same as in
  // ResourceManager.java
  private static final class TestApplicationManagerEventDispatcher implements
      EventHandler<RMAppManagerEvent> {
    @Override
    public void handle(RMAppManagerEvent event) {
    }
  }

  // handle all the scheduler events - same as in ResourceManager.java
  private static final class TestSchedulerEventDispatcher implements
      EventHandler<SchedulerEvent> {
    public SchedulerEvent lastSchedulerEvent;
    
    @Override
    public void handle(SchedulerEvent event) {
      lastSchedulerEvent = event;
    }
  }  

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][] {
        { Boolean.FALSE },
        { Boolean.TRUE }
    });
  }

  public TestRMAppTransitions(boolean isSecurityEnabled) {
    this.isSecurityEnabled = isSecurityEnabled;
  }
  
  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    AuthenticationMethod authMethod = AuthenticationMethod.SIMPLE;
    if (isSecurityEnabled) {
      authMethod = AuthenticationMethod.KERBEROS;
    }
    SecurityUtil.setAuthenticationMethod(authMethod, conf);
    UserGroupInformation.setConfiguration(conf);

    rmDispatcher = new DrainDispatcher();
    ContainerAllocationExpirer containerAllocationExpirer = 
        mock(ContainerAllocationExpirer.class);
    AMLivelinessMonitor amLivelinessMonitor = mock(AMLivelinessMonitor.class);
    AMLivelinessMonitor amFinishingMonitor = mock(AMLivelinessMonitor.class);
    store = mock(RMStateStore.class);
    writer = mock(RMApplicationHistoryWriter.class);
    this.rmContext =
        new RMContextImpl(rmDispatcher,
          containerAllocationExpirer, amLivelinessMonitor, amFinishingMonitor,
          null, new AMRMTokenSecretManager(conf, this.rmContext),
          new RMContainerTokenSecretManager(conf),
          new NMTokenSecretManagerInRM(conf),
          new ClientToAMTokenSecretManagerInRM(),
          writer);
    ((RMContextImpl)rmContext).setStateStore(store);

    rmDispatcher.register(RMAppAttemptEventType.class,
        new TestApplicationAttemptEventDispatcher(this.rmContext));

    rmDispatcher.register(RMAppEventType.class,
        new TestApplicationEventDispatcher(rmContext));
    
    rmDispatcher.register(RMAppManagerEventType.class,
        new TestApplicationManagerEventDispatcher());
    
    schedulerDispatcher = new TestSchedulerEventDispatcher();
    rmDispatcher.register(SchedulerEventType.class,
        schedulerDispatcher);
    
    rmDispatcher.init(conf);
    rmDispatcher.start();
  }

  protected RMApp createNewTestApp(ApplicationSubmissionContext submissionContext) {
    ApplicationId applicationId = MockApps.newAppID(appId++);
    String user = MockApps.newUserName();
    String name = MockApps.newAppName();
    String queue = MockApps.newQueue();
    // ensure max application attempts set to known value
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, maxAppAttempts);
    scheduler = mock(YarnScheduler.class);

    ApplicationMasterService masterService =
        new ApplicationMasterService(rmContext, scheduler);
    
    if(submissionContext == null) {
      submissionContext = new ApplicationSubmissionContextPBImpl();
    }
    // applicationId will not be used because RMStateStore is mocked,
    // but applicationId is still set for safety
    submissionContext.setApplicationId(applicationId);

    RMApp application =
        new RMAppImpl(applicationId, rmContext, conf, name, user, queue,
          submissionContext, scheduler, masterService,
          System.currentTimeMillis(), "YARN", null);

    testAppStartState(applicationId, user, name, queue, application);
    this.rmContext.getRMApps().putIfAbsent(application.getApplicationId(),
        application);
    return application;
  }

  // Test expected newly created app state
  private static void testAppStartState(ApplicationId applicationId, 
      String user, String name, String queue, RMApp application) {
    Assert.assertTrue("application start time is not greater then 0", 
        application.getStartTime() > 0);
    Assert.assertTrue("application start time is before currentTime", 
        application.getStartTime() <= System.currentTimeMillis());
    Assert.assertEquals("application user is not correct",
        user, application.getUser());
    Assert.assertEquals("application id is not correct",
        applicationId, application.getApplicationId());
    Assert.assertEquals("application progress is not correct",
        (float)0.0, application.getProgress(), (float)0.0);
    Assert.assertEquals("application queue is not correct",
        queue, application.getQueue());
    Assert.assertEquals("application name is not correct",
        name, application.getName());
    Assert.assertEquals("application finish time is not 0 and should be",
        0, application.getFinishTime());
    Assert.assertEquals("application tracking url is not correct",
        null, application.getTrackingUrl());
    StringBuilder diag = application.getDiagnostics();
    Assert.assertEquals("application diagnostics is not correct",
        0, diag.length());
  }

  // test to make sure times are set when app finishes
  private static void assertStartTimeSet(RMApp application) {
    Assert.assertTrue("application start time is not greater then 0", 
        application.getStartTime() > 0);
    Assert.assertTrue("application start time is before currentTime", 
        application.getStartTime() <= System.currentTimeMillis());
  }

  private static void assertAppState(RMAppState state, RMApp application) {
    Assert.assertEquals("application state should have been " + state, 
        state, application.getState());
  }

  private static void assertFinalAppStatus(FinalApplicationStatus status, RMApp application) {
    Assert.assertEquals("Final application status should have been " + status, 
        status, application.getFinalApplicationStatus());
  }
  
  // test to make sure times are set when app finishes
  private void assertTimesAtFinish(RMApp application) {
    assertStartTimeSet(application);
    Assert.assertTrue("application finish time is not greater then 0",
        (application.getFinishTime() > 0));
    Assert.assertTrue("application finish time is not >= then start time",
        (application.getFinishTime() >= application.getStartTime()));
  }

  private void assertAppFinalStateSaved(RMApp application){
    verify(store, times(1)).updateApplicationState(any(ApplicationState.class));
  }

  private void assertAppFinalStateNotSaved(RMApp application){
    verify(store, times(0)).updateApplicationState(any(ApplicationState.class));
  }

  private void assertKilled(RMApp application) {
    assertTimesAtFinish(application);
    assertAppState(RMAppState.KILLED, application);
    assertFinalAppStatus(FinalApplicationStatus.KILLED, application);
    StringBuilder diag = application.getDiagnostics();
    Assert.assertEquals("application diagnostics is not correct",
        "Application killed by user.", diag.toString());
  }

  private void assertFailed(RMApp application, String regex) {
    assertTimesAtFinish(application);
    assertAppState(RMAppState.FAILED, application);
    assertFinalAppStatus(FinalApplicationStatus.FAILED, application);
    StringBuilder diag = application.getDiagnostics();
    Assert.assertTrue("application diagnostics is not correct",
        diag.toString().matches(regex));
  }

  private void sendAppUpdateSavedEvent(RMApp application) {
    RMAppEvent event =
        new RMAppEvent(application.getApplicationId(), RMAppEventType.APP_UPDATE_SAVED);
    application.handle(event);
    rmDispatcher.await();
  }

  private void sendAttemptUpdateSavedEvent(RMApp application) {
    application.getCurrentAppAttempt().handle(
        new RMAppAttemptEvent(application.getCurrentAppAttempt().getAppAttemptId(),
            RMAppAttemptEventType.ATTEMPT_UPDATE_SAVED));
  }

  protected RMApp testCreateAppNewSaving(
      ApplicationSubmissionContext submissionContext) throws IOException {
  RMApp application = createNewTestApp(submissionContext);
    verify(writer).applicationStarted(any(RMApp.class));
    // NEW => NEW_SAVING event RMAppEventType.START
    RMAppEvent event = 
        new RMAppEvent(application.getApplicationId(), RMAppEventType.START);
    application.handle(event);
    assertStartTimeSet(application);
    assertAppState(RMAppState.NEW_SAVING, application);
    return application;
  }

  protected RMApp testCreateAppSubmittedNoRecovery(
      ApplicationSubmissionContext submissionContext) throws IOException {
  RMApp application = testCreateAppNewSaving(submissionContext);
    // NEW_SAVING => SUBMITTED event RMAppEventType.APP_SAVED
    RMAppEvent event =
        new RMAppEvent(application.getApplicationId(), RMAppEventType.APP_NEW_SAVED);
    application.handle(event);
    assertStartTimeSet(application);
    assertAppState(RMAppState.SUBMITTED, application);
    return application;
  }

  protected RMApp testCreateAppSubmittedRecovery(
      ApplicationSubmissionContext submissionContext) throws IOException {
    RMApp application = createNewTestApp(submissionContext);
    // NEW => SUBMITTED event RMAppEventType.RECOVER
    RMAppEvent event =
        new RMAppEvent(application.getApplicationId(), RMAppEventType.RECOVER);
    application.handle(event);
    assertStartTimeSet(application);
    assertAppState(RMAppState.SUBMITTED, application);
    return application;
  }

  protected RMApp testCreateAppAccepted(
      ApplicationSubmissionContext submissionContext) throws IOException {
    RMApp application = testCreateAppSubmittedNoRecovery(submissionContext);
  // SUBMITTED => ACCEPTED event RMAppEventType.APP_ACCEPTED
    RMAppEvent event = 
        new RMAppEvent(application.getApplicationId(), 
            RMAppEventType.APP_ACCEPTED);
    application.handle(event);
    assertStartTimeSet(application);
    assertAppState(RMAppState.ACCEPTED, application);
    return application;
  }

  protected RMApp testCreateAppRunning(
      ApplicationSubmissionContext submissionContext) throws IOException {
  RMApp application = testCreateAppAccepted(submissionContext);
    // ACCEPTED => RUNNING event RMAppEventType.ATTEMPT_REGISTERED
    RMAppEvent event = 
        new RMAppEvent(application.getApplicationId(), 
            RMAppEventType.ATTEMPT_REGISTERED);
    application.handle(event);
    assertStartTimeSet(application);
    assertAppState(RMAppState.RUNNING, application);
    assertFinalAppStatus(FinalApplicationStatus.UNDEFINED, application);
    return application;
  }

  protected RMApp testCreateAppFinalSaving(
      ApplicationSubmissionContext submissionContext) throws IOException {
    RMApp application = testCreateAppRunning(submissionContext);
    RMAppEvent finishingEvent =
        new RMAppEvent(application.getApplicationId(),
          RMAppEventType.ATTEMPT_UNREGISTERED);
    application.handle(finishingEvent);
    assertAppState(RMAppState.FINAL_SAVING, application);
    assertAppFinalStateSaved(application);
    return application;
  }

  protected RMApp testCreateAppFinishing(
      ApplicationSubmissionContext submissionContext) throws IOException {
    // unmanaged AMs don't use the FINISHING state
    assert submissionContext == null || !submissionContext.getUnmanagedAM();
    RMApp application = testCreateAppFinalSaving(submissionContext);
    // FINAL_SAVING => FINISHING event RMAppEventType.APP_UPDATED
    RMAppEvent appUpdated =
        new RMAppEvent(application.getApplicationId(), RMAppEventType.APP_UPDATE_SAVED);
    application.handle(appUpdated);
    assertAppState(RMAppState.FINISHING, application);
    assertTimesAtFinish(application);
    return application;
  }

  protected RMApp testCreateAppFinished(
      ApplicationSubmissionContext submissionContext,
      String diagnostics) throws IOException {
    // unmanaged AMs don't use the FINISHING state
    RMApp application = null;
    if (submissionContext != null && submissionContext.getUnmanagedAM()) {
      application = testCreateAppRunning(submissionContext);
    } else {
      application = testCreateAppFinishing(submissionContext);
    }
    // RUNNING/FINISHING => FINISHED event RMAppEventType.ATTEMPT_FINISHED
    RMAppEvent finishedEvent = new RMAppFinishedAttemptEvent(
        application.getApplicationId(), diagnostics);
    application.handle(finishedEvent);
    assertAppState(RMAppState.FINISHED, application);
    assertTimesAtFinish(application);
    // finished without a proper unregister implies failed
    assertFinalAppStatus(FinalApplicationStatus.FAILED, application);
    Assert.assertTrue("Finished app missing diagnostics",
        application.getDiagnostics().indexOf(diagnostics) != -1);
    return application;
  }

  @Test
  public void testUnmanagedApp() throws IOException {
    ApplicationSubmissionContext subContext = new ApplicationSubmissionContextPBImpl();
    subContext.setUnmanagedAM(true);

    // test success path
    LOG.info("--- START: testUnmanagedAppSuccessPath ---");
    final String diagMsg = "some diagnostics";
    RMApp application = testCreateAppFinished(subContext, diagMsg);
    Assert.assertTrue("Finished app missing diagnostics",
        application.getDiagnostics().indexOf(diagMsg) != -1);

    // reset the counter of Mockito.verify
    reset(writer);

    // test app fails after 1 app attempt failure
    LOG.info("--- START: testUnmanagedAppFailPath ---");
    application = testCreateAppRunning(subContext);
    RMAppEvent event = new RMAppFailedAttemptEvent(
        application.getApplicationId(), RMAppEventType.ATTEMPT_FAILED, "", false);
    application.handle(event);
    rmDispatcher.await();
    RMAppAttempt appAttempt = application.getCurrentAppAttempt();
    Assert.assertEquals(1, appAttempt.getAppAttemptId().getAttemptId());
    sendAppUpdateSavedEvent(application);
    assertFailed(application,
        ".*Unmanaged application.*Failing the application.*");
    assertAppFinalStateSaved(application);
  }
  
  @Test
  public void testAppSuccessPath() throws IOException {
    LOG.info("--- START: testAppSuccessPath ---");
    final String diagMsg = "some diagnostics";
    RMApp application = testCreateAppFinished(null, diagMsg);
    Assert.assertTrue("Finished application missing diagnostics",
        application.getDiagnostics().indexOf(diagMsg) != -1);
  }

  @Test (timeout = 30000)
  public void testAppRecoverPath() throws IOException {
    LOG.info("--- START: testAppRecoverPath ---");
    testCreateAppSubmittedRecovery(null);
  }

  @Test (timeout = 30000)
  public void testAppNewKill() throws IOException {
    LOG.info("--- START: testAppNewKill ---");

    RMApp application = createNewTestApp(null);
    // NEW => KILLED event RMAppEventType.KILL
    RMAppEvent event = 
        new RMAppEvent(application.getApplicationId(), RMAppEventType.KILL);
    application.handle(event);
    rmDispatcher.await();
    sendAppUpdateSavedEvent(application);
    assertKilled(application);
    assertAppFinalStateNotSaved(application);
    verifyApplicationFinished(RMAppState.KILLED);
    verifyAppRemovedSchedulerEvent(RMAppState.KILLED);
  }

  @Test
  public void testAppNewReject() throws IOException {
    LOG.info("--- START: testAppNewReject ---");

    RMApp application = createNewTestApp(null);
    // NEW => FAILED event RMAppEventType.APP_REJECTED
    String rejectedText = "Test Application Rejected";
    RMAppEvent event = 
        new RMAppRejectedEvent(application.getApplicationId(), rejectedText);
    application.handle(event);
    rmDispatcher.await();
    sendAppUpdateSavedEvent(application);
    assertFailed(application, rejectedText);
    assertAppFinalStateNotSaved(application);
    verifyApplicationFinished(RMAppState.FAILED);
  }

  @Test (timeout = 30000)
  public void testAppNewSavingKill() throws IOException {
    LOG.info("--- START: testAppNewSavingKill ---");

    RMApp application = testCreateAppNewSaving(null);
    // NEW_SAVING => KILLED event RMAppEventType.KILL
    RMAppEvent event =
        new RMAppEvent(application.getApplicationId(), RMAppEventType.KILL);
    application.handle(event);
    rmDispatcher.await();
    sendAppUpdateSavedEvent(application);
    assertKilled(application);
    verifyApplicationFinished(RMAppState.KILLED);
    verifyAppRemovedSchedulerEvent(RMAppState.KILLED);
  }

  @Test (timeout = 30000)
  public void testAppNewSavingReject() throws IOException {
    LOG.info("--- START: testAppNewSavingReject ---");

    RMApp application = testCreateAppNewSaving(null);
    // NEW_SAVING => FAILED event RMAppEventType.APP_REJECTED
    String rejectedText = "Test Application Rejected";
    RMAppEvent event =
        new RMAppRejectedEvent(application.getApplicationId(), rejectedText);
    application.handle(event);
    rmDispatcher.await();
    sendAppUpdateSavedEvent(application);
    assertFailed(application, rejectedText);
    assertAppFinalStateSaved(application);
    verifyApplicationFinished(RMAppState.FAILED);
  }

  @Test (timeout = 30000)
  public void testAppSubmittedRejected() throws IOException {
    LOG.info("--- START: testAppSubmittedRejected ---");

    RMApp application = testCreateAppSubmittedNoRecovery(null);
    // SUBMITTED => FAILED event RMAppEventType.APP_REJECTED
    String rejectedText = "app rejected";
    RMAppEvent event = 
        new RMAppRejectedEvent(application.getApplicationId(), rejectedText);
    application.handle(event);
    rmDispatcher.await();
    sendAppUpdateSavedEvent(application);
    assertFailed(application, rejectedText);
    assertAppFinalStateSaved(application);
    verifyApplicationFinished(RMAppState.FAILED);
  }

  @Test
  public void testAppSubmittedKill() throws IOException, InterruptedException {
    LOG.info("--- START: testAppSubmittedKill---");
    RMApp application = testCreateAppSubmittedNoRecovery(null);
    // SUBMITTED => KILLED event RMAppEventType.KILL
    RMAppEvent event = new RMAppEvent(application.getApplicationId(),
        RMAppEventType.KILL);
    application.handle(event);
    rmDispatcher.await();
    sendAppUpdateSavedEvent(application);
    assertKilled(application);
    assertAppFinalStateSaved(application);
    verifyApplicationFinished(RMAppState.KILLED);
    verifyAppRemovedSchedulerEvent(RMAppState.KILLED);
  }

  @Test
  public void testAppAcceptedFailed() throws IOException {
    LOG.info("--- START: testAppAcceptedFailed ---");

    RMApp application = testCreateAppAccepted(null);
    // ACCEPTED => ACCEPTED event RMAppEventType.RMAppEventType.ATTEMPT_FAILED
    Assert.assertTrue(maxAppAttempts > 1);
    for (int i=1; i < maxAppAttempts; i++) {
      RMAppEvent event = 
          new RMAppFailedAttemptEvent(application.getApplicationId(), 
              RMAppEventType.ATTEMPT_FAILED, "", false);
      application.handle(event);
      assertAppState(RMAppState.ACCEPTED, application);
      event = 
          new RMAppEvent(application.getApplicationId(), 
              RMAppEventType.APP_ACCEPTED);
      application.handle(event);
      rmDispatcher.await();
      assertAppState(RMAppState.ACCEPTED, application);
    }

    // ACCEPTED => FAILED event RMAppEventType.RMAppEventType.ATTEMPT_FAILED 
    // after max application attempts
    String message = "Test fail";
    RMAppEvent event = 
        new RMAppFailedAttemptEvent(application.getApplicationId(), 
            RMAppEventType.ATTEMPT_FAILED, message, false);
    application.handle(event);
    rmDispatcher.await();
    sendAppUpdateSavedEvent(application);
    assertFailed(application, ".*" + message + ".*Failing the application.*");
    assertAppFinalStateSaved(application);
    verifyApplicationFinished(RMAppState.FAILED);
  }

  @Test
  public void testAppAcceptedKill() throws IOException, InterruptedException {
    LOG.info("--- START: testAppAcceptedKill ---");
    RMApp application = testCreateAppAccepted(null);
    // ACCEPTED => KILLED event RMAppEventType.KILL
    RMAppEvent event = new RMAppEvent(application.getApplicationId(),
        RMAppEventType.KILL);
    application.handle(event);
    rmDispatcher.await();

    assertAppState(RMAppState.KILLING, application);
    RMAppEvent appAttemptKilled =
        new RMAppEvent(application.getApplicationId(),
          RMAppEventType.ATTEMPT_KILLED);
    application.handle(appAttemptKilled);
    assertAppState(RMAppState.FINAL_SAVING, application);
    sendAppUpdateSavedEvent(application);
    assertKilled(application);
    assertAppFinalStateSaved(application);
    verifyApplicationFinished(RMAppState.KILLED);
    verifyAppRemovedSchedulerEvent(RMAppState.KILLED);
  }

  @Test
  public void testAppRunningKill() throws IOException {
    LOG.info("--- START: testAppRunningKill ---");

    RMApp application = testCreateAppRunning(null);
    // RUNNING => KILLED event RMAppEventType.KILL
    RMAppEvent event = 
        new RMAppEvent(application.getApplicationId(), RMAppEventType.KILL);
    application.handle(event);
    rmDispatcher.await();

    // Ignore Attempt_Finished if we were supposed to go to Finished.
    assertAppState(RMAppState.KILLING, application);
    RMAppEvent finishEvent =
        new RMAppFinishedAttemptEvent(application.getApplicationId(), null);
    application.handle(finishEvent);
    assertAppState(RMAppState.KILLING, application);
    sendAttemptUpdateSavedEvent(application);
    sendAppUpdateSavedEvent(application);
    assertKilled(application);
    verifyApplicationFinished(RMAppState.KILLED);
    verifyAppRemovedSchedulerEvent(RMAppState.KILLED);
  }

  @Test
  public void testAppRunningFailed() throws IOException {
    LOG.info("--- START: testAppRunningFailed ---");

    RMApp application = testCreateAppRunning(null);
    RMAppAttempt appAttempt = application.getCurrentAppAttempt();
    int expectedAttemptId = 1;
    Assert.assertEquals(expectedAttemptId, 
        appAttempt.getAppAttemptId().getAttemptId());
    // RUNNING => FAILED/RESTARTING event RMAppEventType.ATTEMPT_FAILED
    Assert.assertTrue(maxAppAttempts > 1);
    for (int i=1; i<maxAppAttempts; i++) {
      RMAppEvent event = 
          new RMAppFailedAttemptEvent(application.getApplicationId(), 
              RMAppEventType.ATTEMPT_FAILED, "", false);
      application.handle(event);
      rmDispatcher.await();
      assertAppState(RMAppState.ACCEPTED, application);
      appAttempt = application.getCurrentAppAttempt();
      Assert.assertEquals(++expectedAttemptId, 
          appAttempt.getAppAttemptId().getAttemptId());
      event = 
          new RMAppEvent(application.getApplicationId(), 
              RMAppEventType.APP_ACCEPTED);
      application.handle(event);
      rmDispatcher.await();
      assertAppState(RMAppState.ACCEPTED, application);
      event = 
          new RMAppEvent(application.getApplicationId(), 
              RMAppEventType.ATTEMPT_REGISTERED);
      application.handle(event);
      rmDispatcher.await();
      assertAppState(RMAppState.RUNNING, application);
    }

    // RUNNING => FAILED/RESTARTING event RMAppEventType.ATTEMPT_FAILED 
    // after max application attempts
    RMAppEvent event = 
        new RMAppFailedAttemptEvent(application.getApplicationId(), 
            RMAppEventType.ATTEMPT_FAILED, "", false);
    application.handle(event);
    rmDispatcher.await();
    sendAppUpdateSavedEvent(application);
    assertFailed(application, ".*Failing the application.*");
    assertAppFinalStateSaved(application);

    // FAILED => FAILED event RMAppEventType.KILL
    event = new RMAppEvent(application.getApplicationId(), RMAppEventType.KILL);
    application.handle(event);
    rmDispatcher.await();
    assertFailed(application, ".*Failing the application.*");
    assertAppFinalStateSaved(application);
    verifyApplicationFinished(RMAppState.FAILED);
  }

  @Test
  public void testAppAtFinishingIgnoreKill() throws IOException {
    LOG.info("--- START: testAppAtFinishingIgnoreKill ---");

    RMApp application = testCreateAppFinishing(null);
    // FINISHING => FINISHED event RMAppEventType.KILL
    RMAppEvent event =
        new RMAppEvent(application.getApplicationId(), RMAppEventType.KILL);
    application.handle(event);
    rmDispatcher.await();
    assertAppState(RMAppState.FINISHING, application);
  }

  // While App is at FINAL_SAVING, Attempt_Finished event may come before
  // App_Saved event, we stay on FINAL_SAVING on Attempt_Finished event
  // and then directly jump from FINAL_SAVING to FINISHED state on App_Saved
  // event
  @Test
  public void testAppFinalSavingToFinished() throws IOException {
    LOG.info("--- START: testAppFinalSavingToFinished ---");

    RMApp application = testCreateAppFinalSaving(null);
    final String diagMsg = "some diagnostics";
    // attempt_finished event comes before attempt_saved event
    RMAppEvent event =
        new RMAppFinishedAttemptEvent(application.getApplicationId(), diagMsg);
    application.handle(event);
    assertAppState(RMAppState.FINAL_SAVING, application);
    RMAppEvent appUpdated =
        new RMAppEvent(application.getApplicationId(), RMAppEventType.APP_UPDATE_SAVED);
    application.handle(appUpdated);
    assertAppState(RMAppState.FINISHED, application);

    assertTimesAtFinish(application);
    // finished without a proper unregister implies failed
    assertFinalAppStatus(FinalApplicationStatus.FAILED, application);
    Assert.assertTrue("Finished app missing diagnostics", application
      .getDiagnostics().indexOf(diagMsg) != -1);
  }

  @Test
  public void testAppFinishedFinished() throws IOException {
    LOG.info("--- START: testAppFinishedFinished ---");

    RMApp application = testCreateAppFinished(null, "");
    // FINISHED => FINISHED event RMAppEventType.KILL
    RMAppEvent event = 
        new RMAppEvent(application.getApplicationId(), RMAppEventType.KILL);
    application.handle(event);
    rmDispatcher.await();
    assertTimesAtFinish(application);
    assertAppState(RMAppState.FINISHED, application);
    StringBuilder diag = application.getDiagnostics();
    Assert.assertEquals("application diagnostics is not correct",
        "", diag.toString());
    verifyApplicationFinished(RMAppState.FINISHED);
  }

  @Test (timeout = 30000)
  public void testAppFailedFailed() throws IOException {
    LOG.info("--- START: testAppFailedFailed ---");

    RMApp application = testCreateAppNewSaving(null);

    // NEW_SAVING => FAILED event RMAppEventType.APP_REJECTED
    RMAppEvent event =
        new RMAppRejectedEvent(application.getApplicationId(), "");
    application.handle(event);
    rmDispatcher.await();
    sendAppUpdateSavedEvent(application);
    assertTimesAtFinish(application);
    assertAppState(RMAppState.FAILED, application);

    // FAILED => FAILED event RMAppEventType.KILL
    event =
        new RMAppEvent(application.getApplicationId(), RMAppEventType.KILL);
    application.handle(event);
    rmDispatcher.await();
    assertTimesAtFinish(application);
    assertAppState(RMAppState.FAILED, application);
    verifyApplicationFinished(RMAppState.FAILED);

    assertTimesAtFinish(application);
    assertAppState(RMAppState.FAILED, application);
  }

  @Test (timeout = 30000)
  public void testAppKilledKilled() throws IOException {
    LOG.info("--- START: testAppKilledKilled ---");

    RMApp application = testCreateAppRunning(null);

    // RUNNING => KILLED event RMAppEventType.KILL
    RMAppEvent event = 
        new RMAppEvent(application.getApplicationId(), RMAppEventType.KILL);
    application.handle(event);
    rmDispatcher.await();
    sendAttemptUpdateSavedEvent(application);
    sendAppUpdateSavedEvent(application);
    assertTimesAtFinish(application);
    assertAppState(RMAppState.KILLED, application);

    // KILLED => KILLED event RMAppEventType.ATTEMPT_FINISHED
    event = new RMAppFinishedAttemptEvent(
        application.getApplicationId(), "");
    application.handle(event);
    rmDispatcher.await();
    assertTimesAtFinish(application);
    assertAppState(RMAppState.KILLED, application);

    // KILLED => KILLED event RMAppEventType.ATTEMPT_FAILED
    event = 
        new RMAppFailedAttemptEvent(application.getApplicationId(), 
            RMAppEventType.ATTEMPT_FAILED, "", false);
    application.handle(event);
    rmDispatcher.await();
    assertTimesAtFinish(application);
    assertAppState(RMAppState.KILLED, application);


    // KILLED => KILLED event RMAppEventType.KILL
    event = new RMAppEvent(application.getApplicationId(), RMAppEventType.KILL);
    application.handle(event);
    rmDispatcher.await();
    assertTimesAtFinish(application);
    assertAppState(RMAppState.KILLED, application);
    verifyApplicationFinished(RMAppState.KILLED);

    assertTimesAtFinish(application);
    assertAppState(RMAppState.KILLED, application);
  }
  
  @Test(timeout = 30000)
  public void testAppsRecoveringStates() throws Exception {
    RMState state = new RMState();
    Map<ApplicationId, ApplicationState> applicationState =
        state.getApplicationState();
    createRMStateForApplications(applicationState, RMAppState.FINISHED);
    createRMStateForApplications(applicationState, RMAppState.KILLED);
    createRMStateForApplications(applicationState, RMAppState.FAILED);
    for (ApplicationState appState : applicationState.values()) {
      testRecoverApplication(appState, state);
    }
  }
  
  public void testRecoverApplication(ApplicationState appState, RMState rmState)
      throws Exception {
    ApplicationSubmissionContext submissionContext =
        appState.getApplicationSubmissionContext();
    RMAppImpl application =
        new RMAppImpl(appState.getAppId(), rmContext, conf,
            submissionContext.getApplicationName(), null,
            submissionContext.getQueue(), submissionContext, null, null,
            appState.getSubmitTime(), submissionContext.getApplicationType(),
            submissionContext.getApplicationTags());
    Assert.assertEquals(RMAppState.NEW, application.getState());
    application.recover(rmState);

    // Application final status looked from recoveredFinalStatus
    Assert.assertTrue("Application is not in recoveredFinalStatus.",
        RMAppImpl.isAppInFinalState(application));

    // Trigger RECOVER event.
    application.handle(new RMAppEvent(appState.getAppId(),
        RMAppEventType.RECOVER));
    rmDispatcher.await();
    RMAppState finalState = appState.getState();
    Assert.assertEquals("Application is not in finalState.", finalState,
        application.getState());
  }
  
  public void createRMStateForApplications(
      Map<ApplicationId, ApplicationState> applicationState,
      RMAppState rmAppState) {
    RMApp app = createNewTestApp(null);
    ApplicationState appState =
        new ApplicationState(app.getSubmitTime(), app.getStartTime(),
            app.getApplicationSubmissionContext(), app.getUser(), rmAppState,
            null, app.getFinishTime());
    applicationState.put(app.getApplicationId(), appState);
  }
  
  @Test
  public void testGetAppReport() {
    RMApp app = createNewTestApp(null);
    assertAppState(RMAppState.NEW, app);
    ApplicationReport report = app.createAndGetApplicationReport(null, true);
    Assert.assertNotNull(report.getApplicationResourceUsageReport());
    Assert.assertEquals(report.getApplicationResourceUsageReport(),RMServerUtils.DUMMY_APPLICATION_RESOURCE_USAGE_REPORT);
    report = app.createAndGetApplicationReport("clientuser", true);
    Assert.assertNotNull(report.getApplicationResourceUsageReport());
  }

  private void verifyApplicationFinished(RMAppState state) {
    ArgumentCaptor<RMAppState> finalState =
        ArgumentCaptor.forClass(RMAppState.class);
    verify(writer).applicationFinished(any(RMApp.class), finalState.capture());
    Assert.assertEquals(state, finalState.getValue());
  }
  
  private void verifyAppRemovedSchedulerEvent(RMAppState finalState) {
    Assert.assertEquals(SchedulerEventType.APP_REMOVED,
      schedulerDispatcher.lastSchedulerEvent.getType());
    if(schedulerDispatcher.lastSchedulerEvent instanceof 
        AppRemovedSchedulerEvent) {
      AppRemovedSchedulerEvent appRemovedEvent =
          (AppRemovedSchedulerEvent) schedulerDispatcher.lastSchedulerEvent;
      Assert.assertEquals(finalState, appRemovedEvent.getFinalState());
    }
  }
}
