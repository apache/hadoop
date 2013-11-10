/*
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRMRestart {

  private YarnConfiguration conf;

  // Fake rmAddr for token-renewal
  private static InetSocketAddress rmAddr;

  @Before
  public void setup() throws UnknownHostException {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    ExitUtil.disableSystemExit();
    conf = new YarnConfiguration();
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    rmAddr = new InetSocketAddress("localhost", 8032);
    Assert.assertTrue(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS > 1);
  }

  @Test (timeout=180000)
  public void testRMRestart() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();
    Map<ApplicationId, ApplicationState> rmAppState = 
                                                  rmState.getApplicationState();
    
    
    // PHASE 1: create state in an RM
    
    // start RM
    MockRM rm1 = new MockRM(conf, memStore);
    
    // start like normal because state is empty
    rm1.start();
    
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    MockNM nm2 =
        new MockNM("127.0.0.2:5678", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm2.registerNode(); // nm2 will not heartbeat with RM1
    
    // create app that will finish and the final state should be saved.
    RMApp app0 = rm1.submitApp(200);
    RMAppAttempt attempt0 = app0.getCurrentAppAttempt();
    // spot check that app is saved
    Assert.assertEquals(1, rmAppState.size());
    nm1.nodeHeartbeat(true);
    MockAM am0 = rm1.sendAMLaunched(attempt0.getAppAttemptId());
    am0.registerAppAttempt();
    finishApplicationMaster(app0, rm1, nm1, am0);

    // create app that gets launched and does allocate before RM restart
    RMApp app1 = rm1.submitApp(200);
    // assert app1 info is saved
    ApplicationState appState = rmAppState.get(app1.getApplicationId());
    Assert.assertNotNull(appState);
    Assert.assertEquals(0, appState.getAttemptCount());
    Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), app1.getApplicationSubmissionContext()
        .getApplicationId());

    //kick the scheduling to allocate AM container
    nm1.nodeHeartbeat(true);
    
    // assert app1 attempt is saved
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    ApplicationAttemptId attemptId1 = attempt1.getAppAttemptId();
    rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    Assert.assertEquals(1, appState.getAttemptCount());
    ApplicationAttemptState attemptState = 
                                appState.getAttempt(attemptId1);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1), 
                        attemptState.getMasterContainer().getId());
    
    // launch the AM
    MockAM am1 = rm1.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    // AM request for containers
    am1.allocate("127.0.0.1" , 1000, 1, new ArrayList<ContainerId>());    
    // kick the scheduler
    nm1.nodeHeartbeat(true);
    List<Container> conts = am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() == 0) {
      nm1.nodeHeartbeat(true);
      conts.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }
    
    // create app that does not get launched by RM before RM restart
    RMApp app2 = rm1.submitApp(200);

    // assert app2 info is saved
    appState = rmAppState.get(app2.getApplicationId());
    Assert.assertNotNull(appState);
    Assert.assertEquals(0, appState.getAttemptCount());
    Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), app2.getApplicationSubmissionContext()
        .getApplicationId());
    
    // create unmanaged app
    RMApp appUnmanaged = rm1.submitApp(200, "someApp", "someUser", null, true,
        null, conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null);
    ApplicationAttemptId unmanagedAttemptId = 
                        appUnmanaged.getCurrentAppAttempt().getAppAttemptId();
    // assert appUnmanaged info is saved
    ApplicationId unmanagedAppId = appUnmanaged.getApplicationId();
    appState = rmAppState.get(unmanagedAppId);
    Assert.assertNotNull(appState);
    // wait for attempt to reach LAUNCHED state 
    rm1.waitForState(unmanagedAttemptId, RMAppAttemptState.LAUNCHED);
    rm1.waitForState(unmanagedAppId, RMAppState.ACCEPTED);
    // assert unmanaged attempt info is saved
    Assert.assertEquals(1, appState.getAttemptCount());
    Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), appUnmanaged.getApplicationSubmissionContext()
        .getApplicationId());  
    
    // PHASE 2: create new RM and start from old state
    
    // create new RM to represent restart and recover state
    MockRM rm2 = new MockRM(conf, memStore);
    
    // start new RM
    rm2.start();
    
    // change NM to point to new RM
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm2.setResourceTrackerService(rm2.getResourceTrackerService());

    // verify load of old state
    // 4 apps are loaded.
    // FINISHED app and attempt is also loaded back.
    // Unmanaged app state is still loaded back but it cannot be restarted by
    // the RM. this will change with work preserving RM restart in which AMs/NMs
    // are not rebooted.
    Assert.assertEquals(4, rm2.getRMContext().getRMApps().size());
    // check that earlier finished app and attempt is also loaded back and move
    // to finished state.
    rm2.waitForState(app0.getApplicationId(), RMAppState.FINISHED);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.FINISHED);

    // verify correct number of attempts and other data
    RMApp loadedApp1 = rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    Assert.assertNotNull(loadedApp1);
    //Assert.assertEquals(1, loadedApp1.getAppAttempts().size());
    Assert.assertEquals(app1.getApplicationSubmissionContext()
        .getApplicationId(), loadedApp1.getApplicationSubmissionContext()
        .getApplicationId());
    
    RMApp loadedApp2 = rm2.getRMContext().getRMApps().get(app2.getApplicationId());
    Assert.assertNotNull(loadedApp2);
    //Assert.assertEquals(0, loadedApp2.getAppAttempts().size());
    Assert.assertEquals(app2.getApplicationSubmissionContext()
        .getApplicationId(), loadedApp2.getApplicationSubmissionContext()
        .getApplicationId());
    
    // verify state machine kicked into expected states
    rm2.waitForState(loadedApp1.getApplicationId(), RMAppState.ACCEPTED);
    rm2.waitForState(loadedApp2.getApplicationId(), RMAppState.ACCEPTED);
    
    // verify new attempts created
    Assert.assertEquals(2, loadedApp1.getAppAttempts().size());
    Assert.assertEquals(1, loadedApp2.getAppAttempts().size());
    
    // verify old AM is not accepted
    // change running AM to talk to new RM
    am1.setAMRMProtocol(rm2.getApplicationMasterService());
    AllocateResponse allocResponse = am1.allocate(
        new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>());
    Assert.assertTrue(allocResponse.getAMCommand() == AMCommand.AM_RESYNC);
    
    // NM should be rebooted on heartbeat, even first heartbeat for nm2
    NodeHeartbeatResponse hbResponse = nm1.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, hbResponse.getNodeAction());
    hbResponse = nm2.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, hbResponse.getNodeAction());
    
    // new NM to represent NM re-register
    nm1 = rm2.registerNode("127.0.0.1:1234", 15120);
    nm2 = rm2.registerNode("127.0.0.2:5678", 15120);

    // verify no more reboot response sent
    hbResponse = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.RESYNC != hbResponse.getNodeAction());
    hbResponse = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.RESYNC != hbResponse.getNodeAction());
    
    // assert app1 attempt is saved
    attempt1 = loadedApp1.getCurrentAppAttempt();
    attemptId1 = attempt1.getAppAttemptId();
    rm2.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    appState = rmAppState.get(loadedApp1.getApplicationId());
    attemptState = appState.getAttempt(attemptId1);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1), 
                        attemptState.getMasterContainer().getId());

    // Nodes on which the AM's run 
    MockNM am1Node = nm1;
    if (attemptState.getMasterContainer().getNodeId().toString()
        .contains("127.0.0.2")) {
      am1Node = nm2;
    }

    // assert app2 attempt is saved
    RMAppAttempt attempt2 = loadedApp2.getCurrentAppAttempt();
    ApplicationAttemptId attemptId2 = attempt2.getAppAttemptId();
    rm2.waitForState(attemptId2, RMAppAttemptState.ALLOCATED);
    appState = rmAppState.get(loadedApp2.getApplicationId());
    attemptState = appState.getAttempt(attemptId2);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId2, 1), 
                        attemptState.getMasterContainer().getId());

    MockNM am2Node = nm1;
    if (attemptState.getMasterContainer().getNodeId().toString()
        .contains("127.0.0.2")) {
      am2Node = nm2;
    }
    
    // start the AM's
    am1 = rm2.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    
    MockAM am2 = rm2.sendAMLaunched(attempt2.getAppAttemptId());
    am2.registerAppAttempt();

    //request for containers
    am1.allocate("127.0.0.1" , 1000, 3, new ArrayList<ContainerId>());
    am2.allocate("127.0.0.2" , 1000, 1, new ArrayList<ContainerId>());
    
    // verify container allocate continues to work
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    conts = am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() == 0) {
      nm1.nodeHeartbeat(true);
      nm2.nodeHeartbeat(true);
      conts.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }
    // finish the AMs
    finishApplicationMaster(loadedApp1, rm2, am1Node, am1);
    finishApplicationMaster(loadedApp2, rm2, am2Node, am2);

    // stop RM's
    rm2.stop();
    rm1.stop();
    
    // completed apps are not removed immediately after app finish
    // And finished app is also loaded back.
    Assert.assertEquals(4, rmAppState.size());
 }

  @Test
  public void testRMRestartAppRunningAMFailed() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();
    Map<ApplicationId, ApplicationState> rmAppState =
        rmState.getApplicationState();

    // start RM
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // fail the AM by sending CONTAINER_FINISHED event without registering.
    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am0.waitForState(RMAppAttemptState.FAILED);

    ApplicationState appState = rmAppState.get(app0.getApplicationId());
    // assert the AM failed state is saved.
    Assert.assertEquals(RMAppAttemptState.FAILED,
      appState.getAttempt(am0.getApplicationAttemptId()).getState());

    // assert app state has not been saved.
    Assert.assertNull(rmAppState.get(app0.getApplicationId()).getState());

    // new AM started but not registered, app still stays at ACCECPTED state.
    rm1.waitForState(app0.getApplicationId(), RMAppState.ACCEPTED);

    // start new RM
    MockRM rm2 = new MockRM(conf, memStore);
    rm2.start();
    // assert the previous AM state is loaded back on RM recovery.
    RMApp recoveredApp =
        rm2.getRMContext().getRMApps().get(app0.getApplicationId());
    Assert.assertEquals(RMAppAttemptState.FAILED, recoveredApp
      .getAppAttempts().get(am0.getApplicationAttemptId()).getAppAttemptState());
  }

  @Test
  public void testRMRestartFailedApp() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();
    Map<ApplicationId, ApplicationState> rmAppState =
        rmState.getApplicationState();

    // start RM
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // fail the AM by sending CONTAINER_FINISHED event without registering.
    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am0.waitForState(RMAppAttemptState.FAILED);
    rm1.waitForState(app0.getApplicationId(), RMAppState.FAILED);

    // assert the app/attempt failed state is saved.
    ApplicationState appState = rmAppState.get(app0.getApplicationId());
    Assert.assertEquals(RMAppState.FAILED, appState.getState());
    Assert.assertEquals(RMAppAttemptState.FAILED,
      appState.getAttempt(am0.getApplicationAttemptId()).getState());

    // start new RM
    MockRM rm2 = new MockRM(conf, memStore);
    rm2.start();
    RMApp loadedApp0 = rm2.getRMContext().getRMApps().get(app0.getApplicationId());
    rm2.waitForState(app0.getApplicationId(), RMAppState.FAILED);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    // no new attempt is created.
    Assert.assertEquals(1, loadedApp0.getAppAttempts().size());

    verifyAppReportAfterRMRestart(app0, rm2);
    Assert.assertTrue(app0.getDiagnostics().toString()
      .contains("Failing the application."));
    // failed diagnostics from attempt is lost because the diagnostics from
    // attempt is not yet available by the time app is saving the app state.
  }

  @Test
  public void testRMRestartKilledApp() throws Exception{
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();
    Map<ApplicationId, ApplicationState> rmAppState =
        rmState.getApplicationState();

    // start RM
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // kill the app.
    rm1.killApp(app0.getApplicationId());
    rm1.waitForState(app0.getApplicationId(), RMAppState.KILLED);
    rm1.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.KILLED);

    // killed state is saved.
    ApplicationState appState = rmAppState.get(app0.getApplicationId());
    Assert.assertEquals(RMAppState.KILLED, appState.getState());
    Assert.assertEquals(RMAppAttemptState.KILLED,
      appState.getAttempt(am0.getApplicationAttemptId()).getState());

    // restart rm
    MockRM rm2 = new MockRM(conf, memStore);
    rm2.start();
    RMApp loadedApp0 = rm2.getRMContext().getRMApps().get(app0.getApplicationId());
    rm2.waitForState(app0.getApplicationId(), RMAppState.KILLED);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.KILLED);
    // no new attempt is created.
    Assert.assertEquals(1, loadedApp0.getAppAttempts().size());

    ApplicationReport appReport = verifyAppReportAfterRMRestart(app0, rm2);
    Assert.assertEquals(app0.getDiagnostics().toString(),
      appReport.getDiagnostics());
  }

  @Test
  public void testRMRestartSucceededApp() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();
    Map<ApplicationId, ApplicationState> rmAppState =
        rmState.getApplicationState();

    // start RM
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create an app and finish the app.
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // unregister am
    FinishApplicationMasterRequest req =
        FinishApplicationMasterRequest.newInstance(
          FinalApplicationStatus.SUCCEEDED, "diagnostics", "trackingUrl");
    finishApplicationMaster(app0, rm1, nm1, am0, req);
 
    // check the state store about the unregistered info.
    ApplicationState appState = rmAppState.get(app0.getApplicationId());
    ApplicationAttemptState attemptState0 =
      appState.getAttempt(am0.getApplicationAttemptId());
    Assert.assertEquals("diagnostics", attemptState0.getDiagnostics());
    Assert.assertEquals(FinalApplicationStatus.SUCCEEDED,
      attemptState0.getFinalApplicationStatus());
    Assert.assertEquals("trackingUrl", attemptState0.getFinalTrackingUrl());
    Assert.assertEquals(app0.getFinishTime(), appState.getFinishTime());

    // restart rm
    MockRM rm2 = new MockRM(conf, memStore);
    rm2.start();

    // verify application report returns the same app info as the app info
    // before RM restarts.
    ApplicationReport appReport = verifyAppReportAfterRMRestart(app0, rm2);
    Assert.assertEquals(FinalApplicationStatus.SUCCEEDED,
      appReport.getFinalApplicationStatus());
    Assert.assertEquals("trackingUrl", appReport.getOriginalTrackingUrl());
  }

  @Test
  public void testRMRestartGetApplicationList() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    // start RM
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // a succeeded app.
    RMApp app0 = rm1.submitApp(200, "name", "user", null,
      false, "default", 1, null, "myType");
    MockAM am0 = launchAM(app0, rm1, nm1);
    finishApplicationMaster(app0, rm1, nm1, am0);

    // a failed app.
    RMApp app1 = rm1.submitApp(200, "name", "user", null,
      false, "default", 1, null, "myType");
    MockAM am1 = launchAM(app1, rm1, nm1);
    // fail the AM by sending CONTAINER_FINISHED event without registering.
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am1.waitForState(RMAppAttemptState.FAILED);
    rm1.waitForState(app1.getApplicationId(), RMAppState.FAILED);

    // a killed app.
    RMApp app2 = rm1.submitApp(200, "name", "user", null,
      false, "default", 1, null, "myType");
    MockAM am2 = launchAM(app2, rm1, nm1);
    rm1.killApp(app2.getApplicationId());
    rm1.waitForState(app2.getApplicationId(), RMAppState.KILLED);
    rm1.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.KILLED);

    // restart rm
    MockRM rm2 = new MockRM(conf, memStore);
    rm2.start();

    GetApplicationsRequest request1 =
        GetApplicationsRequest.newInstance(EnumSet.of(
          YarnApplicationState.FINISHED, YarnApplicationState.KILLED,
          YarnApplicationState.FAILED));
    GetApplicationsResponse response1 =
        rm2.getClientRMService().getApplications(request1);
    List<ApplicationReport> appList1 = response1.getApplicationList();

    // assert all applications exist according to application state after RM
    // restarts.
    boolean forApp0 = false, forApp1 = false, forApp2 = false;
    for (ApplicationReport report : appList1) {
      if (report.getApplicationId().equals(app0.getApplicationId())) {
        Assert.assertEquals(YarnApplicationState.FINISHED,
          report.getYarnApplicationState());
        forApp0 = true;
      }
      if (report.getApplicationId().equals(app1.getApplicationId())) {
        Assert.assertEquals(YarnApplicationState.FAILED,
          report.getYarnApplicationState());
        forApp1 = true;
      }
      if (report.getApplicationId().equals(app2.getApplicationId())) {
        Assert.assertEquals(YarnApplicationState.KILLED,
          report.getYarnApplicationState());
        forApp2 = true;
      }
    }
    Assert.assertTrue(forApp0 && forApp1 && forApp2);

    // assert all applications exist according to application type after RM
    // restarts.
    Set<String> appTypes = new HashSet<String>();
    appTypes.add("myType");
    GetApplicationsRequest request2 =
        GetApplicationsRequest.newInstance(appTypes);
    GetApplicationsResponse response2 =
        rm2.getClientRMService().getApplications(request2);
    List<ApplicationReport> appList2 = response2.getApplicationList();
    Assert.assertTrue(3 == appList2.size());
  }

  private MockAM launchAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    nm.nodeHeartbeat(true);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    return am;
  }

  private ApplicationReport verifyAppReportAfterRMRestart(RMApp app, MockRM rm)
      throws Exception {
    GetApplicationReportRequest reportRequest =
        GetApplicationReportRequest.newInstance(app.getApplicationId());
    GetApplicationReportResponse response =
        rm.getClientRMService().getApplicationReport(reportRequest);
    ApplicationReport report = response.getApplicationReport();
    Assert.assertEquals(app.getStartTime(), report.getStartTime());
    Assert.assertEquals(app.getFinishTime(), report.getFinishTime());
    Assert.assertEquals(app.createApplicationState(),
      report.getYarnApplicationState());
    Assert.assertTrue(1 == report.getProgress());
    return response.getApplicationReport();
  }

  private void finishApplicationMaster(RMApp rmApp, MockRM rm, MockNM nm,
      MockAM am) throws Exception {
    final FinishApplicationMasterRequest req =
        FinishApplicationMasterRequest.newInstance(
          FinalApplicationStatus.SUCCEEDED, "", "");
    finishApplicationMaster(rmApp, rm, nm, am, req);
  }

  private void finishApplicationMaster(RMApp rmApp, MockRM rm, MockNM nm,
      MockAM am, FinishApplicationMasterRequest req) throws Exception {
    RMState rmState =
        ((MemoryRMStateStore) rm.getRMContext().getStateStore()).getState();
    Map<ApplicationId, ApplicationState> rmAppState =
        rmState.getApplicationState();
    am.unregisterAppAttempt(req);
    am.waitForState(RMAppAttemptState.FINISHING);
    nm.nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am.waitForState(RMAppAttemptState.FINISHED);
    rm.waitForState(rmApp.getApplicationId(), RMAppState.FINISHED);
    // check that app/attempt is saved with the final state
    ApplicationState appState = rmAppState.get(rmApp.getApplicationId());
    Assert
      .assertEquals(RMAppState.FINISHED, appState.getState());
    Assert.assertEquals(RMAppAttemptState.FINISHED,
      appState.getAttempt(am.getApplicationAttemptId()).getState());
  }

  @Test
  public void testRMRestartOnMaxAppAttempts() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();

    Map<ApplicationId, ApplicationState> rmAppState =
        rmState.getApplicationState();  
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // submit an app with maxAppAttempts equals to 1
    RMApp app1 = rm1.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", 1,
          null);
    // submit an app with maxAppAttempts equals to -1
    RMApp app2 = rm1.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null);

    // assert app1 info is saved
    ApplicationState appState = rmAppState.get(app1.getApplicationId());
    Assert.assertNotNull(appState);
    Assert.assertEquals(0, appState.getAttemptCount());
    Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), app1.getApplicationSubmissionContext()
        .getApplicationId());

    // Allocate the AM
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt = app1.getCurrentAppAttempt();
    ApplicationAttemptId attemptId1 = attempt.getAppAttemptId();
    rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    Assert.assertEquals(1, appState.getAttemptCount());
    ApplicationAttemptState attemptState = 
                                appState.getAttempt(attemptId1);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1), 
                        attemptState.getMasterContainer().getId());

    // start new RM   
    MockRM rm2 = new MockRM(conf, memStore);
    rm2.start();

    // verify that maxAppAttempts is set to global value
    Assert.assertEquals(2, 
        rm2.getRMContext().getRMApps().get(app2.getApplicationId())
        .getMaxAppAttempts());

    // app1 and app2 are loaded back, but app1 failed because it's
    // hitting max-retry.
    Assert.assertEquals(2, rm2.getRMContext().getRMApps().size());
    rm2.waitForState(app1.getApplicationId(), RMAppState.FAILED);
    rm2.waitForState(app2.getApplicationId(), RMAppState.ACCEPTED);

    // app1 failed state is saved in state store. app2 final saved state is not
    // determined yet.
    Assert.assertEquals(RMAppState.FAILED,
      rmAppState.get(app1.getApplicationId()).getState());
    Assert.assertNull(rmAppState.get(app2.getApplicationId()).getState());

    // stop the RM  
    rm1.stop();
    rm2.stop();
  }

  @Test
  public void testDelegationTokenRestoredInDelegationTokenRenewer()
      throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();

    Map<ApplicationId, ApplicationState> rmAppState =
        rmState.getApplicationState();
    MockRM rm1 = new TestSecurityMockRM(conf, memStore);
    rm1.start();

    HashSet<Token<RMDelegationTokenIdentifier>> tokenSet =
        new HashSet<Token<RMDelegationTokenIdentifier>>();

    // create an empty credential
    Credentials ts = new Credentials();

    // create tokens and add into credential
    Text userText1 = new Text("user1");
    RMDelegationTokenIdentifier dtId1 =
        new RMDelegationTokenIdentifier(userText1, new Text("renewer1"),
          userText1);
    Token<RMDelegationTokenIdentifier> token1 =
        new Token<RMDelegationTokenIdentifier>(dtId1,
          rm1.getRMDTSecretManager());
    SecurityUtil.setTokenService(token1, rmAddr);
    ts.addToken(userText1, token1);
    tokenSet.add(token1);

    Text userText2 = new Text("user2");
    RMDelegationTokenIdentifier dtId2 =
        new RMDelegationTokenIdentifier(userText2, new Text("renewer2"),
          userText2);
    Token<RMDelegationTokenIdentifier> token2 =
        new Token<RMDelegationTokenIdentifier>(dtId2,
          rm1.getRMDTSecretManager());
    SecurityUtil.setTokenService(token2, rmAddr);
    ts.addToken(userText2, token2);
    tokenSet.add(token2);

    // submit an app with customized credential
    RMApp app = rm1.submitApp(200, "name", "user",
        new HashMap<ApplicationAccessType, String>(), false, "default", 1, ts);

    // assert app info is saved
    ApplicationState appState = rmAppState.get(app.getApplicationId());
    Assert.assertNotNull(appState);

    // assert delegation tokens exist in rm1 DelegationTokenRenewr
    Assert.assertEquals(tokenSet, rm1.getRMContext()
      .getDelegationTokenRenewer().getDelegationTokens());

    // assert delegation tokens are saved
    DataOutputBuffer dob = new DataOutputBuffer();
    ts.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens =
        ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    securityTokens.rewind();
    Assert.assertEquals(securityTokens, appState
      .getApplicationSubmissionContext().getAMContainerSpec()
      .getTokens());

    // start new RM
    MockRM rm2 = new TestSecurityMockRM(conf, memStore);
    rm2.start();

    // verify tokens are properly populated back to rm2 DelegationTokenRenewer
    Assert.assertEquals(tokenSet, rm2.getRMContext()
      .getDelegationTokenRenewer().getDelegationTokens());

    // stop the RM
    rm1.stop();
    rm2.stop();
  }

  @Test
  public void testAppAttemptTokensRestoredOnRMRestart() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    UserGroupInformation.setConfiguration(conf);

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();

    Map<ApplicationId, ApplicationState> rmAppState =
        rmState.getApplicationState();
    MockRM rm1 = new TestSecurityMockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("0.0.0.0:4321", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // submit an app
    RMApp app1 =
        rm1.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), "default");

    // assert app info is saved
    ApplicationState appState = rmAppState.get(app1.getApplicationId());
    Assert.assertNotNull(appState);

    // Allocate the AM
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    ApplicationAttemptId attemptId1 = attempt1.getAppAttemptId();
    rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);

    // assert attempt info is saved
    ApplicationAttemptState attemptState = appState.getAttempt(attemptId1);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1),
      attemptState.getMasterContainer().getId());

    // the appToken and clientTokenMasterKey that are generated when
    // RMAppAttempt is created,
    HashSet<Token<?>> tokenSet = new HashSet<Token<?>>();
    tokenSet.add(attempt1.getAMRMToken());
    byte[] clientTokenMasterKey =
        attempt1.getClientTokenMasterKey().getEncoded();

    // assert application credentials are saved
    Credentials savedCredentials = attemptState.getAppAttemptCredentials();
    HashSet<Token<?>> savedTokens = new HashSet<Token<?>>();
    savedTokens.addAll(savedCredentials.getAllTokens());
    Assert.assertEquals(tokenSet, savedTokens);
    Assert.assertArrayEquals("client token master key not saved",
        clientTokenMasterKey, savedCredentials.getSecretKey(
            RMStateStore.AM_CLIENT_TOKEN_MASTER_KEY_NAME));

    // start new RM
    MockRM rm2 = new TestSecurityMockRM(conf, memStore);
    rm2.start();

    RMApp loadedApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    RMAppAttempt loadedAttempt1 = loadedApp1.getRMAppAttempt(attemptId1);

    // assert loaded attempt recovered attempt tokens
    Assert.assertNotNull(loadedAttempt1);
    savedTokens.clear();
    savedTokens.add(loadedAttempt1.getAMRMToken());
    Assert.assertEquals(tokenSet, savedTokens);

    // assert client token master key is recovered back to api-versioned
    // client token master key
    Assert.assertEquals("client token master key not restored",
        attempt1.getClientTokenMasterKey(),
        loadedAttempt1.getClientTokenMasterKey());

    // assert ClientTokenSecretManager also knows about the key
    Assert.assertArrayEquals(clientTokenMasterKey,
        rm2.getClientToAMTokenSecretManager().getMasterKey(attemptId1)
            .getEncoded());

    // assert AMRMTokenSecretManager also knows about the AMRMToken password
    Token<AMRMTokenIdentifier> amrmToken = loadedAttempt1.getAMRMToken();
    Assert.assertArrayEquals(amrmToken.getPassword(),
      rm2.getAMRMTokenSecretManager().retrievePassword(
        amrmToken.decodeIdentifier()));
    rm1.stop();
    rm2.stop();
  }

  @Test
  public void testRMDelegationTokenRestoredOnRMRestart() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    
    conf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    conf.set(YarnConfiguration.RM_ADDRESS, "localhost:8032");
    UserGroupInformation.setConfiguration(conf);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();

    Map<ApplicationId, ApplicationState> rmAppState =
        rmState.getApplicationState();
    Map<RMDelegationTokenIdentifier, Long> rmDTState =
        rmState.getRMDTSecretManagerState().getTokenState();
    Set<DelegationKey> rmDTMasterKeyState =
        rmState.getRMDTSecretManagerState().getMasterKeyState();

    MockRM rm1 = new TestSecurityMockRM(conf, memStore);

    rm1.start();

    // create an empty credential
    Credentials ts = new Credentials();

    // request a token and add into credential
    GetDelegationTokenRequest request1 =
        GetDelegationTokenRequest.newInstance("renewer1");
    UserGroupInformation.getCurrentUser().setAuthenticationMethod(
        AuthMethod.KERBEROS);
    GetDelegationTokenResponse response1 =
        rm1.getClientRMService().getDelegationToken(request1);
    org.apache.hadoop.yarn.api.records.Token delegationToken1 =
        response1.getRMDelegationToken();
    Token<RMDelegationTokenIdentifier> token1 =
        ConverterUtils.convertFromYarn(delegationToken1, rmAddr);
    RMDelegationTokenIdentifier dtId1 = token1.decodeIdentifier();

    HashSet<RMDelegationTokenIdentifier> tokenIdentSet =
        new HashSet<RMDelegationTokenIdentifier>();
    ts.addToken(token1.getService(), token1);
    tokenIdentSet.add(dtId1);

    // submit an app with customized credential
    RMApp app = rm1.submitApp(200, "name", "user",
        new HashMap<ApplicationAccessType, String>(), false, "default", 1, ts);

    // assert app info is saved
    ApplicationState appState = rmAppState.get(app.getApplicationId());
    Assert.assertNotNull(appState);

    // assert all master keys are saved
    Set<DelegationKey> allKeysRM1 = rm1.getRMDTSecretManager().getAllMasterKeys();
    Assert.assertEquals(allKeysRM1, rmDTMasterKeyState);

    // assert all tokens are saved
    Map<RMDelegationTokenIdentifier, Long> allTokensRM1 =
        rm1.getRMDTSecretManager().getAllTokens();
    Assert.assertEquals(tokenIdentSet, allTokensRM1.keySet());
    Assert.assertEquals(allTokensRM1, rmDTState);
    
    // assert sequence number is saved
    Assert.assertEquals(
      rm1.getRMDTSecretManager().getLatestDTSequenceNumber(),
      rmState.getRMDTSecretManagerState().getDTSequenceNumber());

    // request one more token
    GetDelegationTokenRequest request2 =
        GetDelegationTokenRequest.newInstance("renewer2");
    GetDelegationTokenResponse response2 =
        rm1.getClientRMService().getDelegationToken(request2);
    org.apache.hadoop.yarn.api.records.Token delegationToken2 =
        response2.getRMDelegationToken();
    Token<RMDelegationTokenIdentifier> token2 =
        ConverterUtils.convertFromYarn(delegationToken2, rmAddr);
    RMDelegationTokenIdentifier dtId2 = token2.decodeIdentifier();

    // cancel token2
    try{
      rm1.getRMDTSecretManager().cancelToken(token2,
        UserGroupInformation.getCurrentUser().getUserName());
    } catch(Exception e) {
      Assert.fail();
    }

    // Assert the token which has the latest delegationTokenSequenceNumber is removed
    Assert.assertEquals(
      rm1.getRMDTSecretManager().getLatestDTSequenceNumber(),
      dtId2.getSequenceNumber());
    Assert.assertFalse(rmDTState.containsKey(dtId2));

    // start new RM
    MockRM rm2 = new TestSecurityMockRM(conf, memStore);
    rm2.start();

    // assert master keys and tokens are populated back to DTSecretManager
    Map<RMDelegationTokenIdentifier, Long> allTokensRM2 =
        rm2.getRMDTSecretManager().getAllTokens();
    Assert.assertEquals(allTokensRM2.keySet(), allTokensRM1.keySet());
    // rm2 has its own master keys when it starts, we use containsAll here
    Assert.assertTrue(rm2.getRMDTSecretManager().getAllMasterKeys()
      .containsAll(allKeysRM1));

    // assert sequenceNumber is properly recovered,
    // even though the token which has max sequenceNumber is not stored
    Assert.assertEquals(rm1.getRMDTSecretManager().getLatestDTSequenceNumber(),
      rm2.getRMDTSecretManager().getLatestDTSequenceNumber());

    // renewDate before renewing
    Long renewDateBeforeRenew = allTokensRM2.get(dtId1);
    try{
      // renew recovered token
      rm2.getRMDTSecretManager().renewToken(token1, "renewer1");
    } catch(Exception e) {
      Assert.fail();
    }

    allTokensRM2 = rm2.getRMDTSecretManager().getAllTokens();
    Long renewDateAfterRenew = allTokensRM2.get(dtId1);
    // assert token is renewed
    Assert.assertTrue(renewDateAfterRenew > renewDateBeforeRenew);

    // assert new token is added into state store
    Assert.assertTrue(rmDTState.containsValue(renewDateAfterRenew));
    // assert old token is removed from state store
    Assert.assertFalse(rmDTState.containsValue(renewDateBeforeRenew));

    try{
      rm2.getRMDTSecretManager().cancelToken(token1,
        UserGroupInformation.getCurrentUser().getUserName());
    } catch(Exception e) {
      Assert.fail();
    }

    // assert token is removed from state after its cancelled
    allTokensRM2 = rm2.getRMDTSecretManager().getAllTokens();
    Assert.assertFalse(allTokensRM2.containsKey(dtId1));
    Assert.assertFalse(rmDTState.containsKey(dtId1));

    // stop the RM
    rm1.stop();
    rm2.stop();
  }

  @Test
  public void testRMStateStoreDispatcherDrainedOnRMStop() throws Exception {
    MemoryRMStateStore memStore = new MemoryRMStateStore() {
      volatile boolean wait = true;
      @Override
      public void serviceStop() throws Exception {
        // Unblock app saving request.
        wait = false;
        super.serviceStop();
      }

      @Override
      protected void handleStoreEvent(RMStateStoreEvent event) {
        // Block app saving request.
        while (wait);
        super.handleStoreEvent(event);
      }
    };
    memStore.init(conf);

    // start RM
    final MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();

    // create apps.
    final ArrayList<RMApp> appList = new ArrayList<RMApp>();
    final int NUM_APPS = 5;

    for (int i = 0; i < NUM_APPS; i++) {
      RMApp app = rm1.submitApp(200, "name", "user",
            new HashMap<ApplicationAccessType, String>(), false,
            "default", -1, null, "MAPREDUCE", false);
      appList.add(app);
      rm1.waitForState(app.getApplicationId(), RMAppState.NEW_SAVING);
    }
    // all apps's saving request are now enqueued to RMStateStore's dispatcher
    // queue, and will be processed once rm.stop() is called.

    // Nothing exist in state store before stop is called.
    Map<ApplicationId, ApplicationState> rmAppState =
        memStore.getState().getApplicationState();
    Assert.assertTrue(rmAppState.size() == 0);

    // stop rm
    rm1.stop();

    // Assert app info is still saved even if stop is called with pending saving
    // request on dispatcher.
    for (RMApp app : appList) {
      ApplicationState appState = rmAppState.get(app.getApplicationId());
      Assert.assertNotNull(appState);
      Assert.assertEquals(0, appState.getAttemptCount());
      Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), app.getApplicationSubmissionContext()
        .getApplicationId());
    }
    Assert.assertTrue(rmAppState.size() == NUM_APPS);
  }

  public static class TestSecurityMockRM extends MockRM {

    public TestSecurityMockRM(Configuration conf, RMStateStore store) {
      super(conf, store);
    }

    @Override
    protected ClientRMService createClientRMService() {
      return new ClientRMService(getRMContext(), getResourceScheduler(),
          rmAppManager, applicationACLsManager, null, rmDTSecretManager){
        @Override
        protected void serviceStart() throws Exception {
          // do nothing
        }

        @Override
        protected void serviceStop() throws Exception {
          //do nothing
        }
      };
    }

    @Override
    protected void doSecureLogin() throws IOException {
      // Do nothing.
    }
  }
}
