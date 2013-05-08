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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TestRMRestart {
  
  @Test
  public void testRMRestart() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    ExitUtil.disableSystemExit();
    
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());
    Assert.assertTrue(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS > 1);
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
    
    MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
    MockNM nm2 = new MockNM("h2:5678", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm2.registerNode(); // nm2 will not heartbeat with RM1
    
    // create app that will not be saved because it will finish
    RMApp app0 = rm1.submitApp(200);
    RMAppAttempt attempt0 = app0.getCurrentAppAttempt();
    // spot check that app is saved
    Assert.assertEquals(1, rmAppState.size());
    nm1.nodeHeartbeat(true);
    MockAM am0 = rm1.sendAMLaunched(attempt0.getAppAttemptId());
    am0.registerAppAttempt();
    am0.unregisterAppAttempt();
    nm1.nodeHeartbeat(attempt0.getAppAttemptId(), 1, ContainerState.COMPLETE);
    am0.waitForState(RMAppAttemptState.FINISHED);
    rm1.waitForState(app0.getApplicationId(), RMAppState.FINISHED);

    // spot check that app is not saved anymore
    Assert.assertEquals(0, rmAppState.size());
        
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
    am1.allocate("h1" , 1000, 1, new ArrayList<ContainerId>());    
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
    // only 2 apps are loaded since unmanaged app is not loaded back since it
    // cannot be restarted by the RM this will change with work preserving RM
    // restart in which AMs/NMs are not rebooted
    Assert.assertEquals(2, rm2.getRMContext().getRMApps().size());
    
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
    Assert.assertTrue(allocResponse.getReboot());
    
    // NM should be rebooted on heartbeat, even first heartbeat for nm2
    NodeHeartbeatResponse hbResponse = nm1.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, hbResponse.getNodeAction());
    hbResponse = nm2.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, hbResponse.getNodeAction());
    
    // new NM to represent NM re-register
    nm1 = rm2.registerNode("h1:1234", 15120);
    nm2 = rm2.registerNode("h2:5678", 15120);

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
    if(attemptState.getMasterContainer().getNodeId().toString().contains("h2")){
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
    if(attemptState.getMasterContainer().getNodeId().toString().contains("h2")){
      am2Node = nm2;
    }
    
    // start the AM's
    am1 = rm2.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    
    MockAM am2 = rm2.sendAMLaunched(attempt2.getAppAttemptId());
    am2.registerAppAttempt();

    //request for containers
    am1.allocate("h1" , 1000, 3, new ArrayList<ContainerId>());
    am2.allocate("h2" , 1000, 1, new ArrayList<ContainerId>());
    
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

    // finish the AM's
    am1.unregisterAppAttempt();
    am1Node.nodeHeartbeat(attempt1.getAppAttemptId(), 1, ContainerState.COMPLETE);
    am1.waitForState(RMAppAttemptState.FINISHED);
    
    am2.unregisterAppAttempt();
    am2Node.nodeHeartbeat(attempt2.getAppAttemptId(), 1, ContainerState.COMPLETE);
    am2.waitForState(RMAppAttemptState.FINISHED);
    
    // stop RM's
    rm2.stop();
    rm1.stop();
    
    // completed apps should be removed
    Assert.assertEquals(0, rmAppState.size());
 }
  
  @Test
  public void testRMRestartOnMaxAppAttempts() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    ExitUtil.disableSystemExit();

    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    Assert.assertTrue(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS > 1);
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();

    Map<ApplicationId, ApplicationState> rmAppState =
        rmState.getApplicationState();  
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
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

    // verify that app2 exists  app1 is removed
    Assert.assertEquals(1, rm2.getRMContext().getRMApps().size());
    Assert.assertNotNull(rm2.getRMContext().getRMApps()
        .get(app2.getApplicationId()));
    Assert.assertNull(rm2.getRMContext().getRMApps()
        .get(app1.getApplicationId()));

    // verify that app2 is stored, app1 is removed
    Assert.assertNotNull(rmAppState.get(app2.getApplicationId()));
    Assert.assertNull(rmAppState.get(app1.getApplicationId()));

    // stop the RM  
    rm1.stop();
    rm2.stop();
  }

  @Test
  public void testDelegationTokenRestoredInDelegationTokenRenewer()
      throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    ExitUtil.disableSystemExit();

    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
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
    ts.addToken(userText1, token1);
    tokenSet.add(token1);

    Text userText2 = new Text("user2");
    RMDelegationTokenIdentifier dtId2 =
        new RMDelegationTokenIdentifier(userText2, new Text("renewer2"),
          userText2);
    Token<RMDelegationTokenIdentifier> token2 =
        new Token<RMDelegationTokenIdentifier>(dtId2,
          rm1.getRMDTSecretManager());
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
      .getContainerTokens());

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
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    ExitUtil.disableSystemExit();

    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
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

    // the appToken and clientToken that are generated when RMAppAttempt is created,
    HashSet<Token<?>> tokenSet = new HashSet<Token<?>>();
    tokenSet.add(attempt1.getApplicationToken());
    tokenSet.add(attempt1.getClientToken());

    // assert application Token is saved
    HashSet<Token<?>> savedTokens = new HashSet<Token<?>>();
    savedTokens.addAll(attemptState.getAppAttemptTokens().getAllTokens());
    Assert.assertEquals(tokenSet, savedTokens);

    // start new RM
    MockRM rm2 = new TestSecurityMockRM(conf, memStore);
    rm2.start();

    RMApp loadedApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    RMAppAttempt loadedAttempt1 = loadedApp1.getRMAppAttempt(attemptId1);

    // assert loaded attempt recovered attempt tokens
    Assert.assertNotNull(loadedAttempt1);
    savedTokens.clear();
    savedTokens.add(loadedAttempt1.getApplicationToken());
    savedTokens.add(loadedAttempt1.getClientToken());
    Assert.assertEquals(tokenSet, savedTokens);

    // assert clientToken is recovered back to api-versioned clientToken
    Assert.assertEquals(attempt1.getClientToken(),
      loadedAttempt1.getClientToken());

    // Not testing ApplicationTokenSecretManager has the password populated back,
    // that is needed in work-preserving restart

    rm1.stop();
    rm2.stop();
  }

  class TestSecurityMockRM extends MockRM {

    public TestSecurityMockRM(Configuration conf, RMStateStore store) {
      super(conf, store);
    }

    @Override
    protected void doSecureLogin() throws IOException {
      // Do nothing.
    }

    @Override
    protected DelegationTokenRenewer createDelegationTokenRenewer() {
      return new DelegationTokenRenewer() {
        @Override
        protected void renewToken(final DelegationTokenToRenew dttr)
            throws IOException {
          // Do nothing
        }

        @Override
        protected void setTimerForTokenRenewal(DelegationTokenToRenew token)
            throws IOException {
          // Do nothing
        }
      };
    }
  }
}
