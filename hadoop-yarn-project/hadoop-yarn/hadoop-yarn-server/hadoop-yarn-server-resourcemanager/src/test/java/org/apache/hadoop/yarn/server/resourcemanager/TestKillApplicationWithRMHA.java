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

import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestKillApplicationWithRMHA extends ClientBaseWithFixes{

  public static final Log LOG = LogFactory
      .getLog(TestKillApplicationWithRMHA.class);
  private static final int ZK_TIMEOUT_MS = 5000;
  private static StateChangeRequestInfo requestInfo =
      new StateChangeRequestInfo(
          HAServiceProtocol.RequestSource.REQUEST_BY_USER);
  private Configuration configuration = new YarnConfiguration();
  static MockRM rm1 = null;
  static MockRM rm2 = null;
  Configuration confForRM1;
  Configuration confForRM2;

  @Before
  public void setup() throws Exception {
    configuration.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    configuration.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");
    configuration.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    configuration.set(YarnConfiguration.RM_STORE,
        ZKRMStateStore.class.getName());
    configuration.set(YarnConfiguration.RM_ZK_ADDRESS, hostPort);
    configuration.setInt(YarnConfiguration.RM_ZK_TIMEOUT_MS, ZK_TIMEOUT_MS);
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    configuration.set(YarnConfiguration.RM_CLUSTER_ID, "test-yarn-cluster");
    int base = 100;
    for (String confKey : YarnConfiguration
        .getServiceAddressConfKeys(configuration)) {
      configuration.set(HAUtil.addSuffix(confKey, "rm1"), "0.0.0.0:"
          + (base + 20));
      configuration.set(HAUtil.addSuffix(confKey, "rm2"), "0.0.0.0:"
          + (base + 40));
      base = base * 2;
    }
    confForRM1 = new Configuration(configuration);
    confForRM1.set(YarnConfiguration.RM_HA_ID, "rm1");
    confForRM2 = new Configuration(configuration);
    confForRM2.set(YarnConfiguration.RM_HA_ID, "rm2");
  }

  @After
  public void teardown() {
    if (rm1 != null) {
      rm1.stop();
    }
    if (rm2 != null) {
      rm2.stop();
    }
  }

  @Test (timeout = 20000)
  public void testKillAppWhenFailoverHappensAtNewState()
      throws Exception {
    // create a customized RMAppManager
    // During the process of Application submission,
    // the RMAppState will always be NEW.
    // The ApplicationState will not be saved in RMStateStore.
    startRMsWithCustomizedRMAppManager();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // Submit the application
    RMApp app0 =
        rm1.submitApp(200, "", UserGroupInformation
            .getCurrentUser().getShortUserName(), null, false, null,
            configuration.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
                YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null,
            false, false);

    // failover and kill application
    // When FailOver happens, the state of this application is NEW,
    // and ApplicationState is not saved in RMStateStore. The active RM
    // can not load the ApplicationState of this application.
    // Expected to get ApplicationNotFoundException
    // when receives the KillApplicationRequest
    try {
      failOverAndKillApp(app0.getApplicationId(), RMAppState.NEW);
      fail("Should get an exception here");
    } catch (ApplicationNotFoundException ex) {
      Assert.assertTrue(ex.getMessage().contains(
          "Trying to kill an absent application " + app0.getApplicationId()));
    }
  }

  @Test (timeout = 20000)
  public void testKillAppWhenFailoverHappensAtRunningState()
      throws Exception {
    startRMs();
    MockNM nm1 = new MockNM("127.0.0.1:1234", 15120,
        rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // failover and kill application
    // The application is at RUNNING State when failOver happens.
    // Since RMStateStore has already saved ApplicationState, the active RM
    // will load the ApplicationState. After that, the application will be at
    // ACCEPTED State. Because the application is not at Final State,
    // KillApplicationResponse.getIsKillCompleted is expected to return false.
    failOverAndKillApp(app0.getApplicationId(),
        am0.getApplicationAttemptId(), RMAppState.RUNNING,
        RMAppAttemptState.RUNNING, RMAppState.ACCEPTED);
  }

  @Test (timeout = 20000)
  public void testKillAppWhenFailoverHappensAtFinalState()
      throws Exception {
    startRMs();
    MockNM nm1 = new MockNM("127.0.0.1:1234", 15120,
        rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // kill the app.
    rm1.killApp(app0.getApplicationId());
    rm1.waitForState(app0.getApplicationId(), RMAppState.KILLED);
    rm1.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.KILLED);

    // failover and kill application
    // The application is at Killed State and RMStateStore has already
    // saved this applicationState. After failover happens, the current
    // active RM will load the ApplicationState whose RMAppState is killed.
    // Because this application is at Final State,
    // KillApplicationResponse.getIsKillCompleted is expected to return true.
    failOverAndKillApp(app0.getApplicationId(),
        am0.getApplicationAttemptId(), RMAppState.KILLED,
        RMAppAttemptState.KILLED, RMAppState.KILLED);
  }

  @Test (timeout = 20000)
  public void testKillAppWhenFailOverHappensDuringApplicationKill()
      throws Exception {
    // create a customized ClientRMService
    // When receives the killApplicationRequest, simply return the response
    // and make sure the application will not be KILLED State
    startRMsWithCustomizedClientRMService();
    MockNM nm1 = new MockNM("127.0.0.1:1234", 15120,
        rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // ensure that the app is in running state
    Assert.assertEquals(app0.getState(), RMAppState.RUNNING);

    // kill the app.
    rm1.killApp(app0.getApplicationId());

    // failover happens before this application goes to final state.
    // The RMAppState that will be loaded by the active rm
    // should be ACCEPTED.
    failOverAndKillApp(app0.getApplicationId(),
        am0.getApplicationAttemptId(), RMAppState.RUNNING,
        RMAppAttemptState.RUNNING, RMAppState.ACCEPTED);

  }

  private MockAM launchAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    nm.nodeHeartbeat(true);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    rm.waitForState(app.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.RUNNING);
    return am;
  }

  private void failOverAndKillApp(ApplicationId appId,
      ApplicationAttemptId appAttemptId, RMAppState initialRMAppState,
      RMAppAttemptState initialRMAppAttemptState,
      RMAppState expectedAppStateBeforeKillApp) throws Exception {
    Assert.assertEquals(initialRMAppState,
        rm1.getRMContext().getRMApps().get(appId).getState());
    Assert.assertEquals(initialRMAppAttemptState, rm1.getRMContext()
        .getRMApps().get(appId).getAppAttempts().get(appAttemptId).getState());
    explicitFailover();
    Assert.assertEquals(expectedAppStateBeforeKillApp,
        rm2.getRMContext().getRMApps().get(appId).getState());
    killApplication(rm2, appId, appAttemptId, initialRMAppState);
  }

  private void failOverAndKillApp(ApplicationId appId,
      RMAppState initialRMAppState) throws Exception {
    Assert.assertEquals(initialRMAppState,
        rm1.getRMContext().getRMApps().get(appId).getState());
    explicitFailover();
    Assert.assertTrue(rm2.getRMContext().getRMApps().get(appId) == null);
    killApplication(rm2, appId, null, initialRMAppState);
  }

  private void startRMs() throws IOException {
    rm1 = new MockRM(confForRM1);
    rm2 = new MockRM(confForRM2);
    startRMs(rm1, confForRM1, rm2, confForRM2);

  }

  private void startRMsWithCustomizedRMAppManager() throws IOException {
    final Configuration conf1 = new Configuration(confForRM1);

    rm1 = new MockRM(conf1) {
      @Override
      protected RMAppManager createRMAppManager() {
        return new MyRMAppManager(this.rmContext, this.scheduler,
            this.masterService, this.applicationACLsManager, conf1);
      }
    };

    rm2 = new MockRM(confForRM2);

    startRMs(rm1, conf1, rm2, confForRM2);
  }

  private void startRMsWithCustomizedClientRMService() throws IOException {
    final Configuration conf1 = new Configuration(confForRM1);

    rm1 = new MockRM(conf1) {
      @Override
      protected ClientRMService createClientRMService() {
        return new MyClientRMService(this.rmContext, this.scheduler,
            this.rmAppManager, this.applicationACLsManager,
            this.queueACLsManager, getRMDTSecretManager());
      }
    };

    rm2 = new MockRM(confForRM2);

    startRMs(rm1, conf1, rm2, confForRM2);
  }

  private static class MyRMAppManager extends RMAppManager {

    private Configuration conf;
    private RMContext rmContext;

    public MyRMAppManager(RMContext context, YarnScheduler scheduler,
        ApplicationMasterService masterService,
        ApplicationACLsManager applicationACLsManager, Configuration conf) {
      super(context, scheduler, masterService, applicationACLsManager, conf);
      this.conf = conf;
      this.rmContext = context;
    }

    @Override
    protected void submitApplication(
        ApplicationSubmissionContext submissionContext, long submitTime,
        String user, boolean isRecovered, RMState state) throws YarnException {
      //Do nothing, just add the application to RMContext
      RMAppImpl application =
          new RMAppImpl(submissionContext.getApplicationId(), this.rmContext,
              this.conf, submissionContext.getApplicationName(), user,
              submissionContext.getQueue(), submissionContext,
              this.rmContext.getScheduler(),
              this.rmContext.getApplicationMasterService(),
              submitTime, submissionContext.getApplicationType(),
              submissionContext.getApplicationTags());
      this.rmContext.getRMApps().put(submissionContext.getApplicationId(),
          application);
      //Do not send RMAppEventType.START event
      //so the state of Application will not reach to NEW_SAVING state.
    }
  }

  private static class MyClientRMService extends ClientRMService {

    private RMContext rmContext;

    public MyClientRMService(RMContext rmContext, YarnScheduler scheduler,
        RMAppManager rmAppManager,
        ApplicationACLsManager applicationACLsManager,
        QueueACLsManager queueACLsManager,
        RMDelegationTokenSecretManager rmDTSecretManager) {
      super(rmContext, scheduler, rmAppManager, applicationACLsManager,
          queueACLsManager, rmDTSecretManager);
      this.rmContext = rmContext;
    }

    @Override
    protected void serviceStart() {
      // override to not start rpc handler
    }

    @Override
    protected void serviceStop() {
      // don't do anything
    }

    @Override
    public KillApplicationResponse forceKillApplication(
        KillApplicationRequest request) throws YarnException {
      ApplicationId applicationId = request.getApplicationId();
      RMApp application = this.rmContext.getRMApps().get(applicationId);
      if (application.isAppSafeToTerminate()) {
        return KillApplicationResponse.newInstance(true);
      } else {
        return KillApplicationResponse.newInstance(false);
      }
    }
  }

  private boolean isFinalState(RMAppState state) {
    return state.equals(RMAppState.FINISHING)
    || state.equals(RMAppState.FINISHED) || state.equals(RMAppState.FAILED)
    || state.equals(RMAppState.KILLED);
  }

  private void explicitFailover() throws IOException {
    rm1.adminService.transitionToStandby(requestInfo);
    rm2.adminService.transitionToActive(requestInfo);
    Assert.assertTrue(rm1.getRMContext().getHAServiceState()
        == HAServiceState.STANDBY);
    Assert.assertTrue(rm2.getRMContext().getHAServiceState()
        == HAServiceState.ACTIVE);
  }

  private void killApplication(MockRM rm, ApplicationId appId,
      ApplicationAttemptId appAttemptId, RMAppState rmAppState)
      throws Exception {
    KillApplicationResponse response = rm.killApp(appId);
    Assert
        .assertTrue(response.getIsKillCompleted() == isFinalState(rmAppState));
    RMApp loadedApp0 =
        rm.getRMContext().getRMApps().get(appId);
    rm.waitForState(appId, RMAppState.KILLED);
    if (appAttemptId != null) {
      rm.waitForState(appAttemptId, RMAppAttemptState.KILLED);
    }
    // no new attempt is created.
    Assert.assertEquals(1, loadedApp0.getAppAttempts().size());
  }

  private void startRMs(MockRM rm1, Configuration confForRM1, MockRM rm2,
      Configuration confForRM2) throws IOException {
    rm1.init(confForRM1);
    rm1.start();
    Assert.assertTrue(rm1.getRMContext().getHAServiceState()
        == HAServiceState.STANDBY);

    rm2.init(confForRM2);
    rm2.start();
    Assert.assertTrue(rm2.getRMContext().getHAServiceState()
        == HAServiceState.STANDBY);

    rm1.adminService.transitionToActive(requestInfo);
    Assert.assertTrue(rm1.getRMContext().getHAServiceState()
        == HAServiceState.ACTIVE);
  }
}
