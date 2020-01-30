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

import org.junit.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.junit.Test;


public class TestSubmitApplicationWithRMHA extends RMHATestBase{

  public static final Logger LOG = LoggerFactory
      .getLogger(TestSubmitApplicationWithRMHA.class);

  @Test
  public void
      testHandleRMHABeforeSubmitApplicationCallWithSavedApplicationState()
          throws Exception {
    // start two RMs, and transit rm1 to active, rm2 to standby
    startRMs();

    // get a new applicationId from rm1
    ApplicationId appId = rm1.getNewAppId().getApplicationId();

    // Do the failover
    explicitFailover();

    // submit the application with previous assigned applicationId
    // to current active rm: rm2
    RMApp app1 =
        MockRMAppSubmitter.submit(rm2,
            MockRMAppSubmissionData.Builder.createWithMemory(200, rm2)
                .withAppName("")
                .withUser(UserGroupInformation
                    .getCurrentUser().getShortUserName())
                .withAcls(null)
                .withUnmanagedAM(false)
                .withQueue(null)
                .withWaitForAppAcceptedState(false)
                .withApplicationId(appId)
                .build());

    // verify application submission
    verifySubmitApp(rm2, app1, appId);
  }

  private void verifySubmitApp(MockRM rm, RMApp app,
      ApplicationId expectedAppId) throws Exception {
    int maxWaittingTimes = 20;
    int count = 0;
    while (true) {
      YarnApplicationState state =
          rm.getApplicationReport(app.getApplicationId())
              .getYarnApplicationState();
      if (!state.equals(YarnApplicationState.NEW) &&
          !state.equals(YarnApplicationState.NEW_SAVING)) {
        break;
      }
      if (count > maxWaittingTimes) {
        break;
      }
      Thread.sleep(200);
      count++;
    }
    // Verify submittion is successful
    YarnApplicationState state =
        rm.getApplicationReport(app.getApplicationId())
            .getYarnApplicationState();
    Assert.assertTrue(state == YarnApplicationState.ACCEPTED
        || state == YarnApplicationState.SUBMITTED);
    Assert.assertEquals(expectedAppId, app.getApplicationId());
  }

  // There are two scenarios when RM failover happens
  // after SubmitApplication Call:
  // 1) RMStateStore already saved the ApplicationState when failover happens
  // 2) RMStateStore did not save the ApplicationState when failover happens

  @Test
  public void
      testHandleRMHAafterSubmitApplicationCallWithSavedApplicationState()
          throws Exception {
    // Test scenario 1 when RM failover happens
    // after SubmitApplication Call:
    // RMStateStore already saved the ApplicationState when failover happens
    startRMs();

    // Submit Application
    // After submission, the applicationState will be saved in RMStateStore.
    RMApp app0 = MockRMAppSubmitter.submitWithMemory(200, rm1);

    // Do the failover
    explicitFailover();

    // Since the applicationState has already been saved in RMStateStore
    // before failover happens, the current active rm can load the previous
    // applicationState.
    ApplicationReport appReport =
        rm2.getApplicationReport(app0.getApplicationId());

    // verify previous submission is successful.
    Assert.assertTrue(appReport.getYarnApplicationState()
        == YarnApplicationState.ACCEPTED ||
        appReport.getYarnApplicationState()
        == YarnApplicationState.SUBMITTED);
  }

  @Test
  public void
      testHandleRMHAafterSubmitApplicationCallWithoutSavedApplicationState()
          throws Exception {
    // Test scenario 2 when RM failover happens
    // after SubmitApplication Call:
    // RMStateStore did not save the ApplicationState when failover happens.
    // Using customized RMAppManager.
    startRMsWithCustomizedRMAppManager();

    // Submit Application
    // After submission, the applicationState will
    // not be saved in RMStateStore
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
        .withAppName("")
        .withUser(UserGroupInformation
            .getCurrentUser().getShortUserName())
        .withAcls(null)
        .withUnmanagedAM(false)
        .withQueue(null)
        .withMaxAppAttempts(configuration.getInt(
            YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS))
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(false)
        .withKeepContainers(false)
        .build();
    RMApp app0 =
        MockRMAppSubmitter.submit(rm1, data);

    // Do the failover
    explicitFailover();

    // Since the applicationState is not saved in RMStateStore
    // when failover happens. The current active RM can not load
    // previous applicationState.
    // Expect ApplicationNotFoundException by calling getApplicationReport().
    try {
      rm2.getApplicationReport(app0.getApplicationId());
      Assert.fail("Should get ApplicationNotFoundException here");
    } catch (ApplicationNotFoundException ex) {
      // expected ApplicationNotFoundException
    }

    // Submit the application with previous ApplicationId to current active RM
    // This will mimic the similar behavior of YarnClient which will re-submit
    // Application with previous applicationId
    // when catches the ApplicationNotFoundException
    RMApp app1 =
        MockRMAppSubmitter.submit(rm2,
            MockRMAppSubmissionData.Builder.createWithMemory(200, rm2)
                .withAppName("")
                .withUser(UserGroupInformation
                    .getCurrentUser().getShortUserName())
                .withAcls(null)
                .withUnmanagedAM(false)
                .withQueue(null)
                .withMaxAppAttempts(configuration.getInt(
                    YarnConfiguration.RM_AM_MAX_ATTEMPTS,
                    YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS))
                .withCredentials(null)
                .withAppType(null)
                .withWaitForAppAcceptedState(false)
                .withKeepContainers(false)
                .withApplicationId(app0.getApplicationId())
                .build());

    verifySubmitApp(rm2, app1, app0.getApplicationId());
  }

  /**
   * Test multiple calls of getApplicationReport, to make sure
   * it is idempotent
   */
  @Test
  public void testGetApplicationReportIdempotent() throws Exception{
    // start two RMs, and transit rm1 to active, rm2 to standby
    startRMs();

    // Submit Application
    // After submission, the applicationState will be saved in RMStateStore.
    RMApp app = MockRMAppSubmitter.submitWithMemory(200, rm1);

    ApplicationReport appReport1 =
        rm1.getApplicationReport(app.getApplicationId());
    Assert.assertTrue(appReport1.getYarnApplicationState() ==
        YarnApplicationState.ACCEPTED ||
        appReport1.getYarnApplicationState() ==
        YarnApplicationState.SUBMITTED);

    // call getApplicationReport again
    ApplicationReport appReport2 =
        rm1.getApplicationReport(app.getApplicationId());
    Assert.assertEquals(appReport1.getApplicationId(),
        appReport2.getApplicationId());
    Assert.assertEquals(appReport1.getYarnApplicationState(),
        appReport2.getYarnApplicationState());

    // Do the failover
    explicitFailover();

    // call getApplicationReport
    ApplicationReport appReport3 =
        rm2.getApplicationReport(app.getApplicationId());
    Assert.assertEquals(appReport1.getApplicationId(),
        appReport3.getApplicationId());
    Assert.assertEquals(appReport1.getYarnApplicationState(),
        appReport3.getYarnApplicationState());

    // call getApplicationReport again
    ApplicationReport appReport4 =
        rm2.getApplicationReport(app.getApplicationId());
    Assert.assertEquals(appReport3.getApplicationId(),
        appReport4.getApplicationId());
    Assert.assertEquals(appReport3.getYarnApplicationState(),
        appReport4.getYarnApplicationState());
  }

  // There are two scenarios when RM failover happens
  // during SubmitApplication Call:
  // 1) RMStateStore already saved the ApplicationState when failover happens
  // 2) RMStateStore did not save the ApplicationState when failover happens
  @Test (timeout = 50000)
  public void
      testHandleRMHADuringSubmitApplicationCallWithSavedApplicationState()
          throws Exception {
    // Test scenario 1 when RM failover happens
    // druing SubmitApplication Call:
    // RMStateStore already saved the ApplicationState when failover happens
    startRMs();

    // Submit Application
    // After submission, the applicationState will be saved in RMStateStore.
    RMApp app0 = MockRMAppSubmitter.submitWithMemory(200, rm1);

    // Do the failover
    explicitFailover();

    // Since the applicationState has already been saved in RMStateStore
    // before failover happens, the current active rm can load the previous
    // applicationState.
    // This RMApp should exist in the RMContext of current active RM
    Assert.assertTrue(rm2.getRMContext().getRMApps()
        .containsKey(app0.getApplicationId()));

    // When we re-submit the application with same applicationId, it will
    // check whether this application has been exist. If yes, just simply
    // return submitApplicationResponse.
    RMApp app1 =
        MockRMAppSubmitter.submit(rm2,
            MockRMAppSubmissionData.Builder.createWithMemory(200, rm2)
                .withAppName("")
                .withUser(UserGroupInformation
                    .getCurrentUser().getShortUserName())
                .withAcls(null)
                .withUnmanagedAM(false)
                .withQueue(null)
                .withMaxAppAttempts(configuration.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
                    YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS))
                .withCredentials(null)
                .withAppType(null)
                .withWaitForAppAcceptedState(false)
                .withKeepContainers(false)
                .withApplicationId(app0.getApplicationId())
                .build());

    Assert.assertEquals(app1.getApplicationId(), app0.getApplicationId());
  }

  @Test (timeout = 50000)
  public void
      testHandleRMHADuringSubmitApplicationCallWithoutSavedApplicationState()
          throws Exception {
    // Test scenario 2 when RM failover happens
    // during SubmitApplication Call:
    // RMStateStore did not save the ApplicationState when failover happens.
    // Using customized RMAppManager.
    startRMsWithCustomizedRMAppManager();

    // Submit Application
    // After submission, the applicationState will
    // not be saved in RMStateStore
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
        .withAppName("")
        .withUser(UserGroupInformation
            .getCurrentUser().getShortUserName())
        .withAcls(null)
        .withUnmanagedAM(false)
        .withQueue(null)
        .withMaxAppAttempts(configuration.getInt(
            YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS))
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(false)
        .withKeepContainers(false)
        .build();
    RMApp app0 =
        MockRMAppSubmitter.submit(rm1, data);

    // Do the failover
    explicitFailover();

    // When failover happens, the RMStateStore has not saved applicationState.
    // The applicationState of this RMApp is lost.
    // We should not find the RMApp in the RMContext of current active rm.
    Assert.assertFalse(rm2.getRMContext().getRMApps()
        .containsKey(app0.getApplicationId()));

    // Submit the application with previous ApplicationId to current active RM
    // This will mimic the similar behavior of ApplicationClientProtocol#
    // submitApplication() when failover happens during the submission process
    // because the submitApplication api is marked as idempotent
    RMApp app1 =
        MockRMAppSubmitter.submit(rm2,
            MockRMAppSubmissionData.Builder.createWithMemory(200, rm2)
                .withAppName("")
                .withUser(UserGroupInformation
                    .getCurrentUser().getShortUserName())
                .withAcls(null)
                .withUnmanagedAM(false)
                .withQueue(null)
                .withMaxAppAttempts(configuration.getInt(
                    YarnConfiguration.RM_AM_MAX_ATTEMPTS,
                    YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS))
                .withCredentials(null)
                .withAppType(null)
                .withWaitForAppAcceptedState(false)
                .withKeepContainers(false)
                .withApplicationId(app0.getApplicationId())
                .build());

    verifySubmitApp(rm2, app1, app0.getApplicationId());
    Assert.assertTrue(rm2.getRMContext().getRMApps()
        .containsKey(app0.getApplicationId()));
  }
}
