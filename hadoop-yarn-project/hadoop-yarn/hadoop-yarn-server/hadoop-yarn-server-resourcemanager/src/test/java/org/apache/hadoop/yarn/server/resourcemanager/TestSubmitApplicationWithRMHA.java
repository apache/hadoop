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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.junit.Test;


public class TestSubmitApplicationWithRMHA extends RMHATestBase{

  public static final Log LOG = LogFactory
      .getLog(TestSubmitApplicationWithRMHA.class);

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
        rm2.submitApp(200, "", UserGroupInformation
            .getCurrentUser().getShortUserName(), null, false, null,
            configuration.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
                YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null,
            false, false, true, appId);

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
    Assert.assertFalse(rm.getApplicationReport(app.getApplicationId())
        .getYarnApplicationState() == YarnApplicationState.NEW);
    Assert.assertFalse(rm.getApplicationReport(app.getApplicationId())
        .getYarnApplicationState() == YarnApplicationState.NEW_SAVING);
    Assert.assertEquals(expectedAppId, app.getApplicationId());
  }
}
