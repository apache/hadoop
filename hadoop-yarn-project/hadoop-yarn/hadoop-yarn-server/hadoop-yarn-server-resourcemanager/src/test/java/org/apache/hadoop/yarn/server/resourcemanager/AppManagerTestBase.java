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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;

/**
 * Base class for AppManager related test.
 *
 */
public class AppManagerTestBase {

  // Extend and make the functions we want to test public
  protected class TestRMAppManager extends RMAppManager {
    private final RMStateStore stateStore;

    public TestRMAppManager(RMContext context, Configuration conf) {
      super(context, null, null, new ApplicationACLsManager(conf), conf);
      this.stateStore = context.getStateStore();
    }

    public TestRMAppManager(RMContext context,
        ClientToAMTokenSecretManagerInRM clientToAMSecretManager,
        YarnScheduler scheduler, ApplicationMasterService masterService,
        ApplicationACLsManager applicationACLsManager, Configuration conf) {
      super(context, scheduler, masterService, applicationACLsManager, conf);
      this.stateStore = context.getStateStore();
    }

    public void checkAppNumCompletedLimit() {
      super.checkAppNumCompletedLimit();
    }

    public void finishApplication(ApplicationId appId) {
      super.finishApplication(appId);
    }

    public int getCompletedAppsListSize() {
      return super.getCompletedAppsListSize();
    }

    public int getNumberOfCompletedAppsInStateStore() {
      return this.completedAppsInStateStore;
    }

    public void submitApplication(
        ApplicationSubmissionContext submissionContext, String user)
        throws YarnException {
      super.submitApplication(submissionContext, System.currentTimeMillis(),
          user);
    }

    public String getUserNameForPlacement(final String user,
        final ApplicationSubmissionContext context,
        final PlacementManager placementManager) throws YarnException {
      return super.getUserNameForPlacement(user, context, placementManager);
    }
  }
}
