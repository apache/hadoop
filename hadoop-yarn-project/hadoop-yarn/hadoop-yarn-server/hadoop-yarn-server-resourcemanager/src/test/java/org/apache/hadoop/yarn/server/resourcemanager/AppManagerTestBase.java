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

import static java.util.stream.Collectors.toSet;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.mockito.ArgumentCaptor;

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

    public List<ApplicationId> getCompletedApps() {
      return completedApps;
    }

    public Set<ApplicationId> getFirstNCompletedApps(int n) {
      return getCompletedApps().stream().limit(n).collect(toSet());
    }

    public Set<ApplicationId> getCompletedAppsWithEvenIdsInRange(int n) {
      return getCompletedApps().stream().limit(n)
          .filter(app -> app.getId() % 2 == 0).collect(toSet());
    }

    public Set<ApplicationId> getRemovedAppsFromStateStore(int numRemoves) {
      ArgumentCaptor<RMApp> argumentCaptor =
          ArgumentCaptor.forClass(RMApp.class);
      verify(stateStore, times(numRemoves))
          .removeApplication(argumentCaptor.capture());
      return argumentCaptor.getAllValues().stream().map(RMApp::getApplicationId)
          .collect(toSet());
    }

    public void submitApplication(
        ApplicationSubmissionContext submissionContext, String user)
        throws YarnException {
      super.submitApplication(submissionContext, System.currentTimeMillis(),
          user);
    }
  }
}
