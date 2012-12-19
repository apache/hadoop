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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class MemoryRMStateStore extends RMStateStore {
  
  RMState state = new RMState();
  
  @VisibleForTesting
  public RMState getState() {
    return state;
  }
  
  @Override
  public synchronized RMState loadState() throws Exception {
    // return a copy of the state to allow for modification of the real state
    RMState returnState = new RMState();
    returnState.appState.putAll(state.appState);
    return returnState;
  }
  
  @Override
  public synchronized void initInternal(Configuration conf) {
  }
  
  @Override
  protected synchronized void closeInternal() throws Exception {
  }

  @Override
  public void storeApplicationState(String appId, 
                                     ApplicationStateDataPBImpl appStateData)
      throws Exception {
    ApplicationState appState = new ApplicationState(
                         appStateData.getSubmitTime(), 
                         appStateData.getApplicationSubmissionContext());
    state.appState.put(appState.getAppId(), appState);
  }

  @Override
  public synchronized void storeApplicationAttemptState(String attemptIdStr, 
                            ApplicationAttemptStateDataPBImpl attemptStateData)
                            throws Exception {
    ApplicationAttemptId attemptId = ConverterUtils
                                        .toApplicationAttemptId(attemptIdStr);
    ApplicationAttemptState attemptState = new ApplicationAttemptState(
                            attemptId, attemptStateData.getMasterContainer());

    ApplicationState appState = state.getApplicationState().get(
        attemptState.getAttemptId().getApplicationId());
    assert appState != null;

    appState.attempts.put(attemptState.getAttemptId(), attemptState);
  }

  @Override
  public synchronized void removeApplicationState(ApplicationState appState) 
                                                            throws Exception {
    ApplicationId appId = appState.getAppId();
    ApplicationState removed = state.appState.remove(appId);
    assert removed != null;
  }
}
