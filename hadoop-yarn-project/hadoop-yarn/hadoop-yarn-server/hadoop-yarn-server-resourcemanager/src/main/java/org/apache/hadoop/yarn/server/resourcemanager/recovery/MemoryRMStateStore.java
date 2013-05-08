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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
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
    if (state.appState.containsKey(appState.getAppId())) {
      Exception e = new IOException("App: " + appId + " is already stored.");
      LOG.info("Error storing info for app: " + appId, e);
      throw e;
    }
    state.appState.put(appState.getAppId(), appState);
  }

  @Override
  public synchronized void storeApplicationAttemptState(String attemptIdStr, 
                            ApplicationAttemptStateDataPBImpl attemptStateData)
                            throws Exception {
    ApplicationAttemptId attemptId = ConverterUtils
                                        .toApplicationAttemptId(attemptIdStr);
    Credentials credentials = null;
    if(attemptStateData.getAppAttemptTokens() != null){
      DataInputByteBuffer dibb = new DataInputByteBuffer();
      credentials = new Credentials();
      dibb.reset(attemptStateData.getAppAttemptTokens());
      credentials.readTokenStorageStream(dibb);
    }
    ApplicationAttemptState attemptState =
        new ApplicationAttemptState(attemptId,
          attemptStateData.getMasterContainer(), credentials);

    ApplicationState appState = state.getApplicationState().get(
        attemptState.getAttemptId().getApplicationId());
    assert appState != null;

    if (appState.attempts.containsKey(attemptState.getAttemptId())) {
      Exception e = new IOException("Attempt: " +
          attemptState.getAttemptId() + " is already stored.");
      LOG.info("Error storing info for attempt: " +
          attemptState.getAttemptId(), e);
      throw e;
    }
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
