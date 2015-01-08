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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class MemoryRMStateStore extends RMStateStore {
  
  RMState state = new RMState();
  private long epoch = 0L;
  
  @VisibleForTesting
  public RMState getState() {
    return state;
  }

  @Override
  public void checkVersion() throws Exception {
  }

  @Override
  public synchronized long getAndIncrementEpoch() throws Exception {
    long currentEpoch = epoch;
    epoch = epoch + 1;
    return currentEpoch;
  }

  @Override
  public synchronized RMState loadState() throws Exception {
    // return a copy of the state to allow for modification of the real state
    RMState returnState = new RMState();
    returnState.appState.putAll(state.appState);
    returnState.rmSecretManagerState.getMasterKeyState()
      .addAll(state.rmSecretManagerState.getMasterKeyState());
    returnState.rmSecretManagerState.getTokenState().putAll(
      state.rmSecretManagerState.getTokenState());
    returnState.rmSecretManagerState.dtSequenceNumber =
        state.rmSecretManagerState.dtSequenceNumber;
    returnState.amrmTokenSecretManagerState =
        state.amrmTokenSecretManagerState == null ? null
            : AMRMTokenSecretManagerState
              .newInstance(state.amrmTokenSecretManagerState);
    return returnState;
  }
  
  @Override
  public synchronized void initInternal(Configuration conf) {
  }

  @Override
  protected synchronized void startInternal() throws Exception {
  }

  @Override
  protected synchronized void closeInternal() throws Exception {
  }

  @Override
  public synchronized void storeApplicationStateInternal(
      ApplicationId appId, ApplicationStateData appState)
      throws Exception {
    state.appState.put(appId, appState);
  }

  @Override
  public synchronized void updateApplicationStateInternal(
      ApplicationId appId, ApplicationStateData appState) 
      throws Exception {
    LOG.info("Updating final state " + appState.getState() + " for app: "
        + appId);
    if (state.appState.get(appId) != null) {
      // add the earlier attempts back
      appState.attempts.putAll(state.appState.get(appId).attempts);
    }
    state.appState.put(appId, appState);
  }

  @Override
  public synchronized void storeApplicationAttemptStateInternal(
      ApplicationAttemptId appAttemptId,
      ApplicationAttemptStateData attemptState)
      throws Exception {
    ApplicationStateData appState = state.getApplicationState().get(
        attemptState.getAttemptId().getApplicationId());
    if (appState == null) {
      throw new YarnRuntimeException("Application doesn't exist");
    }
    appState.attempts.put(attemptState.getAttemptId(), attemptState);
  }

  @Override
  public synchronized void updateApplicationAttemptStateInternal(
      ApplicationAttemptId appAttemptId,
      ApplicationAttemptStateData attemptState)
      throws Exception {
    ApplicationStateData appState =
        state.getApplicationState().get(appAttemptId.getApplicationId());
    if (appState == null) {
      throw new YarnRuntimeException("Application doesn't exist");
    }
    LOG.info("Updating final state " + attemptState.getState()
        + " for attempt: " + attemptState.getAttemptId());
    appState.attempts.put(attemptState.getAttemptId(), attemptState);
  }

  @Override
  public synchronized void removeApplicationStateInternal(
      ApplicationStateData appState) throws Exception {
    ApplicationId appId =
        appState.getApplicationSubmissionContext().getApplicationId();
    ApplicationStateData removed = state.appState.remove(appId);

    if (removed == null) {
      throw new YarnRuntimeException("Removing non-exsisting application state");
    }
  }

  private void storeOrUpdateRMDT(RMDelegationTokenIdentifier rmDTIdentifier,
      Long renewDate, boolean isUpdate) throws Exception {
    Map<RMDelegationTokenIdentifier, Long> rmDTState =
        state.rmSecretManagerState.getTokenState();
    if (rmDTState.containsKey(rmDTIdentifier)) {
      IOException e = new IOException("RMDelegationToken: " + rmDTIdentifier
          + "is already stored.");
      LOG.info("Error storing info for RMDelegationToken: " + rmDTIdentifier, e);
      throw e;
    }
    rmDTState.put(rmDTIdentifier, renewDate);
    if(!isUpdate) {
      state.rmSecretManagerState.dtSequenceNumber = 
          rmDTIdentifier.getSequenceNumber();
    }
    LOG.info("Store RMDT with sequence number "
             + rmDTIdentifier.getSequenceNumber());
  }

  @Override
  public synchronized void storeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    storeOrUpdateRMDT(rmDTIdentifier, renewDate, false);
  }

  @Override
  public synchronized void removeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier) throws Exception{
    Map<RMDelegationTokenIdentifier, Long> rmDTState =
        state.rmSecretManagerState.getTokenState();
    rmDTState.remove(rmDTIdentifier);
    LOG.info("Remove RMDT with sequence number "
        + rmDTIdentifier.getSequenceNumber());
  }

  @Override
  protected synchronized void updateRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    removeRMDelegationTokenState(rmDTIdentifier);
    storeOrUpdateRMDT(rmDTIdentifier, renewDate, true);
    LOG.info("Update RMDT with sequence number "
        + rmDTIdentifier.getSequenceNumber());
  }

  @Override
  public synchronized void storeRMDTMasterKeyState(DelegationKey delegationKey)
      throws Exception {
    Set<DelegationKey> rmDTMasterKeyState =
        state.rmSecretManagerState.getMasterKeyState();

    if (rmDTMasterKeyState.contains(delegationKey)) {
      IOException e = new IOException("RMDTMasterKey with keyID: "
              + delegationKey.getKeyId() + " is already stored");
      LOG.info("Error storing info for RMDTMasterKey with keyID: "
          + delegationKey.getKeyId(), e);
      throw e;
    }
    state.getRMDTSecretManagerState().getMasterKeyState().add(delegationKey);
    LOG.info("Store RMDT master key with key id: " + delegationKey.getKeyId()
        + ". Currently rmDTMasterKeyState size: " + rmDTMasterKeyState.size());
  }

  @Override
  public synchronized void removeRMDTMasterKeyState(DelegationKey delegationKey)
      throws Exception {
    LOG.info("Remove RMDT master key with key id: " + delegationKey.getKeyId());
    Set<DelegationKey> rmDTMasterKeyState =
        state.rmSecretManagerState.getMasterKeyState();
    rmDTMasterKeyState.remove(delegationKey);
  }

  @Override
  protected Version loadVersion() throws Exception {
    return null;
  }

  @Override
  protected void storeVersion() throws Exception {
  }

  @Override
  protected Version getCurrentVersion() {
    return null;
  }

  @Override
  public synchronized void storeOrUpdateAMRMTokenSecretManagerState(
      AMRMTokenSecretManagerState amrmTokenSecretManagerState,
      boolean isUpdate) {
    if (amrmTokenSecretManagerState != null) {
      state.amrmTokenSecretManagerState = AMRMTokenSecretManagerState
          .newInstance(amrmTokenSecretManagerState);
    }
  }

  @Override
  public void deleteStore() throws Exception {
  }

}
