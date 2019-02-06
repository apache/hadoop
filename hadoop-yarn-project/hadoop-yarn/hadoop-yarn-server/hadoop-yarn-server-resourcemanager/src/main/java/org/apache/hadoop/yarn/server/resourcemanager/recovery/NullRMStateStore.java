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


import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

@Unstable
public class NullRMStateStore extends RMStateStore {

  @Override
  protected void initInternal(Configuration conf) throws Exception {
    // Do nothing
  }

  @Override
  protected void startInternal() throws Exception {
    // Do nothing
  }

  @Override
  protected void closeInternal() throws Exception {
    // Do nothing
  }

  @Override
  public synchronized long getAndIncrementEpoch() throws Exception {
    return 0L;
  }

  @Override
  public RMState loadState() throws Exception {
    throw new UnsupportedOperationException("Cannot load state from null store");
  }

  @Override
  protected void storeApplicationStateInternal(ApplicationId appId,
      ApplicationStateData appStateData) throws Exception {
    // Do nothing
  }

  @Override
  protected void storeApplicationAttemptStateInternal(ApplicationAttemptId attemptId,
      ApplicationAttemptStateData attemptStateData) throws Exception {
    // Do nothing
  }

  @Override
  protected void removeApplicationStateInternal(ApplicationStateData appState)
      throws Exception {
    // Do nothing
  }

  @Override
  public void storeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    // Do nothing
  }

  @Override
  public void removeRMDelegationTokenState(RMDelegationTokenIdentifier rmDTIdentifier)
      throws Exception {
    // Do nothing
  }

  @Override
  protected void updateRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    // Do nothing
  }

  @Override
  public void storeRMDTMasterKeyState(DelegationKey delegationKey) throws Exception {
    // Do nothing
  }

  @Override
  protected void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
    // Do nothing
  }

  @Override
  protected void removeReservationState(String planName,
      String reservationIdName) throws Exception {
      // Do nothing
  }

  @Override
  public void removeRMDTMasterKeyState(DelegationKey delegationKey) throws Exception {
    // Do nothing
  }

  @Override
  protected void updateApplicationStateInternal(ApplicationId appId,
      ApplicationStateData appStateData) throws Exception {
    // Do nothing 
  }

  @Override
  protected void updateApplicationAttemptStateInternal(ApplicationAttemptId attemptId,
      ApplicationAttemptStateData attemptStateData) throws Exception {
  }

  @Override
  public synchronized void removeApplicationAttemptInternal(
      ApplicationAttemptId attemptId) throws Exception {
    // Do nothing
  }

  @Override
  public void checkVersion() throws Exception {
    // Do nothing
  }

  @Override
  protected Version loadVersion() throws Exception {
    // Do nothing
    return null;
  }

  @Override
  protected void storeVersion() throws Exception {
    // Do nothing
  }

  @Override
  protected Version getCurrentVersion() {
    // Do nothing
    return null;
  }

  @Override
  public void storeOrUpdateAMRMTokenSecretManagerState(
      AMRMTokenSecretManagerState state, boolean isUpdate) {
    //DO Nothing
  }

  @Override
  public void deleteStore() throws Exception {
    // Do nothing
  }

  @Override
  public void removeApplication(ApplicationId removeAppId) throws Exception {
    // Do nothing
  }

  @Override
  protected void storeProxyCACertState(
      X509Certificate caCert, PrivateKey caPrivateKey) throws Exception {
    // Do nothing
  }
}
