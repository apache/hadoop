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
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMStateVersion;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;

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
  public RMState loadState() throws Exception {
    throw new UnsupportedOperationException("Cannot load state from null store");
  }

  @Override
  protected void storeApplicationStateInternal(ApplicationId appId,
      ApplicationStateDataPBImpl appStateData) throws Exception {
    // Do nothing
  }

  @Override
  protected void storeApplicationAttemptStateInternal(ApplicationAttemptId attemptId,
      ApplicationAttemptStateDataPBImpl attemptStateData) throws Exception {
    // Do nothing
  }

  @Override
  protected void removeApplicationStateInternal(ApplicationState appState)
      throws Exception {
    // Do nothing
  }

  @Override
  public void storeRMDelegationTokenAndSequenceNumberState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber) throws Exception {
    // Do nothing
  }

  @Override
  public void removeRMDelegationTokenState(RMDelegationTokenIdentifier rmDTIdentifier)
      throws Exception {
    // Do nothing
  }

  @Override
  protected void updateRMDelegationTokenAndSequenceNumberInternal(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber) throws Exception {
    // Do nothing
  }

  @Override
  public void storeRMDTMasterKeyState(DelegationKey delegationKey) throws Exception {
    // Do nothing
  }

  @Override
  public void removeRMDTMasterKeyState(DelegationKey delegationKey) throws Exception {
    // Do nothing
  }

  @Override
  protected void updateApplicationStateInternal(ApplicationId appId,
      ApplicationStateDataPBImpl appStateData) throws Exception {
    // Do nothing 
  }

  @Override
  protected void updateApplicationAttemptStateInternal(ApplicationAttemptId attemptId,
      ApplicationAttemptStateDataPBImpl attemptStateData) throws Exception {
  }

  @Override
  public void checkVersion() throws Exception {
    // Do nothing
  }

  @Override
  protected RMStateVersion loadVersion() throws Exception {
    // Do nothing
    return null;
  }

  @Override
  protected void storeVersion() throws Exception {
    // Do nothing
  }

  @Override
  protected RMStateVersion getCurrentVersion() {
    // Do nothing
    return null;
  }

}
