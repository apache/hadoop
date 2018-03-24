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

package org.apache.hadoop.yarn.server.nodemanager.recovery;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LogDeleterProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

// The state store to use when state isn't being stored
public class NMNullStateStoreService extends NMStateStoreService {

  public NMNullStateStoreService() {
    super(NMNullStateStoreService.class.getName());
  }

  @Override
  public boolean canRecover() {
    return false;
  }

  @Override
  public RecoveredApplicationsState loadApplicationsState() throws IOException {
    throw new UnsupportedOperationException(
        "Recovery not supported by this state store");
  }

  @Override
  public void storeApplication(ApplicationId appId,
      ContainerManagerApplicationProto p) throws IOException {
  }

  @Override
  public void removeApplication(ApplicationId appId) throws IOException {
  }

  @Override
  public List<RecoveredContainerState> loadContainersState()
      throws IOException {
    throw new UnsupportedOperationException(
        "Recovery not supported by this state store");
  }

  @Override
  public void storeContainer(ContainerId containerId, int version,
      long startTime, StartContainerRequest startRequest) throws IOException {
  }

  @Override
  public void storeContainerQueued(ContainerId containerId) throws IOException {
  }

  @Override
  public void storeContainerPaused(ContainerId containerId) throws IOException {
  }

  @Override
  public void removeContainerPaused(ContainerId containerId)
      throws IOException {
  }

  @Override
  public void storeContainerDiagnostics(ContainerId containerId,
      StringBuilder diagnostics) throws IOException {
  }

  @Override
  public void storeContainerLaunched(ContainerId containerId)
      throws IOException {
  }

  @Override
  public void storeContainerUpdateToken(ContainerId containerId,
      ContainerTokenIdentifier containerTokenIdentifier) throws IOException {
  }

  @Override
  public void storeContainerKilled(ContainerId containerId)
      throws IOException {
  }

  @Override
  public void storeContainerCompleted(ContainerId containerId, int exitCode)
      throws IOException {
  }

  @Override
  public void storeContainerRemainingRetryAttempts(ContainerId containerId,
      int remainingRetryAttempts) throws IOException {
  }

  @Override
  public void storeContainerRestartTimes(ContainerId containerId,
      List<Long> restartTimes) throws IOException {
  }

  @Override
  public void storeContainerWorkDir(ContainerId containerId,
      String workDir) throws IOException {
  }

  @Override
  public void storeContainerLogDir(ContainerId containerId,
      String logDir) throws IOException {
  }

  @Override
  public void removeContainer(ContainerId containerId) throws IOException {
  }

  @Override
  public RecoveredLocalizationState loadLocalizationState()
      throws IOException {
    throw new UnsupportedOperationException(
        "Recovery not supported by this state store");
  }

  @Override
  public void startResourceLocalization(String user, ApplicationId appId,
      LocalResourceProto proto, Path localPath) throws IOException {
  }

  @Override
  public void finishResourceLocalization(String user, ApplicationId appId,
      LocalizedResourceProto proto) throws IOException {
  }

  @Override
  public void removeLocalizedResource(String user, ApplicationId appId,
      Path localPath) throws IOException {
  }

  @Override
  public RecoveredDeletionServiceState loadDeletionServiceState()
      throws IOException {
    throw new UnsupportedOperationException(
        "Recovery not supported by this state store");
  }

  @Override
  public void storeDeletionTask(int taskId,
      DeletionServiceDeleteTaskProto taskProto) throws IOException {
  }

  @Override
  public void removeDeletionTask(int taskId) throws IOException {
  }

  @Override
  public RecoveredNMTokensState loadNMTokensState() throws IOException {
    throw new UnsupportedOperationException(
        "Recovery not supported by this state store");
  }

  @Override
  public void storeNMTokenCurrentMasterKey(MasterKey key)
      throws IOException {
  }

  @Override
  public void storeNMTokenPreviousMasterKey(MasterKey key)
      throws IOException {
  }

  @Override
  public void storeNMTokenApplicationMasterKey(ApplicationAttemptId attempt,
      MasterKey key) throws IOException {
  }

  @Override
  public void removeNMTokenApplicationMasterKey(ApplicationAttemptId attempt)
      throws IOException {
  }

  @Override
  public RecoveredContainerTokensState loadContainerTokensState()
      throws IOException {
    throw new UnsupportedOperationException(
        "Recovery not supported by this state store");
  }

  @Override
  public void storeContainerTokenCurrentMasterKey(MasterKey key)
      throws IOException {
  }

  @Override
  public void storeContainerTokenPreviousMasterKey(MasterKey key)
      throws IOException {
  }

  @Override
  public void storeContainerToken(ContainerId containerId,
      Long expirationTime) throws IOException {
  }

  @Override
  public void removeContainerToken(ContainerId containerId)
      throws IOException {
  }

  @Override
  public RecoveredLogDeleterState loadLogDeleterState() throws IOException {
    throw new UnsupportedOperationException(
        "Recovery not supported by this state store");
  }

  @Override
  public void storeLogDeleter(ApplicationId appId, LogDeleterProto proto)
      throws IOException {
  }

  @Override
  public void removeLogDeleter(ApplicationId appId) throws IOException {
  }

  @Override
  public RecoveredAMRMProxyState loadAMRMProxyState() throws IOException {
    throw new UnsupportedOperationException(
        "Recovery not supported by this state store");
  }

  @Override
  public void storeAMRMProxyCurrentMasterKey(MasterKey key) throws IOException {
  }

  @Override
  public void storeAMRMProxyNextMasterKey(MasterKey key) throws IOException {
  }

  @Override
  public void storeAMRMProxyAppContextEntry(ApplicationAttemptId attempt,
      String key, byte[] data) throws IOException {
  }

  @Override
  public void removeAMRMProxyAppContextEntry(ApplicationAttemptId attempt,
      String key) throws IOException {
  }

  @Override
  public void removeAMRMProxyAppContext(ApplicationAttemptId attempt)
      throws IOException {
  }

  @Override
  public void storeAssignedResources(Container container,
      String resourceType, List<Serializable> assignedResources)
      throws IOException {
    updateContainerResourceMapping(container, resourceType, assignedResources);
  }

  @Override
  protected void initStorage(Configuration conf) throws IOException {
  }

  @Override
  protected void startStorage() throws IOException {
  }

  @Override
  protected void closeStorage() throws IOException {
  }
}
