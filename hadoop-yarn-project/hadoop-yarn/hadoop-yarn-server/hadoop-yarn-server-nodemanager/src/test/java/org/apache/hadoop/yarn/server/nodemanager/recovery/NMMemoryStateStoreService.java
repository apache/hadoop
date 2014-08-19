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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;

public class NMMemoryStateStoreService extends NMStateStoreService {
  private Map<ApplicationId, ContainerManagerApplicationProto> apps;
  private Set<ApplicationId> finishedApps;
  private Map<ContainerId, RecoveredContainerState> containerStates;
  private Map<TrackerKey, TrackerState> trackerStates;
  private Map<Integer, DeletionServiceDeleteTaskProto> deleteTasks;
  private RecoveredNMTokensState nmTokenState;
  private RecoveredContainerTokensState containerTokenState;

  public NMMemoryStateStoreService() {
    super(NMMemoryStateStoreService.class.getName());
  }

  @Override
  protected void initStorage(Configuration conf) {
    apps = new HashMap<ApplicationId, ContainerManagerApplicationProto>();
    finishedApps = new HashSet<ApplicationId>();
    containerStates = new HashMap<ContainerId, RecoveredContainerState>();
    nmTokenState = new RecoveredNMTokensState();
    nmTokenState.applicationMasterKeys =
        new HashMap<ApplicationAttemptId, MasterKey>();
    containerTokenState = new RecoveredContainerTokensState();
    containerTokenState.activeTokens = new HashMap<ContainerId, Long>();
    trackerStates = new HashMap<TrackerKey, TrackerState>();
    deleteTasks = new HashMap<Integer, DeletionServiceDeleteTaskProto>();
  }

  @Override
  protected void startStorage() {
  }

  @Override
  protected void closeStorage() {
  }


  @Override
  public RecoveredApplicationsState loadApplicationsState()
      throws IOException {
    RecoveredApplicationsState state = new RecoveredApplicationsState();
    state.applications = new ArrayList<ContainerManagerApplicationProto>(
        apps.values());
    state.finishedApplications = new ArrayList<ApplicationId>(finishedApps);
    return state;
  }

  @Override
  public void storeApplication(ApplicationId appId,
      ContainerManagerApplicationProto proto) throws IOException {
    ContainerManagerApplicationProto protoCopy =
        ContainerManagerApplicationProto.parseFrom(proto.toByteString());
    apps.put(appId, protoCopy);
  }

  @Override
  public void storeFinishedApplication(ApplicationId appId) {
    finishedApps.add(appId);
  }

  @Override
  public void removeApplication(ApplicationId appId) throws IOException {
    apps.remove(appId);
    finishedApps.remove(appId);
  }

  @Override
  public List<RecoveredContainerState> loadContainersState()
      throws IOException {
    // return a copy so caller can't modify our state
    List<RecoveredContainerState> result =
        new ArrayList<RecoveredContainerState>(containerStates.size());
    for (RecoveredContainerState rcs : containerStates.values()) {
      RecoveredContainerState rcsCopy = new RecoveredContainerState();
      rcsCopy.status = rcs.status;
      rcsCopy.exitCode = rcs.exitCode;
      rcsCopy.killed = rcs.killed;
      rcsCopy.diagnostics = rcs.diagnostics;
      rcsCopy.startRequest = rcs.startRequest;
      result.add(rcsCopy);
    }
    return new ArrayList<RecoveredContainerState>();
  }

  @Override
  public void storeContainer(ContainerId containerId,
      StartContainerRequest startRequest) throws IOException {
    RecoveredContainerState rcs = new RecoveredContainerState();
    rcs.startRequest = startRequest;
    containerStates.put(containerId, rcs);
  }

  @Override
  public void storeContainerDiagnostics(ContainerId containerId,
      StringBuilder diagnostics) throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.diagnostics = diagnostics.toString();
  }

  @Override
  public void storeContainerLaunched(ContainerId containerId)
      throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    if (rcs.exitCode != ContainerExitStatus.INVALID) {
      throw new IOException("Container already completed");
    }
    rcs.status = RecoveredContainerStatus.LAUNCHED;
  }

  @Override
  public void storeContainerKilled(ContainerId containerId)
      throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.killed = true;
  }

  @Override
  public void storeContainerCompleted(ContainerId containerId, int exitCode)
      throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.status = RecoveredContainerStatus.COMPLETED;
    rcs.exitCode = exitCode;
  }

  @Override
  public void removeContainer(ContainerId containerId) throws IOException {
    containerStates.remove(containerId);
  }

  private RecoveredContainerState getRecoveredContainerState(
      ContainerId containerId) throws IOException {
    RecoveredContainerState rcs = containerStates.get(containerId);
    if (rcs == null) {
      throw new IOException("No start request for " + containerId);
    }
    return rcs;
  }

  private LocalResourceTrackerState loadTrackerState(TrackerState ts) {
    LocalResourceTrackerState result = new LocalResourceTrackerState();
    result.localizedResources.addAll(ts.localizedResources.values());
    for (Map.Entry<Path, LocalResourceProto> entry :
         ts.inProgressMap.entrySet()) {
      result.inProgressResources.put(entry.getValue(), entry.getKey());
    }
    return result;
  }

  private TrackerState getTrackerState(TrackerKey key) {
    TrackerState ts = trackerStates.get(key);
    if (ts == null) {
      ts = new TrackerState();
      trackerStates.put(key, ts);
    }
    return ts;
  }

  @Override
  public synchronized RecoveredLocalizationState loadLocalizationState() {
    RecoveredLocalizationState result = new RecoveredLocalizationState();
    for (Map.Entry<TrackerKey, TrackerState> e : trackerStates.entrySet()) {
      TrackerKey tk = e.getKey();
      TrackerState ts = e.getValue();
      // check what kind of tracker state we have and recover appropriately
      // public trackers have user == null
      // private trackers have a valid user but appId == null
      // app-specific trackers have a valid user and valid appId
      if (tk.user == null) {
        result.publicTrackerState = loadTrackerState(ts);
      } else {
        RecoveredUserResources rur = result.userResources.get(tk.user);
        if (rur == null) {
          rur = new RecoveredUserResources();
          result.userResources.put(tk.user, rur);
        }
        if (tk.appId == null) {
          rur.privateTrackerState = loadTrackerState(ts);
        } else {
          rur.appTrackerStates.put(tk.appId, loadTrackerState(ts));
        }
      }
    }
    return result;
  }

  @Override
  public synchronized void startResourceLocalization(String user,
      ApplicationId appId, LocalResourceProto proto, Path localPath) {
    TrackerState ts = getTrackerState(new TrackerKey(user, appId));
    ts.inProgressMap.put(localPath, proto);
  }

  @Override
  public synchronized void finishResourceLocalization(String user,
      ApplicationId appId, LocalizedResourceProto proto) {
    TrackerState ts = getTrackerState(new TrackerKey(user, appId));
    Path localPath = new Path(proto.getLocalPath());
    ts.inProgressMap.remove(localPath);
    ts.localizedResources.put(localPath, proto);
  }

  @Override
  public synchronized void removeLocalizedResource(String user,
      ApplicationId appId, Path localPath) {
    TrackerState ts = trackerStates.get(new TrackerKey(user, appId));
    if (ts != null) {
      ts.inProgressMap.remove(localPath);
      ts.localizedResources.remove(localPath);
    }
  }


  @Override
  public RecoveredDeletionServiceState loadDeletionServiceState()
      throws IOException {
    RecoveredDeletionServiceState result =
        new RecoveredDeletionServiceState();
    result.tasks = new ArrayList<DeletionServiceDeleteTaskProto>(
        deleteTasks.values());
    return result;
  }

  @Override
  public synchronized void storeDeletionTask(int taskId,
      DeletionServiceDeleteTaskProto taskProto) throws IOException {
    deleteTasks.put(taskId, taskProto);
  }

  @Override
  public synchronized void removeDeletionTask(int taskId) throws IOException {
    deleteTasks.remove(taskId);
  }


  @Override
  public RecoveredNMTokensState loadNMTokensState() throws IOException {
    // return a copy so caller can't modify our state
    RecoveredNMTokensState result = new RecoveredNMTokensState();
    result.currentMasterKey = nmTokenState.currentMasterKey;
    result.previousMasterKey = nmTokenState.previousMasterKey;
    result.applicationMasterKeys =
        new HashMap<ApplicationAttemptId, MasterKey>(
            nmTokenState.applicationMasterKeys);
    return result;
  }

  @Override
  public void storeNMTokenCurrentMasterKey(MasterKey key)
      throws IOException {
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    nmTokenState.currentMasterKey = new MasterKeyPBImpl(keypb.getProto());
  }

  @Override
  public void storeNMTokenPreviousMasterKey(MasterKey key)
      throws IOException {
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    nmTokenState.previousMasterKey = new MasterKeyPBImpl(keypb.getProto());
  }

  @Override
  public void storeNMTokenApplicationMasterKey(ApplicationAttemptId attempt,
      MasterKey key) throws IOException {
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    nmTokenState.applicationMasterKeys.put(attempt,
        new MasterKeyPBImpl(keypb.getProto()));
  }

  @Override
  public void removeNMTokenApplicationMasterKey(ApplicationAttemptId attempt)
      throws IOException {
    nmTokenState.applicationMasterKeys.remove(attempt);
  }


  @Override
  public RecoveredContainerTokensState loadContainerTokensState()
      throws IOException {
    // return a copy so caller can't modify our state
    RecoveredContainerTokensState result =
        new RecoveredContainerTokensState();
    result.currentMasterKey = containerTokenState.currentMasterKey;
    result.previousMasterKey = containerTokenState.previousMasterKey;
    result.activeTokens =
        new HashMap<ContainerId, Long>(containerTokenState.activeTokens);
    return result;
  }

  @Override
  public void storeContainerTokenCurrentMasterKey(MasterKey key)
      throws IOException {
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    containerTokenState.currentMasterKey =
        new MasterKeyPBImpl(keypb.getProto());
  }

  @Override
  public void storeContainerTokenPreviousMasterKey(MasterKey key)
      throws IOException {
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    containerTokenState.previousMasterKey =
        new MasterKeyPBImpl(keypb.getProto());
  }

  @Override
  public void storeContainerToken(ContainerId containerId,
      Long expirationTime) throws IOException {
    containerTokenState.activeTokens.put(containerId, expirationTime);
  }

  @Override
  public void removeContainerToken(ContainerId containerId)
      throws IOException {
    containerTokenState.activeTokens.remove(containerId);
  }


  private static class TrackerState {
    Map<Path, LocalResourceProto> inProgressMap =
        new HashMap<Path, LocalResourceProto>();
    Map<Path, LocalizedResourceProto> localizedResources =
        new HashMap<Path, LocalizedResourceProto>();
  }

  private static class TrackerKey {
    String user;
    ApplicationId appId;

    public TrackerKey(String user, ApplicationId appId) {
      this.user = user;
      this.appId = appId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((appId == null) ? 0 : appId.hashCode());
      result = prime * result + ((user == null) ? 0 : user.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof TrackerKey))
        return false;
      TrackerKey other = (TrackerKey) obj;
      if (appId == null) {
        if (other.appId != null)
          return false;
      } else if (!appId.equals(other.appId))
        return false;
      if (user == null) {
        if (other.user != null)
          return false;
      } else if (!user.equals(other.user))
        return false;
      return true;
    }
  }
}
