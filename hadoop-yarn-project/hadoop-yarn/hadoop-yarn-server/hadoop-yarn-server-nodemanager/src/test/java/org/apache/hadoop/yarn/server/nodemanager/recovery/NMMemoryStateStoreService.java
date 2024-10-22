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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LogDeleterProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;


import org.apache.hadoop.yarn.server.utils.BuilderUtils;

public class NMMemoryStateStoreService extends NMStateStoreService {
  private Map<ApplicationId, ContainerManagerApplicationProto> apps;
  private Map<ContainerId, RecoveredContainerState> containerStates;
  private Map<TrackerKey, TrackerState> trackerStates;
  private Map<Integer, DeletionServiceDeleteTaskProto> deleteTasks;
  private RecoveredNMTokensState nmTokenState;
  private RecoveredContainerTokensState containerTokenState;
  private Map<ApplicationAttemptId, MasterKey> applicationMasterKeys;
  private Map<ContainerId, Long> activeTokens;
  private Map<ApplicationId, LogDeleterProto> logDeleterState;
  private Set<ContainerId> logAggregatorState;
  private RecoveredAMRMProxyState amrmProxyState;

  public NMMemoryStateStoreService() {
    super(NMMemoryStateStoreService.class.getName());
  }

  @Override
  protected void initStorage(Configuration conf) {
    apps = new HashMap<ApplicationId, ContainerManagerApplicationProto>();
    containerStates = new HashMap<ContainerId, RecoveredContainerState>();
    nmTokenState = new RecoveredNMTokensState();
    applicationMasterKeys = new HashMap<ApplicationAttemptId, MasterKey>();
    containerTokenState = new RecoveredContainerTokensState();
    activeTokens = new HashMap<ContainerId, Long>();
    trackerStates = new HashMap<TrackerKey, TrackerState>();
    deleteTasks = new HashMap<Integer, DeletionServiceDeleteTaskProto>();
    logDeleterState = new HashMap<ApplicationId, LogDeleterProto>();
    logAggregatorState = new HashSet<ContainerId>();
    amrmProxyState = new RecoveredAMRMProxyState();
  }

  @Override
  protected void startStorage() {
  }

  @Override
  protected void closeStorage() {
  }

  // Recovery Iterator Implementation.
  private class NMMemoryRecoveryIterator<T> implements RecoveryIterator<T> {

    private Iterator<T> it;

    NMMemoryRecoveryIterator(Iterator<T> it){
      this.it = it;
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public T next() throws IOException {
      return it.next();
    }

    @Override
    public void close() throws IOException {

    }
  }

  @Override
  public synchronized RecoveredApplicationsState loadApplicationsState()
      throws IOException {
    RecoveredApplicationsState state = new RecoveredApplicationsState();
    List<ContainerManagerApplicationProto> containerList =
        new ArrayList<ContainerManagerApplicationProto>(apps.values());
    state.it = new NMMemoryRecoveryIterator<ContainerManagerApplicationProto>(
        containerList.iterator());
    return state;
  }

  @Override
  public synchronized void storeApplication(ApplicationId appId,
      ContainerManagerApplicationProto proto) throws IOException {
    ContainerManagerApplicationProto protoCopy =
        ContainerManagerApplicationProto.parseFrom(proto.toByteString());
    apps.put(appId, protoCopy);
  }

  @Override
  public synchronized void removeApplication(ApplicationId appId)
      throws IOException {
    apps.remove(appId);
  }

  @Override
  public RecoveryIterator<RecoveredContainerState> getContainerStateIterator()
      throws IOException {
    // return a copy so caller can't modify our state
    List<RecoveredContainerState> result =
        new ArrayList<RecoveredContainerState>(containerStates.size());
    for (RecoveredContainerState rcs : containerStates.values()) {
      RecoveredContainerState rcsCopy = new RecoveredContainerState(rcs.getContainerId());
      rcsCopy.status = rcs.status;
      rcsCopy.exitCode = rcs.exitCode;
      rcsCopy.killed = rcs.killed;
      rcsCopy.diagnostics = rcs.diagnostics;
      rcsCopy.startRequest = rcs.startRequest;
      rcsCopy.capability = rcs.capability;
      rcsCopy.setRemainingRetryAttempts(rcs.getRemainingRetryAttempts());
      rcsCopy.setRestartTimes(rcs.getRestartTimes());
      rcsCopy.setWorkDir(rcs.getWorkDir());
      rcsCopy.setLogDir(rcs.getLogDir());
      rcsCopy.setResourceMappings(rcs.getResourceMappings());
      result.add(rcsCopy);
    }
    return new NMMemoryRecoveryIterator<RecoveredContainerState>(
        result.iterator());
  }

  @Override
  public synchronized void storeContainer(ContainerId containerId,
      int version, long startTime, StartContainerRequest startRequest) {
    RecoveredContainerState rcs = new RecoveredContainerState(containerId);
    rcs.startRequest = startRequest;
    rcs.status = RecoveredContainerStatus.REQUESTED;
    rcs.version = version;
    try {
      ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils
          .newContainerTokenIdentifier(startRequest.getContainerToken());
      rcs.capability =
          new ResourcePBImpl(containerTokenIdentifier.getProto().getResource());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    rcs.setStartTime(startTime);
    containerStates.put(containerId, rcs);
  }

  @Override
  public void storeContainerQueued(ContainerId containerId) throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.status = RecoveredContainerStatus.QUEUED;
  }

  @Override
  public void storeContainerPaused(ContainerId containerId) throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.status = RecoveredContainerStatus.PAUSED;
  }

  @Override
  public void removeContainerPaused(ContainerId containerId)
      throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.status = RecoveredContainerStatus.LAUNCHED;
  }

  @Override
  public synchronized void storeContainerDiagnostics(ContainerId containerId,
      StringBuilder diagnostics) throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.diagnostics = diagnostics.toString();
  }

  @Override
  public synchronized void storeContainerLaunched(ContainerId containerId)
      throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    if (rcs.exitCode != ContainerExitStatus.INVALID) {
      throw new IOException("Container already completed");
    }
    rcs.status = RecoveredContainerStatus.LAUNCHED;
  }

  @Override
  public void storeContainerUpdateToken(ContainerId containerId,
      ContainerTokenIdentifier containerTokenIdentifier) throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.capability = containerTokenIdentifier.getResource();
    rcs.version = containerTokenIdentifier.getVersion();
    Token currentToken = rcs.getStartRequest().getContainerToken();
    Token updatedToken = Token
        .newInstance(containerTokenIdentifier.getBytes(),
            ContainerTokenIdentifier.KIND.toString(),
            currentToken.getPassword().array(), currentToken.getService());
    rcs.startRequest.setContainerToken(updatedToken);
  }

  @Override
  public synchronized void storeContainerKilled(ContainerId containerId)
      throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.killed = true;
  }

  @Override
  public synchronized void storeContainerCompleted(ContainerId containerId,
      int exitCode) throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.status = RecoveredContainerStatus.COMPLETED;
    rcs.exitCode = exitCode;
  }

  @Override
  public void storeContainerRemainingRetryAttempts(ContainerId containerId,
      int remainingRetryAttempts) throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.setRemainingRetryAttempts(remainingRetryAttempts);
  }

  @Override
  public void storeContainerRestartTimes(
      ContainerId containerId, List<Long> restartTimes)
      throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.setRestartTimes(restartTimes);
  }

  @Override
  public void storeContainerWorkDir(ContainerId containerId,
      String workDir) throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.setWorkDir(workDir);
  }

  @Override
  public void storeContainerLogDir(ContainerId containerId,
      String logDir) throws IOException {
    RecoveredContainerState rcs = getRecoveredContainerState(containerId);
    rcs.setLogDir(logDir);
  }

  @Override
  public synchronized void removeContainer(ContainerId containerId)
      throws IOException {
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
    List<LocalizedResourceProto> completedResources =
        new ArrayList<LocalizedResourceProto>(ts.localizedResources.values());
    RecoveryIterator<LocalizedResourceProto> crIt =
        new NMMemoryRecoveryIterator<LocalizedResourceProto>(
            completedResources.iterator());

    Map<LocalResourceProto, Path> inProgressMap =
        new HashMap<LocalResourceProto, Path>();
    for (Map.Entry<Path, LocalResourceProto> entry :
         ts.inProgressMap.entrySet()) {
      inProgressMap.put(entry.getValue(), entry.getKey());
    }
    RecoveryIterator<Map.Entry<LocalResourceProto, Path>> srIt =
        new NMMemoryRecoveryIterator<Map.Entry<LocalResourceProto, Path>>(
            inProgressMap.entrySet().iterator());

    return new LocalResourceTrackerState(crIt, srIt);
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
    Map<String, RecoveredUserResources> userResources =
        new HashMap<String, RecoveredUserResources>();
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
        RecoveredUserResources rur = userResources.get(tk.user);
        if (rur == null) {
          rur = new RecoveredUserResources();
          userResources.put(tk.user, rur);
        }
        if (tk.appId == null) {
          rur.privateTrackerState = loadTrackerState(ts);
        } else {
          rur.appTrackerStates.put(tk.appId, loadTrackerState(ts));
        }
      }
    }
    result.it = new NMMemoryRecoveryIterator<Map.Entry<String, RecoveredUserResources>>(
        userResources.entrySet().iterator());
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
  public synchronized RecoveredDeletionServiceState loadDeletionServiceState()
      throws IOException {
    RecoveredDeletionServiceState result =
        new RecoveredDeletionServiceState();
    List<DeletionServiceDeleteTaskProto> deleteTaskProtos =
        new ArrayList<DeletionServiceDeleteTaskProto>(deleteTasks.values());
    result.it = new NMMemoryRecoveryIterator<DeletionServiceDeleteTaskProto>(
        deleteTaskProtos.iterator());
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
  public synchronized RecoveredNMTokensState loadNMTokensState()
      throws IOException {
    // return a copy so caller can't modify our state
    RecoveredNMTokensState result = new RecoveredNMTokensState();
    result.currentMasterKey = nmTokenState.currentMasterKey;
    result.previousMasterKey = nmTokenState.previousMasterKey;
    Map<ApplicationAttemptId, MasterKey> masterKeysMap =
        new HashMap<ApplicationAttemptId, MasterKey>(applicationMasterKeys);
    result.it = new NMMemoryRecoveryIterator<Map.Entry<ApplicationAttemptId, MasterKey>>(
        masterKeysMap.entrySet().iterator());
    return result;
  }

  @Override
  public synchronized void storeNMTokenCurrentMasterKey(MasterKey key)
      throws IOException {
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    nmTokenState.currentMasterKey = new MasterKeyPBImpl(keypb.getProto());
  }

  @Override
  public synchronized void storeNMTokenPreviousMasterKey(MasterKey key)
      throws IOException {
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    nmTokenState.previousMasterKey = new MasterKeyPBImpl(keypb.getProto());
  }

  @Override
  public synchronized void storeNMTokenApplicationMasterKey(
      ApplicationAttemptId attempt, MasterKey key) throws IOException {
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    applicationMasterKeys.put(attempt,
        new MasterKeyPBImpl(keypb.getProto()));
  }

  @Override
  public synchronized void removeNMTokenApplicationMasterKey(
      ApplicationAttemptId attempt) throws IOException {
    applicationMasterKeys.remove(attempt);
  }


  @Override
  public synchronized RecoveredContainerTokensState loadContainerTokensState()
      throws IOException {
    // return a copy so caller can't modify our state
    RecoveredContainerTokensState result =
        new RecoveredContainerTokensState();
    result.currentMasterKey = containerTokenState.currentMasterKey;
    result.previousMasterKey = containerTokenState.previousMasterKey;
    Map<ContainerId, Long> containersTokenMap =
        new HashMap<ContainerId, Long>(activeTokens);
    result.it = new NMMemoryRecoveryIterator<Map.Entry<ContainerId, Long>>(
        containersTokenMap.entrySet().iterator());
    return result;
  }

  @Override
  public synchronized void storeContainerTokenCurrentMasterKey(MasterKey key)
      throws IOException {
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    containerTokenState.currentMasterKey =
        new MasterKeyPBImpl(keypb.getProto());
  }

  @Override
  public synchronized void storeContainerTokenPreviousMasterKey(MasterKey key)
      throws IOException {
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    containerTokenState.previousMasterKey =
        new MasterKeyPBImpl(keypb.getProto());
  }

  @Override
  public synchronized void storeContainerToken(ContainerId containerId,
      Long expirationTime) throws IOException {
    activeTokens.put(containerId, expirationTime);
  }

  @Override
  public synchronized void removeContainerToken(ContainerId containerId)
      throws IOException {
    activeTokens.remove(containerId);
  }


  @Override
  public synchronized RecoveredLogDeleterState loadLogDeleterState()
      throws IOException {
    RecoveredLogDeleterState state = new RecoveredLogDeleterState();
    state.logDeleterMap = new HashMap<ApplicationId,LogDeleterProto>(
        logDeleterState);
    return state;
  }

  @Override
  public synchronized void storeLogDeleter(ApplicationId appId,
      LogDeleterProto proto)
      throws IOException {
    logDeleterState.put(appId, proto);
  }

  @Override
  public synchronized void removeLogDeleter(ApplicationId appId)
      throws IOException {
    logDeleterState.remove(appId);
  }

  @Override
  public synchronized RecoveredLogAggregatorState loadLogAggregatorState()
      throws IOException {
    RecoveredLogAggregatorState state = new RecoveredLogAggregatorState();
    state.logAggregators = new ArrayList<>(logAggregatorState);
    return state;
  }

  @Override
  public synchronized void storeLogAggregator(ContainerId containerId)
      throws IOException {
    logAggregatorState.add(containerId);
  }

  @Override
  public synchronized void removeLogAggregator(ContainerId containerId)
      throws IOException {
        logAggregatorState.remove(containerId);
  }

  @Override
  public synchronized RecoveredAMRMProxyState loadAMRMProxyState()
      throws IOException {
    // return a copy so caller can't modify our state
    RecoveredAMRMProxyState result = new RecoveredAMRMProxyState();
    result.setCurrentMasterKey(amrmProxyState.getCurrentMasterKey());
    result.setNextMasterKey(amrmProxyState.getNextMasterKey());
    for (Map.Entry<ApplicationAttemptId, Map<String, byte[]>> entry :
        amrmProxyState.getAppContexts().entrySet()) {
      result.getAppContexts().put(entry.getKey(),
          new HashMap<String, byte[]>(entry.getValue()));
    }
    return result;
  }

  @Override
  public synchronized void storeAMRMProxyCurrentMasterKey(MasterKey key)
      throws IOException {
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    amrmProxyState.setCurrentMasterKey(new MasterKeyPBImpl(keypb.getProto()));
  }

  @Override
  public synchronized void storeAMRMProxyNextMasterKey(MasterKey key)
      throws IOException {
    if (key == null) {
      amrmProxyState.setNextMasterKey(null);
      return;
    }
    MasterKeyPBImpl keypb = (MasterKeyPBImpl) key;
    amrmProxyState.setNextMasterKey(new MasterKeyPBImpl(keypb.getProto()));
  }

  @Override
  public synchronized void storeAMRMProxyAppContextEntry(
      ApplicationAttemptId attempt, String key, byte[] data)
      throws IOException {
    Map<String, byte[]> entryMap = amrmProxyState.getAppContexts().get(attempt);
    if (entryMap == null) {
      entryMap = new HashMap<>();
      amrmProxyState.getAppContexts().put(attempt, entryMap);
    }
    entryMap.put(key, Arrays.copyOf(data, data.length));
  }

  @Override
  public synchronized void removeAMRMProxyAppContextEntry(
      ApplicationAttemptId attempt, String key) throws IOException {
    Map<String, byte[]> entryMap = amrmProxyState.getAppContexts().get(attempt);
    if (entryMap != null) {
      entryMap.remove(key);
    }
  }

  @Override
  public synchronized void removeAMRMProxyAppContext(
      ApplicationAttemptId attempt) throws IOException {
    amrmProxyState.getAppContexts().remove(attempt);
  }

  @Override
  public void storeAssignedResources(Container container,
      String resourceType, List<Serializable> assignedResources)
      throws IOException {
    ResourceMappings.AssignedResources ar =
        new ResourceMappings.AssignedResources();
    ar.updateAssignedResources(assignedResources);
    containerStates.get(container.getContainerId()).getResourceMappings()
        .addAssignedResources(resourceType, ar);

    // update container resource mapping.
    updateContainerResourceMapping(container, resourceType, assignedResources);
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
