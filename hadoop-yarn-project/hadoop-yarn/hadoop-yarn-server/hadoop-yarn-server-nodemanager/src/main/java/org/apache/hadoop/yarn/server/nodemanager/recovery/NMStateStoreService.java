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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LogDeleterProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;

@Private
@Unstable
public abstract class NMStateStoreService extends AbstractService {

  public NMStateStoreService(String name) {
    super(name);
  }

  public static class RecoveredApplicationsState {
    List<ContainerManagerApplicationProto> applications;

    public List<ContainerManagerApplicationProto> getApplications() {
      return applications;
    }

  }

  /**
   * Type of post recovery action.
   */
  public enum RecoveredContainerType {
    KILL, RECOVER
  }

  public enum RecoveredContainerStatus {
    REQUESTED,
    QUEUED,
    LAUNCHED,
    COMPLETED,
    PAUSED
  }

  public static class RecoveredContainerState {
    RecoveredContainerStatus status;
    int exitCode = ContainerExitStatus.INVALID;
    boolean killed = false;
    String diagnostics = "";
    StartContainerRequest startRequest;
    Resource capability;
    private int remainingRetryAttempts = ContainerRetryContext.RETRY_INVALID;
    private String workDir;
    private String logDir;
    int version;
    private RecoveredContainerType recoveryType =
        RecoveredContainerType.RECOVER;
    private long startTime;
    private ResourceMappings resMappings = new ResourceMappings();

    public RecoveredContainerStatus getStatus() {
      return status;
    }

    public int getExitCode() {
      return exitCode;
    }

    public boolean getKilled() {
      return killed;
    }

    public String getDiagnostics() {
      return diagnostics;
    }

    public int getVersion() {
      return version;
    }

    public long getStartTime() {
      return startTime;
    }

    public void setStartTime(long ts) {
      startTime = ts;
    }

    public StartContainerRequest getStartRequest() {
      return startRequest;
    }

    public Resource getCapability() {
      return capability;
    }

    public int getRemainingRetryAttempts() {
      return remainingRetryAttempts;
    }

    public void setRemainingRetryAttempts(int retryAttempts) {
      this.remainingRetryAttempts = retryAttempts;
    }

    public String getWorkDir() {
      return workDir;
    }

    public void setWorkDir(String workDir) {
      this.workDir = workDir;
    }

    public String getLogDir() {
      return logDir;
    }

    public void setLogDir(String logDir) {
      this.logDir = logDir;
    }

    @Override
    public String toString() {
      return new StringBuffer("Status: ").append(getStatus())
          .append(", Exit code: ").append(exitCode)
          .append(", Version: ").append(version)
          .append(", Start Time: ").append(startTime)
          .append(", Killed: ").append(getKilled())
          .append(", Diagnostics: ").append(getDiagnostics())
          .append(", Capability: ").append(getCapability())
          .append(", StartRequest: ").append(getStartRequest())
          .append(", RemainingRetryAttempts: ").append(remainingRetryAttempts)
          .append(", WorkDir: ").append(workDir)
          .append(", LogDir: ").append(logDir)
          .toString();
    }

    public RecoveredContainerType getRecoveryType() {
      return recoveryType;
    }

    public void setRecoveryType(RecoveredContainerType recoveryType) {
      this.recoveryType = recoveryType;
    }

    public ResourceMappings getResourceMappings() {
      return resMappings;
    }

    public void setResourceMappings(ResourceMappings mappings) {
      this.resMappings = mappings;
    }
  }

  public static class LocalResourceTrackerState {
    List<LocalizedResourceProto> localizedResources =
        new ArrayList<LocalizedResourceProto>();
    Map<LocalResourceProto, Path> inProgressResources =
        new HashMap<LocalResourceProto, Path>();

    public List<LocalizedResourceProto> getLocalizedResources() {
      return localizedResources;
    }

    public Map<LocalResourceProto, Path> getInProgressResources() {
      return inProgressResources;
    }

    public boolean isEmpty() {
      return localizedResources.isEmpty() && inProgressResources.isEmpty();
    }
  }

  public static class RecoveredUserResources {
    LocalResourceTrackerState privateTrackerState =
        new LocalResourceTrackerState();
    Map<ApplicationId, LocalResourceTrackerState> appTrackerStates =
        new HashMap<ApplicationId, LocalResourceTrackerState>();

    public LocalResourceTrackerState getPrivateTrackerState() {
      return privateTrackerState;
    }

    public Map<ApplicationId, LocalResourceTrackerState>
    getAppTrackerStates() {
      return appTrackerStates;
    }
  }

  public static class RecoveredLocalizationState {
    LocalResourceTrackerState publicTrackerState =
        new LocalResourceTrackerState();
    Map<String, RecoveredUserResources> userResources =
        new HashMap<String, RecoveredUserResources>();

    public LocalResourceTrackerState getPublicTrackerState() {
      return publicTrackerState;
    }

    public Map<String, RecoveredUserResources> getUserResources() {
      return userResources;
    }
  }

  public static class RecoveredDeletionServiceState {
    List<DeletionServiceDeleteTaskProto> tasks;

    public List<DeletionServiceDeleteTaskProto> getTasks() {
      return tasks;
    }
  }

  public static class RecoveredNMTokensState {
    MasterKey currentMasterKey;
    MasterKey previousMasterKey;
    Map<ApplicationAttemptId, MasterKey> applicationMasterKeys;

    public MasterKey getCurrentMasterKey() {
      return currentMasterKey;
    }

    public MasterKey getPreviousMasterKey() {
      return previousMasterKey;
    }

    public Map<ApplicationAttemptId, MasterKey> getApplicationMasterKeys() {
      return applicationMasterKeys;
    }
  }

  public static class RecoveredContainerTokensState {
    MasterKey currentMasterKey;
    MasterKey previousMasterKey;
    Map<ContainerId, Long> activeTokens;

    public MasterKey getCurrentMasterKey() {
      return currentMasterKey;
    }

    public MasterKey getPreviousMasterKey() {
      return previousMasterKey;
    }

    public Map<ContainerId, Long> getActiveTokens() {
      return activeTokens;
    }
  }

  public static class RecoveredLogDeleterState {
    Map<ApplicationId, LogDeleterProto> logDeleterMap;

    public Map<ApplicationId, LogDeleterProto> getLogDeleterMap() {
      return logDeleterMap;
    }
  }

  /**
   * Recovered states for AMRMProxy.
   */
  public static class RecoveredAMRMProxyState {
    private MasterKey currentMasterKey;
    private MasterKey nextMasterKey;
    // For each app, stores amrmToken, user name, as well as various AMRMProxy
    // intercepter states
    private Map<ApplicationAttemptId, Map<String, byte[]>> appContexts;

    public RecoveredAMRMProxyState() {
      appContexts = new HashMap<>();
    }

    public MasterKey getCurrentMasterKey() {
      return currentMasterKey;
    }

    public MasterKey getNextMasterKey() {
      return nextMasterKey;
    }

    public Map<ApplicationAttemptId, Map<String, byte[]>> getAppContexts() {
      return appContexts;
    }

    public void setCurrentMasterKey(MasterKey currentKey) {
      currentMasterKey = currentKey;
    }

    public void setNextMasterKey(MasterKey nextKey) {
      nextMasterKey = nextKey;
    }
  }

  /** Initialize the state storage */
  @Override
  public void serviceInit(Configuration conf) throws IOException {
    initStorage(conf);
  }

  /** Start the state storage for use */
  @Override
  public void serviceStart() throws IOException {
    startStorage();
  }

  /** Shutdown the state storage. */
  @Override
  public void serviceStop() throws IOException {
    closeStorage();
  }

  public boolean canRecover() {
    return true;
  }

  public boolean isNewlyCreated() {
    return false;
  }

  /**
   * Load the state of applications.
   * @return recovered state for applications.
   * @throws IOException IO Exception.
   */
  public abstract RecoveredApplicationsState loadApplicationsState()
      throws IOException;

  /**
   * Record the start of an application
   * @param appId the application ID
   * @param p state to store for the application
   * @throws IOException
   */
  public abstract void storeApplication(ApplicationId appId,
      ContainerManagerApplicationProto p) throws IOException;

  /**
   * Remove records corresponding to an application
   * @param appId the application ID
   * @throws IOException
   */
  public abstract void removeApplication(ApplicationId appId)
      throws IOException;


  /**
   * Load the state of containers
   * @return recovered state for containers
   * @throws IOException
   */
  public abstract List<RecoveredContainerState> loadContainersState()
      throws IOException;

  /**
   * Record a container start request
   * @param containerId the container ID
   * @param containerVersion the container Version
   * @param startTime container start time
   * @param startRequest the container start request
   * @throws IOException
   */
  public abstract void storeContainer(ContainerId containerId,
      int containerVersion, long startTime, StartContainerRequest startRequest)
      throws IOException;

  /**
   * Record that a container has been queued at the NM
   * @param containerId the container ID
   * @throws IOException
   */
  public abstract void storeContainerQueued(ContainerId containerId)
      throws IOException;

  /**
   * Record that a container has been paused at the NM.
   * @param containerId the container ID.
   * @throws IOException IO Exception.
   */
  public abstract void storeContainerPaused(ContainerId containerId)
      throws IOException;

  /**
   * Record that a container has been resumed at the NM by removing the
   * fact that it has be paused.
   * @param containerId the container ID.
   * @throws IOException IO Exception.
   */
  public abstract void removeContainerPaused(ContainerId containerId)
      throws IOException;

  /**
   * Record that a container has been launched
   * @param containerId the container ID
   * @throws IOException
   */
  public abstract void storeContainerLaunched(ContainerId containerId)
      throws IOException;

  /**
   * Record that a container has been updated
   * @param containerId the container ID
   * @param containerTokenIdentifier container token identifier
   * @throws IOException
   */
  public abstract void storeContainerUpdateToken(ContainerId containerId,
      ContainerTokenIdentifier containerTokenIdentifier) throws IOException;

  /**
   * Record that a container has completed
   * @param containerId the container ID
   * @param exitCode the exit code from the container
   * @throws IOException
   */
  public abstract void storeContainerCompleted(ContainerId containerId,
      int exitCode) throws IOException;

  /**
   * Record a request to kill a container
   * @param containerId the container ID
   * @throws IOException
   */
  public abstract void storeContainerKilled(ContainerId containerId)
      throws IOException;

  /**
   * Record diagnostics for a container
   * @param containerId the container ID
   * @param diagnostics the container diagnostics
   * @throws IOException
   */
  public abstract void storeContainerDiagnostics(ContainerId containerId,
      StringBuilder diagnostics) throws IOException;

  /**
   * Record remaining retry attempts for a container.
   * @param containerId the container ID
   * @param remainingRetryAttempts the remain retry times when container
   *                               fails to run
   * @throws IOException
   */
  public abstract void storeContainerRemainingRetryAttempts(
      ContainerId containerId, int remainingRetryAttempts) throws IOException;

  /**
   * Record working directory for a container.
   * @param containerId the container ID
   * @param workDir the working directory
   * @throws IOException
   */
  public abstract void storeContainerWorkDir(
      ContainerId containerId, String workDir) throws IOException;

  /**
   * Record log directory for a container.
   * @param containerId the container ID
   * @param logDir the log directory
   * @throws IOException
   */
  public abstract void storeContainerLogDir(
      ContainerId containerId, String logDir) throws IOException;

  /**
   * Remove records corresponding to a container
   * @param containerId the container ID
   * @throws IOException
   */
  public abstract void removeContainer(ContainerId containerId)
      throws IOException;


  /**
   * Load the state of localized resources
   * @return recovered localized resource state
   * @throws IOException
   */
  public abstract RecoveredLocalizationState loadLocalizationState()
      throws IOException;

  /**
   * Record the start of localization for a resource
   * @param user the username or null if the resource is public
   * @param appId the application ID if the resource is app-specific or null
   * @param proto the resource request
   * @param localPath local filesystem path where the resource will be stored
   * @throws IOException
   */
  public abstract void startResourceLocalization(String user,
      ApplicationId appId, LocalResourceProto proto, Path localPath)
          throws IOException;

  /**
   * Record the completion of a resource localization
   * @param user the username or null if the resource is public
   * @param appId the application ID if the resource is app-specific or null
   * @param proto the serialized localized resource
   * @throws IOException
   */
  public abstract void finishResourceLocalization(String user,
      ApplicationId appId, LocalizedResourceProto proto) throws IOException;

  /**
   * Remove records related to a resource localization
   * @param user the username or null if the resource is public
   * @param appId the application ID if the resource is app-specific or null
   * @param localPath local filesystem path where the resource will be stored
   * @throws IOException
   */
  public abstract void removeLocalizedResource(String user,
      ApplicationId appId, Path localPath) throws IOException;


  /**
   * Load the state of the deletion service
   * @return recovered deletion service state
   * @throws IOException
   */
  public abstract RecoveredDeletionServiceState loadDeletionServiceState()
      throws IOException;

  /**
   * Record a deletion task
   * @param taskId the deletion task ID
   * @param taskProto the deletion task protobuf
   * @throws IOException
   */
  public abstract void storeDeletionTask(int taskId,
      DeletionServiceDeleteTaskProto taskProto) throws IOException;

  /**
   * Remove records corresponding to a deletion task
   * @param taskId the deletion task ID
   * @throws IOException
   */
  public abstract void removeDeletionTask(int taskId) throws IOException;


  /**
   * Load the state of NM tokens
   * @return recovered state of NM tokens
   * @throws IOException
   */
  public abstract RecoveredNMTokensState loadNMTokensState()
      throws IOException;

  /**
   * Record the current NM token master key
   * @param key the master key
   * @throws IOException
   */
  public abstract void storeNMTokenCurrentMasterKey(MasterKey key)
      throws IOException;

  /**
   * Record the previous NM token master key
   * @param key the previous master key
   * @throws IOException
   */
  public abstract void storeNMTokenPreviousMasterKey(MasterKey key)
      throws IOException;

  /**
   * Record a master key corresponding to an application
   * @param attempt the application attempt ID
   * @param key the master key
   * @throws IOException
   */
  public abstract void storeNMTokenApplicationMasterKey(
      ApplicationAttemptId attempt, MasterKey key) throws IOException;

  /**
   * Remove a master key corresponding to an application
   * @param attempt the application attempt ID
   * @throws IOException
   */
  public abstract void removeNMTokenApplicationMasterKey(
      ApplicationAttemptId attempt) throws IOException;


  /**
   * Load the state of container tokens
   * @return recovered state of container tokens
   * @throws IOException
   */
  public abstract RecoveredContainerTokensState loadContainerTokensState()
      throws IOException;

  /**
   * Record the current container token master key
   * @param key the master key
   * @throws IOException
   */
  public abstract void storeContainerTokenCurrentMasterKey(MasterKey key)
      throws IOException;

  /**
   * Record the previous container token master key
   * @param key the previous master key
   * @throws IOException
   */
  public abstract void storeContainerTokenPreviousMasterKey(MasterKey key)
      throws IOException;

  /**
   * Record the expiration time for a container token
   * @param containerId the container ID
   * @param expirationTime the container token expiration time
   * @throws IOException
   */
  public abstract void storeContainerToken(ContainerId containerId,
      Long expirationTime) throws IOException;

  /**
   * Remove records for a container token
   * @param containerId the container ID
   * @throws IOException
   */
  public abstract void removeContainerToken(ContainerId containerId)
      throws IOException;


  /**
   * Load the state of log deleters
   * @return recovered log deleter state
   * @throws IOException
   */
  public abstract RecoveredLogDeleterState loadLogDeleterState()
      throws IOException;

  /**
   * Store the state of a log deleter
   * @param appId the application ID for the log deleter
   * @param proto the serialized state of the log deleter
   * @throws IOException
   */
  public abstract void storeLogDeleter(ApplicationId appId,
      LogDeleterProto proto) throws IOException;

  /**
   * Remove the state of a log deleter
   * @param appId the application ID for the log deleter
   * @throws IOException
   */
  public abstract void removeLogDeleter(ApplicationId appId)
      throws IOException;

  /**
   * Load the state of AMRMProxy.
   * @return recovered state of AMRMProxy
   * @throws IOException if fails
   */
  public abstract RecoveredAMRMProxyState loadAMRMProxyState()
      throws IOException;

  /**
   * Record the current AMRMProxyTokenSecretManager master key.
   * @param key the current master key
   * @throws IOException if fails
   */
  public abstract void storeAMRMProxyCurrentMasterKey(MasterKey key)
      throws IOException;

  /**
   * Record the next AMRMProxyTokenSecretManager master key.
   * @param key the next master key
   * @throws IOException if fails
   */
  public abstract void storeAMRMProxyNextMasterKey(MasterKey key)
      throws IOException;

  /**
   * Add a context entry for an application attempt in AMRMProxyService.
   * @param attempt app attempt ID
   * @param key key string
   * @param data state data to store
   * @throws IOException if fails
   */
  public abstract void storeAMRMProxyAppContextEntry(
      ApplicationAttemptId attempt, String key, byte[] data) throws IOException;

  /**
   * Remove a context entry for an application attempt in AMRMProxyService.
   * @param attempt attempt ID
   * @param key key string
   * @throws IOException if fails
   */
  public abstract void removeAMRMProxyAppContextEntry(
      ApplicationAttemptId attempt, String key) throws IOException;

  /**
   * Remove the entire context map for an application attempt in
   * AMRMProxyService.
   * @param attempt attempt ID
   * @throws IOException if fails
   */
  public abstract void removeAMRMProxyAppContext(ApplicationAttemptId attempt)
      throws IOException;

  /**
   * Store the assigned resources to a container.
   *
   * @param container NMContainer
   * @param resourceType Resource Type
   * @param assignedResources Assigned resources
   * @throws IOException if fails
   */
  public abstract void storeAssignedResources(Container container,
      String resourceType, List<Serializable> assignedResources)
      throws IOException;

  protected abstract void initStorage(Configuration conf) throws IOException;

  protected abstract void startStorage() throws IOException;

  protected abstract void closeStorage() throws IOException;

  protected void updateContainerResourceMapping(Container container,
      String resourceType, List<Serializable> assignedResources) {
    // Update Container#getResourceMapping.
    ResourceMappings.AssignedResources newAssigned =
        new ResourceMappings.AssignedResources();
    newAssigned.updateAssignedResources(assignedResources);
    container.getResourceMappings().addAssignedResources(resourceType,
        newAssigned);
  }
}
