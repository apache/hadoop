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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.server.api.records.MasterKey;

@Private
@Unstable
public abstract class NMStateStoreService extends AbstractService {

  public NMStateStoreService(String name) {
    super(name);
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


  public abstract RecoveredDeletionServiceState loadDeletionServiceState()
      throws IOException;

  public abstract void storeDeletionTask(int taskId,
      DeletionServiceDeleteTaskProto taskProto) throws IOException;

  public abstract void removeDeletionTask(int taskId) throws IOException;


  public abstract RecoveredNMTokensState loadNMTokensState()
      throws IOException;

  public abstract void storeNMTokenCurrentMasterKey(MasterKey key)
      throws IOException;

  public abstract void storeNMTokenPreviousMasterKey(MasterKey key)
      throws IOException;

  public abstract void storeNMTokenApplicationMasterKey(
      ApplicationAttemptId attempt, MasterKey key) throws IOException;

  public abstract void removeNMTokenApplicationMasterKey(
      ApplicationAttemptId attempt) throws IOException;


  public abstract RecoveredContainerTokensState loadContainerTokensState()
      throws IOException;

  public abstract void storeContainerTokenCurrentMasterKey(MasterKey key)
      throws IOException;

  public abstract void storeContainerTokenPreviousMasterKey(MasterKey key)
      throws IOException;

  public abstract void storeContainerToken(ContainerId containerId,
      Long expirationTime) throws IOException;

  public abstract void removeContainerToken(ContainerId containerId)
      throws IOException;


  protected abstract void initStorage(Configuration conf) throws IOException;

  protected abstract void startStorage() throws IOException;

  protected abstract void closeStorage() throws IOException;
}
