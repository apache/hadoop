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

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import com.google.common.annotations.VisibleForTesting;

public class NMLeveldbStateStoreService extends NMStateStoreService {

  public static final Log LOG =
      LogFactory.getLog(NMLeveldbStateStoreService.class);

  private static final String DB_NAME = "yarn-nm-state";
  private static final String DB_SCHEMA_VERSION_KEY = "nm-schema-version";
  
  private static final Version CURRENT_VERSION_INFO = Version
      .newInstance(1, 0);

  private static final String DELETION_TASK_KEY_PREFIX =
      "DeletionService/deltask_";

  private static final String APPLICATIONS_KEY_PREFIX =
      "ContainerManager/applications/";
  private static final String FINISHED_APPS_KEY_PREFIX =
      "ContainerManager/finishedApps/";

  private static final String LOCALIZATION_KEY_PREFIX = "Localization/";
  private static final String LOCALIZATION_PUBLIC_KEY_PREFIX =
      LOCALIZATION_KEY_PREFIX + "public/";
  private static final String LOCALIZATION_PRIVATE_KEY_PREFIX =
      LOCALIZATION_KEY_PREFIX + "private/";
  private static final String LOCALIZATION_STARTED_SUFFIX = "started/";
  private static final String LOCALIZATION_COMPLETED_SUFFIX = "completed/";
  private static final String LOCALIZATION_FILECACHE_SUFFIX = "filecache/";
  private static final String LOCALIZATION_APPCACHE_SUFFIX = "appcache/";

  private static final String CONTAINERS_KEY_PREFIX =
      "ContainerManager/containers/";
  private static final String CONTAINER_REQUEST_KEY_SUFFIX = "/request";
  private static final String CONTAINER_DIAGS_KEY_SUFFIX = "/diagnostics";
  private static final String CONTAINER_LAUNCHED_KEY_SUFFIX = "/launched";
  private static final String CONTAINER_KILLED_KEY_SUFFIX = "/killed";
  private static final String CONTAINER_EXIT_CODE_KEY_SUFFIX = "/exitcode";

  private static final String CURRENT_MASTER_KEY_SUFFIX = "CurrentMasterKey";
  private static final String PREV_MASTER_KEY_SUFFIX = "PreviousMasterKey";
  private static final String NM_TOKENS_KEY_PREFIX = "NMTokens/";
  private static final String NM_TOKENS_CURRENT_MASTER_KEY =
      NM_TOKENS_KEY_PREFIX + CURRENT_MASTER_KEY_SUFFIX;
  private static final String NM_TOKENS_PREV_MASTER_KEY =
      NM_TOKENS_KEY_PREFIX + PREV_MASTER_KEY_SUFFIX;
  private static final String CONTAINER_TOKENS_KEY_PREFIX =
      "ContainerTokens/";
  private static final String CONTAINER_TOKENS_CURRENT_MASTER_KEY =
      CONTAINER_TOKENS_KEY_PREFIX + CURRENT_MASTER_KEY_SUFFIX;
  private static final String CONTAINER_TOKENS_PREV_MASTER_KEY =
      CONTAINER_TOKENS_KEY_PREFIX + PREV_MASTER_KEY_SUFFIX;

  private static final byte[] EMPTY_VALUE = new byte[0];

  private DB db;
  private boolean isNewlyCreated;

  public NMLeveldbStateStoreService() {
    super(NMLeveldbStateStoreService.class.getName());
  }

  @Override
  protected void startStorage() throws IOException {
  }

  @Override
  protected void closeStorage() throws IOException {
    if (db != null) {
      db.close();
    }
  }

  @Override
  public boolean isNewlyCreated() {
    return isNewlyCreated;
  }


  @Override
  public List<RecoveredContainerState> loadContainersState()
      throws IOException {
    ArrayList<RecoveredContainerState> containers =
        new ArrayList<RecoveredContainerState>();
    ArrayList<ContainerId> containersToRemove =
              new ArrayList<ContainerId>();
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(CONTAINERS_KEY_PREFIX));

      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.peekNext();
        String key = asString(entry.getKey());
        if (!key.startsWith(CONTAINERS_KEY_PREFIX)) {
          break;
        }

        int idEndPos = key.indexOf('/', CONTAINERS_KEY_PREFIX.length());
        if (idEndPos < 0) {
          throw new IOException("Unable to determine container in key: " + key);
        }
        ContainerId containerId = ConverterUtils.toContainerId(
            key.substring(CONTAINERS_KEY_PREFIX.length(), idEndPos));
        String keyPrefix = key.substring(0, idEndPos+1);
        RecoveredContainerState rcs = loadContainerState(containerId,
            iter, keyPrefix);
        // Don't load container without StartContainerRequest
        if (rcs.startRequest != null) {
          containers.add(rcs);
        } else {
          containersToRemove.add(containerId);
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }

    // remove container without StartContainerRequest
    for (ContainerId containerId : containersToRemove) {
      LOG.warn("Remove container " + containerId +
          " with incomplete records");
      try {
        removeContainer(containerId);
        // TODO: kill and cleanup the leaked container
      } catch (IOException e) {
        LOG.error("Unable to remove container " + containerId +
            " in store", e);
      }
    }

    return containers;
  }

  private RecoveredContainerState loadContainerState(ContainerId containerId,
      LeveldbIterator iter, String keyPrefix) throws IOException {
    RecoveredContainerState rcs = new RecoveredContainerState();
    rcs.status = RecoveredContainerStatus.REQUESTED;
    while (iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        break;
      }
      iter.next();

      String suffix = key.substring(keyPrefix.length()-1);  // start with '/'
      if (suffix.equals(CONTAINER_REQUEST_KEY_SUFFIX)) {
        rcs.startRequest = new StartContainerRequestPBImpl(
            StartContainerRequestProto.parseFrom(entry.getValue()));
      } else if (suffix.equals(CONTAINER_DIAGS_KEY_SUFFIX)) {
        rcs.diagnostics = asString(entry.getValue());
      } else if (suffix.equals(CONTAINER_LAUNCHED_KEY_SUFFIX)) {
        if (rcs.status == RecoveredContainerStatus.REQUESTED) {
          rcs.status = RecoveredContainerStatus.LAUNCHED;
        }
      } else if (suffix.equals(CONTAINER_KILLED_KEY_SUFFIX)) {
        rcs.killed = true;
      } else if (suffix.equals(CONTAINER_EXIT_CODE_KEY_SUFFIX)) {
        rcs.status = RecoveredContainerStatus.COMPLETED;
        rcs.exitCode = Integer.parseInt(asString(entry.getValue()));
      } else {
        throw new IOException("Unexpected container state key: " + key);
      }
    }
    return rcs;
  }

  @Override
  public void storeContainer(ContainerId containerId,
      StartContainerRequest startRequest) throws IOException {
    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_REQUEST_KEY_SUFFIX;
    try {
      db.put(bytes(key),
        ((StartContainerRequestPBImpl) startRequest).getProto().toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerDiagnostics(ContainerId containerId,
      StringBuilder diagnostics) throws IOException {
    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_DIAGS_KEY_SUFFIX;
    try {
      db.put(bytes(key), bytes(diagnostics.toString()));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerLaunched(ContainerId containerId)
      throws IOException {
    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_LAUNCHED_KEY_SUFFIX;
    try {
      db.put(bytes(key), EMPTY_VALUE);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerKilled(ContainerId containerId)
      throws IOException {
    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_KILLED_KEY_SUFFIX;
    try {
      db.put(bytes(key), EMPTY_VALUE);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerCompleted(ContainerId containerId,
      int exitCode) throws IOException {
    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_EXIT_CODE_KEY_SUFFIX;
    try {
      db.put(bytes(key), bytes(Integer.toString(exitCode)));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeContainer(ContainerId containerId)
      throws IOException {
    String keyPrefix = CONTAINERS_KEY_PREFIX + containerId.toString();
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        batch.delete(bytes(keyPrefix + CONTAINER_REQUEST_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_DIAGS_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_LAUNCHED_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_KILLED_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_EXIT_CODE_KEY_SUFFIX));
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }


  @Override
  public RecoveredApplicationsState loadApplicationsState()
      throws IOException {
    RecoveredApplicationsState state = new RecoveredApplicationsState();
    state.applications = new ArrayList<ContainerManagerApplicationProto>();
    String keyPrefix = APPLICATIONS_KEY_PREFIX;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(keyPrefix));
      while (iter.hasNext()) {
        Entry<byte[], byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(keyPrefix)) {
          break;
        }
        state.applications.add(
            ContainerManagerApplicationProto.parseFrom(entry.getValue()));
      }

      state.finishedApplications = new ArrayList<ApplicationId>();
      keyPrefix = FINISHED_APPS_KEY_PREFIX;
      iter.seek(bytes(keyPrefix));
      while (iter.hasNext()) {
        Entry<byte[], byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(keyPrefix)) {
          break;
        }
        ApplicationId appId =
            ConverterUtils.toApplicationId(key.substring(keyPrefix.length()));
        state.finishedApplications.add(appId);
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }

    return state;
  }

  @Override
  public void storeApplication(ApplicationId appId,
      ContainerManagerApplicationProto p) throws IOException {
    String key = APPLICATIONS_KEY_PREFIX + appId;
    try {
      db.put(bytes(key), p.toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void storeFinishedApplication(ApplicationId appId)
      throws IOException {
    String key = FINISHED_APPS_KEY_PREFIX + appId;
    try {
      db.put(bytes(key), new byte[0]);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeApplication(ApplicationId appId)
      throws IOException {
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        String key = APPLICATIONS_KEY_PREFIX + appId;
        batch.delete(bytes(key));
        key = FINISHED_APPS_KEY_PREFIX + appId;
        batch.delete(bytes(key));
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }


  @Override
  public RecoveredLocalizationState loadLocalizationState()
      throws IOException {
    RecoveredLocalizationState state = new RecoveredLocalizationState();

    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(LOCALIZATION_PUBLIC_KEY_PREFIX));
      state.publicTrackerState = loadResourceTrackerState(iter,
          LOCALIZATION_PUBLIC_KEY_PREFIX);

      iter.seek(bytes(LOCALIZATION_PRIVATE_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.peekNext();
        String key = asString(entry.getKey());
        if (!key.startsWith(LOCALIZATION_PRIVATE_KEY_PREFIX)) {
          break;
        }

        int userEndPos = key.indexOf('/',
            LOCALIZATION_PRIVATE_KEY_PREFIX.length());
        if (userEndPos < 0) {
          throw new IOException("Unable to determine user in resource key: "
              + key);
        }
        String user = key.substring(
            LOCALIZATION_PRIVATE_KEY_PREFIX.length(), userEndPos);
        state.userResources.put(user, loadUserLocalizedResources(iter,
            key.substring(0, userEndPos+1)));
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }

    return state;
  }

  private LocalResourceTrackerState loadResourceTrackerState(
      LeveldbIterator iter, String keyPrefix) throws IOException {
    final String completedPrefix = keyPrefix + LOCALIZATION_COMPLETED_SUFFIX;
    final String startedPrefix = keyPrefix + LOCALIZATION_STARTED_SUFFIX;
    LocalResourceTrackerState state = new LocalResourceTrackerState();
    while (iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        break;
      }

      if (key.startsWith(completedPrefix)) {
        state.localizedResources = loadCompletedResources(iter,
            completedPrefix);
      } else if (key.startsWith(startedPrefix)) {
        state.inProgressResources = loadStartedResources(iter, startedPrefix);
      } else {
        throw new IOException("Unexpected key in resource tracker state: "
            + key);
      }
    }

    return state;
  }

  private List<LocalizedResourceProto> loadCompletedResources(
      LeveldbIterator iter, String keyPrefix) throws IOException {
    List<LocalizedResourceProto> rsrcs =
        new ArrayList<LocalizedResourceProto>();
    while (iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        break;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Loading completed resource from " + key);
      }
      rsrcs.add(LocalizedResourceProto.parseFrom(entry.getValue()));
      iter.next();
    }

    return rsrcs;
  }

  private Map<LocalResourceProto, Path> loadStartedResources(
      LeveldbIterator iter, String keyPrefix) throws IOException {
    Map<LocalResourceProto, Path> rsrcs =
        new HashMap<LocalResourceProto, Path>();
    while (iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        break;
      }

      Path localPath = new Path(key.substring(keyPrefix.length()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Loading in-progress resource at " + localPath);
      }
      rsrcs.put(LocalResourceProto.parseFrom(entry.getValue()), localPath);
      iter.next();
    }

    return rsrcs;
  }

  private RecoveredUserResources loadUserLocalizedResources(
      LeveldbIterator iter, String keyPrefix) throws IOException {
    RecoveredUserResources userResources = new RecoveredUserResources();
    while (iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        break;
      }

      if (key.startsWith(LOCALIZATION_FILECACHE_SUFFIX, keyPrefix.length())) {
        userResources.privateTrackerState = loadResourceTrackerState(iter,
            keyPrefix + LOCALIZATION_FILECACHE_SUFFIX);
      } else if (key.startsWith(LOCALIZATION_APPCACHE_SUFFIX,
          keyPrefix.length())) {
        int appIdStartPos = keyPrefix.length() +
            LOCALIZATION_APPCACHE_SUFFIX.length();
        int appIdEndPos = key.indexOf('/', appIdStartPos);
        if (appIdEndPos < 0) {
          throw new IOException("Unable to determine appID in resource key: "
              + key);
        }
        ApplicationId appId = ConverterUtils.toApplicationId(
            key.substring(appIdStartPos, appIdEndPos));
        userResources.appTrackerStates.put(appId,
            loadResourceTrackerState(iter, key.substring(0, appIdEndPos+1)));
      } else {
        throw new IOException("Unexpected user resource key " + key);
      }
    }
    return userResources;
  }

  @Override
  public void startResourceLocalization(String user, ApplicationId appId,
      LocalResourceProto proto, Path localPath) throws IOException {
    String key = getResourceStartedKey(user, appId, localPath.toString());
    try {
      db.put(bytes(key), proto.toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void finishResourceLocalization(String user, ApplicationId appId,
      LocalizedResourceProto proto) throws IOException {
    String localPath = proto.getLocalPath();
    String startedKey = getResourceStartedKey(user, appId, localPath);
    String completedKey = getResourceCompletedKey(user, appId, localPath);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing localized resource to " + completedKey);
    }
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        batch.delete(bytes(startedKey));
        batch.put(bytes(completedKey), proto.toByteArray());
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeLocalizedResource(String user, ApplicationId appId,
      Path localPath) throws IOException {
    String localPathStr = localPath.toString();
    String startedKey = getResourceStartedKey(user, appId, localPathStr);
    String completedKey = getResourceCompletedKey(user, appId, localPathStr);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing local resource at " + localPathStr);
    }
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        batch.delete(bytes(startedKey));
        batch.delete(bytes(completedKey));
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  private String getResourceStartedKey(String user, ApplicationId appId,
      String localPath) {
    return getResourceTrackerKeyPrefix(user, appId)
        + LOCALIZATION_STARTED_SUFFIX + localPath;
  }

  private String getResourceCompletedKey(String user, ApplicationId appId,
      String localPath) {
    return getResourceTrackerKeyPrefix(user, appId)
        + LOCALIZATION_COMPLETED_SUFFIX + localPath;
  }

  private String getResourceTrackerKeyPrefix(String user,
      ApplicationId appId) {
    if (user == null) {
      return LOCALIZATION_PUBLIC_KEY_PREFIX;
    }
    if (appId == null) {
      return LOCALIZATION_PRIVATE_KEY_PREFIX + user + "/"
          + LOCALIZATION_FILECACHE_SUFFIX;
    }
    return LOCALIZATION_PRIVATE_KEY_PREFIX + user + "/"
        + LOCALIZATION_APPCACHE_SUFFIX + appId + "/";
  }


  @Override
  public RecoveredDeletionServiceState loadDeletionServiceState()
      throws IOException {
    RecoveredDeletionServiceState state = new RecoveredDeletionServiceState();
    state.tasks = new ArrayList<DeletionServiceDeleteTaskProto>();
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(DELETION_TASK_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[], byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(DELETION_TASK_KEY_PREFIX)) {
          break;
        }
        state.tasks.add(
            DeletionServiceDeleteTaskProto.parseFrom(entry.getValue()));
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    return state;
  }

  @Override
  public void storeDeletionTask(int taskId,
      DeletionServiceDeleteTaskProto taskProto) throws IOException {
    String key = DELETION_TASK_KEY_PREFIX + taskId;
    try {
      db.put(bytes(key), taskProto.toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeDeletionTask(int taskId) throws IOException {
    String key = DELETION_TASK_KEY_PREFIX + taskId;
    try {
      db.delete(bytes(key));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }


  @Override
  public RecoveredNMTokensState loadNMTokensState() throws IOException {
    RecoveredNMTokensState state = new RecoveredNMTokensState();
    state.applicationMasterKeys =
        new HashMap<ApplicationAttemptId, MasterKey>();
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(NM_TOKENS_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[], byte[]> entry = iter.next();
        String fullKey = asString(entry.getKey());
        if (!fullKey.startsWith(NM_TOKENS_KEY_PREFIX)) {
          break;
        }
        String key = fullKey.substring(NM_TOKENS_KEY_PREFIX.length());
        if (key.equals(CURRENT_MASTER_KEY_SUFFIX)) {
          state.currentMasterKey = parseMasterKey(entry.getValue());
        } else if (key.equals(PREV_MASTER_KEY_SUFFIX)) {
          state.previousMasterKey = parseMasterKey(entry.getValue());
        } else if (key.startsWith(
            ApplicationAttemptId.appAttemptIdStrPrefix)) {
          ApplicationAttemptId attempt;
          try {
            attempt = ConverterUtils.toApplicationAttemptId(key);
          } catch (IllegalArgumentException e) {
            throw new IOException("Bad application master key state for "
                + fullKey, e);
          }
          state.applicationMasterKeys.put(attempt,
              parseMasterKey(entry.getValue()));
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    return state;
  }

  @Override
  public void storeNMTokenCurrentMasterKey(MasterKey key)
      throws IOException {
    storeMasterKey(NM_TOKENS_CURRENT_MASTER_KEY, key);
  }

  @Override
  public void storeNMTokenPreviousMasterKey(MasterKey key)
      throws IOException {
    storeMasterKey(NM_TOKENS_PREV_MASTER_KEY, key);
  }

  @Override
  public void storeNMTokenApplicationMasterKey(
      ApplicationAttemptId attempt, MasterKey key) throws IOException {
    storeMasterKey(NM_TOKENS_KEY_PREFIX + attempt, key);
  }

  @Override
  public void removeNMTokenApplicationMasterKey(
      ApplicationAttemptId attempt) throws IOException {
    String key = NM_TOKENS_KEY_PREFIX + attempt;
    try {
      db.delete(bytes(key));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  private MasterKey parseMasterKey(byte[] keyData) throws IOException {
    return new MasterKeyPBImpl(MasterKeyProto.parseFrom(keyData));
  }

  private void storeMasterKey(String dbKey, MasterKey key)
      throws IOException {
    MasterKeyPBImpl pb = (MasterKeyPBImpl) key;
    try {
      db.put(bytes(dbKey), pb.getProto().toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }


  @Override
  public RecoveredContainerTokensState loadContainerTokensState()
      throws IOException {
    RecoveredContainerTokensState state = new RecoveredContainerTokensState();
    state.activeTokens = new HashMap<ContainerId, Long>();
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(CONTAINER_TOKENS_KEY_PREFIX));
      final int containerTokensKeyPrefixLength =
          CONTAINER_TOKENS_KEY_PREFIX.length();
      while (iter.hasNext()) {
        Entry<byte[], byte[]> entry = iter.next();
        String fullKey = asString(entry.getKey());
        if (!fullKey.startsWith(CONTAINER_TOKENS_KEY_PREFIX)) {
          break;
        }
        String key = fullKey.substring(containerTokensKeyPrefixLength);
        if (key.equals(CURRENT_MASTER_KEY_SUFFIX)) {
          state.currentMasterKey = parseMasterKey(entry.getValue());
        } else if (key.equals(PREV_MASTER_KEY_SUFFIX)) {
          state.previousMasterKey = parseMasterKey(entry.getValue());
        } else if (key.startsWith(ConverterUtils.CONTAINER_PREFIX)) {
          loadContainerToken(state, fullKey, key, entry.getValue());
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    return state;
  }

  private static void loadContainerToken(RecoveredContainerTokensState state,
      String key, String containerIdStr, byte[] value) throws IOException {
    ContainerId containerId;
    Long expTime;
    try {
      containerId = ConverterUtils.toContainerId(containerIdStr);
      expTime = Long.parseLong(asString(value));
    } catch (IllegalArgumentException e) {
      throw new IOException("Bad container token state for " + key, e);
    }
    state.activeTokens.put(containerId, expTime);
  }

  @Override
  public void storeContainerTokenCurrentMasterKey(MasterKey key)
      throws IOException {
    storeMasterKey(CONTAINER_TOKENS_CURRENT_MASTER_KEY, key);
  }

  @Override
  public void storeContainerTokenPreviousMasterKey(MasterKey key)
      throws IOException {
    storeMasterKey(CONTAINER_TOKENS_PREV_MASTER_KEY, key);
  }

  @Override
  public void storeContainerToken(ContainerId containerId, Long expTime)
      throws IOException {
    String key = CONTAINER_TOKENS_KEY_PREFIX + containerId;
    try {
      db.put(bytes(key), bytes(expTime.toString()));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeContainerToken(ContainerId containerId)
      throws IOException {
    String key = CONTAINER_TOKENS_KEY_PREFIX + containerId;
    try {
      db.delete(bytes(key));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }


  @Override
  protected void initStorage(Configuration conf)
      throws IOException {
    Path storeRoot = createStorageDir(conf);
    Options options = new Options();
    options.createIfMissing(false);
    options.logger(new LeveldbLogger());
    LOG.info("Using state database at " + storeRoot + " for recovery");
    File dbfile = new File(storeRoot.toString());
    try {
      db = JniDBFactory.factory.open(dbfile, options);
    } catch (NativeDB.DBException e) {
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        LOG.info("Creating state database at " + dbfile);
        isNewlyCreated = true;
        options.createIfMissing(true);
        try {
          db = JniDBFactory.factory.open(dbfile, options);
          // store version
          storeVersion();
        } catch (DBException dbErr) {
          throw new IOException(dbErr.getMessage(), dbErr);
        }
      } else {
        throw e;
      }
    }
    checkVersion();
  }

  private Path createStorageDir(Configuration conf) throws IOException {
    final String storeUri = conf.get(YarnConfiguration.NM_RECOVERY_DIR);
    if (storeUri == null) {
      throw new IOException("No store location directory configured in " +
          YarnConfiguration.NM_RECOVERY_DIR);
    }

    Path root = new Path(storeUri, DB_NAME);
    FileSystem fs = FileSystem.getLocal(conf);
    fs.mkdirs(root, new FsPermission((short)0700));
    return root;
  }


  private static class LeveldbLogger implements Logger {
    private static final Log LOG = LogFactory.getLog(LeveldbLogger.class);

    @Override
    public void log(String message) {
      LOG.info(message);
    }
  }


  Version loadVersion() throws IOException {
    byte[] data = db.get(bytes(DB_SCHEMA_VERSION_KEY));
    // if version is not stored previously, treat it as 1.0.
    if (data == null || data.length == 0) {
      return Version.newInstance(1, 0);
    }
    Version version =
        new VersionPBImpl(VersionProto.parseFrom(data));
    return version;
  }

  private void storeVersion() throws IOException {
    dbStoreVersion(CURRENT_VERSION_INFO);
  }
  
  // Only used for test
  @VisibleForTesting
  void storeVersion(Version state) throws IOException {
    dbStoreVersion(state);
  }
  
  private void dbStoreVersion(Version state) throws IOException {
    String key = DB_SCHEMA_VERSION_KEY;
    byte[] data = 
        ((VersionPBImpl) state).getProto().toByteArray();
    try {
      db.put(bytes(key), data);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }
  
  /**
   * 1) Versioning scheme: major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
   * 2) Any incompatible change of state-store is a major upgrade, and any
   *    compatible change of state-store is a minor upgrade.
   * 3) Within a minor upgrade, say 1.1 to 1.2:
   *    overwrite the version info and proceed as normal.
   * 4) Within a major upgrade, say 1.2 to 2.0:
   *    throw exception and indicate user to use a separate upgrade tool to
   *    upgrade NM state or remove incompatible old state.
   */
  private void checkVersion() throws IOException {
    Version loadedVersion = loadVersion();
    LOG.info("Loaded NM state version info " + loadedVersion);
    if (loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {
      LOG.info("Storing NM state version info " + getCurrentVersion());
      storeVersion();
    } else {
      throw new IOException(
        "Incompatible version for NM state: expecting NM state version " 
            + getCurrentVersion() + ", but loading version " + loadedVersion);
    }
  }
  
}
