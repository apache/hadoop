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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.ContainerTokenIdentifierProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LogDeleterProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class NMLeveldbStateStoreService extends NMStateStoreService {

  public static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(NMLeveldbStateStoreService.class);

  private static final String DB_NAME = "yarn-nm-state";
  private static final String DB_SCHEMA_VERSION_KEY = "nm-schema-version";

  /**
   * Changes from 1.0 to 1.1: Save AMRMProxy state in NMSS.
   * Changes from 1.1 to 1.2: Save queued container information.
   */
  private static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 2);

  private static final String DELETION_TASK_KEY_PREFIX =
      "DeletionService/deltask_";

  private static final String APPLICATIONS_KEY_PREFIX =
      "ContainerManager/applications/";
  @Deprecated
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
  private static final String CONTAINER_VERSION_KEY_SUFFIX = "/version";
  private static final String CONTAINER_START_TIME_KEY_SUFFIX = "/starttime";
  private static final String CONTAINER_DIAGS_KEY_SUFFIX = "/diagnostics";
  private static final String CONTAINER_LAUNCHED_KEY_SUFFIX = "/launched";
  private static final String CONTAINER_QUEUED_KEY_SUFFIX = "/queued";
  private static final String CONTAINER_PAUSED_KEY_SUFFIX = "/paused";
  private static final String CONTAINER_UPDATE_TOKEN_SUFFIX =
      "/updateToken";
  private static final String CONTAINER_KILLED_KEY_SUFFIX = "/killed";
  private static final String CONTAINER_EXIT_CODE_KEY_SUFFIX = "/exitcode";
  private static final String CONTAINER_REMAIN_RETRIES_KEY_SUFFIX =
      "/remainingRetryAttempts";
  private static final String CONTAINER_RESTART_TIMES_SUFFIX =
      "/restartTimes";
  private static final String CONTAINER_WORK_DIR_KEY_SUFFIX = "/workdir";
  private static final String CONTAINER_LOG_DIR_KEY_SUFFIX = "/logdir";

  private static final String CURRENT_MASTER_KEY_SUFFIX = "CurrentMasterKey";
  private static final String PREV_MASTER_KEY_SUFFIX = "PreviousMasterKey";
  private static final String NEXT_MASTER_KEY_SUFFIX = "NextMasterKey";
  private static final String NM_TOKENS_KEY_PREFIX = "NMTokens/";
  private static final String NM_TOKENS_CURRENT_MASTER_KEY =
      NM_TOKENS_KEY_PREFIX + CURRENT_MASTER_KEY_SUFFIX;
  private static final String NM_TOKENS_PREV_MASTER_KEY =
      NM_TOKENS_KEY_PREFIX + PREV_MASTER_KEY_SUFFIX;
  private static final String CONTAINER_TOKENS_KEY_PREFIX =
      "ContainerTokens/";
  private static final String CONTAINER_TOKEN_SECRETMANAGER_CURRENT_MASTER_KEY =
      CONTAINER_TOKENS_KEY_PREFIX + CURRENT_MASTER_KEY_SUFFIX;
  private static final String CONTAINER_TOKEN_SECRETMANAGER_PREV_MASTER_KEY =
      CONTAINER_TOKENS_KEY_PREFIX + PREV_MASTER_KEY_SUFFIX;

  private static final String LOG_DELETER_KEY_PREFIX = "LogDeleters/";

  private static final String AMRMPROXY_KEY_PREFIX = "AMRMProxy/";

  /**
   * The Local Tracker State DB key locations - "completed" and "started".
   * To seek through app tracker states in RecoveredUserResources
   * we need to move from one app tracker state to another using key "zzz".
   * zzz comes later in lexicographical order than started.
   * Similarly to move one user to another in RLS,we can use "zzz",
   * as RecoveredUserResources uses two keys appcache and filecache.
   */
  private static final String BEYOND_ENTRIES_SUFFIX = "zzz/";

  private static final String CONTAINER_ASSIGNED_RESOURCES_KEY_SUFFIX =
      "/assignedResources_";

  private static final byte[] EMPTY_VALUE = new byte[0];

  private DB db;
  private boolean isNewlyCreated;
  private boolean isHealthy;
  private Timer compactionTimer;

  /**
   * Map of containerID vs List of unknown key suffixes.
   */
  private ListMultimap<ContainerId, String> containerUnknownKeySuffixes =
      ArrayListMultimap.create();

  public NMLeveldbStateStoreService() {
    super(NMLeveldbStateStoreService.class.getName());
  }

  @Override
  protected void startStorage() throws IOException {
    // Assume that we're healthy when we start
    isHealthy = true;
  }

  @Override
  protected void closeStorage() throws IOException {
    if (compactionTimer != null) {
      compactionTimer.cancel();
      compactionTimer = null;
    }
    if (db != null) {
      db.close();
    }
  }

  @Override
  public boolean isNewlyCreated() {
    return isNewlyCreated;
  }

  /**
   * If the state store throws an error after recovery has been performed
   * then we can not trust it any more to reflect the NM state. We need to
   * mark the store and node unhealthy.
   * Errors during the recovery will cause a service failure and thus a NM
   * start failure. Do not need to mark the store unhealthy for those.
   * @param dbErr Exception
   */
  private void markStoreUnHealthy(DBException dbErr) {
    // Always log the error here, we might not see the error in the caller
    LOG.error("Statestore exception: ", dbErr);
    // We have already been marked unhealthy so no need to do it again.
    if (!isHealthy) {
      return;
    }
    // Mark unhealthy, an out of band heartbeat will be sent and the state
    // will remain unhealthy (not recoverable).
    // No need to close the store: does not make any difference at this point.
    isHealthy = false;
    // We could get here before the nodeStatusUpdater is set
    NodeStatusUpdater nsu = getNodeStatusUpdater();
    if (nsu != null) {
      nsu.reportException(dbErr);
    }
  }

  @VisibleForTesting
  boolean isHealthy() {
    return isHealthy;
  }

  // LeveldbIterator starting at startkey
  private LeveldbIterator getLevelDBIterator(String startKey)
      throws IOException {
    try {
      LeveldbIterator it = new LeveldbIterator(db);
      it.seek(bytes(startKey));
      return it;
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  // Base Recovery Iterator
  private abstract class BaseRecoveryIterator<T> implements
      RecoveryIterator<T> {
    LeveldbIterator it;
    T nextItem;

    BaseRecoveryIterator(String dbKey) throws IOException {
      this.it = getLevelDBIterator(dbKey);
      this.nextItem = null;
    }

    protected abstract T getNextItem(LeveldbIterator it) throws IOException;

    @Override
    public boolean hasNext() throws IOException {
      if (nextItem == null) {
        nextItem = getNextItem(it);
      }
      return (nextItem != null);
    }

    @Override
    public T next() throws IOException, NoSuchElementException {
      T tmp = nextItem;
      if (tmp != null) {
        nextItem = null;
        return tmp;
      } else {
        tmp = getNextItem(it);
        if (tmp == null) {
          throw new NoSuchElementException();
        }
        return tmp;
      }
    }

    @Override
    public void close() throws IOException {
      if (it != null) {
        it.close();
      }
    }
  }

  //  Container Recovery Iterator
  private class ContainerStateIterator extends
      BaseRecoveryIterator<RecoveredContainerState> {
    ContainerStateIterator() throws IOException {
      super(CONTAINERS_KEY_PREFIX);
    }

    @Override
    protected RecoveredContainerState getNextItem(LeveldbIterator it)
        throws IOException {
      return getNextRecoveredContainer(it);
    }
  }

  private RecoveredContainerState getNextRecoveredContainer(LeveldbIterator it)
      throws IOException {
    RecoveredContainerState rcs = null;
    try {
      while (it.hasNext()) {
        Entry<byte[], byte[]> entry = it.peekNext();
        String key = asString(entry.getKey());
        if (!key.startsWith(CONTAINERS_KEY_PREFIX)) {
          return null;
        }

        int idEndPos = key.indexOf('/', CONTAINERS_KEY_PREFIX.length());
        if (idEndPos < 0) {
          throw new IOException("Unable to determine container in key: " + key);
        }
        String keyPrefix = key.substring(0, idEndPos + 1);
        rcs = loadContainerState(it, keyPrefix);
        if (rcs.startRequest != null) {
          break;
        } else {
          removeContainer(rcs.getContainerId());
          rcs = null;
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return rcs;
  }


  @Override
  public RecoveryIterator<RecoveredContainerState> getContainerStateIterator()
      throws IOException {
    return new ContainerStateIterator();
  }

  private RecoveredContainerState loadContainerState(LeveldbIterator iter,
       String keyPrefix) throws IOException {
    ContainerId containerId = ContainerId.fromString(
        keyPrefix.substring(CONTAINERS_KEY_PREFIX.length(),
            keyPrefix.length()-1));
    RecoveredContainerState rcs = new RecoveredContainerState(containerId);
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
        ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils
            .newContainerTokenIdentifier(rcs.startRequest.getContainerToken());
        rcs.capability = new ResourcePBImpl(
            containerTokenIdentifier.getProto().getResource());
      } else if (suffix.equals(CONTAINER_VERSION_KEY_SUFFIX)) {
        rcs.version = Integer.parseInt(asString(entry.getValue()));
      } else if (suffix.equals(CONTAINER_START_TIME_KEY_SUFFIX)) {
        rcs.setStartTime(Long.parseLong(asString(entry.getValue())));
      } else if (suffix.equals(CONTAINER_DIAGS_KEY_SUFFIX)) {
        rcs.diagnostics = asString(entry.getValue());
      } else if (suffix.equals(CONTAINER_QUEUED_KEY_SUFFIX)) {
        if (rcs.status == RecoveredContainerStatus.REQUESTED) {
          rcs.status = RecoveredContainerStatus.QUEUED;
        }
      } else if (suffix.equals(CONTAINER_PAUSED_KEY_SUFFIX)) {
        if ((rcs.status == RecoveredContainerStatus.LAUNCHED)
            ||(rcs.status == RecoveredContainerStatus.QUEUED)
            ||(rcs.status == RecoveredContainerStatus.REQUESTED)) {
          rcs.status = RecoveredContainerStatus.PAUSED;
        }
      } else if (suffix.equals(CONTAINER_LAUNCHED_KEY_SUFFIX)) {
        if ((rcs.status == RecoveredContainerStatus.REQUESTED)
            || (rcs.status == RecoveredContainerStatus.QUEUED)
            ||(rcs.status == RecoveredContainerStatus.PAUSED)) {
          rcs.status = RecoveredContainerStatus.LAUNCHED;
        }
      } else if (suffix.equals(CONTAINER_KILLED_KEY_SUFFIX)) {
        rcs.killed = true;
      } else if (suffix.equals(CONTAINER_EXIT_CODE_KEY_SUFFIX)) {
        rcs.status = RecoveredContainerStatus.COMPLETED;
        rcs.exitCode = Integer.parseInt(asString(entry.getValue()));
      } else if (suffix.equals(CONTAINER_UPDATE_TOKEN_SUFFIX)) {
        ContainerTokenIdentifierProto tokenIdentifierProto =
            ContainerTokenIdentifierProto.parseFrom(entry.getValue());
        Token currentToken = rcs.getStartRequest().getContainerToken();
        Token updatedToken = Token
            .newInstance(tokenIdentifierProto.toByteArray(),
                ContainerTokenIdentifier.KIND.toString(),
                currentToken.getPassword().array(), currentToken.getService());
        rcs.startRequest.setContainerToken(updatedToken);
        rcs.capability = new ResourcePBImpl(tokenIdentifierProto.getResource());
        rcs.version = tokenIdentifierProto.getVersion();
      } else if (suffix.equals(CONTAINER_REMAIN_RETRIES_KEY_SUFFIX)) {
        rcs.setRemainingRetryAttempts(
            Integer.parseInt(asString(entry.getValue())));
      } else if (suffix.equals(CONTAINER_RESTART_TIMES_SUFFIX)) {
        String value = asString(entry.getValue());
        // parse the string format of List<Long>, e.g. [34, 21, 22]
        String[] unparsedRestartTimes =
            value.substring(1, value.length() - 1).split(", ");
        List<Long> restartTimes = new ArrayList<>();
        for (String restartTime : unparsedRestartTimes) {
          if (!restartTime.isEmpty()) {
            restartTimes.add(Long.parseLong(restartTime));
          }
        }
        rcs.setRestartTimes(restartTimes);
      } else if (suffix.equals(CONTAINER_WORK_DIR_KEY_SUFFIX)) {
        rcs.setWorkDir(asString(entry.getValue()));
      } else if (suffix.equals(CONTAINER_LOG_DIR_KEY_SUFFIX)) {
        rcs.setLogDir(asString(entry.getValue()));
      } else if (suffix.startsWith(CONTAINER_ASSIGNED_RESOURCES_KEY_SUFFIX)) {
        String resourceType = suffix.substring(
            CONTAINER_ASSIGNED_RESOURCES_KEY_SUFFIX.length());
        ResourceMappings.AssignedResources assignedResources =
            ResourceMappings.AssignedResources.fromBytes(entry.getValue());
        rcs.getResourceMappings().addAssignedResources(resourceType,
            assignedResources);
      } else {
        LOG.warn("the container " + containerId
            + " will be killed because of the unknown key " + key
            + " during recovery.");
        containerUnknownKeySuffixes.put(containerId, suffix);
        rcs.setRecoveryType(RecoveredContainerType.KILL);
      }
    }
    return rcs;
  }

  @Override
  public void storeContainer(ContainerId containerId, int containerVersion,
      long startTime, StartContainerRequest startRequest) throws IOException {
    String idStr = containerId.toString();
    LOG.debug("storeContainer: containerId= {}, startRequest= {}",
        idStr, startRequest);
    final String keyVersion = getContainerVersionKey(idStr);
    final String keyRequest =
        getContainerKey(idStr, CONTAINER_REQUEST_KEY_SUFFIX);
    final StartContainerRequestProto startContainerRequest =
        ((StartContainerRequestPBImpl) startRequest).getProto();

    final String keyStartTime =
        getContainerKey(idStr, CONTAINER_START_TIME_KEY_SUFFIX);
    final String startTimeValue = Long.toString(startTime);

    try {
      try (WriteBatch batch = db.createWriteBatch()) {
        batch.put(bytes(keyRequest), startContainerRequest.toByteArray());
        batch.put(bytes(keyStartTime), bytes(startTimeValue));
        if (containerVersion != 0) {
          batch.put(bytes(keyVersion),
                  bytes(Integer.toString(containerVersion)));
        }
        db.write(batch);
      }
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  String getContainerVersionKey(String containerId) {
    return getContainerKey(containerId, CONTAINER_VERSION_KEY_SUFFIX);
  }

  private String getContainerKey(String containerId, String suffix) {
    return CONTAINERS_KEY_PREFIX + containerId + suffix;
  }

  @Override
  public void storeContainerQueued(ContainerId containerId) throws IOException {
    LOG.debug("storeContainerQueued: containerId={}", containerId);

    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_QUEUED_KEY_SUFFIX;
    try {
      db.put(bytes(key), EMPTY_VALUE);
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  private void removeContainerQueued(ContainerId containerId)
      throws IOException {
    LOG.debug("removeContainerQueued: containerId={}", containerId);

    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_QUEUED_KEY_SUFFIX;
    try {
      db.delete(bytes(key));
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerPaused(ContainerId containerId) throws IOException {
    LOG.debug("storeContainerPaused: containerId={}", containerId);

    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_PAUSED_KEY_SUFFIX;
    try {
      db.put(bytes(key), EMPTY_VALUE);
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void removeContainerPaused(ContainerId containerId)
      throws IOException {
    LOG.debug("removeContainerPaused: containerId={}", containerId);

    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_PAUSED_KEY_SUFFIX;
    try {
      db.delete(bytes(key));
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerDiagnostics(ContainerId containerId,
      StringBuilder diagnostics) throws IOException {
    LOG.debug("storeContainerDiagnostics: containerId={}, diagnostics=",
        containerId, diagnostics);

    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_DIAGS_KEY_SUFFIX;
    try {
      db.put(bytes(key), bytes(diagnostics.toString()));
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerLaunched(ContainerId containerId)
      throws IOException {
    LOG.debug("storeContainerLaunched: containerId={}", containerId);

    // Removing the container if queued for backward compatibility reasons
    removeContainerQueued(containerId);
    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_LAUNCHED_KEY_SUFFIX;
    try {
      db.put(bytes(key), EMPTY_VALUE);
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerUpdateToken(ContainerId containerId,
      ContainerTokenIdentifier containerTokenIdentifier) throws IOException {
    LOG.debug("storeContainerUpdateToken: containerId={}", containerId);

    String keyUpdateToken = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_UPDATE_TOKEN_SUFFIX;
    String keyVersion = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_VERSION_KEY_SUFFIX;

    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        // New value will overwrite old values for the same key
        batch.put(bytes(keyUpdateToken),
            containerTokenIdentifier.getProto().toByteArray());
        batch.put(bytes(keyVersion),
            bytes(Integer.toString(containerTokenIdentifier.getVersion())));
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerKilled(ContainerId containerId)
      throws IOException {
    LOG.debug("storeContainerKilled: containerId={}", containerId);

    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_KILLED_KEY_SUFFIX;
    try {
      db.put(bytes(key), EMPTY_VALUE);
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerCompleted(ContainerId containerId,
      int exitCode) throws IOException {
    LOG.debug("storeContainerCompleted: containerId={}", containerId);

    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_EXIT_CODE_KEY_SUFFIX;
    try {
      db.put(bytes(key), bytes(Integer.toString(exitCode)));
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerRemainingRetryAttempts(ContainerId containerId,
      int remainingRetryAttempts) throws IOException {
    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_REMAIN_RETRIES_KEY_SUFFIX;
    try {
      db.put(bytes(key), bytes(Integer.toString(remainingRetryAttempts)));
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerRestartTimes(ContainerId containerId,
      List<Long> restartTimes) throws IOException {
    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_RESTART_TIMES_SUFFIX;
    try {
      db.put(bytes(key), bytes(restartTimes.toString()));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerWorkDir(ContainerId containerId,
      String workDir) throws IOException {
    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_WORK_DIR_KEY_SUFFIX;
    try {
      db.put(bytes(key), bytes(workDir));
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void storeContainerLogDir(ContainerId containerId,
      String logDir) throws IOException {
    String key = CONTAINERS_KEY_PREFIX + containerId.toString()
        + CONTAINER_LOG_DIR_KEY_SUFFIX;
    try {
      db.put(bytes(key), bytes(logDir));
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void removeContainer(ContainerId containerId)
      throws IOException {
    LOG.debug("removeContainer: containerId={}", containerId);

    String keyPrefix = CONTAINERS_KEY_PREFIX + containerId.toString();
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        batch.delete(bytes(keyPrefix + CONTAINER_REQUEST_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_DIAGS_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_LAUNCHED_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_QUEUED_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_PAUSED_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_KILLED_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_EXIT_CODE_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_UPDATE_TOKEN_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_START_TIME_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_LOG_DIR_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_VERSION_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_REMAIN_RETRIES_KEY_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_RESTART_TIMES_SUFFIX));
        batch.delete(bytes(keyPrefix + CONTAINER_WORK_DIR_KEY_SUFFIX));
        List<String> unknownKeysForContainer = containerUnknownKeySuffixes
            .removeAll(containerId);
        for (String unknownKeySuffix : unknownKeysForContainer) {
          batch.delete(bytes(keyPrefix + unknownKeySuffix));
        }
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }


  // Application Recovery Iterator
  private class ApplicationStateIterator extends
      BaseRecoveryIterator<ContainerManagerApplicationProto> {
    ApplicationStateIterator() throws IOException {
      super(APPLICATIONS_KEY_PREFIX);
    }

    @Override
    protected ContainerManagerApplicationProto getNextItem(LeveldbIterator it)
        throws IOException {
      return getNextRecoveredApplication(it);
    }
  }

  private ContainerManagerApplicationProto getNextRecoveredApplication(
      LeveldbIterator it) throws IOException {
    ContainerManagerApplicationProto applicationProto = null;
    try {
      if (it.hasNext()) {
        Entry<byte[], byte[]> entry = it.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(APPLICATIONS_KEY_PREFIX)) {
          return null;
        }
        applicationProto = ContainerManagerApplicationProto.parseFrom(
            entry.getValue());
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return applicationProto;
  }

  @Override
  public RecoveredApplicationsState loadApplicationsState()
      throws IOException {
    RecoveredApplicationsState state = new RecoveredApplicationsState();
    state.it = new ApplicationStateIterator();
    cleanupDeprecatedFinishedApps();
    return state;
  }

  @Override
  public void storeApplication(ApplicationId appId,
      ContainerManagerApplicationProto p) throws IOException {
    LOG.debug("storeApplication: appId={}, proto={}", appId, p);

    String key = APPLICATIONS_KEY_PREFIX + appId;
    try {
      db.put(bytes(key), p.toByteArray());
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void removeApplication(ApplicationId appId)
      throws IOException {
    LOG.debug("removeApplication: appId={}", appId);

    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        String key = APPLICATIONS_KEY_PREFIX + appId;
        batch.delete(bytes(key));
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }


  // User Resource Recovery Iterator.
  private class UserResourcesIterator extends
      BaseRecoveryIterator<Entry<String, RecoveredUserResources>> {
    UserResourcesIterator() throws IOException {
      super(LOCALIZATION_PRIVATE_KEY_PREFIX);
    }

    @Override
    protected Entry<String, RecoveredUserResources> getNextItem(
        LeveldbIterator it) throws IOException {
      return getNextRecoveredPrivateLocalizationEntry(it);
    }
  }

  private Entry<String, RecoveredUserResources> getNextRecoveredPrivateLocalizationEntry(
      LeveldbIterator it) throws IOException {
    Entry<String, RecoveredUserResources> localEntry = null;
    try {
      if (it.hasNext()) {
        Entry<byte[], byte[]> entry = it.peekNext();
        String key = asString(entry.getKey());
        if (!key.startsWith(LOCALIZATION_PRIVATE_KEY_PREFIX)) {
          return null;
        }

        int userEndPos = key.indexOf('/',
            LOCALIZATION_PRIVATE_KEY_PREFIX.length());
        if (userEndPos < 0) {
          throw new IOException("Unable to determine user in resource key: "
              + key);
        }
        String user = key.substring(
            LOCALIZATION_PRIVATE_KEY_PREFIX.length(), userEndPos);
        RecoveredUserResources val = loadUserLocalizedResources(it,
            key.substring(0, userEndPos+1));
        localEntry = new AbstractMap.SimpleEntry<>(user, val);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return localEntry;
  }

  @Override
  public RecoveredLocalizationState loadLocalizationState()
      throws IOException {
    RecoveredLocalizationState state = new RecoveredLocalizationState();
    state.publicTrackerState = loadResourceTrackerState(
        LOCALIZATION_PUBLIC_KEY_PREFIX);
    state.it = new UserResourcesIterator();
    return state;
  }

  private LocalResourceTrackerState loadResourceTrackerState(String keyPrefix)
      throws IOException {
    final String completedPrefix = keyPrefix + LOCALIZATION_COMPLETED_SUFFIX;
    final String startedPrefix = keyPrefix + LOCALIZATION_STARTED_SUFFIX;

    RecoveryIterator<LocalizedResourceProto> crIt =
        new CompletedResourcesIterator(completedPrefix);
    RecoveryIterator<Entry<LocalResourceProto, Path>> srIt =
        new StartedResourcesIterator(startedPrefix);

    return new LocalResourceTrackerState(crIt, srIt);
  }

  private class CompletedResourcesIterator extends
      BaseRecoveryIterator<LocalizedResourceProto> {
    private String startKey;
    CompletedResourcesIterator(String startKey) throws IOException {
      super(startKey);
      this.startKey = startKey;
    }

    @Override
    protected LocalizedResourceProto getNextItem(LeveldbIterator it)
        throws IOException {
      return getNextCompletedResource(it, startKey);
    }
  }

  private LocalizedResourceProto getNextCompletedResource(
      LeveldbIterator iter, String keyPrefix) throws IOException {
    LocalizedResourceProto nextCompletedResource = null;
    if (iter.hasNext()){
      Entry<byte[], byte[]> entry = iter.next();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        return null;
      }

      LOG.debug("Loading completed resource from {}", key);
      nextCompletedResource = LocalizedResourceProto.parseFrom(
          entry.getValue());
    }
    return nextCompletedResource;
  }

  private class StartedResourcesIterator extends
      BaseRecoveryIterator<Entry<LocalResourceProto, Path>> {
    private String startKey;
    StartedResourcesIterator(String startKey) throws IOException {
      super(startKey);
      this.startKey = startKey;
    }

    @Override
    protected Entry<LocalResourceProto, Path> getNextItem(LeveldbIterator it)
        throws IOException {
      return getNextStartedResource(it, startKey);
    }
  }

  private Entry<LocalResourceProto, Path> getNextStartedResource(
      LeveldbIterator iter, String keyPrefix) throws IOException {
    Entry<LocalResourceProto, Path> nextStartedResource = null;
    if (iter.hasNext()){
      Entry<byte[], byte[]> entry = iter.next();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        return null;
      }

      Path localPath = new Path(key.substring(keyPrefix.length()));
      LOG.debug("Loading in-progress resource at {}", localPath);
      nextStartedResource = new SimpleEntry<LocalResourceProto, Path>(
          LocalResourceProto.parseFrom(entry.getValue()), localPath);
    }
    return nextStartedResource;
  }

  private void seekPastPrefix(LeveldbIterator iter, String keyPrefix)
      throws IOException {
    try{
      iter.seek(bytes(keyPrefix + BEYOND_ENTRIES_SUFFIX));
      while (iter.hasNext()) {
        Entry<byte[], byte[]> entry = iter.peekNext();
        String key = asString(entry.getKey());
        if (key.startsWith(keyPrefix)) {
          iter.next();
        } else {
          break;
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  private RecoveredUserResources loadUserLocalizedResources(
      LeveldbIterator iter, String keyPrefix) throws IOException {
    RecoveredUserResources userResources = new RecoveredUserResources();

    // seek through App cache
    String appCachePrefix = keyPrefix + LOCALIZATION_APPCACHE_SUFFIX;
    iter.seek(bytes(appCachePrefix));
    while (iter.hasNext()) {
      Entry<byte[], byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());

      if (!key.startsWith(appCachePrefix)) {
        break;
      }

      int appIdStartPos = appCachePrefix.length();
      int appIdEndPos = key.indexOf('/', appIdStartPos);
      if (appIdEndPos < 0) {
        throw new IOException("Unable to determine appID in resource key: "
            + key);
      }
      ApplicationId appId = ApplicationId.fromString(
          key.substring(appIdStartPos, appIdEndPos));
      String trackerStateKey = key.substring(0, appIdEndPos+1);
      userResources.appTrackerStates.put(appId,
          loadResourceTrackerState(trackerStateKey));
      // Seek to next application
      seekPastPrefix(iter, trackerStateKey);
    }

    // File Cache
    String fileCachePrefix = keyPrefix + LOCALIZATION_FILECACHE_SUFFIX;
    iter.seek(bytes(fileCachePrefix));
    Entry<byte[], byte[]> entry = iter.peekNext();
    String key = asString(entry.getKey());
    if (key.startsWith(fileCachePrefix)) {
      userResources.privateTrackerState =
          loadResourceTrackerState(fileCachePrefix);
    }

    // seek to Next User.
    seekPastPrefix(iter, keyPrefix);
    return userResources;
  }

  @Override
  public void startResourceLocalization(String user, ApplicationId appId,
      LocalResourceProto proto, Path localPath) throws IOException {
    String key = getResourceStartedKey(user, appId, localPath.toString());
    try {
      db.put(bytes(key), proto.toByteArray());
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void finishResourceLocalization(String user, ApplicationId appId,
      LocalizedResourceProto proto) throws IOException {
    String localPath = proto.getLocalPath();
    String startedKey = getResourceStartedKey(user, appId, localPath);
    String completedKey = getResourceCompletedKey(user, appId, localPath);
    LOG.debug("Storing localized resource to {}", completedKey);
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
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void removeLocalizedResource(String user, ApplicationId appId,
      Path localPath) throws IOException {
    String localPathStr = localPath.toString();
    String startedKey = getResourceStartedKey(user, appId, localPathStr);
    String completedKey = getResourceCompletedKey(user, appId, localPathStr);
    LOG.debug("Removing local resource at {}", localPathStr);
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
      markStoreUnHealthy(e);
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

  // Deletion State Recovery Iterator.
  private class DeletionStateIterator extends
      BaseRecoveryIterator<DeletionServiceDeleteTaskProto> {
    DeletionStateIterator() throws IOException {
      super(DELETION_TASK_KEY_PREFIX);
    }

    @Override
    protected DeletionServiceDeleteTaskProto getNextItem(LeveldbIterator it)
        throws IOException {
      return getNextRecoveredDeletionService(it);
    }
  }

  private DeletionServiceDeleteTaskProto getNextRecoveredDeletionService(
      LeveldbIterator it) throws IOException {
    DeletionServiceDeleteTaskProto deleteProto = null;
    try {
      if (it.hasNext()) {
        Entry<byte[], byte[]> entry = it.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(DELETION_TASK_KEY_PREFIX)) {
          return null;
        }
        deleteProto = DeletionServiceDeleteTaskProto.parseFrom(
            entry.getValue());
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return deleteProto;
  }

  @Override
  public RecoveredDeletionServiceState loadDeletionServiceState()
      throws IOException {
    RecoveredDeletionServiceState state = new RecoveredDeletionServiceState();
    state.it = new DeletionStateIterator();
    return state;
  }

  @Override
  public void storeDeletionTask(int taskId,
      DeletionServiceDeleteTaskProto taskProto) throws IOException {
    String key = DELETION_TASK_KEY_PREFIX + taskId;
    try {
      db.put(bytes(key), taskProto.toByteArray());
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void removeDeletionTask(int taskId) throws IOException {
    String key = DELETION_TASK_KEY_PREFIX + taskId;
    try {
      db.delete(bytes(key));
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  private MasterKey getMasterKey(String dbKey) throws IOException {
    try{
      byte[] data = db.get(bytes(dbKey));
      if (data == null || data.length == 0) {
        return null;
      }
      return parseMasterKey(data);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  // Recover NMTokens Iterator
  private class NMTokensStateIterator extends
      BaseRecoveryIterator<Entry<ApplicationAttemptId, MasterKey>> {
    NMTokensStateIterator() throws IOException {
      super(NM_TOKENS_KEY_PREFIX);
    }

    @Override
    protected Entry<ApplicationAttemptId, MasterKey> getNextItem(
        LeveldbIterator it) throws IOException {
      return getNextMasterKeyEntry(it);
    }
  }

  private Entry<ApplicationAttemptId, MasterKey> getNextMasterKeyEntry(
      LeveldbIterator it) throws IOException {
    Entry<ApplicationAttemptId, MasterKey> masterKeyentry = null;
    try {
      while (it.hasNext()) {
        Entry<byte[], byte[]> entry = it.next();
        String fullKey = asString(entry.getKey());
        if (!fullKey.startsWith(NM_TOKENS_KEY_PREFIX)) {
          break;
        }
        String key = fullKey.substring(NM_TOKENS_KEY_PREFIX.length());
        if (key.startsWith(ApplicationAttemptId.appAttemptIdStrPrefix)) {
          ApplicationAttemptId attempt;
          try {
            attempt = ApplicationAttemptId.fromString(key);
          } catch (IllegalArgumentException e) {
            throw new IOException("Bad application master key state for "
                + fullKey, e);
          }
          masterKeyentry = new AbstractMap.SimpleEntry<>(attempt,
              parseMasterKey(entry.getValue()));
          break;
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return masterKeyentry;
  }

  @Override
  public RecoveredNMTokensState loadNMTokensState() throws IOException {
    RecoveredNMTokensState state = new RecoveredNMTokensState();
    state.currentMasterKey = getMasterKey(NM_TOKENS_KEY_PREFIX
                                          + CURRENT_MASTER_KEY_SUFFIX);
    state.previousMasterKey = getMasterKey(NM_TOKENS_KEY_PREFIX
                                            + PREV_MASTER_KEY_SUFFIX);
    state.it = new NMTokensStateIterator();
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
      markStoreUnHealthy(e);
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
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  // Recover ContainersToken Iterator.
  private class ContainerTokensStateIterator extends
      BaseRecoveryIterator<Entry<ContainerId, Long>> {
    ContainerTokensStateIterator() throws IOException {
      super(CONTAINER_TOKENS_KEY_PREFIX);
    }

    @Override
    protected Entry<ContainerId, Long> getNextItem(LeveldbIterator it)
        throws IOException {
      return getNextContainerToken(it);
    }
  }

  private Entry<ContainerId, Long> getNextContainerToken(LeveldbIterator it)
      throws IOException {
    Entry<ContainerId, Long> containerTokenEntry = null;
    try {
      while (it.hasNext()) {
        Entry<byte[], byte[]> entry = it.next();
        String fullKey = asString(entry.getKey());
        if (!fullKey.startsWith(CONTAINER_TOKENS_KEY_PREFIX)) {
          break;
        }
        String key = fullKey.substring(CONTAINER_TOKENS_KEY_PREFIX.length());
        if (key.startsWith(ConverterUtils.CONTAINER_PREFIX)) {
          containerTokenEntry = loadContainerToken(fullKey, key,
              entry.getValue());
          break;
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return containerTokenEntry;
  }

  private static Entry<ContainerId, Long> loadContainerToken(String key,
      String containerIdStr, byte[] value) throws IOException {
    ContainerId containerId;
    Long expTime;
    try {
      containerId = ContainerId.fromString(containerIdStr);
      expTime = Long.parseLong(asString(value));
    } catch (IllegalArgumentException e) {
      throw new IOException("Bad container token state for " + key, e);
    }
    return new AbstractMap.SimpleEntry<>(containerId, expTime);
  }

  @Override
  public RecoveredContainerTokensState loadContainerTokensState()
      throws IOException {
    RecoveredContainerTokensState state = new RecoveredContainerTokensState();
    state.currentMasterKey = getMasterKey(CONTAINER_TOKENS_KEY_PREFIX
        + CURRENT_MASTER_KEY_SUFFIX);
    state.previousMasterKey = getMasterKey(CONTAINER_TOKENS_KEY_PREFIX
        + PREV_MASTER_KEY_SUFFIX);
    state.it = new ContainerTokensStateIterator();
    return state;
  }

  @Override
  public void storeContainerTokenCurrentMasterKey(MasterKey key)
      throws IOException {
    storeMasterKey(CONTAINER_TOKEN_SECRETMANAGER_CURRENT_MASTER_KEY, key);
  }

  @Override
  public void storeContainerTokenPreviousMasterKey(MasterKey key)
      throws IOException {
    storeMasterKey(CONTAINER_TOKEN_SECRETMANAGER_PREV_MASTER_KEY, key);
  }

  @Override
  public void storeContainerToken(ContainerId containerId, Long expTime)
      throws IOException {
    String key = CONTAINER_TOKENS_KEY_PREFIX + containerId;
    try {
      db.put(bytes(key), bytes(expTime.toString()));
    } catch (DBException e) {
      markStoreUnHealthy(e);
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
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }


  @Override
  public RecoveredLogDeleterState loadLogDeleterState() throws IOException {
    RecoveredLogDeleterState state = new RecoveredLogDeleterState();
    state.logDeleterMap = new HashMap<ApplicationId, LogDeleterProto>();
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(LOG_DELETER_KEY_PREFIX));
      final int logDeleterKeyPrefixLength = LOG_DELETER_KEY_PREFIX.length();
      while (iter.hasNext()) {
        Entry<byte[], byte[]> entry = iter.next();
        String fullKey = asString(entry.getKey());
        if (!fullKey.startsWith(LOG_DELETER_KEY_PREFIX)) {
          break;
        }

        String appIdStr = fullKey.substring(logDeleterKeyPrefixLength);
        ApplicationId appId = null;
        try {
          appId = ApplicationId.fromString(appIdStr);
        } catch (IllegalArgumentException e) {
          LOG.warn("Skipping unknown log deleter key " + fullKey);
          continue;
        }

        LogDeleterProto proto = LogDeleterProto.parseFrom(entry.getValue());
        state.logDeleterMap.put(appId, proto);
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
  public void storeLogDeleter(ApplicationId appId, LogDeleterProto proto)
      throws IOException {
    String key = getLogDeleterKey(appId);
    try {
      db.put(bytes(key), proto.toByteArray());
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void removeLogDeleter(ApplicationId appId) throws IOException {
    String key = getLogDeleterKey(appId);
    try {
      db.delete(bytes(key));
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void storeAssignedResources(Container container,
      String resourceType, List<Serializable> assignedResources)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "storeAssignedResources: containerId=" + container.getContainerId()
              + ", assignedResources=" + StringUtils
              .join(",", assignedResources));
    }

    String keyResChng = CONTAINERS_KEY_PREFIX + container.getContainerId().toString()
        + CONTAINER_ASSIGNED_RESOURCES_KEY_SUFFIX + resourceType;
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        ResourceMappings.AssignedResources res =
            new ResourceMappings.AssignedResources();
        res.updateAssignedResources(assignedResources);

        // New value will overwrite old values for the same key
        batch.put(bytes(keyResChng), res.toBytes());
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }

    // update container resource mapping.
    updateContainerResourceMapping(container, resourceType, assignedResources);
  }

  @SuppressWarnings("deprecation")
  private void cleanupDeprecatedFinishedApps() {
    try {
      cleanupKeysWithPrefix(FINISHED_APPS_KEY_PREFIX);
    } catch (Exception e) {
      LOG.warn("cleanup keys with prefix " + FINISHED_APPS_KEY_PREFIX +
              " from leveldb failed", e);
    }
  }

  private void cleanupKeysWithPrefix(String prefix) throws IOException {
    WriteBatch batch = null;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      try {
        batch = db.createWriteBatch();
        iter.seek(bytes(prefix));
        while (iter.hasNext()) {
          byte[] key = iter.next().getKey();
          String keyStr = asString(key);
          if (!keyStr.startsWith(prefix)) {
            break;
          }
          batch.delete(key);
          LOG.debug("cleanup {} from leveldb", keyStr);
        }
        db.write(batch);
      } catch (DBException e) {
        throw new IOException(e);
      } finally {
        if (batch != null) {
          batch.close();
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
  }

  private String getLogDeleterKey(ApplicationId appId) {
    return LOG_DELETER_KEY_PREFIX + appId;
  }

  @Override
  public RecoveredAMRMProxyState loadAMRMProxyState() throws IOException {
    RecoveredAMRMProxyState result = new RecoveredAMRMProxyState();
    Set<String> unknownKeys = new HashSet<>();
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(AMRMPROXY_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[], byte[]> entry = iter.peekNext();
        String key = asString(entry.getKey());
        if (!key.startsWith(AMRMPROXY_KEY_PREFIX)) {
          break;
        }

        String suffix = key.substring(AMRMPROXY_KEY_PREFIX.length());
        if (suffix.equals(CURRENT_MASTER_KEY_SUFFIX)) {
          iter.next();
          result.setCurrentMasterKey(parseMasterKey(entry.getValue()));
          LOG.info("Recovered for AMRMProxy: current master key id "
              + result.getCurrentMasterKey().getKeyId());

        } else if (suffix.equals(NEXT_MASTER_KEY_SUFFIX)) {
          iter.next();
          result.setNextMasterKey(parseMasterKey(entry.getValue()));
          LOG.info("Recovered for AMRMProxy: next master key id "
              + result.getNextMasterKey().getKeyId());

        } else { // Load AMRMProxy application context map for an app attempt
          // Parse appAttemptId, also handle the unknown keys
          int idEndPos;
          ApplicationAttemptId attemptId;
          try {
            idEndPos = key.indexOf('/', AMRMPROXY_KEY_PREFIX.length());
            if (idEndPos < 0) {
              throw new IOException(
                  "Unable to determine attemptId in key: " + key);
            }
            attemptId = ApplicationAttemptId.fromString(
                key.substring(AMRMPROXY_KEY_PREFIX.length(), idEndPos));
          } catch (Exception e) {
            // Try to move on for back-forward compatibility
            LOG.warn("Unknown key " + key + ", remove and move on", e);
            // Do this because iter.remove() is not supported here
            unknownKeys.add(key);
            continue;
          }
          // Parse the context map for the appAttemptId
          Map<String, byte[]> appContext =
              loadAMRMProxyAppContextMap(iter, key.substring(0, idEndPos + 1));
          result.getAppContexts().put(attemptId, appContext);

          LOG.info("Recovered for AMRMProxy: " + attemptId + ", map size "
              + appContext.size());
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }

    // Delete all unknown keys
    try {
      for (String key : unknownKeys) {
        db.delete(bytes(key));
      }
    } catch (DBException e) {
      throw new IOException(e);
    }

    return result;
  }

  private Map<String, byte[]> loadAMRMProxyAppContextMap(LeveldbIterator iter,
      String keyPrefix) throws IOException {
    Map<String, byte[]> appContextMap = new HashMap<>();
    while (iter.hasNext()) {
      Entry<byte[], byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        break;
      }
      iter.next();
      String suffix = key.substring(keyPrefix.length());
      byte[] data = entry.getValue();
      appContextMap.put(suffix, Arrays.copyOf(data, data.length));
    }
    return appContextMap;
  }

  @Override
  public void storeAMRMProxyCurrentMasterKey(MasterKey key) throws IOException {
    storeMasterKey(AMRMPROXY_KEY_PREFIX + CURRENT_MASTER_KEY_SUFFIX, key);
  }

  @Override
  public void storeAMRMProxyNextMasterKey(MasterKey key) throws IOException {
    String dbkey = AMRMPROXY_KEY_PREFIX + NEXT_MASTER_KEY_SUFFIX;
    if (key == null) {
      // When key is null, delete the entry instead
      try {
        db.delete(bytes(dbkey));
      } catch (DBException e) {
        markStoreUnHealthy(e);
        throw new IOException(e);
      }
      return;
    }
    storeMasterKey(dbkey, key);
  }

  @Override
  public void storeAMRMProxyAppContextEntry(ApplicationAttemptId attempt,
      String key, byte[] data) throws IOException {
    String fullkey = AMRMPROXY_KEY_PREFIX + attempt + "/" + key;
    try {
      db.put(bytes(fullkey), data);
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void removeAMRMProxyAppContextEntry(ApplicationAttemptId attempt,
      String key) throws IOException {
    String fullkey = AMRMPROXY_KEY_PREFIX + attempt + "/" + key;
    try {
      db.delete(bytes(fullkey));
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  public void removeAMRMProxyAppContext(ApplicationAttemptId attempt)
      throws IOException {
    Set<String> candidates = new HashSet<>();
    String keyPrefix = AMRMPROXY_KEY_PREFIX + attempt + "/";
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
        // Do this because iter.remove() is not supported here
        candidates.add(key);
      }
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }

    // Delete all candidate keys
    try {
      for (String key : candidates) {
        db.delete(bytes(key));
      }
    } catch (DBException e) {
      markStoreUnHealthy(e);
      throw new IOException(e);
    }
  }

  @Override
  protected void initStorage(Configuration conf)
      throws IOException {
    db = openDatabase(conf);
    checkVersion();
    startCompactionTimer(conf);
  }

  protected DB openDatabase(Configuration conf) throws IOException {
    Path storeRoot = createStorageDir(conf);
    Options options = new Options();
    options.createIfMissing(false);
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
    return db;
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

  private void startCompactionTimer(Configuration conf) {
    long intervalMsec = conf.getLong(
        YarnConfiguration.NM_RECOVERY_COMPACTION_INTERVAL_SECS,
        YarnConfiguration.DEFAULT_NM_RECOVERY_COMPACTION_INTERVAL_SECS) * 1000;
    if (intervalMsec > 0) {
      compactionTimer = new Timer(
          this.getClass().getSimpleName() + " compaction timer", true);
      compactionTimer.schedule(new CompactionTimerTask(),
          intervalMsec, intervalMsec);
    }
  }


  private class CompactionTimerTask extends TimerTask {
    @Override
    public void run() {
      long start = Time.monotonicNow();
      LOG.info("Starting full compaction cycle");
      try {
        db.compactRange(null, null);
      } catch (DBException e) {
        LOG.error("Error compacting database", e);
      }
      long duration = Time.monotonicNow() - start;
      LOG.info("Full compaction cycle completed in " + duration + " msec");
    }
  }

  Version loadVersion() throws IOException {
    byte[] data = db.get(bytes(DB_SCHEMA_VERSION_KEY));
    // if version is not stored previously, treat it as CURRENT_VERSION_INFO.
    if (data == null || data.length == 0) {
      return getCurrentVersion();
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
  
  @VisibleForTesting
  DB getDB() {
    return db;
  }

  @VisibleForTesting
  void setDB(DB testDb) {
    this.db = testDb;
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
  protected void checkVersion() throws IOException {
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
