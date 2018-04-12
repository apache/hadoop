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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.Epoch;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMDelegationTokenIdentifierData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.AMRMTokenSecretManagerStatePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import com.google.common.annotations.VisibleForTesting;

/**
 * Changes from 1.0 to 1.1, Addition of ReservationSystem state.
 */
public class LeveldbRMStateStore extends RMStateStore {

  public static final Log LOG =
      LogFactory.getLog(LeveldbRMStateStore.class);

  private static final String SEPARATOR = "/";
  private static final String DB_NAME = "yarn-rm-state";
  private static final String RM_DT_MASTER_KEY_KEY_PREFIX =
      RM_DT_SECRET_MANAGER_ROOT + SEPARATOR + DELEGATION_KEY_PREFIX;
  private static final String RM_DT_TOKEN_KEY_PREFIX =
      RM_DT_SECRET_MANAGER_ROOT + SEPARATOR + DELEGATION_TOKEN_PREFIX;
  private static final String RM_DT_SEQUENCE_NUMBER_KEY =
      RM_DT_SECRET_MANAGER_ROOT + SEPARATOR + "RMDTSequentialNumber";
  private static final String RM_APP_KEY_PREFIX =
      RM_APP_ROOT + SEPARATOR + ApplicationId.appIdStrPrefix;
  private static final String RM_RESERVATION_KEY_PREFIX =
      RESERVATION_SYSTEM_ROOT + SEPARATOR;

  private static final Version CURRENT_VERSION_INFO = Version
      .newInstance(1, 1);

  private DB db;
  private Timer compactionTimer;
  private long compactionIntervalMsec;

  private String getApplicationNodeKey(ApplicationId appId) {
    return RM_APP_ROOT + SEPARATOR + appId;
  }

  private String getApplicationAttemptNodeKey(ApplicationAttemptId attemptId) {
    return getApplicationAttemptNodeKey(
        getApplicationNodeKey(attemptId.getApplicationId()), attemptId);
  }

  private String getApplicationAttemptNodeKey(String appNodeKey,
      ApplicationAttemptId attemptId) {
    return appNodeKey + SEPARATOR + attemptId;
  }

  private String getRMDTMasterKeyNodeKey(DelegationKey masterKey) {
    return RM_DT_MASTER_KEY_KEY_PREFIX + masterKey.getKeyId();
  }

  private String getRMDTTokenNodeKey(RMDelegationTokenIdentifier tokenId) {
    return RM_DT_TOKEN_KEY_PREFIX + tokenId.getSequenceNumber();
  }

  private String getReservationNodeKey(String planName,
      String reservationId) {
    return RESERVATION_SYSTEM_ROOT + SEPARATOR + planName + SEPARATOR
        + reservationId;
  }

  @Override
  protected void initInternal(Configuration conf) throws Exception {
    compactionIntervalMsec = conf.getLong(
        YarnConfiguration.RM_LEVELDB_COMPACTION_INTERVAL_SECS,
        YarnConfiguration.DEFAULT_RM_LEVELDB_COMPACTION_INTERVAL_SECS) * 1000;
  }

  private Path getStorageDir() throws IOException {
    Configuration conf = getConfig();
    String storePath = conf.get(YarnConfiguration.RM_LEVELDB_STORE_PATH);
    if (storePath == null) {
      throw new IOException("No store location directory configured in " +
          YarnConfiguration.RM_LEVELDB_STORE_PATH);
    }
    return new Path(storePath, DB_NAME);
  }

  private Path createStorageDir() throws IOException {
    Path root = getStorageDir();
    FileSystem fs = FileSystem.getLocal(getConfig());
    fs.mkdirs(root, new FsPermission((short)0700));
    return root;
  }

  @Override
  protected void startInternal() throws Exception {
    db = openDatabase();
    startCompactionTimer();
  }

  protected DB openDatabase() throws Exception {
    Path storeRoot = createStorageDir();
    Options options = new Options();
    options.createIfMissing(false);
    LOG.info("Using state database at " + storeRoot + " for recovery");
    File dbfile = new File(storeRoot.toString());
    try {
      db = JniDBFactory.factory.open(dbfile, options);
    } catch (NativeDB.DBException e) {
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        LOG.info("Creating state database at " + dbfile);
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

  private void startCompactionTimer() {
    if (compactionIntervalMsec > 0) {
      compactionTimer = new Timer(
          this.getClass().getSimpleName() + " compaction timer", true);
      compactionTimer.schedule(new CompactionTimerTask(),
          compactionIntervalMsec, compactionIntervalMsec);
    }
  }

  @Override
  protected void closeInternal() throws Exception {
    if (compactionTimer != null) {
      compactionTimer.cancel();
      compactionTimer = null;
    }
    if (db != null) {
      db.close();
      db = null;
    }
  }

  @VisibleForTesting
  boolean isClosed() {
    return db == null;
  }

  @VisibleForTesting
  DB getDatabase() {
    return db;
  }

  @Override
  protected Version loadVersion() throws Exception {
    Version version = null;
    try {
      byte[] data = db.get(bytes(VERSION_NODE));
      if (data != null) {
        version = new VersionPBImpl(VersionProto.parseFrom(data));
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return version;
  }

  @Override
  protected void storeVersion() throws Exception {
    dbStoreVersion(CURRENT_VERSION_INFO);
  }

  void dbStoreVersion(Version state) throws IOException {
    String key = VERSION_NODE;
    byte[] data = ((VersionPBImpl) state).getProto().toByteArray();
    try {
      db.put(bytes(key), data);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  public synchronized long getAndIncrementEpoch() throws Exception {
    long currentEpoch = baseEpoch;
    byte[] dbKeyBytes = bytes(EPOCH_NODE);
    try {
      byte[] data = db.get(dbKeyBytes);
      if (data != null) {
        currentEpoch = EpochProto.parseFrom(data).getEpoch();
      }
      EpochProto proto = Epoch.newInstance(currentEpoch + 1).getProto();
      db.put(dbKeyBytes, proto.toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
    return currentEpoch;
  }

  @Override
  public RMState loadState() throws Exception {
    RMState rmState = new RMState();
     loadRMDTSecretManagerState(rmState);
     loadRMApps(rmState);
     loadAMRMTokenSecretManagerState(rmState);
    loadReservationState(rmState);
    return rmState;
   }

  private void loadReservationState(RMState rmState) throws IOException {
    int numReservations = 0;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(RM_RESERVATION_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(RM_RESERVATION_KEY_PREFIX)) {
          break;
        }

        String planReservationString =
            key.substring(RM_RESERVATION_KEY_PREFIX.length());
        String[] parts = planReservationString.split(SEPARATOR);
        if (parts.length != 2) {
          LOG.warn("Incorrect reservation state key " + key);
          continue;
        }
        String planName = parts[0];
        String reservationName = parts[1];
        ReservationAllocationStateProto allocationState =
            ReservationAllocationStateProto.parseFrom(entry.getValue());
        if (!rmState.getReservationState().containsKey(planName)) {
          rmState.getReservationState().put(planName,
              new HashMap<ReservationId, ReservationAllocationStateProto>());
        }
        ReservationId reservationId =
            ReservationId.parseReservationId(reservationName);
        rmState.getReservationState().get(planName).put(reservationId,
            allocationState);
        numReservations++;
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    LOG.info("Recovered " + numReservations + " reservations");
  }

  private void loadRMDTSecretManagerState(RMState state) throws IOException {
    int numKeys = loadRMDTSecretManagerKeys(state);
    LOG.info("Recovered " + numKeys + " RM delegation token master keys");
    int numTokens = loadRMDTSecretManagerTokens(state);
    LOG.info("Recovered " + numTokens + " RM delegation tokens");
    loadRMDTSecretManagerTokenSequenceNumber(state);
  }

  private int loadRMDTSecretManagerKeys(RMState state) throws IOException {
    int numKeys = 0;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(RM_DT_MASTER_KEY_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(RM_DT_MASTER_KEY_KEY_PREFIX)) {
          break;
        }
        DelegationKey masterKey = loadDelegationKey(entry.getValue());
        state.rmSecretManagerState.masterKeyState.add(masterKey);
        ++numKeys;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loaded RM delegation key from " + key
              + ": keyId=" + masterKey.getKeyId()
              + ", expirationDate=" + masterKey.getExpiryDate());
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    return numKeys;
  }

  private DelegationKey loadDelegationKey(byte[] data) throws IOException {
    DelegationKey key = new DelegationKey();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    try {
      key.readFields(in);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
    return key;
  }

  private int loadRMDTSecretManagerTokens(RMState state) throws IOException {
    int numTokens = 0;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(RM_DT_TOKEN_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(RM_DT_TOKEN_KEY_PREFIX)) {
          break;
        }
        RMDelegationTokenIdentifierData tokenData = loadDelegationToken(
            entry.getValue());
        RMDelegationTokenIdentifier tokenId = tokenData.getTokenIdentifier();
        long renewDate = tokenData.getRenewDate();
        state.rmSecretManagerState.delegationTokenState.put(tokenId,
            renewDate);
        ++numTokens;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loaded RM delegation token from " + key
              + ": tokenId=" + tokenId + ", renewDate=" + renewDate);
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    return numTokens;
  }

  private RMDelegationTokenIdentifierData loadDelegationToken(byte[] data)
      throws IOException {
    RMDelegationTokenIdentifierData tokenData = null;
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    try {
      tokenData = RMStateStoreUtils.readRMDelegationTokenIdentifierData(in);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
    return tokenData;
  }

  private void loadRMDTSecretManagerTokenSequenceNumber(RMState state)
      throws IOException {
    byte[] data = null;
    try {
      data = db.get(bytes(RM_DT_SEQUENCE_NUMBER_KEY));
    } catch (DBException e) {
      throw new IOException(e);
    }
    if (data != null) {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
      try {
        state.rmSecretManagerState.dtSequenceNumber = in.readInt();
      } finally {
        IOUtils.cleanup(LOG, in);
      }
    }
  }

  private void loadRMApps(RMState state) throws IOException {
    int numApps = 0;
    int numAppAttempts = 0;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(RM_APP_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(RM_APP_KEY_PREFIX)) {
          break;
        }

        String appIdStr = key.substring(RM_APP_ROOT.length() + 1);
        if (appIdStr.contains(SEPARATOR)) {
          LOG.warn("Skipping extraneous data " + key);
          continue;
        }

        numAppAttempts += loadRMApp(state, iter, appIdStr, entry.getValue());
        ++numApps;
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    LOG.info("Recovered " + numApps + " applications and " + numAppAttempts
        + " application attempts");
  }

  private int loadRMApp(RMState rmState, LeveldbIterator iter, String appIdStr,
      byte[] appData) throws IOException {
    ApplicationStateData appState = createApplicationState(appIdStr, appData);
    ApplicationId appId =
        appState.getApplicationSubmissionContext().getApplicationId();
    rmState.appState.put(appId, appState);
    String attemptNodePrefix = getApplicationNodeKey(appId) + SEPARATOR;
    while (iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(attemptNodePrefix)) {
        break;
      }

      String attemptId = key.substring(attemptNodePrefix.length());
      if (attemptId.startsWith(ApplicationAttemptId.appAttemptIdStrPrefix)) {
        ApplicationAttemptStateData attemptState =
            createAttemptState(attemptId, entry.getValue());
        appState.attempts.put(attemptState.getAttemptId(), attemptState);
      } else {
        LOG.warn("Ignoring unknown application key: " + key);
      }
      iter.next();
    }
    int numAttempts = appState.attempts.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Loaded application " + appId + " with " + numAttempts
          + " attempts");
    }
    return numAttempts;
  }

  private ApplicationStateData createApplicationState(String appIdStr,
      byte[] data) throws IOException {
    ApplicationId appId = ApplicationId.fromString(appIdStr);
    ApplicationStateDataPBImpl appState =
        new ApplicationStateDataPBImpl(
            ApplicationStateDataProto.parseFrom(data));
    if (!appId.equals(
        appState.getApplicationSubmissionContext().getApplicationId())) {
      throw new YarnRuntimeException("The database entry for " + appId
          + " contains data for "
          + appState.getApplicationSubmissionContext().getApplicationId());
    }
    return appState;
  }

  @VisibleForTesting
  ApplicationStateData loadRMAppState(ApplicationId appId) throws IOException {
    String appKey = getApplicationNodeKey(appId);
    byte[] data = null;
    try {
      data = db.get(bytes(appKey));
    } catch (DBException e) {
      throw new IOException(e);
    }
    if (data == null) {
      return null;
    }
    return createApplicationState(appId.toString(), data);
  }

  @VisibleForTesting
  ApplicationAttemptStateData loadRMAppAttemptState(
      ApplicationAttemptId attemptId) throws IOException {
    String attemptKey = getApplicationAttemptNodeKey(attemptId);
    byte[] data = null;
    try {
      data = db.get(bytes(attemptKey));
    } catch (DBException e) {
      throw new IOException(e);
    }
    if (data == null) {
      return null;
    }
    return createAttemptState(attemptId.toString(), data);
  }

  private ApplicationAttemptStateData createAttemptState(String itemName,
      byte[] data) throws IOException {
    ApplicationAttemptId attemptId = ApplicationAttemptId.fromString(itemName);
    ApplicationAttemptStateDataPBImpl attemptState =
        new ApplicationAttemptStateDataPBImpl(
            ApplicationAttemptStateDataProto.parseFrom(data));
    if (!attemptId.equals(attemptState.getAttemptId())) {
      throw new YarnRuntimeException("The database entry for " + attemptId
          + " contains data for " + attemptState.getAttemptId());
    }
    return attemptState;
  }

  private void loadAMRMTokenSecretManagerState(RMState rmState)
      throws IOException {
    try {
      byte[] data = db.get(bytes(AMRMTOKEN_SECRET_MANAGER_ROOT));
      if (data != null) {
        AMRMTokenSecretManagerStatePBImpl stateData =
            new AMRMTokenSecretManagerStatePBImpl(
                AMRMTokenSecretManagerStateProto.parseFrom(data));
        rmState.amrmTokenSecretManagerState =
            AMRMTokenSecretManagerState.newInstance(
                stateData.getCurrentMasterKey(),
                stateData.getNextMasterKey());
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void storeApplicationStateInternal(ApplicationId appId,
      ApplicationStateData appStateData) throws IOException {
    String key = getApplicationNodeKey(appId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing state for app " + appId + " at " + key);
    }
    try {
      db.put(bytes(key), appStateData.getProto().toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void updateApplicationStateInternal(ApplicationId appId,
      ApplicationStateData appStateData) throws IOException {
    storeApplicationStateInternal(appId, appStateData);
  }

  @Override
  protected void storeApplicationAttemptStateInternal(
      ApplicationAttemptId attemptId,
      ApplicationAttemptStateData attemptStateData) throws IOException {
    String key = getApplicationAttemptNodeKey(attemptId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing state for attempt " + attemptId + " at " + key);
    }
    try {
      db.put(bytes(key), attemptStateData.getProto().toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void updateApplicationAttemptStateInternal(
      ApplicationAttemptId attemptId,
      ApplicationAttemptStateData attemptStateData) throws IOException {
    storeApplicationAttemptStateInternal(attemptId, attemptStateData);
  }

  @Override
  public synchronized void removeApplicationAttemptInternal(
      ApplicationAttemptId attemptId)
      throws IOException {
    String attemptKey = getApplicationAttemptNodeKey(attemptId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing state for attempt " + attemptId + " at "
          + attemptKey);
    }
    try {
      db.delete(bytes(attemptKey));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void removeApplicationStateInternal(ApplicationStateData appState)
      throws IOException {
    ApplicationId appId =
        appState.getApplicationSubmissionContext().getApplicationId();
    String appKey = getApplicationNodeKey(appId);
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        batch.delete(bytes(appKey));
        for (ApplicationAttemptId attemptId : appState.attempts.keySet()) {
          String attemptKey = getApplicationAttemptNodeKey(appKey, attemptId);
          batch.delete(bytes(attemptKey));
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removing state for app " + appId + " and "
              + appState.attempts.size() + " attempts" + " at " + appKey);
        }
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        String key = getReservationNodeKey(planName, reservationIdName);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Storing state for reservation " + reservationIdName
              + " plan " + planName + " at " + key);
        }
        batch.put(bytes(key), reservationAllocation.toByteArray());
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void removeReservationState(String planName,
      String reservationIdName) throws Exception {
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        String reservationKey =
            getReservationNodeKey(planName, reservationIdName);
        batch.delete(bytes(reservationKey));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removing state for reservation " + reservationIdName
              + " plan " + planName + " at " + reservationKey);
        }
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  private void storeOrUpdateRMDT(RMDelegationTokenIdentifier tokenId,
      Long renewDate, boolean isUpdate) throws IOException {
    String tokenKey = getRMDTTokenNodeKey(tokenId);
    RMDelegationTokenIdentifierData tokenData =
        new RMDelegationTokenIdentifierData(tokenId, renewDate);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing token to " + tokenKey);
    }
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        batch.put(bytes(tokenKey), tokenData.toByteArray());
        if(!isUpdate) {
          ByteArrayOutputStream bs = new ByteArrayOutputStream();
          try (DataOutputStream ds = new DataOutputStream(bs)) {
            ds.writeInt(tokenId.getSequenceNumber());
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Storing " + tokenId.getSequenceNumber() + " to "
                + RM_DT_SEQUENCE_NUMBER_KEY);   
          }
          batch.put(bytes(RM_DT_SEQUENCE_NUMBER_KEY), bs.toByteArray());
        }
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void storeRMDelegationTokenState(
      RMDelegationTokenIdentifier tokenId, Long renewDate)
      throws IOException {
    storeOrUpdateRMDT(tokenId, renewDate, false);
  }

  @Override
  protected void updateRMDelegationTokenState(
      RMDelegationTokenIdentifier tokenId, Long renewDate)
      throws IOException {
    storeOrUpdateRMDT(tokenId, renewDate, true);
  }

  @Override
  protected void removeRMDelegationTokenState(
      RMDelegationTokenIdentifier tokenId) throws IOException {
    String tokenKey = getRMDTTokenNodeKey(tokenId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing token at " + tokenKey);
    }
    try {
      db.delete(bytes(tokenKey));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void storeRMDTMasterKeyState(DelegationKey masterKey)
      throws IOException {
    String dbKey = getRMDTMasterKeyNodeKey(masterKey);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing token master key to " + dbKey);
    }
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(os);
    try {
      masterKey.write(out);
    } finally {
      out.close();
    }
    try {
      db.put(bytes(dbKey), os.toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void removeRMDTMasterKeyState(DelegationKey masterKey)
      throws IOException {
    String dbKey = getRMDTMasterKeyNodeKey(masterKey);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing token master key at " + dbKey);
    }
    try {
      db.delete(bytes(dbKey));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void storeOrUpdateAMRMTokenSecretManagerState(
      AMRMTokenSecretManagerState state, boolean isUpdate) {
    AMRMTokenSecretManagerState data =
        AMRMTokenSecretManagerState.newInstance(state);
    byte[] stateData = data.getProto().toByteArray();
    db.put(bytes(AMRMTOKEN_SECRET_MANAGER_ROOT), stateData);
  }

  @Override
  public void deleteStore() throws IOException {
    Path root = getStorageDir();
    LOG.info("Deleting state database at " + root);
    db.close();
    db = null;
    FileSystem fs = FileSystem.getLocal(getConfig());
    fs.delete(root, true);
  }

  @Override
  public synchronized void removeApplication(ApplicationId removeAppId)
      throws IOException {
    String appKey = getApplicationNodeKey(removeAppId);
    LOG.info("Removing state for app " + removeAppId);
    try {
      db.delete(bytes(appKey));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  int getNumEntriesInDatabase() throws IOException {
    int numEntries = 0;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seekToFirst();
      while (iter.hasNext()) {
        Entry<byte[], byte[]> entry = iter.next();
        LOG.info("entry: " + asString(entry.getKey()));
        ++numEntries;
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    return numEntries;
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
}
