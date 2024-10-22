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
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.yarn.server.resourcemanager.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.Epoch;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMDelegationTokenIdentifierData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.AMRMTokenSecretManagerStatePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Changes from 1.0 to 1.1, Addition of ReservationSystem state.
 */
public class LeveldbRMStateStore extends RMStateStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(LeveldbRMStateStore.class);

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
  private DBManager dbManager = new DBManager();
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

  private String getProxyCACertNodeKey() {
    return PROXY_CA_ROOT + SEPARATOR + PROXY_CA_CERT_NODE;
  }

  private String getProxyCAPrivateKeyNodeKey() {
    return PROXY_CA_ROOT + SEPARATOR + PROXY_CA_PRIVATE_KEY_NODE;
  }

  @Override
  protected void initInternal(Configuration conf) {
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
    FsPermission perm = new FsPermission((short)0700);
    fs.mkdirs(root, perm);
    if (!perm.equals(perm.applyUMask(FsPermission.getUMask(getConfig())))) {
      fs.setPermission(root, perm);
    }
    return root;
  }

  @Override
  protected void startInternal() throws Exception {
    Path storeRoot = createStorageDir();
    Options options = new Options();
    options.createIfMissing(false);
    LOG.info("Using state database at " + storeRoot + " for recovery");
    File dbfile = new File(storeRoot.toString());
    db = dbManager.initDatabase(dbfile, options, (database) ->
        storeVersion(CURRENT_VERSION_INFO));
    dbManager.startCompactionTimer(compactionIntervalMsec,
        this.getClass().getSimpleName());
  }

  @Override
  protected void closeInternal() throws Exception {
    dbManager.close();
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
    return dbManager.loadVersion(VERSION_NODE);
  }

  @Override
  protected void storeVersion() throws Exception {
    try {
      storeVersion(CURRENT_VERSION_INFO);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  protected void storeVersion(Version version) {
    dbManager.storeVersion(VERSION_NODE, version);
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
      EpochProto proto = Epoch.newInstance(nextEpoch(currentEpoch)).getProto();
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
    loadProxyCAManagerState(rmState);
    return rmState;
   }

  private void loadReservationState(RMState rmState) throws IOException {
    int numReservations = 0;
    try (LeveldbIterator iter = new LeveldbIterator(db)) {
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
    try (LeveldbIterator iter = new LeveldbIterator(db)) {
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
        LOG.debug("Loaded RM delegation key from {}: keyId={},"
            + " expirationDate={}", key, masterKey.getKeyId(),
            masterKey.getExpiryDate());
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return numKeys;
  }

  private DelegationKey loadDelegationKey(byte[] data) throws IOException {
    DelegationKey key = new DelegationKey();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    try {
      key.readFields(in);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    return key;
  }

  private int loadRMDTSecretManagerTokens(RMState state) throws IOException {
    int numTokens = 0;
    try (LeveldbIterator iter = new LeveldbIterator(db)) {
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
        LOG.debug("Loaded RM delegation token from {}: tokenId={},"
            + " renewDate={}", key, tokenId, renewDate);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return numTokens;
  }

  private RMDelegationTokenIdentifierData loadDelegationToken(byte[] data)
      throws IOException {
    RMDelegationTokenIdentifierData tokenData;
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    try {
      tokenData = RMStateStoreUtils.readRMDelegationTokenIdentifierData(in);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    return tokenData;
  }

  private void loadRMDTSecretManagerTokenSequenceNumber(RMState state)
      throws IOException {
    byte[] data;
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
        IOUtils.cleanupWithLogger(LOG, in);
      }
    }
  }

  private void loadRMApps(RMState state) throws IOException {
    int numApps = 0;
    int numAppAttempts = 0;
    try (LeveldbIterator iter = new LeveldbIterator(db)) {
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
    LOG.debug("Loaded application {} with {} attempts", appId, numAttempts);
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
    byte[] data;
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
    byte[] data;
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

  private void loadProxyCAManagerState(RMState rmState) throws Exception {
    byte[] caCertData;
    byte[] caPrivateKeyData;

    String caCertKey = getProxyCACertNodeKey();
    String caPrivateKeyKey = getProxyCAPrivateKeyNodeKey();

    try {
      caCertData = db.get(bytes(caCertKey));
    } catch (DBException e) {
      throw new IOException(e);
    }

    try {
      caPrivateKeyData = db.get(bytes(caPrivateKeyKey));
    } catch (DBException e) {
      throw new IOException(e);
    }

    if (caCertData == null || caPrivateKeyData == null) {
      LOG.warn("Couldn't find Proxy CA data");
      return;
    }

    rmState.proxyCAState.setCaCert(caCertData);
    rmState.proxyCAState.setCaPrivateKey(caPrivateKeyData);
  }

  @Override
  protected void storeApplicationStateInternal(ApplicationId appId,
      ApplicationStateData appStateData) throws IOException {
    String key = getApplicationNodeKey(appId);
    LOG.debug("Storing state for app {} at {}", appId, key);
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
    LOG.debug("Storing state for attempt {} at {}", attemptId, key);
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
    LOG.debug("Removing state for attempt {} at {}", attemptId, attemptKey);
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
      try (WriteBatch batch = db.createWriteBatch()) {
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
      try (WriteBatch batch = db.createWriteBatch()) {
        String key = getReservationNodeKey(planName, reservationIdName);
        LOG.debug("Storing state for reservation {} plan {} at {}",
            reservationIdName, planName, key);

        batch.put(bytes(key), reservationAllocation.toByteArray());
        db.write(batch);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void removeReservationState(String planName,
      String reservationIdName) throws Exception {
    try {
      try (WriteBatch batch = db.createWriteBatch()) {
        String reservationKey =
            getReservationNodeKey(planName, reservationIdName);
        batch.delete(bytes(reservationKey));
        LOG.debug("Removing state for reservation {} plan {} at {}",
            reservationIdName, planName, reservationKey);
        db.write(batch);
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
    LOG.debug("Storing token to {}", tokenKey);
    try {
      try (WriteBatch batch = db.createWriteBatch()) {
        batch.put(bytes(tokenKey), tokenData.toByteArray());
        if (!isUpdate) {
          ByteArrayOutputStream bs = new ByteArrayOutputStream();
          try (DataOutputStream ds = new DataOutputStream(bs)) {
            ds.writeInt(tokenId.getSequenceNumber());
          }
          LOG.debug("Storing {} to {}", tokenId.getSequenceNumber(),
              RM_DT_SEQUENCE_NUMBER_KEY);
          batch.put(bytes(RM_DT_SEQUENCE_NUMBER_KEY), bs.toByteArray());
        }
        db.write(batch);
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
    LOG.debug("Removing token at {}", tokenKey);
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
    LOG.debug("Storing token master key to {}", dbKey);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(os)) {
      masterKey.write(out);
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
    LOG.debug("Removing token master key at {}", dbKey);
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
  protected void storeProxyCACertState(
      X509Certificate caCert, PrivateKey caPrivateKey) throws Exception {
    byte[] caCertData = caCert.getEncoded();
    byte[] caPrivateKeyData = caPrivateKey.getEncoded();

    String caCertKey = getProxyCACertNodeKey();
    String caPrivateKeyKey = getProxyCAPrivateKeyNodeKey();

    try {
      try (WriteBatch batch = db.createWriteBatch()) {
        batch.put(bytes(caCertKey), caCertData);
        batch.put(bytes(caPrivateKeyKey), caPrivateKeyData);
        db.write(batch);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
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
    try (LeveldbIterator iter = new LeveldbIterator(db)) {
      iter.seekToFirst();
      while (iter.hasNext()) {
        Entry<byte[], byte[]> entry = iter.next();
        LOG.info("entry: " + asString(entry.getKey()));
        ++numEntries;
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return numEntries;
  }

  @VisibleForTesting
  protected void setDbManager(DBManager dbManager) {
    this.dbManager = dbManager;
  }
}
