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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
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
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.EpochPBImpl;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
/**
 * A simple class for storing RM state in any storage that implements a basic
 * FileSystem interface. Does not use directories so that simple key-value
 * stores can be used. The retry policy for the real filesystem client must be
 * configured separately to enable retry of filesystem operations when needed.
 *
 * Changes from 1.1 to 1.2, AMRMTokenSecretManager state has been saved
 * separately. The currentMasterkey and nextMasterkey have been stored.
 * Also, AMRMToken has been removed from ApplicationAttemptState.
 *
 * Changes from 1.2 to 1.3, Addition of ReservationSystem state.
 */
public class FileSystemRMStateStore extends RMStateStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(FileSystemRMStateStore.class);

  protected static final String ROOT_DIR_NAME = "FSRMStateRoot";
  protected static final Version CURRENT_VERSION_INFO = Version
    .newInstance(1, 3);
  protected static final String AMRMTOKEN_SECRET_MANAGER_NODE =
      "AMRMTokenSecretManagerNode";

  private static final String UNREADABLE_BY_SUPERUSER_XATTRIB =
          "security.hdfs.unreadable.by.superuser";
  protected FileSystem fs;
  @VisibleForTesting
  protected Configuration fsConf;

  private Path rootDirPath;
  @Private
  @VisibleForTesting
  Path rmDTSecretManagerRoot;
  private Path rmAppRoot;
  private Path dtSequenceNumberPath = null;
  private int fsNumRetries;
  private long fsRetryInterval;
  private boolean intermediateEncryptionEnabled =
      YarnConfiguration.DEFAULT_YARN_INTERMEDIATE_DATA_ENCRYPTION;

  @VisibleForTesting
  Path fsWorkingPath;

  Path amrmTokenSecretManagerRoot;
  private Path reservationRoot;
  private Path proxyCARoot;

  @Override
  public synchronized void initInternal(Configuration conf)
      throws Exception{
    fsWorkingPath = new Path(conf.get(YarnConfiguration.FS_RM_STATE_STORE_URI));
    rootDirPath = new Path(fsWorkingPath, ROOT_DIR_NAME);
    rmDTSecretManagerRoot = new Path(rootDirPath, RM_DT_SECRET_MANAGER_ROOT);
    rmAppRoot = new Path(rootDirPath, RM_APP_ROOT);
    amrmTokenSecretManagerRoot =
        new Path(rootDirPath, AMRMTOKEN_SECRET_MANAGER_ROOT);
    reservationRoot = new Path(rootDirPath, RESERVATION_SYSTEM_ROOT);
    proxyCARoot = new Path(rootDirPath, PROXY_CA_ROOT);
    fsNumRetries =
        conf.getInt(YarnConfiguration.FS_RM_STATE_STORE_NUM_RETRIES,
            YarnConfiguration.DEFAULT_FS_RM_STATE_STORE_NUM_RETRIES);
    fsRetryInterval =
        conf.getLong(YarnConfiguration.FS_RM_STATE_STORE_RETRY_INTERVAL_MS,
                YarnConfiguration.DEFAULT_FS_RM_STATE_STORE_RETRY_INTERVAL_MS);
    intermediateEncryptionEnabled =
        conf.getBoolean(YarnConfiguration.YARN_INTERMEDIATE_DATA_ENCRYPTION,
          YarnConfiguration.DEFAULT_YARN_INTERMEDIATE_DATA_ENCRYPTION);
  }

  @Override
  protected synchronized void startInternal() throws Exception {
    // create filesystem only now, as part of service-start. By this time, RM is
    // authenticated with kerberos so we are good to create a file-system
    // handle.
    fsConf = new Configuration(getConfig());

    String scheme = fsWorkingPath.toUri().getScheme();
    if (scheme == null) {
      scheme = FileSystem.getDefaultUri(fsConf).getScheme();
    }
    if (scheme != null) {
      String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
      fsConf.setBoolean(disableCacheName, true);
    }

    fs = fsWorkingPath.getFileSystem(fsConf);
    mkdirsWithRetries(rmDTSecretManagerRoot);
    mkdirsWithRetries(rmAppRoot);
    mkdirsWithRetries(amrmTokenSecretManagerRoot);
    mkdirsWithRetries(reservationRoot);
    mkdirsWithRetries(proxyCARoot);
  }

  @Override
  protected synchronized void closeInternal() throws Exception {
    closeWithRetries();
  }

  @Override
  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  protected synchronized Version loadVersion() throws Exception {
    Path versionNodePath = getNodePath(rootDirPath, VERSION_NODE);
    FileStatus status = getFileStatusWithRetries(versionNodePath);
    if (status != null) {
      byte[] data = readFileWithRetries(versionNodePath, status.getLen());
      Version version =
          new VersionPBImpl(VersionProto.parseFrom(data));
      return version;
    }
    return null;
  }

  @Override
  protected synchronized void storeVersion() throws Exception {
    Path versionNodePath = getNodePath(rootDirPath, VERSION_NODE);
    byte[] data =
        ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();
    if (existsWithRetries(versionNodePath)) {
      updateFile(versionNodePath, data, false);
    } else {
      writeFileWithRetries(versionNodePath, data, false);
    }
  }
  
  @Override
  public synchronized long getAndIncrementEpoch() throws Exception {
    Path epochNodePath = getNodePath(rootDirPath, EPOCH_NODE);
    long currentEpoch = baseEpoch;
    FileStatus status = getFileStatusWithRetries(epochNodePath);
    if (status != null) {
      // load current epoch
      byte[] data = readFileWithRetries(epochNodePath, status.getLen());
      Epoch epoch = new EpochPBImpl(EpochProto.parseFrom(data));
      currentEpoch = epoch.getEpoch();
      // increment epoch and store it
      byte[] storeData = Epoch.newInstance(nextEpoch(currentEpoch)).getProto()
          .toByteArray();
      updateFile(epochNodePath, storeData, false);
    } else {
      // initialize epoch file with 1 for the next time.
      byte[] storeData = Epoch.newInstance(nextEpoch(currentEpoch)).getProto()
          .toByteArray();
      writeFileWithRetries(epochNodePath, storeData, false);
    }
    return currentEpoch;
  }
  
  @Override
  public synchronized RMState loadState() throws Exception {
    RMState rmState = new RMState();
    // recover DelegationTokenSecretManager
    loadRMDTSecretManagerState(rmState);
    // recover RM applications
    loadRMAppState(rmState);
    // recover AMRMTokenSecretManager
    loadAMRMTokenSecretManagerState(rmState);
    // recover reservation state
    loadReservationSystemState(rmState);
    // recover ProxyCAManager state
    loadProxyCAManagerState(rmState);
    return rmState;
  }

  private void loadReservationSystemState(RMState rmState) throws Exception {
    try {
      final ReservationStateFileProcessor fileProcessor = new
          ReservationStateFileProcessor(rmState);
      final Path rootDirectory = this.reservationRoot;

      processDirectoriesOfFiles(fileProcessor, rootDirectory);
    } catch (Exception e) {
      LOG.error("Failed to load state.", e);
      throw e;
    }
  }

  private void loadAMRMTokenSecretManagerState(RMState rmState)
      throws Exception {
    checkAndResumeUpdateOperation(amrmTokenSecretManagerRoot);
    Path amrmTokenSecretManagerStateDataDir =
        new Path(amrmTokenSecretManagerRoot, AMRMTOKEN_SECRET_MANAGER_NODE);
    FileStatus status = getFileStatusWithRetries(
        amrmTokenSecretManagerStateDataDir);
    if (status == null) {
      return;
    }
    assert status.isFile();
    byte[] data = readFileWithRetries(amrmTokenSecretManagerStateDataDir,
            status.getLen());
    AMRMTokenSecretManagerStatePBImpl stateData =
        new AMRMTokenSecretManagerStatePBImpl(
          AMRMTokenSecretManagerStateProto.parseFrom(data));
    rmState.amrmTokenSecretManagerState =
        AMRMTokenSecretManagerState.newInstance(
          stateData.getCurrentMasterKey(), stateData.getNextMasterKey());
  }

  private void loadRMAppState(RMState rmState) throws Exception {
    try {
      List<ApplicationAttemptStateData> attempts = new ArrayList<>();
      final RMAppStateFileProcessor rmAppStateFileProcessor =
          new RMAppStateFileProcessor(rmState, attempts);
      final Path rootDirectory = this.rmAppRoot;

      processDirectoriesOfFiles(rmAppStateFileProcessor, rootDirectory);

      // go through all attempts and add them to their apps, Ideally, each
      // attempt node must have a corresponding app node, because remove
      // directory operation remove both at the same time
      for (ApplicationAttemptStateData attemptState : attempts) {
        ApplicationId appId = attemptState.getAttemptId().getApplicationId();
        ApplicationStateData appState = rmState.appState.get(appId);
        assert appState != null;
        appState.attempts.put(attemptState.getAttemptId(), attemptState);
      }
      LOG.info("Done loading applications from FS state store");
    } catch (Exception e) {
      LOG.error("Failed to load state.", e);
      throw e;
    }
  }

  private void processDirectoriesOfFiles(
      RMStateFileProcessor rmAppStateFileProcessor, Path rootDirectory)
    throws Exception {
    for (FileStatus dir : listStatusWithRetries(rootDirectory)) {
      checkAndResumeUpdateOperation(dir.getPath());
      String dirName = dir.getPath().getName();
      for (FileStatus fileNodeStatus : listStatusWithRetries(dir.getPath())) {
        assert fileNodeStatus.isFile();
        String fileName = fileNodeStatus.getPath().getName();
        if (checkAndRemovePartialRecordWithRetries(fileNodeStatus.getPath())) {
          continue;
        }
        byte[] fileData = readFileWithRetries(fileNodeStatus.getPath(),
                fileNodeStatus.getLen());
        // Set attribute if not already set
        setUnreadableBySuperuserXattrib(fileNodeStatus.getPath());

        rmAppStateFileProcessor.processChildNode(dirName, fileName,
            fileData);
      }
    }
  }

  private boolean checkAndRemovePartialRecord(Path record) throws IOException {
    // If the file ends with .tmp then it shows that it failed
    // during saving state into state store. The file will be deleted as a
    // part of this call
    if (record.getName().endsWith(".tmp")) {
      LOG.error("incomplete rm state store entry found :"
          + record);
      fs.delete(record, false);
      return true;
    }
    return false;
  }

  private void checkAndResumeUpdateOperation(Path path) throws Exception {
    // Before loading the state information, check whether .new file exists.
    // If it does, the prior updateFile is failed on half way. We need to
    // complete replacing the old file first.
    FileStatus[] newChildNodes =
        listStatusWithRetries(path, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".new");
      }
    });
    for(FileStatus newChildNodeStatus : newChildNodes) {
      assert newChildNodeStatus.isFile();
      String newChildNodeName = newChildNodeStatus.getPath().getName();
      String childNodeName = newChildNodeName.substring(
              0, newChildNodeName.length() - ".new".length());
      Path childNodePath =
          new Path(newChildNodeStatus.getPath().getParent(), childNodeName);
      replaceFile(newChildNodeStatus.getPath(), childNodePath);
    }
  }
  private void loadRMDTSecretManagerState(RMState rmState) throws Exception {
    checkAndResumeUpdateOperation(rmDTSecretManagerRoot);
    FileStatus[] childNodes = listStatusWithRetries(rmDTSecretManagerRoot);

    for(FileStatus childNodeStatus : childNodes) {
      assert childNodeStatus.isFile();
      String childNodeName = childNodeStatus.getPath().getName();
      if (checkAndRemovePartialRecordWithRetries(childNodeStatus.getPath())) {
        continue;
      }
      if(childNodeName.startsWith(DELEGATION_TOKEN_SEQUENCE_NUMBER_PREFIX)) {
        rmState.rmSecretManagerState.dtSequenceNumber =
            Integer.parseInt(childNodeName.split("_")[1]);
        continue;
      }

      Path childNodePath = getNodePath(rmDTSecretManagerRoot, childNodeName);
      byte[] childData = readFileWithRetries(childNodePath,
          childNodeStatus.getLen());
      ByteArrayInputStream is = new ByteArrayInputStream(childData);
      try (DataInputStream fsIn = new DataInputStream(is)) {
        if (childNodeName.startsWith(DELEGATION_KEY_PREFIX)) {
          DelegationKey key = new DelegationKey();
          key.readFields(fsIn);
          rmState.rmSecretManagerState.masterKeyState.add(key);
          LOG.debug("Loaded delegation key: keyId={}, expirationDate={}",
              key.getKeyId(), key.getExpiryDate());
        } else if (childNodeName.startsWith(DELEGATION_TOKEN_PREFIX)) {
          RMDelegationTokenIdentifierData identifierData =
              RMStateStoreUtils.readRMDelegationTokenIdentifierData(fsIn);
          RMDelegationTokenIdentifier identifier =
              identifierData.getTokenIdentifier();
          long renewDate = identifierData.getRenewDate();
          rmState.rmSecretManagerState.delegationTokenState.put(identifier,
            renewDate);
          LOG.debug("Loaded RMDelegationTokenIdentifier: {} renewDate={}",
              identifier, renewDate);
        } else {
          LOG.warn("Unknown file for recovering RMDelegationTokenSecretManager");
        }
      }
    }
  }

  private void loadProxyCAManagerState(RMState rmState) throws Exception {
    checkAndResumeUpdateOperation(proxyCARoot);

    Path caCertPath = getNodePath(proxyCARoot, PROXY_CA_CERT_NODE);
    Path caPrivateKeyPath = getNodePath(proxyCARoot, PROXY_CA_PRIVATE_KEY_NODE);

    if (!existsWithRetries(caCertPath)
        || !existsWithRetries(caPrivateKeyPath)) {
      LOG.warn("Couldn't find Proxy CA data");
      return;
    }

    FileStatus caCertFileStatus = getFileStatus(caCertPath);
    byte[] caCertData = readFileWithRetries(caCertPath,
        caCertFileStatus.getLen());

    FileStatus caPrivateKeyFileStatus = getFileStatus(caPrivateKeyPath);
    byte[] caPrivateKeyData = readFileWithRetries(caPrivateKeyPath,
        caPrivateKeyFileStatus.getLen());

    rmState.getProxyCAState().setCaCert(caCertData);
    rmState.getProxyCAState().setCaPrivateKey(caPrivateKeyData);
  }

  @Override
  public synchronized void storeApplicationStateInternal(ApplicationId appId,
      ApplicationStateData appStateDataPB) throws Exception {
    Path appDirPath = getAppDir(rmAppRoot, appId);
    mkdirsWithRetries(appDirPath);
    Path nodeCreatePath = getNodePath(appDirPath, appId.toString());

    LOG.info("Storing info for app: " + appId + " at: " + nodeCreatePath);
    byte[] appStateData = appStateDataPB.getProto().toByteArray();
    try {
      // currently throw all exceptions. May need to respond differently for HA
      // based on whether we have lost the right to write to FS
      writeFileWithRetries(nodeCreatePath, appStateData, true);
    } catch (Exception e) {
      LOG.info("Error storing info for app: " + appId, e);
      throw e;
    }
  }

  @Override
  public synchronized void updateApplicationStateInternal(ApplicationId appId,
      ApplicationStateData appStateDataPB) throws Exception {
    Path appDirPath = getAppDir(rmAppRoot, appId);
    Path nodeCreatePath = getNodePath(appDirPath, appId.toString());

    LOG.info("Updating info for app: " + appId + " at: " + nodeCreatePath);
    byte[] appStateData = appStateDataPB.getProto().toByteArray();
    try {
      // currently throw all exceptions. May need to respond differently for HA
      // based on whether we have lost the right to write to FS
      updateFile(nodeCreatePath, appStateData, true);
    } catch (Exception e) {
      LOG.info("Error updating info for app: " + appId, e);
      throw e;
    }
  }

  @Override
  public synchronized void storeApplicationAttemptStateInternal(
      ApplicationAttemptId appAttemptId,
      ApplicationAttemptStateData attemptStateDataPB)
      throws Exception {
    Path appDirPath =
        getAppDir(rmAppRoot, appAttemptId.getApplicationId());
    Path nodeCreatePath = getNodePath(appDirPath, appAttemptId.toString());
    LOG.info("Storing info for attempt: " + appAttemptId + " at: "
        + nodeCreatePath);
    byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();
    try {
      // currently throw all exceptions. May need to respond differently for HA
      // based on whether we have lost the right to write to FS
      writeFileWithRetries(nodeCreatePath, attemptStateData, true);
    } catch (Exception e) {
      LOG.info("Error storing info for attempt: " + appAttemptId, e);
      throw e;
    }
  }

  @Override
  public synchronized void updateApplicationAttemptStateInternal(
      ApplicationAttemptId appAttemptId,
      ApplicationAttemptStateData attemptStateDataPB)
      throws Exception {
    Path appDirPath =
        getAppDir(rmAppRoot, appAttemptId.getApplicationId());
    Path nodeCreatePath = getNodePath(appDirPath, appAttemptId.toString());
    LOG.info("Updating info for attempt: " + appAttemptId + " at: "
        + nodeCreatePath);
    byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();
    try {
      // currently throw all exceptions. May need to respond differently for HA
      // based on whether we have lost the right to write to FS
      updateFile(nodeCreatePath, attemptStateData, true);
    } catch (Exception e) {
      LOG.info("Error updating info for attempt: " + appAttemptId, e);
      throw e;
    }
  }

  @Override
  public synchronized void removeApplicationAttemptInternal(
      ApplicationAttemptId appAttemptId)
      throws Exception {
    Path appDirPath =
        getAppDir(rmAppRoot, appAttemptId.getApplicationId());
    Path nodeRemovePath = getNodePath(appDirPath, appAttemptId.toString());
    LOG.info("Removing info for attempt: " + appAttemptId + " at: "
        + nodeRemovePath);
    deleteFileWithRetries(nodeRemovePath);
  }

  @Override
  public synchronized void removeApplicationStateInternal(
      ApplicationStateData appState)
      throws Exception {
    ApplicationId appId =
        appState.getApplicationSubmissionContext().getApplicationId();
    Path nodeRemovePath = getAppDir(rmAppRoot, appId);
    LOG.info("Removing info for app: " + appId + " at: " + nodeRemovePath);
    deleteFileWithRetries(nodeRemovePath);
  }

  @Override
  public synchronized void storeRMDelegationTokenState(
      RMDelegationTokenIdentifier identifier, Long renewDate)
      throws Exception {
    storeOrUpdateRMDelegationTokenState(identifier, renewDate, false);
  }

  @Override
  public synchronized void removeRMDelegationTokenState(
      RMDelegationTokenIdentifier identifier) throws Exception {
    Path nodeCreatePath = getNodePath(rmDTSecretManagerRoot,
            DELEGATION_TOKEN_PREFIX + identifier.getSequenceNumber());
    LOG.info("Removing RMDelegationToken_" + identifier.getSequenceNumber());
    deleteFileWithRetries(nodeCreatePath);
  }

  @Override
  protected synchronized void updateRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    storeOrUpdateRMDelegationTokenState(rmDTIdentifier, renewDate, true);
  }

  private void storeOrUpdateRMDelegationTokenState(
      RMDelegationTokenIdentifier identifier, Long renewDate,
      boolean isUpdate) throws Exception {
    Path nodeCreatePath =
        getNodePath(rmDTSecretManagerRoot,
          DELEGATION_TOKEN_PREFIX + identifier.getSequenceNumber());
    RMDelegationTokenIdentifierData identifierData =
        new RMDelegationTokenIdentifierData(identifier, renewDate);
    if (isUpdate) {
      LOG.info("Updating RMDelegationToken_" + identifier.getSequenceNumber());
      updateFile(nodeCreatePath, identifierData.toByteArray(), true);
    } else {
      LOG.info("Storing RMDelegationToken_" + identifier.getSequenceNumber());
      writeFileWithRetries(nodeCreatePath, identifierData.toByteArray(), true);

      // store sequence number
      Path latestSequenceNumberPath = getNodePath(rmDTSecretManagerRoot,
            DELEGATION_TOKEN_SEQUENCE_NUMBER_PREFIX
            + identifier.getSequenceNumber());
      LOG.info("Storing " + DELEGATION_TOKEN_SEQUENCE_NUMBER_PREFIX
          + identifier.getSequenceNumber());
      if (dtSequenceNumberPath == null) {
        if (!createFileWithRetries(latestSequenceNumberPath)) {
          throw new Exception("Failed to create " + latestSequenceNumberPath);
        }
      } else {
        if (!renameFileWithRetries(dtSequenceNumberPath,
            latestSequenceNumberPath)) {
          throw new Exception("Failed to rename " + dtSequenceNumberPath);
        }
      }
      dtSequenceNumberPath = latestSequenceNumberPath;
    }
  }

  @Override
  public synchronized void storeRMDTMasterKeyState(DelegationKey masterKey)
      throws Exception {
    Path nodeCreatePath = getNodePath(rmDTSecretManagerRoot,
          DELEGATION_KEY_PREFIX + masterKey.getKeyId());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (DataOutputStream fsOut = new DataOutputStream(os)) {
      LOG.info("Storing RMDelegationKey_" + masterKey.getKeyId());
      masterKey.write(fsOut);
      writeFileWithRetries(nodeCreatePath, os.toByteArray(), true);
    }
  }

  @Override
  public synchronized void
      removeRMDTMasterKeyState(DelegationKey masterKey) throws Exception {
    Path nodeCreatePath = getNodePath(rmDTSecretManagerRoot,
          DELEGATION_KEY_PREFIX + masterKey.getKeyId());
    LOG.info("Removing RMDelegationKey_"+ masterKey.getKeyId());
    deleteFileWithRetries(nodeCreatePath);
  }

  @Override
  public synchronized void deleteStore() throws Exception {
    if (existsWithRetries(rootDirPath)) {
      deleteFileWithRetries(rootDirPath);
    }
  }

  @Override
  public synchronized void removeApplication(ApplicationId removeAppId)
      throws Exception {
    Path nodeRemovePath = getAppDir(rmAppRoot, removeAppId);
    if (existsWithRetries(nodeRemovePath)) {
      deleteFileWithRetries(nodeRemovePath);
    }
  }

  @Override
  synchronized protected void storeProxyCACertState(
      X509Certificate caCert, PrivateKey caPrivateKey) throws Exception {
    byte[] caCertData = caCert.getEncoded();
    byte[] caPrivateKeyData = caPrivateKey.getEncoded();

    Path caCertPath = getNodePath(proxyCARoot, PROXY_CA_CERT_NODE);
    Path caPrivateKeyPath = getNodePath(proxyCARoot, PROXY_CA_PRIVATE_KEY_NODE);

    if (existsWithRetries(caCertPath)) {
      updateFile(caCertPath, caCertData, true);
    } else {
      writeFileWithRetries(caCertPath, caCertData, true);
    }

    if (existsWithRetries(caPrivateKeyPath)) {
      updateFile(caPrivateKeyPath, caPrivateKeyData, true);
    } else {
      writeFileWithRetries(caPrivateKeyPath, caPrivateKeyData, true);
    }
  }

  private Path getAppDir(Path root, ApplicationId appId) {
    return getNodePath(root, appId.toString());
  }

  @VisibleForTesting
  protected Path getAppDir(ApplicationId appId) {
    return getAppDir(rmAppRoot, appId);
  }

  @VisibleForTesting
  protected Path getAppAttemptDir(ApplicationAttemptId appAttId) {
    return getNodePath(getAppDir(appAttId.getApplicationId()), appAttId
            .toString());
  }
  // FileSystem related code

  private boolean checkAndRemovePartialRecordWithRetries(final Path record)
      throws Exception {
    return new FSAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return checkAndRemovePartialRecord(record);
      }
    }.runWithRetries();
  }

  private void mkdirsWithRetries(final Path appDirPath) throws Exception {
    new FSAction<Void>() {
      @Override
      public Void run() throws Exception {
        fs.mkdirs(appDirPath);
        return null;
      }
    }.runWithRetries();
  }

  private void writeFileWithRetries(final Path outputPath, final byte[] data,
                                    final boolean makeUnreadableByAdmin)
          throws Exception {
    new FSAction<Void>() {
      @Override
      public Void run() throws Exception {
        writeFile(outputPath, data, makeUnreadableByAdmin);
        return null;
      }
    }.runWithRetries();
  }

  private void deleteFileWithRetries(final Path deletePath) throws Exception {
    new FSAction<Void>() {
      @Override
      public Void run() throws Exception {
        deleteFile(deletePath);
        return null;
      }
    }.runWithRetries();
  }

  private boolean renameFileWithRetries(final Path src, final Path dst)
      throws Exception {
    return new FSAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return renameFile(src, dst);
      }
    }.runWithRetries();
  }

  private boolean createFileWithRetries(final Path newFile) throws Exception {
    return new FSAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return createFile(newFile);
      }
    }.runWithRetries();
  }

  private FileStatus getFileStatusWithRetries(final Path path)
      throws Exception {
    return new FSAction<FileStatus>() {
      @Override
      public FileStatus run() throws Exception {
        return getFileStatus(path);
      }
    }.runWithRetries();
  }

  private boolean existsWithRetries(final Path path) throws Exception {
    return new FSAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return fs.exists(path);
      }
    }.runWithRetries();
  }

  private byte[] readFileWithRetries(final Path inputPath, final long len)
      throws Exception {
    return new FSAction<byte[]>() {
      @Override
      public byte[] run() throws Exception {
        return readFile(inputPath, len);
      }
    }.runWithRetries();
  }

  private FileStatus[] listStatusWithRetries(final Path path)
      throws Exception {
    return new FSAction<FileStatus[]>() {
      @Override
      public FileStatus[] run() throws Exception {
        return fs.listStatus(path);
      }
    }.runWithRetries();
  }

  private FileStatus[] listStatusWithRetries(final Path path,
      final PathFilter filter) throws Exception {
    return new FSAction<FileStatus[]>() {
      @Override
      public FileStatus[] run() throws Exception {
        return fs.listStatus(path, filter);
      }
    }.runWithRetries();
  }

  private void closeWithRetries() throws Exception {
    new FSAction<Void>() {
      @Override
      public Void run() throws Exception {
        IOUtils.closeStream(fs);
        return null;
      }
    }.runWithRetries();
  }

  private abstract class FSAction<T> {
    abstract T run() throws Exception;

    T runWithRetries() throws Exception {
      int retry = 0;
      while (true) {
        try {
          return run();
        } catch (IOException e) {
          LOG.info("Exception while executing an FS operation.", e);
          if (++retry > fsNumRetries) {
            LOG.info("Maxed out FS retries. Giving up!");
            throw e;
          }
          LOG.info("Retrying operation on FS. Retry no. " + retry);
          Thread.sleep(fsRetryInterval);
        }
      }
    }
  }

  private void deleteFile(Path deletePath) throws Exception {
    if(!fs.delete(deletePath, true)) {
      throw new Exception("Failed to delete " + deletePath);
    }
  }

  private byte[] readFile(Path inputPath, long len) throws Exception {
    FSDataInputStream fsIn = null;
    try {
      fsIn = fs.open(inputPath);
      // state data will not be that "long"
      byte[] data = new byte[(int) len];
      fsIn.readFully(data);
      return data;
    } finally {
      IOUtils.cleanupWithLogger(LOG, fsIn);
    }
  }

  private FileStatus getFileStatus(Path path) throws Exception {
    try {
      return fs.getFileStatus(path);
    } catch (FileNotFoundException e) {
      return null;
    }
  }

  /*
   * In order to make this write atomic as a part of write we will first write
   * data to .tmp file and then rename it. Here we are assuming that rename is
   * atomic for underlying file system.
   */
  protected void writeFile(Path outputPath, byte[] data, boolean
          makeUnreadableByAdmin) throws Exception {
    Path tempPath =
        new Path(outputPath.getParent(), outputPath.getName() + ".tmp");
    FSDataOutputStream fsOut = null;
    // This file will be overwritten when app/attempt finishes for saving the
    // final status.
    try {
      fsOut = fs.create(tempPath, true);
      if (makeUnreadableByAdmin) {
        setUnreadableBySuperuserXattrib(tempPath);
      }
      fsOut.write(data);
      fsOut.close();
      fsOut = null;
      fs.rename(tempPath, outputPath);
    } finally {
      IOUtils.cleanupWithLogger(LOG, fsOut);
    }
  }

  /*
   * In order to make this update atomic as a part of write we will first write
   * data to .new file and then rename it. Here we are assuming that rename is
   * atomic for underlying file system.
   */
  protected void updateFile(Path outputPath, byte[] data, boolean
          makeUnreadableByAdmin) throws Exception {
    Path newPath = new Path(outputPath.getParent(), outputPath.getName() + ".new");
    // use writeFileWithRetries to make sure .new file is created atomically
    writeFileWithRetries(newPath, data, makeUnreadableByAdmin);
    replaceFile(newPath, outputPath);
  }

  protected void replaceFile(Path srcPath, Path dstPath) throws Exception {
    if (existsWithRetries(dstPath)) {
      deleteFileWithRetries(dstPath);
    } else {
      LOG.info("File doesn't exist. Skip deleting the file " + dstPath);
    }
    renameFileWithRetries(srcPath, dstPath);
  }

  @Private
  @VisibleForTesting
  boolean renameFile(Path src, Path dst) throws Exception {
    return fs.rename(src, dst);
  }

  private boolean createFile(Path newFile) throws Exception {
    return fs.createNewFile(newFile);
  }

  @Private
  @VisibleForTesting
  Path getNodePath(Path root, String nodeName) {
    return new Path(root, nodeName);
  }

  @Override
  public synchronized void storeOrUpdateAMRMTokenSecretManagerState(
      AMRMTokenSecretManagerState amrmTokenSecretManagerState, boolean isUpdate)
      throws Exception {
    Path nodeCreatePath =
        getNodePath(amrmTokenSecretManagerRoot, AMRMTOKEN_SECRET_MANAGER_NODE);
    AMRMTokenSecretManagerState data =
        AMRMTokenSecretManagerState.newInstance(amrmTokenSecretManagerState);
    byte[] stateData = data.getProto().toByteArray();
    if (isUpdate) {
      updateFile(nodeCreatePath, stateData, true);
    } else {
      writeFileWithRetries(nodeCreatePath, stateData, true);
    }
  }

  @Override
  protected void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
    Path planCreatePath = getNodePath(reservationRoot, planName);
    mkdirsWithRetries(planCreatePath);
    Path reservationPath = getNodePath(planCreatePath, reservationIdName);
    LOG.info("Storing state for reservation " + reservationIdName + " from " +
        "plan " + planName + " at path " + reservationPath);
    byte[] reservationData = reservationAllocation.toByteArray();
    writeFileWithRetries(reservationPath, reservationData, true);
  }

  @Override
  protected void removeReservationState(
      String planName, String reservationIdName) throws Exception {
    Path planCreatePath = getNodePath(reservationRoot, planName);
    Path reservationPath = getNodePath(planCreatePath, reservationIdName);
    LOG.info("Removing state for reservation " + reservationIdName + " from " +
        "plan " + planName + " at path " + reservationPath);
    deleteFileWithRetries(reservationPath);
  }

  @VisibleForTesting
  public int getNumRetries() {
    return fsNumRetries;
  }

  @VisibleForTesting
  public long getRetryInterval() {
    return fsRetryInterval;
  }

  private void setUnreadableBySuperuserXattrib(Path p) throws IOException {
    if (fs.getScheme().toLowerCase().contains("hdfs")
        && intermediateEncryptionEnabled
        && !fs.getXAttrs(p).containsKey(UNREADABLE_BY_SUPERUSER_XATTRIB)) {
      fs.setXAttr(p, UNREADABLE_BY_SUPERUSER_XATTRIB, null,
        EnumSet.of(XAttrSetFlag.CREATE));
    }
  }

  private static class ReservationStateFileProcessor implements
      RMStateFileProcessor {
    private RMState rmState;
    public ReservationStateFileProcessor(RMState state) {
      this.rmState = state;
    }

    @Override
    public void processChildNode(String planName, String childNodeName,
        byte[] childData) throws IOException {
      ReservationAllocationStateProto allocationState =
          ReservationAllocationStateProto.parseFrom(childData);
      if (!rmState.getReservationState().containsKey(planName)) {
        rmState.getReservationState().put(planName,
            new HashMap<ReservationId, ReservationAllocationStateProto>());
      }
      ReservationId reservationId =
          ReservationId.parseReservationId(childNodeName);
      rmState.getReservationState().get(planName).put(reservationId,
          allocationState);
    }
  }

  private static class RMAppStateFileProcessor implements RMStateFileProcessor {
    private RMState rmState;
    private List<ApplicationAttemptStateData> attempts;

    public RMAppStateFileProcessor(RMState rmState,
        List<ApplicationAttemptStateData> attempts) {
      this.rmState = rmState;
      this.attempts = attempts;
    }

    @Override
    public void processChildNode(String appDirName, String childNodeName,
        byte[] childData)
        throws com.google.protobuf.InvalidProtocolBufferException {
      if (childNodeName.startsWith(ApplicationId.appIdStrPrefix)) {
        // application
        LOG.debug("Loading application from node: {}", childNodeName);
        ApplicationStateDataPBImpl appState =
            new ApplicationStateDataPBImpl(
                ApplicationStateDataProto.parseFrom(childData));
        ApplicationId appId =
            appState.getApplicationSubmissionContext().getApplicationId();
        rmState.appState.put(appId, appState);
      } else if (childNodeName.startsWith(
          ApplicationAttemptId.appAttemptIdStrPrefix)) {
        // attempt
        LOG.debug("Loading application attempt from node: {}", childNodeName);
        ApplicationAttemptStateDataPBImpl attemptState =
            new ApplicationAttemptStateDataPBImpl(
                ApplicationAttemptStateDataProto.parseFrom(childData));
        attempts.add(attemptState);
      } else {
        LOG.info("Unknown child node with name: " + childNodeName);
      }
    }
  }

  // Interface for common state processing of directory of file layout
  private interface RMStateFileProcessor {
    void processChildNode(String appDirName, String childNodeName,
        byte[] childData)
        throws IOException;
  }
}
