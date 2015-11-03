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
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMZKUtils;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.Epoch;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMDelegationTokenIdentifierData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.AMRMTokenSecretManagerStatePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.EpochPBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import com.google.common.annotations.VisibleForTesting;

/**
 * {@link RMStateStore} implementation backed by ZooKeeper.
 *
 * The znode structure is as follows:
 * ROOT_DIR_PATH
 * |--- VERSION_INFO
 * |--- EPOCH_NODE
 * |--- RM_ZK_FENCING_LOCK
 * |--- RM_APP_ROOT
 * |     |----- (#ApplicationId1)
 * |     |        |----- (#ApplicationAttemptIds)
 * |     |
 * |     |----- (#ApplicationId2)
 * |     |       |----- (#ApplicationAttemptIds)
 * |     ....
 * |
 * |--- RM_DT_SECRET_MANAGER_ROOT
 *        |----- RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME
 *        |----- RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME
 *        |       |----- Token_1
 *        |       |----- Token_2
 *        |       ....
 *        |
 *        |----- RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME
 *        |      |----- Key_1
 *        |      |----- Key_2
 *                ....
 * |--- AMRMTOKEN_SECRET_MANAGER_ROOT
 *        |----- currentMasterKey
 *        |----- nextMasterKey
 *
 * |-- RESERVATION_SYSTEM_ROOT
 *        |------PLAN_1
 *        |      |------ RESERVATION_1
 *        |      |------ RESERVATION_2
 *        |      ....
 *        |------PLAN_2
 *        ....
 * Note: Changes from 1.1 to 1.2 - AMRMTokenSecretManager state has been saved
 * separately. The currentMasterkey and nextMasterkey have been stored.
 * Also, AMRMToken has been removed from ApplicationAttemptState.
 *
 * Changes from 1.2 to 1.3, Addition of ReservationSystem state.
 */
@Private
@Unstable
public class ZKRMStateStore extends RMStateStore {

  public static final Log LOG = LogFactory.getLog(ZKRMStateStore.class);
  private final SecureRandom random = new SecureRandom();

  protected static final String ROOT_ZNODE_NAME = "ZKRMStateRoot";
  protected static final Version CURRENT_VERSION_INFO = Version
      .newInstance(1, 3);
  private static final String RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME =
      "RMDelegationTokensRoot";
  private static final String RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME =
      "RMDTSequentialNumber";
  private static final String RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME =
      "RMDTMasterKeysRoot";

  private String zkHostPort = null;
  private int numRetries;
  private int zkSessionTimeout;
  @VisibleForTesting
  int zkRetryInterval;

  /** Znode paths */
  private String zkRootNodePath;
  private String rmAppRoot;
  private String rmDTSecretManagerRoot;
  private String dtMasterKeysRootPath;
  private String delegationTokensRootPath;
  private String dtSequenceNumberPath;
  private String amrmTokenSecretManagerRoot;
  private String reservationRoot;
  @VisibleForTesting
  protected String znodeWorkingPath;

  /** Fencing related variables */
  private static final String FENCING_LOCK = "RM_ZK_FENCING_LOCK";
  private boolean useDefaultFencingScheme = false;
  private String fencingNodePath;
  private Thread verifyActiveStatusThread;

  /** ACL and auth info */
  private List<ACL> zkAcl;
  private List<ZKUtil.ZKAuthInfo> zkAuths;
  @VisibleForTesting
  List<ACL> zkRootNodeAcl;
  private String zkRootNodeUsername;
  private final String zkRootNodePassword = Long.toString(random.nextLong());
  public static final int CREATE_DELETE_PERMS =
      ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE;
  private final String zkRootNodeAuthScheme =
      new DigestAuthenticationProvider().getScheme();

  @VisibleForTesting
  protected CuratorFramework curatorFramework;
  /**
   * Given the {@link Configuration} and {@link ACL}s used (zkAcl) for
   * ZooKeeper access, construct the {@link ACL}s for the store's root node.
   * In the constructed {@link ACL}, all the users allowed by zkAcl are given
   * rwa access, while the current RM has exclude create-delete access.
   *
   * To be called only when HA is enabled and the configuration doesn't set ACL
   * for the root node.
   */
  @VisibleForTesting
  @Private
  @Unstable
  protected List<ACL> constructZkRootNodeACL(
      Configuration conf, List<ACL> sourceACLs) throws NoSuchAlgorithmException {
    List<ACL> zkRootNodeAcl = new ArrayList<>();
    for (ACL acl : sourceACLs) {
      zkRootNodeAcl.add(new ACL(
          ZKUtil.removeSpecificPerms(acl.getPerms(), CREATE_DELETE_PERMS),
          acl.getId()));
    }

    zkRootNodeUsername = HAUtil.getConfValueForRMInstance(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS, conf);
    Id rmId = new Id(zkRootNodeAuthScheme,
        DigestAuthenticationProvider.generateDigest(
            zkRootNodeUsername + ":" + zkRootNodePassword));
    zkRootNodeAcl.add(new ACL(CREATE_DELETE_PERMS, rmId));
    return zkRootNodeAcl;
  }

  @Override
  public synchronized void initInternal(Configuration conf) throws Exception {
    zkHostPort = conf.get(YarnConfiguration.RM_ZK_ADDRESS);
    if (zkHostPort == null) {
      throw new YarnRuntimeException("No server address specified for " +
          "zookeeper state store for Resource Manager recovery. " +
          YarnConfiguration.RM_ZK_ADDRESS + " is not configured.");
    }
    numRetries =
        conf.getInt(YarnConfiguration.RM_ZK_NUM_RETRIES,
            YarnConfiguration.DEFAULT_ZK_RM_NUM_RETRIES);
    znodeWorkingPath =
        conf.get(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH,
            YarnConfiguration.DEFAULT_ZK_RM_STATE_STORE_PARENT_PATH);
    zkSessionTimeout =
        conf.getInt(YarnConfiguration.RM_ZK_TIMEOUT_MS,
            YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS);

    if (HAUtil.isHAEnabled(conf)) {
      zkRetryInterval = zkSessionTimeout / numRetries;
    } else {
      zkRetryInterval =
          conf.getInt(YarnConfiguration.RM_ZK_RETRY_INTERVAL_MS,
              YarnConfiguration.DEFAULT_RM_ZK_RETRY_INTERVAL_MS);
    }

    zkAcl = RMZKUtils.getZKAcls(conf);
    zkAuths = RMZKUtils.getZKAuths(conf);

    zkRootNodePath = getNodePath(znodeWorkingPath, ROOT_ZNODE_NAME);
    rmAppRoot = getNodePath(zkRootNodePath, RM_APP_ROOT);

    /* Initialize fencing related paths, acls, and ops */
    fencingNodePath = getNodePath(zkRootNodePath, FENCING_LOCK);
    if (HAUtil.isHAEnabled(conf)) {
      String zkRootNodeAclConf = HAUtil.getConfValueForRMInstance
          (YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL, conf);
      if (zkRootNodeAclConf != null) {
        zkRootNodeAclConf = ZKUtil.resolveConfIndirection(zkRootNodeAclConf);
        try {
          zkRootNodeAcl = ZKUtil.parseACLs(zkRootNodeAclConf);
        } catch (ZKUtil.BadAclFormatException bafe) {
          LOG.error("Invalid format for " +
              YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL);
          throw bafe;
        }
      } else {
        useDefaultFencingScheme = true;
        zkRootNodeAcl = constructZkRootNodeACL(conf, zkAcl);
      }
    }

    rmDTSecretManagerRoot =
        getNodePath(zkRootNodePath, RM_DT_SECRET_MANAGER_ROOT);
    dtMasterKeysRootPath = getNodePath(rmDTSecretManagerRoot,
        RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME);
    delegationTokensRootPath = getNodePath(rmDTSecretManagerRoot,
        RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME);
    dtSequenceNumberPath = getNodePath(rmDTSecretManagerRoot,
        RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME);
    amrmTokenSecretManagerRoot =
        getNodePath(zkRootNodePath, AMRMTOKEN_SECRET_MANAGER_ROOT);
    reservationRoot = getNodePath(zkRootNodePath, RESERVATION_SYSTEM_ROOT);
  }

  @Override
  public synchronized void startInternal() throws Exception {
    // createConnection for future API calls
    createConnection();

    // ensure root dirs exist
    createRootDirRecursively(znodeWorkingPath);
    create(zkRootNodePath);
    setRootNodeAcls();
    delete(fencingNodePath);
    if (HAUtil.isHAEnabled(getConfig())) {
      verifyActiveStatusThread = new VerifyActiveStatusThread();
      verifyActiveStatusThread.start();
    }
    create(rmAppRoot);
    create(rmDTSecretManagerRoot);
    create(dtMasterKeysRootPath);
    create(delegationTokensRootPath);
    create(dtSequenceNumberPath);
    create(amrmTokenSecretManagerRoot);
    create(reservationRoot);
  }

  private void logRootNodeAcls(String prefix) throws Exception {
    Stat getStat = new Stat();
    List<ACL> getAcls = getACL(zkRootNodePath);

    StringBuilder builder = new StringBuilder();
    builder.append(prefix);
    for (ACL acl : getAcls) {
      builder.append(acl.toString());
    }
    builder.append(getStat.toString());
    LOG.debug(builder.toString());
  }

  private void setRootNodeAcls() throws Exception {
    if (LOG.isDebugEnabled()) {
      logRootNodeAcls("Before setting ACLs'\n");
    }

    if (HAUtil.isHAEnabled(getConfig())) {
      curatorFramework.setACL().withACL(zkRootNodeAcl).forPath(zkRootNodePath);
    } else {
      curatorFramework.setACL().withACL(zkAcl).forPath(zkRootNodePath);
    }

    if (LOG.isDebugEnabled()) {
      logRootNodeAcls("After setting ACLs'\n");
    }
  }

  @Override
  protected synchronized void closeInternal() throws Exception {
    if (verifyActiveStatusThread != null) {
      verifyActiveStatusThread.interrupt();
      verifyActiveStatusThread.join(1000);
    }
    IOUtils.closeStream(curatorFramework);
  }

  @Override
  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  protected synchronized void storeVersion() throws Exception {
    String versionNodePath = getNodePath(zkRootNodePath, VERSION_NODE);
    byte[] data =
        ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();
    if (exists(versionNodePath)) {
      safeSetData(versionNodePath, data, -1);
    } else {
      safeCreate(versionNodePath, data, zkAcl, CreateMode.PERSISTENT);
    }
  }

  @Override
  protected synchronized Version loadVersion() throws Exception {
    String versionNodePath = getNodePath(zkRootNodePath, VERSION_NODE);

    if (exists(versionNodePath)) {
      byte[] data = getData(versionNodePath);
      return new VersionPBImpl(VersionProto.parseFrom(data));
    }
    return null;
  }

  @Override
  public synchronized long getAndIncrementEpoch() throws Exception {
    String epochNodePath = getNodePath(zkRootNodePath, EPOCH_NODE);
    long currentEpoch = 0;
    if (exists(epochNodePath)) {
      // load current epoch
      byte[] data = getData(epochNodePath);
      Epoch epoch = new EpochPBImpl(EpochProto.parseFrom(data));
      currentEpoch = epoch.getEpoch();
      // increment epoch and store it
      byte[] storeData = Epoch.newInstance(currentEpoch + 1).getProto()
          .toByteArray();
      safeSetData(epochNodePath, storeData, -1);
    } else {
      // initialize epoch node with 1 for the next time.
      byte[] storeData = Epoch.newInstance(currentEpoch + 1).getProto()
          .toByteArray();
      safeCreate(epochNodePath, storeData, zkAcl, CreateMode.PERSISTENT);
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
    return rmState;
  }

  private void loadReservationSystemState(RMState rmState) throws Exception {
    List<String> planNodes = getChildren(reservationRoot);
    for (String planName : planNodes) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Loading plan from znode: " + planName);
      }
      String planNodePath = getNodePath(reservationRoot, planName);

      List<String> reservationNodes = getChildren(planNodePath);
      for (String reservationNodeName : reservationNodes) {
        String reservationNodePath = getNodePath(planNodePath,
            reservationNodeName);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading reservation from znode: " + reservationNodePath);
        }
        byte[] reservationData = getData(reservationNodePath);
        ReservationAllocationStateProto allocationState =
            ReservationAllocationStateProto.parseFrom(reservationData);
        if (!rmState.getReservationState().containsKey(planName)) {
          rmState.getReservationState().put(planName,
              new HashMap<ReservationId, ReservationAllocationStateProto>());
        }
        ReservationId reservationId =
            ReservationId.parseReservationId(reservationNodeName);
        rmState.getReservationState().get(planName).put(reservationId,
            allocationState);
      }
    }
  }

  private void loadAMRMTokenSecretManagerState(RMState rmState)
      throws Exception {
    byte[] data = getData(amrmTokenSecretManagerRoot);
    if (data == null) {
      LOG.warn("There is no data saved");
      return;
    }
    AMRMTokenSecretManagerStatePBImpl stateData =
        new AMRMTokenSecretManagerStatePBImpl(
          AMRMTokenSecretManagerStateProto.parseFrom(data));
    rmState.amrmTokenSecretManagerState =
        AMRMTokenSecretManagerState.newInstance(
          stateData.getCurrentMasterKey(), stateData.getNextMasterKey());
  }

  private synchronized void loadRMDTSecretManagerState(RMState rmState)
      throws Exception {
    loadRMDelegationKeyState(rmState);
    loadRMSequentialNumberState(rmState);
    loadRMDelegationTokenState(rmState);
  }

  private void loadRMDelegationKeyState(RMState rmState) throws Exception {
    List<String> childNodes =
        getChildren(dtMasterKeysRootPath);
    for (String childNodeName : childNodes) {
      String childNodePath = getNodePath(dtMasterKeysRootPath, childNodeName);
      byte[] childData = getData(childNodePath);

      if (childData == null) {
        LOG.warn("Content of " + childNodePath + " is broken.");
        continue;
      }

      ByteArrayInputStream is = new ByteArrayInputStream(childData);
      DataInputStream fsIn = new DataInputStream(is);

      try {
        if (childNodeName.startsWith(DELEGATION_KEY_PREFIX)) {
          DelegationKey key = new DelegationKey();
          key.readFields(fsIn);
          rmState.rmSecretManagerState.masterKeyState.add(key);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Loaded delegation key: keyId=" + key.getKeyId()
                + ", expirationDate=" + key.getExpiryDate());
          }
        }
      } finally {
        is.close();
      }
    }
  }

  private void loadRMSequentialNumberState(RMState rmState) throws Exception {
    byte[] seqData = getData(dtSequenceNumberPath);
    if (seqData != null) {
      ByteArrayInputStream seqIs = new ByteArrayInputStream(seqData);
      DataInputStream seqIn = new DataInputStream(seqIs);

      try {
        rmState.rmSecretManagerState.dtSequenceNumber = seqIn.readInt();
      } finally {
        seqIn.close();
      }
    }
  }

  private void loadRMDelegationTokenState(RMState rmState) throws Exception {
    List<String> childNodes =
        getChildren(delegationTokensRootPath);
    for (String childNodeName : childNodes) {
      String childNodePath =
          getNodePath(delegationTokensRootPath, childNodeName);
      byte[] childData = getData(childNodePath);

      if (childData == null) {
        LOG.warn("Content of " + childNodePath + " is broken.");
        continue;
      }

      ByteArrayInputStream is = new ByteArrayInputStream(childData);
      DataInputStream fsIn = new DataInputStream(is);

      try {
        if (childNodeName.startsWith(DELEGATION_TOKEN_PREFIX)) {
          RMDelegationTokenIdentifierData identifierData =
              new RMDelegationTokenIdentifierData();
          identifierData.readFields(fsIn);
          RMDelegationTokenIdentifier identifier =
              identifierData.getTokenIdentifier();
          long renewDate = identifierData.getRenewDate();
          rmState.rmSecretManagerState.delegationTokenState.put(identifier,
              renewDate);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Loaded RMDelegationTokenIdentifier: " + identifier
                + " renewDate=" + renewDate);
          }
        }
      } finally {
        is.close();
      }
    }
  }

  private synchronized void loadRMAppState(RMState rmState) throws Exception {
    List<String> childNodes = getChildren(rmAppRoot);
    for (String childNodeName : childNodes) {
      String childNodePath = getNodePath(rmAppRoot, childNodeName);
      byte[] childData = getData(childNodePath);
      if (childNodeName.startsWith(ApplicationId.appIdStrPrefix)) {
        // application
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading application from znode: " + childNodeName);
        }
        ApplicationId appId = ConverterUtils.toApplicationId(childNodeName);
        ApplicationStateDataPBImpl appState =
            new ApplicationStateDataPBImpl(
                ApplicationStateDataProto.parseFrom(childData));
        if (!appId.equals(
            appState.getApplicationSubmissionContext().getApplicationId())) {
          throw new YarnRuntimeException("The child node name is different " +
              "from the application id");
        }
        rmState.appState.put(appId, appState);
        loadApplicationAttemptState(appState, appId);
      } else {
        LOG.info("Unknown child node with name: " + childNodeName);
      }
    }
  }

  private void loadApplicationAttemptState(ApplicationStateData appState,
      ApplicationId appId)
      throws Exception {
    String appPath = getNodePath(rmAppRoot, appId.toString());
    List<String> attempts = getChildren(appPath);
    for (String attemptIDStr : attempts) {
      if (attemptIDStr.startsWith(ApplicationAttemptId.appAttemptIdStrPrefix)) {
        String attemptPath = getNodePath(appPath, attemptIDStr);
        byte[] attemptData = getData(attemptPath);

        ApplicationAttemptStateDataPBImpl attemptState =
            new ApplicationAttemptStateDataPBImpl(
                ApplicationAttemptStateDataProto.parseFrom(attemptData));

        appState.attempts.put(attemptState.getAttemptId(), attemptState);
      }
    }
    LOG.debug("Done loading applications from ZK state store");
  }

  @Override
  public synchronized void storeApplicationStateInternal(ApplicationId appId,
      ApplicationStateData appStateDataPB) throws Exception {
    String nodeCreatePath = getNodePath(rmAppRoot, appId.toString());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing info for app: " + appId + " at: " + nodeCreatePath);
    }
    byte[] appStateData = appStateDataPB.getProto().toByteArray();
    safeCreate(nodeCreatePath, appStateData, zkAcl,
        CreateMode.PERSISTENT);

  }

  @Override
  public synchronized void updateApplicationStateInternal(ApplicationId appId,
      ApplicationStateData appStateDataPB) throws Exception {
    String nodeUpdatePath = getNodePath(rmAppRoot, appId.toString());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing final state info for app: " + appId + " at: "
          + nodeUpdatePath);
    }
    byte[] appStateData = appStateDataPB.getProto().toByteArray();

    if (exists(nodeUpdatePath)) {
      safeSetData(nodeUpdatePath, appStateData, -1);
    } else {
      safeCreate(nodeUpdatePath, appStateData, zkAcl,
          CreateMode.PERSISTENT);
      LOG.debug(appId + " znode didn't exist. Created a new znode to"
          + " update the application state.");
    }
  }

  @Override
  public synchronized void storeApplicationAttemptStateInternal(
      ApplicationAttemptId appAttemptId,
      ApplicationAttemptStateData attemptStateDataPB)
      throws Exception {
    String appDirPath = getNodePath(rmAppRoot,
        appAttemptId.getApplicationId().toString());
    String nodeCreatePath = getNodePath(appDirPath, appAttemptId.toString());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing info for attempt: " + appAttemptId + " at: "
          + nodeCreatePath);
    }
    byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();
    safeCreate(nodeCreatePath, attemptStateData, zkAcl,
        CreateMode.PERSISTENT);
  }

  @Override
  public synchronized void updateApplicationAttemptStateInternal(
      ApplicationAttemptId appAttemptId,
      ApplicationAttemptStateData attemptStateDataPB)
      throws Exception {
    String appIdStr = appAttemptId.getApplicationId().toString();
    String appAttemptIdStr = appAttemptId.toString();
    String appDirPath = getNodePath(rmAppRoot, appIdStr);
    String nodeUpdatePath = getNodePath(appDirPath, appAttemptIdStr);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing final state info for attempt: " + appAttemptIdStr
          + " at: " + nodeUpdatePath);
    }
    byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();

    if (exists(nodeUpdatePath)) {
      safeSetData(nodeUpdatePath, attemptStateData, -1);
    } else {
      safeCreate(nodeUpdatePath, attemptStateData, zkAcl,
          CreateMode.PERSISTENT);
      LOG.debug(appAttemptId + " znode didn't exist. Created a new znode to"
          + " update the application attempt state.");
    }
  }

  @Override
  public synchronized void removeApplicationStateInternal(
      ApplicationStateData  appState)
      throws Exception {
    String appId = appState.getApplicationSubmissionContext().getApplicationId()
        .toString();
    String appIdRemovePath = getNodePath(rmAppRoot, appId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing info for app: " + appId + " at: " + appIdRemovePath
          + " and its attempts.");
    }

    for (ApplicationAttemptId attemptId : appState.attempts.keySet()) {
      String attemptRemovePath = getNodePath(appIdRemovePath, attemptId.toString());
      safeDelete(attemptRemovePath);
    }
    safeDelete(appIdRemovePath);
  }

  @Override
  protected synchronized void storeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    SafeTransaction trx = new SafeTransaction();
    addStoreOrUpdateOps(trx, rmDTIdentifier, renewDate, false);
    trx.commit();
  }

  @Override
  protected synchronized void removeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier) throws Exception {
    String nodeRemovePath =
        getNodePath(delegationTokensRootPath, DELEGATION_TOKEN_PREFIX
            + rmDTIdentifier.getSequenceNumber());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing RMDelegationToken_"
          + rmDTIdentifier.getSequenceNumber());
    }
    safeDelete(nodeRemovePath);
  }

  @Override
  protected synchronized void updateRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    SafeTransaction trx = new SafeTransaction();
    String nodeRemovePath =
        getNodePath(delegationTokensRootPath, DELEGATION_TOKEN_PREFIX
            + rmDTIdentifier.getSequenceNumber());
    if (exists(nodeRemovePath)) {
      // in case znode exists
      addStoreOrUpdateOps(trx, rmDTIdentifier, renewDate, true);
    } else {
      // in case znode doesn't exist
      addStoreOrUpdateOps(trx, rmDTIdentifier, renewDate, false);
      LOG.debug("Attempted to update a non-existing znode " + nodeRemovePath);
    }
    trx.commit();
  }

  private void addStoreOrUpdateOps(SafeTransaction trx,
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      boolean isUpdate) throws Exception {
    // store RM delegation token
    String nodeCreatePath =
        getNodePath(delegationTokensRootPath, DELEGATION_TOKEN_PREFIX
            + rmDTIdentifier.getSequenceNumber());
    ByteArrayOutputStream seqOs = new ByteArrayOutputStream();
    DataOutputStream seqOut = new DataOutputStream(seqOs);
    RMDelegationTokenIdentifierData identifierData =
        new RMDelegationTokenIdentifierData(rmDTIdentifier, renewDate);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug((isUpdate ? "Storing " : "Updating ") + "RMDelegationToken_" +
            rmDTIdentifier.getSequenceNumber());
      }

      if (isUpdate) {
        trx.setData(nodeCreatePath, identifierData.toByteArray(), -1);
      } else {
        trx.create(nodeCreatePath, identifierData.toByteArray(), zkAcl,
            CreateMode.PERSISTENT);
        // Update Sequence number only while storing DT
        seqOut.writeInt(rmDTIdentifier.getSequenceNumber());
        if (LOG.isDebugEnabled()) {
          LOG.debug((isUpdate ? "Storing " : "Updating ") +
              dtSequenceNumberPath + ". SequenceNumber: "
              + rmDTIdentifier.getSequenceNumber());
        }
        trx.setData(dtSequenceNumberPath, seqOs.toByteArray(), -1);
      }
    } finally {
      seqOs.close();
    }
  }

  @Override
  protected synchronized void storeRMDTMasterKeyState(
      DelegationKey delegationKey) throws Exception {
    String nodeCreatePath =
        getNodePath(dtMasterKeysRootPath, DELEGATION_KEY_PREFIX
            + delegationKey.getKeyId());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream fsOut = new DataOutputStream(os);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing RMDelegationKey_" + delegationKey.getKeyId());
    }
    delegationKey.write(fsOut);
    try {
      safeCreate(nodeCreatePath, os.toByteArray(), zkAcl,
          CreateMode.PERSISTENT);
    } finally {
      os.close();
    }
  }

  @Override
  protected synchronized void removeRMDTMasterKeyState(
      DelegationKey delegationKey) throws Exception {
    String nodeRemovePath =
        getNodePath(dtMasterKeysRootPath, DELEGATION_KEY_PREFIX
            + delegationKey.getKeyId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing RMDelegationKey_" + delegationKey.getKeyId());
    }
    safeDelete(nodeRemovePath);
  }

  @Override
  public synchronized void deleteStore() throws Exception {
    delete(zkRootNodePath);
  }

  @Override
  public synchronized void removeApplication(ApplicationId removeAppId)
      throws Exception {
    String appIdRemovePath = getNodePath(rmAppRoot, removeAppId.toString());
    delete(appIdRemovePath);
  }

  @VisibleForTesting
  String getNodePath(String root, String nodeName) {
    return (root + "/" + nodeName);
  }

  @Override
  public synchronized void storeOrUpdateAMRMTokenSecretManagerState(
      AMRMTokenSecretManagerState amrmTokenSecretManagerState, boolean isUpdate)
      throws Exception {
    AMRMTokenSecretManagerState data =
        AMRMTokenSecretManagerState.newInstance(amrmTokenSecretManagerState);
    byte[] stateData = data.getProto().toByteArray();
    safeSetData(amrmTokenSecretManagerRoot, stateData, -1);
  }

  @Override
  protected synchronized void removeReservationState(String planName,
      String reservationIdName)
      throws Exception {
    String planNodePath =
        getNodePath(reservationRoot, planName);
    String reservationPath = getNodePath(planNodePath,
        reservationIdName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing reservationallocation " + reservationIdName + " for" +
          " plan " + planName);
    }
    safeDelete(reservationPath);

    List<String> reservationNodes = getChildren(planNodePath);
    if (reservationNodes.isEmpty()) {
      safeDelete(planNodePath);
    }
  }

  @Override
  protected synchronized void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName)
      throws Exception {
    SafeTransaction trx = new SafeTransaction();
    addOrUpdateReservationState(
        reservationAllocation, planName, reservationIdName, trx, false);
    trx.commit();
  }

  @Override
  protected synchronized void updateReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName)
      throws Exception {
    SafeTransaction trx = new SafeTransaction();
    addOrUpdateReservationState(
        reservationAllocation, planName, reservationIdName, trx, true);
    trx.commit();
  }

  private void addOrUpdateReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName, SafeTransaction trx, boolean isUpdate)
      throws Exception {
    String planCreatePath =
        getNodePath(reservationRoot, planName);
    String reservationPath = getNodePath(planCreatePath,
        reservationIdName);
    byte[] reservationData = reservationAllocation.toByteArray();

    if (!exists(planCreatePath)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating plan node: " + planName + " at: " + planCreatePath);
      }
      trx.create(planCreatePath, null, zkAcl, CreateMode.PERSISTENT);
    }

    if (isUpdate) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating reservation: " + reservationIdName + " in plan:"
            + planName + " at: " + reservationPath);
      }
      trx.setData(reservationPath, reservationData, -1);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing reservation: " + reservationIdName + " in plan:"
            + planName + " at: " + reservationPath);
      }
      trx.create(reservationPath, reservationData, zkAcl,
          CreateMode.PERSISTENT);
    }
  }

  /**
   * Utility function to ensure that the configured base znode exists.
   * This recursively creates the znode as well as all of its parents.
   */
  private void createRootDirRecursively(String path) throws Exception {
    String pathParts[] = path.split("/");
    Preconditions.checkArgument(pathParts.length >= 1 && pathParts[0].isEmpty(),
        "Invalid path: %s", path);
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < pathParts.length; i++) {
      sb.append("/").append(pathParts[i]);
      create(sb.toString());
    }
  }

  /*
   * ZK operations using curator
   */
  private void createConnection() throws Exception {
    // Curator connection
    CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
    builder = builder.connectString(zkHostPort)
        .connectionTimeoutMs(zkSessionTimeout)
        .retryPolicy(new RetryNTimes(numRetries, zkRetryInterval));

    // Set up authorization based on fencing scheme
    List<AuthInfo> authInfos = new ArrayList<>();
    for (ZKUtil.ZKAuthInfo zkAuth : zkAuths) {
      authInfos.add(new AuthInfo(zkAuth.getScheme(), zkAuth.getAuth()));
    }
    if (useDefaultFencingScheme) {
      byte[] defaultFencingAuth =
          (zkRootNodeUsername + ":" + zkRootNodePassword).getBytes(
              Charset.forName("UTF-8"));
      authInfos.add(new AuthInfo(zkRootNodeAuthScheme, defaultFencingAuth));
    }
    builder = builder.authorization(authInfos);

    // Connect to ZK
    curatorFramework = builder.build();
    curatorFramework.start();
  }

  @VisibleForTesting
  byte[] getData(final String path) throws Exception {
    return curatorFramework.getData().forPath(path);
  }

  @VisibleForTesting
  List<ACL> getACL(final String path) throws Exception {
    return curatorFramework.getACL().forPath(path);
  }

  private List<String> getChildren(final String path) throws Exception {
    return curatorFramework.getChildren().forPath(path);
  }

  private boolean exists(final String path) throws Exception {
    return curatorFramework.checkExists().forPath(path) != null;
  }

  @VisibleForTesting
  void create(final String path) throws Exception {
    if (!exists(path)) {
      curatorFramework.create()
          .withMode(CreateMode.PERSISTENT).withACL(zkAcl)
          .forPath(path, null);
    }
  }

  @VisibleForTesting
  void delete(final String path) throws Exception {
    if (exists(path)) {
      curatorFramework.delete().deletingChildrenIfNeeded().forPath(path);
    }
  }

  private void safeCreate(String path, byte[] data, List<ACL> acl,
      CreateMode mode) throws Exception {
    if (!exists(path)) {
      SafeTransaction transaction = new SafeTransaction();
      transaction.create(path, data, acl, mode);
      transaction.commit();
    }
  }

  private void safeDelete(final String path) throws Exception {
    if (exists(path)) {
      SafeTransaction transaction = new SafeTransaction();
      transaction.delete(path);
      transaction.commit();
    }
  }

  private void safeSetData(String path, byte[] data, int version)
      throws Exception {
    SafeTransaction transaction = new SafeTransaction();
    transaction.setData(path, data, version);
    transaction.commit();
  }

  /**
   * Use curator transactions to ensure zk-operations are performed in an all
   * or nothing fashion. This is equivalent to using ZooKeeper#multi.
   *
   * TODO (YARN-3774): Curator 3.0 introduces CuratorOp similar to Op. We ll
   * have to rewrite this inner class when we adopt that.
   */
  private class SafeTransaction {
    private CuratorTransactionFinal transactionFinal;

    SafeTransaction() throws Exception {
      CuratorTransaction transaction = curatorFramework.inTransaction();
      transactionFinal =
          transaction.create()
              .withMode(CreateMode.PERSISTENT).withACL(zkAcl)
              .forPath(fencingNodePath, new byte[0]).and();
    }

    public void commit() throws Exception {
      transactionFinal = transactionFinal.delete()
          .forPath(fencingNodePath).and();
      transactionFinal.commit();
    }

    public void create(String path, byte[] data, List<ACL> acl, CreateMode mode)
        throws Exception {
      transactionFinal = transactionFinal.create()
          .withMode(mode).withACL(acl).forPath(path, data).and();
    }

    public void delete(String path) throws Exception {
      transactionFinal = transactionFinal.delete().forPath(path).and();
    }

    public void setData(String path, byte[] data, int version)
        throws Exception {
      transactionFinal = transactionFinal.setData()
          .withVersion(version).forPath(path, data).and();
    }
  }

  /**
   * Helper class that periodically attempts creating a znode to ensure that
   * this RM continues to be the Active.
   */
  private class VerifyActiveStatusThread extends Thread {
    VerifyActiveStatusThread() {
      super(VerifyActiveStatusThread.class.getName());
    }

    public void run() {
      try {
        while (true) {
          if(isFencedState()) {
            break;
          }
          // Create and delete fencing node
          new SafeTransaction().commit();
          Thread.sleep(zkSessionTimeout);
        }
      } catch (InterruptedException ie) {
        LOG.info(VerifyActiveStatusThread.class.getName() + " thread " +
            "interrupted! Exiting!");
      } catch (Exception e) {
        notifyStoreOperationFailed(new StoreFencedException());
      }
    }
  }
}
