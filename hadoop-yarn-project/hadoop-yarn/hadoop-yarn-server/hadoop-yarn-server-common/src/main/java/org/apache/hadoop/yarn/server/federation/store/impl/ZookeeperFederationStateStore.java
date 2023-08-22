/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.impl;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.Comparator;
import java.util.stream.Collectors;

import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterPolicyConfigurationProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterIdPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterInfoPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterPolicyConfigurationPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.ApplicationHomeSubClusterPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationApplicationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationMembershipStateStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationPolicyStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationReservationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationRouterRMTokenInputValidator;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;

import static org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils.filterHomeSubCluster;
import static org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE;
import static org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.util.curator.ZKCuratorManager.getNodePath;

/**
 * ZooKeeper implementation of {@link FederationStateStore}.
 * The znode structure is as follows:
 *
 * ROOT_DIR_PATH
 * |--- MEMBERSHIP
 * |     |----- SC1
 * |     |----- SC2
 * |--- APPLICATION
 * |     |----- APP1
 * |     |----- APP2
 * |--- POLICY
 * |     |----- QUEUE1
 * |     |----- QUEUE1
 * |--- RESERVATION
 * |     |----- RESERVATION1
 * |     |----- RESERVATION2
 * |--- ROUTER_RM_DT_SECRET_MANAGER_ROOT
 * |     |----- ROUTER_RM_DELEGATION_TOKENS_ROOT
 * |     |       |----- RM_DELEGATION_TOKEN_1
 * |     |       |----- RM_DELEGATION_TOKEN_2
 * |     |       |----- RM_DELEGATION_TOKEN_3
 * |     |----- ROUTER_RM_DT_MASTER_KEYS_ROOT
 * |     |       |----- DELEGATION_KEY_1
 * |     |----- ROUTER_RM_DT_SEQUENTIAL_NUMBER
 */
public class ZookeeperFederationStateStore implements FederationStateStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZookeeperFederationStateStore.class);

  private final static String ROOT_ZNODE_NAME_MEMBERSHIP = "memberships";
  private final static String ROOT_ZNODE_NAME_APPLICATION = "applications";
  private final static String ROOT_ZNODE_NAME_POLICY = "policies";
  private final static String ROOT_ZNODE_NAME_RESERVATION = "reservation";

  protected static final String ROOT_ZNODE_NAME_VERSION = "version";

  /** Store Delegation Token Node. */
  private final static String ROUTER_RM_DT_SECRET_MANAGER_ROOT = "router_rm_dt_secret_manager_root";
  private static final String ROUTER_RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME =
      "router_rm_dt_master_keys_root";
  private static final String ROUTER_RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME =
      "router_rm_delegation_tokens_root";
  private static final String ROUTER_RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME =
      "router_rm_dt_sequential_number";
  private static final String ROUTER_RM_DT_MASTER_KEY_ID_ZNODE_NAME =
      "router_rm_dt_master_key_id";
  private static final String ROUTER_RM_DELEGATION_KEY_PREFIX = "delegation_key_";
  private static final String ROUTER_RM_DELEGATION_TOKEN_PREFIX = "rm_delegation_token_";

  /** Interface to Zookeeper. */
  private ZKCuratorManager zkManager;

  /** Store sequenceNum. **/
  private int seqNumBatchSize;
  private int currentSeqNum;
  private int currentMaxSeqNum;
  private SharedCount delTokSeqCounter;
  private SharedCount keyIdSeqCounter;

  /** Directory to store the state store data. */
  private String baseZNode;

  private String appsZNode;
  private String membershipZNode;
  private String policiesZNode;
  private String reservationsZNode;
  private String versionNode;
  private int maxAppsInStateStore;

  /** Directory to store the delegation token data. **/
  private String routerRMDTSecretManagerRoot;
  private String routerRMDTMasterKeysRootPath;
  private String routerRMDelegationTokensRootPath;
  private String routerRMSequenceNumberPath;
  private String routerRMMasterKeyIdPath;

  private volatile Clock clock = SystemClock.getInstance();

  protected static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 1);

  @VisibleForTesting
  private ZKFederationStateStoreOpDurations opDurations =
      ZKFederationStateStoreOpDurations.getInstance();

  @Override
  public void init(Configuration conf) throws YarnException {

    LOG.info("Initializing ZooKeeper connection");

    maxAppsInStateStore = conf.getInt(
       YarnConfiguration.FEDERATION_STATESTORE_MAX_APPLICATIONS,
       YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_MAX_APPLICATIONS);

    baseZNode = conf.get(
        YarnConfiguration.FEDERATION_STATESTORE_ZK_PARENT_PATH,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_ZK_PARENT_PATH);
    try {
      this.zkManager = new ZKCuratorManager(conf);
      this.zkManager.start();
    } catch (IOException e) {
      LOG.error("Cannot initialize the ZK connection", e);
    }

    // Base znodes
    membershipZNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_MEMBERSHIP);
    appsZNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_APPLICATION);
    policiesZNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_POLICY);
    reservationsZNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_RESERVATION);
    versionNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_VERSION);

    // delegation token znodes
    routerRMDTSecretManagerRoot = getNodePath(baseZNode, ROUTER_RM_DT_SECRET_MANAGER_ROOT);
    routerRMDTMasterKeysRootPath = getNodePath(routerRMDTSecretManagerRoot,
        ROUTER_RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME);
    routerRMDelegationTokensRootPath = getNodePath(routerRMDTSecretManagerRoot,
        ROUTER_RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME);
    routerRMSequenceNumberPath = getNodePath(routerRMDTSecretManagerRoot,
        ROUTER_RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME);
    routerRMMasterKeyIdPath = getNodePath(routerRMDTSecretManagerRoot,
        ROUTER_RM_DT_MASTER_KEY_ID_ZNODE_NAME);

    // Create base znode for each entity
    try {
      List<ACL> zkAcl = ZKCuratorManager.getZKAcls(conf);
      zkManager.createRootDirRecursively(membershipZNode, zkAcl);
      zkManager.createRootDirRecursively(appsZNode, zkAcl);
      zkManager.createRootDirRecursively(policiesZNode, zkAcl);
      zkManager.createRootDirRecursively(reservationsZNode, zkAcl);
      zkManager.createRootDirRecursively(routerRMDTSecretManagerRoot, zkAcl);
      zkManager.createRootDirRecursively(routerRMDTMasterKeysRootPath, zkAcl);
      zkManager.createRootDirRecursively(routerRMDelegationTokensRootPath, zkAcl);
      zkManager.createRootDirRecursively(versionNode, zkAcl);
    } catch (Exception e) {
      String errMsg = "Cannot create base directories: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    // Distributed sequenceNum.
    try {
      seqNumBatchSize = conf.getInt(ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE,
          ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE_DEFAULT);

      delTokSeqCounter = new SharedCount(zkManager.getCurator(), routerRMSequenceNumberPath, 0);

      if (delTokSeqCounter != null) {
        delTokSeqCounter.start();
      }

      // the first batch range should be allocated during this starting window
      // by calling the incrSharedCount
      currentSeqNum = incrSharedCount(delTokSeqCounter, seqNumBatchSize);
      currentMaxSeqNum = currentSeqNum + seqNumBatchSize;

      LOG.info("Fetched initial range of seq num, from {} to {} ",
          currentSeqNum + 1, currentMaxSeqNum);
    } catch (Exception e) {
      throw new YarnException("Could not start Sequence Counter.", e);
    }

    // Distributed masterKeyId.
    try {
      keyIdSeqCounter = new SharedCount(zkManager.getCurator(), routerRMMasterKeyIdPath, 0);
      if (keyIdSeqCounter != null) {
        keyIdSeqCounter.start();
      }
    } catch (Exception e) {
      throw new YarnException("Could not start Master KeyId Counter", e);
    }
  }

  @Override
  public void close() throws Exception {

    try {
      if (delTokSeqCounter != null) {
        delTokSeqCounter.close();
        delTokSeqCounter = null;
      }
    } catch (Exception e) {
      LOG.error("Could not Stop Delegation Token Counter.", e);
    }

    try {
      if (keyIdSeqCounter != null) {
        keyIdSeqCounter.close();
        keyIdSeqCounter = null;
      }
    } catch (Exception e) {
      LOG.error("Could not stop Master KeyId Counter.", e);
    }

    if (zkManager != null) {
      zkManager.close();
    }
  }

  @Override
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException {

    long start = clock.getTime();
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    // parse parameters
    // We need to get applicationId, subClusterId, appSubmissionContext 3 parameters.
    ApplicationHomeSubCluster requestApplicationHomeSubCluster =
        request.getApplicationHomeSubCluster();
    ApplicationId requestApplicationId = requestApplicationHomeSubCluster.getApplicationId();
    SubClusterId requestSubClusterId = requestApplicationHomeSubCluster.getHomeSubCluster();
    ApplicationSubmissionContext requestApplicationSubmissionContext =
         requestApplicationHomeSubCluster.getApplicationSubmissionContext();

    LOG.debug("applicationId = {}, subClusterId = {}, appSubmissionContext = {}.",
        requestApplicationId, requestSubClusterId, requestApplicationSubmissionContext);

    // Try to write the subcluster
    try {
      storeOrUpdateApplicationHomeSubCluster(requestApplicationId,
          requestApplicationHomeSubCluster, false);
    } catch (Exception e) {
      String errMsg = "Cannot add application home subcluster for " + requestApplicationId;
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    // Check for the actual subcluster
    try {
      // We try to get the ApplicationHomeSubCluster actually stored in ZK
      // according to the applicationId.
      ApplicationHomeSubCluster actualAppHomeSubCluster =
          getApplicationHomeSubCluster(requestApplicationId);
      SubClusterId responseSubClusterId = actualAppHomeSubCluster.getHomeSubCluster();
      long end = clock.getTime();
      opDurations.addAppHomeSubClusterDuration(start, end);
      return AddApplicationHomeSubClusterResponse.newInstance(responseSubClusterId);
    } catch (Exception e) {
      String errMsg = "Cannot check app home subcluster for " + requestApplicationId;
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    // Throw YarnException.
    throw new YarnException("Cannot addApplicationHomeSubCluster by request");
  }

  @Override
  public UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubCluster(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException {

    long start = clock.getTime();
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationHomeSubCluster requestApplicationHomeSubCluster =
        request.getApplicationHomeSubCluster();
    ApplicationId requestApplicationId = requestApplicationHomeSubCluster.getApplicationId();
    ApplicationHomeSubCluster zkStoreApplicationHomeSubCluster =
        getApplicationHomeSubCluster(requestApplicationId);

    if (zkStoreApplicationHomeSubCluster == null) {
      String errMsg = "Application " + requestApplicationId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    SubClusterId oldSubClusterId = zkStoreApplicationHomeSubCluster.getHomeSubCluster();
    SubClusterId newSubClusterId = requestApplicationHomeSubCluster.getHomeSubCluster();
    ApplicationSubmissionContext requestApplicationSubmissionContext =
        requestApplicationHomeSubCluster.getApplicationSubmissionContext();

    LOG.debug("applicationId = {}, oldHomeSubClusterId = {}, newHomeSubClusterId = {}, " +
        "appSubmissionContext = {}.", requestApplicationId, oldSubClusterId, newSubClusterId,
        requestApplicationSubmissionContext);

    // update stored ApplicationHomeSubCluster
    storeOrUpdateApplicationHomeSubCluster(requestApplicationId,
        requestApplicationHomeSubCluster, true);

    long end = clock.getTime();
    opDurations.addUpdateAppHomeSubClusterDuration(start, end);
    return UpdateApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException {

    long start = clock.getTime();
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationId requestApplicationId = request.getApplicationId();

    ApplicationHomeSubCluster zkStoreApplicationHomeSubCluster =
        getApplicationHomeSubCluster(requestApplicationId);
    if (zkStoreApplicationHomeSubCluster == null) {
      String errMsg = "Application " + requestApplicationId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    // Prepare to return data
    SubClusterId subClusterId = zkStoreApplicationHomeSubCluster.getHomeSubCluster();
    long createTime = zkStoreApplicationHomeSubCluster.getCreateTime();

    long end = clock.getTime();
    opDurations.addGetAppHomeSubClusterDuration(start, end);

    // If the request asks for an ApplicationSubmissionContext to be returned,
    // we will return
    if (request.getContainsAppSubmissionContext()) {
      ApplicationSubmissionContext submissionContext =
          zkStoreApplicationHomeSubCluster.getApplicationSubmissionContext();
      return GetApplicationHomeSubClusterResponse.newInstance(
          requestApplicationId, subClusterId, createTime, submissionContext);
    }

    return GetApplicationHomeSubClusterResponse.newInstance(requestApplicationId,
        subClusterId, createTime);
  }

  @Override
  public GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException {

    if (request == null) {
      throw new YarnException("Missing getApplicationsHomeSubCluster request");
    }

    try {
      long start = clock.getTime();
      SubClusterId requestSC = request.getSubClusterId();
      List<String> children = zkManager.getChildren(appsZNode);
      List<ApplicationHomeSubCluster> result = children.stream()
          .map(child -> generateAppHomeSC(child))
          .sorted(Comparator.comparing(ApplicationHomeSubCluster::getCreateTime).reversed())
          .filter(appHomeSC -> filterHomeSubCluster(requestSC, appHomeSC.getHomeSubCluster()))
          .limit(maxAppsInStateStore)
          .collect(Collectors.toList());
      long end = clock.getTime();
      opDurations.addGetAppsHomeSubClusterDuration(start, end);
      LOG.info("filterSubClusterId = {}, appCount = {}.", requestSC, result.size());
      return GetApplicationsHomeSubClusterResponse.newInstance(result);
    } catch (Exception e) {
      String errMsg = "Cannot get apps: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    throw new YarnException("Cannot get app by request");
  }

  private ApplicationHomeSubCluster generateAppHomeSC(String appId) {
    try {
      // Parse ApplicationHomeSubCluster
      ApplicationId applicationId = ApplicationId.fromString(appId);
      ApplicationHomeSubCluster zkStoreApplicationHomeSubCluster =
          getApplicationHomeSubCluster(applicationId);

      // Prepare to return data
      SubClusterId subClusterId = zkStoreApplicationHomeSubCluster.getHomeSubCluster();
      ApplicationHomeSubCluster resultApplicationHomeSubCluster =
          ApplicationHomeSubCluster.newInstance(applicationId, subClusterId);
      return resultApplicationHomeSubCluster;
    } catch (Exception ex) {
      LOG.error("get homeSubCluster by appId = {}.", appId, ex);
    }
    return null;
  }

  @Override
  public DeleteApplicationHomeSubClusterResponse
      deleteApplicationHomeSubCluster(
          DeleteApplicationHomeSubClusterRequest request)
              throws YarnException {
    long start = clock.getTime();
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationId appId = request.getApplicationId();
    String appZNode = getNodePath(appsZNode, appId.toString());

    boolean exists = false;
    try {
      exists = zkManager.exists(appZNode);
    } catch (Exception e) {
      String errMsg = "Cannot check app: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    if (!exists) {
      String errMsg = "Application " + appId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    try {
      zkManager.delete(appZNode);
    } catch (Exception e) {
      String errMsg = "Cannot delete app: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    long end = clock.getTime();
    opDurations.addDeleteAppHomeSubClusterDuration(start, end);
    return DeleteApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest request) throws YarnException {
    long start = clock.getTime();
    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterInfo subClusterInfo = request.getSubClusterInfo();
    SubClusterId subclusterId = subClusterInfo.getSubClusterId();

    // Update the heartbeat time
    long currentTime = getCurrentTime();
    subClusterInfo.setLastHeartBeat(currentTime);

    try {
      putSubclusterInfo(subclusterId, subClusterInfo, true);
    } catch (Exception e) {
      String errMsg = "Cannot register subcluster: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    long end = clock.getTime();
    opDurations.addRegisterSubClusterDuration(start, end);
    return SubClusterRegisterResponse.newInstance();
  }

  @Override
  public SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest request) throws YarnException {
    long start = clock.getTime();
    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterId subClusterId = request.getSubClusterId();
    SubClusterState state = request.getState();

    // Get the current information and update it
    SubClusterInfo subClusterInfo = getSubclusterInfo(subClusterId);
    if (subClusterInfo == null) {
      String errMsg = "SubCluster " + subClusterId + " not found";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    } else {
      subClusterInfo.setState(state);
      putSubclusterInfo(subClusterId, subClusterInfo, true);
    }
    long end = clock.getTime();
    opDurations.addDeregisterSubClusterDuration(start, end);
    return SubClusterDeregisterResponse.newInstance();
  }

  @Override
  public SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest request) throws YarnException {
    long start = clock.getTime();
    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterId subClusterId = request.getSubClusterId();

    SubClusterInfo subClusterInfo = getSubclusterInfo(subClusterId);
    if (subClusterInfo == null) {
      String errMsg = "SubCluster " + subClusterId
          + " does not exist; cannot heartbeat";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    long currentTime = getCurrentTime();
    subClusterInfo.setLastHeartBeat(currentTime);
    subClusterInfo.setState(request.getState());
    subClusterInfo.setCapability(request.getCapability());

    putSubclusterInfo(subClusterId, subClusterInfo, true);
    long end = clock.getTime();
    opDurations.addSubClusterHeartbeatDuration(start, end);
    return SubClusterHeartbeatResponse.newInstance();
  }

  @Override
  public GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest request) throws YarnException {
    long start = clock.getTime();
    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterId subClusterId = request.getSubClusterId();
    SubClusterInfo subClusterInfo = null;
    try {
      subClusterInfo = getSubclusterInfo(subClusterId);
      if (subClusterInfo == null) {
        LOG.warn("The queried SubCluster: {} does not exist.", subClusterId);
        return null;
      }
    } catch (Exception e) {
      String errMsg = "Cannot get subcluster: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    long end = clock.getTime();
    opDurations.addGetSubClusterDuration(start, end);
    return GetSubClusterInfoResponse.newInstance(subClusterInfo);
  }

  @Override
  public GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest request) throws YarnException {
    long start = clock.getTime();
    List<SubClusterInfo> result = new ArrayList<>();

    try {
      for (String child : zkManager.getChildren(membershipZNode)) {
        SubClusterId subClusterId = SubClusterId.newInstance(child);
        SubClusterInfo info = getSubclusterInfo(subClusterId);
        if (!request.getFilterInactiveSubClusters() ||
            info.getState().isActive()) {
          result.add(info);
        }
      }
    } catch (Exception e) {
      String errMsg = "Cannot get subclusters: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    long end = clock.getTime();
    opDurations.addGetSubClustersDuration(start, end);
    return GetSubClustersInfoResponse.newInstance(result);
  }


  @Override
  public GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException {
    long start = clock.getTime();
    FederationPolicyStoreInputValidator.validate(request);
    String queue = request.getQueue();
    SubClusterPolicyConfiguration policy = null;
    try {
      policy = getPolicy(queue);
    } catch (Exception e) {
      String errMsg = "Cannot get policy: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    if (policy == null) {
      LOG.warn("Policy for queue: {} does not exist.", queue);
      return null;
    }
    long end = clock.getTime();
    opDurations.addGetPolicyConfigurationDuration(start, end);
    return GetSubClusterPolicyConfigurationResponse
        .newInstance(policy);
  }

  @Override
  public SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException {
    long start = clock.getTime();
    FederationPolicyStoreInputValidator.validate(request);
    SubClusterPolicyConfiguration policy =
        request.getPolicyConfiguration();
    try {
      String queue = policy.getQueue();
      putPolicy(queue, policy, true);
    } catch (Exception e) {
      String errMsg = "Cannot set policy: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    long end = clock.getTime();
    opDurations.addSetPolicyConfigurationDuration(start, end);
    return SetSubClusterPolicyConfigurationResponse.newInstance();
  }

  @Override
  public GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException {
    long start = clock.getTime();
    List<SubClusterPolicyConfiguration> result = new ArrayList<>();

    try {
      for (String child : zkManager.getChildren(policiesZNode)) {
        SubClusterPolicyConfiguration policy = getPolicy(child);
        if (policy == null) {
          LOG.warn("Policy for queue: {} does not exist.", child);
          continue;
        }
        result.add(policy);
      }
    } catch (Exception e) {
      String errMsg = "Cannot get policies: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    long end = clock.getTime();
    opDurations.addGetPoliciesConfigurationsDuration(start, end);
    return GetSubClusterPoliciesConfigurationsResponse.newInstance(result);
  }

  @Override
  public Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  public Version loadVersion() throws Exception {
    if (exists(versionNode)) {
      byte[] data = get(versionNode);
      if (data != null) {
        return new VersionPBImpl(VersionProto.parseFrom(data));
      }
    }
    return null;
  }

  @Override
  public void storeVersion() throws Exception {
    byte[] data = ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();
    boolean isUpdate = exists(versionNode);
    put(versionNode, data, isUpdate);
  }

  /**
   * Get the subcluster for an application.
   *
   * @param appId Application identifier.
   * @return ApplicationHomeSubCluster identifier.
   * @throws Exception If it cannot contact ZooKeeper.
   */
  private ApplicationHomeSubCluster getApplicationHomeSubCluster(
      final ApplicationId appId) throws YarnException {
    String appZNode = getNodePath(appsZNode, appId.toString());

    ApplicationHomeSubCluster appHomeSubCluster = null;
    byte[] data = get(appZNode);
    if (data != null) {
      try {
        appHomeSubCluster = new ApplicationHomeSubClusterPBImpl(
            ApplicationHomeSubClusterProto.parseFrom(data));
      } catch (InvalidProtocolBufferException e) {
        String errMsg = "Cannot parse application at " + appZNode;
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
    }
    return appHomeSubCluster;
  }

  /**
   * We will store the data of ApplicationHomeSubCluster according to appId.
   *
   * @param applicationId ApplicationId.
   * @param applicationHomeSubCluster ApplicationHomeSubCluster.
   * @param update false, add records; true, update records.
   * @throws Exception If it cannot contact ZooKeeper.
   */
  private void storeOrUpdateApplicationHomeSubCluster(final ApplicationId applicationId,
      final ApplicationHomeSubCluster applicationHomeSubCluster, boolean update)
      throws YarnException {
    String appZNode = getNodePath(appsZNode, applicationId.toString());
    ApplicationHomeSubClusterProto proto =
        ((ApplicationHomeSubClusterPBImpl) applicationHomeSubCluster).getProto();
    byte[] data = proto.toByteArray();
    put(appZNode, data, update);
  }

  /**
   * Get the current information for a subcluster from Zookeeper.
   * @param subclusterId Subcluster identifier.
   * @return Subcluster information or null if it doesn't exist.
   * @throws Exception If it cannot contact ZooKeeper.
   */
  private SubClusterInfo getSubclusterInfo(final SubClusterId subclusterId)
      throws YarnException {
    String memberZNode = getNodePath(membershipZNode, subclusterId.toString());

    SubClusterInfo policy = null;
    byte[] data = get(memberZNode);
    if (data != null) {
      try {
        policy = new SubClusterInfoPBImpl(
            SubClusterInfoProto.parseFrom(data));
      } catch (InvalidProtocolBufferException e) {
        String errMsg = "Cannot parse subcluster info at " + memberZNode;
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
    }
    return policy;
  }

  /**
   * Put the subcluster information in Zookeeper.
   * @param subclusterId Subcluster identifier.
   * @param subClusterInfo Subcluster information.
   * @throws Exception If it cannot contact ZooKeeper.
   */
  private void putSubclusterInfo(final SubClusterId subclusterId,
      final SubClusterInfo subClusterInfo, final boolean update)
          throws YarnException {
    String memberZNode = getNodePath(membershipZNode, subclusterId.toString());
    SubClusterInfoProto proto =
        ((SubClusterInfoPBImpl)subClusterInfo).getProto();
    byte[] data = proto.toByteArray();
    put(memberZNode, data, update);
  }

  /**
   * Get the queue policy from Zookeeper.
   * @param queue Name of the queue.
   * @return Subcluster policy configuration.
   * @throws YarnException If it cannot contact ZooKeeper.
   */
  private SubClusterPolicyConfiguration getPolicy(final String queue)
      throws YarnException {
    String policyZNode = getNodePath(policiesZNode, queue);

    SubClusterPolicyConfiguration policy = null;
    byte[] data = get(policyZNode);
    if (data != null) {
      try {
        policy = new SubClusterPolicyConfigurationPBImpl(
            SubClusterPolicyConfigurationProto.parseFrom(data));
      } catch (InvalidProtocolBufferException e) {
        String errMsg = "Cannot parse policy at " + policyZNode;
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
    }
    return policy;
  }

  /**
   * Put the subcluster information in Zookeeper.
   * @param queue Name of the queue.
   * @param policy Subcluster policy configuration.
   * @throws YarnException If it cannot contact ZooKeeper.
   */
  private void putPolicy(final String queue,
      final SubClusterPolicyConfiguration policy, boolean update)
          throws YarnException {
    String policyZNode = getNodePath(policiesZNode, queue);

    SubClusterPolicyConfigurationProto proto =
        ((SubClusterPolicyConfigurationPBImpl)policy).getProto();
    byte[] data = proto.toByteArray();
    put(policyZNode, data, update);
  }

  /**
   * Get data from a znode in Zookeeper.
   * @param znode Path of the znode.
   * @return Data in the znode.
   * @throws YarnException If it cannot contact ZooKeeper.
   */
  private byte[] get(String znode) throws YarnException {
    boolean exists = false;
    try {
      exists = zkManager.exists(znode);
    } catch (Exception e) {
      String errMsg = "Cannot find znode " + znode;
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    if (!exists) {
      LOG.error("{} does not exist", znode);
      return null;
    }

    byte[] data = null;
    try {
      data = zkManager.getData(znode);
    } catch (Exception e) {
      String errMsg = "Cannot get data from znode " + znode
          + ": " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    return data;
  }

  /**
   * Put data into a znode in Zookeeper.
   * @param znode Path of the znode.
   * @param data Data to write.
   * @throws YarnException If it cannot contact ZooKeeper.
   */
  private void put(String znode, byte[] data, boolean update)
      throws YarnException {
    // Create the znode
    boolean created = false;
    try {
      created = zkManager.create(znode);
    } catch (Exception e) {
      String errMsg = "Cannot create znode " + znode + ": " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    if (!created) {
      LOG.debug("{} not created", znode);
      if (!update) {
        LOG.info("{} already existed and we are not updating", znode);
        return;
      }
    }

    // Write the data into the znode
    try {
      zkManager.setData(znode, data, -1);
    } catch (Exception e) {
      String errMsg = "Cannot write data into znode " + znode
          + ": " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
  }

  /**
   * Get the current time.
   * @return Current time in milliseconds.
   */
  private static long getCurrentTime() {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    return cal.getTimeInMillis();
  }

  private void putReservation(final ReservationId reservationId,
      final SubClusterId subClusterId, boolean update) throws YarnException {
    String reservationZNode = getNodePath(reservationsZNode, reservationId.toString());
    SubClusterIdProto proto = ((SubClusterIdPBImpl)subClusterId).getProto();
    byte[] data = proto.toByteArray();
    put(reservationZNode, data, update);
  }

  private SubClusterId getReservation(final ReservationId reservationId)
      throws YarnException {
    String reservationIdZNode = getNodePath(reservationsZNode, reservationId.toString());
    SubClusterId subClusterId = null;
    byte[] data = get(reservationIdZNode);
    if (data != null) {
      try {
        subClusterId = new SubClusterIdPBImpl(SubClusterIdProto.parseFrom(data));
      } catch (InvalidProtocolBufferException e) {
        String errMsg = "Cannot parse reservation at " + reservationId;
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
    }
    return subClusterId;
  }

  @VisibleForTesting
  public ZKFederationStateStoreOpDurations getOpDurations() {
    return opDurations;
  }

  @Override
  public AddReservationHomeSubClusterResponse addReservationHomeSubCluster(
      AddReservationHomeSubClusterRequest request) throws YarnException {

    long start = clock.getTime();
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);
    ReservationHomeSubCluster reservationHomeSubCluster = request.getReservationHomeSubCluster();
    ReservationId reservationId = reservationHomeSubCluster.getReservationId();

    // Try to write the subcluster
    SubClusterId homeSubCluster = reservationHomeSubCluster.getHomeSubCluster();
    try {
      putReservation(reservationId, homeSubCluster, false);
    } catch (Exception e) {
      String errMsg = "Cannot add reservation home subcluster for " + reservationId;
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    // Check for the actual subcluster
    try {
      homeSubCluster = getReservation(reservationId);
    } catch (Exception e) {
      String errMsg = "Cannot check app home subcluster for " + reservationId;
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    long end = clock.getTime();
    opDurations.addReservationHomeSubClusterDuration(start, end);
    return AddReservationHomeSubClusterResponse.newInstance(homeSubCluster);
  }

  @Override
  public GetReservationHomeSubClusterResponse getReservationHomeSubCluster(
      GetReservationHomeSubClusterRequest request) throws YarnException {

    long start = clock.getTime();
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);
    ReservationId reservationId = request.getReservationId();
    SubClusterId homeSubCluster = getReservation(reservationId);

    if (homeSubCluster == null) {
      String errMsg = "Reservation " + reservationId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    ReservationHomeSubCluster reservationHomeSubCluster =
        ReservationHomeSubCluster.newInstance(reservationId, homeSubCluster);
    long end = clock.getTime();
    opDurations.addGetReservationHomeSubClusterDuration(start, end);
    return GetReservationHomeSubClusterResponse.newInstance(reservationHomeSubCluster);
  }

  @Override
  public GetReservationsHomeSubClusterResponse getReservationsHomeSubCluster(
      GetReservationsHomeSubClusterRequest request) throws YarnException {
    long start = clock.getTime();
    List<ReservationHomeSubCluster> result = new ArrayList<>();

    try {
      for (String child : zkManager.getChildren(reservationsZNode)) {
        ReservationId reservationId = ReservationId.parseReservationId(child);
        SubClusterId homeSubCluster = getReservation(reservationId);
        ReservationHomeSubCluster app =
            ReservationHomeSubCluster.newInstance(reservationId, homeSubCluster);
        result.add(app);
      }
    } catch (Exception e) {
      String errMsg = "Cannot get apps: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    long end = clock.getTime();
    opDurations.addGetReservationsHomeSubClusterDuration(start, end);
    return GetReservationsHomeSubClusterResponse.newInstance(result);
  }

  @Override
  public DeleteReservationHomeSubClusterResponse deleteReservationHomeSubCluster(
      DeleteReservationHomeSubClusterRequest request) throws YarnException {
    long start = clock.getTime();
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);
    ReservationId reservationId = request.getReservationId();
    String reservationZNode = getNodePath(reservationsZNode, reservationId.toString());

    boolean exists = false;
    try {
      exists = zkManager.exists(reservationZNode);
    } catch (Exception e) {
      String errMsg = "Cannot check reservation: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    if (!exists) {
      String errMsg = "Reservation " + reservationId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    try {
      zkManager.delete(reservationZNode);
    } catch (Exception e) {
      String errMsg = "Cannot delete reservation: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    long end = clock.getTime();
    opDurations.addDeleteReservationHomeSubClusterDuration(start, end);
    return DeleteReservationHomeSubClusterResponse.newInstance();
  }

  @Override
  public UpdateReservationHomeSubClusterResponse updateReservationHomeSubCluster(
      UpdateReservationHomeSubClusterRequest request) throws YarnException {

    long start = clock.getTime();
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);
    ReservationHomeSubCluster reservationHomeSubCluster = request.getReservationHomeSubCluster();
    ReservationId reservationId = reservationHomeSubCluster.getReservationId();
    SubClusterId homeSubCluster = getReservation(reservationId);

    if (homeSubCluster == null) {
      String errMsg = "Reservation " + reservationId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    SubClusterId newSubClusterId = reservationHomeSubCluster.getHomeSubCluster();
    putReservation(reservationId, newSubClusterId, true);
    long end = clock.getTime();
    opDurations.addUpdateReservationHomeSubClusterDuration(start, end);
    return UpdateReservationHomeSubClusterResponse.newInstance();
  }

  /**
   * ZookeeperFederationStateStore Supports Store NewMasterKey.
   *
   * @param request The request contains RouterMasterKey, which is an abstraction for DelegationKey
   * @return routerMasterKeyResponse, the response contains the RouterMasterKey.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterMasterKeyResponse storeNewMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {

    long start = clock.getTime();
    // For the verification of the request, after passing the verification,
    // the request and the internal objects will not be empty and can be used directly.
    FederationRouterRMTokenInputValidator.validate(request);

    // Parse the delegationKey from the request and get the ZK storage path.
    DelegationKey delegationKey = convertMasterKeyToDelegationKey(request);
    String nodeCreatePath = getMasterKeyZNodePathByDelegationKey(delegationKey);
    LOG.debug("Storing RMDelegationKey_{}, ZkNodePath = {}.", delegationKey.getKeyId(),
        nodeCreatePath);

    // Write master key data to zk.
    try (ByteArrayOutputStream os = new ByteArrayOutputStream();
         DataOutputStream fsOut = new DataOutputStream(os)) {
      delegationKey.write(fsOut);
      put(nodeCreatePath, os.toByteArray(), false);
    }

    // Get the stored masterKey from zk.
    RouterMasterKey masterKeyFromZK = getRouterMasterKeyFromZK(nodeCreatePath);
    long end = clock.getTime();
    opDurations.addStoreNewMasterKeyDuration(start, end);
    return RouterMasterKeyResponse.newInstance(masterKeyFromZK);
  }

  /**
   * ZookeeperFederationStateStore Supports Remove MasterKey.
   *
   * @param request The request contains RouterMasterKey, which is an abstraction for DelegationKey
   * @return routerMasterKeyResponse, the response contains the RouterMasterKey.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterMasterKeyResponse removeStoredMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {

    long start = clock.getTime();
    // For the verification of the request, after passing the verification,
    // the request and the internal objects will not be empty and can be used directly.
    FederationRouterRMTokenInputValidator.validate(request);

    try {
      // Parse the delegationKey from the request and get the ZK storage path.
      RouterMasterKey masterKey = request.getRouterMasterKey();
      DelegationKey delegationKey = convertMasterKeyToDelegationKey(request);
      String nodeRemovePath = getMasterKeyZNodePathByDelegationKey(delegationKey);
      LOG.debug("Removing RMDelegationKey_{}, ZkNodePath = {}.", delegationKey.getKeyId(),
          nodeRemovePath);

      // Check if the path exists, Throws an exception if the path does not exist.
      if (!exists(nodeRemovePath)) {
        throw new YarnException("ZkNodePath = " + nodeRemovePath + " not exists!");
      }

      // try to remove masterKey.
      zkManager.delete(nodeRemovePath);
      long end = clock.getTime();
      opDurations.removeStoredMasterKeyDuration(start, end);
      return RouterMasterKeyResponse.newInstance(masterKey);
    } catch (Exception e) {
      throw new YarnException(e);
    }
  }

  /**
   * ZookeeperFederationStateStore Supports Remove MasterKey.
   *
   * @param request The request contains RouterMasterKey, which is an abstraction for DelegationKey
   * @return routerMasterKeyResponse, the response contains the RouterMasterKey.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterMasterKeyResponse getMasterKeyByDelegationKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {

    long start = clock.getTime();
    // For the verification of the request, after passing the verification,
    // the request and the internal objects will not be empty and can be used directly.
    FederationRouterRMTokenInputValidator.validate(request);

    try {

      // Parse the delegationKey from the request and get the ZK storage path.
      DelegationKey delegationKey = convertMasterKeyToDelegationKey(request);
      String nodePath = getMasterKeyZNodePathByDelegationKey(delegationKey);

      // Check if the path exists, Throws an exception if the path does not exist.
      if (!exists(nodePath)) {
        throw new YarnException("ZkNodePath = " + nodePath + " not exists!");
      }

      // Get the stored masterKey from zk.
      RouterMasterKey routerMasterKey = getRouterMasterKeyFromZK(nodePath);
      long end = clock.getTime();
      opDurations.getMasterKeyByDelegationKeyDuration(start, end);
      return RouterMasterKeyResponse.newInstance(routerMasterKey);
    } catch (Exception e) {
      throw new YarnException(e);
    }
  }

  /**
   * Get MasterKeyZNodePath based on DelegationKey.
   *
   * @param delegationKey delegationKey.
   * @return masterKey ZNodePath.
   */
  private String getMasterKeyZNodePathByDelegationKey(DelegationKey delegationKey) {
    return getMasterKeyZNodePathByKeyId(delegationKey.getKeyId());
  }

  /**
   * Get MasterKeyZNodePath based on KeyId.
   *
   * @param keyId master key id.
   * @return masterKey ZNodePath.
   */
  private String getMasterKeyZNodePathByKeyId(int keyId) {
    String nodeName = ROUTER_RM_DELEGATION_KEY_PREFIX + keyId;
    return getNodePath(routerRMDTMasterKeysRootPath, nodeName);
  }

  /**
   * Get RouterMasterKey from ZK.
   *
   * @param nodePath The path where masterKey is stored in zk.
   *
   * @return RouterMasterKey.
   * @throws IOException An IO Error occurred.
   */
  private RouterMasterKey getRouterMasterKeyFromZK(String nodePath)
      throws IOException {
    try {
      byte[] data = get(nodePath);
      if ((data == null) || (data.length == 0)) {
        return null;
      }

      ByteArrayInputStream bin = new ByteArrayInputStream(data);
      DataInputStream din = new DataInputStream(bin);
      DelegationKey key = new DelegationKey();
      key.readFields(din);

      return RouterMasterKey.newInstance(key.getKeyId(),
          ByteBuffer.wrap(key.getEncodedKey()), key.getExpiryDate());
    } catch (Exception ex) {
      LOG.error("No node in path {}.", nodePath);
      throw new IOException(ex);
    }
  }

  /**
   * ZookeeperFederationStateStore Supports Store RMDelegationTokenIdentifier.
   *
   * The stored token method is a synchronized method
   * used to ensure that storeNewToken is a thread-safe method.
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return routerRMTokenResponse, the response contains the RouterStoreToken.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterRMTokenResponse storeNewToken(RouterRMTokenRequest request)
      throws YarnException, IOException {

    long start = clock.getTime();
    // We verify the RouterRMTokenRequest to ensure that the request is not empty,
    // and that the internal RouterStoreToken is not empty.
    FederationRouterRMTokenInputValidator.validate(request);

    try {

      // add delegationToken
      storeOrUpdateRouterRMDT(request, false);

      // Get the stored delegationToken from ZK and return.
      RouterStoreToken resultStoreToken = getStoreTokenFromZK(request);
      long end = clock.getTime();
      opDurations.getStoreNewTokenDuration(start, end);
      return RouterRMTokenResponse.newInstance(resultStoreToken);
    } catch (YarnException | IOException e) {
      throw e;
    } catch (Exception e) {
      throw new YarnException(e);
    }
  }

  /**
   * ZookeeperFederationStateStore Supports Update RMDelegationTokenIdentifier.
   *
   * The update stored token method is a synchronized method
   * used to ensure that storeNewToken is a thread-safe method.
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return routerRMTokenResponse, the response contains the RouterStoreToken.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterRMTokenResponse updateStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException {

    long start = clock.getTime();
    // We verify the RouterRMTokenRequest to ensure that the request is not empty,
    // and that the internal RouterStoreToken is not empty.
    FederationRouterRMTokenInputValidator.validate(request);

    try {

      // get the Token storage path
      String nodePath = getStoreTokenZNodePathByTokenRequest(request);

      // updateStoredToken needs to determine whether the zkNode exists.
      // If it exists, update the token data.
      // If it does not exist, write the new token data directly.
      boolean pathExists = true;
      if (!exists(nodePath)) {
        pathExists = false;
      }

      if (pathExists) {
        // update delegationToken
        storeOrUpdateRouterRMDT(request, true);
      } else {
        // add new delegationToken
        storeNewToken(request);
      }

      // Get the stored delegationToken from ZK and return.
      RouterStoreToken resultStoreToken = getStoreTokenFromZK(request);
      long end = clock.getTime();
      opDurations.updateStoredTokenDuration(start, end);
      return RouterRMTokenResponse.newInstance(resultStoreToken);
    } catch (YarnException | IOException e) {
      throw e;
    } catch (Exception e) {
      throw new YarnException(e);
    }
  }

  /**
   * ZookeeperFederationStateStore Supports Remove RMDelegationTokenIdentifier.
   *
   * The remove stored token method is a synchronized method
   * used to ensure that storeNewToken is a thread-safe method.
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return routerRMTokenResponse, the response contains the RouterStoreToken.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterRMTokenResponse removeStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException {

    long start = clock.getTime();
    // We verify the RouterRMTokenRequest to ensure that the request is not empty,
    // and that the internal RouterStoreToken is not empty.
    FederationRouterRMTokenInputValidator.validate(request);

    try {

      // get the Token storage path
      String nodePath = getStoreTokenZNodePathByTokenRequest(request);

      // If the path to be deleted does not exist, throw an exception directly.
      if (!exists(nodePath)) {
        throw new YarnException("ZkNodePath = " + nodePath + " not exists!");
      }

      // Check again, first get the data from ZK,
      // if the data is not empty, then delete it
      RouterStoreToken storeToken = getStoreTokenFromZK(request);
      if (storeToken != null) {
        zkManager.delete(nodePath);
      }

      // return deleted token data.
      long end = clock.getTime();
      opDurations.removeStoredTokenDuration(start, end);
      return RouterRMTokenResponse.newInstance(storeToken);
    } catch (YarnException | IOException e) {
      throw e;
    } catch (Exception e) {
      throw new YarnException(e);
    }
  }

  /**
   * The Router Supports GetTokenByRouterStoreToken.
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return RouterRMTokenResponse.
   * @throws YarnException if the call to the state store is unsuccessful
   * @throws IOException An IO Error occurred
   */
  @Override
  public RouterRMTokenResponse getTokenByRouterStoreToken(RouterRMTokenRequest request)
      throws YarnException, IOException {

    long start = clock.getTime();
    // We verify the RouterRMTokenRequest to ensure that the request is not empty,
    // and that the internal RouterStoreToken is not empty.
    FederationRouterRMTokenInputValidator.validate(request);

    try {

      // Before get the token,
      // we need to determine whether the path where the token is stored exists.
      // If it doesn't exist, we will throw an exception.
      String nodePath = getStoreTokenZNodePathByTokenRequest(request);
      if (!exists(nodePath)) {
        throw new YarnException("ZkNodePath = " + nodePath + " not exists!");
      }

      // Get the stored delegationToken from ZK and return.
      RouterStoreToken resultStoreToken = getStoreTokenFromZK(request);
      // return deleted token data.
      long end = clock.getTime();
      opDurations.getTokenByRouterStoreTokenDuration(start, end);
      return RouterRMTokenResponse.newInstance(resultStoreToken);
    } catch (YarnException | IOException e) {
      throw e;
    } catch (Exception e) {
      throw new YarnException(e);
    }
  }

  /**
   * Convert MasterKey to DelegationKey.
   *
   * Before using this function,
   * please use FederationRouterRMTokenInputValidator to verify the request.
   * By default, the request is not empty, and the internal object is not empty.
   *
   * @param request RouterMasterKeyRequest
   * @return DelegationKey.
   */
  private DelegationKey convertMasterKeyToDelegationKey(RouterMasterKeyRequest request) {
    RouterMasterKey masterKey = request.getRouterMasterKey();
    return convertMasterKeyToDelegationKey(masterKey);
  }

  /**
   * Convert MasterKey to DelegationKey.
   *
   * @param masterKey masterKey.
   * @return DelegationKey.
   */
  private DelegationKey convertMasterKeyToDelegationKey(RouterMasterKey masterKey) {
    ByteBuffer keyByteBuf = masterKey.getKeyBytes();
    byte[] keyBytes = new byte[keyByteBuf.remaining()];
    keyByteBuf.get(keyBytes);
    return new DelegationKey(masterKey.getKeyId(), masterKey.getExpiryDate(), keyBytes);
  }

  /**
   * Check if a path exists in zk.
   *
   * @param path Path to be checked.
   * @return Returns true if the path exists, false if the path does not exist.
   * @throws Exception When an exception to access zk occurs.
   */
  @VisibleForTesting
  boolean exists(final String path) throws Exception {
    return zkManager.exists(path);
  }

  /**
   * Add or update delegationToken.
   *
   * Before using this function,
   * please use FederationRouterRMTokenInputValidator to verify the request.
   * By default, the request is not empty, and the internal object is not empty.
   *
   * @param request storeToken
   * @param isUpdate true, update the token; false, create a new token.
   * @throws Exception exception occurs.
   */
  private void storeOrUpdateRouterRMDT(RouterRMTokenRequest request,  boolean isUpdate)
      throws Exception {

    RouterStoreToken routerStoreToken  = request.getRouterStoreToken();
    String nodeCreatePath = getStoreTokenZNodePathByTokenRequest(request);
    LOG.debug("nodeCreatePath = {}, isUpdate = {}", nodeCreatePath, isUpdate);
    put(nodeCreatePath, routerStoreToken.toByteArray(), isUpdate);
  }

  /**
   * Get ZNode Path of StoreToken.
   *
   * Before using this method, we should use FederationRouterRMTokenInputValidator
   * to verify the request,ensure that the request is not empty,
   * and ensure that the object in the request is not empty.
   *
   * @param request RouterMasterKeyRequest.
   * @return RouterRMToken ZNode Path.
   * @throws IOException io exception occurs.
   */
  private String getStoreTokenZNodePathByTokenRequest(RouterRMTokenRequest request)
      throws IOException {
    RouterStoreToken routerStoreToken = request.getRouterStoreToken();
    YARNDelegationTokenIdentifier identifier = routerStoreToken.getTokenIdentifier();
    return getStoreTokenZNodePathByIdentifier(identifier);
  }

  /**
   * Get ZNode Path of StoreToken.
   *
   * @param identifier YARNDelegationTokenIdentifier
   * @return RouterRMToken ZNode Path.
   */
  private String getStoreTokenZNodePathByIdentifier(YARNDelegationTokenIdentifier identifier) {
    String nodePath = getNodePath(routerRMDelegationTokensRootPath,
        ROUTER_RM_DELEGATION_TOKEN_PREFIX + identifier.getSequenceNumber());
    return nodePath;
  }

  /**
   * Get RouterStoreToken from ZK.
   *
   * @param request RouterMasterKeyRequest.
   * @return RouterStoreToken.
   * @throws IOException io exception occurs.
   */
  private RouterStoreToken getStoreTokenFromZK(RouterRMTokenRequest request) throws IOException {
    RouterStoreToken routerStoreToken = request.getRouterStoreToken();
    YARNDelegationTokenIdentifier identifier = routerStoreToken.getTokenIdentifier();
    return getStoreTokenFromZK(identifier);
  }

  /**
   * Get RouterStoreToken from ZK.
   *
   * @param identifier YARN DelegationToken Identifier.
   * @return RouterStoreToken.
   * @throws IOException io exception occurs.
   */
  private RouterStoreToken getStoreTokenFromZK(YARNDelegationTokenIdentifier identifier)
      throws IOException {
    // get the Token storage path
    String nodePath = getStoreTokenZNodePathByIdentifier(identifier);
    return getStoreTokenFromZK(nodePath);
  }

  /**
   * Get RouterStoreToken from ZK.
   *
   * @param nodePath Znode location where data is stored.
   * @return RouterStoreToken.
   * @throws IOException io exception occurs.
   */
  private RouterStoreToken getStoreTokenFromZK(String nodePath)
      throws IOException {
    try {
      byte[] data = get(nodePath);
      if ((data == null) || (data.length == 0)) {
        return null;
      }
      ByteArrayInputStream bin = new ByteArrayInputStream(data);
      DataInputStream din = new DataInputStream(bin);
      RouterStoreToken storeToken = Records.newRecord(RouterStoreToken.class);
      storeToken.readFields(din);
      return storeToken;
    } catch (Exception ex) {
      LOG.error("No node in path [{}]", nodePath, ex);
      throw new IOException(ex);
    }
  }

  /**
   * Increase SequenceNum. For zk, this is a distributed value.
   * To ensure data consistency, we will use the synchronized keyword.
   *
   * For ZookeeperFederationStateStore, in order to reduce the interaction with ZK,
   * we will apply for SequenceNum from ZK in batches(Apply
   * when currentSeqNum &gt;= currentMaxSeqNum),
   * and assign this value to the variable currentMaxSeqNum.
   *
   * When calling the method incrementDelegationTokenSeqNum,
   * if currentSeqNum &lt; currentMaxSeqNum, we return ++currentMaxSeqNum,
   * When currentSeqNum &gt;= currentMaxSeqNum, we re-apply SequenceNum from zk.
   *
   * @return SequenceNum.
   */
  @Override
  public int incrementDelegationTokenSeqNum() {
    // The secret manager will keep a local range of seq num which won't be
    // seen by peers, so only when the range is exhausted it will ask zk for
    // another range again
    if (currentSeqNum >= currentMaxSeqNum) {
      try {
        // after a successful batch request, we can get the range starting point
        currentSeqNum = incrSharedCount(delTokSeqCounter, seqNumBatchSize);
        currentMaxSeqNum = currentSeqNum + seqNumBatchSize;
        LOG.info("Fetched new range of seq num, from {} to {} ",
            currentSeqNum + 1, currentMaxSeqNum);
      } catch (InterruptedException e) {
        // The ExpirationThread is just finishing.. so don't do anything..
        LOG.debug("Thread interrupted while performing token counter increment", e);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        throw new RuntimeException("Could not increment shared counter !!", e);
      }
    }
    return ++currentSeqNum;
  }

  /**
   * Increment the value of the shared variable.
   *
   * @param sharedCount zk SharedCount.
   * @param batchSize batch size.
   * @return new SequenceNum.
   * @throws Exception exception occurs.
   */
  private int incrSharedCount(SharedCount sharedCount, int batchSize)
      throws Exception {
    while (true) {
      // Loop until we successfully increment the counter
      VersionedValue<Integer> versionedValue = sharedCount.getVersionedValue();
      if (sharedCount.trySetCount(versionedValue, versionedValue.getValue() + batchSize)) {
        return versionedValue.getValue();
      }
    }
  }

  /**
   * Get DelegationToken SeqNum.
   *
   * @return delegationTokenSeqNum.
   */
  @Override
  public int getDelegationTokenSeqNum() {
    return delTokSeqCounter.getCount();
  }

  /**
   * Set DelegationToken SeqNum.
   *
   * @param seqNum sequenceNum.
   */
  @Override
  public void setDelegationTokenSeqNum(int seqNum) {
    try {
      delTokSeqCounter.setCount(seqNum);
    } catch (Exception e) {
      throw new RuntimeException("Could not set shared counter !!", e);
    }
  }

  /**
   * Get Current KeyId.
   *
   * @return currentKeyId.
   */
  @Override
  public int getCurrentKeyId() {
    return keyIdSeqCounter.getCount();
  }

  /**
   * The Router Supports incrementCurrentKeyId.
   *
   * @return CurrentKeyId.
   */
  @Override
  public int incrementCurrentKeyId() {
    try {
      // It should be noted that the BatchSize of MasterKeyId defaults to 1.
      incrSharedCount(keyIdSeqCounter, 1);
    } catch (InterruptedException e) {
      // The ExpirationThread is just finishing.. so don't do anything..
      LOG.debug("Thread interrupted while performing Master keyId increment", e);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      throw new RuntimeException("Could not increment shared Master keyId counter !!", e);
    }
    return keyIdSeqCounter.getCount();
  }
}