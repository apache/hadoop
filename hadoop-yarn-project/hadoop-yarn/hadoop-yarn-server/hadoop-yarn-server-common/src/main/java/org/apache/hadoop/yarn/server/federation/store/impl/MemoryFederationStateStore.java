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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.Comparator;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
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
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMDTSecretManagerState;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationApplicationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationReservationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationMembershipStateStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationPolicyStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils.filterHomeSubCluster;

/**
 * In-memory implementation of {@link FederationStateStore}.
 */
public class MemoryFederationStateStore implements FederationStateStore {

  private Map<SubClusterId, SubClusterInfo> membership;
  private Map<ApplicationId, SubClusterId> applications;
  private Map<ReservationId, SubClusterId> reservations;
  private Map<String, SubClusterPolicyConfiguration> policies;
  private RouterRMDTSecretManagerState routerRMSecretManagerState;
  private int maxAppsInStateStore;

  private final MonotonicClock clock = new MonotonicClock();

  public static final Logger LOG =
      LoggerFactory.getLogger(MemoryFederationStateStore.class);

  @Override
  public void init(Configuration conf) {
    membership = new ConcurrentHashMap<SubClusterId, SubClusterInfo>();
    applications = new ConcurrentHashMap<ApplicationId, SubClusterId>();
    reservations = new ConcurrentHashMap<ReservationId, SubClusterId>();
    policies = new ConcurrentHashMap<String, SubClusterPolicyConfiguration>();
    routerRMSecretManagerState = new RouterRMDTSecretManagerState();
    maxAppsInStateStore = conf.getInt(
        YarnConfiguration.FEDERATION_STATESTORE_MAX_APPLICATIONS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_MAX_APPLICATIONS);
  }

  @Override
  public void close() {
    membership = null;
    applications = null;
    reservations = null;
    policies = null;
  }

  @Override
  public SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest request) throws YarnException {
    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterInfo subClusterInfo = request.getSubClusterInfo();

    long currentTime =
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();

    SubClusterInfo subClusterInfoToSave =
        SubClusterInfo.newInstance(subClusterInfo.getSubClusterId(),
            subClusterInfo.getAMRMServiceAddress(),
            subClusterInfo.getClientRMServiceAddress(),
            subClusterInfo.getRMAdminServiceAddress(),
            subClusterInfo.getRMWebServiceAddress(), currentTime,
            subClusterInfo.getState(), subClusterInfo.getLastStartTime(),
            subClusterInfo.getCapability());

    membership.put(subClusterInfo.getSubClusterId(), subClusterInfoToSave);
    return SubClusterRegisterResponse.newInstance();
  }

  @Override
  public SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest request) throws YarnException {
    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterInfo subClusterInfo = membership.get(request.getSubClusterId());
    if (subClusterInfo == null) {
      String errMsg =
          "SubCluster " + request.getSubClusterId().toString() + " not found";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    } else {
      subClusterInfo.setState(request.getState());
    }

    return SubClusterDeregisterResponse.newInstance();
  }

  @Override
  public SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest request) throws YarnException {

    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterId subClusterId = request.getSubClusterId();
    SubClusterInfo subClusterInfo = membership.get(subClusterId);

    if (subClusterInfo == null) {
      String errMsg = "SubCluster " + subClusterId.toString()
          + " does not exist; cannot heartbeat";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    long currentTime =
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();

    subClusterInfo.setLastHeartBeat(currentTime);
    subClusterInfo.setState(request.getState());
    subClusterInfo.setCapability(request.getCapability());

    return SubClusterHeartbeatResponse.newInstance();
  }

  @Override
  public GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest request) throws YarnException {

    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterId subClusterId = request.getSubClusterId();
    if (!membership.containsKey(subClusterId)) {
      LOG.warn("The queried SubCluster: {} does not exist.", subClusterId);
      return null;
    }

    return GetSubClusterInfoResponse.newInstance(membership.get(subClusterId));
  }

  @Override
  public GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest request) throws YarnException {
    List<SubClusterInfo> result = new ArrayList<SubClusterInfo>();

    for (SubClusterInfo info : membership.values()) {
      if (!request.getFilterInactiveSubClusters()
          || info.getState().isActive()) {
        result.add(info);
      }
    }
    return GetSubClustersInfoResponse.newInstance(result);
  }

  // FederationApplicationHomeSubClusterStore methods

  @Override
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException {

    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationId appId =
        request.getApplicationHomeSubCluster().getApplicationId();

    if (!applications.containsKey(appId)) {
      applications.put(appId,
          request.getApplicationHomeSubCluster().getHomeSubCluster());
    }

    return AddApplicationHomeSubClusterResponse
        .newInstance(applications.get(appId));
  }

  @Override
  public UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubCluster(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException {

    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationId appId =
        request.getApplicationHomeSubCluster().getApplicationId();
    if (!applications.containsKey(appId)) {
      String errMsg = "Application " + appId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    applications.put(appId,
        request.getApplicationHomeSubCluster().getHomeSubCluster());
    return UpdateApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException {

    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationId appId = request.getApplicationId();
    if (!applications.containsKey(appId)) {
      String errMsg = "Application " + appId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    return GetApplicationHomeSubClusterResponse.newInstance(appId, applications.get(appId));
  }

  @Override
  public GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException {

    if (request == null) {
      throw new YarnException("Missing getApplicationsHomeSubCluster request");
    }

    SubClusterId requestSC = request.getSubClusterId();
    List<ApplicationHomeSubCluster> result = applications.keySet().stream()
        .map(applicationId -> generateAppHomeSC(applicationId))
        .sorted(Comparator.comparing(ApplicationHomeSubCluster::getCreateTime).reversed())
        .filter(appHomeSC -> filterHomeSubCluster(requestSC, appHomeSC.getHomeSubCluster()))
        .limit(maxAppsInStateStore)
        .collect(Collectors.toList());

    LOG.info("filterSubClusterId = {}, appCount = {}.", requestSC, result.size());
    return GetApplicationsHomeSubClusterResponse.newInstance(result);
  }

  private ApplicationHomeSubCluster generateAppHomeSC(ApplicationId applicationId) {
    SubClusterId subClusterId = applications.get(applicationId);
    return ApplicationHomeSubCluster.newInstance(applicationId, subClusterId);
  }

  @Override
  public DeleteApplicationHomeSubClusterResponse deleteApplicationHomeSubCluster(
      DeleteApplicationHomeSubClusterRequest request) throws YarnException {

    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationId appId = request.getApplicationId();
    if (!applications.containsKey(appId)) {
      String errMsg = "Application " + appId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    applications.remove(appId);
    return DeleteApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException {

    FederationPolicyStoreInputValidator.validate(request);
    String queue = request.getQueue();
    if (!policies.containsKey(queue)) {
      LOG.warn("Policy for queue: {} does not exist.", queue);
      return null;
    }

    return GetSubClusterPolicyConfigurationResponse
        .newInstance(policies.get(queue));
  }

  @Override
  public SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException {

    FederationPolicyStoreInputValidator.validate(request);
    policies.put(request.getPolicyConfiguration().getQueue(),
        request.getPolicyConfiguration());
    return SetSubClusterPolicyConfigurationResponse.newInstance();
  }

  @Override
  public GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException {
    ArrayList<SubClusterPolicyConfiguration> result =
        new ArrayList<SubClusterPolicyConfiguration>();
    for (SubClusterPolicyConfiguration policy : policies.values()) {
      result.add(policy);
    }
    return GetSubClusterPoliciesConfigurationsResponse.newInstance(result);
  }

  @Override
  public Version getCurrentVersion() {
    return null;
  }

  @Override
  public Version loadVersion() {
    return null;
  }

  @Override
  public AddReservationHomeSubClusterResponse addReservationHomeSubCluster(
      AddReservationHomeSubClusterRequest request) throws YarnException {
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);
    ReservationHomeSubCluster homeSubCluster = request.getReservationHomeSubCluster();
    ReservationId reservationId = homeSubCluster.getReservationId();
    if (!reservations.containsKey(reservationId)) {
      reservations.put(reservationId, homeSubCluster.getHomeSubCluster());
    }
    return AddReservationHomeSubClusterResponse.newInstance(reservations.get(reservationId));
  }

  @Override
  public GetReservationHomeSubClusterResponse getReservationHomeSubCluster(
      GetReservationHomeSubClusterRequest request) throws YarnException {
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);
    ReservationId reservationId = request.getReservationId();
    if (!reservations.containsKey(reservationId)) {
      throw new YarnException("Reservation " + reservationId + " does not exist");
    }
    SubClusterId subClusterId = reservations.get(reservationId);
    ReservationHomeSubCluster homeSubCluster =
        ReservationHomeSubCluster.newInstance(reservationId, subClusterId);
    return GetReservationHomeSubClusterResponse.newInstance(homeSubCluster);
  }

  @Override
  public GetReservationsHomeSubClusterResponse getReservationsHomeSubCluster(
      GetReservationsHomeSubClusterRequest request) throws YarnException {
    List<ReservationHomeSubCluster> result = new ArrayList<>();

    for (Entry<ReservationId, SubClusterId> entry : reservations.entrySet()) {
      ReservationId reservationId = entry.getKey();
      SubClusterId subClusterId = entry.getValue();
      ReservationHomeSubCluster homeSubCluster =
          ReservationHomeSubCluster.newInstance(reservationId, subClusterId);
      result.add(homeSubCluster);
    }

    return GetReservationsHomeSubClusterResponse.newInstance(result);
  }

  @Override
  public UpdateReservationHomeSubClusterResponse updateReservationHomeSubCluster(
      UpdateReservationHomeSubClusterRequest request) throws YarnException {
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);
    ReservationId reservationId = request.getReservationHomeSubCluster().getReservationId();

    if (!reservations.containsKey(reservationId)) {
      throw new YarnException("Reservation " + reservationId + " does not exist.");
    }

    SubClusterId subClusterId = request.getReservationHomeSubCluster().getHomeSubCluster();
    reservations.put(reservationId, subClusterId);
    return UpdateReservationHomeSubClusterResponse.newInstance();
  }

  @Override
  public DeleteReservationHomeSubClusterResponse deleteReservationHomeSubCluster(
      DeleteReservationHomeSubClusterRequest request) throws YarnException {
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);
    ReservationId reservationId = request.getReservationId();
    if (!reservations.containsKey(reservationId)) {
      throw new YarnException("Reservation " + reservationId + " does not exist");
    }
    reservations.remove(reservationId);
    return DeleteReservationHomeSubClusterResponse.newInstance();
  }

  @Override
  public RouterMasterKeyResponse storeNewMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {
    // Restore the DelegationKey from the request
    RouterMasterKey masterKey = request.getRouterMasterKey();
    DelegationKey delegationKey = getDelegationKeyByMasterKey(masterKey);

    Set<DelegationKey> rmDTMasterKeyState = routerRMSecretManagerState.getMasterKeyState();
    if (rmDTMasterKeyState.contains(delegationKey)) {
      LOG.info("Error storing info for RMDTMasterKey with keyID: {}.", delegationKey.getKeyId());
      throw new IOException("RMDTMasterKey with keyID: " + delegationKey.getKeyId() +
          " is already stored");
    }

    routerRMSecretManagerState.getMasterKeyState().add(delegationKey);
    LOG.info("Store Router-RMDT master key with key id: {}. Currently rmDTMasterKeyState size: {}",
        delegationKey.getKeyId(), rmDTMasterKeyState.size());

    return RouterMasterKeyResponse.newInstance(masterKey);
  }

  @Override
  public RouterMasterKeyResponse removeStoredMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {
    // Restore the DelegationKey from the request
    RouterMasterKey masterKey = request.getRouterMasterKey();
    DelegationKey delegationKey = getDelegationKeyByMasterKey(masterKey);

    LOG.info("Remove Router-RMDT master key with key id: {}.", delegationKey.getKeyId());
    Set<DelegationKey> rmDTMasterKeyState = routerRMSecretManagerState.getMasterKeyState();
    rmDTMasterKeyState.remove(delegationKey);

    return RouterMasterKeyResponse.newInstance(masterKey);
  }

  @Override
  public RouterMasterKeyResponse getMasterKeyByDelegationKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {
    // Restore the DelegationKey from the request
    RouterMasterKey masterKey = request.getRouterMasterKey();
    DelegationKey delegationKey = getDelegationKeyByMasterKey(masterKey);

    Set<DelegationKey> rmDTMasterKeyState = routerRMSecretManagerState.getMasterKeyState();
    if (!rmDTMasterKeyState.contains(delegationKey)) {
      throw new IOException("GetMasterKey with keyID: " + masterKey.getKeyId() +
          " does not exist.");
    }
    RouterMasterKey resultRouterMasterKey = RouterMasterKey.newInstance(delegationKey.getKeyId(),
        ByteBuffer.wrap(delegationKey.getEncodedKey()), delegationKey.getExpiryDate());
    return RouterMasterKeyResponse.newInstance(resultRouterMasterKey);
  }

  /**
   * Get DelegationKey By based on MasterKey.
   *
   * @param masterKey masterKey
   * @return DelegationKey
   */
  private static DelegationKey getDelegationKeyByMasterKey(RouterMasterKey masterKey) {
    ByteBuffer keyByteBuf = masterKey.getKeyBytes();
    byte[] keyBytes = new byte[keyByteBuf.remaining()];
    keyByteBuf.get(keyBytes);
    return new DelegationKey(masterKey.getKeyId(), masterKey.getExpiryDate(), keyBytes);
  }

  @VisibleForTesting
  public RouterRMDTSecretManagerState getRouterRMSecretManagerState() {
    return routerRMSecretManagerState;
  }

  @VisibleForTesting
  public Map<SubClusterId, SubClusterInfo> getMembership() {
    return membership;
  }

  @VisibleForTesting
  public void setMembership(Map<SubClusterId, SubClusterInfo> membership) {
    this.membership = membership;
  }
}
