/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.federation.store.impl;

import com.sun.jersey.api.client.Client;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
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
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.records.Version;

import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_STATE_STORE_TIMEOUT_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_STATE_STORE_FALLBACK_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HTTP_PROXY_URL;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_STATESTORE_HTTP_PROXY_URL;

/**
 * REST implementation of {@link FederationStateStore}.
 */
public class HttpProxyFederationStateStore implements FederationStateStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(HttpProxyFederationStateStore.class);

  // Query Parameters
  public static final String QUERY_SC_FILTER = "filterInactiveSubClusters";

  // URL Parameters
  public static final String PARAM_SCID = "subcluster";
  public static final String PARAM_APPID = "appid";
  public static final String PARAM_QUEUE = "queue";

  // Paths
  public static final String ROOT = "/ws/v1/statestore";

  public static final String PATH_SUBCLUSTERS = "/subclusters";
  public static final String PATH_SUBCLUSTERS_SCID = "/subclusters/{" + PARAM_SCID + "}";

  public static final String PATH_POLICY = "/policy";
  public static final String PATH_POLICY_QUEUE = "/policy/{" + PARAM_QUEUE + "}";

  public static final String PATH_VIP_HEARTBEAT = "/heartbeat";
  public static final String PATH_APP_HOME = "/apphome";
  public static final String PATH_APP_HOME_APPID = "/apphome/{" + PARAM_APPID + "}";

  public static final String PATH_REGISTER = "/subcluster/register";
  public static final String PATH_DEREGISTER = "/subcluster/deregister";
  public static final String PATH_HEARTBEAT = "/subcluster/heartbeat";

  // It is very expensive to create the client
  // Jersey will spawn a thread for every client request
  private Client client = null;
  private String webAppUrl;
  private Map<SubClusterId, SubClusterInfo> subClusters;
  private boolean fallbackToConfig;

  @Override
  public void init(Configuration conf) throws YarnException {
    this.client = createJerseyClient(conf);
    this.subClusters = new ConcurrentHashMap<>();
    this.webAppUrl = WebAppUtils.HTTP_PREFIX + conf.get(FEDERATION_STATESTORE_HTTP_PROXY_URL,
        DEFAULT_FEDERATION_STATESTORE_HTTP_PROXY_URL);
    this.fallbackToConfig = conf.getBoolean(FEDERATION_STATE_STORE_FALLBACK_ENABLED,
        DEFAULT_FEDERATION_STATE_STORE_TIMEOUT_ENABLED);
  }

  @Override
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException {
    return null;
  }

  @Override
  public UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubCluster(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException {
    return null;
  }

  @Override
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException {
    return null;
  }

  @Override
  public GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException {
    return null;
  }

  @Override
  public DeleteApplicationHomeSubClusterResponse deleteApplicationHomeSubCluster(
      DeleteApplicationHomeSubClusterRequest request) throws YarnException {
    return null;
  }

  @Override
  public RouterMasterKeyResponse storeNewMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public RouterMasterKeyResponse removeStoredMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public RouterMasterKeyResponse getMasterKeyByDelegationKey(
      RouterMasterKeyRequest request) throws YarnException, IOException {
    return null;
  }

  @Override
  public RouterRMTokenResponse storeNewToken(
      RouterRMTokenRequest request) throws YarnException, IOException {
    return null;
  }

  @Override
  public RouterRMTokenResponse updateStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public RouterRMTokenResponse removeStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public RouterRMTokenResponse getTokenByRouterStoreToken(RouterRMTokenRequest request)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public int incrementDelegationTokenSeqNum() {
    return 0;
  }

  @Override
  public int getDelegationTokenSeqNum() {
    return 0;
  }

  @Override
  public void setDelegationTokenSeqNum(int seqNum) {
  }

  @Override
  public int getCurrentKeyId() {
    return 0;
  }

  @Override
  public int incrementCurrentKeyId() {
    return 0;
  }

  @Override
  public SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest registerSubClusterRequest) throws YarnException {
    return null;
  }

  @Override
  public SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest subClusterDeregisterRequest) throws YarnException {
    return null;
  }

  @Override
  public SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest subClusterHeartbeatRequest) throws YarnException {
    return null;
  }

  @Override
  public GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest subClusterRequest) throws YarnException {
    return null;
  }

  @Override
  public GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest subClustersRequest) throws YarnException {
    return null;
  }

  @Override
  public GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException {
    return null;
  }

  @Override
  public SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException {
    return null;
  }

  @Override
  public GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException {
    return null;
  }

  @Override
  public AddReservationHomeSubClusterResponse addReservationHomeSubCluster(
      AddReservationHomeSubClusterRequest request) throws YarnException {
    return null;
  }

  @Override
  public GetReservationHomeSubClusterResponse getReservationHomeSubCluster(
      GetReservationHomeSubClusterRequest request) throws YarnException {
    return null;
  }

  @Override
  public GetReservationsHomeSubClusterResponse getReservationsHomeSubCluster(
      GetReservationsHomeSubClusterRequest request) throws YarnException {
    return null;
  }

  @Override
  public UpdateReservationHomeSubClusterResponse updateReservationHomeSubCluster(
      UpdateReservationHomeSubClusterRequest request) throws YarnException {
    return null;
  }

  @Override
  public DeleteReservationHomeSubClusterResponse deleteReservationHomeSubCluster(
      DeleteReservationHomeSubClusterRequest request) throws YarnException {
    return null;
  }

  @Override
  public void close() throws Exception {
    client.destroy();
  }

  @Override
  public Version getCurrentVersion() {
    return null;
  }

  @Override
  public Version loadVersion() throws Exception {
    return null;
  }

  @Override
  public void storeVersion() throws Exception {

  }

  /**
   * Create a Jersey client instance.
   */
  private Client createJerseyClient(Configuration conf) {
    Client client = Client.create();
    int connectTimeOut = (int) conf.getTimeDuration(
        YarnConfiguration.FEDERATION_STATESTORE_HTTP_PROXY_CONNECT_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HTTP_PROXY_CONNECT_TIMEOUT_MS,
        TimeUnit.MILLISECONDS);
    int readTimeout = (int) conf.getTimeDuration(
        YarnConfiguration.FEDERATION_STATESTORE_HTTP_PROXY_READ_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HTTP_PROXY_READ_TIMEOUT_MS,
        TimeUnit.MILLISECONDS);
    // Set the connect timeout interval, in milliseconds.
    client.setConnectTimeout(connectTimeOut);
    // Set the read timeout interval, in milliseconds.
    client.setReadTimeout(readTimeout);
    return client;
  }
}
