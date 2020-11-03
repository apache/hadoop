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

package org.apache.hadoop.yarn.server.resourcemanager.federation;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
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
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Implements {@link FederationStateStore} and provides a service for
 * participating in the federation membership.
 */
public class FederationStateStoreService extends AbstractService
    implements FederationStateStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreService.class);

  private Configuration config;
  private ScheduledExecutorService scheduledExecutorService;
  private FederationStateStoreHeartbeat stateStoreHeartbeat;
  private FederationStateStore stateStoreClient = null;
  private SubClusterId subClusterId;
  private long heartbeatInterval;
  private RMContext rmContext;

  public FederationStateStoreService(RMContext rmContext) {
    super(FederationStateStoreService.class.getName());
    LOG.info("FederationStateStoreService initialized");
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.config = conf;

    RetryPolicy retryPolicy =
        FederationStateStoreFacade.createRetryPolicy(conf);

    this.stateStoreClient =
        (FederationStateStore) FederationStateStoreFacade.createRetryInstance(
            conf, YarnConfiguration.FEDERATION_STATESTORE_CLIENT_CLASS,
            YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CLIENT_CLASS,
            FederationStateStore.class, retryPolicy);
    this.stateStoreClient.init(conf);
    LOG.info("Initialized state store client class");

    this.subClusterId =
        SubClusterId.newInstance(YarnConfiguration.getClusterId(conf));

    heartbeatInterval = conf.getLong(
        YarnConfiguration.FEDERATION_STATESTORE_HEARTBEAT_INTERVAL_SECS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HEARTBEAT_INTERVAL_SECS);
    if (heartbeatInterval <= 0) {
      heartbeatInterval =
          YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HEARTBEAT_INTERVAL_SECS;
    }
    LOG.info("Initialized federation membership service.");

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {

    registerAndInitializeHeartbeat();

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    Exception ex = null;
    try {
      if (this.scheduledExecutorService != null
          && !this.scheduledExecutorService.isShutdown()) {
        this.scheduledExecutorService.shutdown();
        LOG.info("Stopped federation membership heartbeat");
      }
    } catch (Exception e) {
      LOG.error("Failed to shutdown ScheduledExecutorService", e);
      ex = e;
    }

    if (this.stateStoreClient != null) {
      try {
        deregisterSubCluster(SubClusterDeregisterRequest
            .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED));
      } finally {
        this.stateStoreClient.close();
      }
    }

    if (ex != null) {
      throw ex;
    }
  }

  // Return a client accessible string representation of the service address.
  private String getServiceAddress(InetSocketAddress address) {
    InetSocketAddress socketAddress = NetUtils.getConnectAddress(address);
    return socketAddress.getAddress().getHostAddress() + ":"
        + socketAddress.getPort();
  }

  private void registerAndInitializeHeartbeat() {
    String clientRMAddress =
        getServiceAddress(rmContext.getClientRMService().getBindAddress());
    String amRMAddress = getServiceAddress(
        rmContext.getApplicationMasterService().getBindAddress());
    String rmAdminAddress = getServiceAddress(
        config.getSocketAddr(YarnConfiguration.RM_ADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADMIN_PORT));
    String webAppAddress = getServiceAddress(NetUtils
        .createSocketAddr(WebAppUtils.getRMWebAppURLWithScheme(config)));

    SubClusterInfo subClusterInfo = SubClusterInfo.newInstance(subClusterId,
        amRMAddress, clientRMAddress, rmAdminAddress, webAppAddress,
        SubClusterState.SC_NEW, ResourceManager.getClusterTimeStamp(), "");
    try {
      registerSubCluster(SubClusterRegisterRequest.newInstance(subClusterInfo));
      LOG.info("Successfully registered for federation subcluster: {}",
          subClusterInfo);
    } catch (Exception e) {
      throw new YarnRuntimeException(
          "Failed to register Federation membership with the StateStore", e);
    }
    stateStoreHeartbeat = new FederationStateStoreHeartbeat(subClusterId,
        stateStoreClient, rmContext.getScheduler());
    scheduledExecutorService =
        HadoopExecutors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleWithFixedDelay(stateStoreHeartbeat,
        heartbeatInterval, heartbeatInterval, TimeUnit.SECONDS);
    LOG.info("Started federation membership heartbeat with interval: {}",
        heartbeatInterval);
  }

  @VisibleForTesting
  public FederationStateStore getStateStoreClient() {
    return stateStoreClient;
  }

  @VisibleForTesting
  public FederationStateStoreHeartbeat getStateStoreHeartbeatThread() {
    return stateStoreHeartbeat;
  }

  @Override
  public Version getCurrentVersion() {
    return stateStoreClient.getCurrentVersion();
  }

  @Override
  public Version loadVersion() {
    return stateStoreClient.getCurrentVersion();
  }

  @Override
  public GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException {
    return stateStoreClient.getPolicyConfiguration(request);
  }

  @Override
  public SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException {
    return stateStoreClient.setPolicyConfiguration(request);
  }

  @Override
  public GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException {
    return stateStoreClient.getPoliciesConfigurations(request);
  }

  @Override
  public SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest registerSubClusterRequest)
      throws YarnException {
    return stateStoreClient.registerSubCluster(registerSubClusterRequest);
  }

  @Override
  public SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest subClusterDeregisterRequest)
      throws YarnException {
    return stateStoreClient.deregisterSubCluster(subClusterDeregisterRequest);
  }

  @Override
  public SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest subClusterHeartbeatRequest)
      throws YarnException {
    return stateStoreClient.subClusterHeartbeat(subClusterHeartbeatRequest);
  }

  @Override
  public GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest subClusterRequest) throws YarnException {
    return stateStoreClient.getSubCluster(subClusterRequest);
  }

  @Override
  public GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest subClustersRequest) throws YarnException {
    return stateStoreClient.getSubClusters(subClustersRequest);
  }

  @Override
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException {
    return stateStoreClient.addApplicationHomeSubCluster(request);
  }

  @Override
  public UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubCluster(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException {
    return stateStoreClient.updateApplicationHomeSubCluster(request);
  }

  @Override
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException {
    return stateStoreClient.getApplicationHomeSubCluster(request);
  }

  @Override
  public GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException {
    return stateStoreClient.getApplicationsHomeSubCluster(request);
  }

  @Override
  public DeleteApplicationHomeSubClusterResponse deleteApplicationHomeSubCluster(
      DeleteApplicationHomeSubClusterRequest request) throws YarnException {
    return stateStoreClient.deleteApplicationHomeSubCluster(request);
  }
}
