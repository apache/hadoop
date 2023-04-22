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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.retry.FederationActionRetry;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;
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
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

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
  private long heartbeatInitialDelay;
  private RMContext rmContext;
  private final Clock clock = new MonotonicClock();
  private FederationStateStoreServiceMetrics metrics;
  private String cleanUpThreadNamePrefix = "FederationStateStoreService-Clean-Thread";
  private int cleanUpRetryCountNum;
  private long cleanUpRetrySleepTime;

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

    heartbeatInitialDelay = conf.getTimeDuration(
        YarnConfiguration.FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY,
        TimeUnit.SECONDS);

    if (heartbeatInitialDelay <= 0) {
      LOG.warn("{} configured value is wrong, must be > 0; using default value of {}",
          YarnConfiguration.FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY,
          YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY);
      heartbeatInitialDelay =
          YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY;
    }

    cleanUpRetryCountNum = conf.getInt(YarnConfiguration.FEDERATION_STATESTORE_CLEANUP_RETRY_COUNT,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CLEANUP_RETRY_COUNT);

    cleanUpRetrySleepTime = conf.getTimeDuration(
        YarnConfiguration.FEDERATION_STATESTORE_CLEANUP_RETRY_SLEEP_TIME,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CLEANUP_RETRY_SLEEP_TIME,
        TimeUnit.MILLISECONDS);

    LOG.info("Initialized federation membership service.");

    this.metrics = FederationStateStoreServiceMetrics.getMetrics();
    LOG.info("Initialized federation statestore service metrics.");

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
        heartbeatInitialDelay, heartbeatInterval, TimeUnit.SECONDS);
    LOG.info("Started federation membership heartbeat with interval: {} and initial delay: {}",
        heartbeatInterval, heartbeatInitialDelay);
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
  public Version loadVersion() throws Exception {
    return stateStoreClient.getCurrentVersion();
  }

  @Override
  public void storeVersion() throws Exception {
    stateStoreClient.storeVersion();
  }

  @Override
  public void checkVersion() throws Exception {
    stateStoreClient.checkVersion();
  }

  @Override
  public GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException {
    FederationClientMethod<GetSubClusterPolicyConfigurationResponse> clientMethod =
        new FederationClientMethod<>("getPolicyConfiguration",
        GetSubClusterPolicyConfigurationRequest.class, request,
        GetSubClusterPolicyConfigurationResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException {
    FederationClientMethod<SetSubClusterPolicyConfigurationResponse> clientMethod =
        new FederationClientMethod<>("setPolicyConfiguration",
        SetSubClusterPolicyConfigurationRequest.class, request,
        SetSubClusterPolicyConfigurationResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException {
    FederationClientMethod<GetSubClusterPoliciesConfigurationsResponse> clientMethod =
        new FederationClientMethod<>("getPoliciesConfigurations",
        GetSubClusterPoliciesConfigurationsRequest.class, request,
        GetSubClusterPoliciesConfigurationsResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public SubClusterRegisterResponse registerSubCluster(SubClusterRegisterRequest request)
      throws YarnException {
    FederationClientMethod<SubClusterRegisterResponse> clientMethod =
        new FederationClientMethod<>("registerSubCluster",
        SubClusterRegisterRequest.class, request,
        SubClusterRegisterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public SubClusterDeregisterResponse deregisterSubCluster(SubClusterDeregisterRequest request)
      throws YarnException {
    FederationClientMethod<SubClusterDeregisterResponse> clientMethod =
        new FederationClientMethod<>("deregisterSubCluster",
        SubClusterDeregisterRequest.class, request,
        SubClusterDeregisterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public SubClusterHeartbeatResponse subClusterHeartbeat(SubClusterHeartbeatRequest request)
      throws YarnException {
    FederationClientMethod<SubClusterHeartbeatResponse> clientMethod =
        new FederationClientMethod<>("subClusterHeartbeat",
        SubClusterHeartbeatRequest.class, request,
        SubClusterHeartbeatResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public GetSubClusterInfoResponse getSubCluster(GetSubClusterInfoRequest request)
      throws YarnException {
    FederationClientMethod<GetSubClusterInfoResponse> clientMethod =
        new FederationClientMethod<>("getSubCluster",
        GetSubClusterInfoRequest.class, request,
        GetSubClusterInfoResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public GetSubClustersInfoResponse getSubClusters(GetSubClustersInfoRequest request)
      throws YarnException {
    FederationClientMethod<GetSubClustersInfoResponse> clientMethod =
        new FederationClientMethod<>("getSubClusters",
        GetSubClustersInfoRequest.class, request,
        GetSubClustersInfoResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException {
    FederationClientMethod<AddApplicationHomeSubClusterResponse> clientMethod =
        new FederationClientMethod<>("addApplicationHomeSubCluster",
        AddApplicationHomeSubClusterRequest.class, request,
        AddApplicationHomeSubClusterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubCluster(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException {
    FederationClientMethod<UpdateApplicationHomeSubClusterResponse> clientMethod =
        new FederationClientMethod<>("updateApplicationHomeSubCluster",
        AddApplicationHomeSubClusterRequest.class, request,
        UpdateApplicationHomeSubClusterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException {
    FederationClientMethod<GetApplicationHomeSubClusterResponse> clientMethod =
        new FederationClientMethod<>("getApplicationHomeSubCluster",
        GetApplicationHomeSubClusterRequest.class, request,
        GetApplicationHomeSubClusterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException {
    FederationClientMethod<GetApplicationsHomeSubClusterResponse> clientMethod =
        new FederationClientMethod<>("getApplicationsHomeSubCluster",
        GetApplicationsHomeSubClusterRequest.class, request,
        GetApplicationsHomeSubClusterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public DeleteApplicationHomeSubClusterResponse deleteApplicationHomeSubCluster(
      DeleteApplicationHomeSubClusterRequest request) throws YarnException {
    FederationClientMethod<DeleteApplicationHomeSubClusterResponse> clientMethod =
        new FederationClientMethod<>("deleteApplicationHomeSubCluster",
        DeleteApplicationHomeSubClusterRequest.class, request,
        DeleteApplicationHomeSubClusterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public AddReservationHomeSubClusterResponse addReservationHomeSubCluster(
      AddReservationHomeSubClusterRequest request) throws YarnException {
    FederationClientMethod<AddReservationHomeSubClusterResponse> clientMethod =
        new FederationClientMethod<>("addReservationHomeSubCluster",
        AddReservationHomeSubClusterRequest.class, request,
        AddReservationHomeSubClusterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public GetReservationHomeSubClusterResponse getReservationHomeSubCluster(
      GetReservationHomeSubClusterRequest request) throws YarnException {
    FederationClientMethod<GetReservationHomeSubClusterResponse> clientMethod =
        new FederationClientMethod<>("getReservationHomeSubCluster",
        GetReservationHomeSubClusterRequest.class, request,
        GetReservationHomeSubClusterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public GetReservationsHomeSubClusterResponse getReservationsHomeSubCluster(
      GetReservationsHomeSubClusterRequest request) throws YarnException {
    FederationClientMethod<GetReservationsHomeSubClusterResponse> clientMethod =
        new FederationClientMethod<>("getReservationsHomeSubCluster",
        GetReservationsHomeSubClusterRequest.class, request,
        GetReservationsHomeSubClusterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public UpdateReservationHomeSubClusterResponse updateReservationHomeSubCluster(
      UpdateReservationHomeSubClusterRequest request) throws YarnException {
    FederationClientMethod<UpdateReservationHomeSubClusterResponse> clientMethod =
        new FederationClientMethod<>("updateReservationHomeSubCluster",
        GetReservationsHomeSubClusterRequest.class, request,
        UpdateReservationHomeSubClusterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public DeleteReservationHomeSubClusterResponse deleteReservationHomeSubCluster(
      DeleteReservationHomeSubClusterRequest request) throws YarnException {
    FederationClientMethod<DeleteReservationHomeSubClusterResponse> clientMethod =
        new FederationClientMethod<>("deleteReservationHomeSubCluster",
        DeleteReservationHomeSubClusterRequest.class, request,
        DeleteReservationHomeSubClusterResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public RouterMasterKeyResponse storeNewMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {
    FederationClientMethod<RouterMasterKeyResponse> clientMethod = new FederationClientMethod<>(
        "storeNewMasterKey",
        RouterMasterKeyRequest.class, request,
        RouterMasterKeyResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public RouterMasterKeyResponse removeStoredMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {
    FederationClientMethod<RouterMasterKeyResponse> clientMethod = new FederationClientMethod<>(
        "removeStoredMasterKey",
        RouterMasterKeyRequest.class, request,
        RouterMasterKeyResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public RouterMasterKeyResponse getMasterKeyByDelegationKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {
    FederationClientMethod<RouterMasterKeyResponse> clientMethod = new FederationClientMethod<>(
        "getMasterKeyByDelegationKey",
        RouterMasterKeyRequest.class, request,
        RouterMasterKeyResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public RouterRMTokenResponse storeNewToken(RouterRMTokenRequest request)
      throws YarnException, IOException {
    FederationClientMethod<RouterRMTokenResponse> clientMethod = new FederationClientMethod<>(
        "storeNewToken",
        RouterRMTokenRequest.class, request,
        RouterRMTokenResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public RouterRMTokenResponse updateStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException {
    FederationClientMethod<RouterRMTokenResponse> clientMethod = new FederationClientMethod<>(
        "updateStoredToken",
        RouterRMTokenRequest.class, request,
        RouterRMTokenResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public RouterRMTokenResponse removeStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException {
    FederationClientMethod<RouterRMTokenResponse> clientMethod = new FederationClientMethod<>(
        "removeStoredToken",
        RouterRMTokenRequest.class, request,
        RouterRMTokenResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public RouterRMTokenResponse getTokenByRouterStoreToken(RouterRMTokenRequest request)
      throws YarnException, IOException {
    FederationClientMethod<RouterRMTokenResponse> clientMethod = new FederationClientMethod<>(
        "getTokenByRouterStoreToken",
        RouterRMTokenRequest.class, request,
        RouterRMTokenResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public int incrementDelegationTokenSeqNum() {
    return stateStoreClient.incrementDelegationTokenSeqNum();
  }

  @Override
  public int getDelegationTokenSeqNum() {
    return stateStoreClient.getDelegationTokenSeqNum();
  }

  @Override
  public void setDelegationTokenSeqNum(int seqNum) {
    stateStoreClient.setDelegationTokenSeqNum(seqNum);
  }

  @Override
  public int getCurrentKeyId() {
    return stateStoreClient.getCurrentKeyId();
  }

  @Override
  public int incrementCurrentKeyId() {
    return stateStoreClient.incrementCurrentKeyId();
  }

  /**
   * Create a thread that cleans up the app.
   * @param stage rm-start/rm-stop.
   */
  public void createCleanUpFinishApplicationThread(String stage) {
    String threadName = cleanUpThreadNamePrefix + "-" + stage;
    Thread finishApplicationThread = new Thread(createCleanUpFinishApplicationThread());
    finishApplicationThread.setName(threadName);
    finishApplicationThread.start();
    LOG.info("CleanUpFinishApplicationThread has been started {}.", threadName);
  }

  /**
   * Create a thread that cleans up the apps.
   *
   * @return thread object.
   */
  private Runnable createCleanUpFinishApplicationThread() {
    return () -> {
      createCleanUpFinishApplication();
    };
  }

  /**
   * cleans up the apps.
   */
  private void createCleanUpFinishApplication() {
    try {
      // Get the current RM's App list based on subClusterId
      GetApplicationsHomeSubClusterRequest request =
          GetApplicationsHomeSubClusterRequest.newInstance(subClusterId);
      GetApplicationsHomeSubClusterResponse response =
          getApplicationsHomeSubCluster(request);
      List<ApplicationHomeSubCluster> applicationHomeSCs = response.getAppsHomeSubClusters();

      // Traverse the app list and clean up the app.
      long successCleanUpAppCount = 0;

      // Save a local copy of the map so that it won't change with the map
      Map<ApplicationId, RMApp> rmApps = new HashMap<>(this.rmContext.getRMApps());

      // Need to make sure there is app list in RM memory.
      if (rmApps != null && !rmApps.isEmpty()) {
        for (ApplicationHomeSubCluster applicationHomeSC : applicationHomeSCs) {
          ApplicationId applicationId = applicationHomeSC.getApplicationId();
          if (!rmApps.containsKey(applicationId)) {
            try {
              Boolean cleanUpSuccess = cleanUpFinishApplicationsWithRetries(applicationId, false);
              if (cleanUpSuccess) {
                LOG.info("application = {} has been cleaned up successfully.", applicationId);
                successCleanUpAppCount++;
              }
            } catch (Exception e) {
              LOG.error("problem during application = {} cleanup.", applicationId, e);
            }
          }
        }
      }

      // print app cleanup log
      LOG.info("cleanup finished applications size = {}, number = {} successful cleanup.",
          applicationHomeSCs.size(), successCleanUpAppCount);
    } catch (Exception e) {
      LOG.error("problem during cleanup applications.", e);
    }
  }

  /**
   * Clean up the federation completed Application.
   *
   * @param appId app id.
   * @param isQuery true, need to query from statestore, false not query.
   * @throws Exception exception occurs.
   * @return true, successfully deleted; false, failed to delete or no need to delete
   */
  public boolean cleanUpFinishApplicationsWithRetries(ApplicationId appId, boolean isQuery)
      throws Exception {

    // Generate a request to delete data
    DeleteApplicationHomeSubClusterRequest req =
        DeleteApplicationHomeSubClusterRequest.newInstance(appId);

    // CleanUp Finish App.
    return ((FederationActionRetry<Boolean>) (retry) -> invokeCleanUpFinishApp(appId, isQuery, req))
        .runWithRetries(cleanUpRetryCountNum, cleanUpRetrySleepTime);
  }

  /**
   * CleanUp Finish App.
   *
   * @param applicationId app id.
   * @param isQuery true, need to query from statestore, false not query.
   * @param delRequest delete Application Request
   * @return true, successfully deleted; false, failed to delete or no need to delete
   * @throws YarnException
   */
  private boolean invokeCleanUpFinishApp(ApplicationId applicationId, boolean isQuery,
      DeleteApplicationHomeSubClusterRequest delRequest) throws YarnException {
    boolean isAppNeedClean = true;
    // If we need to query the StateStore
    if (isQuery) {
      isAppNeedClean = isApplicationNeedClean(applicationId);
    }
    // When the App needs to be cleaned up, clean up the App.
    if (isAppNeedClean) {
      DeleteApplicationHomeSubClusterResponse response =
          deleteApplicationHomeSubCluster(delRequest);
      if (response != null) {
        LOG.info("The applicationId = {} has been successfully cleaned up.", applicationId);
        return true;
      }
    }
    return false;
  }

  /**
   * Used to determine whether the Application is cleaned up.
   *
   * When the app in the RM is completed,
   * the HomeSC corresponding to the app will be queried in the StateStore.
   * If the current RM is the HomeSC, the completed app will be cleaned up.
   *
   * @param applicationId applicationId
   * @return true, app needs to be cleaned up;
   *         false, app doesn't need to be cleaned up.
   */
  private boolean isApplicationNeedClean(ApplicationId applicationId) {
    GetApplicationHomeSubClusterRequest queryRequest =
            GetApplicationHomeSubClusterRequest.newInstance(applicationId);
    // Here we need to use try...catch,
    // because getApplicationHomeSubCluster may throw not exist exception
    try {
      GetApplicationHomeSubClusterResponse queryResp =
          getApplicationHomeSubCluster(queryRequest);
      if (queryResp != null) {
        ApplicationHomeSubCluster appHomeSC = queryResp.getApplicationHomeSubCluster();
        SubClusterId homeSubClusterId = appHomeSC.getHomeSubCluster();
        if (!subClusterId.equals(homeSubClusterId)) {
          LOG.warn("The homeSubCluster of applicationId = {} belong subCluster = {}, " +
              " not belong subCluster = {} and is not allowed to delete.",
              applicationId, homeSubClusterId, subClusterId);
          return false;
        }
      } else {
        LOG.warn("The applicationId = {} not belong subCluster = {} " +
            " and is not allowed to delete.", applicationId, subClusterId);
        return false;
      }
    } catch (Exception e) {
      LOG.warn("query applicationId = {} error.", applicationId, e);
      return false;
    }
    return true;
  }
}