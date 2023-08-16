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

package org.apache.hadoop.yarn.server.router.clientrm;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.router.security.authorize.RouterPolicyProvider;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * RouterClientRMService is a service that runs on each router that can be used
 * to intercept and inspect {@link ApplicationClientProtocol} messages from
 * client to the cluster resource manager. It listens
 * {@link ApplicationClientProtocol} messages from the client and creates a
 * request intercepting pipeline instance for each client. The pipeline is a
 * chain of {@link ClientRequestInterceptor} instances that can inspect and
 * modify the request/response as needed. The main difference with
 * AMRMProxyService is the protocol they implement.
 */
public class RouterClientRMService extends AbstractService
    implements ApplicationClientProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterClientRMService.class);

  private Server server;
  private InetSocketAddress listenerEndpoint;

  // For each user we store an interceptors' pipeline.
  // For performance issue we use LRU cache to keep in memory the newest ones
  // and remove the oldest used ones.
  private Map<String, RequestInterceptorChainWrapper> userPipelineMap;

  private URL redirectURL;
  private RouterDelegationTokenSecretManager routerDTSecretManager;

  public RouterClientRMService() {
    super(RouterClientRMService.class.getName());
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting Router ClientRMService.");
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    UserGroupInformation.setConfiguration(conf);

    this.listenerEndpoint =
        conf.getSocketAddr(YarnConfiguration.ROUTER_BIND_HOST,
            YarnConfiguration.ROUTER_CLIENTRM_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_PORT);

    if (RouterServerUtil.isRouterWebProxyEnable(conf)) {
      redirectURL = getRedirectURL();
    }

    int maxCacheSize =
        conf.getInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
            YarnConfiguration.DEFAULT_ROUTER_PIPELINE_CACHE_MAX_SIZE);
    this.userPipelineMap = Collections.synchronizedMap(new LRUCacheHashMap<>(maxCacheSize, true));

    Configuration serverConf = new Configuration(conf);

    int numWorkerThreads =
        serverConf.getInt(YarnConfiguration.RM_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_CLIENT_THREAD_COUNT);

    // Initialize RouterRMDelegationTokenSecretManager.
    routerDTSecretManager = createRouterRMDelegationTokenSecretManager(conf);
    routerDTSecretManager.startThreads();

    this.server = rpc.getServer(ApplicationClientProtocol.class, this,
        listenerEndpoint, serverConf, routerDTSecretManager, numWorkerThreads);

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      refreshServiceAcls(conf, RouterPolicyProvider.getInstance());
    }

    this.server.start();
    LOG.info("Router ClientRMService listening on address: {}.", this.server.getListenerAddress());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping Router ClientRMService.");
    if (this.server != null) {
      this.server.stop();
    }
    userPipelineMap.clear();
    super.serviceStop();
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getNewApplication(request);
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().submitApplication(request);
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().forceKillApplication(request);
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getClusterMetrics(request);
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getClusterNodes(request);
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getQueueInfo(request);
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getQueueUserAcls(request);
  }

  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().moveApplicationAcrossQueues(request);
  }

  @Override
  public GetNewReservationResponse getNewReservation(
      GetNewReservationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getNewReservation(request);
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().submitReservation(request);
  }

  @Override
  public ReservationListResponse listReservations(
      ReservationListRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().listReservations(request);
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().updateReservation(request);
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().deleteReservation(request);
  }

  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getNodeToLabels(request);
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getLabelsToNodes(request);
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getClusterNodeLabels(request);
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    GetApplicationReportResponse response = pipeline.getRootInterceptor()
        .getApplicationReport(request);
    if (RouterServerUtil.isRouterWebProxyEnable(getConfig())) {
      // After redirect url, tracking url in application report will
      // redirect to embeded proxy server of router
      URL url = new URL(response.getApplicationReport().getTrackingUrl());
      String redirectUrl = new URL(redirectURL.getProtocol(),
          redirectURL.getHost(), redirectURL.getPort(), url.getFile())
          .toString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("The tracking url of application {} is redirect from {} to {}",
            response.getApplicationReport().getApplicationId(), url, redirectUrl);
      }
      response.getApplicationReport().setTrackingUrl(redirectUrl);
    }
    return response;
  }

  @Override
  public GetApplicationsResponse getApplications(GetApplicationsRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getApplications(request);
  }

  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getApplicationAttemptReport(request);
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getApplicationAttempts(request);
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getContainerReport(request);
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getContainers(request);
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getDelegationToken(request);
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().renewDelegationToken(request);
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().cancelDelegationToken(request);
  }

  @Override
  public FailApplicationAttemptResponse failApplicationAttempt(
      FailApplicationAttemptRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().failApplicationAttempt(request);
  }

  @Override
  public UpdateApplicationPriorityResponse updateApplicationPriority(
      UpdateApplicationPriorityRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().updateApplicationPriority(request);
  }

  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().signalToContainer(request);
  }

  @Override
  public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
      UpdateApplicationTimeoutsRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().updateApplicationTimeouts(request);
  }

  @Override
  public GetAllResourceProfilesResponse getResourceProfiles(
      GetAllResourceProfilesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getResourceProfiles(request);
  }

  @Override
  public GetResourceProfileResponse getResourceProfile(
      GetResourceProfileRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getResourceProfile(request);
  }

  @Override
  public GetAllResourceTypeInfoResponse getResourceTypeInfo(
      GetAllResourceTypeInfoRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getResourceTypeInfo(request);
  }

  @Override
  public GetAttributesToNodesResponse getAttributesToNodes(
      GetAttributesToNodesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getAttributesToNodes(request);
  }

  @Override
  public GetClusterNodeAttributesResponse getClusterNodeAttributes(
      GetClusterNodeAttributesRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getClusterNodeAttributes(request);
  }

  @Override
  public GetNodesToAttributesResponse getNodesToAttributes(
      GetNodesToAttributesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getNodesToAttributes(request);
  }

  @VisibleForTesting
  public RequestInterceptorChainWrapper getInterceptorChain()
      throws IOException {
    String user = UserGroupInformation.getCurrentUser().getUserName();
    RequestInterceptorChainWrapper chain = userPipelineMap.get(user);
    if (chain != null && chain.getRootInterceptor() != null) {
      return chain;
    }
    return initializePipeline(user);
  }

  void refreshServiceAcls(Configuration configuration,
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  /**
   * Gets the Request interceptor chains for all the users.
   *
   * @return the request interceptor chains.
   */
  @VisibleForTesting
  protected Map<String, RequestInterceptorChainWrapper> getPipelines() {
    return this.userPipelineMap;
  }

  /**
   * This method creates and returns reference of the first interceptor in the
   * chain of request interceptor instances.
   *
   * @return the reference of the first interceptor in the chain
   */
  @VisibleForTesting
  protected ClientRequestInterceptor createRequestInterceptorChain() {
    Configuration conf = getConfig();
    return RouterServerUtil.createRequestInterceptorChain(conf,
        YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
        YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_INTERCEPTOR_CLASS,
        ClientRequestInterceptor.class);
  }

  /**
   * Initializes the request interceptor pipeline for the specified application.
   *
   * @param user
   */
  private RequestInterceptorChainWrapper initializePipeline(String user) {
    synchronized (this.userPipelineMap) {
      if (this.userPipelineMap.containsKey(user)) {
        LOG.info("Request to start an already existing user: {}"
            + " was received, so ignoring.", user);
        return userPipelineMap.get(user);
      }

      RequestInterceptorChainWrapper chainWrapper =
          new RequestInterceptorChainWrapper();
      try {
        // We should init the pipeline instance after it is created and then
        // add to the map, to ensure thread safe.
        LOG.info("Initializing request processing pipeline for application for the user: {}.",
            user);

        ClientRequestInterceptor interceptorChain =
            this.createRequestInterceptorChain();
        interceptorChain.init(user);

        // We set the RouterDelegationTokenSecretManager instance to the interceptorChain
        // and let the interceptor use it.
        if (routerDTSecretManager != null) {
          interceptorChain.setTokenSecretManager(routerDTSecretManager);
        }

        chainWrapper.init(interceptorChain);
      } catch (Exception e) {
        LOG.error("Init ClientRequestInterceptor error for user: {}.", user, e);
        throw e;
      }

      this.userPipelineMap.put(user, chainWrapper);
      return chainWrapper;
    }
  }

  /**
   * Private structure for encapsulating RequestInterceptor and user instances.
   *
   */
  @Private
  public static class RequestInterceptorChainWrapper {
    private ClientRequestInterceptor rootInterceptor;

    /**
     * Initializes the wrapper with the specified parameters.
     *
     * @param interceptor the first interceptor in the pipeline
     */
    public synchronized void init(ClientRequestInterceptor interceptor) {
      this.rootInterceptor = interceptor;
    }

    /**
     * Gets the root request interceptor.
     *
     * @return the root request interceptor
     */
    public synchronized ClientRequestInterceptor getRootInterceptor() {
      return rootInterceptor;
    }

    /**
     * Shutdown the chain of interceptors when the object is destroyed.
     */
    @Override
    protected void finalize() {
      rootInterceptor.shutdown();
    }
  }

  @VisibleForTesting
  public Map<String, RequestInterceptorChainWrapper> getUserPipelineMap() {
    return userPipelineMap;
  }

  /**
   * Create RouterRMDelegationTokenSecretManager.
   * In the YARN federation, the Router will replace the RM to
   * manage the RMDelegationToken (generate, update, cancel),
   * so the relevant configuration parameters still obtain the configuration parameters of the RM.
   *
   * @param conf Configuration
   * @return RouterDelegationTokenSecretManager.
   */
  protected RouterDelegationTokenSecretManager createRouterRMDelegationTokenSecretManager(
      Configuration conf) {

    long secretKeyInterval = conf.getLong(
        YarnConfiguration.RM_DELEGATION_KEY_UPDATE_INTERVAL_KEY,
        YarnConfiguration.RM_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);

    long tokenMaxLifetime = conf.getLong(
        YarnConfiguration.RM_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
        YarnConfiguration.RM_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);

    long tokenRenewInterval = conf.getLong(
        YarnConfiguration.RM_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
        YarnConfiguration.RM_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);

    long removeScanInterval = conf.getTimeDuration(
        YarnConfiguration.RM_DELEGATION_TOKEN_REMOVE_SCAN_INTERVAL_KEY,
        YarnConfiguration.RM_DELEGATION_TOKEN_REMOVE_SCAN_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    return new RouterDelegationTokenSecretManager(secretKeyInterval,
        tokenMaxLifetime, tokenRenewInterval, removeScanInterval);
  }

  @VisibleForTesting
  public RouterDelegationTokenSecretManager getRouterDTSecretManager() {
    return routerDTSecretManager;
  }

  @VisibleForTesting
  public void setRouterDTSecretManager(RouterDelegationTokenSecretManager routerDTSecretManager) {
    this.routerDTSecretManager = routerDTSecretManager;
  }

  @VisibleForTesting
  public void initUserPipelineMap(Configuration conf) {
    int maxCacheSize = conf.getInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
        YarnConfiguration.DEFAULT_ROUTER_PIPELINE_CACHE_MAX_SIZE);
    this.userPipelineMap = Collections.synchronizedMap(new LRUCacheHashMap<>(maxCacheSize, true));
  }

  private URL getRedirectURL() throws Exception {
    Configuration conf = getConfig();
    String webAppAddress = WebAppUtils.getWebAppBindURL(conf, YarnConfiguration.ROUTER_BIND_HOST,
        WebAppUtils.getRouterWebAppURLWithoutScheme(conf));
    String[] hostPort = StringUtils.split(webAppAddress, ':');
    if (hostPort.length != 2) {
      throw new YarnRuntimeException("Router can't get valid redirect proxy url");
    }
    String host = hostPort[0];
    int port = Integer.parseInt(hostPort[1]);
    if (StringUtils.isBlank(host) || host.equals("0.0.0.0")) {
      host = InetAddress.getLocalHost().getCanonicalHostName();
    }
    return new URL(YarnConfiguration.useHttps(this.getConfig()) ? "https" : "http", host, port, "");
  }
}
