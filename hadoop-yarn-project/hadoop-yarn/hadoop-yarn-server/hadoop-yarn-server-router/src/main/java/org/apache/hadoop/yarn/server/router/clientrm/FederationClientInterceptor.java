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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.RouterPolicyFacade;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.retry.FederationActionRetry;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.RouterAuditLogger;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NEW_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SUBMIT_NEW_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APP_REPORT;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.FORCE_KILL_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.TARGET_CLIENT_RM_SERVICE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UNKNOWN;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CLUSTERNODES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_QUEUE_USER_ACLS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APPLICATIONS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CLUSTERMETRICS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_QUEUEINFO;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.MOVE_APPLICATION_ACROSS_QUEUES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NEW_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SUBMIT_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.LIST_RESERVATIONS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UPDATE_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.DELETE_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NODETOLABELS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_LABELSTONODES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CLUSTERNODELABELS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APPLICATION_ATTEMPT_REPORT;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APPLICATION_ATTEMPTS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CONTAINERREPORT;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CONTAINERS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_DELEGATIONTOKEN;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.RENEW_DELEGATIONTOKEN;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.CANCEL_DELEGATIONTOKEN;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.FAIL_APPLICATIONATTEMPT;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UPDATE_APPLICATIONPRIORITY;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SIGNAL_TOCONTAINER;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UPDATE_APPLICATIONTIMEOUTS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_RESOURCEPROFILES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_RESOURCEPROFILE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_RESOURCETYPEINFO;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_ATTRIBUTESTONODES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CLUSTERNODEATTRIBUTES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NODESTOATTRIBUTES;

/**
 * Extends the {@code AbstractRequestInterceptorClient} class and provides an
 * implementation for federation of YARN RM and scaling an application across
 * multiple YARN SubClusters. All the federation specific implementation is
 * encapsulated in this class. This is always the last interceptor in the chain.
 */
public class FederationClientInterceptor
    extends AbstractClientRequestInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationClientInterceptor.class);

  private int numSubmitRetries;
  private Map<SubClusterId, ApplicationClientProtocol> clientRMProxies;
  private FederationStateStoreFacade federationFacade;
  private Random rand;
  private RouterPolicyFacade policyFacade;
  private RouterMetrics routerMetrics;
  private ThreadPoolExecutor executorService;
  private final Clock clock = new MonotonicClock();
  private boolean returnPartialReport;
  private long submitIntervalTime;

  @Override
  public void init(String userName) {
    super.init(userName);

    federationFacade = FederationStateStoreFacade.getInstance(getConf());
    rand = new Random(System.currentTimeMillis());

    int numMinThreads = getNumMinThreads(getConf());

    int numMaxThreads = getNumMaxThreads(getConf());

    long keepAliveTime = getConf().getTimeDuration(
        YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_KEEP_ALIVE_TIME,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREAD_POOL_KEEP_ALIVE_TIME, TimeUnit.SECONDS);

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("RPC Router Client-" + userName + "-%d ").build();

    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    this.executorService = new ThreadPoolExecutor(numMinThreads, numMaxThreads,
        keepAliveTime, TimeUnit.MILLISECONDS, workQueue, threadFactory);

    // Adding this line so that unused user threads will exit and be cleaned up if idle for too long
    this.executorService.allowCoreThreadTimeOut(true);

    final Configuration conf = this.getConf();

    try {
      policyFacade = new RouterPolicyFacade(conf, federationFacade,
          this.federationFacade.getSubClusterResolver(), null);
    } catch (FederationPolicyInitializationException e) {
      LOG.error(e.getMessage());
    }

    numSubmitRetries = conf.getInt(
        YarnConfiguration.ROUTER_CLIENTRM_SUBMIT_RETRY,
        YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_SUBMIT_RETRY);

    submitIntervalTime = conf.getTimeDuration(
        YarnConfiguration.ROUTER_CLIENTRM_SUBMIT_INTERVAL_TIME,
        YarnConfiguration.DEFAULT_CLIENTRM_SUBMIT_INTERVAL_TIME, TimeUnit.MILLISECONDS);

    clientRMProxies = new ConcurrentHashMap<>();
    routerMetrics = RouterMetrics.getMetrics();

    returnPartialReport = conf.getBoolean(
        YarnConfiguration.ROUTER_CLIENTRM_PARTIAL_RESULTS_ENABLED,
        YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_PARTIAL_RESULTS_ENABLED);
  }

  @Override
  public void setNextInterceptor(ClientRequestInterceptor next) {
    throw new YarnRuntimeException("setNextInterceptor is being called on "
        + "FederationClientRequestInterceptor, which should be the last one "
        + "in the chain. Check if the interceptor pipeline configuration "
        + "is correct");
  }

  @VisibleForTesting
  protected ApplicationClientProtocol getClientRMProxyForSubCluster(
      SubClusterId subClusterId) throws YarnException {

    if (clientRMProxies.containsKey(subClusterId)) {
      return clientRMProxies.get(subClusterId);
    }

    ApplicationClientProtocol clientRMProxy = null;
    try {
      boolean serviceAuthEnabled = getConf().getBoolean(
          CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
      UserGroupInformation realUser = user;
      if (serviceAuthEnabled) {
        realUser = UserGroupInformation.createProxyUser(
            user.getShortUserName(), UserGroupInformation.getLoginUser());
      }
      clientRMProxy = FederationProxyProviderUtil.createRMProxy(getConf(),
          ApplicationClientProtocol.class, subClusterId, realUser);
    } catch (Exception e) {
      RouterServerUtil.logAndThrowException(
          "Unable to create the interface to reach the SubCluster " + subClusterId, e);
    }
    clientRMProxies.put(subClusterId, clientRMProxy);
    return clientRMProxy;
  }

  private SubClusterId getRandomActiveSubCluster(
      Map<SubClusterId, SubClusterInfo> activeSubClusters) throws YarnException {
    if (activeSubClusters == null || activeSubClusters.isEmpty()) {
      RouterServerUtil.logAndThrowException(
          FederationPolicyUtils.NO_ACTIVE_SUBCLUSTER_AVAILABLE, null);
    }
    List<SubClusterId> list = new ArrayList<>(activeSubClusters.keySet());
    return list.get(rand.nextInt(list.size()));
  }

  /**
   * YARN Router forwards every getNewApplication requests to any RM. During
   * this operation there will be no communication with the State Store. The
   * Router will forward the requests to any SubCluster. The Router will retry
   * to submit the request on #numSubmitRetries different SubClusters. The
   * SubClusters are randomly chosen from the active ones.
   *
   * Possible failures and behaviors:
   *
   * Client: identical behavior as {@code ClientRMService}.
   *
   * Router: the Client will timeout and resubmit.
   *
   * ResourceManager: the Router will timeout and contacts another RM.
   *
   * StateStore: not in the execution.
   */
  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException, IOException {

    if (request == null) {
      routerMetrics.incrAppsFailedCreated();
      String errMsg = "Missing getNewApplication request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_NEW_APP, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, errMsg);
      RouterServerUtil.logAndThrowException(errMsg, null);
    }

    long startTime = clock.getTime();
    Map<SubClusterId, SubClusterInfo> subClustersActive =
        federationFacade.getSubClusters(true);

    // Try calling the getNewApplication method
    List<SubClusterId> blacklist = new ArrayList<>();
    int activeSubClustersCount = federationFacade.getActiveSubClustersCount();
    int actualRetryNums = Math.min(activeSubClustersCount, numSubmitRetries);

    try {
      GetNewApplicationResponse response =
          ((FederationActionRetry<GetNewApplicationResponse>) (retryCount) ->
          invokeGetNewApplication(subClustersActive, blacklist, request, retryCount)).
          runWithRetries(actualRetryNums, submitIntervalTime);

      if (response != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededAppsCreated(stopTime - startTime);
        return response;
      }
    } catch (Exception e) {
      routerMetrics.incrAppsFailedCreated();
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_NEW_APP, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, e.getMessage());
      RouterServerUtil.logAndThrowException(e.getMessage(), e);
    }

    routerMetrics.incrAppsFailedCreated();
    String errMsg = "Failed to create a new application.";
    RouterAuditLogger.logFailure(user.getShortUserName(), GET_NEW_APP, UNKNOWN,
        TARGET_CLIENT_RM_SERVICE, errMsg);
    throw new YarnException(errMsg);
  }

  /**
   * Invoke GetNewApplication to different subClusters.
   *
   * @param subClustersActive Active SubClusters
   * @param blackList Blacklist avoid repeated calls to unavailable subCluster.
   * @param request getNewApplicationRequest.
   * @param retryCount number of retries.
   * @return Get NewApplicationResponse response, If the response is empty, the request fails,
   * if the response is not empty, the request is successful.
   * @throws YarnException yarn exception.
   * @throws IOException io error.
   */
  private GetNewApplicationResponse invokeGetNewApplication(
      Map<SubClusterId, SubClusterInfo> subClustersActive,
      List<SubClusterId> blackList, GetNewApplicationRequest request, int retryCount)
      throws YarnException, IOException {
    SubClusterId subClusterId =
        federationFacade.getRandomActiveSubCluster(subClustersActive, blackList);
    LOG.info("getNewApplication try #{} on SubCluster {}.", retryCount, subClusterId);
    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    try {
      GetNewApplicationResponse response = clientRMProxy.getNewApplication(request);
      if (response != null) {
        RouterAuditLogger.logSuccess(user.getShortUserName(), GET_NEW_APP,
            TARGET_CLIENT_RM_SERVICE, response.getApplicationId(), subClusterId);
        return response;
      }
    } catch (Exception e) {
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_NEW_APP, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, e.getMessage(), subClusterId);
      LOG.warn("Unable to create a new ApplicationId in SubCluster {}.", subClusterId.getId(), e);
      blackList.add(subClusterId);
      throw e;
    }
    // If SubmitApplicationResponse is empty, the request fails.
    String msg = String.format("Unable to create a new ApplicationId in SubCluster %s.",
        subClusterId.getId());
    throw new YarnException(msg);
  }

  /**
   * Today, in YARN there are no checks of any applicationId submitted.
   *
   * Base scenarios:
   *
   * The Client submits an application to the Router. The Router selects one
   * SubCluster to forward the request. The Router inserts a tuple into
   * StateStore with the selected SubCluster (e.g. SC1) and the appId. The
   * State Store replies with the selected SubCluster (e.g. SC1). The Router
   * submits the request to the selected SubCluster.
   *
   * In case of State Store failure:
   *
   * The client submits an application to the Router. The Router selects one
   * SubCluster to forward the request. The Router inserts a tuple into State
   * Store with the selected SubCluster (e.g. SC1) and the appId. Due to the
   * State Store down the Router times out and it will retry depending on the
   * FederationFacade settings. The Router replies to the client with an error
   * message.
   *
   * If State Store fails after inserting the tuple: identical behavior as
   * {@code ClientRMService}.
   *
   * In case of Router failure:
   *
   * Scenario 1 – Crash before submission to the ResourceManager
   *
   * The Client submits an application to the Router. The Router selects one
   * SubCluster to forward the request. The Router inserts a tuple into State
   * Store with the selected SubCluster (e.g. SC1) and the appId. The Router
   * crashes. The Client timeouts and resubmits the application. The Router
   * selects one SubCluster to forward the request. The Router inserts a tuple
   * into State Store with the selected SubCluster (e.g. SC2) and the appId.
   * Because the tuple is already inserted in the State Store, it returns the
   * previous selected SubCluster (e.g. SC1). The Router submits the request
   * to the selected SubCluster (e.g. SC1).
   *
   * Scenario 2 – Crash after submission to the ResourceManager
   *
   * The Client submits an application to the Router. The Router selects one
   * SubCluster to forward the request. The Router inserts a tuple into State
   * Store with the selected SubCluster (e.g. SC1) and the appId. The Router
   * submits the request to the selected SubCluster. The Router crashes. The
   * Client timeouts and resubmit the application. The Router selects one
   * SubCluster to forward the request. The Router inserts a tuple into State
   * Store with the selected SubCluster (e.g. SC2) and the appId. The State
   * Store replies with the selected SubCluster (e.g. SC1). The Router submits
   * the request to the selected SubCluster (e.g. SC1). When a client re-submits
   * the same application to the same RM, it does not raise an exception and
   * replies with operation successful message.
   *
   * In case of Client failure: identical behavior as {@code ClientRMService}.
   *
   * In case of ResourceManager failure:
   *
   * The Client submits an application to the Router. The Router selects one
   * SubCluster to forward the request. The Router inserts a tuple into State
   * Store with the selected SubCluster (e.g. SC1) and the appId. The Router
   * submits the request to the selected SubCluster. The entire SubCluster is
   * down – all the RMs in HA or the master RM is not reachable. The Router
   * times out. The Router selects a new SubCluster to forward the request.
   * The Router update a tuple into State Store with the selected SubCluster
   * (e.g. SC2) and the appId. The State Store replies with OK answer. The
   * Router submits the request to the selected SubCluster (e.g. SC2).
   */
  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {

    if (request == null || request.getApplicationSubmissionContext() == null
        || request.getApplicationSubmissionContext().getApplicationId() == null) {
      routerMetrics.incrAppsFailedSubmitted();
      String errMsg =
          "Missing submitApplication request or applicationSubmissionContext information.";
      RouterAuditLogger.logFailure(user.getShortUserName(), SUBMIT_NEW_APP, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, errMsg);
      RouterServerUtil.logAndThrowException(errMsg, null);
    }

    long startTime = clock.getTime();
    ApplicationId applicationId =
        request.getApplicationSubmissionContext().getApplicationId();
    List<SubClusterId> blacklist = new ArrayList<>();

    try {

      // We need to handle this situation,
      // the user will provide us with an expected submitRetries,
      // but if the number of Active SubClusters is less than this number at this time,
      // we should provide a high number of retry according to the number of Active SubClusters.
      int activeSubClustersCount = federationFacade.getActiveSubClustersCount();
      int actualRetryNums = Math.min(activeSubClustersCount, numSubmitRetries);

      // Try calling the SubmitApplication method
      SubmitApplicationResponse response =
          ((FederationActionRetry<SubmitApplicationResponse>) (retryCount) ->
          invokeSubmitApplication(blacklist, request, retryCount)).
          runWithRetries(actualRetryNums, submitIntervalTime);

      if (response != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededAppsSubmitted(stopTime - startTime);
        return response;
      }

    } catch (Exception e) {
      routerMetrics.incrAppsFailedSubmitted();
      RouterAuditLogger.logFailure(user.getShortUserName(), SUBMIT_NEW_APP, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, e.getMessage(), applicationId);
      RouterServerUtil.logAndThrowException(e.getMessage(), e);
    }

    routerMetrics.incrAppsFailedSubmitted();
    String msg = String.format("Application %s with appId %s failed to be submitted.",
        request.getApplicationSubmissionContext().getApplicationName(), applicationId);
    RouterAuditLogger.logFailure(user.getShortUserName(), SUBMIT_NEW_APP, UNKNOWN,
        TARGET_CLIENT_RM_SERVICE, msg, applicationId);
    throw new YarnException(msg);
  }

  /**
   * Invoke SubmitApplication to different subClusters.
   *
   * Step1. Select homeSubCluster for Application according to Policy.
   *
   * Step2. Query homeSubCluster according to ApplicationId,
   * if homeSubCluster does not exist or first attempt(consider repeated submissions), write;
   * if homeSubCluster exists, update.
   *
   * Step3. Find the clientRMProxy of the corresponding cluster according to homeSubCluster,
   * and then call the SubmitApplication method.
   *
   * Step4. If SubmitApplicationResponse is empty, the request fails,
   * if SubmitApplicationResponse is not empty, the request is successful.
   *
   * @param blackList Blacklist avoid repeated calls to unavailable subCluster.
   * @param request submitApplicationRequest.
   * @param retryCount number of retries.
   * @return submitApplication response, If the response is empty, the request fails,
   *      if the response is not empty, the request is successful.
   * @throws YarnException yarn exception.
   */
  private SubmitApplicationResponse invokeSubmitApplication(
      List<SubClusterId> blackList, SubmitApplicationRequest request, int retryCount)
      throws YarnException, IOException {

    // The request is not checked here,
    // because the request has been checked before the method is called.
    // We get applicationId and subClusterId from context.
    ApplicationSubmissionContext appSubmissionContext = request.getApplicationSubmissionContext();
    ApplicationId applicationId = appSubmissionContext.getApplicationId();
    SubClusterId subClusterId = null;

    try {

      // Step1. Select homeSubCluster for Application according to Policy.
      subClusterId = policyFacade.getHomeSubcluster(appSubmissionContext, blackList);
      LOG.info("submitApplication appId {} try #{} on SubCluster {}.",
          applicationId, retryCount, subClusterId);

      // Step2. We Store the mapping relationship
      // between Application and HomeSubCluster in stateStore.
      ApplicationSubmissionContext trimmedAppSubmissionContext =
          RouterServerUtil.getTrimmedAppSubmissionContext(appSubmissionContext);
      federationFacade.addOrUpdateApplicationHomeSubCluster(
          applicationId, subClusterId, retryCount, trimmedAppSubmissionContext);

      // Step3. SubmitApplication to the subCluster
      ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
      SubmitApplicationResponse response = clientRMProxy.submitApplication(request);

      // Step4. if SubmitApplicationResponse is not empty, the request is successful.
      if (response != null) {
        LOG.info("Application {} submitted on subCluster {}.", applicationId, subClusterId);
        RouterAuditLogger.logSuccess(user.getShortUserName(), SUBMIT_NEW_APP,
            TARGET_CLIENT_RM_SERVICE, applicationId, subClusterId);
        return response;
      }
    } catch (Exception e) {
      RouterAuditLogger.logFailure(user.getShortUserName(), SUBMIT_NEW_APP, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, e.getMessage(), applicationId, subClusterId);
      LOG.warn("Unable to submitApplication appId {} try #{} on SubCluster {}.",
          applicationId, retryCount, subClusterId, e);
      if (subClusterId != null) {
        blackList.add(subClusterId);
      }
      throw e;
    }

    // If SubmitApplicationResponse is empty, the request fails.
    String msg = String.format("Application %s failed to be submitted.", applicationId);
    throw new YarnException(msg);
  }

  /**
   * The YARN Router will forward to the respective YARN RM in which the AM is
   * running.
   *
   * Possible failures and behaviors:
   *
   * Client: identical behavior as {@code ClientRMService}.
   *
   * Router: the Client will timeout and resubmit the request.
   *
   * ResourceManager: the Router will timeout and the call will fail.
   *
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {

    if (request == null || request.getApplicationId() == null) {
      routerMetrics.incrAppsFailedKilled();
      String msg = "Missing forceKillApplication request or ApplicationId.";
      RouterAuditLogger.logFailure(user.getShortUserName(), FORCE_KILL_APP, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }

    long startTime = clock.getTime();

    ApplicationId applicationId = request.getApplicationId();
    SubClusterId subClusterId = null;

    try {
      subClusterId = federationFacade
          .getApplicationHomeSubCluster(request.getApplicationId());
    } catch (YarnException e) {
      routerMetrics.incrAppsFailedKilled();
      String msg =
          String.format("Application %s does not exist in FederationStateStore.", applicationId);
      RouterAuditLogger.logFailure(user.getShortUserName(), FORCE_KILL_APP, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg, applicationId);
      RouterServerUtil.logAndThrowException(msg, e);
    }

    ApplicationClientProtocol clientRMProxy =
        getClientRMProxyForSubCluster(subClusterId);

    KillApplicationResponse response = null;
    try {
      LOG.info("forceKillApplication {} on SubCluster {}.", applicationId, subClusterId);
      response = clientRMProxy.forceKillApplication(request);
    } catch (Exception e) {
      routerMetrics.incrAppsFailedKilled();
      String msg = "Unable to kill the application report.";
      RouterAuditLogger.logFailure(user.getShortUserName(), FORCE_KILL_APP, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg, applicationId, subClusterId);
      RouterServerUtil.logAndThrowException(msg, e);
    }

    if (response == null) {
      LOG.error("No response when attempting to kill the application {} to SubCluster {}.",
          applicationId, subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededAppsKilled(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), FORCE_KILL_APP,
        TARGET_CLIENT_RM_SERVICE, applicationId);
    return response;
  }

  /**
   * The YARN Router will forward to the respective YARN RM in which the AM is
   * running.
   *
   * Possible failure:
   *
   * Client: identical behavior as {@code ClientRMService}.
   *
   * Router: the Client will timeout and resubmit the request.
   *
   * ResourceManager: the Router will timeout and the call will fail.
   *
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException, IOException {

    if (request == null || request.getApplicationId() == null) {
      routerMetrics.incrAppsFailedRetrieved();
      String errMsg = "Missing getApplicationReport request or applicationId information.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_APP_REPORT, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, errMsg);
      RouterServerUtil.logAndThrowException(errMsg, null);
    }

    long startTime = clock.getTime();
    SubClusterId subClusterId = null;

    try {
      subClusterId = federationFacade
          .getApplicationHomeSubCluster(request.getApplicationId());
    } catch (YarnException e) {
      routerMetrics.incrAppsFailedRetrieved();
      String errMsg = String.format("Application %s does not exist in FederationStateStore.",
          request.getApplicationId());
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_APP_REPORT, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, errMsg, request.getApplicationId());
      RouterServerUtil.logAndThrowException(errMsg, e);
    }

    ApplicationClientProtocol clientRMProxy =
        getClientRMProxyForSubCluster(subClusterId);
    GetApplicationReportResponse response = null;

    try {
      response = clientRMProxy.getApplicationReport(request);
    } catch (Exception e) {
      routerMetrics.incrAppsFailedRetrieved();
      String errMsg = String.format("Unable to get the application report for %s to SubCluster %s.",
          request.getApplicationId(), subClusterId.getId());
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_APP_REPORT, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, errMsg, request.getApplicationId(), subClusterId);
      RouterServerUtil.logAndThrowException(errMsg, e);
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the report of "
          + "the application {} to SubCluster {}.",
          request.getApplicationId(), subClusterId.getId());
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededAppsRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_APP_REPORT,
        TARGET_CLIENT_RM_SERVICE, request.getApplicationId());
    return response;
  }

  /**
   * The Yarn Router will forward the request to all the Yarn RMs in parallel,
   * after that it will group all the ApplicationReports by the ApplicationId.
   *
   * Possible failure:
   *
   * Client: identical behavior as {@code ClientRMService}.
   *
   * Router: the Client will timeout and resubmit the request.
   *
   * ResourceManager: the Router calls each Yarn RM in parallel. In case a
   * Yarn RM fails, a single call will timeout. However, the Router will
   * merge the ApplicationReports it got, and provides a partial list to
   * the client.
   *
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public GetApplicationsResponse getApplications(GetApplicationsRequest request)
      throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrMultipleAppsFailedRetrieved();
      String msg = "Missing getApplications request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_APPLICATIONS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getApplications",
        new Class[] {GetApplicationsRequest.class}, new Object[] {request});
    Collection<GetApplicationsResponse> applications = null;
    try {
      applications = invokeConcurrent(remoteMethod, GetApplicationsResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrMultipleAppsFailedRetrieved();
      String msg = "Unable to get applications due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_APPLICATIONS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededMultipleAppsRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_APPLICATIONS,
        TARGET_CLIENT_RM_SERVICE);
    // Merge the Application Reports
    return RouterYarnClientUtils.mergeApplications(applications, returnPartialReport);
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrGetClusterMetricsFailedRetrieved();
      String msg = "Missing getApplications request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CLUSTERMETRICS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getClusterMetrics",
        new Class[] {GetClusterMetricsRequest.class}, new Object[] {request});
    Collection<GetClusterMetricsResponse> clusterMetrics = null;
    try {
      clusterMetrics = invokeConcurrent(remoteMethod, GetClusterMetricsResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrGetClusterMetricsFailedRetrieved();
      String msg = "Unable to get cluster metrics due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CLUSTERMETRICS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetClusterMetricsRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_CLUSTERMETRICS,
        TARGET_CLIENT_RM_SERVICE);
    return RouterYarnClientUtils.merge(clusterMetrics);
  }

  <R> Collection<R> invokeConcurrent(ClientMethod request, Class<R> clazz)
      throws YarnException {

    // Get Active SubClusters
    Map<SubClusterId, SubClusterInfo> subClusterInfo = federationFacade.getSubClusters(true);
    Collection<SubClusterId> subClusterIds = subClusterInfo.keySet();

    List<Callable<Pair<SubClusterId, Object>>> callables = new ArrayList<>();
    List<Future<Pair<SubClusterId, Object>>> futures = new ArrayList<>();
    Map<SubClusterId, Exception> exceptions = new TreeMap<>();

    // Generate parallel Callable tasks
    for (SubClusterId subClusterId : subClusterIds) {
      callables.add(() -> {
        ApplicationClientProtocol protocol = getClientRMProxyForSubCluster(subClusterId);
        String methodName = request.getMethodName();
        Class<?>[] types = request.getTypes();
        Object[] params = request.getParams();
        Method method = ApplicationClientProtocol.class.getMethod(methodName, types);
        Object result = method.invoke(protocol, params);
        return Pair.of(subClusterId, result);
      });
    }

    // Get results from multiple threads
    Map<SubClusterId, R> results = new TreeMap<>();
    try {
      futures.addAll(executorService.invokeAll(callables));
      futures.stream().forEach(future -> {
        SubClusterId subClusterId = null;
        try {
          Pair<SubClusterId, Object> pair = future.get();
          subClusterId = pair.getKey();
          Object result = pair.getValue();
          results.put(subClusterId, clazz.cast(result));
        } catch (InterruptedException | ExecutionException e) {
          Throwable cause = e.getCause();
          LOG.error("Cannot execute {} on {} : {}", request.getMethodName(),
              subClusterId.getId(), cause.getMessage());
          exceptions.put(subClusterId, e);
        }
      });
    } catch (InterruptedException e) {
      throw new YarnException("invokeConcurrent Failed.", e);
    }

    // All sub-clusters return results to be considered successful,
    // otherwise an exception will be thrown.
    if (exceptions != null && !exceptions.isEmpty()) {
      Set<SubClusterId> subClusterIdSets = exceptions.keySet();
      throw new YarnException("invokeConcurrent Failed, An exception occurred in subClusterIds = " +
          StringUtils.join(subClusterIdSets, ","));
    }

    // return result
    return results.values();
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrClusterNodesFailedRetrieved();
      String msg = "Missing getClusterNodes request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CLUSTERNODES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getClusterNodes",
        new Class[]{GetClusterNodesRequest.class}, new Object[]{request});
    try {
      Collection<GetClusterNodesResponse> clusterNodes =
          invokeConcurrent(remoteMethod, GetClusterNodesResponse.class);
      long stopTime = clock.getTime();
      routerMetrics.succeededGetClusterNodesRetrieved(stopTime - startTime);
      RouterAuditLogger.logSuccess(user.getShortUserName(), GET_CLUSTERNODES,
          TARGET_CLIENT_RM_SERVICE);
      return RouterYarnClientUtils.mergeClusterNodesResponse(clusterNodes);
    } catch (Exception ex) {
      routerMetrics.incrClusterNodesFailedRetrieved();
      String msg = "Unable to get cluster nodes due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CLUSTERNODES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    throw new YarnException("Unable to get cluster nodes.");
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    if (request == null || request.getQueueName() == null) {
      routerMetrics.incrGetQueueInfoFailedRetrieved();
      String msg = "Missing getQueueInfo request or queueName.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_QUEUEINFO, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }

    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getQueueInfo",
        new Class[]{GetQueueInfoRequest.class}, new Object[]{request});
    Collection<GetQueueInfoResponse> queues = null;
    try {
      queues = invokeConcurrent(remoteMethod, GetQueueInfoResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrGetQueueInfoFailedRetrieved();
      String msg = "Unable to get queue [" + request.getQueueName() + "] to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_QUEUEINFO, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetQueueInfoRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_QUEUEINFO, TARGET_CLIENT_RM_SERVICE);
    // Merge the GetQueueInfoResponse
    return RouterYarnClientUtils.mergeQueues(queues);
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException, IOException {
    if(request == null){
      routerMetrics.incrQueueUserAclsFailedRetrieved();
      String msg = "Missing getQueueUserAcls request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_QUEUE_USER_ACLS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getQueueUserAcls",
        new Class[] {GetQueueUserAclsInfoRequest.class}, new Object[] {request});
    Collection<GetQueueUserAclsInfoResponse> queueUserAcls = null;
    try {
      queueUserAcls = invokeConcurrent(remoteMethod, GetQueueUserAclsInfoResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrQueueUserAclsFailedRetrieved();
      String msg = "Unable to get queue user Acls due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_QUEUE_USER_ACLS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetQueueUserAclsRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_QUEUE_USER_ACLS,
        TARGET_CLIENT_RM_SERVICE);
    // Merge the QueueUserAclsInfoResponse
    return RouterYarnClientUtils.mergeQueueUserAcls(queueUserAcls);
  }

  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request)
      throws YarnException, IOException {
    if (request == null || request.getApplicationId() == null || request.getTargetQueue() == null) {
      routerMetrics.incrMoveApplicationAcrossQueuesFailedRetrieved();
      String msg = "Missing moveApplicationAcrossQueues request or " +
          "applicationId or target queue.";
      RouterAuditLogger.logFailure(user.getShortUserName(), MOVE_APPLICATION_ACROSS_QUEUES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg);
    }

    long startTime = clock.getTime();
    SubClusterId subClusterId = null;

    ApplicationId applicationId = request.getApplicationId();
    try {
      subClusterId = federationFacade
          .getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      routerMetrics.incrMoveApplicationAcrossQueuesFailedRetrieved();
      String errMsgFormat = "Application %s does not exist in FederationStateStore.";
      RouterAuditLogger.logFailure(user.getShortUserName(), MOVE_APPLICATION_ACROSS_QUEUES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, String.format(errMsgFormat, applicationId));
      RouterServerUtil.logAndThrowException(e, errMsgFormat, applicationId);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    MoveApplicationAcrossQueuesResponse response = null;
    try {
      response = clientRMProxy.moveApplicationAcrossQueues(request);
    } catch (Exception e) {
      routerMetrics.incrMoveApplicationAcrossQueuesFailedRetrieved();
      RouterServerUtil.logAndThrowException(e,
          "Unable to moveApplicationAcrossQueues for %s to SubCluster %s.", applicationId,
          subClusterId.getId());
    }

    if (response == null) {
      LOG.error("No response when moveApplicationAcrossQueues "
           + "the applicationId {} to Queue {} In SubCluster {}.",
           request.getApplicationId(), request.getTargetQueue(), subClusterId.getId());
    }

    long stopTime = clock.getTime();
    RouterAuditLogger.logSuccess(user.getShortUserName(), MOVE_APPLICATION_ACROSS_QUEUES,
        TARGET_CLIENT_RM_SERVICE, applicationId, subClusterId);
    routerMetrics.succeededMoveApplicationAcrossQueuesRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetNewReservationResponse getNewReservation(
      GetNewReservationRequest request) throws YarnException, IOException {

    if (request == null) {
      routerMetrics.incrGetNewReservationFailedRetrieved();
      String errMsg = "Missing getNewReservation request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_NEW_RESERVATION, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, errMsg);
      RouterServerUtil.logAndThrowException(errMsg, null);
    }

    long startTime = clock.getTime();
    Map<SubClusterId, SubClusterInfo> subClustersActive = federationFacade.getSubClusters(true);

    for (int i = 0; i < numSubmitRetries; ++i) {
      SubClusterId subClusterId = getRandomActiveSubCluster(subClustersActive);
      LOG.info("getNewReservation try #{} on SubCluster {}.", i, subClusterId);
      ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
      try {
        GetNewReservationResponse response = clientRMProxy.getNewReservation(request);
        if (response != null) {
          long stopTime = clock.getTime();
          routerMetrics.succeededGetNewReservationRetrieved(stopTime - startTime);
          RouterAuditLogger.logSuccess(user.getShortUserName(), GET_NEW_RESERVATION,
              TARGET_CLIENT_RM_SERVICE);
          return response;
        }
      } catch (Exception e) {
        String logFormatted = "Unable to create a new Reservation in SubCluster {}.";
        LOG.warn(logFormatted, subClusterId.getId(), e);
        RouterAuditLogger.logFailure(user.getShortUserName(), GET_NEW_RESERVATION, UNKNOWN,
            TARGET_CLIENT_RM_SERVICE, logFormatted, subClusterId.getId());
        subClustersActive.remove(subClusterId);
      }
    }

    routerMetrics.incrGetNewReservationFailedRetrieved();
    String errMsg = "Failed to create a new reservation.";
    RouterAuditLogger.logFailure(user.getShortUserName(), GET_NEW_RESERVATION, UNKNOWN,
        TARGET_CLIENT_RM_SERVICE, errMsg);
    throw new YarnException(errMsg);
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {

    if (request == null || request.getReservationId() == null
        || request.getReservationDefinition() == null || request.getQueue() == null) {
      routerMetrics.incrSubmitReservationFailedRetrieved();
      String msg = "Missing submitReservation request or reservationId " +
          "or reservation definition or queue.";
      RouterAuditLogger.logFailure(user.getShortUserName(), SUBMIT_RESERVATION, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }

    long startTime = clock.getTime();
    ReservationId reservationId = request.getReservationId();

    for (int i = 0; i < numSubmitRetries; i++) {
      try {
        // First, Get SubClusterId according to specific strategy.
        SubClusterId subClusterId = policyFacade.getReservationHomeSubCluster(request);
        LOG.info("submitReservation ReservationId {} try #{} on SubCluster {}.",
            reservationId, i, subClusterId);
        ReservationHomeSubCluster reservationHomeSubCluster =
            ReservationHomeSubCluster.newInstance(reservationId, subClusterId);

        // Second, determine whether the current ReservationId has a corresponding subCluster.
        // If it does not exist, add it. If it exists, update it.
        Boolean exists = existsReservationHomeSubCluster(reservationId);

        // We may encounter the situation of repeated submission of Reservation,
        // at this time we should try to use the reservation that has been allocated
        // !exists indicates that the reservation does not exist and needs to be added
        // i==0, mainly to consider repeated submissions,
        // so the first time to apply for reservation, try to use the original reservation
        if (!exists || i == 0) {
          addReservationHomeSubCluster(reservationId, reservationHomeSubCluster);
        } else {
          updateReservationHomeSubCluster(subClusterId, reservationId, reservationHomeSubCluster);
        }

        // Third, Submit a Reservation request to the subCluster
        ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
        ReservationSubmissionResponse response = clientRMProxy.submitReservation(request);
        if (response != null) {
          LOG.info("Reservation {} submitted on subCluster {}.", reservationId, subClusterId);
          long stopTime = clock.getTime();
          routerMetrics.succeededSubmitReservationRetrieved(stopTime - startTime);
          RouterAuditLogger.logSuccess(user.getShortUserName(), SUBMIT_RESERVATION,
              TARGET_CLIENT_RM_SERVICE);
          return response;
        }
      } catch (Exception e) {
        LOG.warn("Unable to submit(try #{}) the Reservation {}.", i, reservationId, e);
      }
    }

    routerMetrics.incrSubmitReservationFailedRetrieved();
    String msg = String.format("Reservation %s failed to be submitted.", reservationId);
    RouterAuditLogger.logFailure(user.getShortUserName(), SUBMIT_RESERVATION, UNKNOWN,
        TARGET_CLIENT_RM_SERVICE, msg);
    throw new YarnException(msg);
  }

  @Override
  public ReservationListResponse listReservations(
      ReservationListRequest request) throws YarnException, IOException {
    if (request == null || request.getReservationId() == null) {
      routerMetrics.incrListReservationsFailedRetrieved();
      String msg = "Missing listReservations request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), LIST_RESERVATIONS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("listReservations",
        new Class[] {ReservationListRequest.class}, new Object[] {request});
    Collection<ReservationListResponse> listResponses = null;
    try {
      listResponses = invokeConcurrent(remoteMethod, ReservationListResponse.class);
    } catch (Exception ex) {
      String msg = "Unable to list reservations node due to exception.";
      routerMetrics.incrListReservationsFailedRetrieved();
      RouterAuditLogger.logFailure(user.getShortUserName(), LIST_RESERVATIONS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededListReservationsRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), LIST_RESERVATIONS,
        TARGET_CLIENT_RM_SERVICE);
    // Merge the ReservationListResponse
    return RouterYarnClientUtils.mergeReservationsList(listResponses);
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {

    if (request == null || request.getReservationId() == null
        || request.getReservationDefinition() == null) {
      routerMetrics.incrUpdateReservationFailedRetrieved();
      String msg = "Missing updateReservation request or reservationId or reservation definition.";
      RouterAuditLogger.logFailure(user.getShortUserName(), UPDATE_RESERVATION, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }

    long startTime = clock.getTime();
    ReservationId reservationId = request.getReservationId();
    SubClusterId subClusterId = getReservationHomeSubCluster(reservationId);

    try {
      ApplicationClientProtocol client = getClientRMProxyForSubCluster(subClusterId);
      ReservationUpdateResponse response = client.updateReservation(request);
      if (response != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededUpdateReservationRetrieved(stopTime - startTime);
        RouterAuditLogger.logSuccess(user.getShortUserName(), UPDATE_RESERVATION,
            TARGET_CLIENT_RM_SERVICE);
        return response;
      }
    } catch (Exception ex) {
      routerMetrics.incrUpdateReservationFailedRetrieved();
      String msg = "Unable to reservation update due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), UPDATE_RESERVATION, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }

    routerMetrics.incrUpdateReservationFailedRetrieved();
    String msg = String.format("Reservation %s failed to be update.", reservationId);
    RouterAuditLogger.logFailure(user.getShortUserName(), UPDATE_RESERVATION, UNKNOWN,
        TARGET_CLIENT_RM_SERVICE, msg);
    throw new YarnException(msg);
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {
    if (request == null || request.getReservationId() == null) {
      routerMetrics.incrDeleteReservationFailedRetrieved();
      String msg = "Missing deleteReservation request or reservationId.";
      RouterServerUtil.logAndThrowException(msg, null);
      RouterAuditLogger.logFailure(user.getShortUserName(), DELETE_RESERVATION, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
    }

    long startTime = clock.getTime();
    ReservationId reservationId = request.getReservationId();
    SubClusterId subClusterId = getReservationHomeSubCluster(reservationId);

    try {
      ApplicationClientProtocol client = getClientRMProxyForSubCluster(subClusterId);
      ReservationDeleteResponse response = client.deleteReservation(request);
      if (response != null) {
        federationFacade.deleteReservationHomeSubCluster(reservationId);
        long stopTime = clock.getTime();
        routerMetrics.succeededDeleteReservationRetrieved(stopTime - startTime);
        RouterAuditLogger.logSuccess(user.getShortUserName(), DELETE_RESERVATION,
            TARGET_CLIENT_RM_SERVICE);
        return response;
      }
    } catch (Exception ex) {
      routerMetrics.incrUpdateReservationFailedRetrieved();
      String msg = "Unable to reservation delete due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), DELETE_RESERVATION, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }

    routerMetrics.incrDeleteReservationFailedRetrieved();
    String msg = String.format("Reservation %s failed to be delete.", reservationId);
    RouterAuditLogger.logFailure(user.getShortUserName(), DELETE_RESERVATION, UNKNOWN,
        TARGET_CLIENT_RM_SERVICE, msg);
    throw new YarnException(msg);
  }

  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrNodeToLabelsFailedRetrieved();
      String msg = "Missing getNodesToLabels request.";
      RouterServerUtil.logAndThrowException(msg, null);
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_NODETOLABELS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getNodeToLabels",
        new Class[] {GetNodesToLabelsRequest.class}, new Object[] {request});
    Collection<GetNodesToLabelsResponse> clusterNodes = null;
    try {
      clusterNodes = invokeConcurrent(remoteMethod, GetNodesToLabelsResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrNodeToLabelsFailedRetrieved();
      String msg = "Unable to get node label due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_NODETOLABELS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetNodeToLabelsRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_NODETOLABELS,
        TARGET_CLIENT_RM_SERVICE);
    // Merge the NodesToLabelsResponse
    return RouterYarnClientUtils.mergeNodesToLabelsResponse(clusterNodes);
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrLabelsToNodesFailedRetrieved();
      String msg = "Missing getNodesToLabels request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_LABELSTONODES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getLabelsToNodes",
         new Class[] {GetLabelsToNodesRequest.class}, new Object[] {request});
    Collection<GetLabelsToNodesResponse> labelNodes = null;
    try {
      labelNodes = invokeConcurrent(remoteMethod, GetLabelsToNodesResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrLabelsToNodesFailedRetrieved();
      String msg = "Unable to get label node due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_LABELSTONODES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetLabelsToNodesRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_LABELSTONODES,
        TARGET_CLIENT_RM_SERVICE);
    // Merge the LabelsToNodesResponse
    return RouterYarnClientUtils.mergeLabelsToNodes(labelNodes);
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrClusterNodeLabelsFailedRetrieved();
      String msg = "Missing getClusterNodeLabels request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CLUSTERNODELABELS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getClusterNodeLabels",
         new Class[] {GetClusterNodeLabelsRequest.class}, new Object[] {request});
    Collection<GetClusterNodeLabelsResponse> nodeLabels = null;
    try {
      nodeLabels = invokeConcurrent(remoteMethod, GetClusterNodeLabelsResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrClusterNodeLabelsFailedRetrieved();
      String msg = "Unable to get cluster nodeLabels due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CLUSTERNODELABELS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetClusterNodeLabelsRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_CLUSTERNODELABELS,
        TARGET_CLIENT_RM_SERVICE);
    // Merge the ClusterNodeLabelsResponse
    return RouterYarnClientUtils.mergeClusterNodeLabelsResponse(nodeLabels);
  }

  /**
   * The YARN Router will forward to the respective YARN RM in which the AM is
   * running.
   *
   * Possible failure:
   *
   * Client: identical behavior as {@code ClientRMService}.
   *
   * Router: the Client will timeout and resubmit the request.
   *
   * ResourceManager: the Router will timeout and the call will fail.
   *
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request)
      throws YarnException, IOException {

    if (request == null || request.getApplicationAttemptId() == null
            || request.getApplicationAttemptId().getApplicationId() == null) {
      routerMetrics.incrAppAttemptReportFailedRetrieved();
      String msg = "Missing getApplicationAttemptReport request or applicationId " +
          "or applicationAttemptId information.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_APPLICATION_ATTEMPT_REPORT, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }

    long startTime = clock.getTime();
    SubClusterId subClusterId = null;
    ApplicationId applicationId = request.getApplicationAttemptId().getApplicationId();
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      routerMetrics.incrAppAttemptReportFailedRetrieved();
      String msgFormat = "ApplicationAttempt %s belongs to " +
          "Application %s does not exist in FederationStateStore.";
      ApplicationAttemptId applicationAttemptId = request.getApplicationAttemptId();
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_APPLICATION_ATTEMPT_REPORT, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msgFormat, applicationAttemptId, applicationId);
      RouterServerUtil.logAndThrowException(e, msgFormat, applicationAttemptId, applicationId);
    }

    ApplicationClientProtocol clientRMProxy =
        getClientRMProxyForSubCluster(subClusterId);

    GetApplicationAttemptReportResponse response = null;
    try {
      response = clientRMProxy.getApplicationAttemptReport(request);
    } catch (Exception e) {
      routerMetrics.incrAppAttemptReportFailedRetrieved();
      String msg = String.format(
          "Unable to get the applicationAttempt report for %s to SubCluster %s.",
          request.getApplicationAttemptId(), subClusterId.getId());
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_APPLICATION_ATTEMPT_REPORT, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, e);
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the report of "
          + "the applicationAttempt {} to SubCluster {}.",
          request.getApplicationAttemptId(), subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededAppAttemptReportRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_APPLICATION_ATTEMPT_REPORT,
        TARGET_CLIENT_RM_SERVICE);
    return response;
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    if (request == null || request.getApplicationId() == null) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      String msg = "Missing getApplicationAttempts request or application id.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_APPLICATION_ATTEMPTS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg);
    }

    long startTime = clock.getTime();
    ApplicationId applicationId = request.getApplicationId();
    SubClusterId subClusterId = null;
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException ex) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      String msg = "Application " + applicationId + " does not exist in FederationStateStore.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_APPLICATION_ATTEMPTS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    GetApplicationAttemptsResponse response = null;
    try {
      response = clientRMProxy.getApplicationAttempts(request);
    } catch (Exception ex) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      String msg = "Unable to get the application attempts for " +
          applicationId + " from SubCluster " + subClusterId.getId();
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_APPLICATION_ATTEMPTS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the attempts list of " +
           "the application = {} to SubCluster = {}.", applicationId,
           subClusterId.getId());
    }

    long stopTime = clock.getTime();
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_APPLICATION_ATTEMPTS,
        TARGET_CLIENT_RM_SERVICE, applicationId);
    routerMetrics.succeededAppAttemptsRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    if(request == null || request.getContainerId() == null){
      routerMetrics.incrGetContainerReportFailedRetrieved();
      String msg = "Missing getContainerReport request or containerId";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CONTAINERREPORT, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }

    long startTime = clock.getTime();
    ApplicationId applicationId = request.getContainerId().
        getApplicationAttemptId().getApplicationId();
    SubClusterId subClusterId = null;
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException ex) {
      routerMetrics.incrGetContainerReportFailedRetrieved();
      String msg = "Application " + applicationId + " does not exist in FederationStateStore.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CONTAINERREPORT, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    GetContainerReportResponse response = null;

    try {
      response = clientRMProxy.getContainerReport(request);
    } catch (Exception ex) {
      routerMetrics.incrGetContainerReportFailedRetrieved();
      LOG.error("Unable to get the container report for {} from SubCluster {}.",
          applicationId, subClusterId.getId(), ex);
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the container report of " +
           "the ContainerId = {} From SubCluster = {}.", request.getContainerId(),
           subClusterId.getId());
    }

    long stopTime = clock.getTime();
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_CONTAINERREPORT,
        TARGET_CLIENT_RM_SERVICE, applicationId, subClusterId);
    routerMetrics.succeededGetContainerReportRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    if (request == null || request.getApplicationAttemptId() == null) {
      routerMetrics.incrGetContainersFailedRetrieved();
      String msg = "Missing getContainers request or ApplicationAttemptId.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CONTAINERS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }

    long startTime = clock.getTime();
    ApplicationId applicationId = request.getApplicationAttemptId().getApplicationId();
    SubClusterId subClusterId = null;
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException ex) {
      routerMetrics.incrGetContainersFailedRetrieved();
      String msg = "Application " + applicationId + " does not exist in FederationStateStore.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CONTAINERS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    GetContainersResponse response = null;

    try {
      response = clientRMProxy.getContainers(request);
    } catch (Exception ex) {
      routerMetrics.incrGetContainersFailedRetrieved();
      String msg = "Unable to get the containers for " +
          applicationId + " from SubCluster " + subClusterId.getId();
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CONTAINERS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the container report of " +
          "the ApplicationId = {} From SubCluster = {}.", applicationId,
          subClusterId.getId());
    }

    long stopTime = clock.getTime();
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_CONTAINERS,
        TARGET_CLIENT_RM_SERVICE, applicationId, subClusterId);
    routerMetrics.succeededGetContainersRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException, IOException {

    if (request == null || request.getRenewer() == null) {
      routerMetrics.incrGetDelegationTokenFailedRetrieved();
      String msg = "Missing getDelegationToken request or Renewer.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_DELEGATIONTOKEN, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }

    try {
      // Verify that the connection is kerberos authenticated
      if (!RouterServerUtil.isAllowedDelegationTokenOp()) {
        routerMetrics.incrGetDelegationTokenFailedRetrieved();
        String msg = "Delegation Token can be issued only with kerberos authentication.";
        RouterAuditLogger.logFailure(user.getShortUserName(), GET_DELEGATIONTOKEN, UNKNOWN,
            TARGET_CLIENT_RM_SERVICE, msg);
        throw new IOException(msg);
      }

      long startTime = clock.getTime();
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      Text owner = new Text(ugi.getUserName());
      Text realUser = null;
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }

      RMDelegationTokenIdentifier tokenIdentifier =
          new RMDelegationTokenIdentifier(owner, new Text(request.getRenewer()), realUser);
      Token<RMDelegationTokenIdentifier> realRMDToken =
          new Token<>(tokenIdentifier, this.getTokenSecretManager());

      org.apache.hadoop.yarn.api.records.Token routerRMDTToken =
          BuilderUtils.newDelegationToken(realRMDToken.getIdentifier(),
              realRMDToken.getKind().toString(),
              realRMDToken.getPassword(), realRMDToken.getService().toString());

      long stopTime = clock.getTime();
      routerMetrics.succeededGetDelegationTokenRetrieved((stopTime - startTime));
      RouterAuditLogger.logSuccess(user.getShortUserName(), GET_DELEGATIONTOKEN,
          TARGET_CLIENT_RM_SERVICE);
      return GetDelegationTokenResponse.newInstance(routerRMDTToken);
    } catch(IOException e) {
      routerMetrics.incrGetDelegationTokenFailedRetrieved();
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_DELEGATIONTOKEN, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, "getDelegationToken error, errMsg = " + e.getMessage());
      throw new YarnException(e);
    }
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException, IOException {
    try {

      if (!RouterServerUtil.isAllowedDelegationTokenOp()) {
        routerMetrics.incrRenewDelegationTokenFailedRetrieved();
        String msg = "Delegation Token can be renewed only with kerberos authentication";
        RouterAuditLogger.logFailure(user.getShortUserName(), RENEW_DELEGATIONTOKEN, UNKNOWN,
            TARGET_CLIENT_RM_SERVICE, msg);
        throw new IOException(msg);
      }

      long startTime = clock.getTime();
      org.apache.hadoop.yarn.api.records.Token protoToken = request.getDelegationToken();
      Token<RMDelegationTokenIdentifier> token = new Token<>(
          protoToken.getIdentifier().array(), protoToken.getPassword().array(),
          new Text(protoToken.getKind()), new Text(protoToken.getService()));
      String renewer = RouterServerUtil.getRenewerForToken(token);
      long nextExpTime = this.getTokenSecretManager().renewToken(token, renewer);
      RenewDelegationTokenResponse renewResponse =
          Records.newRecord(RenewDelegationTokenResponse.class);
      renewResponse.setNextExpirationTime(nextExpTime);
      long stopTime = clock.getTime();
      routerMetrics.succeededRenewDelegationTokenRetrieved((stopTime - startTime));
      RouterAuditLogger.logSuccess(user.getShortUserName(), RENEW_DELEGATIONTOKEN,
          TARGET_CLIENT_RM_SERVICE);
      return renewResponse;

    } catch (IOException e) {
      routerMetrics.incrRenewDelegationTokenFailedRetrieved();
      RouterAuditLogger.logFailure(user.getShortUserName(), RENEW_DELEGATIONTOKEN, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, "renewDelegationToken error, errMsg = " + e.getMessage());
      throw new YarnException(e);
    }
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException, IOException {
    try {
      if (!RouterServerUtil.isAllowedDelegationTokenOp()) {
        routerMetrics.incrCancelDelegationTokenFailedRetrieved();
        String msg = "Delegation Token can be cancelled only with kerberos authentication";
        RouterAuditLogger.logFailure(user.getShortUserName(), CANCEL_DELEGATIONTOKEN, UNKNOWN,
            TARGET_CLIENT_RM_SERVICE, msg);
        throw new IOException(msg);
      }

      long startTime = clock.getTime();
      org.apache.hadoop.yarn.api.records.Token protoToken = request.getDelegationToken();
      Token<RMDelegationTokenIdentifier> token = new Token<>(
          protoToken.getIdentifier().array(), protoToken.getPassword().array(),
          new Text(protoToken.getKind()), new Text(protoToken.getService()));
      String currentUser = UserGroupInformation.getCurrentUser().getUserName();
      this.getTokenSecretManager().cancelToken(token, currentUser);
      long stopTime = clock.getTime();
      routerMetrics.succeededCancelDelegationTokenRetrieved((stopTime - startTime));
      RouterAuditLogger.logSuccess(user.getShortUserName(), CANCEL_DELEGATIONTOKEN,
          TARGET_CLIENT_RM_SERVICE);
      return Records.newRecord(CancelDelegationTokenResponse.class);
    } catch (IOException e) {
      routerMetrics.incrCancelDelegationTokenFailedRetrieved();
      RouterAuditLogger.logFailure(user.getShortUserName(), CANCEL_DELEGATIONTOKEN, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, "cancelDelegationToken error, errMsg = " + e.getMessage());
      throw new YarnException(e);
    }
  }

  @Override
  public FailApplicationAttemptResponse failApplicationAttempt(
      FailApplicationAttemptRequest request) throws YarnException, IOException {
    if (request == null || request.getApplicationAttemptId() == null
          || request.getApplicationAttemptId().getApplicationId() == null) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      String msg = "Missing failApplicationAttempt request or applicationId " +
          "or applicationAttemptId information.";
      RouterAuditLogger.logFailure(user.getShortUserName(), FAIL_APPLICATIONATTEMPT, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    SubClusterId subClusterId = null;
    ApplicationAttemptId applicationAttemptId = request.getApplicationAttemptId();
    ApplicationId applicationId = applicationAttemptId.getApplicationId();

    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      String msg = "ApplicationAttempt " +
          applicationAttemptId + " belongs to Application " + applicationId +
          " does not exist in FederationStateStore.";
      RouterAuditLogger.logFailure(user.getShortUserName(), FAIL_APPLICATIONATTEMPT, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, e);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    FailApplicationAttemptResponse response = null;
    try {
      response = clientRMProxy.failApplicationAttempt(request);
    } catch (Exception e) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      String msg = "Unable to get the applicationAttempt report for " +
          applicationAttemptId + " to SubCluster " + subClusterId;
      RouterAuditLogger.logFailure(user.getShortUserName(), FAIL_APPLICATIONATTEMPT, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, e);
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the report of " +
          "the applicationAttempt {} to SubCluster {}.",
          request.getApplicationAttemptId(), subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededFailAppAttemptRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), FAIL_APPLICATIONATTEMPT,
        TARGET_CLIENT_RM_SERVICE, applicationId, subClusterId);
    return response;
  }

  @Override
  public UpdateApplicationPriorityResponse updateApplicationPriority(
      UpdateApplicationPriorityRequest request)
      throws YarnException, IOException {
    if (request == null || request.getApplicationId() == null
            || request.getApplicationPriority() == null) {
      routerMetrics.incrUpdateAppPriorityFailedRetrieved();
      String msg = "Missing updateApplicationPriority request or applicationId " +
          "or applicationPriority information.";
      RouterAuditLogger.logFailure(user.getShortUserName(), UPDATE_APPLICATIONPRIORITY, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }

    long startTime = clock.getTime();
    SubClusterId subClusterId = null;
    ApplicationId applicationId = request.getApplicationId();

    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      routerMetrics.incrUpdateAppPriorityFailedRetrieved();
      String msg = "Application " +
          applicationId + " does not exist in FederationStateStore.";
      RouterAuditLogger.logFailure(user.getShortUserName(), UPDATE_APPLICATIONPRIORITY, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, e);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    UpdateApplicationPriorityResponse response = null;
    try {
      response = clientRMProxy.updateApplicationPriority(request);
    } catch (Exception e) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      String msg = "Unable to update application priority for " +
          applicationId + " to SubCluster " + subClusterId;
      RouterAuditLogger.logFailure(user.getShortUserName(), UPDATE_APPLICATIONPRIORITY, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, e);
    }

    if (response == null) {
      LOG.error("No response when update application priority of " +
           "the applicationId {} to SubCluster {}.",
           applicationId, subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededUpdateAppPriorityRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), UPDATE_APPLICATIONPRIORITY,
        TARGET_CLIENT_RM_SERVICE, applicationId, subClusterId);
    return response;
  }

  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    if (request == null || request.getContainerId() == null
            || request.getCommand() == null) {
      routerMetrics.incrSignalToContainerFailedRetrieved();
      String msg = "Missing signalToContainer request or containerId or command information.";
      RouterAuditLogger.logFailure(user.getShortUserName(), SIGNAL_TOCONTAINER, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }

    long startTime = clock.getTime();
    SubClusterId subClusterId = null;
    ApplicationId applicationId =
        request.getContainerId().getApplicationAttemptId().getApplicationId();
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException ex) {
      routerMetrics.incrSignalToContainerFailedRetrieved();
      String msg = "Application " + applicationId + " does not exist in FederationStateStore.";
      RouterAuditLogger.logFailure(user.getShortUserName(), SIGNAL_TOCONTAINER, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    SignalContainerResponse response = null;
    try {
      response = clientRMProxy.signalToContainer(request);
    } catch (Exception ex) {
      String msg = "Unable to signal to container for " + applicationId +
          " from SubCluster " + subClusterId;
      RouterAuditLogger.logFailure(user.getShortUserName(), SIGNAL_TOCONTAINER, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }

    if (response == null) {
      LOG.error("No response when signal to container of " +
          "the applicationId {} to SubCluster {}.", applicationId, subClusterId);
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededSignalToContainerRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), SIGNAL_TOCONTAINER,
        TARGET_CLIENT_RM_SERVICE, applicationId, subClusterId);
    return response;
  }

  @Override
  public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
      UpdateApplicationTimeoutsRequest request)
      throws YarnException, IOException {
    if (request == null || request.getApplicationId() == null
            || request.getApplicationTimeouts() == null) {
      routerMetrics.incrUpdateApplicationTimeoutsRetrieved();
      String msg = "Missing updateApplicationTimeouts request or applicationId or " +
          "applicationTimeouts information.";
      RouterAuditLogger.logFailure(user.getShortUserName(), UPDATE_APPLICATIONTIMEOUTS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }

    long startTime = clock.getTime();
    SubClusterId subClusterId = null;
    ApplicationId applicationId = request.getApplicationId();
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      String msg = "Application " + applicationId + " does not exist in FederationStateStore.";
      RouterAuditLogger.logFailure(user.getShortUserName(), UPDATE_APPLICATIONTIMEOUTS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, e);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    UpdateApplicationTimeoutsResponse response = null;
    try {
      response = clientRMProxy.updateApplicationTimeouts(request);
    } catch (Exception e) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      String msg = "Unable to update application timeout for " + applicationId +
          " to SubCluster " + subClusterId;
      RouterAuditLogger.logFailure(user.getShortUserName(), UPDATE_APPLICATIONTIMEOUTS, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, e);
    }

    if (response == null) {
      LOG.error("No response when update application timeout of " +
          "the applicationId {} to SubCluster {}.",
          applicationId, subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededUpdateAppTimeoutsRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), UPDATE_APPLICATIONTIMEOUTS,
        TARGET_CLIENT_RM_SERVICE, applicationId, subClusterId);
    return response;
  }

  @Override
  public GetAllResourceProfilesResponse getResourceProfiles(
      GetAllResourceProfilesRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrGetResourceProfilesFailedRetrieved();
      String msg = "Missing getResourceProfiles request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_RESOURCEPROFILES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getResourceProfiles",
        new Class[] {GetAllResourceProfilesRequest.class}, new Object[] {request});
    Collection<GetAllResourceProfilesResponse> resourceProfiles = null;
    try {
      resourceProfiles = invokeConcurrent(remoteMethod, GetAllResourceProfilesResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrGetResourceProfilesFailedRetrieved();
      String msg = "Unable to get resource profiles due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_RESOURCEPROFILES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException("Unable to get resource profiles due to exception.",
          ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetResourceProfilesRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_RESOURCEPROFILES,
        TARGET_CLIENT_RM_SERVICE);
    return RouterYarnClientUtils.mergeClusterResourceProfilesResponse(resourceProfiles);
  }

  @Override
  public GetResourceProfileResponse getResourceProfile(
      GetResourceProfileRequest request) throws YarnException, IOException {
    if (request == null || request.getProfileName() == null) {
      routerMetrics.incrGetResourceProfileFailedRetrieved();
      String msg = "Missing getResourceProfile request or profileName.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_RESOURCEPROFILE, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getResourceProfile",
        new Class[] {GetResourceProfileRequest.class}, new Object[] {request});
    Collection<GetResourceProfileResponse> resourceProfile = null;
    try {
      resourceProfile = invokeConcurrent(remoteMethod, GetResourceProfileResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrGetResourceProfileFailedRetrieved();
      String msg = "Unable to get resource profile due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_RESOURCEPROFILE, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetResourceProfileRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_RESOURCEPROFILE,
        TARGET_CLIENT_RM_SERVICE);
    return RouterYarnClientUtils.mergeClusterResourceProfileResponse(resourceProfile);
  }

  @Override
  public GetAllResourceTypeInfoResponse getResourceTypeInfo(
      GetAllResourceTypeInfoRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrResourceTypeInfoFailedRetrieved();
      String msg = "Missing getResourceTypeInfo request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_RESOURCETYPEINFO, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getResourceTypeInfo",
        new Class[] {GetAllResourceTypeInfoRequest.class}, new Object[] {request});
    Collection<GetAllResourceTypeInfoResponse> listResourceTypeInfo;
    try {
      listResourceTypeInfo = invokeConcurrent(remoteMethod, GetAllResourceTypeInfoResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrResourceTypeInfoFailedRetrieved();
      String msg = "Unable to get all resource type info node due to exception.";
      LOG.error(msg, ex);
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_RESOURCETYPEINFO, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      throw ex;
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetResourceTypeInfoRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_RESOURCETYPEINFO,
        TARGET_CLIENT_RM_SERVICE);
    // Merge the GetAllResourceTypeInfoResponse
    return RouterYarnClientUtils.mergeResourceTypes(listResourceTypeInfo);
  }

  @Override
  public void shutdown() {
    executorService.shutdown();
    super.shutdown();
  }

  @Override
  public GetAttributesToNodesResponse getAttributesToNodes(
      GetAttributesToNodesRequest request) throws YarnException, IOException {
    if (request == null || request.getNodeAttributes() == null) {
      routerMetrics.incrGetAttributesToNodesFailedRetrieved();
      String msg = "Missing getAttributesToNodes request or nodeAttributes.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_ATTRIBUTESTONODES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getAttributesToNodes",
        new Class[] {GetAttributesToNodesRequest.class}, new Object[] {request});
    Collection<GetAttributesToNodesResponse> attributesToNodesResponses = null;
    try {
      attributesToNodesResponses =
          invokeConcurrent(remoteMethod, GetAttributesToNodesResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrGetAttributesToNodesFailedRetrieved();
      String msg = "Unable to get attributes to nodes due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_ATTRIBUTESTONODES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetAttributesToNodesRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_ATTRIBUTESTONODES,
        TARGET_CLIENT_RM_SERVICE);
    return RouterYarnClientUtils.mergeAttributesToNodesResponse(attributesToNodesResponses);
  }

  @Override
  public GetClusterNodeAttributesResponse getClusterNodeAttributes(
      GetClusterNodeAttributesRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrGetClusterNodeAttributesFailedRetrieved();
      String msg = "Missing getClusterNodeAttributes request.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CLUSTERNODEATTRIBUTES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getClusterNodeAttributes",
        new Class[] {GetClusterNodeAttributesRequest.class}, new Object[] {request});
    Collection<GetClusterNodeAttributesResponse> clusterNodeAttributesResponses = null;
    try {
      clusterNodeAttributesResponses = invokeConcurrent(remoteMethod,
          GetClusterNodeAttributesResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrGetClusterNodeAttributesFailedRetrieved();
      String msg = "Unable to get cluster node attributes due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_CLUSTERNODEATTRIBUTES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetClusterNodeAttributesRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_CLUSTERNODEATTRIBUTES,
        TARGET_CLIENT_RM_SERVICE);
    return RouterYarnClientUtils.mergeClusterNodeAttributesResponse(clusterNodeAttributesResponses);
  }

  @Override
  public GetNodesToAttributesResponse getNodesToAttributes(
      GetNodesToAttributesRequest request) throws YarnException, IOException {
    if (request == null || request.getHostNames() == null) {
      routerMetrics.incrGetNodesToAttributesFailedRetrieved();
      String msg = "Missing getNodesToAttributes request or hostNames.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_NODESTOATTRIBUTES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getNodesToAttributes",
        new Class[] {GetNodesToAttributesRequest.class}, new Object[] {request});
    Collection<GetNodesToAttributesResponse> nodesToAttributesResponses = null;
    try {
      nodesToAttributesResponses = invokeConcurrent(remoteMethod,
          GetNodesToAttributesResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrGetNodesToAttributesFailedRetrieved();
      String msg = "Unable to get nodes to attributes due to exception.";
      RouterAuditLogger.logFailure(user.getShortUserName(), GET_NODESTOATTRIBUTES, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, msg);
      RouterServerUtil.logAndThrowException(msg, ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetNodesToAttributesRetrieved(stopTime - startTime);
    RouterAuditLogger.logSuccess(user.getShortUserName(), GET_NODESTOATTRIBUTES,
        TARGET_CLIENT_RM_SERVICE);
    return RouterYarnClientUtils.mergeNodesToAttributesResponse(nodesToAttributesResponses);
  }

  protected SubClusterId getApplicationHomeSubCluster(
      ApplicationId applicationId) throws YarnException {
    if (applicationId == null) {
      LOG.error("ApplicationId is Null, Can't find in SubCluster.");
      return null;
    }

    SubClusterId resultSubClusterId = null;

    // try looking for applicationId in Home SubCluster
    try {
      resultSubClusterId = federationFacade.
          getApplicationHomeSubCluster(applicationId);
    } catch (YarnException ex) {
      if(LOG.isDebugEnabled()){
        LOG.debug("Can't find applicationId = {} in home sub cluster, " +
             " try foreach sub clusters.", applicationId);
      }
    }
    if (resultSubClusterId != null) {
      return resultSubClusterId;
    }

    // if applicationId not found in Home SubCluster,
    // foreach Clusters
    Map<SubClusterId, SubClusterInfo> subClusters =
        federationFacade.getSubClusters(true);
    for (SubClusterId subClusterId : subClusters.keySet()) {
      try {
        ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
        if(clientRMProxy == null) {
          continue;
        }
        GetApplicationReportRequest appReportRequest =
            GetApplicationReportRequest.newInstance(applicationId);
        GetApplicationReportResponse appReportResponse =
            clientRMProxy.getApplicationReport(appReportRequest);

        if(appReportResponse!=null && applicationId.equals(
            appReportResponse.getApplicationReport().getApplicationId())){
          resultSubClusterId = federationFacade.addApplicationHomeSubCluster(
               ApplicationHomeSubCluster.newInstance(applicationId, subClusterId));
          return resultSubClusterId;
        }

      } catch (Exception ex) {
        if(LOG.isDebugEnabled()){
          LOG.debug("Can't find applicationId = {} in Sub Cluster!", applicationId);
        }
      }
    }

    String errorMsg =
        String.format("Can't find applicationId = %s in any sub clusters", applicationId);
    throw new YarnException(errorMsg);
  }

  protected SubClusterId getReservationHomeSubCluster(ReservationId reservationId)
      throws YarnException {

    if (reservationId == null) {
      LOG.error("ReservationId is Null, Can't find in SubCluster.");
      return null;
    }

    // try looking for reservation in Home SubCluster
    try {
      SubClusterId resultSubClusterId =
          federationFacade.getReservationHomeSubCluster(reservationId);
      if (resultSubClusterId != null) {
        return resultSubClusterId;
      }
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowException(e,
          "Can't find reservationId = %s in home sub cluster.", reservationId);
    }

    String errorMsg =
        String.format("Can't find reservationId = %s in home sub cluster.", reservationId);
    throw new YarnException(errorMsg);
  }

  @VisibleForTesting
  public FederationStateStoreFacade getFederationFacade() {
    return federationFacade;
  }

  @VisibleForTesting
  public Map<SubClusterId, ApplicationClientProtocol> getClientRMProxies() {
    return clientRMProxies;
  }

  private Boolean existsReservationHomeSubCluster(ReservationId reservationId) {
    try {
      SubClusterId subClusterId = federationFacade.getReservationHomeSubCluster(reservationId);
      if (subClusterId != null) {
        return true;
      }
    } catch (YarnException e) {
      LOG.warn("get homeSubCluster by reservationId = {} error.", reservationId, e);
    }
    return false;
  }

  private void addReservationHomeSubCluster(ReservationId reservationId,
      ReservationHomeSubCluster homeSubCluster) throws YarnException {
    try {
      // persist the mapping of reservationId and the subClusterId which has
      // been selected as its home
      federationFacade.addReservationHomeSubCluster(homeSubCluster);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowException(e,
          "Unable to insert the ReservationId %s into the FederationStateStore.",
          reservationId);
    }
  }

  private void updateReservationHomeSubCluster(SubClusterId subClusterId,
      ReservationId reservationId, ReservationHomeSubCluster homeSubCluster) throws YarnException {
    try {
      // update the mapping of reservationId and the home subClusterId to
      // the new subClusterId we have selected
      federationFacade.updateReservationHomeSubCluster(homeSubCluster);
    } catch (YarnException e) {
      SubClusterId subClusterIdInStateStore =
          federationFacade.getReservationHomeSubCluster(reservationId);
      if (subClusterId == subClusterIdInStateStore) {
        LOG.info("Reservation {} already submitted on SubCluster {}.",
            reservationId, subClusterId);
      } else {
        RouterServerUtil.logAndThrowException(e,
            "Unable to update the ReservationId %s into the FederationStateStore.",
            reservationId);
      }
    }
  }

  protected int getNumMinThreads(Configuration conf) {

    String threadSize = conf.get(YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE);

    // If the user configures YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE,
    // we will still get the number of threads from this configuration.
    if (StringUtils.isNotBlank(threadSize)) {
      LOG.warn("{} is a deprecated property, " +
          "please remove it, use {} to configure the minimum number of thread pool.",
          YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE,
          YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_MINIMUM_POOL_SIZE);
      return Integer.parseInt(threadSize);
    }

    int numMinThreads = conf.getInt(
        YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_MINIMUM_POOL_SIZE,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREAD_POOL_MINIMUM_POOL_SIZE);
    return numMinThreads;
  }

  protected int getNumMaxThreads(Configuration conf) {

    String threadSize = conf.get(YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE);

    // If the user configures YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE,
    // we will still get the number of threads from this configuration.
    if (StringUtils.isNotBlank(threadSize)) {
      LOG.warn("{} is a deprecated property, " +
          "please remove it, use {} to configure the maximum number of thread pool.",
          YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE,
          YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_MAXIMUM_POOL_SIZE);
      return Integer.parseInt(threadSize);
    }

    int numMaxThreads = conf.getInt(
        YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_MAXIMUM_POOL_SIZE,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREAD_POOL_MAXIMUM_POOL_SIZE);
    return numMaxThreads;
  }

  @VisibleForTesting
  public void setNumSubmitRetries(int numSubmitRetries) {
    this.numSubmitRetries = numSubmitRetries;
  }
}
