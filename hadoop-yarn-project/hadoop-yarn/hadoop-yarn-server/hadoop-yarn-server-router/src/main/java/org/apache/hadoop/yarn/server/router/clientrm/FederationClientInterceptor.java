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
import org.apache.commons.lang3.NotImplementedException;
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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.RouterPolicyFacade;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NEW_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SUBMIT_NEW_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APP_REPORT;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.FORCE_KILL_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.TARGET_CLIENT_RM_SERVICE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UNKNOWN;

/**
 * Extends the {@code AbstractRequestInterceptorClient} class and provides an
 * implementation for federation of YARN RM and scaling an application across
 * multiple YARN SubClusters. All the federation specific implementation is
 * encapsulated in this class. This is always the last interceptor in the chain.
 */
public class FederationClientInterceptor
    extends AbstractClientRequestInterceptor {

  /*
   * TODO YARN-6740 Federation Router (hiding multiple RMs for
   * ApplicationClientProtocol) phase 2.
   *
   * The current implementation finalized the main 4 calls (getNewApplication,
   * submitApplication, forceKillApplication and getApplicationReport). Those
   * allow us to execute applications E2E.
   */

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

  @Override
  public void init(String userName) {
    super.init(userName);

    federationFacade = FederationStateStoreFacade.getInstance();
    rand = new Random(System.currentTimeMillis());

    int numThreads = getConf().getInt(
        YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREADS_SIZE);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("RPC Router Client-" + userName + "-%d ").build();

    BlockingQueue workQueue = new LinkedBlockingQueue<>();
    this.executorService = new ThreadPoolExecutor(numThreads, numThreads,
        0L, TimeUnit.MILLISECONDS, workQueue, threadFactory);

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

    for (int i = 0; i < numSubmitRetries; ++i) {
      SubClusterId subClusterId = getRandomActiveSubCluster(subClustersActive);
      LOG.info("getNewApplication try #{} on SubCluster {}.", i, subClusterId);
      ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
      GetNewApplicationResponse response = null;
      try {
        response = clientRMProxy.getNewApplication(request);
      } catch (Exception e) {
        LOG.warn("Unable to create a new ApplicationId in SubCluster {}.", subClusterId.getId(), e);
        subClustersActive.remove(subClusterId);
      }

      if (response != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededAppsCreated(stopTime - startTime);
        RouterAuditLogger.logSuccess(user.getShortUserName(), GET_NEW_APP,
            TARGET_CLIENT_RM_SERVICE, response.getApplicationId());
        return response;
      }
    }

    routerMetrics.incrAppsFailedCreated();
    String errMsg = "Failed to create a new application.";
    RouterAuditLogger.logFailure(user.getShortUserName(), GET_NEW_APP, UNKNOWN,
        TARGET_CLIENT_RM_SERVICE, errMsg);
    throw new YarnException(errMsg);
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

    SubmitApplicationResponse response = null;

    long startTime = clock.getTime();

    ApplicationId applicationId =
        request.getApplicationSubmissionContext().getApplicationId();

    List<SubClusterId> blacklist = new ArrayList<>();

    for (int i = 0; i < numSubmitRetries; ++i) {

      SubClusterId subClusterId = policyFacade.getHomeSubcluster(
          request.getApplicationSubmissionContext(), blacklist);

      LOG.info("submitApplication appId {} try #{} on SubCluster {}.",
           applicationId, i, subClusterId);

      ApplicationHomeSubCluster appHomeSubCluster =
          ApplicationHomeSubCluster.newInstance(applicationId, subClusterId);

      if (i == 0) {
        try {
          // persist the mapping of applicationId and the subClusterId which has
          // been selected as its home
          subClusterId =
              federationFacade.addApplicationHomeSubCluster(appHomeSubCluster);
        } catch (YarnException e) {
          routerMetrics.incrAppsFailedSubmitted();
          String message =
              String.format("Unable to insert the ApplicationId %s into the FederationStateStore.",
              applicationId);
          RouterAuditLogger.logFailure(user.getShortUserName(), SUBMIT_NEW_APP, UNKNOWN,
              TARGET_CLIENT_RM_SERVICE, message, applicationId, subClusterId);
          RouterServerUtil.logAndThrowException(message, e);
        }
      } else {
        try {
          // update the mapping of applicationId and the home subClusterId to
          // the new subClusterId we have selected
          federationFacade.updateApplicationHomeSubCluster(appHomeSubCluster);
        } catch (YarnException e) {
          String message =
              String.format("Unable to update the ApplicationId %s into the FederationStateStore.",
              applicationId);
          SubClusterId subClusterIdInStateStore =
              federationFacade.getApplicationHomeSubCluster(applicationId);
          if (subClusterId == subClusterIdInStateStore) {
            LOG.info("Application {} already submitted on SubCluster {}.",
                applicationId, subClusterId);
          } else {
            routerMetrics.incrAppsFailedSubmitted();
            RouterAuditLogger.logFailure(user.getShortUserName(), SUBMIT_NEW_APP, UNKNOWN,
                TARGET_CLIENT_RM_SERVICE, message, applicationId, subClusterId);
            RouterServerUtil.logAndThrowException(message, e);
          }
        }
      }

      ApplicationClientProtocol clientRMProxy =
          getClientRMProxyForSubCluster(subClusterId);

      try {
        response = clientRMProxy.submitApplication(request);
      } catch (Exception e) {
        LOG.warn("Unable to submit the application {} to SubCluster {} error = {}.",
            applicationId, subClusterId.getId(), e);
      }

      if (response != null) {
        LOG.info("Application {} with appId {} submitted on {}.",
            request.getApplicationSubmissionContext().getApplicationName(),
            applicationId, subClusterId);
        long stopTime = clock.getTime();
        routerMetrics.succeededAppsSubmitted(stopTime - startTime);
        RouterAuditLogger.logSuccess(user.getShortUserName(), SUBMIT_NEW_APP,
            TARGET_CLIENT_RM_SERVICE, applicationId, subClusterId);
        return response;
      } else {
        // Empty response from the ResourceManager.
        // Blacklist this subcluster for this request.
        blacklist.add(subClusterId);
      }
    }

    routerMetrics.incrAppsFailedSubmitted();
    String msg = String.format("Application %s with appId %s failed to be submitted.",
        request.getApplicationSubmissionContext().getApplicationName(), applicationId);
    RouterAuditLogger.logFailure(user.getShortUserName(), SUBMIT_NEW_APP, UNKNOWN,
        TARGET_CLIENT_RM_SERVICE, msg, applicationId);
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
      RouterServerUtil.logAndThrowException("Missing getApplications request.", null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getApplications",
        new Class[] {GetApplicationsRequest.class}, new Object[] {request});
    Collection<GetApplicationsResponse> applications = null;
    try {
      applications = invokeConcurrent(remoteMethod, GetApplicationsResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrMultipleAppsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get applications due to exception.", ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededMultipleAppsRetrieved(stopTime - startTime);
    // Merge the Application Reports
    return RouterYarnClientUtils.mergeApplications(applications, returnPartialReport);
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrGetClusterMetricsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getClusterMetrics request.", null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getClusterMetrics",
        new Class[] {GetClusterMetricsRequest.class}, new Object[] {request});
    Collection<GetClusterMetricsResponse> clusterMetrics = null;
    try {
      clusterMetrics = invokeConcurrent(remoteMethod, GetClusterMetricsResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrGetClusterMetricsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get cluster metrics due to exception.", ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetClusterMetricsRetrieved(stopTime - startTime);
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
          LOG.error("Cannot execute {} on {}: {}", request.getMethodName(),
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
      RouterServerUtil.logAndThrowException("Missing getClusterNodes request.", null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getClusterNodes",
        new Class[]{GetClusterNodesRequest.class}, new Object[]{request});
    try {
      Collection<GetClusterNodesResponse> clusterNodes =
          invokeConcurrent(remoteMethod, GetClusterNodesResponse.class);
      long stopTime = clock.getTime();
      routerMetrics.succeededGetClusterNodesRetrieved(stopTime - startTime);
      return RouterYarnClientUtils.mergeClusterNodesResponse(clusterNodes);
    } catch (Exception ex) {
      routerMetrics.incrClusterNodesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get cluster nodes due to exception.", ex);
    }
    throw new YarnException("Unable to get cluster nodes.");
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    if (request == null || request.getQueueName() == null) {
      routerMetrics.incrGetQueueInfoFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getQueueInfo request or queueName.", null);
    }

    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getQueueInfo",
        new Class[]{GetQueueInfoRequest.class}, new Object[]{request});
    Collection<GetQueueInfoResponse> queues = null;
    try {
      queues = invokeConcurrent(remoteMethod, GetQueueInfoResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrGetQueueInfoFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get queue [" +
          request.getQueueName() + "] to exception.", ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetQueueInfoRetrieved(stopTime - startTime);
    // Merge the GetQueueInfoResponse
    return RouterYarnClientUtils.mergeQueues(queues);
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException, IOException {
    if(request == null){
      routerMetrics.incrQueueUserAclsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getQueueUserAcls request.", null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getQueueUserAcls",
        new Class[] {GetQueueUserAclsInfoRequest.class}, new Object[] {request});
    Collection<GetQueueUserAclsInfoResponse> queueUserAcls = null;
    try {
      queueUserAcls = invokeConcurrent(remoteMethod, GetQueueUserAclsInfoResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrQueueUserAclsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get queue user Acls due to exception.", ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetQueueUserAclsRetrieved(stopTime - startTime);
    // Merge the QueueUserAclsInfoResponse
    return RouterYarnClientUtils.mergeQueueUserAcls(queueUserAcls);
  }

  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request)
      throws YarnException, IOException {
    if (request == null || request.getApplicationId() == null || request.getTargetQueue() == null) {
      routerMetrics.incrMoveApplicationAcrossQueuesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing moveApplicationAcrossQueues request or " +
          "applicationId or target queue.", null);
    }

    long startTime = clock.getTime();
    SubClusterId subClusterId = null;

    ApplicationId applicationId = request.getApplicationId();
    try {
      subClusterId = federationFacade
          .getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      routerMetrics.incrMoveApplicationAcrossQueuesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Application " +
          applicationId + " does not exist in FederationStateStore.", e);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    MoveApplicationAcrossQueuesResponse response = null;
    try {
      response = clientRMProxy.moveApplicationAcrossQueues(request);
    } catch (Exception e) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to moveApplicationAcrossQueues for " +
          applicationId + " to SubCluster " + subClusterId.getId(), e);
    }

    if (response == null) {
      LOG.error("No response when moveApplicationAcrossQueues "
           + "the applicationId {} to Queue {} In SubCluster {}.",
           request.getApplicationId(), request.getTargetQueue(), subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededMoveApplicationAcrossQueuesRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetNewReservationResponse getNewReservation(
      GetNewReservationRequest request) throws YarnException, IOException {

    if (request == null) {
      routerMetrics.incrGetNewReservationFailedRetrieved();
      String errMsg = "Missing getNewReservation request.";
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
          return response;
        }
      } catch (Exception e) {
        LOG.warn("Unable to create a new Reservation in SubCluster {}.", subClusterId.getId(), e);
        subClustersActive.remove(subClusterId);
      }
    }

    routerMetrics.incrGetNewReservationFailedRetrieved();
    String errMsg = "Failed to create a new reservation.";
    throw new YarnException(errMsg);
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {

    if (request == null || request.getReservationId() == null
        || request.getReservationDefinition() == null || request.getQueue() == null) {
      routerMetrics.incrSubmitReservationFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Missing submitReservation request or reservationId " +
          "or reservation definition or queue.", null);
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
          return response;
        }
      } catch (Exception e) {
        LOG.warn("Unable to submit(try #{}) the Reservation {}.", i, reservationId, e);
      }
    }

    routerMetrics.incrSubmitReservationFailedRetrieved();
    String msg = String.format("Reservation %s failed to be submitted.", reservationId);
    throw new YarnException(msg);
  }

  @Override
  public ReservationListResponse listReservations(
      ReservationListRequest request) throws YarnException, IOException {
    if (request == null || request.getReservationId() == null) {
      routerMetrics.incrListReservationsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing listReservations request.", null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("listReservations",
        new Class[] {ReservationListRequest.class}, new Object[] {request});
    Collection<ReservationListResponse> listResponses = null;
    try {
      listResponses = invokeConcurrent(remoteMethod, ReservationListResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrListReservationsFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Unable to list reservations node due to exception.", ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededListReservationsRetrieved(stopTime - startTime);
    // Merge the ReservationListResponse
    return RouterYarnClientUtils.mergeReservationsList(listResponses);
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {

    if (request == null || request.getReservationId() == null
        || request.getReservationDefinition() == null) {
      routerMetrics.incrUpdateReservationFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Missing updateReservation request or reservationId or reservation definition.", null);
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
        return response;
      }
    } catch (Exception ex) {
      routerMetrics.incrUpdateReservationFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Unable to reservation update due to exception.", ex);
    }

    routerMetrics.incrUpdateReservationFailedRetrieved();
    String msg = String.format("Reservation %s failed to be update.", reservationId);
    throw new YarnException(msg);
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {
    if (request == null || request.getReservationId() == null) {
      routerMetrics.incrDeleteReservationFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Missing deleteReservation request or reservationId.", null);
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
        return response;
      }
    } catch (Exception ex) {
      routerMetrics.incrUpdateReservationFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Unable to reservation delete due to exception.", ex);
    }

    routerMetrics.incrDeleteReservationFailedRetrieved();
    String msg = String.format("Reservation %s failed to be delete.", reservationId);
    throw new YarnException(msg);
  }

  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrNodeToLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getNodesToLabels request.", null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getNodeToLabels",
         new Class[] {GetNodesToLabelsRequest.class}, new Object[] {request});
    Collection<GetNodesToLabelsResponse> clusterNodes = null;
    try {
      clusterNodes = invokeConcurrent(remoteMethod, GetNodesToLabelsResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrNodeToLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get node label due to exception.", ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetNodeToLabelsRetrieved(stopTime - startTime);
    // Merge the NodesToLabelsResponse
    return RouterYarnClientUtils.mergeNodesToLabelsResponse(clusterNodes);
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrLabelsToNodesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getLabelsToNodes request.", null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getLabelsToNodes",
         new Class[] {GetLabelsToNodesRequest.class}, new Object[] {request});
    Collection<GetLabelsToNodesResponse> labelNodes = null;
    try {
      labelNodes = invokeConcurrent(remoteMethod, GetLabelsToNodesResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrLabelsToNodesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get label node due to exception.", ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetLabelsToNodesRetrieved(stopTime - startTime);
    // Merge the LabelsToNodesResponse
    return RouterYarnClientUtils.mergeLabelsToNodes(labelNodes);
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrClusterNodeLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getClusterNodeLabels request.", null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getClusterNodeLabels",
         new Class[] {GetClusterNodeLabelsRequest.class}, new Object[] {request});
    Collection<GetClusterNodeLabelsResponse> nodeLabels = null;
    try {
      nodeLabels = invokeConcurrent(remoteMethod, GetClusterNodeLabelsResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrClusterNodeLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get cluster nodeLabels due to exception.",
          ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetClusterNodeLabelsRetrieved(stopTime - startTime);
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
      RouterServerUtil.logAndThrowException(
          "Missing getApplicationAttemptReport request or applicationId " +
          "or applicationAttemptId information.", null);
    }

    long startTime = clock.getTime();
    SubClusterId subClusterId = null;
    ApplicationId applicationId = request.getApplicationAttemptId().getApplicationId();
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      routerMetrics.incrAppAttemptReportFailedRetrieved();
      RouterServerUtil.logAndThrowException("ApplicationAttempt " +
          request.getApplicationAttemptId() + " belongs to Application " +
          request.getApplicationAttemptId().getApplicationId() +
          " does not exist in FederationStateStore.", e);
    }

    ApplicationClientProtocol clientRMProxy =
        getClientRMProxyForSubCluster(subClusterId);

    GetApplicationAttemptReportResponse response = null;
    try {
      response = clientRMProxy.getApplicationAttemptReport(request);
    } catch (Exception e) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      String msg = String.format(
          "Unable to get the applicationAttempt report for %s to SubCluster %s.",
          request.getApplicationAttemptId(), subClusterId.getId());
      RouterServerUtil.logAndThrowException(msg, e);
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the report of "
          + "the applicationAttempt {} to SubCluster {}.",
          request.getApplicationAttemptId(), subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededAppAttemptReportRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    if (request == null || request.getApplicationId() == null) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getApplicationAttempts " +
          "request or application id.", null);
    }

    long startTime = clock.getTime();
    ApplicationId applicationId = request.getApplicationId();
    SubClusterId subClusterId = null;
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException ex) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Application " + applicationId +
          " does not exist in FederationStateStore.", ex);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    GetApplicationAttemptsResponse response = null;
    try {
      response = clientRMProxy.getApplicationAttempts(request);
    } catch (Exception ex) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get the application attempts for " +
          applicationId + " from SubCluster " + subClusterId.getId(), ex);
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the attempts list of " +
           "the application = {} to SubCluster = {}.", applicationId,
           subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededAppAttemptsRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    if(request == null || request.getContainerId() == null){
      routerMetrics.incrContainerReportFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getContainerReport request " +
          "or containerId", null);
    }

    long startTime = clock.getTime();
    ApplicationId applicationId = request.getContainerId().
        getApplicationAttemptId().getApplicationId();
    SubClusterId subClusterId = null;
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException ex) {
      routerMetrics.incrContainerReportFailedRetrieved();
      RouterServerUtil.logAndThrowException("Application " + applicationId +
          " does not exist in FederationStateStore.", ex);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    GetContainerReportResponse response = null;

    try {
      response = clientRMProxy.getContainerReport(request);
    } catch (Exception ex) {
      routerMetrics.incrContainerReportFailedRetrieved();
      LOG.error("Unable to get the container report for {} from SubCluster {}.",
          applicationId, subClusterId.getId(), ex);
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the container report of " +
           "the ContainerId = {} From SubCluster = {}.", request.getContainerId(),
           subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededGetContainerReportRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    if (request == null || request.getApplicationAttemptId() == null) {
      routerMetrics.incrContainerFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Missing getContainers request or ApplicationAttemptId.", null);
    }

    long startTime = clock.getTime();
    ApplicationId applicationId = request.getApplicationAttemptId().getApplicationId();
    SubClusterId subClusterId = null;
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException ex) {
      routerMetrics.incrContainerFailedRetrieved();
      RouterServerUtil.logAndThrowException("Application " + applicationId +
          " does not exist in FederationStateStore.", ex);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    GetContainersResponse response = null;

    try {
      response = clientRMProxy.getContainers(request);
    } catch (Exception ex) {
      routerMetrics.incrContainerFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get the containers for " +
          applicationId + " from SubCluster " + subClusterId.getId(), ex);
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the container report of " +
          "the ApplicationId = {} From SubCluster = {}.", applicationId,
          subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededGetContainersRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public FailApplicationAttemptResponse failApplicationAttempt(
      FailApplicationAttemptRequest request) throws YarnException, IOException {
    if (request == null || request.getApplicationAttemptId() == null
          || request.getApplicationAttemptId().getApplicationId() == null) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Missing failApplicationAttempt request or applicationId " +
          "or applicationAttemptId information.", null);
    }
    long startTime = clock.getTime();
    SubClusterId subClusterId = null;
    ApplicationId applicationId = request.getApplicationAttemptId().getApplicationId();

    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      RouterServerUtil.logAndThrowException("ApplicationAttempt " +
          request.getApplicationAttemptId() + " belongs to Application " +
          request.getApplicationAttemptId().getApplicationId() +
          " does not exist in FederationStateStore.", e);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    FailApplicationAttemptResponse response = null;
    try {
      response = clientRMProxy.failApplicationAttempt(request);
    } catch (Exception e) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get the applicationAttempt report for " +
          request.getApplicationAttemptId() + " to SubCluster " + subClusterId.getId(), e);
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the report of " +
          "the applicationAttempt {} to SubCluster {}.",
          request.getApplicationAttemptId(), subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededFailAppAttemptRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public UpdateApplicationPriorityResponse updateApplicationPriority(
      UpdateApplicationPriorityRequest request)
      throws YarnException, IOException {
    if (request == null || request.getApplicationId() == null
            || request.getApplicationPriority() == null) {
      routerMetrics.incrUpdateAppPriorityFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Missing updateApplicationPriority request or applicationId " +
          "or applicationPriority information.", null);
    }

    long startTime = clock.getTime();
    SubClusterId subClusterId = null;
    ApplicationId applicationId = request.getApplicationId();

    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      routerMetrics.incrUpdateAppPriorityFailedRetrieved();
      RouterServerUtil.logAndThrowException("Application " +
          request.getApplicationId() + " does not exist in FederationStateStore.", e);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    UpdateApplicationPriorityResponse response = null;
    try {
      response = clientRMProxy.updateApplicationPriority(request);
    } catch (Exception e) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to update application priority for " +
          request.getApplicationId() + " to SubCluster " + subClusterId.getId(), e);
    }

    if (response == null) {
      LOG.error("No response when update application priority of " +
           "the applicationId {} to SubCluster {}.",
           applicationId, subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededUpdateAppPriorityRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    if (request == null || request.getContainerId() == null
            || request.getCommand() == null) {
      routerMetrics.incrSignalToContainerFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Missing signalToContainer request or containerId " +
          "or command information.", null);
    }

    long startTime = clock.getTime();
    SubClusterId subClusterId = null;
    ApplicationId applicationId =
        request.getContainerId().getApplicationAttemptId().getApplicationId();
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException ex) {
      routerMetrics.incrSignalToContainerFailedRetrieved();
      RouterServerUtil.logAndThrowException("Application " + applicationId +
          " does not exist in FederationStateStore.", ex);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    SignalContainerResponse response = null;
    try {
      response = clientRMProxy.signalToContainer(request);
    } catch (Exception ex) {
      RouterServerUtil.logAndThrowException("Unable to signal to container for " +
          applicationId + " from SubCluster " + subClusterId.getId(), ex);
    }

    if (response == null) {
      LOG.error("No response when signal to container of " +
          "the applicationId {} to SubCluster {}.", applicationId, subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededSignalToContainerRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
      UpdateApplicationTimeoutsRequest request)
      throws YarnException, IOException {
    if (request == null || request.getApplicationId() == null
            || request.getApplicationTimeouts() == null) {
      routerMetrics.incrUpdateApplicationTimeoutsRetrieved();
      RouterServerUtil.logAndThrowException(
          "Missing updateApplicationTimeouts request or applicationId " +
          "or applicationTimeouts information.", null);
    }

    long startTime = clock.getTime();
    SubClusterId subClusterId = null;
    ApplicationId applicationId = request.getApplicationId();
    try {
      subClusterId = getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      RouterServerUtil.logAndThrowException("Application " +
          request.getApplicationId() +
          " does not exist in FederationStateStore.", e);
    }

    ApplicationClientProtocol clientRMProxy = getClientRMProxyForSubCluster(subClusterId);
    UpdateApplicationTimeoutsResponse response = null;
    try {
      response = clientRMProxy.updateApplicationTimeouts(request);
    } catch (Exception e) {
      routerMetrics.incrFailAppAttemptFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to update application timeout for " +
          request.getApplicationId() + " to SubCluster " + subClusterId.getId(), e);
    }

    if (response == null) {
      LOG.error("No response when update application timeout of " +
          "the applicationId {} to SubCluster {}.",
          applicationId, subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededUpdateAppTimeoutsRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetAllResourceProfilesResponse getResourceProfiles(
      GetAllResourceProfilesRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrGetResourceProfilesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getResourceProfiles request.", null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getResourceProfiles",
        new Class[] {GetAllResourceProfilesRequest.class}, new Object[] {request});
    Collection<GetAllResourceProfilesResponse> resourceProfiles = null;
    try {
      resourceProfiles = invokeConcurrent(remoteMethod, GetAllResourceProfilesResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrGetResourceProfilesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get resource profiles due to exception.",
          ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetResourceProfilesRetrieved(stopTime - startTime);
    return RouterYarnClientUtils.mergeClusterResourceProfilesResponse(resourceProfiles);
  }

  @Override
  public GetResourceProfileResponse getResourceProfile(
      GetResourceProfileRequest request) throws YarnException, IOException {
    if (request == null || request.getProfileName() == null) {
      routerMetrics.incrGetResourceProfileFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getResourceProfile request or profileName.",
          null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getResourceProfile",
        new Class[] {GetResourceProfileRequest.class}, new Object[] {request});
    Collection<GetResourceProfileResponse> resourceProfile = null;
    try {
      resourceProfile = invokeConcurrent(remoteMethod, GetResourceProfileResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrGetResourceProfileFailedRetrieved();
      RouterServerUtil.logAndThrowException("Unable to get resource profile due to exception.",
          ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetResourceProfileRetrieved(stopTime - startTime);
    return RouterYarnClientUtils.mergeClusterResourceProfileResponse(resourceProfile);
  }

  @Override
  public GetAllResourceTypeInfoResponse getResourceTypeInfo(
      GetAllResourceTypeInfoRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrResourceTypeInfoFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getResourceTypeInfo request.", null);
    }
    long startTime = clock.getTime();
    ClientMethod remoteMethod = new ClientMethod("getResourceTypeInfo",
        new Class[] {GetAllResourceTypeInfoRequest.class}, new Object[] {request});
    Collection<GetAllResourceTypeInfoResponse> listResourceTypeInfo;
    try {
      listResourceTypeInfo = invokeConcurrent(remoteMethod, GetAllResourceTypeInfoResponse.class);
    } catch (Exception ex) {
      routerMetrics.incrResourceTypeInfoFailedRetrieved();
      LOG.error("Unable to get all resource type info node due to exception.", ex);
      throw ex;
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetResourceTypeInfoRetrieved(stopTime - startTime);
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
      RouterServerUtil.logAndThrowException("Missing getAttributesToNodes request " +
          "or nodeAttributes.", null);
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
      RouterServerUtil.logAndThrowException("Unable to get attributes to nodes due to exception.",
          ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetAttributesToNodesRetrieved(stopTime - startTime);
    return RouterYarnClientUtils.mergeAttributesToNodesResponse(attributesToNodesResponses);
  }

  @Override
  public GetClusterNodeAttributesResponse getClusterNodeAttributes(
      GetClusterNodeAttributesRequest request) throws YarnException, IOException {
    if (request == null) {
      routerMetrics.incrGetClusterNodeAttributesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getClusterNodeAttributes request.", null);
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
      RouterServerUtil.logAndThrowException("Unable to get cluster node attributes due " +
          " to exception.", ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetClusterNodeAttributesRetrieved(stopTime - startTime);
    return RouterYarnClientUtils.mergeClusterNodeAttributesResponse(clusterNodeAttributesResponses);
  }

  @Override
  public GetNodesToAttributesResponse getNodesToAttributes(
      GetNodesToAttributesRequest request) throws YarnException, IOException {
    if (request == null || request.getHostNames() == null) {
      routerMetrics.incrGetNodesToAttributesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getNodesToAttributes request or " +
          "hostNames.", null);
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
      RouterServerUtil.logAndThrowException("Unable to get nodes to attributes due " +
          " to exception.", ex);
    }
    long stopTime = clock.getTime();
    routerMetrics.succeededGetNodesToAttributesRetrieved(stopTime - startTime);
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
}
