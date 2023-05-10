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

package org.apache.hadoop.yarn.server.router.webapp;

import java.io.IOException;
import java.lang.reflect.Method;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.impl.prefetch.Validate;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.RouterPolicyFacade;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.retry.FederationActionRetry;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.NodeIDsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.RMQueueAclInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.BulkActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDefinitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntry;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.clientrm.ClientMethod;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.router.webapp.cache.RouterAppInfoCacheKey;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationBulkActivitiesInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationRMQueueAclInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.SubClusterResult;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationSchedulerTypeInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationConfInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationClusterUserInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationClusterInfo;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.apache.hadoop.yarn.webapp.dao.ConfInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade.getRandomActiveSubCluster;
import static org.apache.hadoop.yarn.server.router.webapp.RouterWebServiceUtil.extractToken;
import static org.apache.hadoop.yarn.server.router.webapp.RouterWebServiceUtil.getKerberosUserGroupInformation;

/**
 * Extends the {@code AbstractRESTRequestInterceptor} class and provides an
 * implementation for federation of YARN RM and scaling an application across
 * multiple YARN SubClusters. All the federation specific implementation is
 * encapsulated in this class. This is always the last interceptor in the chain.
 */
public class FederationInterceptorREST extends AbstractRESTRequestInterceptor {

  private static final Logger LOG = LoggerFactory.getLogger(FederationInterceptorREST.class);

  private int numSubmitRetries;
  private FederationStateStoreFacade federationFacade;
  private RouterPolicyFacade policyFacade;
  private RouterMetrics routerMetrics;
  private final Clock clock = new MonotonicClock();
  private boolean returnPartialReport;
  private boolean appInfosCacheEnabled;
  private int appInfosCacheCount;
  private boolean allowPartialResult;
  private long submitIntervalTime;

  private Map<SubClusterId, DefaultRequestInterceptorREST> interceptors;
  private LRUCacheHashMap<RouterAppInfoCacheKey, AppsInfo> appInfosCaches;

  /**
   * Thread pool used for asynchronous operations.
   */
  private ExecutorService threadpool;

  @Override
  public void init(String user) {

    super.init(user);

    federationFacade = FederationStateStoreFacade.getInstance();

    final Configuration conf = this.getConf();

    try {
      SubClusterResolver subClusterResolver =
          this.federationFacade.getSubClusterResolver();
      policyFacade = new RouterPolicyFacade(
          conf, federationFacade, subClusterResolver, null);
    } catch (FederationPolicyInitializationException e) {
      throw new YarnRuntimeException(e);
    }

    numSubmitRetries = conf.getInt(
        YarnConfiguration.ROUTER_CLIENTRM_SUBMIT_RETRY,
        YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_SUBMIT_RETRY);

    interceptors = new HashMap<>();
    routerMetrics = RouterMetrics.getMetrics();
    threadpool = HadoopExecutors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setNameFormat("FederationInterceptorREST #%d")
        .build());

    returnPartialReport = conf.getBoolean(
        YarnConfiguration.ROUTER_WEBAPP_PARTIAL_RESULTS_ENABLED,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_PARTIAL_RESULTS_ENABLED);

    appInfosCacheEnabled = conf.getBoolean(
        YarnConfiguration.ROUTER_APPSINFO_ENABLED,
        YarnConfiguration.DEFAULT_ROUTER_APPSINFO_ENABLED);

    if(appInfosCacheEnabled) {
      appInfosCacheCount = conf.getInt(
          YarnConfiguration.ROUTER_APPSINFO_CACHED_COUNT,
          YarnConfiguration.DEFAULT_ROUTER_APPSINFO_CACHED_COUNT);
      appInfosCaches = new LRUCacheHashMap<>(appInfosCacheCount, true);
    }

    allowPartialResult = conf.getBoolean(
        YarnConfiguration.ROUTER_INTERCEPTOR_ALLOW_PARTIAL_RESULT_ENABLED,
        YarnConfiguration.DEFAULT_ROUTER_INTERCEPTOR_ALLOW_PARTIAL_RESULT_ENABLED);

    submitIntervalTime = conf.getTimeDuration(
        YarnConfiguration.ROUTER_CLIENTRM_SUBMIT_INTERVAL_TIME,
        YarnConfiguration.DEFAULT_CLIENTRM_SUBMIT_INTERVAL_TIME, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  protected DefaultRequestInterceptorREST getInterceptorForSubCluster(SubClusterId subClusterId) {
    if (interceptors.containsKey(subClusterId)) {
      return interceptors.get(subClusterId);
    } else {
      LOG.error("The interceptor for SubCluster {} does not exist in the cache.",
          subClusterId);
      return null;
    }
  }

  private DefaultRequestInterceptorREST createInterceptorForSubCluster(
      SubClusterId subClusterId, String webAppAddress) {

    final Configuration conf = this.getConf();

    String interceptorClassName = conf.get(
        YarnConfiguration.ROUTER_WEBAPP_DEFAULT_INTERCEPTOR_CLASS,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_DEFAULT_INTERCEPTOR_CLASS);

    DefaultRequestInterceptorREST interceptorInstance;
    try {
      Class<?> interceptorClass = conf.getClassByName(interceptorClassName);
      if (DefaultRequestInterceptorREST.class.isAssignableFrom(interceptorClass)) {
        interceptorInstance =
            (DefaultRequestInterceptorREST) ReflectionUtils.newInstance(interceptorClass, conf);
        String userName = getUser().getUserName();
        interceptorInstance.init(userName);
      } else {
        throw new YarnRuntimeException("Class: " + interceptorClassName + " not instance of "
            + DefaultRequestInterceptorREST.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate ApplicationMasterRequestInterceptor: " +
          interceptorClassName, e);
    }

    String webAppAddressWithScheme = WebAppUtils.getHttpSchemePrefix(conf) + webAppAddress;
    interceptorInstance.setWebAppAddress(webAppAddressWithScheme);
    interceptorInstance.setSubClusterId(subClusterId);
    interceptors.put(subClusterId, interceptorInstance);
    return interceptorInstance;
  }

  protected DefaultRequestInterceptorREST getOrCreateInterceptorForSubCluster(
      SubClusterInfo subClusterInfo) {
    if (subClusterInfo != null) {
      final SubClusterId subClusterId = subClusterInfo.getSubClusterId();
      final String webServiceAddress = subClusterInfo.getRMWebServiceAddress();
      return getOrCreateInterceptorForSubCluster(subClusterId, webServiceAddress);
    }
    return null;
  }

  protected DefaultRequestInterceptorREST getOrCreateInterceptorByAppId(String appId)
      throws YarnException {
    // We first check the applicationId
    RouterServerUtil.validateApplicationId(appId);

    // Get homeSubCluster By appId
    SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);
    return getOrCreateInterceptorForSubCluster(subClusterInfo);
  }

  protected DefaultRequestInterceptorREST getOrCreateInterceptorByNodeId(String nodeId) {
    SubClusterInfo subClusterInfo = getNodeSubcluster(nodeId);
    return getOrCreateInterceptorForSubCluster(subClusterInfo);
  }

  @VisibleForTesting
  protected DefaultRequestInterceptorREST getOrCreateInterceptorForSubCluster(
      SubClusterId subClusterId, String webAppAddress) {
    DefaultRequestInterceptorREST interceptor = getInterceptorForSubCluster(subClusterId);
    String webAppAddressWithScheme =
        WebAppUtils.getHttpSchemePrefix(this.getConf()) + webAppAddress;
    if (interceptor == null || !webAppAddressWithScheme.equals(interceptor.getWebAppAddress())) {
      interceptor = createInterceptorForSubCluster(subClusterId, webAppAddress);
    }
    return interceptor;
  }

  /**
   * YARN Router forwards every getNewApplication requests to any RM. During
   * this operation there will be no communication with the State Store. The
   * Router will forward the requests to any SubCluster. The Router will retry
   * to submit the request on #numSubmitRetries different SubClusters. The
   * SubClusters are randomly chosen from the active ones.
   * <p>
   * Possible failures and behaviors:
   * <p>
   * Client: identical behavior as {@code RMWebServices}.
   * <p>
   * Router: the Client will timeout and resubmit.
   * <p>
   * ResourceManager: the Router will timeout and contacts another RM.
   * <p>
   * StateStore: not in the execution.
   */
  @Override
  public Response createNewApplication(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {

    long startTime = clock.getTime();

    try {
      Map<SubClusterId, SubClusterInfo> subClustersActive =
          federationFacade.getSubClusters(true);

      // We declare blackList and retries.
      List<SubClusterId> blackList = new ArrayList<>();
      int actualRetryNums = federationFacade.getRetryNumbers(numSubmitRetries);
      Response response = ((FederationActionRetry<Response>) (retryCount) ->
          invokeGetNewApplication(subClustersActive, blackList, hsr, retryCount)).
          runWithRetries(actualRetryNums, submitIntervalTime);

      // If the response is not empty and the status is SC_OK,
      // this request can be returned directly.
      if (response != null && response.getStatus() == HttpServletResponse.SC_OK) {
        long stopTime = clock.getTime();
        routerMetrics.succeededAppsCreated(stopTime - startTime);
        return response;
      }
    } catch (FederationPolicyException e) {
      // If a FederationPolicyException is thrown, the service is unavailable.
      routerMetrics.incrAppsFailedCreated();
      return Response.status(Status.SERVICE_UNAVAILABLE).entity(e.getLocalizedMessage()).build();
    } catch (Exception e) {
      routerMetrics.incrAppsFailedCreated();
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getLocalizedMessage()).build();
    }

    // return error message directly.
    String errMsg = "Fail to create a new application.";
    LOG.error(errMsg);
    routerMetrics.incrAppsFailedCreated();
    return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errMsg).build();
  }

  /**
   * Invoke GetNewApplication to different subClusters.
   *
   * @param subClustersActive Active SubClusters.
   * @param blackList Blacklist avoid repeated calls to unavailable subCluster.
   * @param hsr HttpServletRequest.
   * @param retryCount number of retries.
   * @return Get response, If the response is empty or status not equal SC_OK, the request fails,
   * if the response is not empty and status equal SC_OK, the request is successful.
   * @throws YarnException yarn exception.
   * @throws IOException io error.
   * @throws InterruptedException interrupted exception.
   */
  private Response invokeGetNewApplication(Map<SubClusterId, SubClusterInfo> subClustersActive,
      List<SubClusterId> blackList, HttpServletRequest hsr, int retryCount)
      throws YarnException, IOException, InterruptedException {

    SubClusterId subClusterId = getRandomActiveSubCluster(subClustersActive, blackList);

    LOG.info("getNewApplication try #{} on SubCluster {}.", retryCount, subClusterId);

    DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(subClusterId,
        subClustersActive.get(subClusterId).getRMWebServiceAddress());

    try {
      Response response = interceptor.createNewApplication(hsr);
      if (response != null && response.getStatus() == HttpServletResponse.SC_OK) {
        return response;
      }
    } catch (Exception e) {
      blackList.add(subClusterId);
      RouterServerUtil.logAndThrowException(e.getMessage(), e);
    }

    // We need to throw the exception directly.
    String msg = String.format("Unable to create a new ApplicationId in SubCluster %s.",
        subClusterId.getId());
    throw new YarnException(msg);
  }

  /**
   * Today, in YARN there are no checks of any applicationId submitted.
   * <p>
   * Base scenarios:
   * <p>
   * The Client submits an application to the Router. The Router selects one
   * SubCluster to forward the request. The Router inserts a tuple into
   * StateStore with the selected SubCluster (e.g. SC1) and the appId. The
   * State Store replies with the selected SubCluster (e.g. SC1). The Router
   * submits the request to the selected SubCluster.
   * <p>
   * In case of State Store failure:
   * <p>
   * The client submits an application to the Router. The Router selects one
   * SubCluster to forward the request. The Router inserts a tuple into State
   * Store with the selected SubCluster (e.g. SC1) and the appId. Due to the
   * State Store down the Router times out and it will retry depending on the
   * FederationFacade settings. The Router replies to the client with an error
   * message.
   * <p>
   * If State Store fails after inserting the tuple: identical behavior as
   * {@code RMWebServices}.
   * <p>
   * In case of Router failure:
   * <p>
   * Scenario 1 – Crash before submission to the ResourceManager
   * <p>
   * The Client submits an application to the Router. The Router selects one
   * SubCluster to forward the request. The Router inserts a tuple into State
   * Store with the selected SubCluster (e.g. SC1) and the appId. The Router
   * crashes. The Client timeouts and resubmits the application. The Router
   * selects one SubCluster to forward the request. The Router inserts a tuple
   * into State Store with the selected SubCluster (e.g. SC2) and the appId.
   * Because the tuple is already inserted in the State Store, it returns the
   * previous selected SubCluster (e.g. SC1). The Router submits the request
   * to the selected SubCluster (e.g. SC1).
   * <p>
   * Scenario 2 – Crash after submission to the ResourceManager
   * <p>
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
   * <p>
   * In case of Client failure: identical behavior as {@code RMWebServices}.
   * <p>
   * In case of ResourceManager failure:
   * <p>
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
  public Response submitApplication(ApplicationSubmissionContextInfo newApp, HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {

    long startTime = clock.getTime();

    // We verify the parameters to ensure that newApp is not empty and
    // that the format of applicationId is correct.
    if (newApp == null || newApp.getApplicationId() == null) {
      routerMetrics.incrAppsFailedSubmitted();
      String errMsg = "Missing ApplicationSubmissionContextInfo or "
          + "applicationSubmissionContext information.";
      return Response.status(Status.BAD_REQUEST).entity(errMsg).build();
    }

    try {
      String applicationId = newApp.getApplicationId();
      RouterServerUtil.validateApplicationId(applicationId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrAppsFailedSubmitted();
      return Response.status(Status.BAD_REQUEST).entity(e.getLocalizedMessage()).build();
    }

    List<SubClusterId> blackList = new ArrayList<>();
    try {
      int activeSubClustersCount = federationFacade.getActiveSubClustersCount();
      int actualRetryNums = Math.min(activeSubClustersCount, numSubmitRetries);
      Response response = ((FederationActionRetry<Response>) (retryCount) ->
          invokeSubmitApplication(newApp, blackList, hsr, retryCount)).
          runWithRetries(actualRetryNums, submitIntervalTime);
      if (response != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededAppsSubmitted(stopTime - startTime);
        return response;
      }
    } catch (Exception e) {
      routerMetrics.incrAppsFailedSubmitted();
      return Response.status(Status.SERVICE_UNAVAILABLE).entity(e.getLocalizedMessage()).build();
    }

    routerMetrics.incrAppsFailedSubmitted();
    String errMsg = String.format("Application %s with appId %s failed to be submitted.",
        newApp.getApplicationName(), newApp.getApplicationId());
    LOG.error(errMsg);
    return Response.status(Status.SERVICE_UNAVAILABLE).entity(errMsg).build();
  }

  /**
   * Invoke SubmitApplication to different subClusters.
   *
   * @param submissionContext application submission context.
   * @param blackList Blacklist avoid repeated calls to unavailable subCluster.
   * @param hsr HttpServletRequest.
   * @param retryCount number of retries.
   * @return Get response, If the response is empty or status not equal SC_ACCEPTED,
   * the request fails, if the response is not empty and status equal SC_OK,
   * the request is successful.
   * @throws YarnException yarn exception.
   * @throws IOException io error.
   */
  private Response invokeSubmitApplication(ApplicationSubmissionContextInfo submissionContext,
      List<SubClusterId> blackList, HttpServletRequest hsr, int retryCount)
      throws YarnException, IOException, InterruptedException {

    // Step1. We convert ApplicationSubmissionContextInfo to ApplicationSubmissionContext
    // and Prepare parameters.
    ApplicationSubmissionContext context =
        RMWebAppUtil.createAppSubmissionContext(submissionContext, this.getConf());
    ApplicationId applicationId = ApplicationId.fromString(submissionContext.getApplicationId());
    SubClusterId subClusterId = null;

    try {
      // Get subClusterId from policy.
      subClusterId = policyFacade.getHomeSubcluster(context, blackList);

      // Print the log of submitting the submitApplication.
      LOG.info("submitApplication appId {} try #{} on SubCluster {}.",
          applicationId, retryCount, subClusterId);

      // Step2. We Store the mapping relationship
      // between Application and HomeSubCluster in stateStore.
      ApplicationSubmissionContext trimmedAppSubmissionContext =
          RouterServerUtil.getTrimmedAppSubmissionContext(context);
      federationFacade.addOrUpdateApplicationHomeSubCluster(
          applicationId, subClusterId, retryCount, trimmedAppSubmissionContext);

      // Step3. We get subClusterInfo based on subClusterId.
      SubClusterInfo subClusterInfo = federationFacade.getSubCluster(subClusterId);
      if (subClusterInfo == null) {
        throw new YarnException("Can't Find SubClusterId = " + subClusterId);
      }

      // Step4. Submit the request, if the response is HttpServletResponse.SC_ACCEPTED,
      // We return the response, otherwise we throw an exception.
      Response response = getOrCreateInterceptorForSubCluster(subClusterId,
          subClusterInfo.getRMWebServiceAddress()).submitApplication(submissionContext, hsr);
      if (response != null && response.getStatus() == HttpServletResponse.SC_ACCEPTED) {
        LOG.info("Application {} with appId {} submitted on {}.",
            context.getApplicationName(), applicationId, subClusterId);
        return response;
      }
      String msg = String.format("application %s failed to be submitted.", applicationId);
      throw new YarnException(msg);
    } catch (Exception e) {
      LOG.warn("Unable to submit the application {} to SubCluster {}.", applicationId,
          subClusterId, e);
      if (subClusterId != null) {
        blackList.add(subClusterId);
      }
      throw e;
    }
  }

  /**
   * The YARN Router will forward to the respective YARN RM in which the AM is
   * running.
   * <p>
   * Possible failure:
   * <p>
   * Client: identical behavior as {@code RMWebServices}.
   * <p>
   * Router: the Client will timeout and resubmit the request.
   * <p>
   * ResourceManager: the Router will timeout and the call will fail.
   * <p>
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public AppInfo getApp(HttpServletRequest hsr, String appId, Set<String> unselectedFields) {

    try {
      long startTime = clock.getTime();

      // Get SubClusterInfo according to applicationId
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      if (interceptor == null) {
        routerMetrics.incrAppsFailedRetrieved();
        return null;
      }
      AppInfo response = interceptor.getApp(hsr, appId, unselectedFields);
      long stopTime = clock.getTime();
      routerMetrics.succeededAppsRetrieved(stopTime - startTime);
      return response;
    } catch (YarnException e) {
      routerMetrics.incrAppsFailedRetrieved();
      LOG.error("getApp Error, applicationId = {}.", appId, e);
      return null;
    } catch (IllegalArgumentException e) {
      routerMetrics.incrAppsFailedRetrieved();
      throw e;
    }
  }

  /**
   * The YARN Router will forward to the respective YARN RM in which the AM is
   * running.
   * <p>
   * Possible failures and behaviors:
   * <p>
   * Client: identical behavior as {@code RMWebServices}.
   * <p>
   * Router: the Client will timeout and resubmit the request.
   * <p>
   * ResourceManager: the Router will timeout and the call will fail.
   * <p>
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public Response updateAppState(AppState targetState, HttpServletRequest hsr, String appId)
      throws AuthorizationException, YarnException, InterruptedException, IOException {

    long startTime = clock.getTime();

    ApplicationId applicationId;
    try {
      applicationId = ApplicationId.fromString(appId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrAppsFailedKilled();
      return Response
          .status(Status.BAD_REQUEST)
          .entity(e.getLocalizedMessage())
          .build();
    }

    SubClusterInfo subClusterInfo;
    SubClusterId subClusterId;
    try {
      subClusterId =
          federationFacade.getApplicationHomeSubCluster(applicationId);
      subClusterInfo = federationFacade.getSubCluster(subClusterId);
    } catch (YarnException e) {
      routerMetrics.incrAppsFailedKilled();
      return Response
          .status(Status.BAD_REQUEST)
          .entity(e.getLocalizedMessage())
          .build();
    }

    Response response = getOrCreateInterceptorForSubCluster(subClusterId,
        subClusterInfo.getRMWebServiceAddress()).updateAppState(targetState,
            hsr, appId);

    long stopTime = clock.getTime();
    routerMetrics.succeededAppsRetrieved(stopTime - startTime);

    return response;
  }

  /**
   * The YARN Router will forward the request to all the YARN RMs in parallel,
   * after that it will group all the ApplicationReports by the ApplicationId.
   * <p>
   * Possible failure:
   * <p>
   * Client: identical behavior as {@code RMWebServices}.
   * <p>
   * Router: the Client will timeout and resubmit the request.
   * <p>
   * ResourceManager: the Router calls each YARN RM in parallel by using one
   * thread for each YARN RM. In case a YARN RM fails, a single call will
   * timeout. However, the Router will merge the ApplicationReports it got, and
   * provides a partial list to the client.
   * <p>
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public AppsInfo getApps(HttpServletRequest hsr, String stateQuery,
      Set<String> statesQuery, String finalStatusQuery, String userQuery,
      String queueQuery, String count, String startedBegin, String startedEnd,
      String finishBegin, String finishEnd, Set<String> applicationTypes,
      Set<String> applicationTags, String name, Set<String> unselectedFields) {

    RouterAppInfoCacheKey routerAppInfoCacheKey = RouterAppInfoCacheKey.newInstance(
        hsr, stateQuery, statesQuery, finalStatusQuery, userQuery, queueQuery, count,
        startedBegin, startedEnd, finishBegin, finishEnd, applicationTypes,
        applicationTags, name, unselectedFields);

    if (appInfosCacheEnabled && routerAppInfoCacheKey != null) {
      if (appInfosCaches.containsKey(routerAppInfoCacheKey)) {
        return appInfosCaches.get(routerAppInfoCacheKey);
      }
    }

    AppsInfo apps = new AppsInfo();
    long startTime = clock.getTime();

    // HttpServletRequest does not work with ExecutorCompletionService.
    // Create a duplicate hsr.
    final HttpServletRequest hsrCopy = clone(hsr);
    Collection<SubClusterInfo> subClusterInfos = federationFacade.getActiveSubClusters();

    List<AppsInfo> appsInfos = subClusterInfos.parallelStream().map(subCluster -> {
      try {
        DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(subCluster);
        AppsInfo rmApps = interceptor.getApps(hsrCopy, stateQuery, statesQuery, finalStatusQuery,
            userQuery, queueQuery, count, startedBegin, startedEnd, finishBegin, finishEnd,
            applicationTypes, applicationTags, name, unselectedFields);
        if (rmApps != null) {
          return rmApps;
        }
      } catch (Exception e) {
        LOG.warn("Failed to get application report.", e);
      }
      routerMetrics.incrMultipleAppsFailedRetrieved();
      LOG.error("Subcluster {} failed to return appReport.", subCluster.getSubClusterId());
      return null;
    }).collect(Collectors.toList());

    appsInfos.forEach(appsInfo -> {
      if (appsInfo != null) {
        apps.addAll(appsInfo.getApps());
        long stopTime = clock.getTime();
        routerMetrics.succeededMultipleAppsRetrieved(stopTime - startTime);
      }
    });

    if (apps.getApps().isEmpty()) {
      return null;
    }

    // Merge all the application reports got from all the available YARN RMs
    AppsInfo resultAppsInfo = RouterWebServiceUtil.mergeAppsInfo(
        apps.getApps(), returnPartialReport);

    if (appInfosCacheEnabled && routerAppInfoCacheKey != null) {
      appInfosCaches.put(routerAppInfoCacheKey, resultAppsInfo);
    }

    return resultAppsInfo;
  }

  /**
   * Get a copy of a HTTP request. This is for thread safety.
   * @param hsr HTTP servlet request to copy.
   * @return Copy of the HTTP request.
   */
  private HttpServletRequestWrapper clone(final HttpServletRequest hsr) {
    if (hsr == null) {
      return null;
    }

    final Map<String, String[]> parameterMap = hsr.getParameterMap();
    final String pathInfo = hsr.getPathInfo();
    final String user = hsr.getRemoteUser();
    final Principal principal = hsr.getUserPrincipal();
    final String mediaType = RouterWebServiceUtil.getMediaTypeFromHttpServletRequest(
        hsr, AppsInfo.class);
    return new HttpServletRequestWrapper(hsr) {
        public Map<String, String[]> getParameterMap() {
          return parameterMap;
        }
        public String getPathInfo() {
          return pathInfo;
        }
        public String getRemoteUser() {
          return user;
        }
        public Principal getUserPrincipal() {
          return principal;
        }
        public String getHeader(String value) {
          // we override only Accept
          if (value.equals(HttpHeaders.ACCEPT)) {
            return mediaType;
          }
          return null;
        }
      };
  }

  /**
   * Get the active subcluster in the federation.
   *
   * @param subClusterId subClusterId.
   * @return subClusterInfo.
   * @throws NotFoundException If the subclusters cannot be found.
   */
  private SubClusterInfo getActiveSubCluster(String subClusterId)
      throws NotFoundException {
    try {
      SubClusterId pSubClusterId = SubClusterId.newInstance(subClusterId);
      return federationFacade.getSubCluster(pSubClusterId);
    } catch (YarnException e) {
      throw new NotFoundException(e.getMessage());
    }
  }

  /**
   * The YARN Router will forward to the request to all the SubClusters to find
   * where the node is running.
   * <p>
   * Possible failure:
   * <p>
   * Client: identical behavior as {@code RMWebServices}.
   * <p>
   * Router: the Client will timeout and resubmit the request.
   * <p>
   * ResourceManager: the Router will timeout and the call will fail.
   * <p>
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public NodeInfo getNode(String nodeId) {

    final Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();

    if (subClustersActive.isEmpty()) {
      throw new NotFoundException(FederationPolicyUtils.NO_ACTIVE_SUBCLUSTER_AVAILABLE);
    }

    final Map<SubClusterInfo, NodeInfo> results = getNode(subClustersActive, nodeId);

    // Collect the responses
    NodeInfo nodeInfo = null;
    for (NodeInfo nodeResponse : results.values()) {
      try {
        // Check if the node was already found in a different SubCluster and
        // it has an old health report
        if (nodeInfo == null || nodeInfo.getLastHealthUpdate() <
            nodeResponse.getLastHealthUpdate()) {
          nodeInfo = nodeResponse;
        }
      } catch (Throwable e) {
        LOG.warn("Failed to get node report ", e);
      }
    }

    if (nodeInfo == null) {
      throw new NotFoundException("nodeId, " + nodeId + ", is not found");
    }
    return nodeInfo;
  }

  /**
   * Get a node and the subcluster where it is.
   *
   * @param subClusters Subclusters where to search.
   * @param nodeId      Identifier of the node we are looking for.
   * @return Map between subcluster and node.
   */
  private Map<SubClusterInfo, NodeInfo> getNode(Collection<SubClusterInfo> subClusters,
      String nodeId) {

    // Parallel traversal of subClusters
    Stream<Pair<SubClusterInfo, NodeInfo>> pairStream = subClusters.parallelStream().map(
        subClusterInfo -> {
            final SubClusterId subClusterId = subClusterInfo.getSubClusterId();
            try {
              DefaultRequestInterceptorREST interceptor =
                   getOrCreateInterceptorForSubCluster(subClusterInfo);
              return Pair.of(subClusterInfo, interceptor.getNode(nodeId));
            } catch (Exception e) {
              LOG.error("Subcluster {} failed to return nodeInfo.", subClusterId, e);
              return null;
            }
        });

    // Collect the results
    final Map<SubClusterInfo, NodeInfo> results = new HashMap<>();
    pairStream.forEach(pair -> {
      if (pair != null) {
        SubClusterInfo subCluster = pair.getKey();
        NodeInfo nodeInfo = pair.getValue();
        results.put(subCluster, nodeInfo);
      }
    });

    return results;
  }

  /**
   * Get the subcluster a node belongs to.
   *
   * @param nodeId Identifier of the node we are looking for.
   * @return The subcluster containing the node.
   * @throws NotFoundException If the node cannot be found.
   */
  private SubClusterInfo getNodeSubcluster(String nodeId) throws NotFoundException {

    final Collection<SubClusterInfo> subClusters = federationFacade.getActiveSubClusters();
    final Map<SubClusterInfo, NodeInfo> results = getNode(subClusters, nodeId);

    SubClusterInfo subcluster = null;
    NodeInfo nodeInfo = null;
    for (Entry<SubClusterInfo, NodeInfo> entry : results.entrySet()) {
      NodeInfo nodeResponse = entry.getValue();
      if (nodeInfo == null || nodeInfo.getLastHealthUpdate() <
          nodeResponse.getLastHealthUpdate()) {
        subcluster = entry.getKey();
        nodeInfo = nodeResponse;
      }
    }
    if (subcluster == null) {
      throw new NotFoundException("Cannot find " + nodeId + " in any subcluster");
    }
    return subcluster;
  }

  /**
   * The YARN Router will forward the request to all the YARN RMs in parallel,
   * after that it will remove all the duplicated NodeInfo by using the NodeId.
   * <p>
   * Possible failure:
   * <p>
   * Client: identical behavior as {@code RMWebServices}.
   * <p>
   * Router: the Client will timeout and resubmit the request.
   * <p>
   * ResourceManager: the Router calls each YARN RM in parallel by using one
   * thread for each YARN RM. In case a YARN RM fails, a single call will
   * timeout. However, the Router will use the NodesInfo it got, and provides a
   * partial list to the client.
   * <p>
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public NodesInfo getNodes(String states) {

    NodesInfo nodes = new NodesInfo();
    try {
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      Class[] argsClasses = new Class[]{String.class};
      Object[] args = new Object[]{states};
      ClientMethod remoteMethod = new ClientMethod("getNodes", argsClasses, args);
      Map<SubClusterInfo, NodesInfo> nodesMap =
          invokeConcurrent(subClustersActive, remoteMethod, NodesInfo.class);
      nodesMap.values().forEach(nodesInfo -> nodes.addAll(nodesInfo.getNodes()));
    } catch (NotFoundException e) {
      LOG.error("get all active sub cluster(s) error.", e);
      throw e;
    } catch (YarnException e) {
      LOG.error("getNodes by states = {} error.", states, e);
      throw new YarnRuntimeException(e);
    } catch (IOException e) {
      LOG.error("getNodes by states = {} error with io error.", states, e);
      throw new YarnRuntimeException(e);
    }

    // Delete duplicate from all the node reports got from all the available
    // YARN RMs. Nodes can be moved from one subclusters to another. In this
    // operation they result LOST/RUNNING in the previous SubCluster and
    // NEW/RUNNING in the new one.
    return RouterWebServiceUtil.deleteDuplicateNodesInfo(nodes.getNodes());
  }

  /**
   * This method changes the resources of a specific node, and it is reachable
   * by using {@link RMWSConsts#NODE_RESOURCE}.
   *
   * @param hsr The servlet request.
   * @param nodeId The node we want to retrieve the information for.
   *               It is a PathParam.
   * @param resourceOption The resource change.
   * @return the resources of a specific node.
   */
  @Override
  public ResourceInfo updateNodeResource(HttpServletRequest hsr,
      String nodeId, ResourceOptionInfo resourceOption) {
    DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByNodeId(nodeId);
    return interceptor.updateNodeResource(hsr, nodeId, resourceOption);
  }

  @Override
  public ClusterMetricsInfo getClusterMetricsInfo() {
    ClusterMetricsInfo metrics = new ClusterMetricsInfo();

    Collection<SubClusterInfo> subClusterInfos = federationFacade.getActiveSubClusters();

    Stream<ClusterMetricsInfo> clusterMetricsInfoStream = subClusterInfos.parallelStream()
        .map(subClusterInfo -> {
          DefaultRequestInterceptorREST interceptor =
              getOrCreateInterceptorForSubCluster(subClusterInfo);
          try {
            return interceptor.getClusterMetricsInfo();
          } catch (Exception e) {
            LOG.error("Subcluster {} failed to return Cluster Metrics.",
                subClusterInfo.getSubClusterId());
            return null;
          }
        });

    clusterMetricsInfoStream.forEach(clusterMetricsInfo -> {
      try {
        if (clusterMetricsInfo != null) {
          RouterWebServiceUtil.mergeMetrics(metrics, clusterMetricsInfo);
        }
      } catch (Throwable e) {
        LOG.warn("Failed to get nodes report.", e);
      }
    });

    return metrics;
  }

  /**
   * The YARN Router will forward to the respective YARN RM in which the AM is
   * running.
   * <p>
   * Possible failure:
   * <p>
   * Client: identical behavior as {@code RMWebServices}.
   * <p>
   * Router: the Client will timeout and resubmit the request.
   * <p>
   * ResourceManager: the Router will timeout and the call will fail.
   * <p>
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public AppState getAppState(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    try {
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      if (interceptor != null) {
        return interceptor.getAppState(hsr, appId);
      }
    } catch (YarnException | IllegalArgumentException e) {
      LOG.error("getHomeSubClusterInfoByAppId error, applicationId = {}.", appId, e);
    }
    return null;
  }

  @Override
  public ClusterInfo get() {
    return getClusterInfo();
  }

  /**
   * This method retrieves the cluster information, and it is reachable by using
   * {@link RMWSConsts#INFO}.
   *
   * In Federation mode, we will return a FederationClusterInfo object,
   * which contains a set of ClusterInfo.
   *
   * @return the cluster information.
   */
  @Override
  public ClusterInfo getClusterInfo() {
    try {
      long startTime = Time.now();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      Class[] argsClasses = new Class[]{};
      Object[] args = new Object[]{};
      ClientMethod remoteMethod = new ClientMethod("getClusterInfo", argsClasses, args);
      Map<SubClusterInfo, ClusterInfo> subClusterInfoMap =
          invokeConcurrent(subClustersActive, remoteMethod, ClusterInfo.class);
      FederationClusterInfo federationClusterInfo = new FederationClusterInfo();
      subClusterInfoMap.forEach((subClusterInfo, clusterInfo) -> {
        SubClusterId subClusterId = subClusterInfo.getSubClusterId();
        clusterInfo.setSubClusterId(subClusterId.getId());
        federationClusterInfo.getList().add(clusterInfo);
      });
      long stopTime = Time.now();
      routerMetrics.succeededGetClusterInfoRetrieved(stopTime - startTime);
      return federationClusterInfo;
    } catch (NotFoundException e) {
      routerMetrics.incrGetClusterInfoFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("Get all active sub cluster(s) error.", e);
    } catch (YarnException | IOException e) {
      routerMetrics.incrGetClusterInfoFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("getClusterInfo error.", e);
    }
    routerMetrics.incrGetClusterInfoFailedRetrieved();
    throw new RuntimeException("getClusterInfo error.");
  }

  /**
   * This method retrieves the cluster user information, and it is reachable by using
   * {@link RMWSConsts#CLUSTER_USER_INFO}.
   *
   * In Federation mode, we will return a ClusterUserInfo object,
   * which contains a set of ClusterUserInfo.
   *
   * @param hsr the servlet request
   * @return the cluster user information
   */
  @Override
  public ClusterUserInfo getClusterUserInfo(HttpServletRequest hsr) {
    try {
      long startTime = Time.now();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class};
      Object[] args = new Object[]{hsrCopy};
      ClientMethod remoteMethod = new ClientMethod("getClusterUserInfo", argsClasses, args);
      Map<SubClusterInfo, ClusterUserInfo> subClusterInfoMap =
          invokeConcurrent(subClustersActive, remoteMethod, ClusterUserInfo.class);
      FederationClusterUserInfo federationClusterUserInfo = new FederationClusterUserInfo();
      subClusterInfoMap.forEach((subClusterInfo, clusterUserInfo) -> {
        SubClusterId subClusterId = subClusterInfo.getSubClusterId();
        clusterUserInfo.setSubClusterId(subClusterId.getId());
        federationClusterUserInfo.getList().add(clusterUserInfo);
      });
      long stopTime = Time.now();
      routerMetrics.succeededGetClusterUserInfoRetrieved(stopTime - startTime);
      return federationClusterUserInfo;
    } catch (NotFoundException e) {
      routerMetrics.incrGetClusterUserInfoFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("Get all active sub cluster(s) error.", e);
    } catch (YarnException | IOException e) {
      routerMetrics.incrGetClusterUserInfoFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("getClusterUserInfo error.", e);
    }
    routerMetrics.incrGetClusterUserInfoFailedRetrieved();
    throw new RuntimeException("getClusterUserInfo error.");
  }

  /**
   * This method retrieves the current scheduler status, and it is reachable by
   * using {@link RMWSConsts#SCHEDULER}.
   * For the federation mode, the SchedulerType information of the cluster
   * cannot be integrated and displayed, and the specific cluster information needs to be marked.
   *
   * @return the current scheduler status
   */
  @Override
  public SchedulerTypeInfo getSchedulerInfo() {
    try {
      long startTime = Time.now();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      Class[] argsClasses = new Class[]{};
      Object[] args = new Object[]{};
      ClientMethod remoteMethod = new ClientMethod("getSchedulerInfo", argsClasses, args);
      Map<SubClusterInfo, SchedulerTypeInfo> subClusterInfoMap =
          invokeConcurrent(subClustersActive, remoteMethod, SchedulerTypeInfo.class);
      FederationSchedulerTypeInfo federationSchedulerTypeInfo = new FederationSchedulerTypeInfo();
      subClusterInfoMap.forEach((subClusterInfo, schedulerTypeInfo) -> {
        SubClusterId subClusterId = subClusterInfo.getSubClusterId();
        schedulerTypeInfo.setSubClusterId(subClusterId.getId());
        federationSchedulerTypeInfo.getList().add(schedulerTypeInfo);
      });
      long stopTime = Time.now();
      routerMetrics.succeededGetSchedulerInfoRetrieved(stopTime - startTime);
      return federationSchedulerTypeInfo;
    } catch (NotFoundException e) {
      routerMetrics.incrGetSchedulerInfoFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("Get all active sub cluster(s) error.", e);
    } catch (YarnException | IOException e) {
      routerMetrics.incrGetSchedulerInfoFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("getSchedulerInfo error.", e);
    }
    routerMetrics.incrGetSchedulerInfoFailedRetrieved();
    throw new RuntimeException("getSchedulerInfo error.");
  }

  /**
   * This method dumps the scheduler logs for the time got in input, and it is
   * reachable by using {@link RMWSConsts#SCHEDULER_LOGS}.
   *
   * @param time the period of time. It is a FormParam.
   * @param hsr the servlet request
   * @return the result of the operation
   * @throws IOException when it cannot create dump log file
   */
  @Override
  public String dumpSchedulerLogs(String time, HttpServletRequest hsr)
      throws IOException {

    // Step1. We will check the time parameter to
    // ensure that the time parameter is not empty and greater than 0.

    if (StringUtils.isBlank(time)) {
      routerMetrics.incrDumpSchedulerLogsFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the time is empty or null.");
    }

    try {
      int period = Integer.parseInt(time);
      if (period <= 0) {
        throw new IllegalArgumentException("time must be greater than 0.");
      }
    } catch (NumberFormatException e) {
      routerMetrics.incrDumpSchedulerLogsFailedRetrieved();
      throw new IllegalArgumentException("time must be a number.");
    } catch (IllegalArgumentException e) {
      routerMetrics.incrDumpSchedulerLogsFailedRetrieved();
      throw e;
    }

    // Step2. Call dumpSchedulerLogs of each subcluster.
    try {
      long startTime = clock.getTime();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{String.class, HttpServletRequest.class};
      Object[] args = new Object[]{time, hsrCopy};
      ClientMethod remoteMethod = new ClientMethod("dumpSchedulerLogs", argsClasses, args);
      Map<SubClusterInfo, String> dumpSchedulerLogsMap = invokeConcurrent(
          subClustersActive, remoteMethod, String.class);
      StringBuilder stringBuilder = new StringBuilder();
      dumpSchedulerLogsMap.forEach((subClusterInfo, msg) -> {
        SubClusterId subClusterId = subClusterInfo.getSubClusterId();
        stringBuilder.append("subClusterId")
            .append(subClusterId).append(" : ").append(msg).append("; ");
      });
      long stopTime = clock.getTime();
      routerMetrics.succeededDumpSchedulerLogsRetrieved(stopTime - startTime);
      return stringBuilder.toString();
    } catch (IllegalArgumentException e) {
      routerMetrics.incrDumpSchedulerLogsFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to dump SchedulerLogs by time: %s.", time);
    } catch (YarnException e) {
      routerMetrics.incrDumpSchedulerLogsFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "dumpSchedulerLogs by time = %s error .", time);
    }

    routerMetrics.incrDumpSchedulerLogsFailedRetrieved();
    throw new RuntimeException("dumpSchedulerLogs Failed.");
  }

  /**
   * This method retrieve all the activities in a specific node, and it is
   * reachable by using {@link RMWSConsts#SCHEDULER_ACTIVITIES}.
   *
   * @param hsr the servlet request
   * @param nodeId the node we want to retrieve the activities. It is a
   *          QueryParam.
   * @param groupBy the groupBy type by which the activities should be
   *          aggregated. It is a QueryParam.
   * @return all the activities in the specific node
   */
  @Override
  public ActivitiesInfo getActivities(HttpServletRequest hsr, String nodeId,
      String groupBy) {
    try {
      // Check the parameters to ensure that the parameters are not empty
      Validate.checkNotNullAndNotEmpty(nodeId, "nodeId");
      Validate.checkNotNullAndNotEmpty(groupBy, "groupBy");

      // Query SubClusterInfo according to id,
      // if the nodeId cannot get SubClusterInfo, an exception will be thrown directly.
      // Call the corresponding subCluster to get ActivitiesInfo.
      long startTime = clock.getTime();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByNodeId(nodeId);
      final HttpServletRequest hsrCopy = clone(hsr);
      ActivitiesInfo activitiesInfo = interceptor.getActivities(hsrCopy, nodeId, groupBy);
      if (activitiesInfo != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetActivitiesLatencyRetrieved(stopTime - startTime);
        return activitiesInfo;
      }
    } catch (IllegalArgumentException | NotFoundException e) {
      routerMetrics.incrGetActivitiesFailedRetrieved();
      throw e;
    }

    routerMetrics.incrGetActivitiesFailedRetrieved();
    throw new RuntimeException("getActivities Failed.");
  }

  /**
   * This method retrieve the last n activities inside scheduler, and it is
   * reachable by using {@link RMWSConsts#SCHEDULER_BULK_ACTIVITIES}.
   *
   * @param hsr the servlet request
   * @param groupBy the groupBy type by which the activities should be
   *        aggregated. It is a QueryParam.
   * @param activitiesCount number of activities
   * @return last n activities
   */
  @Override
  public BulkActivitiesInfo getBulkActivities(HttpServletRequest hsr,
      String groupBy, int activitiesCount) throws InterruptedException {
    try {
      // Step1. Check the parameters to ensure that the parameters are not empty
      Validate.checkNotNullAndNotEmpty(groupBy, "groupBy");
      Validate.checkNotNegative(activitiesCount, "activitiesCount");

      // Step2. Call the interface of subCluster concurrently and get the returned result.
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class, String.class, int.class};
      Object[] args = new Object[]{hsrCopy, groupBy, activitiesCount};
      ClientMethod remoteMethod = new ClientMethod("getBulkActivities", argsClasses, args);
      Map<SubClusterInfo, BulkActivitiesInfo> appStatisticsMap = invokeConcurrent(
          subClustersActive, remoteMethod, BulkActivitiesInfo.class);

      // Step3. Generate Federation objects and set subCluster information.
      long startTime = clock.getTime();
      FederationBulkActivitiesInfo fedBulkActivitiesInfo = new FederationBulkActivitiesInfo();
      appStatisticsMap.forEach((subClusterInfo, bulkActivitiesInfo) -> {
        SubClusterId subClusterId = subClusterInfo.getSubClusterId();
        bulkActivitiesInfo.setSubClusterId(subClusterId.getId());
        fedBulkActivitiesInfo.getList().add(bulkActivitiesInfo);
      });
      long stopTime = clock.getTime();
      routerMetrics.succeededGetBulkActivitiesRetrieved(stopTime - startTime);
      return fedBulkActivitiesInfo;
    } catch (IllegalArgumentException e) {
      routerMetrics.incrGetBulkActivitiesFailedRetrieved();
      throw e;
    } catch (NotFoundException e) {
      routerMetrics.incrGetBulkActivitiesFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("get all active sub cluster(s) error.", e);
    } catch (IOException e) {
      routerMetrics.incrGetBulkActivitiesFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "getBulkActivities by groupBy = %s, activitiesCount = %s with io error.",
          groupBy, String.valueOf(activitiesCount));
    } catch (YarnException e) {
      routerMetrics.incrGetBulkActivitiesFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "getBulkActivities by groupBy = %s, activitiesCount = %s with yarn error.",
          groupBy, String.valueOf(activitiesCount));
    }

    routerMetrics.incrGetBulkActivitiesFailedRetrieved();
    throw new RuntimeException("getBulkActivities Failed.");
  }

  @Override
  public AppActivitiesInfo getAppActivities(HttpServletRequest hsr,
      String appId, String time, Set<String> requestPriorities,
      Set<String> allocationRequestIds, String groupBy, String limit,
      Set<String> actions, boolean summarize) {

    try {
      long startTime = clock.getTime();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      final HttpServletRequest hsrCopy = clone(hsr);
      AppActivitiesInfo appActivitiesInfo = interceptor.getAppActivities(hsrCopy, appId, time,
          requestPriorities, allocationRequestIds, groupBy, limit, actions, summarize);
      if (appActivitiesInfo != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetAppActivitiesRetrieved(stopTime - startTime);
        return appActivitiesInfo;
      }
    } catch (IllegalArgumentException e) {
      routerMetrics.incrGetAppActivitiesFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get subCluster by appId: %s.", appId);
    } catch (YarnException e) {
      routerMetrics.incrGetAppActivitiesFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "getAppActivities by appId = %s error .", appId);
    }
    routerMetrics.incrGetAppActivitiesFailedRetrieved();
    throw new RuntimeException("getAppActivities Failed.");
  }

  @Override
  public ApplicationStatisticsInfo getAppStatistics(HttpServletRequest hsr,
      Set<String> stateQueries, Set<String> typeQueries) {
    try {
      long startTime = clock.getTime();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class, Set.class, Set.class};
      Object[] args = new Object[]{hsrCopy, stateQueries, typeQueries};
      ClientMethod remoteMethod = new ClientMethod("getAppStatistics", argsClasses, args);
      Map<SubClusterInfo, ApplicationStatisticsInfo> appStatisticsMap = invokeConcurrent(
          subClustersActive, remoteMethod, ApplicationStatisticsInfo.class);
      ApplicationStatisticsInfo applicationStatisticsInfo  =
          RouterWebServiceUtil.mergeApplicationStatisticsInfo(appStatisticsMap.values());
      if (applicationStatisticsInfo != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetAppStatisticsRetrieved(stopTime - startTime);
        return applicationStatisticsInfo;
      }
    } catch (NotFoundException e) {
      routerMetrics.incrGetAppStatisticsFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("get all active sub cluster(s) error.", e);
    } catch (IOException e) {
      routerMetrics.incrGetAppStatisticsFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "getAppStatistics error by stateQueries = %s, typeQueries = %s with io error.",
          StringUtils.join(stateQueries, ","), StringUtils.join(typeQueries, ","));
    } catch (YarnException e) {
      routerMetrics.incrGetAppStatisticsFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "getAppStatistics by stateQueries = %s, typeQueries = %s with yarn error.",
          StringUtils.join(stateQueries, ","), StringUtils.join(typeQueries, ","));
    }
    routerMetrics.incrGetAppStatisticsFailedRetrieved();
    throw RouterServerUtil.logAndReturnRunTimeException(
        "getAppStatistics by stateQueries = %s, typeQueries = %s Failed.",
        StringUtils.join(stateQueries, ","), StringUtils.join(typeQueries, ","));
  }

  @Override
  public NodeToLabelsInfo getNodeToLabels(HttpServletRequest hsr)
      throws IOException {
    try {
      long startTime = clock.getTime();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class};
      Object[] args = new Object[]{hsrCopy};
      ClientMethod remoteMethod = new ClientMethod("getNodeToLabels", argsClasses, args);
      Map<SubClusterInfo, NodeToLabelsInfo> nodeToLabelsInfoMap =
          invokeConcurrent(subClustersActive, remoteMethod, NodeToLabelsInfo.class);
      NodeToLabelsInfo nodeToLabelsInfo =
          RouterWebServiceUtil.mergeNodeToLabels(nodeToLabelsInfoMap);
      if (nodeToLabelsInfo != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetNodeToLabelsRetrieved(stopTime - startTime);
        return nodeToLabelsInfo;
      }
    } catch (NotFoundException e) {
      routerMetrics.incrNodeToLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      routerMetrics.incrNodeToLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("getNodeToLabels error.", e);
    }
    routerMetrics.incrNodeToLabelsFailedRetrieved();
    throw new RuntimeException("getNodeToLabels Failed.");
  }

  @Override
  public NodeLabelsInfo getRMNodeLabels(HttpServletRequest hsr) throws IOException {
    try {
      long startTime = clock.getTime();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class};
      Object[] args = new Object[]{hsrCopy};
      ClientMethod remoteMethod = new ClientMethod("getRMNodeLabels", argsClasses, args);
      Map<SubClusterInfo, NodeLabelsInfo> nodeToLabelsInfoMap =
          invokeConcurrent(subClustersActive, remoteMethod, NodeLabelsInfo.class);
      NodeLabelsInfo nodeToLabelsInfo =
          RouterWebServiceUtil.mergeNodeLabelsInfo(nodeToLabelsInfoMap);
      if (nodeToLabelsInfo != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetRMNodeLabelsRetrieved(stopTime - startTime);
        return nodeToLabelsInfo;
      }
    } catch (NotFoundException e) {
      routerMetrics.incrGetRMNodeLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      routerMetrics.incrGetRMNodeLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("getRMNodeLabels error.", e);
    }
    routerMetrics.incrGetRMNodeLabelsFailedRetrieved();
    throw new RuntimeException("getRMNodeLabels Failed.");
  }

  @Override
  public LabelsToNodesInfo getLabelsToNodes(Set<String> labels)
      throws IOException {
    try {
      long startTime = clock.getTime();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      Class[] argsClasses = new Class[]{Set.class};
      Object[] args = new Object[]{labels};
      ClientMethod remoteMethod = new ClientMethod("getLabelsToNodes", argsClasses, args);
      Map<SubClusterInfo, LabelsToNodesInfo> labelsToNodesInfoMap =
          invokeConcurrent(subClustersActive, remoteMethod, LabelsToNodesInfo.class);
      Map<NodeLabelInfo, NodeIDsInfo> labelToNodesMap = new HashMap<>();
      labelsToNodesInfoMap.values().forEach(labelsToNode -> {
        Map<NodeLabelInfo, NodeIDsInfo> values = labelsToNode.getLabelsToNodes();
        for (Map.Entry<NodeLabelInfo, NodeIDsInfo> item : values.entrySet()) {
          NodeLabelInfo key = item.getKey();
          NodeIDsInfo leftValue = item.getValue();
          NodeIDsInfo rightValue = labelToNodesMap.getOrDefault(key, null);
          NodeIDsInfo newValue = NodeIDsInfo.add(leftValue, rightValue);
          labelToNodesMap.put(key, newValue);
        }
      });
      LabelsToNodesInfo labelsToNodesInfo = new LabelsToNodesInfo(labelToNodesMap);
      if (labelsToNodesInfo != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetLabelsToNodesRetrieved(stopTime - startTime);
        return labelsToNodesInfo;
      }
    } catch (NotFoundException e) {
      routerMetrics.incrLabelsToNodesFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      routerMetrics.incrLabelsToNodesFailedRetrieved();
      RouterServerUtil.logAndThrowIOException(
          e, "getLabelsToNodes by labels = %s with yarn error.", StringUtils.join(labels, ","));
    }
    routerMetrics.incrLabelsToNodesFailedRetrieved();
    throw RouterServerUtil.logAndReturnRunTimeException(
        "getLabelsToNodes by labels = %s Failed.", StringUtils.join(labels, ","));
  }

  /**
   * This method replaces all the node labels for specific nodes, and it is
   * reachable by using {@link RMWSConsts#REPLACE_NODE_TO_LABELS}.
   *
   * @see ResourceManagerAdministrationProtocol#replaceLabelsOnNode
   * @param newNodeToLabels the list of new labels. It is a content param.
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws IOException if an exception happened
   */
  @Override
  public Response replaceLabelsOnNodes(NodeToLabelsEntryList newNodeToLabels,
      HttpServletRequest hsr) throws IOException {

    // Step1. Check the parameters to ensure that the parameters are not empty.
    if (newNodeToLabels == null) {
      routerMetrics.incrReplaceLabelsOnNodesFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, newNodeToLabels must not be empty.");
    }
    List<NodeToLabelsEntry> nodeToLabelsEntries = newNodeToLabels.getNodeToLabels();
    if (CollectionUtils.isEmpty(nodeToLabelsEntries)) {
      routerMetrics.incrReplaceLabelsOnNodesFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, " +
         "nodeToLabelsEntries must not be empty.");
    }

    try {

      // Step2. We map the NodeId and NodeToLabelsEntry in the request.
      Map<String, NodeToLabelsEntry> nodeIdToLabels = new HashMap<>();
      newNodeToLabels.getNodeToLabels().forEach(nodeIdToLabel -> {
        String nodeId = nodeIdToLabel.getNodeId();
        nodeIdToLabels.put(nodeId, nodeIdToLabel);
      });

      // Step3. We map SubCluster with NodeToLabelsEntryList
      Map<SubClusterInfo, NodeToLabelsEntryList> subClusterToNodeToLabelsEntryList =
          new HashMap<>();
      nodeIdToLabels.forEach((nodeId, nodeToLabelsEntry) -> {
        SubClusterInfo subClusterInfo = getNodeSubcluster(nodeId);
        NodeToLabelsEntryList nodeToLabelsEntryList = subClusterToNodeToLabelsEntryList.
            getOrDefault(subClusterInfo, new NodeToLabelsEntryList());
        nodeToLabelsEntryList.getNodeToLabels().add(nodeToLabelsEntry);
        subClusterToNodeToLabelsEntryList.put(subClusterInfo, nodeToLabelsEntryList);
      });

      // Step4. Traverse the subCluster and call the replaceLabelsOnNodes interface.
      long startTime = clock.getTime();
      final HttpServletRequest hsrCopy = clone(hsr);
      StringBuilder builder = new StringBuilder();
      subClusterToNodeToLabelsEntryList.forEach((subClusterInfo, nodeToLabelsEntryList) -> {
        SubClusterId subClusterId = subClusterInfo.getSubClusterId();
        try {
          DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
              subClusterInfo);
          interceptor.replaceLabelsOnNodes(nodeToLabelsEntryList, hsrCopy);
          builder.append("subCluster-").append(subClusterId.getId()).append(":Success,");
        } catch (Exception e) {
          LOG.error("replaceLabelsOnNodes Failed. subClusterId = {}.", subClusterId, e);
          builder.append("subCluster-").append(subClusterId.getId()).append(":Failed,");
        }
      });
      long stopTime = clock.getTime();
      routerMetrics.succeededReplaceLabelsOnNodesRetrieved(stopTime - startTime);

      // Step5. return call result.
      return Response.status(Status.OK).entity(builder.toString()).build();
    } catch (Exception e) {
      routerMetrics.incrReplaceLabelsOnNodesFailedRetrieved();
      throw e;
    }
  }

  /**
   * This method replaces all the node labels for specific node, and it is
   * reachable by using {@link RMWSConsts#NODES_NODEID_REPLACE_LABELS}.
   *
   * @see ResourceManagerAdministrationProtocol#replaceLabelsOnNode
   * @param newNodeLabelsName the list of new labels. It is a QueryParam.
   * @param hsr the servlet request
   * @param nodeId the node we want to replace the node labels. It is a
   *     PathParam.
   * @return Response containing the status code
   * @throws Exception if an exception happened
   */
  @Override
  public Response replaceLabelsOnNode(Set<String> newNodeLabelsName,
      HttpServletRequest hsr, String nodeId) throws Exception {

    // Step1. Check the parameters to ensure that the parameters are not empty.
    if (StringUtils.isBlank(nodeId)) {
      routerMetrics.incrReplaceLabelsOnNodeFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, nodeId must not be null or empty.");
    }
    if (CollectionUtils.isEmpty(newNodeLabelsName)) {
      routerMetrics.incrReplaceLabelsOnNodeFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, newNodeLabelsName must not be empty.");
    }

    try {
      // Step2. We find the subCluster according to the nodeId,
      // and then call the replaceLabelsOnNode of the subCluster.
      long startTime = clock.getTime();
      SubClusterInfo subClusterInfo = getNodeSubcluster(nodeId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByNodeId(nodeId);
      final HttpServletRequest hsrCopy = clone(hsr);
      interceptor.replaceLabelsOnNode(newNodeLabelsName, hsrCopy, nodeId);

      // Step3. Return the response result.
      long stopTime = clock.getTime();
      routerMetrics.succeededReplaceLabelsOnNodeRetrieved(stopTime - startTime);
      String msg = "subCluster#" + subClusterInfo.getSubClusterId().getId() + ":Success;";
      return Response.status(Status.OK).entity(msg).build();
    } catch (Exception e) {
      routerMetrics.incrReplaceLabelsOnNodeFailedRetrieved();
      throw e;
    }
  }

  @Override
  public NodeLabelsInfo getClusterNodeLabels(HttpServletRequest hsr)
      throws IOException {
    try {
      long startTime = clock.getTime();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class};
      Object[] args = new Object[]{hsrCopy};
      ClientMethod remoteMethod = new ClientMethod("getClusterNodeLabels", argsClasses, args);
      Map<SubClusterInfo, NodeLabelsInfo> nodeToLabelsInfoMap =
          invokeConcurrent(subClustersActive, remoteMethod, NodeLabelsInfo.class);
      Set<NodeLabel> hashSets = Sets.newHashSet();
      nodeToLabelsInfoMap.values().forEach(item -> hashSets.addAll(item.getNodeLabels()));
      NodeLabelsInfo nodeLabelsInfo = new NodeLabelsInfo(hashSets);
      if (nodeLabelsInfo != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetClusterNodeLabelsRetrieved(stopTime - startTime);
        return nodeLabelsInfo;
      }
    } catch (NotFoundException e) {
      routerMetrics.incrClusterNodeLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      routerMetrics.incrClusterNodeLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("getClusterNodeLabels with yarn error.", e);
    }
    routerMetrics.incrClusterNodeLabelsFailedRetrieved();
    throw new RuntimeException("getClusterNodeLabels Failed.");
  }

  /**
   * This method adds specific node labels for specific nodes, and it is
   * reachable by using {@link RMWSConsts#ADD_NODE_LABELS}.
   *
   * @see ResourceManagerAdministrationProtocol#addToClusterNodeLabels
   * @param newNodeLabels the node labels to add. It is a content param.
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws Exception in case of bad request
   */
  @Override
  public Response addToClusterNodeLabels(NodeLabelsInfo newNodeLabels,
      HttpServletRequest hsr) throws Exception {

    if (newNodeLabels == null) {
      routerMetrics.incrAddToClusterNodeLabelsFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the newNodeLabels is null.");
    }

    List<NodeLabelInfo> nodeLabelInfos = newNodeLabels.getNodeLabelsInfo();
    if (CollectionUtils.isEmpty(nodeLabelInfos)) {
      routerMetrics.incrAddToClusterNodeLabelsFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the nodeLabelsInfo is null or empty.");
    }

    try {
      long startTime = clock.getTime();
      Collection<SubClusterInfo> subClustersActives = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{NodeLabelsInfo.class, HttpServletRequest.class};
      Object[] args = new Object[]{newNodeLabels, hsrCopy};
      ClientMethod remoteMethod = new ClientMethod("addToClusterNodeLabels", argsClasses, args);
      Map<SubClusterInfo, Response> responseInfoMap =
          invokeConcurrent(subClustersActives, remoteMethod, Response.class);
      StringBuffer buffer = new StringBuffer();
      // SubCluster-0:SUCCESS,SubCluster-1:SUCCESS
      responseInfoMap.forEach((subClusterInfo, response) ->
          buildAppendMsg(subClusterInfo, buffer, response));
      long stopTime = clock.getTime();
      routerMetrics.succeededAddToClusterNodeLabelsRetrieved((stopTime - startTime));
      return Response.status(Status.OK).entity(buffer.toString()).build();
    } catch (NotFoundException e) {
      routerMetrics.incrAddToClusterNodeLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      routerMetrics.incrAddToClusterNodeLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("addToClusterNodeLabels with yarn error.", e);
    }

    routerMetrics.incrAddToClusterNodeLabelsFailedRetrieved();
    throw new RuntimeException("addToClusterNodeLabels Failed.");
  }

  /**
   * This method removes all the node labels for specific nodes, and it is
   * reachable by using {@link RMWSConsts#REMOVE_NODE_LABELS}.
   *
   * @see ResourceManagerAdministrationProtocol#removeFromClusterNodeLabels
   * @param oldNodeLabels the node labels to remove. It is a QueryParam.
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws Exception in case of bad request
   */
  @Override
  public Response removeFromClusterNodeLabels(Set<String> oldNodeLabels,
      HttpServletRequest hsr) throws Exception {

    if (CollectionUtils.isEmpty(oldNodeLabels)) {
      routerMetrics.incrRemoveFromClusterNodeLabelsFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the oldNodeLabels is null or empty.");
    }

    try {
      long startTime = clock.getTime();
      Collection<SubClusterInfo> subClustersActives = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{Set.class, HttpServletRequest.class};
      Object[] args = new Object[]{oldNodeLabels, hsrCopy};
      ClientMethod remoteMethod =
          new ClientMethod("removeFromClusterNodeLabels", argsClasses, args);
      Map<SubClusterInfo, Response> responseInfoMap =
          invokeConcurrent(subClustersActives, remoteMethod, Response.class);
      StringBuffer buffer = new StringBuffer();
      // SubCluster-0:SUCCESS,SubCluster-1:SUCCESS
      responseInfoMap.forEach((subClusterInfo, response) ->
          buildAppendMsg(subClusterInfo, buffer, response));
      long stopTime = clock.getTime();
      routerMetrics.succeededRemoveFromClusterNodeLabelsRetrieved(stopTime - startTime);
      return Response.status(Status.OK).entity(buffer.toString()).build();
    } catch (NotFoundException e) {
      routerMetrics.incrRemoveFromClusterNodeLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      routerMetrics.incrRemoveFromClusterNodeLabelsFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("removeFromClusterNodeLabels with yarn error.", e);
    }

    routerMetrics.incrRemoveFromClusterNodeLabelsFailedRetrieved();
    throw new RuntimeException("removeFromClusterNodeLabels Failed.");
  }

  /**
   * Build Append information.
   *
   * @param subClusterInfo subCluster information.
   * @param buffer StringBuffer.
   * @param response response message.
   */
  private void buildAppendMsg(SubClusterInfo subClusterInfo, StringBuffer buffer,
      Response response) {
    SubClusterId subClusterId = subClusterInfo.getSubClusterId();
    String state = response != null &&
        (response.getStatus() == Status.OK.getStatusCode()) ? "SUCCESS" : "FAILED";
    buffer.append("SubCluster-")
        .append(subClusterId.getId())
        .append(":")
        .append(state)
        .append(",");
  }

  @Override
  public NodeLabelsInfo getLabelsOnNode(HttpServletRequest hsr, String nodeId)
      throws IOException {
    try {
      long startTime = clock.getTime();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class, String.class};
      Object[] args = new Object[]{hsrCopy, nodeId};
      ClientMethod remoteMethod = new ClientMethod("getLabelsOnNode", argsClasses, args);
      Map<SubClusterInfo, NodeLabelsInfo> nodeToLabelsInfoMap =
          invokeConcurrent(subClustersActive, remoteMethod, NodeLabelsInfo.class);
      Set<NodeLabel> hashSets = Sets.newHashSet();
      nodeToLabelsInfoMap.values().forEach(item -> hashSets.addAll(item.getNodeLabels()));
      NodeLabelsInfo nodeLabelsInfo = new NodeLabelsInfo(hashSets);
      if (nodeLabelsInfo != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetLabelsToNodesRetrieved(stopTime - startTime);
        return nodeLabelsInfo;
      }
    } catch (NotFoundException e) {
      routerMetrics.incrLabelsToNodesFailedRetrieved();
      RouterServerUtil.logAndThrowIOException("get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      routerMetrics.incrLabelsToNodesFailedRetrieved();
      RouterServerUtil.logAndThrowIOException(
          e, "getLabelsOnNode nodeId = %s with yarn error.", nodeId);
    }
    routerMetrics.incrLabelsToNodesFailedRetrieved();
    throw RouterServerUtil.logAndReturnRunTimeException(
        "getLabelsOnNode by nodeId = %s Failed.", nodeId);
  }

  @Override
  public AppPriority getAppPriority(HttpServletRequest hsr, String appId)
      throws AuthorizationException {

    try {
      long startTime = clock.getTime();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      AppPriority appPriority = interceptor.getAppPriority(hsr, appId);
      if (appPriority != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetAppPriorityRetrieved(stopTime - startTime);
        return appPriority;
      }
    } catch (IllegalArgumentException e) {
      routerMetrics.incrGetAppPriorityFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the getAppPriority appId: %s.", appId);
    } catch (YarnException e) {
      routerMetrics.incrGetAppPriorityFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("getAppPriority error.", e);
    }
    routerMetrics.incrGetAppPriorityFailedRetrieved();
    throw new RuntimeException("getAppPriority Failed.");
  }

  @Override
  public Response updateApplicationPriority(AppPriority targetPriority,
      HttpServletRequest hsr, String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {

    if (targetPriority == null) {
      routerMetrics.incrUpdateAppPriorityFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the targetPriority is empty or null.");
    }

    try {
      long startTime = clock.getTime();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      Response response = interceptor.updateApplicationPriority(targetPriority, hsr, appId);
      if (response != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededUpdateAppPriorityRetrieved(stopTime - startTime);
        return response;
      }
    } catch (IllegalArgumentException e) {
      routerMetrics.incrUpdateAppPriorityFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the updateApplicationPriority appId: %s.", appId);
    } catch (YarnException e) {
      routerMetrics.incrUpdateAppPriorityFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("updateApplicationPriority error.", e);
    }
    routerMetrics.incrUpdateAppPriorityFailedRetrieved();
    throw new RuntimeException("updateApplicationPriority Failed.");
  }

  @Override
  public AppQueue getAppQueue(HttpServletRequest hsr, String appId)
      throws AuthorizationException {

    try {
      long startTime = clock.getTime();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      AppQueue queue = interceptor.getAppQueue(hsr, appId);
      if (queue != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetAppQueueRetrieved((stopTime - startTime));
        return queue;
      }
    } catch (IllegalArgumentException e) {
      routerMetrics.incrGetAppQueueFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e, "Unable to get queue by appId: %s.", appId);
    } catch (YarnException e) {
      routerMetrics.incrGetAppQueueFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("getAppQueue error.", e);
    }
    routerMetrics.incrGetAppQueueFailedRetrieved();
    throw new RuntimeException("getAppQueue Failed.");
  }

  @Override
  public Response updateAppQueue(AppQueue targetQueue, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException {

    if (targetQueue == null) {
      routerMetrics.incrUpdateAppQueueFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the targetQueue is null.");
    }

    try {
      long startTime = clock.getTime();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      Response response = interceptor.updateAppQueue(targetQueue, hsr, appId);
      if (response != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededUpdateAppQueueRetrieved(stopTime - startTime);
        return response;
      }
    } catch (IllegalArgumentException e) {
      routerMetrics.incrUpdateAppQueueFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to update app queue by appId: %s.", appId);
    } catch (YarnException e) {
      routerMetrics.incrUpdateAppQueueFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("updateAppQueue error.", e);
    }
    routerMetrics.incrUpdateAppQueueFailedRetrieved();
    throw new RuntimeException("updateAppQueue Failed.");
  }

  /**
   * This method posts a delegation token from the client.
   *
   * @param tokenData the token to delegate. It is a content param.
   * @param hsr the servlet request.
   * @return Response containing the status code.
   * @throws AuthorizationException if Kerberos auth failed.
   * @throws IOException if the delegation failed.
   * @throws InterruptedException if interrupted.
   * @throws Exception in case of bad request.
   */
  @Override
  public Response postDelegationToken(DelegationToken tokenData, HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException, Exception {

    if (tokenData == null || hsr == null) {
      throw new IllegalArgumentException("Parameter error, the tokenData or hsr is null.");
    }

    try {
      // get Caller UserGroupInformation
      Configuration conf = federationFacade.getConf();
      UserGroupInformation callerUGI = getKerberosUserGroupInformation(conf, hsr);

      // create a delegation token
      return createDelegationToken(tokenData, callerUGI);
    } catch (YarnException e) {
      LOG.error("Create delegation token request failed.", e);
      return Response.status(Status.FORBIDDEN).entity(e.getMessage()).build();
    }
  }

  /**
   * Create DelegationToken.
   *
   * @param dtoken DelegationToken Data.
   * @param callerUGI UserGroupInformation.
   * @return Response.
   * @throws Exception An exception occurred when creating a delegationToken.
   */
  private Response createDelegationToken(DelegationToken dtoken, UserGroupInformation callerUGI)
      throws IOException, InterruptedException {

    String renewer = dtoken.getRenewer();

    GetDelegationTokenResponse resp = callerUGI.doAs(
        (PrivilegedExceptionAction<GetDelegationTokenResponse>) () -> {
        GetDelegationTokenRequest createReq = GetDelegationTokenRequest.newInstance(renewer);
        return this.getRouterClientRMService().getDelegationToken(createReq);
      });

    DelegationToken respToken = getDelegationToken(renewer, resp);
    return Response.status(Status.OK).entity(respToken).build();
  }

  /**
   * Get DelegationToken.
   *
   * @param renewer renewer.
   * @param resp GetDelegationTokenResponse.
   * @return DelegationToken.
   * @throws IOException if there are I/O errors.
   */
  private DelegationToken getDelegationToken(String renewer, GetDelegationTokenResponse resp)
      throws IOException {
    // Step1. Parse token from GetDelegationTokenResponse.
    Token<RMDelegationTokenIdentifier> tk = getToken(resp);
    String tokenKind = tk.getKind().toString();
    RMDelegationTokenIdentifier tokenIdentifier = tk.decodeIdentifier();
    String owner = tokenIdentifier.getOwner().toString();
    long maxDate = tokenIdentifier.getMaxDate();

    // Step2. Call the interface to get the expiration time of Token.
    RouterClientRMService clientRMService = this.getRouterClientRMService();
    RouterDelegationTokenSecretManager tokenSecretManager =
        clientRMService.getRouterDTSecretManager();
    long currentExpiration = tokenSecretManager.getRenewDate(tokenIdentifier);

    // Step3. Generate Delegation token.
    DelegationToken delegationToken = new DelegationToken(tk.encodeToUrlString(),
        renewer, owner, tokenKind, currentExpiration, maxDate);

    return delegationToken;
  }

  /**
   * GetToken.
   * We convert RMDelegationToken in GetDelegationTokenResponse to Token.
   *
   * @param resp GetDelegationTokenResponse.
   * @return Token.
   */
  private static Token<RMDelegationTokenIdentifier> getToken(GetDelegationTokenResponse resp) {
    org.apache.hadoop.yarn.api.records.Token token = resp.getRMDelegationToken();
    byte[] identifier = token.getIdentifier().array();
    byte[] password = token.getPassword().array();
    Text kind = new Text(token.getKind());
    Text service = new Text(token.getService());
    return new Token<>(identifier, password, kind, service);
  }

  /**
   * This method updates the expiration for a delegation token from the client.
   *
   * @param hsr the servlet request
   * @return Response containing the status code.
   * @throws AuthorizationException if Kerberos auth failed.
   * @throws IOException if the delegation failed.
   * @throws InterruptedException  if interrupted.
   * @throws Exception in case of bad request.
   */
  @Override
  public Response postDelegationTokenExpiration(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException, Exception {

    if (hsr == null) {
      throw new IllegalArgumentException("Parameter error, the hsr is null.");
    }

    try {
      // get Caller UserGroupInformation
      Configuration conf = federationFacade.getConf();
      UserGroupInformation callerUGI = getKerberosUserGroupInformation(conf, hsr);
      return renewDelegationToken(hsr, callerUGI);
    } catch (YarnException e) {
      LOG.error("Renew delegation token request failed.", e);
      return Response.status(Status.FORBIDDEN).entity(e.getMessage()).build();
    }
  }

  /**
   * Renew DelegationToken.
   *
   * @param hsr HttpServletRequest.
   * @param callerUGI UserGroupInformation.
   * @return Response
   * @throws IOException if there are I/O errors.
   * @throws InterruptedException if any thread has interrupted.
   */
  private Response renewDelegationToken(HttpServletRequest hsr, UserGroupInformation callerUGI)
      throws IOException, InterruptedException {

    // renew Delegation Token
    DelegationToken tokenData = new DelegationToken();
    String encodeToken = extractToken(hsr).encodeToUrlString();
    tokenData.setToken(encodeToken);

    // Parse token data
    Token<RMDelegationTokenIdentifier> token = extractToken(tokenData.getToken());
    org.apache.hadoop.yarn.api.records.Token dToken =
        BuilderUtils.newDelegationToken(token.getIdentifier(), token.getKind().toString(),
        token.getPassword(), token.getService().toString());

    // Renew token
    RenewDelegationTokenResponse resp = callerUGI.doAs(
        (PrivilegedExceptionAction<RenewDelegationTokenResponse>) () -> {
        RenewDelegationTokenRequest req = RenewDelegationTokenRequest.newInstance(dToken);
        return this.getRouterClientRMService().renewDelegationToken(req);
      });

    // return DelegationToken
    long renewTime = resp.getNextExpirationTime();
    DelegationToken respToken = new DelegationToken();
    respToken.setNextExpirationTime(renewTime);
    return Response.status(Status.OK).entity(respToken).build();
  }

  /**
   * Cancel DelegationToken.
   *
   * @param hsr the servlet request
   * @return  Response containing the status code.
   * @throws AuthorizationException if Kerberos auth failed.
   * @throws IOException if the delegation failed.
   * @throws InterruptedException if interrupted.
   * @throws Exception in case of bad request.
   */
  @Override
  public Response cancelDelegationToken(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException, Exception {
    try {
      // get Caller UserGroupInformation
      Configuration conf = federationFacade.getConf();
      UserGroupInformation callerUGI = getKerberosUserGroupInformation(conf, hsr);

      // parse Token Data
      Token<RMDelegationTokenIdentifier> token = extractToken(hsr);
      org.apache.hadoop.yarn.api.records.Token dToken = BuilderUtils
          .newDelegationToken(token.getIdentifier(), token.getKind().toString(),
          token.getPassword(), token.getService().toString());

      // cancelDelegationToken
      callerUGI.doAs((PrivilegedExceptionAction<CancelDelegationTokenResponse>) () -> {
        CancelDelegationTokenRequest req = CancelDelegationTokenRequest.newInstance(dToken);
        return this.getRouterClientRMService().cancelDelegationToken(req);
      });

      return Response.status(Status.OK).build();
    } catch (YarnException e) {
      LOG.error("Cancel delegation token request failed.", e);
      return Response.status(Status.FORBIDDEN).entity(e.getMessage()).build();
    }
  }

  @Override
  public Response createNewReservation(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    long startTime = clock.getTime();
    try {
      Map<SubClusterId, SubClusterInfo> subClustersActive =
          federationFacade.getSubClusters(true);
      // We declare blackList and retries.
      List<SubClusterId> blackList = new ArrayList<>();
      int actualRetryNums = federationFacade.getRetryNumbers(numSubmitRetries);
      Response response = ((FederationActionRetry<Response>) (retryCount) ->
          invokeCreateNewReservation(subClustersActive, blackList, hsr, retryCount)).
          runWithRetries(actualRetryNums, submitIntervalTime);
      // If the response is not empty and the status is SC_OK,
      // this request can be returned directly.
      if (response != null && response.getStatus() == HttpServletResponse.SC_OK) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetNewReservationRetrieved(stopTime - startTime);
        return response;
      }
    } catch (FederationPolicyException e) {
      // If a FederationPolicyException is thrown, the service is unavailable.
      routerMetrics.incrGetNewReservationFailedRetrieved();
      return Response.status(Status.SERVICE_UNAVAILABLE).entity(e.getLocalizedMessage()).build();
    } catch (Exception e) {
      routerMetrics.incrGetNewReservationFailedRetrieved();
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getLocalizedMessage()).build();
    }

    // return error message directly.
    String errMsg = "Fail to create a new reservation.";
    LOG.error(errMsg);
    routerMetrics.incrGetNewReservationFailedRetrieved();
    return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errMsg).build();
  }

  private Response invokeCreateNewReservation(Map<SubClusterId, SubClusterInfo> subClustersActive,
      List<SubClusterId> blackList, HttpServletRequest hsr, int retryCount)
      throws YarnException {
    SubClusterId subClusterId = getRandomActiveSubCluster(subClustersActive, blackList);
    LOG.info("createNewReservation try #{} on SubCluster {}.", retryCount, subClusterId);
    SubClusterInfo subClusterInfo = subClustersActive.get(subClusterId);
    DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
        subClusterId, subClusterInfo.getRMWebServiceAddress());
    try {
      Response response = interceptor.createNewReservation(hsr);
      if (response != null && response.getStatus() == HttpServletResponse.SC_OK) {
        return response;
      }
    } catch (Exception e) {
      blackList.add(subClusterId);
      RouterServerUtil.logAndThrowException(e.getMessage(), e);
    }
    // We need to throw the exception directly.
    String msg = String.format("Unable to create a new ReservationId in SubCluster %s.",
        subClusterId.getId());
    throw new YarnException(msg);
  }

  @Override
  public Response submitReservation(ReservationSubmissionRequestInfo resContext,
      HttpServletRequest hsr) throws AuthorizationException, IOException, InterruptedException {
    long startTime = clock.getTime();
    if (resContext == null || resContext.getReservationId() == null
        || resContext.getReservationDefinition() == null || resContext.getQueue() == null) {
      routerMetrics.incrSubmitReservationFailedRetrieved();
      String errMsg = "Missing submitReservation resContext or reservationId " +
          "or reservation definition or queue.";
      return Response.status(Status.BAD_REQUEST).entity(errMsg).build();
    }

    // Check that the resId format is accurate
    String resId = resContext.getReservationId();
    try {
      RouterServerUtil.validateReservationId(resId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrSubmitReservationFailedRetrieved();
      throw e;
    }

    List<SubClusterId> blackList = new ArrayList<>();
    try {
      int activeSubClustersCount = federationFacade.getActiveSubClustersCount();
      int actualRetryNums = Math.min(activeSubClustersCount, numSubmitRetries);
      Response response = ((FederationActionRetry<Response>) (retryCount) ->
          invokeSubmitReservation(resContext, blackList, hsr, retryCount)).
          runWithRetries(actualRetryNums, submitIntervalTime);
      if (response != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededSubmitReservationRetrieved(stopTime - startTime);
        return response;
      }
    } catch (Exception e) {
      routerMetrics.incrSubmitReservationFailedRetrieved();
      return Response.status(Status.SERVICE_UNAVAILABLE).entity(e.getLocalizedMessage()).build();
    }

    routerMetrics.incrSubmitReservationFailedRetrieved();
    String msg = String.format("Reservation %s failed to be submitted.", resId);
    return Response.status(Status.SERVICE_UNAVAILABLE).entity(msg).build();
  }

  private Response invokeSubmitReservation(ReservationSubmissionRequestInfo requestContext,
      List<SubClusterId> blackList, HttpServletRequest hsr, int retryCount)
      throws YarnException, IOException, InterruptedException {
    String resId = requestContext.getReservationId();
    ReservationId reservationId = ReservationId.parseReservationId(resId);
    ReservationDefinitionInfo definitionInfo = requestContext.getReservationDefinition();
    ReservationDefinition definition =
         RouterServerUtil.convertReservationDefinition(definitionInfo);

    // First, Get SubClusterId according to specific strategy.
    ReservationSubmissionRequest request = ReservationSubmissionRequest.newInstance(
        definition, requestContext.getQueue(), reservationId);
    SubClusterId subClusterId = null;

    try {
      // Get subClusterId from policy.
      subClusterId = policyFacade.getReservationHomeSubCluster(request);

      // Print the log of submitting the submitApplication.
      LOG.info("submitReservation ReservationId {} try #{} on SubCluster {}.", reservationId,
          retryCount, subClusterId);

      // Step2. We Store the mapping relationship
      // between Application and HomeSubCluster in stateStore.
      federationFacade.addOrUpdateReservationHomeSubCluster(reservationId,
          subClusterId, retryCount);

      // Step3. We get subClusterInfo based on subClusterId.
      SubClusterInfo subClusterInfo = federationFacade.getSubCluster(subClusterId);

      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      HttpServletRequest hsrCopy = clone(hsr);
      Response response = interceptor.submitReservation(requestContext, hsrCopy);
      if (response != null && response.getStatus() == HttpServletResponse.SC_ACCEPTED) {
        LOG.info("Reservation {} submitted on subCluster {}.", reservationId, subClusterId);
        return response;
      }
      String msg = String.format("application %s failed to be submitted.", resId);
      throw new YarnException(msg);
    } catch (Exception e) {
      LOG.warn("Unable to submit the reservation {} to SubCluster {}.", resId,
          subClusterId, e);
      if (subClusterId != null) {
        blackList.add(subClusterId);
      }
      throw e;
    }
  }

  @Override
  public Response updateReservation(ReservationUpdateRequestInfo resContext,
      HttpServletRequest hsr) throws AuthorizationException, IOException, InterruptedException {

    // parameter verification
    if (resContext == null || resContext.getReservationId() == null
        || resContext.getReservationDefinition() == null) {
      routerMetrics.incrUpdateReservationFailedRetrieved();
      String errMsg = "Missing updateReservation resContext or reservationId " +
          "or reservation definition.";
      return Response.status(Status.BAD_REQUEST).entity(errMsg).build();
    }

    // get reservationId
    String reservationId = resContext.getReservationId();

    // Check that the reservationId format is accurate
    try {
      RouterServerUtil.validateReservationId(reservationId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrUpdateReservationFailedRetrieved();
      throw e;
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByReservationId(reservationId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      HttpServletRequest hsrCopy = clone(hsr);
      Response response = interceptor.updateReservation(resContext, hsrCopy);
      if (response != null) {
        return response;
      }
    } catch (Exception e) {
      routerMetrics.incrUpdateReservationFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("updateReservation Failed.", e);
    }

    // throw an exception
    routerMetrics.incrUpdateReservationFailedRetrieved();
    throw new YarnRuntimeException("updateReservation Failed, reservationId = " + reservationId);
  }

  @Override
  public Response deleteReservation(ReservationDeleteRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {

    // parameter verification
    if (resContext == null || resContext.getReservationId() == null) {
      routerMetrics.incrDeleteReservationFailedRetrieved();
      String errMsg = "Missing deleteReservation request or reservationId.";
      return Response.status(Status.BAD_REQUEST).entity(errMsg).build();
    }

    // get ReservationId
    String reservationId = resContext.getReservationId();

    // Check that the reservationId format is accurate
    try {
      RouterServerUtil.validateReservationId(reservationId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrDeleteReservationFailedRetrieved();
      throw e;
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByReservationId(reservationId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      HttpServletRequest hsrCopy = clone(hsr);
      Response response = interceptor.deleteReservation(resContext, hsrCopy);
      if (response != null) {
        return response;
      }
    } catch (Exception e) {
      routerMetrics.incrDeleteReservationFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("deleteReservation Failed.", e);
    }

    // throw an exception
    routerMetrics.incrDeleteReservationFailedRetrieved();
    throw new YarnRuntimeException("deleteReservation Failed, reservationId = " + reservationId);
  }

  @Override
  public Response listReservation(String queue, String reservationId,
      long startTime, long endTime, boolean includeResourceAllocations,
      HttpServletRequest hsr) throws Exception {

    if (queue == null || queue.isEmpty()) {
      routerMetrics.incrListReservationFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the queue is empty or null.");
    }

    if (reservationId == null || reservationId.isEmpty()) {
      routerMetrics.incrListReservationFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the reservationId is empty or null.");
    }

    // Check that the reservationId format is accurate
    try {
      RouterServerUtil.validateReservationId(reservationId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrListReservationFailedRetrieved();
      throw e;
    }

    try {
      long startTime1 = clock.getTime();
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByReservationId(reservationId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      HttpServletRequest hsrCopy = clone(hsr);
      Response response = interceptor.listReservation(queue, reservationId, startTime, endTime,
          includeResourceAllocations, hsrCopy);
      if (response != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededListReservationRetrieved(stopTime - startTime1);
        return response;
      }
    } catch (YarnException e) {
      routerMetrics.incrListReservationFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("listReservation error.", e);
    }

    routerMetrics.incrListReservationFailedRetrieved();
    throw new YarnException("listReservation Failed.");
  }

  @Override
  public AppTimeoutInfo getAppTimeout(HttpServletRequest hsr, String appId,
      String type) throws AuthorizationException {

    if (type == null || type.isEmpty()) {
      routerMetrics.incrGetAppTimeoutFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the type is empty or null.");
    }

    try {
      long startTime = clock.getTime();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      AppTimeoutInfo appTimeoutInfo = interceptor.getAppTimeout(hsr, appId, type);
      if (appTimeoutInfo != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetAppTimeoutRetrieved((stopTime - startTime));
        return appTimeoutInfo;
      }
    } catch (IllegalArgumentException e) {
      routerMetrics.incrGetAppTimeoutFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the getAppTimeout appId: %s.", appId);
    } catch (YarnException e) {
      routerMetrics.incrGetAppTimeoutFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("getAppTimeout error.", e);
    }
    routerMetrics.incrGetAppTimeoutFailedRetrieved();
    throw new RuntimeException("getAppTimeout Failed.");
  }

  @Override
  public AppTimeoutsInfo getAppTimeouts(HttpServletRequest hsr, String appId)
      throws AuthorizationException {

    try {
      long startTime = clock.getTime();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      AppTimeoutsInfo appTimeoutsInfo = interceptor.getAppTimeouts(hsr, appId);
      if (appTimeoutsInfo != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetAppTimeoutsRetrieved((stopTime - startTime));
        return appTimeoutsInfo;
      }
    } catch (IllegalArgumentException e) {
      routerMetrics.incrGetAppTimeoutsFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the getAppTimeouts appId: %s.", appId);
    } catch (YarnException e) {
      routerMetrics.incrGetAppTimeoutsFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("getAppTimeouts error.", e);
    }

    routerMetrics.incrGetAppTimeoutsFailedRetrieved();
    throw new RuntimeException("getAppTimeouts Failed.");
  }

  @Override
  public Response updateApplicationTimeout(AppTimeoutInfo appTimeout,
      HttpServletRequest hsr, String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {

    if (appTimeout == null) {
      routerMetrics.incrUpdateApplicationTimeoutsRetrieved();
      throw new IllegalArgumentException("Parameter error, the appTimeout is null.");
    }

    try {
      long startTime = Time.now();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      Response response = interceptor.updateApplicationTimeout(appTimeout, hsr, appId);
      if (response != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededUpdateAppTimeoutsRetrieved((stopTime - startTime));
        return response;
      }
    } catch (IllegalArgumentException e) {
      routerMetrics.incrUpdateApplicationTimeoutsRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the updateApplicationTimeout appId: %s.", appId);
    } catch (YarnException e) {
      routerMetrics.incrUpdateApplicationTimeoutsRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("updateApplicationTimeout error.", e);
    }

    routerMetrics.incrUpdateApplicationTimeoutsRetrieved();
    throw new RuntimeException("updateApplicationTimeout Failed.");
  }

  @Override
  public AppAttemptsInfo getAppAttempts(HttpServletRequest hsr, String appId) {

    try {
      long startTime = Time.now();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      AppAttemptsInfo appAttemptsInfo = interceptor.getAppAttempts(hsr, appId);
      if (appAttemptsInfo != null) {
        long stopTime = Time.now();
        routerMetrics.succeededAppAttemptsRetrieved(stopTime - startTime);
        return appAttemptsInfo;
      }
    } catch (IllegalArgumentException e) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the AppAttempt appId: %s.", appId);
    } catch (YarnException e) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("getAppAttempts error.", e);
    }

    routerMetrics.incrAppAttemptsFailedRetrieved();
    throw new RuntimeException("getAppAttempts Failed.");
  }

  @Override
  public RMQueueAclInfo checkUserAccessToQueue(String queue, String username,
      String queueAclType, HttpServletRequest hsr) throws AuthorizationException {

    // Parameter Verification
    if (queue == null || queue.isEmpty()) {
      routerMetrics.incrCheckUserAccessToQueueFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the queue is empty or null.");
    }

    if (username == null || username.isEmpty()) {
      routerMetrics.incrCheckUserAccessToQueueFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the username is empty or null.");
    }

    if (queueAclType == null || queueAclType.isEmpty()) {
      routerMetrics.incrCheckUserAccessToQueueFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the queueAclType is empty or null.");
    }

    // Traverse SubCluster and call checkUserAccessToQueue Api
    try {
      long startTime = Time.now();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{String.class, String.class, String.class,
          HttpServletRequest.class};
      Object[] args = new Object[]{queue, username, queueAclType, hsrCopy};
      ClientMethod remoteMethod = new ClientMethod("checkUserAccessToQueue", argsClasses, args);
      Map<SubClusterInfo, RMQueueAclInfo> rmQueueAclInfoMap =
          invokeConcurrent(subClustersActive, remoteMethod, RMQueueAclInfo.class);
      FederationRMQueueAclInfo aclInfo = new FederationRMQueueAclInfo();
      rmQueueAclInfoMap.forEach((subClusterInfo, rMQueueAclInfo) -> {
        SubClusterId subClusterId = subClusterInfo.getSubClusterId();
        rMQueueAclInfo.setSubClusterId(subClusterId.getId());
        aclInfo.getList().add(rMQueueAclInfo);
      });
      long stopTime = Time.now();
      routerMetrics.succeededCheckUserAccessToQueueRetrieved(stopTime - startTime);
      return aclInfo;
    } catch (NotFoundException e) {
      routerMetrics.incrCheckUserAccessToQueueFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("Get all active sub cluster(s) error.", e);
    } catch (YarnException | IOException e) {
      routerMetrics.incrCheckUserAccessToQueueFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("checkUserAccessToQueue error.", e);
    }

    routerMetrics.incrCheckUserAccessToQueueFailedRetrieved();
    throw new RuntimeException("checkUserAccessToQueue error.");
  }

  @Override
  public AppAttemptInfo getAppAttempt(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {

    // Check that the appId/appAttemptId format is accurate
    try {
      RouterServerUtil.validateApplicationAttemptId(appAttemptId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrAppAttemptReportFailedRetrieved();
      throw e;
    }

    // Call the getAppAttempt method
    try {
      long startTime = Time.now();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      AppAttemptInfo appAttemptInfo = interceptor.getAppAttempt(req, res, appId, appAttemptId);
      if (appAttemptInfo != null) {
        long stopTime = Time.now();
        routerMetrics.succeededAppAttemptReportRetrieved(stopTime - startTime);
        return appAttemptInfo;
      }
    } catch (IllegalArgumentException e) {
      routerMetrics.incrAppAttemptReportFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to getAppAttempt by appId: %s, appAttemptId: %s.", appId, appAttemptId);
    } catch (YarnException e) {
      routerMetrics.incrAppAttemptReportFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "getAppAttempt error, appId: %s, appAttemptId: %s.", appId, appAttemptId);
    }

    routerMetrics.incrAppAttemptReportFailedRetrieved();
    throw RouterServerUtil.logAndReturnRunTimeException(
        "getAppAttempt failed, appId: %s, appAttemptId: %s.", appId, appAttemptId);
  }

  @Override
  public ContainersInfo getContainers(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {

    // Check that the appId/appAttemptId format is accurate
    try {
      RouterServerUtil.validateApplicationId(appId);
      RouterServerUtil.validateApplicationAttemptId(appAttemptId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrGetContainersFailedRetrieved();
      throw e;
    }

    try {
      long startTime = clock.getTime();
      ContainersInfo containersInfo = new ContainersInfo();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      Class[] argsClasses = new Class[]{
          HttpServletRequest.class, HttpServletResponse.class, String.class, String.class};
      Object[] args = new Object[]{req, res, appId, appAttemptId};
      ClientMethod remoteMethod = new ClientMethod("getContainers", argsClasses, args);
      Map<SubClusterInfo, ContainersInfo> containersInfoMap =
          invokeConcurrent(subClustersActive, remoteMethod, ContainersInfo.class);
      if (containersInfoMap != null && !containersInfoMap.isEmpty()) {
        containersInfoMap.values().forEach(containers ->
            containersInfo.addAll(containers.getContainers()));
      }
      if (containersInfo != null) {
        long stopTime = clock.getTime();
        routerMetrics.succeededGetContainersRetrieved(stopTime - startTime);
        return containersInfo;
      }
    } catch (NotFoundException e) {
      routerMetrics.incrGetContainersFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e, "getContainers error, appId = %s, " +
          " appAttemptId = %s, Probably getActiveSubclusters error.", appId, appAttemptId);
    } catch (IOException | YarnException e) {
      routerMetrics.incrGetContainersFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e, "getContainers error, appId = %s, " +
          " appAttemptId = %s.", appId, appAttemptId);
    }

    routerMetrics.incrGetContainersFailedRetrieved();
    throw RouterServerUtil.logAndReturnRunTimeException(
        "getContainers failed, appId: %s, appAttemptId: %s.", appId, appAttemptId);
  }

  @Override
  public ContainerInfo getContainer(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId,
      String containerId) {

    // FederationInterceptorREST#getContainer is logically
    // the same as FederationClientInterceptor#getContainerReport,
    // so use the same Metric.

    // Check that the appId/appAttemptId/containerId format is accurate
    try {
      RouterServerUtil.validateApplicationAttemptId(appAttemptId);
      RouterServerUtil.validateContainerId(containerId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrGetContainerReportFailedRetrieved();
      throw e;
    }

    try {
      long startTime = Time.now();
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorByAppId(appId);
      ContainerInfo containerInfo =
          interceptor.getContainer(req, res, appId, appAttemptId, containerId);
      if (containerInfo != null) {
        long stopTime = Time.now();
        routerMetrics.succeededGetContainerReportRetrieved(stopTime - startTime);
        return containerInfo;
      }
    } catch (IllegalArgumentException e) {
      String msg = String.format(
          "Unable to get the AppAttempt appId: %s, appAttemptId: %s, containerId: %s.", appId,
          appAttemptId, containerId);
      routerMetrics.incrGetContainerReportFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(msg, e);
    } catch (YarnException e) {
      routerMetrics.incrGetContainerReportFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("getContainer Failed.", e);
    }

    routerMetrics.incrGetContainerReportFailedRetrieved();
    throw new RuntimeException("getContainer Failed.");
  }

  /**
   * This method updates the Scheduler configuration, and it is reachable by
   * using {@link RMWSConsts#SCHEDULER_CONF}.
   *
   * @param mutationInfo th information for making scheduler configuration
   *        changes (supports adding, removing, or updating a queue, as well
   *        as global scheduler conf changes)
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws AuthorizationException if the user is not authorized to invoke this
   *         method
   * @throws InterruptedException if interrupted
   */
  @Override
  public Response updateSchedulerConfiguration(SchedConfUpdateInfo mutationInfo,
      HttpServletRequest hsr) throws AuthorizationException, InterruptedException {

    // Make Sure mutationInfo is not null.
    if (mutationInfo == null) {
      routerMetrics.incrUpdateSchedulerConfigurationFailedRetrieved();
      throw new IllegalArgumentException(
          "Parameter error, the schedConfUpdateInfo is empty or null.");
    }

    // In federated mode, we may have a mix of multiple schedulers.
    // In order to ensure accurate update scheduler configuration,
    // we need users to explicitly set subClusterId.
    String pSubClusterId = mutationInfo.getSubClusterId();
    if (StringUtils.isBlank(pSubClusterId)) {
      routerMetrics.incrUpdateSchedulerConfigurationFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, " +
          "the subClusterId is empty or null.");
    }

    // Get the subClusterInfo , then update the scheduler configuration.
    try {
      long startTime = clock.getTime();
      SubClusterInfo subClusterInfo = getActiveSubCluster(pSubClusterId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      Response response = interceptor.updateSchedulerConfiguration(mutationInfo, hsr);
      if (response != null) {
        long endTime = clock.getTime();
        routerMetrics.succeededUpdateSchedulerConfigurationRetrieved(endTime - startTime);
        return Response.status(response.getStatus()).entity(response.getEntity()).build();
      }
    } catch (NotFoundException e) {
      routerMetrics.incrUpdateSchedulerConfigurationFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Get subCluster error. subClusterId = %s", pSubClusterId);
    } catch (Exception e) {
      routerMetrics.incrUpdateSchedulerConfigurationFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException(e,
          "UpdateSchedulerConfiguration error. subClusterId = %s", pSubClusterId);
    }

    routerMetrics.incrUpdateSchedulerConfigurationFailedRetrieved();
    throw new RuntimeException("UpdateSchedulerConfiguration error. subClusterId = "
        + pSubClusterId);
  }

  /**
   * This method retrieves all the Scheduler configuration, and it is reachable
   * by using {@link RMWSConsts#SCHEDULER_CONF}.
   *
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws AuthorizationException if the user is not authorized to invoke this
   *      method.
   */
  @Override
  public Response getSchedulerConfiguration(HttpServletRequest hsr)
      throws AuthorizationException {
    try {
      long startTime = clock.getTime();
      FederationConfInfo federationConfInfo = new FederationConfInfo();
      Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class};
      Object[] args = new Object[]{hsrCopy};
      ClientMethod remoteMethod = new ClientMethod("getSchedulerConfiguration", argsClasses, args);
      Map<SubClusterInfo, Response> responseMap =
          invokeConcurrent(subClustersActive, remoteMethod, Response.class);
      responseMap.forEach((subClusterInfo, response) -> {
        SubClusterId subClusterId = subClusterInfo.getSubClusterId();
        if (response == null) {
          String errorMsg = subClusterId + " Can't getSchedulerConfiguration.";
          federationConfInfo.getErrorMsgs().add(errorMsg);
        } else if (response.getStatus() == Status.BAD_REQUEST.getStatusCode()) {
          String errorMsg = String.valueOf(response.getEntity());
          federationConfInfo.getErrorMsgs().add(errorMsg);
        } else if (response.getStatus() == Status.OK.getStatusCode()) {
          ConfInfo fedConfInfo = (ConfInfo) response.getEntity();
          fedConfInfo.setSubClusterId(subClusterId.getId());
          federationConfInfo.getList().add(fedConfInfo);
        }
      });
      long endTime = clock.getTime();
      routerMetrics.succeededGetSchedulerConfigurationRetrieved(endTime - startTime);
      return Response.status(Status.OK).entity(federationConfInfo).build();
    } catch (NotFoundException e) {
      RouterServerUtil.logAndThrowRunTimeException("get all active sub cluster(s) error.", e);
      routerMetrics.incrGetSchedulerConfigurationFailedRetrieved();
    } catch (Exception e) {
      routerMetrics.incrGetSchedulerConfigurationFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("getSchedulerConfiguration error.", e);
      return Response.status(Status.BAD_REQUEST).entity("getSchedulerConfiguration error.").build();
    }

    routerMetrics.incrGetSchedulerConfigurationFailedRetrieved();
    throw new RuntimeException("getSchedulerConfiguration error.");
  }

  @Override
  public void setNextInterceptor(RESTRequestInterceptor next) {
    throw new YarnRuntimeException("setNextInterceptor is being called on "
        + "FederationInterceptorREST, which should be the last one "
        + "in the chain. Check if the interceptor pipeline configuration "
        + "is correct");
  }

  @Override
  public Response signalToContainer(String containerId, String command,
      HttpServletRequest req) {

    // Check if containerId is empty or null
    try {
      RouterServerUtil.validateContainerId(containerId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrSignalToContainerFailedRetrieved();
      throw e;
    }

    // Check if command is empty or null
    if (command == null || command.isEmpty()) {
      routerMetrics.incrSignalToContainerFailedRetrieved();
      throw new IllegalArgumentException("Parameter error, the command is empty or null.");
    }

    try {
      long startTime = Time.now();

      ContainerId containerIdObj = ContainerId.fromString(containerId);
      ApplicationId applicationId = containerIdObj.getApplicationAttemptId().getApplicationId();
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(applicationId.toString());
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());

      Response response = interceptor.signalToContainer(containerId, command, req);
      if (response != null) {
        long stopTime = Time.now();
        routerMetrics.succeededSignalToContainerRetrieved(stopTime - startTime);
        return response;
      }
    } catch (YarnException e) {
      routerMetrics.incrSignalToContainerFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("signalToContainer Failed.", e);
    } catch (AuthorizationException e) {
      routerMetrics.incrSignalToContainerFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("signalToContainer Author Failed.", e);
    }

    routerMetrics.incrSignalToContainerFailedRetrieved();
    throw new RuntimeException("signalToContainer Failed.");
  }

  @Override
  public void shutdown() {
    if (threadpool != null) {
      threadpool.shutdown();
    }
  }

  private <R> Map<SubClusterInfo, R> invokeConcurrent(Collection<SubClusterInfo> clusterIds,
      ClientMethod request, Class<R> clazz) throws YarnException {

    Map<SubClusterInfo, R> results = new HashMap<>();

    // If there is a sub-cluster access error,
    // we should choose whether to throw exception information according to user configuration.
    // Send the requests in parallel.
    CompletionService<SubClusterResult<R>> compSvc = new ExecutorCompletionService<>(threadpool);

    // This part of the code should be able to expose the accessed Exception information.
    // We use Pair to store related information. The left value of the Pair is the response,
    // and the right value is the exception.
    // If the request is normal, the response is not empty and the exception is empty;
    // if the request is abnormal, the response is empty and the exception is not empty.
    for (final SubClusterInfo info : clusterIds) {
      compSvc.submit(() -> {
        DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
            info.getSubClusterId(), info.getRMWebServiceAddress());
        try {
          Method method = DefaultRequestInterceptorREST.class.
              getMethod(request.getMethodName(), request.getTypes());
          Object retObj = method.invoke(interceptor, request.getParams());
          R ret = clazz.cast(retObj);
          return new SubClusterResult<>(info, ret, null);
        } catch (Exception e) {
          LOG.error("SubCluster {} failed to call {} method.",
              info.getSubClusterId(), request.getMethodName(), e);
          return new SubClusterResult<>(info, null, e);
        }
      });
    }

    for (int i = 0; i < clusterIds.size(); i++) {
      SubClusterInfo subClusterInfo = null;
      try {
        Future<SubClusterResult<R>> future = compSvc.take();
        SubClusterResult<R> result = future.get();
        subClusterInfo = result.getSubClusterInfo();

        R response = result.getResponse();
        if (response != null) {
          results.put(subClusterInfo, response);
        }

        Exception exception = result.getException();

        // If allowPartialResult=false, it means that if an exception occurs in a subCluster,
        // an exception will be thrown directly.
        if (!allowPartialResult && exception != null) {
          throw exception;
        }
      } catch (Throwable e) {
        String subClusterId = subClusterInfo != null ?
            subClusterInfo.getSubClusterId().getId() : "UNKNOWN";
        LOG.error("SubCluster {} failed to {} report.", subClusterId, request.getMethodName(), e);
        throw new YarnRuntimeException(e.getCause().getMessage(), e);
      }
    }

    return results;
  }

  /**
   * get the HomeSubCluster according to ApplicationId.
   *
   * @param appId applicationId
   * @return HomeSubCluster
   * @throws YarnException on failure
   */
  private SubClusterInfo getHomeSubClusterInfoByAppId(String appId)
      throws YarnException {

    if (StringUtils.isBlank(appId)) {
      throw new IllegalArgumentException("applicationId can't null or empty.");
    }

    try {
      ApplicationId applicationId = ApplicationId.fromString(appId);
      SubClusterId subClusterId = federationFacade.getApplicationHomeSubCluster(applicationId);
      if (subClusterId == null) {
        RouterServerUtil.logAndThrowException(null,
            "Can't get HomeSubCluster by applicationId %s", applicationId);
      }
      return federationFacade.getSubCluster(subClusterId);
    } catch (IllegalArgumentException e){
      throw new IllegalArgumentException(e);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowException(e,
          "Get HomeSubClusterInfo by applicationId %s failed.", appId);
    }
    throw new YarnException("Unable to get subCluster by applicationId = " + appId);
  }

  /**
   * get the HomeSubCluster according to ReservationId.
   *
   * @param resId reservationId
   * @return HomeSubCluster
   * @throws YarnException on failure
   */
  private SubClusterInfo getHomeSubClusterInfoByReservationId(String resId)
      throws YarnException {
    try {
      ReservationId reservationId = ReservationId.parseReservationId(resId);
      SubClusterId subClusterId = federationFacade.getReservationHomeSubCluster(reservationId);
      if (subClusterId == null) {
        RouterServerUtil.logAndThrowException(null,
            "Can't get HomeSubCluster by reservationId %s", resId);
      }
      return federationFacade.getSubCluster(subClusterId);
    } catch (YarnException | IOException e) {
      RouterServerUtil.logAndThrowException(e,
          "Get HomeSubClusterInfo by reservationId %s failed.", resId);
    }
    throw new YarnException("Unable to get subCluster by reservationId = " + resId);
  }

  @VisibleForTesting
  public LRUCacheHashMap<RouterAppInfoCacheKey, AppsInfo> getAppInfosCaches() {
    return appInfosCaches;
  }

  @VisibleForTesting
  public Map<SubClusterId, DefaultRequestInterceptorREST> getInterceptors() {
    return interceptors;
  }

  public void setAllowPartialResult(boolean allowPartialResult) {
    this.allowPartialResult = allowPartialResult;
  }

  @VisibleForTesting
  public Map<SubClusterInfo, NodesInfo> invokeConcurrentGetNodeLabel()
      throws IOException, YarnException {
    Collection<SubClusterInfo> subClustersActive = federationFacade.getActiveSubClusters();
    Class[] argsClasses = new Class[]{String.class};
    Object[] args = new Object[]{null};
    ClientMethod remoteMethod = new ClientMethod("getNodes", argsClasses, args);
    return invokeConcurrent(subClustersActive, remoteMethod, NodesInfo.class);
  }
}