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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.RouterPolicyFacade;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.NodeIDsInfo;
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
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.clientrm.ClientMethod;
import org.apache.hadoop.yarn.server.router.webapp.cache.RouterAppInfoCacheKey;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Extends the {@code AbstractRESTRequestInterceptor} class and provides an
 * implementation for federation of YARN RM and scaling an application across
 * multiple YARN SubClusters. All the federation specific implementation is
 * encapsulated in this class. This is always the last interceptor in the chain.
 */
public class FederationInterceptorREST extends AbstractRESTRequestInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationInterceptorREST.class);

  private int numSubmitRetries;
  private FederationStateStoreFacade federationFacade;
  private Random rand;
  private RouterPolicyFacade policyFacade;
  private RouterMetrics routerMetrics;
  private final Clock clock = new MonotonicClock();
  private boolean returnPartialReport;
  private boolean appInfosCacheEnabled;
  private int appInfosCacheCount;

  private Map<SubClusterId, DefaultRequestInterceptorREST> interceptors;
  private LRUCacheHashMap<RouterAppInfoCacheKey, AppsInfo> appInfosCaches;

  /**
   * Thread pool used for asynchronous operations.
   */
  private ExecutorService threadpool;

  @Override
  public void init(String user) {
    federationFacade = FederationStateStoreFacade.getInstance();
    rand = new Random();

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
    threadpool = HadoopExecutors.newCachedThreadPool(
        new ThreadFactoryBuilder()
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
  }

  private SubClusterId getRandomActiveSubCluster(
      Map<SubClusterId, SubClusterInfo> activeSubclusters,
      List<SubClusterId> blackListSubClusters) throws YarnException {

    if (activeSubclusters == null || activeSubclusters.size() < 1) {
      RouterServerUtil.logAndThrowException(
          FederationPolicyUtils.NO_ACTIVE_SUBCLUSTER_AVAILABLE, null);
    }
    Collection<SubClusterId> keySet = activeSubclusters.keySet();
    FederationPolicyUtils.validateSubClusterAvailability(keySet, blackListSubClusters);
    if (blackListSubClusters != null) {
      keySet.removeAll(blackListSubClusters);
    }

    List<SubClusterId> list = keySet.stream().collect(Collectors.toList());
    return list.get(rand.nextInt(list.size()));
  }

  @VisibleForTesting
  protected DefaultRequestInterceptorREST getInterceptorForSubCluster(
      SubClusterId subClusterId) {
    if (interceptors.containsKey(subClusterId)) {
      return interceptors.get(subClusterId);
    } else {
      LOG.error(
          "The interceptor for SubCluster {} does not exist in the cache.",
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
    DefaultRequestInterceptorREST interceptorInstance = null;
    try {
      Class<?> interceptorClass = conf.getClassByName(interceptorClassName);
      if (DefaultRequestInterceptorREST.class
          .isAssignableFrom(interceptorClass)) {
        interceptorInstance = (DefaultRequestInterceptorREST) ReflectionUtils
            .newInstance(interceptorClass, conf);

      } else {
        throw new YarnRuntimeException(
            "Class: " + interceptorClassName + " not instance of "
                + DefaultRequestInterceptorREST.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate ApplicationMasterRequestInterceptor: "
              + interceptorClassName,
          e);
    }

    String webAppAddresswithScheme =
        WebAppUtils.getHttpSchemePrefix(this.getConf()) + webAppAddress;
    interceptorInstance.setWebAppAddress(webAppAddresswithScheme);
    interceptorInstance.setSubClusterId(subClusterId);
    interceptors.put(subClusterId, interceptorInstance);
    return interceptorInstance;
  }

  @VisibleForTesting
  protected DefaultRequestInterceptorREST getOrCreateInterceptorForSubCluster(
      SubClusterId subClusterId, String webAppAddress) {
    DefaultRequestInterceptorREST interceptor =
        getInterceptorForSubCluster(subClusterId);
    String webAppAddresswithScheme = WebAppUtils.getHttpSchemePrefix(
            this.getConf()) + webAppAddress;
    if (interceptor == null || !webAppAddresswithScheme.equals(interceptor.
        getWebAppAddress())){
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

    Map<SubClusterId, SubClusterInfo> subClustersActive;
    try {
      subClustersActive = federationFacade.getSubClusters(true);
    } catch (YarnException e) {
      routerMetrics.incrAppsFailedCreated();
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(e.getLocalizedMessage()).build();
    }

    List<SubClusterId> blacklist = new ArrayList<>();

    for (int i = 0; i < numSubmitRetries; ++i) {

      SubClusterId subClusterId;
      try {
        subClusterId = getRandomActiveSubCluster(subClustersActive, blacklist);
      } catch (YarnException e) {
        routerMetrics.incrAppsFailedCreated();
        return Response.status(Status.SERVICE_UNAVAILABLE)
            .entity(e.getLocalizedMessage()).build();
      }

      LOG.debug("getNewApplication try #{} on SubCluster {}.", i, subClusterId);

      DefaultRequestInterceptorREST interceptor =
          getOrCreateInterceptorForSubCluster(subClusterId,
              subClustersActive.get(subClusterId).getRMWebServiceAddress());
      Response response = null;
      try {
        response = interceptor.createNewApplication(hsr);
      } catch (Exception e) {
        LOG.warn("Unable to create a new ApplicationId in SubCluster {}.",
            subClusterId.getId(), e);
      }

      if (response != null &&
          response.getStatus() == HttpServletResponse.SC_OK) {

        long stopTime = clock.getTime();
        routerMetrics.succeededAppsCreated(stopTime - startTime);

        return response;
      } else {
        // Empty response from the ResourceManager.
        // Blacklist this subcluster for this request.
        blacklist.add(subClusterId);
      }
    }

    String errMsg = "Fail to create a new application.";
    LOG.error(errMsg);
    routerMetrics.incrAppsFailedCreated();
    return Response
        .status(Status.INTERNAL_SERVER_ERROR)
        .entity(errMsg)
        .build();
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
  public Response submitApplication(ApplicationSubmissionContextInfo newApp,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {

    long startTime = clock.getTime();

    if (newApp == null || newApp.getApplicationId() == null) {
      routerMetrics.incrAppsFailedSubmitted();
      String errMsg = "Missing ApplicationSubmissionContextInfo or "
          + "applicationSubmissionContext information.";
      return Response
          .status(Status.BAD_REQUEST)
          .entity(errMsg)
          .build();
    }

    ApplicationId applicationId = null;
    try {
      applicationId = ApplicationId.fromString(newApp.getApplicationId());
    } catch (IllegalArgumentException e) {
      routerMetrics.incrAppsFailedSubmitted();
      return Response
          .status(Status.BAD_REQUEST)
          .entity(e.getLocalizedMessage())
          .build();
    }

    List<SubClusterId> blacklist = new ArrayList<>();

    for (int i = 0; i < numSubmitRetries; ++i) {

      ApplicationSubmissionContext context =
          RMWebAppUtil.createAppSubmissionContext(newApp, this.getConf());

      SubClusterId subClusterId = null;
      try {
        subClusterId = policyFacade.getHomeSubcluster(context, blacklist);
      } catch (YarnException e) {
        routerMetrics.incrAppsFailedSubmitted();
        return Response
            .status(Status.SERVICE_UNAVAILABLE)
            .entity(e.getLocalizedMessage())
            .build();
      }
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
          String errMsg = "Unable to insert the ApplicationId " + applicationId
              + " into the FederationStateStore";
          return Response
              .status(Status.SERVICE_UNAVAILABLE)
              .entity(errMsg + " " + e.getLocalizedMessage())
              .build();
        }
      } else {
        try {
          // update the mapping of applicationId and the home subClusterId to
          // the new subClusterId we have selected
          federationFacade.updateApplicationHomeSubCluster(appHomeSubCluster);
        } catch (YarnException e) {
          String errMsg = "Unable to update the ApplicationId " + applicationId
              + " into the FederationStateStore";
          SubClusterId subClusterIdInStateStore;
          try {
            subClusterIdInStateStore =
                federationFacade.getApplicationHomeSubCluster(applicationId);
          } catch (YarnException e1) {
            routerMetrics.incrAppsFailedSubmitted();
            return Response
                .status(Status.SERVICE_UNAVAILABLE)
                .entity(e1.getLocalizedMessage())
                .build();
          }
          if (subClusterId == subClusterIdInStateStore) {
            LOG.info("Application {} already submitted on SubCluster {}.",
                applicationId, subClusterId);
          } else {
            routerMetrics.incrAppsFailedSubmitted();
            return Response
                .status(Status.SERVICE_UNAVAILABLE)
                .entity(errMsg)
                .build();
          }
        }
      }

      SubClusterInfo subClusterInfo;
      try {
        subClusterInfo = federationFacade.getSubCluster(subClusterId);
      } catch (YarnException e) {
        routerMetrics.incrAppsFailedSubmitted();
        return Response
            .status(Status.SERVICE_UNAVAILABLE)
            .entity(e.getLocalizedMessage())
            .build();
      }

      Response response = null;
      try {
        response = getOrCreateInterceptorForSubCluster(subClusterId,
            subClusterInfo.getRMWebServiceAddress()).submitApplication(newApp,
                hsr);
      } catch (Exception e) {
        LOG.warn("Unable to submit the application {} to SubCluster {}",
            applicationId, subClusterId.getId(), e);
      }

      if (response != null &&
          response.getStatus() == HttpServletResponse.SC_ACCEPTED) {
        LOG.info("Application {} with appId {} submitted on {}",
            context.getApplicationName(), applicationId, subClusterId);

        long stopTime = clock.getTime();
        routerMetrics.succeededAppsSubmitted(stopTime - startTime);

        return response;
      } else {
        // Empty response from the ResourceManager.
        // Blacklist this subcluster for this request.
        blacklist.add(subClusterId);
      }
    }

    routerMetrics.incrAppsFailedSubmitted();
    String errMsg = "Application " + newApp.getApplicationName()
        + " with appId " + applicationId + " failed to be submitted.";
    LOG.error(errMsg);
    return Response
        .status(Status.SERVICE_UNAVAILABLE)
        .entity(errMsg)
        .build();
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
  public AppInfo getApp(HttpServletRequest hsr, String appId,
      Set<String> unselectedFields) {

    long startTime = clock.getTime();

    ApplicationId applicationId = null;
    try {
      applicationId = ApplicationId.fromString(appId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrAppsFailedRetrieved();
      return null;
    }

    SubClusterInfo subClusterInfo = null;
    SubClusterId subClusterId = null;
    try {
      subClusterId =
          federationFacade.getApplicationHomeSubCluster(applicationId);
      if (subClusterId == null) {
        routerMetrics.incrAppsFailedRetrieved();
        return null;
      }
      subClusterInfo = federationFacade.getSubCluster(subClusterId);
    } catch (YarnException e) {
      routerMetrics.incrAppsFailedRetrieved();
      return null;
    }

    DefaultRequestInterceptorREST interceptor =
        getOrCreateInterceptorForSubCluster(
            subClusterId, subClusterInfo.getRMWebServiceAddress());
    AppInfo response = interceptor.getApp(hsr, appId, unselectedFields);

    long stopTime = clock.getTime();
    routerMetrics.succeededAppsRetrieved(stopTime - startTime);

    return response;
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
  public Response updateAppState(AppState targetState, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException {

    long startTime = clock.getTime();

    ApplicationId applicationId = null;
    try {
      applicationId = ApplicationId.fromString(appId);
    } catch (IllegalArgumentException e) {
      routerMetrics.incrAppsFailedKilled();
      return Response
          .status(Status.BAD_REQUEST)
          .entity(e.getLocalizedMessage())
          .build();
    }

    SubClusterInfo subClusterInfo = null;
    SubClusterId subClusterId = null;
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

    Map<SubClusterId, SubClusterInfo> subClustersActive = null;
    try {
      subClustersActive = federationFacade.getSubClusters(true);
    } catch (YarnException e) {
      routerMetrics.incrMultipleAppsFailedRetrieved();
      return null;
    }

    // Send the requests in parallel
    CompletionService<AppsInfo> compSvc =
        new ExecutorCompletionService<>(this.threadpool);

    // HttpServletRequest does not work with ExecutorCompletionService.
    // Create a duplicate hsr.
    final HttpServletRequest hsrCopy = clone(hsr);
    for (final SubClusterInfo info : subClustersActive.values()) {
      compSvc.submit(new Callable<AppsInfo>() {
        @Override
        public AppsInfo call() {
          DefaultRequestInterceptorREST interceptor =
              getOrCreateInterceptorForSubCluster(
                  info.getSubClusterId(), info.getRMWebServiceAddress());
          AppsInfo rmApps = interceptor.getApps(hsrCopy, stateQuery,
              statesQuery, finalStatusQuery, userQuery, queueQuery, count,
              startedBegin, startedEnd, finishBegin, finishEnd,
              applicationTypes, applicationTags, name, unselectedFields);

          if (rmApps == null) {
            routerMetrics.incrMultipleAppsFailedRetrieved();
            LOG.error("Subcluster {} failed to return appReport.", info.getSubClusterId());
            return null;
          }
          return rmApps;
        }
      });
    }

    // Collect all the responses in parallel
    for (int i = 0; i < subClustersActive.size(); i++) {
      try {
        Future<AppsInfo> future = compSvc.take();
        AppsInfo appsResponse = future.get();

        long stopTime = clock.getTime();
        routerMetrics.succeededMultipleAppsRetrieved(stopTime - startTime);

        if (appsResponse != null) {
          apps.addAll(appsResponse.getApps());
        }
      } catch (Throwable e) {
        routerMetrics.incrMultipleAppsFailedRetrieved();
        LOG.warn("Failed to get application report", e);
      }
    }

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
    @SuppressWarnings("unchecked")
    final Map<String, String[]> parameterMap =
        (Map<String, String[]>) hsr.getParameterMap();
    final String pathInfo = hsr.getPathInfo();
    final String user = hsr.getRemoteUser();
    final Principal principal = hsr.getUserPrincipal();
    final String mediaType =
        RouterWebServiceUtil.getMediaTypeFromHttpServletRequest(
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
   * Get the active subclusters in the federation.
   * @return Map from subcluster id to its info.
   * @throws NotFoundException If the subclusters cannot be found.
   */
  private Map<SubClusterId, SubClusterInfo> getActiveSubclusters()
      throws NotFoundException {
    try {
      return federationFacade.getSubClusters(true);
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
    final Map<SubClusterId, SubClusterInfo> subClustersActive =
        getActiveSubclusters();
    if (subClustersActive.isEmpty()) {
      throw new NotFoundException(
          FederationPolicyUtils.NO_ACTIVE_SUBCLUSTER_AVAILABLE);
    }
    final Map<SubClusterInfo, NodeInfo> results =
        getNode(subClustersActive.values(), nodeId);

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
   * @param subClusters Subclusters where to search.
   * @param nodeId Identifier of the node we are looking for.
   * @return Map between subcluster and node.
   */
  private Map<SubClusterInfo, NodeInfo> getNode(
      Collection<SubClusterInfo> subClusters, String nodeId) {

    // Send the requests in parallel
    CompletionService<NodeInfo> compSvc =
        new ExecutorCompletionService<NodeInfo>(this.threadpool);
    final Map<SubClusterInfo, Future<NodeInfo>> futures = new HashMap<>();
    for (final SubClusterInfo subcluster : subClusters) {
      final SubClusterId subclusterId = subcluster.getSubClusterId();
      Future<NodeInfo> result = compSvc.submit(() -> {
        try {
          DefaultRequestInterceptorREST interceptor =
              getOrCreateInterceptorForSubCluster(
                  subclusterId, subcluster.getRMWebServiceAddress());
          return interceptor.getNode(nodeId);
        } catch (Exception e) {
          LOG.error("Subcluster {} failed to return nodeInfo.", subclusterId, e);
          return null;
        }
      });
      futures.put(subcluster, result);
    }

    // Collect the results
    final Map<SubClusterInfo, NodeInfo> results = new HashMap<>();
    for (Entry<SubClusterInfo, Future<NodeInfo>> entry : futures.entrySet()) {
      try {
        final Future<NodeInfo> future = entry.getValue();
        final NodeInfo nodeInfo = future.get();
        // Check if the node was found in this SubCluster
        if (nodeInfo != null) {
          SubClusterInfo subcluster = entry.getKey();
          results.put(subcluster, nodeInfo);
        }
      } catch (Throwable e) {
        LOG.warn("Failed to get node report ", e);
      }
    }

    return results;
  }

  /**
   * Get the subcluster a node belongs to.
   * @param nodeId Identifier of the node we are looking for.
   * @return The subcluster containing the node.
   * @throws NotFoundException If the node cannot be found.
   */
  private SubClusterInfo getNodeSubcluster(String nodeId)
      throws NotFoundException {

    final Collection<SubClusterInfo> subClusters =
        getActiveSubclusters().values();
    final Map<SubClusterInfo, NodeInfo> results =
        getNode(subClusters, nodeId);
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
      throw new NotFoundException(
          "Cannot find " + nodeId + " in any subcluster");
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
      Map<SubClusterId, SubClusterInfo> subClustersActive = getActiveSubclusters();
      Class[] argsClasses = new Class[]{String.class};
      Object[] args = new Object[]{states};
      ClientMethod remoteMethod = new ClientMethod("getNodes", argsClasses, args);
      Map<SubClusterInfo, NodesInfo> nodesMap =
          invokeConcurrent(subClustersActive.values(), remoteMethod, NodesInfo.class);
      nodesMap.values().stream().forEach(nodesInfo -> {
        nodes.addAll(nodesInfo.getNodes());
      });
    } catch (NotFoundException e) {
      LOG.error("Get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      LOG.error("getNodes error.", e);
    } catch (IOException e) {
      LOG.error("getNodes error with io error.", e);
    }

    // Delete duplicate from all the node reports got from all the available
    // YARN RMs. Nodes can be moved from one subclusters to another. In this
    // operation they result LOST/RUNNING in the previous SubCluster and
    // NEW/RUNNING in the new one.
    return RouterWebServiceUtil.deleteDuplicateNodesInfo(nodes.getNodes());
  }

  @Override
  public ResourceInfo updateNodeResource(HttpServletRequest hsr,
      String nodeId, ResourceOptionInfo resourceOption) {
    SubClusterInfo subcluster = getNodeSubcluster(nodeId);
    DefaultRequestInterceptorREST interceptor =
        getOrCreateInterceptorForSubCluster(
            subcluster.getSubClusterId(),
            subcluster.getRMWebServiceAddress());
    return interceptor.updateNodeResource(hsr, nodeId, resourceOption);
  }

  @Override
  public ClusterMetricsInfo getClusterMetricsInfo() {
    ClusterMetricsInfo metrics = new ClusterMetricsInfo();

    final Map<SubClusterId, SubClusterInfo> subClustersActive;
    try {
      subClustersActive = getActiveSubclusters();
    } catch (Exception e) {
      LOG.error(e.getLocalizedMessage());
      return metrics;
    }

    // Send the requests in parallel
    CompletionService<ClusterMetricsInfo> compSvc =
        new ExecutorCompletionService<ClusterMetricsInfo>(this.threadpool);

    for (final SubClusterInfo info : subClustersActive.values()) {
      compSvc.submit(new Callable<ClusterMetricsInfo>() {
        @Override
        public ClusterMetricsInfo call() {
          DefaultRequestInterceptorREST interceptor =
              getOrCreateInterceptorForSubCluster(
                  info.getSubClusterId(), info.getRMWebServiceAddress());
          try {
            ClusterMetricsInfo metrics = interceptor.getClusterMetricsInfo();
            return metrics;
          } catch (Exception e) {
            LOG.error("Subcluster {} failed to return Cluster Metrics.",
                info.getSubClusterId());
            return null;
          }
        }
      });
    }

    // Collect all the responses in parallel
    for (int i = 0; i < subClustersActive.size(); i++) {
      try {
        Future<ClusterMetricsInfo> future = compSvc.take();
        ClusterMetricsInfo metricsResponse = future.get();

        if (metricsResponse != null) {
          RouterWebServiceUtil.mergeMetrics(metrics, metricsResponse);
        }
      } catch (Throwable e) {
        LOG.warn("Failed to get nodes report ", e);
      }
    }

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

    ApplicationId applicationId = null;
    try {
      applicationId = ApplicationId.fromString(appId);
    } catch (IllegalArgumentException e) {
      return null;
    }

    SubClusterInfo subClusterInfo = null;
    SubClusterId subClusterId = null;
    try {
      subClusterId =
          federationFacade.getApplicationHomeSubCluster(applicationId);
      if (subClusterId == null) {
        return null;
      }
      subClusterInfo = federationFacade.getSubCluster(subClusterId);
    } catch (YarnException e) {
      return null;
    }

    DefaultRequestInterceptorREST interceptor =
        getOrCreateInterceptorForSubCluster(subClusterId,
            subClusterInfo.getRMWebServiceAddress());
    return interceptor.getAppState(hsr, appId);
  }

  @Override
  public ClusterInfo get() {
    return getClusterInfo();
  }

  @Override
  public ClusterInfo getClusterInfo() {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public ClusterUserInfo getClusterUserInfo(HttpServletRequest hsr) {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public SchedulerTypeInfo getSchedulerInfo() {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public String dumpSchedulerLogs(String time, HttpServletRequest hsr)
      throws IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public ActivitiesInfo getActivities(HttpServletRequest hsr, String nodeId,
      String groupBy) {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public BulkActivitiesInfo getBulkActivities(HttpServletRequest hsr,
      String groupBy, int activitiesCount) throws InterruptedException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public AppActivitiesInfo getAppActivities(HttpServletRequest hsr,
      String appId, String time, Set<String> requestPriorities,
      Set<String> allocationRequestIds, String groupBy, String limit,
      Set<String> actions, boolean summarize) {

    // Only verify the app_id,
    // because the specific subCluster needs to be found according to the app_id,
    // and other verifications are directly handed over to the corresponding subCluster RM
    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());

      final HttpServletRequest hsrCopy = clone(hsr);
      return interceptor.getAppActivities(hsrCopy, appId, time, requestPriorities,
          allocationRequestIds, groupBy, limit, actions, summarize);
    } catch (IllegalArgumentException e) {
      RouterServerUtil.logAndThrowRunTimeException(e, "Unable to get subCluster by appId: %s.",
          appId);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("getAppActivities Failed.", e);
    }

    return null;
  }

  @Override
  public ApplicationStatisticsInfo getAppStatistics(HttpServletRequest hsr,
      Set<String> stateQueries, Set<String> typeQueries) {
    try {
      Map<SubClusterId, SubClusterInfo> subClustersActive = getActiveSubclusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class, Set.class, Set.class};
      Object[] args = new Object[]{hsrCopy, stateQueries, typeQueries};
      ClientMethod remoteMethod = new ClientMethod("getAppStatistics", argsClasses, args);
      Map<SubClusterInfo, ApplicationStatisticsInfo> appStatisticsMap = invokeConcurrent(
          subClustersActive.values(), remoteMethod, ApplicationStatisticsInfo.class);
      return RouterWebServiceUtil.mergeApplicationStatisticsInfo(appStatisticsMap.values());
    } catch (IOException e) {
      RouterServerUtil.logAndThrowRunTimeException(e, "Get all active sub cluster(s) error.");
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException(e, "getAppStatistics error.");
    }
    return null;
  }

  @Override
  public NodeToLabelsInfo getNodeToLabels(HttpServletRequest hsr)
      throws IOException {
    try {
      Map<SubClusterId, SubClusterInfo> subClustersActive = getActiveSubclusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class};
      Object[] args = new Object[]{hsrCopy};
      ClientMethod remoteMethod = new ClientMethod("getNodeToLabels", argsClasses, args);
      Map<SubClusterInfo, NodeToLabelsInfo> nodeToLabelsInfoMap =
          invokeConcurrent(subClustersActive.values(), remoteMethod, NodeToLabelsInfo.class);
      return RouterWebServiceUtil.mergeNodeToLabels(nodeToLabelsInfoMap);
    } catch (NotFoundException e) {
      LOG.error("Get all active sub cluster(s) error.", e);
      throw new IOException("Get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      LOG.error("getNodeToLabels error.", e);
      throw new IOException("getNodeToLabels error.", e);
    }
  }

  @Override
  public LabelsToNodesInfo getLabelsToNodes(Set<String> labels)
      throws IOException {
    try {
      Map<SubClusterId, SubClusterInfo> subClustersActive = getActiveSubclusters();
      Class[] argsClasses = new Class[]{Set.class};
      Object[] args = new Object[]{labels};
      ClientMethod remoteMethod = new ClientMethod("getLabelsToNodes", argsClasses, args);
      Map<SubClusterInfo, LabelsToNodesInfo> labelsToNodesInfoMap =
          invokeConcurrent(subClustersActive.values(), remoteMethod, LabelsToNodesInfo.class);

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
      return new LabelsToNodesInfo(labelToNodesMap);
    } catch (NotFoundException e) {
      RouterServerUtil.logAndThrowIOException("Get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowIOException("getLabelsToNodes error.", e);
    }
    return null;
  }

  @Override
  public Response replaceLabelsOnNodes(NodeToLabelsEntryList newNodeToLabels,
      HttpServletRequest hsr) throws IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Response replaceLabelsOnNode(Set<String> newNodeLabelsName,
      HttpServletRequest hsr, String nodeId) throws Exception {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public NodeLabelsInfo getClusterNodeLabels(HttpServletRequest hsr)
      throws IOException {
    try {
      Map<SubClusterId, SubClusterInfo> subClustersActive = getActiveSubclusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class};
      Object[] args = new Object[]{hsrCopy};
      ClientMethod remoteMethod = new ClientMethod("getClusterNodeLabels", argsClasses, args);
      Map<SubClusterInfo, NodeLabelsInfo> nodeToLabelsInfoMap =
          invokeConcurrent(subClustersActive.values(), remoteMethod, NodeLabelsInfo.class);
      Set<NodeLabel> hashSets = Sets.newHashSet();
      nodeToLabelsInfoMap.values().forEach(item -> hashSets.addAll(item.getNodeLabels()));
      return new NodeLabelsInfo(hashSets);
    } catch (NotFoundException e) {
      RouterServerUtil.logAndThrowIOException("Get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowIOException("getClusterNodeLabels error.", e);
    }
    return null;
  }

  @Override
  public Response addToClusterNodeLabels(NodeLabelsInfo newNodeLabels,
      HttpServletRequest hsr) throws Exception {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Response removeFromClusterNodeLabels(Set<String> oldNodeLabels,
      HttpServletRequest hsr) throws Exception {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public NodeLabelsInfo getLabelsOnNode(HttpServletRequest hsr, String nodeId)
      throws IOException {
    try {
      Map<SubClusterId, SubClusterInfo> subClustersActive = getActiveSubclusters();
      final HttpServletRequest hsrCopy = clone(hsr);
      Class[] argsClasses = new Class[]{HttpServletRequest.class, String.class};
      Object[] args = new Object[]{hsrCopy, nodeId};
      ClientMethod remoteMethod = new ClientMethod("getLabelsOnNode", argsClasses, args);
      Map<SubClusterInfo, NodeLabelsInfo> nodeToLabelsInfoMap =
           invokeConcurrent(subClustersActive.values(), remoteMethod, NodeLabelsInfo.class);
      Set<NodeLabel> hashSets = Sets.newHashSet();
      nodeToLabelsInfoMap.values().forEach(item -> hashSets.addAll(item.getNodeLabels()));
      return new NodeLabelsInfo(hashSets);
    } catch (NotFoundException e) {
      RouterServerUtil.logAndThrowIOException("Get all active sub cluster(s) error.", e);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowIOException("getClusterNodeLabels error.", e);
    }
    return null;
  }

  @Override
  public AppPriority getAppPriority(HttpServletRequest hsr, String appId)
      throws AuthorizationException {

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      return interceptor.getAppPriority(hsr, appId);
    } catch (IllegalArgumentException e) {
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the getAppPriority appId: %s.", appId);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("getAppPriority Failed.", e);
    }

    return null;
  }

  @Override
  public Response updateApplicationPriority(AppPriority targetPriority,
      HttpServletRequest hsr, String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }

    if (targetPriority == null) {
      throw new IllegalArgumentException("Parameter error, the targetPriority is empty or null.");
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      return interceptor.updateApplicationPriority(targetPriority, hsr, appId);
    } catch (IllegalArgumentException e) {
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the updateApplicationPriority appId: %s.", appId);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("updateApplicationPriority Failed.", e);
    }

    return null;
  }

  @Override
  public AppQueue getAppQueue(HttpServletRequest hsr, String appId)
      throws AuthorizationException {

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      return interceptor.getAppQueue(hsr, appId);
    } catch (IllegalArgumentException e) {
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get queue by appId: %s.", appId);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("getAppQueue Failed.", e);
    }

    return null;
  }

  @Override
  public Response updateAppQueue(AppQueue targetQueue, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException {

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }

    if (targetQueue == null) {
      throw new IllegalArgumentException("Parameter error, the targetQueue is null.");
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      return interceptor.updateAppQueue(targetQueue, hsr, appId);
    } catch (IllegalArgumentException e) {
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to update app queue by appId: %s.", appId);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("updateAppQueue Failed.", e);
    }

    return null;
  }

  @Override
  public Response postDelegationToken(DelegationToken tokenData,
      HttpServletRequest hsr) throws AuthorizationException, IOException,
      InterruptedException, Exception {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Response postDelegationTokenExpiration(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Response cancelDelegationToken(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Response createNewReservation(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Response submitReservation(ReservationSubmissionRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Response updateReservation(ReservationUpdateRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Response deleteReservation(ReservationDeleteRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    throw new NotImplementedException("Code is not implemented");
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

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByReservationId(reservationId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      HttpServletRequest hsrCopy = clone(hsr);
      Response response = interceptor.listReservation(queue, reservationId, startTime, endTime,
          includeResourceAllocations, hsrCopy);
      if (response != null) {
        return response;
      }
    } catch (YarnException e) {
      routerMetrics.incrListReservationFailedRetrieved();
      RouterServerUtil.logAndThrowRunTimeException("listReservation Failed.", e);
    }

    routerMetrics.incrListReservationFailedRetrieved();
    throw new YarnException("listReservation Failed.");
  }

  @Override
  public AppTimeoutInfo getAppTimeout(HttpServletRequest hsr, String appId,
      String type) throws AuthorizationException {

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }

    if (type == null || type.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the type is empty or null.");
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      return interceptor.getAppTimeout(hsr, appId, type);
    } catch (IllegalArgumentException e) {
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the getAppTimeout appId: %s.", appId);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("getAppTimeout Failed.", e);
    }
    return null;
  }

  @Override
  public AppTimeoutsInfo getAppTimeouts(HttpServletRequest hsr, String appId)
      throws AuthorizationException {

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      return interceptor.getAppTimeouts(hsr, appId);
    } catch (IllegalArgumentException e) {
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the getAppTimeouts appId: %s.", appId);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("getAppTimeouts Failed.", e);
    }
    return null;
  }

  @Override
  public Response updateApplicationTimeout(AppTimeoutInfo appTimeout,
      HttpServletRequest hsr, String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }

    if (appTimeout == null) {
      throw new IllegalArgumentException("Parameter error, the appTimeout is null.");
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      return interceptor.updateApplicationTimeout(appTimeout, hsr, appId);
    } catch (IllegalArgumentException e) {
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the updateApplicationTimeout appId: %s.", appId);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("updateApplicationTimeout Failed.", e);
    }
    return null;
  }

  @Override
  public AppAttemptsInfo getAppAttempts(HttpServletRequest hsr, String appId) {

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);
      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      return interceptor.getAppAttempts(hsr, appId);
    } catch (IllegalArgumentException e) {
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the AppAttempt appId: %s.", appId);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("getAppAttempts Failed.", e);
    }
    return null;
  }

  @Override
  public RMQueueAclInfo checkUserAccessToQueue(String queue, String username,
      String queueAclType, HttpServletRequest hsr) {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public AppAttemptInfo getAppAttempt(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }
    if (appAttemptId == null || appAttemptId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appAttemptId is empty or null.");
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);

      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      return interceptor.getAppAttempt(req, res, appId, appAttemptId);
    } catch (IllegalArgumentException e) {
      RouterServerUtil.logAndThrowRunTimeException(e,
          "Unable to get the AppAttempt appId: %s, appAttemptId: %s.", appId, appAttemptId);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("getContainer Failed.", e);
    }

    return null;
  }

  @Override
  public ContainersInfo getContainers(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {

    ContainersInfo containersInfo = new ContainersInfo();

    Map<SubClusterId, SubClusterInfo> subClustersActive;
    try {
      subClustersActive = getActiveSubclusters();
    } catch (NotFoundException e) {
      LOG.error("Get all active sub cluster(s) error.", e);
      return containersInfo;
    }

    try {
      Class[] argsClasses = new Class[]{
          HttpServletRequest.class, HttpServletResponse.class, String.class, String.class};
      Object[] args = new Object[]{req, res, appId, appAttemptId};
      ClientMethod remoteMethod = new ClientMethod("getContainers", argsClasses, args);
      Map<SubClusterInfo, ContainersInfo> containersInfoMap =
          invokeConcurrent(subClustersActive.values(), remoteMethod, ContainersInfo.class);
      if (containersInfoMap != null) {
        containersInfoMap.values().forEach(containers ->
            containersInfo.addAll(containers.getContainers()));
      }
    } catch (Exception ex) {
      LOG.error("Failed to return GetContainers.",  ex);
    }

    return containersInfo;
  }

  @Override
  public ContainerInfo getContainer(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId,
      String containerId) {

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }
    if (appAttemptId == null || appAttemptId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appAttemptId is empty or null.");
    }
    if (containerId == null || containerId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the containerId is empty or null.");
    }

    try {
      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(appId);

      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      return interceptor.getContainer(req, res, appId, appAttemptId, containerId);
    } catch (IllegalArgumentException e) {
      String msg = String.format(
          "Unable to get the AppAttempt appId: %s, appAttemptId: %s, containerId: %s.", appId,
          appAttemptId, containerId);
      RouterServerUtil.logAndThrowRunTimeException(msg, e);
    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("getContainer Failed.", e);
    }

    return null;
  }

  @Override
  public Response updateSchedulerConfiguration(SchedConfUpdateInfo mutationInfo,
      HttpServletRequest hsr)
      throws AuthorizationException, InterruptedException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Response getSchedulerConfiguration(HttpServletRequest hsr)
      throws AuthorizationException {
    throw new NotImplementedException("Code is not implemented");
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

    if (containerId == null || containerId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the containerId is empty or null.");
    }

    if (command == null || command.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the command is empty or null.");
    }

    try {
      ContainerId containerIdObj = ContainerId.fromString(containerId);
      ApplicationId applicationId = containerIdObj.getApplicationAttemptId().getApplicationId();

      SubClusterInfo subClusterInfo = getHomeSubClusterInfoByAppId(applicationId.toString());

      DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
          subClusterInfo.getSubClusterId(), subClusterInfo.getRMWebServiceAddress());
      return interceptor.signalToContainer(containerId, command, req);

    } catch (YarnException e) {
      RouterServerUtil.logAndThrowRunTimeException("signalToContainer Failed.", e);
    } catch (AuthorizationException e) {
      RouterServerUtil.logAndThrowRunTimeException("signalToContainer Author Failed.", e);
    }

    return null;
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

    // Send the requests in parallel
    CompletionService<R> compSvc = new ExecutorCompletionService<>(this.threadpool);

    for (final SubClusterInfo info : clusterIds) {
      compSvc.submit(() -> {
        DefaultRequestInterceptorREST interceptor = getOrCreateInterceptorForSubCluster(
            info.getSubClusterId(), info.getRMWebServiceAddress());
        try {
          Method method = DefaultRequestInterceptorREST.class.
              getMethod(request.getMethodName(), request.getTypes());
          Object retObj = method.invoke(interceptor, request.getParams());
          R ret = clazz.cast(retObj);
          return ret;
        } catch (Exception e) {
          LOG.error("SubCluster {} failed to call {} method.",
              info.getSubClusterId(), request.getMethodName(), e);
          return null;
        }
      });
    }

    clusterIds.stream().forEach(clusterId -> {
      try {
        Future<R> future = compSvc.take();
        R response = future.get();
        if (response != null) {
          results.put(clusterId, response);
        }
      } catch (Throwable e) {
        String msg = String.format("SubCluster %s failed to %s report.",
            clusterId, request.getMethodName());
        LOG.warn(msg, e);
        throw new YarnRuntimeException(msg, e);
      }
    });
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
    SubClusterInfo subClusterInfo = null;
    try {
      ApplicationId applicationId = ApplicationId.fromString(appId);
      SubClusterId subClusterId = federationFacade.getApplicationHomeSubCluster(applicationId);
      if (subClusterId == null) {
        RouterServerUtil.logAndThrowException(null,
            "Can't get HomeSubCluster by applicationId %s", applicationId);
      }
      subClusterInfo = federationFacade.getSubCluster(subClusterId);
      return subClusterInfo;
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
      SubClusterInfo subClusterInfo = federationFacade.getSubCluster(subClusterId);
      return subClusterInfo;
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
}