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

package org.apache.hadoop.yarn.server.router.webapp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServiceProtocol;
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
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * RouterWebServices is a service that runs on each router that can be used to
 * intercept and inspect {@link RMWebServiceProtocol} messages from client to
 * the cluster resource manager. It listens {@link RMWebServiceProtocol} REST
 * messages from the client and creates a request intercepting pipeline instance
 * for each client. The pipeline is a chain of {@link RESTRequestInterceptor}
 * instances that can inspect and modify the request/response as needed. The
 * main difference with AMRMProxyService is the protocol they implement.
 **/
@Singleton
@Path("/ws/v1/cluster")
public class RouterWebServices implements RMWebServiceProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterWebServices.class);
  private final Router router;
  private final Configuration conf;
  private @Context HttpServletResponse response;

  private Map<String, RequestInterceptorChainWrapper> userPipelineMap;

  // -------Default values of QueryParams for RMWebServiceProtocol--------

  public static final String DEFAULT_QUEUE = "default";
  public static final String DEFAULT_RESERVATION_ID = "";
  public static final String DEFAULT_START_TIME = "0";
  public static final String DEFAULT_END_TIME = "-1";
  public static final String DEFAULT_INCLUDE_RESOURCE = "false";

  @Inject
  public RouterWebServices(final Router router, Configuration conf) {
    this.router = router;
    this.conf = conf;
    int maxCacheSize =
        conf.getInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
            YarnConfiguration.DEFAULT_ROUTER_PIPELINE_CACHE_MAX_SIZE);
    this.userPipelineMap = Collections.synchronizedMap(
        new LRUCacheHashMap<String, RequestInterceptorChainWrapper>(
            maxCacheSize, true));
  }

  /**
   * Returns the comma separated intercepter class names from the configuration.
   *
   * @param conf
   * @return the intercepter class names as an instance of ArrayList
   */
  private List<String> getInterceptorClassNames(Configuration config) {
    String configuredInterceptorClassNames =
        config.get(YarnConfiguration.ROUTER_WEBAPP_INTERCEPTOR_CLASS_PIPELINE,
            YarnConfiguration.DEFAULT_ROUTER_WEBAPP_INTERCEPTOR_CLASS);

    List<String> interceptorClassNames = new ArrayList<String>();
    Collection<String> tempList =
        StringUtils.getStringCollection(configuredInterceptorClassNames);
    for (String item : tempList) {
      interceptorClassNames.add(item.trim());
    }

    return interceptorClassNames;
  }

  private void init() {
    // clear content type
    response.setContentType(null);
  }

  @VisibleForTesting
  protected RequestInterceptorChainWrapper getInterceptorChain(
      final HttpServletRequest hsr) {
    String user = "";
    if (hsr != null) {
      user = hsr.getRemoteUser();
    }
    try {
      if (user == null || user.equals("")) {
        // Yarn Router user
        user = UserGroupInformation.getCurrentUser().getUserName();
      }
    } catch (IOException e) {
      LOG.error("Cannot get user: {}", e.getMessage());
    }
    if (!userPipelineMap.containsKey(user)) {
      initializePipeline(user);
    }
    return userPipelineMap.get(user);
  }

  /**
   * Gets the Request intercepter chains for all the users.
   *
   * @return the request intercepter chains.
   */
  @VisibleForTesting
  protected Map<String, RequestInterceptorChainWrapper> getPipelines() {
    return this.userPipelineMap;
  }

  /**
   * This method creates and returns reference of the first intercepter in the
   * chain of request intercepter instances.
   *
   * @return the reference of the first intercepter in the chain
   */
  @VisibleForTesting
  protected RESTRequestInterceptor createRequestInterceptorChain() {

    List<String> interceptorClassNames = getInterceptorClassNames(conf);

    RESTRequestInterceptor pipeline = null;
    RESTRequestInterceptor current = null;
    for (String interceptorClassName : interceptorClassNames) {
      try {
        Class<?> interceptorClass = conf.getClassByName(interceptorClassName);
        if (RESTRequestInterceptor.class.isAssignableFrom(interceptorClass)) {
          RESTRequestInterceptor interceptorInstance =
              (RESTRequestInterceptor) ReflectionUtils
                  .newInstance(interceptorClass, conf);
          if (pipeline == null) {
            pipeline = interceptorInstance;
            current = interceptorInstance;
            continue;
          } else {
            current.setNextInterceptor(interceptorInstance);
            current = interceptorInstance;
          }
        } else {
          throw new YarnRuntimeException(
              "Class: " + interceptorClassName + " not instance of "
                  + RESTRequestInterceptor.class.getCanonicalName());
        }
      } catch (ClassNotFoundException e) {
        throw new YarnRuntimeException(
            "Could not instantiate RESTRequestInterceptor: "
                + interceptorClassName,
            e);
      }
    }

    if (pipeline == null) {
      throw new YarnRuntimeException(
          "RequestInterceptor pipeline is not configured in the system");
    }
    return pipeline;
  }

  /**
   * Initializes the request intercepter pipeline for the specified user.
   *
   * @param user
   */
  private void initializePipeline(String user) {
    RequestInterceptorChainWrapper chainWrapper = null;
    synchronized (this.userPipelineMap) {
      if (this.userPipelineMap.containsKey(user)) {
        LOG.info("Request to start an already existing user: {}"
            + " was received, so ignoring.", user);
        return;
      }

      chainWrapper = new RequestInterceptorChainWrapper();
      this.userPipelineMap.put(user, chainWrapper);
    }

    // We register the pipeline instance in the map first and then initialize it
    // later because chain initialization can be expensive and we would like to
    // release the lock as soon as possible to prevent other applications from
    // blocking when one application's chain is initializing
    LOG.info("Initializing request processing pipeline for the user: {}", user);

    try {
      RESTRequestInterceptor interceptorChain =
          this.createRequestInterceptorChain();
      interceptorChain.init(user);
      chainWrapper.init(interceptorChain);
    } catch (Exception e) {
      synchronized (this.userPipelineMap) {
        this.userPipelineMap.remove(user);
      }
      throw e;
    }
  }

  /**
   * Private structure for encapsulating RequestInterceptor and user instances.
   *
   */
  @Private
  public static class RequestInterceptorChainWrapper {
    private RESTRequestInterceptor rootInterceptor;

    /**
     * Initializes the wrapper with the specified parameters.
     *
     * @param interceptor the first interceptor in the pipeline
     */
    public synchronized void init(RESTRequestInterceptor interceptor) {
      this.rootInterceptor = interceptor;
    }

    /**
     * Gets the root request intercepter.
     *
     * @return the root request intercepter
     */
    public synchronized RESTRequestInterceptor getRootInterceptor() {
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

  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ClusterInfo get() {
    return getClusterInfo();
  }

  @GET
  @Path(RMWSConsts.INFO)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ClusterInfo getClusterInfo() {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getClusterInfo();
  }

  @GET
  @Path(RMWSConsts.CLUSTER_USER_INFO)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ClusterUserInfo getClusterUserInfo(@Context HttpServletRequest hsr) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getClusterUserInfo(hsr);
  }

  @GET
  @Path(RMWSConsts.METRICS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ClusterMetricsInfo getClusterMetricsInfo() {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getClusterMetricsInfo();
  }

  @GET
  @Path(RMWSConsts.SCHEDULER)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public SchedulerTypeInfo getSchedulerInfo() {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getSchedulerInfo();
  }

  @POST
  @Path(RMWSConsts.SCHEDULER_LOGS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public String dumpSchedulerLogs(@FormParam(RMWSConsts.TIME) String time,
      @Context HttpServletRequest hsr) throws IOException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().dumpSchedulerLogs(time, hsr);
  }

  @GET
  @Path(RMWSConsts.NODES)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodesInfo getNodes(@QueryParam(RMWSConsts.STATES) String states) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getNodes(states);
  }

  @GET
  @Path(RMWSConsts.NODES_NODEID)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodeInfo getNode(@PathParam(RMWSConsts.NODEID) String nodeId) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getNode(nodeId);
  }

  @GET
  @Path(RMWSConsts.APPS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppsInfo getApps(@Context HttpServletRequest hsr,
      @QueryParam(RMWSConsts.STATE) String stateQuery,
      @QueryParam(RMWSConsts.STATES) Set<String> statesQuery,
      @QueryParam(RMWSConsts.FINAL_STATUS) String finalStatusQuery,
      @QueryParam(RMWSConsts.USER) String userQuery,
      @QueryParam(RMWSConsts.QUEUE) String queueQuery,
      @QueryParam(RMWSConsts.LIMIT) String count,
      @QueryParam(RMWSConsts.STARTED_TIME_BEGIN) String startedBegin,
      @QueryParam(RMWSConsts.STARTED_TIME_END) String startedEnd,
      @QueryParam(RMWSConsts.FINISHED_TIME_BEGIN) String finishBegin,
      @QueryParam(RMWSConsts.FINISHED_TIME_END) String finishEnd,
      @QueryParam(RMWSConsts.APPLICATION_TYPES) Set<String> applicationTypes,
      @QueryParam(RMWSConsts.APPLICATION_TAGS) Set<String> applicationTags,
      @QueryParam(RMWSConsts.DESELECTS) Set<String> unselectedFields) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getApps(hsr, stateQuery, statesQuery,
        finalStatusQuery, userQuery, queueQuery, count, startedBegin,
        startedEnd, finishBegin, finishEnd, applicationTypes, applicationTags,
        unselectedFields);
  }

  @GET
  @Path(RMWSConsts.SCHEDULER_ACTIVITIES)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ActivitiesInfo getActivities(@Context HttpServletRequest hsr,
      @QueryParam(RMWSConsts.NODEID) String nodeId) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getActivities(hsr, nodeId);
  }

  @GET
  @Path(RMWSConsts.SCHEDULER_APP_ACTIVITIES)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppActivitiesInfo getAppActivities(@Context HttpServletRequest hsr,
      @QueryParam(RMWSConsts.APP_ID) String appId,
      @QueryParam(RMWSConsts.MAX_TIME) String time) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getAppActivities(hsr, appId, time);
  }

  @GET
  @Path(RMWSConsts.APP_STATISTICS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ApplicationStatisticsInfo getAppStatistics(
      @Context HttpServletRequest hsr,
      @QueryParam(RMWSConsts.STATES) Set<String> stateQueries,
      @QueryParam(RMWSConsts.APPLICATION_TYPES) Set<String> typeQueries) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getAppStatistics(hsr, stateQueries,
        typeQueries);
  }

  @GET
  @Path(RMWSConsts.APPS_APPID)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppInfo getApp(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId,
      @QueryParam(RMWSConsts.DESELECTS) Set<String> unselectedFields) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getApp(hsr, appId, unselectedFields);
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_STATE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppState getAppState(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getAppState(hsr, appId);
  }

  @PUT
  @Path(RMWSConsts.APPS_APPID_STATE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response updateAppState(AppState targetState,
      @Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().updateAppState(targetState, hsr,
        appId);
  }

  @GET
  @Path(RMWSConsts.GET_NODE_TO_LABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodeToLabelsInfo getNodeToLabels(@Context HttpServletRequest hsr)
      throws IOException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getNodeToLabels(hsr);
  }

  @GET
  @Path(RMWSConsts.LABEL_MAPPINGS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public LabelsToNodesInfo getLabelsToNodes(
      @QueryParam(RMWSConsts.LABELS) Set<String> labels) throws IOException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getLabelsToNodes(labels);
  }

  @POST
  @Path(RMWSConsts.REPLACE_NODE_TO_LABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response replaceLabelsOnNodes(
      final NodeToLabelsEntryList newNodeToLabels,
      @Context HttpServletRequest hsr) throws Exception {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().replaceLabelsOnNodes(newNodeToLabels,
        hsr);
  }

  @POST
  @Path(RMWSConsts.NODES_NODEID_REPLACE_LABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response replaceLabelsOnNode(
      @QueryParam(RMWSConsts.LABELS) Set<String> newNodeLabelsName,
      @Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.NODEID) String nodeId) throws Exception {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().replaceLabelsOnNode(newNodeLabelsName,
        hsr, nodeId);
  }

  @GET
  @Path(RMWSConsts.GET_NODE_LABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodeLabelsInfo getClusterNodeLabels(@Context HttpServletRequest hsr)
      throws IOException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getClusterNodeLabels(hsr);
  }

  @POST
  @Path(RMWSConsts.ADD_NODE_LABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response addToClusterNodeLabels(NodeLabelsInfo newNodeLabels,
      @Context HttpServletRequest hsr) throws Exception {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().addToClusterNodeLabels(newNodeLabels,
        hsr);
  }

  @POST
  @Path(RMWSConsts.REMOVE_NODE_LABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response removeFromCluserNodeLabels(
      @QueryParam(RMWSConsts.LABELS) Set<String> oldNodeLabels,
      @Context HttpServletRequest hsr) throws Exception {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor()
        .removeFromCluserNodeLabels(oldNodeLabels, hsr);
  }

  @GET
  @Path(RMWSConsts.NODES_NODEID_GETLABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodeLabelsInfo getLabelsOnNode(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.NODEID) String nodeId) throws IOException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getLabelsOnNode(hsr, nodeId);
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_PRIORITY)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppPriority getAppPriority(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getAppPriority(hsr, appId);
  }

  @PUT
  @Path(RMWSConsts.APPS_APPID_PRIORITY)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response updateApplicationPriority(AppPriority targetPriority,
      @Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor()
        .updateApplicationPriority(targetPriority, hsr, appId);
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_QUEUE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppQueue getAppQueue(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getAppQueue(hsr, appId);
  }

  @PUT
  @Path(RMWSConsts.APPS_APPID_QUEUE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response updateAppQueue(AppQueue targetQueue,
      @Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().updateAppQueue(targetQueue, hsr,
        appId);
  }

  @POST
  @Path(RMWSConsts.APPS_NEW_APPLICATION)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response createNewApplication(@Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().createNewApplication(hsr);
  }

  @POST
  @Path(RMWSConsts.APPS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response submitApplication(ApplicationSubmissionContextInfo newApp,
      @Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().submitApplication(newApp, hsr);
  }

  @POST
  @Path(RMWSConsts.DELEGATION_TOKEN)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response postDelegationToken(DelegationToken tokenData,
      @Context HttpServletRequest hsr) throws AuthorizationException,
      IOException, InterruptedException, Exception {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().postDelegationToken(tokenData, hsr);
  }

  @POST
  @Path(RMWSConsts.DELEGATION_TOKEN_EXPIRATION)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response postDelegationTokenExpiration(@Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, Exception {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().postDelegationTokenExpiration(hsr);
  }

  @DELETE
  @Path(RMWSConsts.DELEGATION_TOKEN)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response cancelDelegationToken(@Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().cancelDelegationToken(hsr);
  }

  @POST
  @Path(RMWSConsts.RESERVATION_NEW)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response createNewReservation(@Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().createNewReservation(hsr);
  }

  @POST
  @Path(RMWSConsts.RESERVATION_SUBMIT)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response submitReservation(ReservationSubmissionRequestInfo resContext,
      @Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().submitReservation(resContext, hsr);
  }

  @POST
  @Path(RMWSConsts.RESERVATION_UPDATE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response updateReservation(ReservationUpdateRequestInfo resContext,
      @Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().updateReservation(resContext, hsr);
  }

  @POST
  @Path(RMWSConsts.RESERVATION_DELETE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response deleteReservation(ReservationDeleteRequestInfo resContext,
      @Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().deleteReservation(resContext, hsr);
  }

  @GET
  @Path(RMWSConsts.RESERVATION_LIST)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response listReservation(
      @QueryParam(RMWSConsts.QUEUE) @DefaultValue(DEFAULT_QUEUE) String queue,
      @QueryParam(RMWSConsts.RESERVATION_ID) @DefaultValue(DEFAULT_RESERVATION_ID) String reservationId,
      @QueryParam(RMWSConsts.START_TIME) @DefaultValue(DEFAULT_START_TIME) long startTime,
      @QueryParam(RMWSConsts.END_TIME) @DefaultValue(DEFAULT_END_TIME) long endTime,
      @QueryParam(RMWSConsts.INCLUDE_RESOURCE) @DefaultValue(DEFAULT_INCLUDE_RESOURCE) boolean includeResourceAllocations,
      @Context HttpServletRequest hsr) throws Exception {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().listReservation(queue, reservationId,
        startTime, endTime, includeResourceAllocations, hsr);
  }

  @GET
  @Path(RMWSConsts.APPS_TIMEOUTS_TYPE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppTimeoutInfo getAppTimeout(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId,
      @PathParam(RMWSConsts.TYPE) String type) throws AuthorizationException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getAppTimeout(hsr, appId, type);
  }

  @GET
  @Path(RMWSConsts.APPS_TIMEOUTS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppTimeoutsInfo getAppTimeouts(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getAppTimeouts(hsr, appId);
  }

  @PUT
  @Path(RMWSConsts.APPS_TIMEOUT)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response updateApplicationTimeout(AppTimeoutInfo appTimeout,
      @Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().updateApplicationTimeout(appTimeout,
        hsr, appId);
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_APPATTEMPTS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppAttemptsInfo getAppAttempts(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getAppAttempts(hsr, appId);
  }

  @GET
  @Path(RMWSConsts.CHECK_USER_ACCESS_TO_QUEUE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
                MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public RMQueueAclInfo checkUserAccessToQueue(
      @PathParam(RMWSConsts.QUEUE) String queue,
      @QueryParam(RMWSConsts.USER) String username,
      @QueryParam(RMWSConsts.QUEUE_ACL_TYPE)
      @DefaultValue("SUBMIT_APPLICATIONS") String queueAclType,
      @Context HttpServletRequest hsr) throws AuthorizationException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().checkUserAccessToQueue(queue,
        username, queueAclType, hsr);
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_APPATTEMPTS_APPATTEMPTID)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo getAppAttempt(
      @Context HttpServletRequest req, @Context HttpServletResponse res,
      @PathParam(RMWSConsts.APPID) String appId,
      @PathParam(RMWSConsts.APPATTEMPTID) String appAttemptId) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(req);
    return pipeline.getRootInterceptor().getAppAttempt(req, res, appId,
        appAttemptId);
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_APPATTEMPTS_APPATTEMPTID_CONTAINERS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public ContainersInfo getContainers(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(RMWSConsts.APPID) String appId,
      @PathParam(RMWSConsts.APPATTEMPTID) String appAttemptId) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(req);
    return pipeline.getRootInterceptor().getContainers(req, res, appId,
        appAttemptId);
  }

  @GET
  @Path(RMWSConsts.GET_CONTAINER)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public ContainerInfo getContainer(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(RMWSConsts.APPID) String appId,
      @PathParam(RMWSConsts.APPATTEMPTID) String appAttemptId,
      @PathParam(RMWSConsts.CONTAINERID) String containerId) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(req);
    return pipeline.getRootInterceptor().getContainer(req, res, appId,
        appAttemptId, containerId);
  }

  @VisibleForTesting
  protected void setResponse(HttpServletResponse response) {
    this.response = response;
  }

}
