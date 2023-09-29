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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import com.sun.jersey.api.client.Client;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
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
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * Extends the AbstractRequestInterceptorClient class and provides an
 * implementation that simply forwards the client requests to the resource
 * manager.
 */
public class DefaultRequestInterceptorREST
    extends AbstractRESTRequestInterceptor {

  private String webAppAddress;
  private SubClusterId subClusterId = null;

  // It is very expensive to create the client
  // Jersey will spawn a thread for every client request
  private Client client = null;

  public void setWebAppAddress(String webAppAddress) {
    this.webAppAddress = webAppAddress;
  }

  protected String getWebAppAddress() {
    return this.webAppAddress;
  }

  protected void setSubClusterId(SubClusterId scId) {
    this.subClusterId = scId;
  }

  protected SubClusterId getSubClusterId() {
    return this.subClusterId;
  }

  @Override
  public void init(String user) {
    super.init(user);
    webAppAddress = WebAppUtils.getRMWebAppURLWithScheme(getConf());
    client = RouterWebServiceUtil.createJerseyClient(getConf());
  }

  @Override
  public ClusterInfo get() {
    return getClusterInfo();
  }

  @Override
  public ClusterInfo getClusterInfo() {
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        ClusterInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.INFO, null, null,
        getConf(), client);
  }

  @Override
  public ClusterUserInfo getClusterUserInfo(HttpServletRequest hsr) {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        ClusterUserInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.CLUSTER_USER_INFO, null,
        null, getConf(), client);
  }

  @Override
  public ClusterMetricsInfo getClusterMetricsInfo() {
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        ClusterMetricsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null,
        getConf(), client);
  }

  @Override
  public SchedulerTypeInfo getSchedulerInfo() {
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        SchedulerTypeInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER, null, null,
        getConf(), client);
  }

  @Override
  public String dumpSchedulerLogs(String time, HttpServletRequest hsr)
      throws IOException {
    // time is specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        String.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_LOGS, null, null,
        getConf(), client);
  }

  @Override
  public NodesInfo getNodes(String states) {
    // states will be part of additionalParam
    Map<String, String[]> additionalParam = new HashMap<String, String[]>();
    if (states != null && !states.isEmpty()) {
      additionalParam.put(RMWSConsts.STATES, new String[] {states});
    }
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        NodesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES, null,
        additionalParam, getConf(), client);
  }

  @Override
  public NodeInfo getNode(String nodeId) {
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        NodeInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES + "/" + nodeId, null,
        null, getConf(), client);
  }

  @Override
  public ResourceInfo updateNodeResource(HttpServletRequest hsr,
      String nodeId, ResourceOptionInfo resourceOption) {
    final String nodePath =
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES + "/" + nodeId;
    return RouterWebServiceUtil
        .genericForward(webAppAddress, hsr, ResourceInfo.class,
            HTTPMethods.POST, nodePath + "/resource", resourceOption, null,
            getConf(), client);
  }

  @Override
  public AppsInfo getApps(HttpServletRequest hsr, String stateQuery,
      Set<String> statesQuery, String finalStatusQuery, String userQuery,
      String queueQuery, String count, String startedBegin, String startedEnd,
      String finishBegin, String finishEnd, Set<String> applicationTypes,
      Set<String> applicationTags, String name, Set<String> unselectedFields) {
    // all the params are specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS, null, null,
        getConf(), client);
  }

  @Override
  public ActivitiesInfo getActivities(HttpServletRequest hsr, String nodeId,
      String groupBy) {
    // nodeId is specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        ActivitiesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_ACTIVITIES, null,
        null, getConf(), client);
  }

  @Override
  public BulkActivitiesInfo getBulkActivities(HttpServletRequest hsr,
      String groupBy, int activitiesCount) {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        BulkActivitiesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_BULK_ACTIVITIES,
        null, null, getConf(), client);
  }

  @Override
  public AppActivitiesInfo getAppActivities(HttpServletRequest hsr,
      String appId, String time, Set<String> requestPriorities,
      Set<String> allocationRequestIds, String groupBy, String limit,
      Set<String> actions, boolean summarize) {
    // time and appId are specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppActivitiesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_APP_ACTIVITIES,
        null, null, getConf(), client);
  }

  @Override
  public ApplicationStatisticsInfo getAppStatistics(HttpServletRequest hsr,
      Set<String> stateQueries, Set<String> typeQueries) {
    // stateQueries and typeQueries are specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        ApplicationStatisticsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APP_STATISTICS, null, null,
        getConf(), client);
  }

  @Override
  public AppInfo getApp(HttpServletRequest hsr, String appId,
      Set<String> unselectedFields) {
    // unselectedFields is specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId, null,
        null, getConf(), client);
  }

  @Override
  public AppState getAppState(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppState.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.STATE,
        null, null, getConf(), client);
  }

  @Override
  public Response updateAppState(AppState targetState, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.PUT, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.STATE,
        targetState, null, getConf(), client);
  }

  @Override
  public NodeToLabelsInfo getNodeToLabels(HttpServletRequest hsr)
      throws IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        NodeToLabelsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.GET_NODE_TO_LABELS, null,
        null, getConf(), client);
  }

  @Override
  public LabelsToNodesInfo getLabelsToNodes(Set<String> labels)
      throws IOException {
    // labels will be part of additionalParam
    Map<String, String[]> additionalParam = new HashMap<>();
    if (labels != null && !labels.isEmpty()) {
      additionalParam.put(RMWSConsts.LABELS,
          labels.toArray(new String[labels.size()]));
    }
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        LabelsToNodesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.LABEL_MAPPINGS, null,
        additionalParam, getConf(), client);
  }

  @Override
  public Response replaceLabelsOnNodes(NodeToLabelsEntryList newNodeToLabels,
      HttpServletRequest hsr) throws IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.REPLACE_NODE_TO_LABELS,
        newNodeToLabels, null, getConf(), client);
  }

  @Override
  public Response replaceLabelsOnNode(Set<String> newNodeLabelsName,
      HttpServletRequest hsr, String nodeId) throws Exception {
    // newNodeLabelsName is specified inside hsr
    return RouterWebServiceUtil
        .genericForward(webAppAddress, hsr,
            Response.class, HTTPMethods.POST, RMWSConsts.RM_WEB_SERVICE_PATH
                + RMWSConsts.NODES + "/" + nodeId + "/replace-labels",
            null, null, getConf(), client);
  }

  @Override
  public NodeLabelsInfo getClusterNodeLabels(HttpServletRequest hsr)
      throws IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        NodeLabelsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.GET_NODE_LABELS, null,
        null, getConf(), client);
  }

  @Override
  public Response addToClusterNodeLabels(NodeLabelsInfo newNodeLabels,
      HttpServletRequest hsr) throws Exception {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.ADD_NODE_LABELS,
        newNodeLabels, null, getConf(), client);
  }

  @Override
  public Response removeFromClusterNodeLabels(Set<String> oldNodeLabels,
      HttpServletRequest hsr) throws Exception {
    // oldNodeLabels is specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.REMOVE_NODE_LABELS, null,
        null, getConf(), client);
  }

  @Override
  public NodeLabelsInfo getLabelsOnNode(HttpServletRequest hsr, String nodeId)
      throws IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        NodeLabelsInfo.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.NODES + "/" + nodeId + "/get-labels",
        null, null, getConf(), client);
  }

  @Override
  public AppPriority getAppPriority(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppPriority.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.PRIORITY,
        null, null, getConf(), client);
  }

  @Override
  public Response updateApplicationPriority(AppPriority targetPriority,
      HttpServletRequest hsr, String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.PUT, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.PRIORITY,
        targetPriority, null, getConf(), client);
  }

  @Override
  public AppQueue getAppQueue(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppQueue.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.QUEUE,
        null, null, getConf(), client);
  }

  @Override
  public Response updateAppQueue(AppQueue targetQueue, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.PUT, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.QUEUE,
        targetQueue, null, getConf(), client);
  }

  @Override
  public Response createNewApplication(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS_NEW_APPLICATION, null,
        null, getConf(), client);
  }

  @Override
  public Response submitApplication(ApplicationSubmissionContextInfo newApp,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS, newApp, null,
        getConf(), client);
  }

  @Override
  public Response postDelegationToken(DelegationToken tokenData,
      HttpServletRequest hsr) throws AuthorizationException, IOException,
      InterruptedException, Exception {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.DELEGATION_TOKEN, tokenData,
        null, getConf(), client);
  }

  @Override
  public Response postDelegationTokenExpiration(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.DELEGATION_TOKEN_EXPIRATION,
        null, null, getConf(), client);
  }

  @Override
  public Response cancelDelegationToken(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.DELETE,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.DELEGATION_TOKEN, null,
        null, getConf(), client);
  }

  @Override
  public Response createNewReservation(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_NEW, null,
        null, getConf(), client);
  }

  @Override
  public Response submitReservation(ReservationSubmissionRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_SUBMIT,
        resContext, null, getConf(), client);
  }

  @Override
  public Response updateReservation(ReservationUpdateRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_UPDATE,
        resContext, null, getConf(), client);
  }

  @Override
  public Response deleteReservation(ReservationDeleteRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_DELETE,
        resContext, null, getConf(), client);
  }

  @Override
  public Response listReservation(String queue, String reservationId,
      long startTime, long endTime, boolean includeResourceAllocations,
      HttpServletRequest hsr) throws Exception {
    // queue, reservationId, startTime, endTime, includeResourceAllocations are
    // specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_LIST, null,
        null, getConf(), client);
  }

  @Override
  public AppTimeoutInfo getAppTimeout(HttpServletRequest hsr, String appId,
      String type) throws AuthorizationException {
    return RouterWebServiceUtil
        .genericForward(webAppAddress, hsr, AppTimeoutInfo.class,
            HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS
                + "/" + appId + "/" + RMWSConsts.TIMEOUTS + "/" + type,
            null, null, getConf(), client);
  }

  @Override
  public AppTimeoutsInfo getAppTimeouts(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppTimeoutsInfo.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.TIMEOUTS,
        null, null, getConf(), client);
  }

  @Override
  public Response updateApplicationTimeout(AppTimeoutInfo appTimeout,
      HttpServletRequest hsr, String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.PUT, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.TIMEOUT,
        appTimeout, null, getConf(), client);
  }

  @Override
  public AppAttemptsInfo getAppAttempts(HttpServletRequest hsr, String appId) {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppAttemptsInfo.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.APPATTEMPTS,
        null, null, getConf(), client);
  }

  @Override
  public RMQueueAclInfo checkUserAccessToQueue(String queue, String username,
      String queueAclType, HttpServletRequest hsr) throws AuthorizationException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        RMQueueAclInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH +  "/" + RMWSConsts.QUEUES + "/" + queue
            + "/access", null, null, getConf(), client);
  }

  @Override
  public AppAttemptInfo getAppAttempt(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {
    return RouterWebServiceUtil.genericForward(webAppAddress, req,
        AppAttemptInfo.class,
        HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/"
            + appId + "/" + RMWSConsts.APPATTEMPTS + "/" + appAttemptId,
        null, null, getConf(), client);
  }

  @Override
  public ContainersInfo getContainers(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {
    return RouterWebServiceUtil.genericForward(webAppAddress, req,
        ContainersInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId + "/"
            + RMWSConsts.APPATTEMPTS + "/" + appAttemptId + "/"
            + RMWSConsts.CONTAINERS,
        null, null, getConf(), client);
  }

  @Override
  public ContainerInfo getContainer(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId,
      String containerId) {
    return RouterWebServiceUtil.genericForward(webAppAddress, req,
        ContainerInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId + "/"
            + RMWSConsts.APPATTEMPTS + "/" + appAttemptId + "/"
            + RMWSConsts.CONTAINERS + "/" + containerId,
        null, null, getConf(), client);
  }

  @Override
  public Response updateSchedulerConfiguration(SchedConfUpdateInfo mutationInfo,
      HttpServletRequest req)
      throws AuthorizationException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, req,
        Response.class, HTTPMethods.PUT,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_CONF,
        mutationInfo, null, getConf(), client);
  }

  @Override
  public Response getSchedulerConfiguration(HttpServletRequest req)
      throws AuthorizationException {
    return RouterWebServiceUtil.genericForward(webAppAddress, req,
        Response.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_CONF,
        null, null, getConf(), client);
  }

  @Override
  public void setNextInterceptor(RESTRequestInterceptor next) {
    throw new YarnRuntimeException("setNextInterceptor is being called on "
        + "DefaultRequestInterceptorREST, which should be the last one "
        + "in the chain. Check if the interceptor pipeline configuration "
        + "is correct");
  }

  @Override
  public Response signalToContainer(String containerId, String command,
      HttpServletRequest req) throws AuthorizationException {
    return RouterWebServiceUtil
        .genericForward(webAppAddress, req, Response.class, HTTPMethods.POST,
            RMWSConsts.RM_WEB_SERVICE_PATH + "/" + RMWSConsts.CONTAINERS + "/"
                + containerId + "/" + RMWSConsts.SIGNAL + "/" + command, null,
            null, getConf(), client);
  }

  @VisibleForTesting
  public Client getClient() {
    return client;
  }

  @Override
  public NodeLabelsInfo getRMNodeLabels(HttpServletRequest hsr) {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        NodeLabelsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.GET_RM_NODE_LABELS,
        null, null, getConf(), client);
  }
}