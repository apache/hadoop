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
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
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

  public void setWebAppAddress(String webAppAddress) {
    this.webAppAddress = webAppAddress;
  }

  protected void setSubClusterId(SubClusterId scId) {
    this.subClusterId = scId;
  }

  protected SubClusterId getSubClusterId() {
    return this.subClusterId;
  }

  @Override
  public void init(String user) {
    webAppAddress = WebAppUtils.getRMWebAppURLWithScheme(getConf());
  }

  @Override
  public ClusterInfo get() {
    return getClusterInfo();
  }

  @Override
  public ClusterInfo getClusterInfo() {
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        ClusterInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.INFO, null, null);
  }

  @Override
  public ClusterUserInfo getClusterUserInfo(HttpServletRequest hsr) {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
            ClusterUserInfo.class, HTTPMethods.GET,
            RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.CLUSTER_USER_INFO, null, null);
  }

  @Override
  public ClusterMetricsInfo getClusterMetricsInfo() {
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        ClusterMetricsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null);
  }

  @Override
  public SchedulerTypeInfo getSchedulerInfo() {
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        SchedulerTypeInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER, null, null);
  }

  @Override
  public String dumpSchedulerLogs(String time, HttpServletRequest hsr)
      throws IOException {
    // time is specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        String.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_LOGS, null, null);
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
        additionalParam);
  }

  @Override
  public NodeInfo getNode(String nodeId) {
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        NodeInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES + "/" + nodeId, null,
        null);
  }

  @Override
  public AppsInfo getApps(HttpServletRequest hsr, String stateQuery,
      Set<String> statesQuery, String finalStatusQuery, String userQuery,
      String queueQuery, String count, String startedBegin, String startedEnd,
      String finishBegin, String finishEnd, Set<String> applicationTypes,
      Set<String> applicationTags, Set<String> unselectedFields) {
    // all the params are specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS, null, null);
  }

  @Override
  public ActivitiesInfo getActivities(HttpServletRequest hsr, String nodeId) {
    // nodeId is specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        ActivitiesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_ACTIVITIES, null,
        null);
  }

  @Override
  public AppActivitiesInfo getAppActivities(HttpServletRequest hsr,
      String appId, String time) {
    // time and appId are specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppActivitiesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_APP_ACTIVITIES,
        null, null);
  }

  @Override
  public ApplicationStatisticsInfo getAppStatistics(HttpServletRequest hsr,
      Set<String> stateQueries, Set<String> typeQueries) {
    // stateQueries and typeQueries are specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        ApplicationStatisticsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APP_STATISTICS, null, null);
  }

  @Override
  public AppInfo getApp(HttpServletRequest hsr, String appId,
      Set<String> unselectedFields) {
    // unselectedFields is specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId, null,
        null);
  }

  @Override
  public AppState getAppState(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppState.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.STATE,
        null, null);
  }

  @Override
  public Response updateAppState(AppState targetState, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.PUT, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.STATE,
        targetState, null);
  }

  @Override
  public NodeToLabelsInfo getNodeToLabels(HttpServletRequest hsr)
      throws IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        NodeToLabelsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.GET_NODE_TO_LABELS, null,
        null);
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
        additionalParam);
  }

  @Override
  public Response replaceLabelsOnNodes(NodeToLabelsEntryList newNodeToLabels,
      HttpServletRequest hsr) throws IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.REPLACE_NODE_TO_LABELS,
        newNodeToLabels, null);
  }

  @Override
  public Response replaceLabelsOnNode(Set<String> newNodeLabelsName,
      HttpServletRequest hsr, String nodeId) throws Exception {
    // newNodeLabelsName is specified inside hsr
    return RouterWebServiceUtil
        .genericForward(webAppAddress, hsr,
            Response.class, HTTPMethods.POST, RMWSConsts.RM_WEB_SERVICE_PATH
                + RMWSConsts.NODES + "/" + nodeId + "/replace-labels",
            null, null);
  }

  @Override
  public NodeLabelsInfo getClusterNodeLabels(HttpServletRequest hsr)
      throws IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        NodeLabelsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.GET_NODE_LABELS, null,
        null);
  }

  @Override
  public Response addToClusterNodeLabels(NodeLabelsInfo newNodeLabels,
      HttpServletRequest hsr) throws Exception {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.ADD_NODE_LABELS,
        newNodeLabels, null);
  }

  @Override
  public Response removeFromCluserNodeLabels(Set<String> oldNodeLabels,
      HttpServletRequest hsr) throws Exception {
    // oldNodeLabels is specified inside hsr
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.REMOVE_NODE_LABELS, null,
        null);
  }

  @Override
  public NodeLabelsInfo getLabelsOnNode(HttpServletRequest hsr, String nodeId)
      throws IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        NodeLabelsInfo.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.NODES + "/" + nodeId + "/get-labels",
        null, null);
  }

  @Override
  public AppPriority getAppPriority(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppPriority.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.PRIORITY,
        null, null);
  }

  @Override
  public Response updateApplicationPriority(AppPriority targetPriority,
      HttpServletRequest hsr, String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.PUT, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.PRIORITY,
        targetPriority, null);
  }

  @Override
  public AppQueue getAppQueue(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppQueue.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.QUEUE,
        null, null);
  }

  @Override
  public Response updateAppQueue(AppQueue targetQueue, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.PUT, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.QUEUE,
        targetQueue, null);
  }

  @Override
  public Response createNewApplication(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS_NEW_APPLICATION, null,
        null);
  }

  @Override
  public Response submitApplication(ApplicationSubmissionContextInfo newApp,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS, newApp, null);
  }

  @Override
  public Response postDelegationToken(DelegationToken tokenData,
      HttpServletRequest hsr) throws AuthorizationException, IOException,
      InterruptedException, Exception {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.DELEGATION_TOKEN, tokenData,
        null);
  }

  @Override
  public Response postDelegationTokenExpiration(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.DELEGATION_TOKEN_EXPIRATION,
        null, null);
  }

  @Override
  public Response cancelDelegationToken(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.DELETE,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.DELEGATION_TOKEN, null,
        null);
  }

  @Override
  public Response createNewReservation(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_NEW, null,
        null);
  }

  @Override
  public Response submitReservation(ReservationSubmissionRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_SUBMIT,
        resContext, null);
  }

  @Override
  public Response updateReservation(ReservationUpdateRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_UPDATE,
        resContext, null);
  }

  @Override
  public Response deleteReservation(ReservationDeleteRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.RESERVATION_DELETE,
        resContext, null);
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
        null);
  }

  @Override
  public AppTimeoutInfo getAppTimeout(HttpServletRequest hsr, String appId,
      String type) throws AuthorizationException {
    return RouterWebServiceUtil
        .genericForward(webAppAddress, hsr, AppTimeoutInfo.class,
            HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS
                + "/" + appId + "/" + RMWSConsts.TIMEOUTS + "/" + type,
            null, null);
  }

  @Override
  public AppTimeoutsInfo getAppTimeouts(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppTimeoutsInfo.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.TIMEOUTS,
        null, null);
  }

  @Override
  public Response updateApplicationTimeout(AppTimeoutInfo appTimeout,
      HttpServletRequest hsr, String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.PUT, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.TIMEOUT,
        appTimeout, null);
  }

  @Override
  public AppAttemptsInfo getAppAttempts(HttpServletRequest hsr, String appId) {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppAttemptsInfo.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.APPATTEMPTS,
        null, null);
  }

  @Override
  public RMQueueAclInfo checkUserAccessToQueue(String queue, String username,
      String queueAclType, HttpServletRequest hsr) {
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        RMQueueAclInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.QUEUES + "/" + queue
            + "/access", null, null);
  }

  @Override
  public AppAttemptInfo getAppAttempt(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {
    return RouterWebServiceUtil.genericForward(webAppAddress, req,
        AppAttemptInfo.class,
        HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/"
            + appId + "/" + RMWSConsts.APPATTEMPTS + "/" + appAttemptId,
        null, null);
  }

  @Override
  public ContainersInfo getContainers(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {
    return RouterWebServiceUtil.genericForward(webAppAddress, req,
        ContainersInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId + "/"
            + RMWSConsts.APPATTEMPTS + "/" + appAttemptId + "/"
            + RMWSConsts.CONTAINERS,
        null, null);
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
        null, null);
  }

  @Override
  public void setNextInterceptor(RESTRequestInterceptor next) {
    throw new YarnRuntimeException("setNextInterceptor is being called on "
        + "DefaultRequestInterceptorREST, which should be the last one "
        + "in the chain. Check if the interceptor pipeline configuration "
        + "is correct");
  }

}