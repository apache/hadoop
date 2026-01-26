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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
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
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

/**
 * <p>
 * The protocol between clients and the <code>ResourceManager</code> to
 * submit/abort jobs and to get information on applications, cluster metrics,
 * nodes, queues, ACLs and reservations via REST calls.
 * </p>
 *
 * The WebService is reachable by using {@link RMWSConsts#RM_WEB_SERVICE_PATH}
 */
@Private
@Evolving
public interface RMWebServiceProtocol {

  /**
   * This method retrieves the cluster information, and it is reachable by using
   * {@link RMWSConsts#INFO}.
   *
   * @return the cluster information
   */
  ClusterInfo get();

  /**
   * This method retrieves the cluster information, and it is reachable by using
   * {@link RMWSConsts#INFO}.
   *
   * @return the cluster information
   */
  ClusterInfo getClusterInfo();


  /**
   * This method retrieves the cluster user information, and it is reachable by using
   * {@link RMWSConsts#CLUSTER_USER_INFO}.
   *
   * @param hsr the servlet request
   * @return the cluster user information
   */
  ClusterUserInfo getClusterUserInfo(HttpServletRequest hsr);

  /**
   * This method retrieves the cluster metrics information, and it is reachable
   * by using {@link RMWSConsts#METRICS}.
   *
   * @see ApplicationClientProtocol#getClusterMetrics
   * @return the cluster metrics information
   */
  ClusterMetricsInfo getClusterMetricsInfo();

  /**
   * This method retrieves the current scheduler status, and it is reachable by
   * using {@link RMWSConsts#SCHEDULER}.
   *
   * @return the current scheduler status
   */
  SchedulerTypeInfo getSchedulerInfo();

  /**
   * This method dumps the scheduler logs for the time got in input, and it is
   * reachable by using {@link RMWSConsts#SCHEDULER_LOGS}.
   *
   * @param time the period of time. It is a FormParam.
   * @param hsr the servlet request
   * @return the result of the operation
   * @throws IOException when it cannot create dump log file
   */
  String dumpSchedulerLogs(String time, HttpServletRequest hsr)
      throws IOException;

  /**
   * This method retrieves all the nodes information in the cluster, and it is
   * reachable by using {@link RMWSConsts#NODES}.
   *
   * @see ApplicationClientProtocol#getClusterNodes
   * @param states the states we want to filter. It is a QueryParam.
   * @return all nodes in the cluster. If the states param is given, returns all
   *         nodes that are in the comma-separated list of states
   */
  NodesInfo getNodes(String states);

  /**
   * This method retrieves a specific node information, and it is reachable by
   * using {@link RMWSConsts#NODES_NODEID}.
   *
   * @param nodeId the node we want to retrieve the information. It is a
   *          PathParam.
   * @return the information about the node in input
   */
  NodeInfo getNode(String nodeId);

  /**
   * This method changes the resources of a specific node, and it is reachable
   * by using {@link RMWSConsts#NODE_RESOURCE}.
   *
   * @param hsr The servlet request.
   * @param nodeId The node we want to retrieve the information for.
   *               It is a PathParam.
   * @param resourceOption The resource change.
   * @throws AuthorizationException If the user is not authorized.
   * @return the resources of a specific node.
   */
  ResourceInfo updateNodeResource(HttpServletRequest hsr, String nodeId,
      ResourceOptionInfo resourceOption) throws AuthorizationException;

  /**
   * This method retrieves all the app reports in the cluster, and it is
   * reachable by using {@link RMWSConsts#APPS}.
   *
   * @see ApplicationClientProtocol#getApplications
   * @param hsr the servlet request
   * @param stateQuery right now the stateQuery is deprecated. It is a
   *          QueryParam.
   * @param statesQuery filter the result by states. It is a QueryParam.
   * @param finalStatusQuery filter the result by final states. It is a
   *          QueryParam.
   * @param userQuery filter the result by user. It is a QueryParam.
   * @param queueQuery filter the result by queue. It is a QueryParam.
   * @param count set a limit of the result. It is a QueryParam.
   * @param startedBegin filter the result by started begin time. It is a
   *          QueryParam.
   * @param startedEnd filter the result by started end time. It is a
   *          QueryParam.
   * @param finishBegin filter the result by finish begin time. It is a
   *          QueryParam.
   * @param finishEnd filter the result by finish end time. It is a QueryParam.
   * @param applicationTypes filter the result by types. It is a QueryParam.
   * @param applicationTags filter the result by tags. It is a QueryParam.
   * @param name filter the name of the application. It is a QueryParam.
   * @param unselectedFields De-selected params to avoid from report. It is a
   *          QueryParam.
   * @return all apps in the cluster
   */
  @SuppressWarnings("checkstyle:parameternumber")
  AppsInfo getApps(HttpServletRequest hsr, String stateQuery,
      Set<String> statesQuery, String finalStatusQuery, String userQuery,
      String queueQuery, String count, String startedBegin, String startedEnd,
      String finishBegin, String finishEnd, Set<String> applicationTypes,
      Set<String> applicationTags, String name, Set<String> unselectedFields);

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
  ActivitiesInfo getActivities(HttpServletRequest hsr, String nodeId,
      String groupBy);

  /**
   * This method retrieve the last n activities inside scheduler, and it is
   * reachable by using {@link RMWSConsts#SCHEDULER_BULK_ACTIVITIES}.
   *
   * @param hsr the servlet request
   * @param groupBy the groupBy type by which the activities should be
   *        aggregated. It is a QueryParam.
   * @param activitiesCount number of activities
   * @return last n activities
   * @throws InterruptedException if interrupted.
   */
  BulkActivitiesInfo getBulkActivities(HttpServletRequest hsr,
      String groupBy, int activitiesCount) throws InterruptedException;

  /**
   * This method retrieves all the activities for a specific app for a specific
   * period of time, and it is reachable by using
   * {@link RMWSConsts#SCHEDULER_APP_ACTIVITIES}.
   *
   * @param hsr the servlet request
   * @param appId the applicationId we want to retrieve the activities. It is a
   *          QueryParam.
   * @param time for how long we want to retrieve the activities. It is a
   *          QueryParam.
   * @param requestPriorities the request priorities we want to retrieve the
   *          activities. It is a QueryParam.
   * @param allocationRequestIds the allocation request ids we want to retrieve
   *          the activities. It is a QueryParam.
   * @param groupBy the groupBy type by which the activities should be
   *          aggregated. It is a QueryParam.
   * @param limit set a limit of the result. It is a QueryParam.
   * @param actions the required actions of app activities. It is a QueryParam.
   * @param summarize whether app activities in multiple scheduling processes
   *          need to be summarized. It is a QueryParam.
   * @return all the activities about a specific app for a specific time
   */
  AppActivitiesInfo getAppActivities(HttpServletRequest hsr, String appId,
      String time, Set<String> requestPriorities,
      Set<String> allocationRequestIds, String groupBy, String limit,
      Set<String> actions, boolean summarize);

  /**
   * This method retrieves all the statistics for a specific app, and it is
   * reachable by using {@link RMWSConsts#APP_STATISTICS}.
   *
   * @param hsr the servlet request
   * @param stateQueries filter the result by states. It is a QueryParam.
   * @param typeQueries filter the result by type names. It is a QueryParam.
   * @return the application's statistics for specific states and types
   */
  ApplicationStatisticsInfo getAppStatistics(HttpServletRequest hsr,
      Set<String> stateQueries, Set<String> typeQueries);

  /**
   * This method retrieves the report for a specific app, and it is reachable by
   * using {@link RMWSConsts#APPS_APPID}.
   *
   * @see ApplicationClientProtocol#getApplicationReport
   * @param hsr the servlet request
   * @param appId the Id of the application we want the report. It is a
   *          PathParam.
   * @param unselectedFields De-selected param list to avoid from report. It is
   *          a QueryParam.
   * @return the app report for a specific application
   */
  AppInfo getApp(HttpServletRequest hsr, String appId,
      Set<String> unselectedFields);

  /**
   * This method retrieves the state for a specific app, and it is reachable by
   * using {@link RMWSConsts#APPS_APPID_STATE}.
   *
   * @param hsr the servlet request
   * @param appId the Id of the application we want the state. It is a
   *          PathParam.
   * @return the state for a specific application
   * @throws AuthorizationException if the user is not authorized
   */
  AppState getAppState(HttpServletRequest hsr, String appId)
      throws AuthorizationException;

  /**
   * This method updates the state of the app in input, and it is reachable by
   * using {@link RMWSConsts#APPS_APPID_STATE}.
   *
   * @param targetState the target state for the app. It is a content param.
   * @param hsr the servlet request
   * @param appId the Id of the application we want to update the state. It is a
   *          PathParam.
   * @return Response containing the status code
   * @throws AuthorizationException if the user is not authorized to invoke this
   *           method
   * @throws YarnException if app does not exist
   * @throws InterruptedException if interrupted
   * @throws IOException if doAs action throws an IOException
   */
  Response updateAppState(AppState targetState, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException;

  /**
   * This method retrieves all the node labels with the respective nodes in the
   * cluster, and it is reachable by using
   * {@link RMWSConsts#GET_NODE_TO_LABELS}.
   *
   * @see ApplicationClientProtocol#getNodeToLabels
   * @param hsr the servlet request
   * @return all the nodes within a node label
   * @throws IOException if an IOException happened
   */
  NodeToLabelsInfo getNodeToLabels(HttpServletRequest hsr) throws IOException;

  NodeLabelsInfo getRMNodeLabels(HttpServletRequest hsr) throws IOException;

  /**
   * This method retrieves all the node within multiple node labels in the
   * cluster, and it is reachable by using {@link RMWSConsts#LABEL_MAPPINGS}.
   *
   * @see ApplicationClientProtocol#getLabelsToNodes
   * @param labels filter the result by node labels. It is a QueryParam.
   * @return all the nodes within multiple node labels
   * @throws IOException if an IOException happened
   */
  LabelsToNodesInfo getLabelsToNodes(Set<String> labels) throws IOException;

  /**
   * This method replaces all the node labels for specific nodes, and it is
   * reachable by using {@link RMWSConsts#REPLACE_NODE_TO_LABELS}.
   *
   * @see ResourceManagerAdministrationProtocol#replaceLabelsOnNode
   * @param newNodeToLabels the list of new labels. It is a content param.
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws Exception if an exception happened
   */
  Response replaceLabelsOnNodes(NodeToLabelsEntryList newNodeToLabels,
      HttpServletRequest hsr) throws Exception;

  /**
   * This method replaces all the node labels for specific node, and it is
   * reachable by using {@link RMWSConsts#NODES_NODEID_REPLACE_LABELS}.
   *
   * @see ResourceManagerAdministrationProtocol#replaceLabelsOnNode
   * @param newNodeLabelsName the list of new labels. It is a QueryParam.
   * @param hsr the servlet request
   * @param nodeId the node we want to replace the node labels. It is a
   *          PathParam.
   * @return Response containing the status code
   * @throws Exception if an exception happened
   */
  Response replaceLabelsOnNode(Set<String> newNodeLabelsName,
      HttpServletRequest hsr, String nodeId) throws Exception;

  /**
   * This method retrieves all the node labels in the cluster, and it is
   * reachable by using {@link RMWSConsts#GET_NODE_LABELS}.
   *
   * @see ApplicationClientProtocol#getClusterNodeLabels
   * @param hsr the servlet request
   * @return all the node labels in the cluster
   * @throws IOException if an IOException happened
   */
  NodeLabelsInfo getClusterNodeLabels(HttpServletRequest hsr)
      throws IOException;

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
  Response addToClusterNodeLabels(NodeLabelsInfo newNodeLabels,
      HttpServletRequest hsr) throws Exception;

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
  Response removeFromClusterNodeLabels(Set<String> oldNodeLabels,
      HttpServletRequest hsr) throws Exception;

  /**
   * This method retrieves all the node labels for specific node, and it is
   * reachable by using {@link RMWSConsts#NODES_NODEID_GETLABELS}.
   *
   * @param hsr the servlet request
   * @param nodeId the node we want to get all the node labels. It is a
   *          PathParam.
   * @return all the labels for a specific node.
   * @throws IOException if an IOException happened
   */
  NodeLabelsInfo getLabelsOnNode(HttpServletRequest hsr, String nodeId)
      throws IOException;

  /**
   * This method retrieves the priority for a specific app, and it is reachable
   * by using {@link RMWSConsts#APPS_APPID_PRIORITY}.
   *
   * @param hsr the servlet request
   * @param appId the app we want to get the priority. It is a PathParam.
   * @return the priority for a specific application
   * @throws AuthorizationException in case of the user is not authorized
   */
  AppPriority getAppPriority(HttpServletRequest hsr, String appId)
      throws AuthorizationException;

  /**
   * This method updates the priority for a specific application, and it is
   * reachable by using {@link RMWSConsts#APPS_APPID_PRIORITY}.
   *
   * @param targetPriority the priority we want to set for the app. It is a
   *          content param.
   * @param hsr the servlet request
   * @param appId the application we want to update its priority. It is a
   *          PathParam.
   * @return Response containing the status code
   * @throws AuthorizationException if the user is not authenticated
   * @throws YarnException if the target is null
   * @throws IOException if the update fails.
   * @throws InterruptedException if interrupted.
   */
  Response updateApplicationPriority(AppPriority targetPriority,
      HttpServletRequest hsr, String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException;

  /**
   * This method retrieves the queue for a specific app, and it is reachable by
   * using {@link RMWSConsts#APPS_APPID_QUEUE}.
   *
   * @param hsr the servlet request
   * @param appId the application we want to retrieve its queue. It is a
   *          PathParam.
   * @return the Queue for a specific application.
   * @throws AuthorizationException if the user is not authenticated
   */
  AppQueue getAppQueue(HttpServletRequest hsr, String appId)
      throws AuthorizationException;

  /**
   * This method updates the queue for a specific application, and it is
   * reachable by using {@link RMWSConsts#APPS_APPID_QUEUE}.
   *
   * @param targetQueue the queue we want to set. It is a content param.
   * @param hsr the servlet request
   * @param appId the application we want to change its queue. It is a
   *          PathParam.
   * @return Response containing the status code
   * @throws AuthorizationException if the user is not authenticated
   * @throws YarnException if the app is not found
   * @throws IOException if the update fails.
   * @throws InterruptedException if interrupted.
   */
  Response updateAppQueue(AppQueue targetQueue, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException;

  /**
   * Generates a new ApplicationId which is then sent to the client. This method
   * is reachable by using {@link RMWSConsts#APPS_NEW_APPLICATION}.
   *
   * @see ApplicationClientProtocol#getNewApplication
   *
   * @param hsr the servlet request
   * @return Response containing the app id and the maximum resource
   *         capabilities
   * @throws AuthorizationException if the user is not authorized to invoke this
   *           method
   * @throws IOException if the creation fails
   * @throws InterruptedException if interrupted
   */
  Response createNewApplication(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException;

  /**
   * Function to submit an app to the RM. This method is reachable by using
   * {@link RMWSConsts#APPS}.
   *
   * @see ApplicationClientProtocol#submitApplication
   *
   * @param newApp structure containing information to construct the
   *          ApplicationSubmissionContext. It is a content param.
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws AuthorizationException if the user is not authorized to invoke this
   *           method
   * @throws IOException if the submission failed
   * @throws InterruptedException if interrupted
   */
  Response submitApplication(ApplicationSubmissionContextInfo newApp,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException;

  /**
   * This method posts a delegation token from the client, and it is reachable
   * by using {@link RMWSConsts#DELEGATION_TOKEN}.
   *
   * @see ApplicationBaseProtocol#getDelegationToken
   * @param tokenData the token to delegate. It is a content param.
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws AuthorizationException if Kerberos auth failed
   * @throws IOException if the delegation failed
   * @throws InterruptedException if interrupted
   * @throws Exception in case of bad request
   */
  Response postDelegationToken(DelegationToken tokenData,
      HttpServletRequest hsr) throws AuthorizationException, IOException,
      InterruptedException, Exception;

  /**
   * This method updates the expiration for a delegation token from the client,
   * and it is reachable by using
   * {@link RMWSConsts#DELEGATION_TOKEN_EXPIRATION}.
   *
   * @see ApplicationBaseProtocol#renewDelegationToken
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws AuthorizationException if Kerberos auth failed
   * @throws IOException if the delegation failed
   * @throws Exception in case of bad request
   */
  Response postDelegationTokenExpiration(HttpServletRequest hsr)
      throws AuthorizationException, IOException, Exception;

  /**
   * This method cancel the delegation token from the client, and it is
   * reachable by using {@link RMWSConsts#DELEGATION_TOKEN}.
   *
   * @see ApplicationBaseProtocol#cancelDelegationToken
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws AuthorizationException if Kerberos auth failed
   * @throws IOException if the delegation failed
   * @throws InterruptedException if interrupted
   * @throws Exception in case of bad request
   */
  Response cancelDelegationToken(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException,
      Exception;

  /**
   * Generates a new ReservationId which is then sent to the client. This method
   * is reachable by using {@link RMWSConsts#RESERVATION_NEW}.
   *
   * @see ApplicationClientProtocol#getNewReservation
   *
   * @param hsr the servlet request
   * @return Response containing the app id and the maximum resource
   *         capabilities
   * @throws AuthorizationException if the user is not authorized to invoke this
   *           method.
   * @throws IOException if creation failed
   * @throws InterruptedException if interrupted
   */
  Response createNewReservation(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException;

  /**
   * Function to submit a Reservation to the RM.This method is reachable by
   * using {@link RMWSConsts#RESERVATION_SUBMIT}.
   *
   * @see ApplicationClientProtocol#submitReservation
   *
   * @param resContext provides information to construct the
   *          ReservationSubmissionRequest. It is a content param.
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws AuthorizationException if the user is not authorized to invoke this
   *           method
   * @throws IOException if creation failed
   * @throws InterruptedException if interrupted
   */
  Response submitReservation(ReservationSubmissionRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException;

  /**
   * Function to update a Reservation to the RM. This method is reachable by
   * using {@link RMWSConsts#RESERVATION_UPDATE}.
   *
   * @see ApplicationClientProtocol#updateReservation
   *
   * @param resContext provides information to construct the
   *          ReservationUpdateRequest. It is a content param.
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws AuthorizationException if the user is not authorized to invoke this
   *           method
   * @throws IOException if the operation failed
   * @throws InterruptedException if interrupted
   */
  Response updateReservation(ReservationUpdateRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException;

  /**
   * Function to delete a Reservation to the RM. This method is reachable by
   * using {@link RMWSConsts#RESERVATION_DELETE}.
   *
   * @see ApplicationClientProtocol#deleteReservation
   *
   * @param resContext provides information to construct the
   *          ReservationDeleteRequest. It is a content param.
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws AuthorizationException when the user group information cannot be
   *           retrieved.
   * @throws IOException when a {@link ReservationDeleteRequest} cannot be
   *           created from the {@link ReservationDeleteRequestInfo}. This
   *           exception is also thrown on
   *           {@code ClientRMService.deleteReservation} invocation failure.
   * @throws InterruptedException if doAs action throws an InterruptedException.
   */
  Response deleteReservation(ReservationDeleteRequestInfo resContext,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException;

  /**
   * Function to retrieve a list of all the reservations. This method is
   * reachable by using {@link RMWSConsts#RESERVATION_LIST}.
   *
   * @see ApplicationClientProtocol#listReservations
   * @param queue filter the result by queue. It is a QueryParam.
   * @param reservationId filter the result by reservationId. It is a
   *          QueryParam.
   * @param startTime filter the result by start time. It is a QueryParam.
   * @param endTime filter the result by end time. It is a QueryParam.
   * @param includeResourceAllocations true if the resource allocation should be
   *          in the result, false otherwise. It is a QueryParam.
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws Exception in case of bad request
   */
  Response listReservation(String queue, String reservationId, long startTime,
      long endTime, boolean includeResourceAllocations, HttpServletRequest hsr)
      throws Exception;

  /**
   * This method retrieves the timeout information for a specific app with a
   * specific type, and it is reachable by using
   * {@link RMWSConsts#APPS_TIMEOUTS_TYPE}.
   *
   * @param hsr the servlet request
   * @param appId the application we want to get the timeout. It is a PathParam.
   * @param type the type of the timeouts. It is a PathParam.
   * @return the timeout for a specific application with a specific type.
   * @throws AuthorizationException if the user is not authorized
   */
  AppTimeoutInfo getAppTimeout(HttpServletRequest hsr, String appId,
      String type) throws AuthorizationException;

  /**
   * This method retrieves the timeout information for a specific app, and it is
   * reachable by using {@link RMWSConsts#APPS_TIMEOUTS}.
   *
   * @param hsr the servlet request
   * @param appId the application we want to get the timeouts. It is a
   *          PathParam.
   * @return the timeouts for a specific application
   * @throws AuthorizationException if the user is not authorized
   */
  AppTimeoutsInfo getAppTimeouts(HttpServletRequest hsr, String appId)
      throws AuthorizationException;

  /**
   * This method updates the timeout information for a specific app, and it is
   * reachable by using {@link RMWSConsts#APPS_TIMEOUT}.
   *
   * @see ApplicationClientProtocol#updateApplicationTimeouts
   * @param appTimeout the appTimeoutInfo. It is a content param.
   * @param hsr the servlet request
   * @param appId the application we want to update. It is a PathParam.
   * @return Response containing the status code
   * @throws AuthorizationException if the user is not authorized to invoke this
   *           method
   * @throws YarnException in case of bad request
   * @throws IOException if the operation failed
   * @throws InterruptedException if interrupted
   */
  Response updateApplicationTimeout(AppTimeoutInfo appTimeout,
      HttpServletRequest hsr, String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException;

  /**
   * This method retrieves all the attempts information for a specific app, and
   * it is reachable by using {@link RMWSConsts#APPS_APPID_APPATTEMPTS}.
   *
   * @see ApplicationBaseProtocol#getApplicationAttempts
   * @param hsr the servlet request
   * @param appId the application we want to get the attempts. It is a
   *          PathParam.
   * @return all the attempts info for a specific application
   */
  AppAttemptsInfo getAppAttempts(HttpServletRequest hsr, String appId);

  /**
   * This method verifies if a user has access to a specified queue.
   *
   * @return Response containing the status code.
   *
   * @param queue queue
   * @param username user
   * @param queueAclType acl type of queue, it could be
   *                     SUBMIT_APPLICATIONS/ADMINISTER_QUEUE
   * @param hsr request
   *
   * @throws AuthorizationException if the user is not authorized to invoke this
   *                                method.
   */
  RMQueueAclInfo checkUserAccessToQueue(String queue, String username,
      String queueAclType, HttpServletRequest hsr)
      throws AuthorizationException;

  /**
   * This method sends a signal to container.
   * @param containerId containerId
   * @param command signal command, it could be OUTPUT_THREAD_DUMP/
   *                GRACEFUL_SHUTDOWN/FORCEFUL_SHUTDOWN
   * @param req request
   * @return Response containing the status code
   * @throws AuthorizationException if the user is not authorized to invoke this
   *                                method.
   */
  Response signalToContainer(String containerId, String command,
      HttpServletRequest req) throws AuthorizationException;

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
  Response updateSchedulerConfiguration(SchedConfUpdateInfo mutationInfo,
      HttpServletRequest hsr) throws AuthorizationException, InterruptedException;

  /**
   * This method retrieves all the Scheduler configuration, and it is reachable
   * by using {@link RMWSConsts#SCHEDULER_CONF}.
   *
   * @param hsr the servlet request
   * @return Response containing the status code
   * @throws AuthorizationException if the user is not authorized to invoke this
   *         method.
   */
  Response getSchedulerConfiguration(HttpServletRequest hsr) throws AuthorizationException;
}
