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
import java.net.ConnectException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityLevel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.MutableCSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.NodeIDsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewReservation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationListInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDefinitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateResponseInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteResponseInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.BulkActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeAllocationInfo;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.PartitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.RMQueueAclInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.dao.ConfInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.router.webapp.BaseRouterWebServicesTest.QUEUE_DEFAULT;
import static org.apache.hadoop.yarn.server.router.webapp.BaseRouterWebServicesTest.QUEUE_DEFAULT_FULL;
import static org.apache.hadoop.yarn.server.router.webapp.BaseRouterWebServicesTest.QUEUE_DEDICATED;
import static org.apache.hadoop.yarn.server.router.webapp.BaseRouterWebServicesTest.QUEUE_DEDICATED_FULL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class mocks the RESTRequestInterceptor.
 */
public class MockDefaultRequestInterceptorREST
    extends DefaultRequestInterceptorREST {

  private static final Logger LOG =
      LoggerFactory.getLogger(MockDefaultRequestInterceptorREST.class);
  final private AtomicInteger applicationCounter = new AtomicInteger(0);
  // True if the Mock RM is running, false otherwise.
  // This property allows us to write tests for specific scenario as YARN RM
  // down e.g. network issue, failover.
  private boolean isRunning = true;
  private Map<ApplicationId, ApplicationReport> applicationMap = new HashMap<>();
  public static final String APP_STATE_RUNNING = "RUNNING";

  // duration(milliseconds), 1mins
  public static final long DURATION = 60*1000;

  // Containers 4
  public static final int NUM_CONTAINERS = 4;

  private Map<ReservationId, SubClusterId> reservationMap = new HashMap<>();
  private AtomicLong resCounter = new AtomicLong();
  private MockRM mockRM = null;

  private void validateRunning() throws ConnectException {
    if (!isRunning) {
      throw new ConnectException("RM is stopped");
    }
  }

  @Override
  public Response createNewApplication(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    validateRunning();

    ApplicationId applicationId =
        ApplicationId.newInstance(Integer.parseInt(getSubClusterId().getId()),
            applicationCounter.incrementAndGet());
    NewApplication appId =
        new NewApplication(applicationId.toString(), new ResourceInfo());
    return Response.status(Status.OK).entity(appId).build();
  }

  @Override
  public Response submitApplication(ApplicationSubmissionContextInfo newApp,
      HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    validateRunning();

    ApplicationId appId = ApplicationId.fromString(newApp.getApplicationId());
    LOG.info("Application submitted: " + appId);

    // Initialize appReport
    ApplicationReport appReport = ApplicationReport.newInstance(
        appId, ApplicationAttemptId.newInstance(appId, 1), null, newApp.getQueue(), null, null, 0,
        null, YarnApplicationState.ACCEPTED, "", null, 0, 0, null, null, null, 0,
        newApp.getApplicationType(), null, null, false, Priority.newInstance(newApp.getPriority()),
        null, null);

    // Initialize appTimeoutsMap
    HashMap<ApplicationTimeoutType, ApplicationTimeout> appTimeoutsMap = new HashMap<>();
    ApplicationTimeoutType timeoutType = ApplicationTimeoutType.LIFETIME;
    ApplicationTimeout appTimeOut =
        ApplicationTimeout.newInstance(ApplicationTimeoutType.LIFETIME, "UNLIMITED", 10);
    appTimeoutsMap.put(timeoutType, appTimeOut);
    appReport.setApplicationTimeouts(appTimeoutsMap);

    applicationMap.put(appId, appReport);
    return Response.status(Status.ACCEPTED).header(HttpHeaders.LOCATION, "")
        .entity(getSubClusterId()).build();
  }

  @Override
  public AppInfo getApp(HttpServletRequest hsr, String appId,
      Set<String> unselectedFields) {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ApplicationId applicationId = ApplicationId.fromString(appId);
    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    return new AppInfo();
  }

  @Override
  public AppsInfo getApps(HttpServletRequest hsr, String stateQuery,
      Set<String> statesQuery, String finalStatusQuery, String userQuery,
      String queueQuery, String count, String startedBegin, String startedEnd,
      String finishBegin, String finishEnd, Set<String> applicationTypes,
      Set<String> applicationTags, String name, Set<String> unselectedFields) {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }
    AppsInfo appsInfo = new AppsInfo();
    AppInfo appInfo = new AppInfo();

    appInfo.setAppId(
        ApplicationId.newInstance(Integer.parseInt(getSubClusterId().getId()),
            applicationCounter.incrementAndGet()).toString());
    appInfo.setAMHostHttpAddress("http://i_am_the_AM:1234");

    appsInfo.add(appInfo);
    return appsInfo;
  }

  @Override
  public Response updateAppState(AppState targetState, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException {
    validateRunning();

    ApplicationId applicationId = ApplicationId.fromString(appId);
    if (applicationMap.remove(applicationId) == null) {
      throw new ApplicationNotFoundException(
          "Trying to kill an absent application: " + appId);
    }

    if (targetState == null) {
      return Response.status(Status.BAD_REQUEST).build();
    }

    LOG.info("Force killing application: " + appId);
    AppState ret = new AppState();
    ret.setState(targetState.toString());
    return Response.status(Status.OK).entity(ret).build();
  }

  @Override
  public NodeInfo getNode(String nodeId) {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }
    NodeInfo node = null;
    SubClusterId subCluster = getSubClusterId();
    String subClusterId = subCluster.getId();
    if (nodeId.contains(subClusterId) || nodeId.contains("test")) {
      node = new NodeInfo();
      node.setId(nodeId);
      node.setLastHealthUpdate(Integer.parseInt(getSubClusterId().getId()));
    }
    return node;
  }

  @Override
  public NodesInfo getNodes(String states) {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }
    NodeInfo node = new NodeInfo();
    node.setId("Node " + Integer.valueOf(getSubClusterId().getId()));
    node.setLastHealthUpdate(Integer.parseInt(getSubClusterId().getId()));
    NodesInfo nodes = new NodesInfo();
    nodes.add(node);
    return nodes;
  }

  @Override
  public ResourceInfo updateNodeResource(HttpServletRequest hsr,
      String nodeId, ResourceOptionInfo resourceOption) {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }
    Resource resource = resourceOption.getResourceOption().getResource();
    return new ResourceInfo(resource);
  }

  @Override
  public ClusterMetricsInfo getClusterMetricsInfo() {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }
    ClusterMetricsInfo metrics = new ClusterMetricsInfo();
    metrics.setAppsSubmitted(Integer.parseInt(getSubClusterId().getId()));
    metrics.setAppsCompleted(Integer.parseInt(getSubClusterId().getId()));
    metrics.setAppsPending(Integer.parseInt(getSubClusterId().getId()));
    metrics.setAppsRunning(Integer.parseInt(getSubClusterId().getId()));
    metrics.setAppsFailed(Integer.parseInt(getSubClusterId().getId()));
    metrics.setAppsKilled(Integer.parseInt(getSubClusterId().getId()));

    return metrics;
  }

  @Override
  public AppState getAppState(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ApplicationId applicationId = ApplicationId.fromString(appId);
    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    return new AppState(APP_STATE_RUNNING);
  }

  public void setSubClusterId(int subClusterId) {
    setSubClusterId(SubClusterId.newInstance(Integer.toString(subClusterId)));
  }

  public boolean isRunning() {
    return isRunning;
  }

  public void setRunning(boolean runningMode) {
    this.isRunning = runningMode;
  }

  @Override
  public ContainersInfo getContainers(HttpServletRequest req, HttpServletResponse res,
      String appId, String appAttemptId) {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    // Try format conversion for app_id
    ApplicationId applicationId = null;
    try {
      applicationId = ApplicationId.fromString(appId);
    } catch (Exception e) {
      throw new BadRequestException(e);
    }

    // Try format conversion for app_attempt_id
    ApplicationAttemptId applicationAttemptId = null;
    try {
      applicationAttemptId =
          ApplicationAttemptId.fromString(appAttemptId);
    } catch (Exception e) {
      throw new BadRequestException(e);
    }

    // We avoid to check if the Application exists in the system because we need
    // to validate that each subCluster returns 1 container.
    ContainersInfo containers = new ContainersInfo();

    int subClusterId = Integer.valueOf(getSubClusterId().getId());

    ContainerId containerId = ContainerId.newContainerId(
        ApplicationAttemptId.fromString(appAttemptId), subClusterId);
    Resource allocatedResource =
        Resource.newInstance(subClusterId, subClusterId);

    NodeId assignedNode = NodeId.newInstance("Node", subClusterId);
    Priority priority = Priority.newInstance(subClusterId);
    long creationTime = subClusterId;
    long finishTime = subClusterId;
    String diagnosticInfo = "Diagnostic " + subClusterId;
    String logUrl = "Log " + subClusterId;
    int containerExitStatus = subClusterId;
    ContainerState containerState = ContainerState.COMPLETE;
    String nodeHttpAddress = "HttpAddress " + subClusterId;

    ContainerReport containerReport = ContainerReport.newInstance(
        containerId, allocatedResource, assignedNode, priority,
        creationTime, finishTime, diagnosticInfo, logUrl,
        containerExitStatus, containerState, nodeHttpAddress);

    ContainerInfo container = new ContainerInfo(containerReport);
    containers.add(container);

    return containers;
  }

  @Override
  public NodeToLabelsInfo getNodeToLabels(HttpServletRequest hsr) throws IOException {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }
    NodeLabelsInfo cpuNode = new NodeLabelsInfo(Collections.singleton("CPU"));
    NodeLabelsInfo gpuNode = new NodeLabelsInfo(Collections.singleton("GPU"));

    HashMap<String, NodeLabelsInfo> nodeLabels = new HashMap<>();
    nodeLabels.put("node1", cpuNode);
    nodeLabels.put("node2", gpuNode);
    return new NodeToLabelsInfo(nodeLabels);
  }

  @Override
  public LabelsToNodesInfo getLabelsToNodes(Set<String> labels) throws IOException {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    Map<NodeLabelInfo, NodeIDsInfo> labelsToNodes = new HashMap<>();

    NodeLabel labelX = NodeLabel.newInstance("x", false);
    NodeLabelInfo nodeLabelInfoX = new NodeLabelInfo(labelX);
    ArrayList<String> hostsX = new ArrayList<>(Arrays.asList("host1A", "host1B"));
    Resource resourceX = Resource.newInstance(20*1024, 10);
    NodeIDsInfo nodeIDsInfoX = new NodeIDsInfo(hostsX, resourceX);
    labelsToNodes.put(nodeLabelInfoX, nodeIDsInfoX);

    NodeLabel labelY = NodeLabel.newInstance("y", false);
    NodeLabelInfo nodeLabelInfoY = new NodeLabelInfo(labelY);
    ArrayList<String> hostsY = new ArrayList<>(Arrays.asList("host2A", "host2B"));
    Resource resourceY = Resource.newInstance(40*1024, 20);
    NodeIDsInfo nodeIDsInfoY = new NodeIDsInfo(hostsY, resourceY);
    labelsToNodes.put(nodeLabelInfoY, nodeIDsInfoY);

    NodeLabel labelZ = NodeLabel.newInstance("z", false);
    NodeLabelInfo nodeLabelInfoZ = new NodeLabelInfo(labelZ);
    ArrayList<String> hostsZ = new ArrayList<>(Arrays.asList("host3A", "host3B"));
    Resource resourceZ = Resource.newInstance(80*1024, 40);
    NodeIDsInfo nodeIDsInfoZ = new NodeIDsInfo(hostsZ, resourceZ);
    labelsToNodes.put(nodeLabelInfoZ, nodeIDsInfoZ);

    return new LabelsToNodesInfo(labelsToNodes);
  }

  @Override
  public NodeLabelsInfo getClusterNodeLabels(HttpServletRequest hsr) throws IOException {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }
    NodeLabel labelCpu = NodeLabel.newInstance("cpu", false);
    NodeLabel labelGpu = NodeLabel.newInstance("gpu", false);
    return new NodeLabelsInfo(Sets.newHashSet(labelCpu, labelGpu));
  }

  @Override
  public NodeLabelsInfo getLabelsOnNode(HttpServletRequest hsr, String nodeId) throws IOException {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    if (StringUtils.equalsIgnoreCase(nodeId, "node1")) {
      NodeLabel labelCpu = NodeLabel.newInstance("x", false);
      NodeLabel labelGpu = NodeLabel.newInstance("y", false);
      return new NodeLabelsInfo(Sets.newHashSet(labelCpu, labelGpu));
    } else {
      return null;
    }
  }

  @Override
  public ContainerInfo getContainer(HttpServletRequest req, HttpServletResponse res,
      String appId, String appAttemptId, String containerId) {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ContainerId newContainerId = ContainerId.fromString(containerId);

    Resource allocatedResource = Resource.newInstance(1024, 2);

    int subClusterId = Integer.valueOf(getSubClusterId().getId());
    NodeId assignedNode = NodeId.newInstance("Node", subClusterId);
    Priority priority = Priority.newInstance(subClusterId);
    long creationTime = subClusterId;
    long finishTime = subClusterId;
    String diagnosticInfo = "Diagnostic " + subClusterId;
    String logUrl = "Log " + subClusterId;
    int containerExitStatus = subClusterId;
    ContainerState containerState = ContainerState.COMPLETE;
    String nodeHttpAddress = "HttpAddress " + subClusterId;

    ContainerReport containerReport = ContainerReport.newInstance(
        newContainerId, allocatedResource, assignedNode, priority,
        creationTime, finishTime, diagnosticInfo, logUrl,
        containerExitStatus, containerState, nodeHttpAddress);

    return new ContainerInfo(containerReport);
  }

  @Override
  public Response signalToContainer(String containerId, String command,
      HttpServletRequest req) throws AuthorizationException {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    if (!EnumUtils.isValidEnum(SignalContainerCommand.class, command.toUpperCase())) {
      String errMsg = "Invalid command: " + command.toUpperCase() + ", valid commands are: "
          + Arrays.asList(SignalContainerCommand.values());
      return Response.status(Status.BAD_REQUEST).entity(errMsg).build();
    }

    return Response.status(Status.OK).build();
  }

  @Override
  public AppAttemptInfo getAppAttempt(HttpServletRequest req, HttpServletResponse res,
      String appId, String appAttemptId) {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ApplicationId applicationId = ApplicationId.fromString(appId);
    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    ApplicationAttemptId attemptId = ApplicationAttemptId.fromString(appAttemptId);

    ApplicationReport newApplicationReport = ApplicationReport.newInstance(
        applicationId, attemptId, "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 1, 2, 3, 4,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);

    ApplicationAttemptReport attempt = ApplicationAttemptReport.newInstance(
        attemptId, "host", 124, "url", "oUrl", "diagnostics",
        YarnApplicationAttemptState.FINISHED, ContainerId.newContainerId(
        newApplicationReport.getCurrentApplicationAttemptId(), 1));

    return new AppAttemptInfo(attempt);
  }

  @Override
  public AppAttemptsInfo getAppAttempts(HttpServletRequest hsr, String appId) {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ApplicationId applicationId = ApplicationId.fromString(appId);
    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    AppAttemptsInfo infos = new AppAttemptsInfo();
    infos.add(TestRouterWebServiceUtil.generateAppAttemptInfo(0));
    infos.add(TestRouterWebServiceUtil.generateAppAttemptInfo(1));
    return infos;
  }

  @Override
  public AppTimeoutInfo getAppTimeout(HttpServletRequest hsr,
      String appId, String type) throws AuthorizationException {

    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ApplicationId applicationId = ApplicationId.fromString(appId);
    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    ApplicationReport appReport = applicationMap.get(applicationId);
    Map<ApplicationTimeoutType, ApplicationTimeout> timeouts = appReport.getApplicationTimeouts();
    ApplicationTimeoutType paramType = ApplicationTimeoutType.valueOf(type);

    if (paramType == null) {
      throw new NotFoundException("application timeout type not found");
    }

    if (!timeouts.containsKey(paramType)) {
      throw new NotFoundException("timeout with id: " + appId + " not found");
    }

    ApplicationTimeout applicationTimeout = timeouts.get(paramType);

    AppTimeoutInfo timeoutInfo = new AppTimeoutInfo();
    timeoutInfo.setExpiryTime(applicationTimeout.getExpiryTime());
    timeoutInfo.setTimeoutType(applicationTimeout.getTimeoutType());
    timeoutInfo.setRemainingTime(applicationTimeout.getRemainingTime());

    return timeoutInfo;
  }

  @Override
  public AppTimeoutsInfo getAppTimeouts(HttpServletRequest hsr, String appId)
      throws AuthorizationException {

    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ApplicationId applicationId = ApplicationId.fromString(appId);

    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    ApplicationReport appReport = applicationMap.get(applicationId);
    Map<ApplicationTimeoutType, ApplicationTimeout> timeouts = appReport.getApplicationTimeouts();

    AppTimeoutsInfo timeoutsInfo = new AppTimeoutsInfo();

    for (ApplicationTimeout timeout : timeouts.values()) {
      AppTimeoutInfo timeoutInfo = new AppTimeoutInfo();
      timeoutInfo.setExpiryTime(timeout.getExpiryTime());
      timeoutInfo.setTimeoutType(timeout.getTimeoutType());
      timeoutInfo.setRemainingTime(timeout.getRemainingTime());
      timeoutsInfo.add(timeoutInfo);
    }

    return timeoutsInfo;
  }

  @Override
  public Response updateApplicationTimeout(AppTimeoutInfo appTimeout, HttpServletRequest hsr,
      String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {

    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ApplicationId applicationId = ApplicationId.fromString(appId);

    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    ApplicationReport appReport = applicationMap.get(applicationId);
    Map<ApplicationTimeoutType, ApplicationTimeout> timeouts = appReport.getApplicationTimeouts();

    ApplicationTimeoutType paramTimeoutType = appTimeout.getTimeoutType();
    if (!timeouts.containsKey(paramTimeoutType)) {
      throw new NotFoundException("TimeOutType with id: " + appId + " not found");
    }

    ApplicationTimeout applicationTimeout = timeouts.get(paramTimeoutType);
    applicationTimeout.setTimeoutType(appTimeout.getTimeoutType());
    applicationTimeout.setExpiryTime(appTimeout.getExpireTime());
    applicationTimeout.setRemainingTime(appTimeout.getRemainingTimeInSec());

    AppTimeoutInfo result = new AppTimeoutInfo(applicationTimeout);

    return Response.status(Status.OK).entity(result).build();
  }

  @Override
  public Response updateApplicationPriority(AppPriority targetPriority, HttpServletRequest hsr,
      String appId) throws YarnException, InterruptedException, IOException {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ApplicationId applicationId = ApplicationId.fromString(appId);
    if (targetPriority == null) {
      return Response.status(Status.BAD_REQUEST).build();
    }

    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    ApplicationReport appReport = applicationMap.get(applicationId);
    Priority newPriority = Priority.newInstance(targetPriority.getPriority());
    appReport.setPriority(newPriority);

    return Response.status(Status.OK).entity(targetPriority).build();
  }

  @Override
  public AppPriority getAppPriority(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ApplicationId applicationId = ApplicationId.fromString(appId);

    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }
    ApplicationReport appReport = applicationMap.get(applicationId);
    Priority priority = appReport.getPriority();

    return new AppPriority(priority.getPriority());
  }

  @Override
  public AppQueue getAppQueue(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }
    ApplicationId applicationId = ApplicationId.fromString(appId);
    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }
    String queue = applicationMap.get(applicationId).getQueue();
    return new AppQueue(queue);
  }

  @Override
  public Response updateAppQueue(AppQueue targetQueue, HttpServletRequest hsr, String appId)
      throws AuthorizationException, YarnException, InterruptedException, IOException {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }
    ApplicationId applicationId = ApplicationId.fromString(appId);
    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }
    if (targetQueue == null || StringUtils.isBlank(targetQueue.getQueue())) {
      return Response.status(Status.BAD_REQUEST).build();
    }

    ApplicationReport appReport = applicationMap.get(applicationId);
    String originalQueue = appReport.getQueue();
    appReport.setQueue(targetQueue.getQueue());
    applicationMap.put(applicationId, appReport);
    LOG.info("Update applicationId = {} from originalQueue = {} to targetQueue = {}.",
        appId, originalQueue, targetQueue);

    AppQueue targetAppQueue = new AppQueue(targetQueue.getQueue());
    return Response.status(Status.OK).entity(targetAppQueue).build();
  }

  public void updateApplicationState(YarnApplicationState appState, String appId)
      throws AuthorizationException, YarnException, InterruptedException, IOException {
    validateRunning();
    ApplicationId applicationId = ApplicationId.fromString(appId);
    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }
    ApplicationReport appReport = applicationMap.get(applicationId);
    appReport.setYarnApplicationState(appState);
  }

  @Override
  public ApplicationStatisticsInfo getAppStatistics(
      HttpServletRequest hsr, Set<String> stateQueries, Set<String> typeQueries) {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    Map<String, StatisticsItemInfo> itemInfoMap = new HashMap<>();

    for (ApplicationReport appReport : applicationMap.values()) {

      YarnApplicationState appState = appReport.getYarnApplicationState();
      String appType = appReport.getApplicationType();

      if (stateQueries.contains(appState.name()) && typeQueries.contains(appType)) {
        String itemInfoMapKey = appState.toString() + "_" + appType;
        StatisticsItemInfo itemInfo = itemInfoMap.getOrDefault(itemInfoMapKey, null);
        if (itemInfo == null) {
          itemInfo = new StatisticsItemInfo(appState, appType, 1);
        } else {
          long newCount = itemInfo.getCount() + 1;
          itemInfo.setCount(newCount);
        }
        itemInfoMap.put(itemInfoMapKey, itemInfo);
      }
    }

    return new ApplicationStatisticsInfo(itemInfoMap.values());
  }

  @Override
  public AppActivitiesInfo getAppActivities(
      HttpServletRequest hsr, String appId, String time, Set<String> requestPriorities,
      Set<String> allocationRequestIds, String groupBy, String limit, Set<String> actions,
      boolean summarize) {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ApplicationId applicationId = ApplicationId.fromString(appId);
    if (!applicationMap.containsKey(applicationId)) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    SchedulerNode schedulerNode = TestUtils.getMockNode("host0", "rack", 1, 10240);

    RMContext rmContext = Mockito.mock(RMContext.class);
    Mockito.when(rmContext.getYarnConfiguration()).thenReturn(this.getConf());
    ResourceScheduler scheduler = Mockito.mock(ResourceScheduler.class);
    Mockito.when(scheduler.getMinimumResourceCapability()).thenReturn(Resources.none());
    Mockito.when(rmContext.getScheduler()).thenReturn(scheduler);
    LeafQueue mockQueue = Mockito.mock(LeafQueue.class);
    Map<ApplicationId, RMApp> rmApps = new ConcurrentHashMap<>();
    Mockito.doReturn(rmApps).when(rmContext).getRMApps();

    FiCaSchedulerNode node = (FiCaSchedulerNode) schedulerNode;
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(applicationId, 0);
    RMApp mockApp = Mockito.mock(RMApp.class);
    Mockito.doReturn(appAttemptId.getApplicationId()).when(mockApp).getApplicationId();
    Mockito.doReturn(FinalApplicationStatus.UNDEFINED).when(mockApp).getFinalApplicationStatus();
    rmApps.put(appAttemptId.getApplicationId(), mockApp);
    FiCaSchedulerApp app = new FiCaSchedulerApp(appAttemptId, "user", mockQueue,
        mock(ActiveUsersManager.class), rmContext);

    ActivitiesManager newActivitiesManager = new ActivitiesManager(rmContext);
    newActivitiesManager.turnOnAppActivitiesRecording(app.getApplicationId(), 3);

    int numActivities = 10;
    for (int i = 0; i < numActivities; i++) {
      ActivitiesLogger.APP.startAppAllocationRecording(newActivitiesManager, node,
          SystemClock.getInstance().getTime(), app);
      ActivitiesLogger.APP.recordAppActivityWithoutAllocation(newActivitiesManager, node, app,
          new SchedulerRequestKey(Priority.newInstance(0), 0, null),
          ActivityDiagnosticConstant.NODE_IS_BLACKLISTED, ActivityState.REJECTED,
          ActivityLevel.NODE);
      ActivitiesLogger.APP.finishSkippedAppAllocationRecording(newActivitiesManager,
          app.getApplicationId(), ActivityState.SKIPPED, ActivityDiagnosticConstant.EMPTY);
    }

    Set<Integer> prioritiesInt =
        requestPriorities.stream().map(pri -> Integer.parseInt(pri)).collect(Collectors.toSet());
    Set<Long> allocationReqIds =
        allocationRequestIds.stream().map(id -> Long.parseLong(id)).collect(Collectors.toSet());
    AppActivitiesInfo appActivitiesInfo = newActivitiesManager.
        getAppActivitiesInfo(app.getApplicationId(), prioritiesInt, allocationReqIds, null,
        Integer.parseInt(limit), summarize, 3);

    return appActivitiesInfo;
  }

  @Override
  public Response listReservation(String queue, String reservationId, long startTime, long endTime,
      boolean includeResourceAllocations, HttpServletRequest hsr) throws Exception {

    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    if (!StringUtils.equals(queue, QUEUE_DEDICATED_FULL)) {
      throw new RuntimeException("The specified queue: " + queue +
          " is not managed by reservation system." +
          " Please try again with a valid reservable queue.");
    }

    ReservationId reservationID =
        ReservationId.parseReservationId(reservationId);

    if (!reservationMap.containsKey(reservationID)) {
      throw new NotFoundException("reservationId with id: " + reservationId + " not found");
    }

    ClientRMService clientService = mockRM.getClientRMService();

    // listReservations
    ReservationListRequest request = ReservationListRequest.newInstance(
        queue, reservationId, startTime, endTime, includeResourceAllocations);
    ReservationListResponse resRespInfo = clientService.listReservations(request);
    ReservationListInfo resResponse =
        new ReservationListInfo(resRespInfo, includeResourceAllocations);

    return Response.status(Status.OK).entity(resResponse).build();
  }

  @Override
  public Response createNewReservation(HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {

    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ReservationId resId = ReservationId.newInstance(Time.now(), resCounter.incrementAndGet());
    LOG.info("Allocated new reservationId: {}.", resId);

    NewReservation reservationId = new NewReservation(resId.toString());
    return Response.status(Status.OK).entity(reservationId).build();
  }

  @Override
  public Response submitReservation(ReservationSubmissionRequestInfo resContext,
      HttpServletRequest hsr) throws AuthorizationException, IOException, InterruptedException {

    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    ReservationId reservationId = ReservationId.parseReservationId(resContext.getReservationId());
    ReservationDefinitionInfo definitionInfo = resContext.getReservationDefinition();
    ReservationDefinition definition =
            RouterServerUtil.convertReservationDefinition(definitionInfo);
    ReservationSubmissionRequest request = ReservationSubmissionRequest.newInstance(
            definition, resContext.getQueue(), reservationId);
    submitReservation(request);

    LOG.info("Reservation submitted: {}.", reservationId);

    SubClusterId subClusterId = getSubClusterId();
    reservationMap.put(reservationId, subClusterId);

    return Response.status(Status.ACCEPTED).build();
  }

  private void submitReservation(ReservationSubmissionRequest request) {
    try {
      // synchronize plan
      ReservationSystem reservationSystem = mockRM.getReservationSystem();
      reservationSystem.synchronizePlan(QUEUE_DEDICATED_FULL, true);
      // Generate reserved resources
      ClientRMService clientService = mockRM.getClientRMService();
      clientService.submitReservation(request);
    } catch (IOException | YarnException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Response updateReservation(ReservationUpdateRequestInfo resContext,
      HttpServletRequest hsr) throws AuthorizationException, IOException, InterruptedException {

    if (resContext == null || resContext.getReservationId() == null ||
        resContext.getReservationDefinition() == null) {
      return Response.status(Status.BAD_REQUEST).build();
    }

    String resId = resContext.getReservationId();
    ReservationId reservationId = ReservationId.parseReservationId(resId);

    if (!reservationMap.containsKey(reservationId)) {
      throw new NotFoundException("reservationId with id: " + reservationId + " not found");
    }

    // Generate reserved resources
    updateReservation(resContext);

    ReservationUpdateResponseInfo resRespInfo = new ReservationUpdateResponseInfo();
    return Response.status(Status.OK).entity(resRespInfo).build();
  }

  private void updateReservation(ReservationUpdateRequestInfo resContext) throws IOException {

    if (resContext == null) {
      throw new BadRequestException("Input ReservationSubmissionContext should not be null");
    }

    ReservationDefinitionInfo resInfo = resContext.getReservationDefinition();
    if (resInfo == null) {
      throw new BadRequestException("Input ReservationDefinition should not be null");
    }

    ReservationRequestsInfo resReqsInfo = resInfo.getReservationRequests();
    if (resReqsInfo == null || resReqsInfo.getReservationRequest() == null
        || resReqsInfo.getReservationRequest().isEmpty()) {
      throw new BadRequestException("The ReservationDefinition should " +
          "contain at least one ReservationRequest");
    }

    if (resContext.getReservationId() == null) {
      throw new BadRequestException("Update operations must specify an existing ReservationId");
    }

    ReservationRequestInterpreter[] values = ReservationRequestInterpreter.values();
    ReservationRequestInterpreter requestInterpreter =
        values[resReqsInfo.getReservationRequestsInterpreter()];
    List<ReservationRequest> list = new ArrayList<>();

    for (ReservationRequestInfo resReqInfo : resReqsInfo.getReservationRequest()) {
      ResourceInfo rInfo = resReqInfo.getCapability();
      Resource capability = Resource.newInstance(rInfo.getMemorySize(), rInfo.getvCores());
      int numContainers = resReqInfo.getNumContainers();
      int minConcurrency = resReqInfo.getMinConcurrency();
      long duration = resReqInfo.getDuration();
      ReservationRequest rr = ReservationRequest.newInstance(
          capability, numContainers, minConcurrency, duration);
      list.add(rr);
    }

    ReservationRequests reqs = ReservationRequests.newInstance(list, requestInterpreter);
    ReservationDefinition rDef = ReservationDefinition.newInstance(
        resInfo.getArrival(), resInfo.getDeadline(), reqs,
        resInfo.getReservationName(), resInfo.getRecurrenceExpression(),
        Priority.newInstance(resInfo.getPriority()));
    ReservationUpdateRequest request = ReservationUpdateRequest.newInstance(
        rDef, ReservationId.parseReservationId(resContext.getReservationId()));

    ClientRMService clientService = mockRM.getClientRMService();
    try {
      clientService.updateReservation(request);
    } catch (YarnException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Response deleteReservation(ReservationDeleteRequestInfo resContext, HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    if (!isRunning) {
      throw new RuntimeException("RM is stopped");
    }

    try {
      String resId = resContext.getReservationId();
      ReservationId reservationId = ReservationId.parseReservationId(resId);

      if (!reservationMap.containsKey(reservationId)) {
        throw new NotFoundException("reservationId with id: " + reservationId + " not found");
      }

      ReservationDeleteRequest reservationDeleteRequest =
          ReservationDeleteRequest.newInstance(reservationId);
      ClientRMService clientService = mockRM.getClientRMService();
      clientService.deleteReservation(reservationDeleteRequest);

      ReservationDeleteResponseInfo resRespInfo = new ReservationDeleteResponseInfo();
      reservationMap.remove(reservationId);

      return Response.status(Status.OK).entity(resRespInfo).build();
    } catch (YarnException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public MockRM getMockRM() {
    return mockRM;
  }

  @VisibleForTesting
  public void setMockRM(MockRM mockResourceManager) {
    this.mockRM = mockResourceManager;
  }

  @Override
  public NodeLabelsInfo getRMNodeLabels(HttpServletRequest hsr) {

    NodeLabelInfo nodeLabelInfo = new NodeLabelInfo();
    nodeLabelInfo.setExclusivity(true);
    nodeLabelInfo.setName("Test-Label");
    nodeLabelInfo.setActiveNMs(10);
    PartitionInfo partitionInfo = new PartitionInfo();

    NodeLabelsInfo nodeLabelsInfo = new NodeLabelsInfo();
    nodeLabelsInfo.getNodeLabelsInfo().add(nodeLabelInfo);

    return nodeLabelsInfo;
  }

  private MockRM setupResourceManager() throws Exception {
    DefaultMetricsSystem.setMiniClusterMode(true);

    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();

    // Define default queue
    conf.setCapacity(QUEUE_DEFAULT_FULL, 20);
    // Define dedicated queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {QUEUE_DEFAULT,  QUEUE_DEDICATED});
    conf.setCapacity(QUEUE_DEDICATED_FULL, 80);
    conf.setReservable(QUEUE_DEDICATED_FULL, true);

    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class, ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    MockRM rm = new MockRM(conf);
    rm.start();
    rm.registerNode("127.0.0.1:5678", 100*1024, 100);
    return rm;
  }

  @Override
  public RMQueueAclInfo checkUserAccessToQueue(String queue, String username,
      String queueAclType, HttpServletRequest hsr) throws AuthorizationException {

    ResourceManager mockResourceManager = mock(ResourceManager.class);
    Configuration conf = new YarnConfiguration();

    ResourceScheduler mockScheduler = new CapacityScheduler() {
      @Override
      public synchronized boolean checkAccess(UserGroupInformation callerUGI,
          QueueACL acl, String queueName) {
        if (acl == QueueACL.ADMINISTER_QUEUE) {
          if (callerUGI.getUserName().equals("admin")) {
            return true;
          }
        } else {
          if (ImmutableSet.of("admin", "yarn").contains(callerUGI.getUserName())) {
            return true;
          }
        }
        return false;
      }
    };

    when(mockResourceManager.getResourceScheduler()).thenReturn(mockScheduler);
    MockRMWebServices webSvc = new MockRMWebServices(mockResourceManager, conf,
        mock(HttpServletResponse.class));
    return webSvc.checkUserAccessToQueue(queue, username, queueAclType, hsr);
  }

  class MockRMWebServices {

    @Context
    private HttpServletResponse httpServletResponse;
    private ResourceManager resourceManager;

    private void initForReadableEndpoints() {
      // clear content type
      httpServletResponse.setContentType(null);
    }

    MockRMWebServices(ResourceManager rm, Configuration conf, HttpServletResponse response) {
      this.resourceManager = rm;
      this.httpServletResponse = response;
    }

    private UserGroupInformation getCallerUserGroupInformation(
        HttpServletRequest hsr, boolean usePrincipal) {

      String remoteUser = hsr.getRemoteUser();

      if (usePrincipal) {
        Principal princ = hsr.getUserPrincipal();
        remoteUser = princ == null ? null : princ.getName();
      }

      UserGroupInformation callerUGI = null;
      if (remoteUser != null) {
        callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
      }

      return callerUGI;
    }

    public RMQueueAclInfo checkUserAccessToQueue(
        String queue, String username, String queueAclType, HttpServletRequest hsr)
        throws AuthorizationException {
      initForReadableEndpoints();

      // For the user who invokes this REST call, he/she should have admin access
      // to the queue. Otherwise we will reject the call.
      UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
      if (callerUGI != null && !this.resourceManager.getResourceScheduler().checkAccess(
              callerUGI, QueueACL.ADMINISTER_QUEUE, queue)) {
        throw new ForbiddenException(
                "User=" + callerUGI.getUserName() + " doesn't haven access to queue="
                        + queue + " so it cannot check ACLs for other users.");
      }

      // Create UGI for the to-be-checked user.
      UserGroupInformation user = UserGroupInformation.createRemoteUser(username);
      if (user == null) {
        throw new ForbiddenException(
           "Failed to retrieve UserGroupInformation for user=" + username);
      }

      // Check if the specified queue acl is valid.
      QueueACL queueACL;
      try {
        queueACL = QueueACL.valueOf(queueAclType);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Specified queueAclType=" + queueAclType
            + " is not a valid type, valid queue acl types={"
            + "SUBMIT_APPLICATIONS/ADMINISTER_QUEUE}");
      }

      if (!this.resourceManager.getResourceScheduler().checkAccess(user, queueACL, queue)) {
        return new RMQueueAclInfo(false, user.getUserName(),
            "User=" + username + " doesn't have access to queue=" + queue
            + " with acl-type=" + queueAclType);
      }

      return new RMQueueAclInfo(true, user.getUserName(), "");
    }

    public String dumpSchedulerLogs(String time, HttpServletRequest hsr)
        throws IOException {

      int period = Integer.parseInt(time);
      if (period <= 0) {
        throw new BadRequestException("Period must be greater than 0");
      }

      return "Capacity scheduler logs are being created.";
    }
  }

  @Override
  public String dumpSchedulerLogs(String time, HttpServletRequest hsr) throws IOException {
    ResourceManager mockResourceManager = mock(ResourceManager.class);
    Configuration conf = new YarnConfiguration();
    MockRMWebServices webSvc = new MockRMWebServices(mockResourceManager, conf,
        mock(HttpServletResponse.class));
    return webSvc.dumpSchedulerLogs(time, hsr);
  }

  public Response replaceLabelsOnNodes(NodeToLabelsEntryList newNodeToLabels,
      HttpServletRequest hsr) throws IOException {
    return super.replaceLabelsOnNodes(newNodeToLabels, hsr);
  }

  @Override
  public Response replaceLabelsOnNode(Set<String> newNodeLabelsName,
      HttpServletRequest hsr, String nodeId) throws Exception {
    return super.replaceLabelsOnNode(newNodeLabelsName, hsr, nodeId);
  }

  public ActivitiesInfo getActivities(HttpServletRequest hsr, String nodeId, String groupBy) {
    if (!EnumUtils.isValidEnum(RMWSConsts.ActivitiesGroupBy.class, groupBy.toUpperCase())) {
      String errMessage = "Got invalid groupBy: " + groupBy + ", valid groupBy types: "
          + Arrays.asList(RMWSConsts.ActivitiesGroupBy.values());
      throw new IllegalArgumentException(errMessage);
    }

    SubClusterId subClusterId = getSubClusterId();
    ActivitiesInfo activitiesInfo = mock(ActivitiesInfo.class);
    Mockito.when(activitiesInfo.getNodeId()).thenReturn(nodeId);
    Mockito.when(activitiesInfo.getTimestamp()).thenReturn(1673081972L);
    Mockito.when(activitiesInfo.getDiagnostic()).thenReturn("Diagnostic:" + subClusterId.getId());

    List<NodeAllocationInfo> allocationInfos = new ArrayList<>();
    NodeAllocationInfo nodeAllocationInfo = mock(NodeAllocationInfo.class);
    Mockito.when(nodeAllocationInfo.getPartition()).thenReturn("p" + subClusterId.getId());
    Mockito.when(nodeAllocationInfo.getFinalAllocationState()).thenReturn("ALLOCATED");

    allocationInfos.add(nodeAllocationInfo);
    Mockito.when(activitiesInfo.getAllocations()).thenReturn(allocationInfos);
    return activitiesInfo;
  }

  @Override
  public BulkActivitiesInfo getBulkActivities(HttpServletRequest hsr,
      String groupBy, int activitiesCount) {

    if (activitiesCount <= 0) {
      throw new IllegalArgumentException("activitiesCount needs to be greater than 0.");
    }

    if (!EnumUtils.isValidEnum(RMWSConsts.ActivitiesGroupBy.class, groupBy.toUpperCase())) {
      String errMessage = "Got invalid groupBy: " + groupBy + ", valid groupBy types: "
          + Arrays.asList(RMWSConsts.ActivitiesGroupBy.values());
      throw new IllegalArgumentException(errMessage);
    }

    BulkActivitiesInfo bulkActivitiesInfo = new BulkActivitiesInfo();

    for (int i = 0; i < activitiesCount; i++) {
      SubClusterId subClusterId = getSubClusterId();
      ActivitiesInfo activitiesInfo = mock(ActivitiesInfo.class);
      Mockito.when(activitiesInfo.getNodeId()).thenReturn(subClusterId + "-nodeId-" + i);
      Mockito.when(activitiesInfo.getTimestamp()).thenReturn(1673081972L);
      Mockito.when(activitiesInfo.getDiagnostic()).thenReturn("Diagnostic:" + subClusterId.getId());

      List<NodeAllocationInfo> allocationInfos = new ArrayList<>();
      NodeAllocationInfo nodeAllocationInfo = mock(NodeAllocationInfo.class);
      Mockito.when(nodeAllocationInfo.getPartition()).thenReturn("p" + subClusterId.getId());
      Mockito.when(nodeAllocationInfo.getFinalAllocationState()).thenReturn("ALLOCATED");

      allocationInfos.add(nodeAllocationInfo);
      Mockito.when(activitiesInfo.getAllocations()).thenReturn(allocationInfos);
      bulkActivitiesInfo.getActivities().add(activitiesInfo);
    }

    return bulkActivitiesInfo;
  }

  public SchedulerTypeInfo getSchedulerInfo() {
    try {
      ResourceManager resourceManager = CapacitySchedulerTestUtilities.createResourceManager();
      CapacityScheduler cs = (CapacityScheduler) resourceManager.getResourceScheduler();
      CSQueue root = cs.getRootQueue();
      SchedulerInfo schedulerInfo = new CapacitySchedulerInfo(root, cs);
      return new SchedulerTypeInfo(schedulerInfo);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Response addToClusterNodeLabels(NodeLabelsInfo newNodeLabels, HttpServletRequest hsr)
      throws Exception {
    List<NodeLabelInfo> nodeLabelInfoList = newNodeLabels.getNodeLabelsInfo();
    NodeLabelInfo nodeLabelInfo = nodeLabelInfoList.get(0);
    String nodeLabelName = nodeLabelInfo.getName();

    // If nodeLabelName is ALL, we let all subclusters pass
    if (StringUtils.equals("ALL", nodeLabelName)) {
      return Response.status(Status.OK).build();
    } else if (StringUtils.equals("A0", nodeLabelName)) {
      SubClusterId subClusterId = getSubClusterId();
      String id = subClusterId.getId();
      if (StringUtils.contains("A0", id)) {
        return Response.status(Status.OK).build();
      } else {
        return Response.status(Status.BAD_REQUEST).entity(null).build();
      }
    }
    throw new YarnException("addToClusterNodeLabels Error");
  }

  @Override
  public Response removeFromClusterNodeLabels(Set<String> oldNodeLabels, HttpServletRequest hsr)
      throws Exception {
    // If oldNodeLabels contains ALL, we let all subclusters pass
    if (oldNodeLabels.contains("ALL")) {
      return Response.status(Status.OK).build();
    } else if (oldNodeLabels.contains("A0")) {
      SubClusterId subClusterId = getSubClusterId();
      String id = subClusterId.getId();
      if (StringUtils.contains("A0", id)) {
        return Response.status(Status.OK).build();
      } else {
        return Response.status(Status.BAD_REQUEST).entity(null).build();
      }
    }
    throw new YarnException("removeFromClusterNodeLabels Error");
  }

  @Override
  public Response updateSchedulerConfiguration(SchedConfUpdateInfo mutationInfo,
      HttpServletRequest req) throws AuthorizationException, InterruptedException {
    RMContext rmContext = mockRM.getRMContext();
    MutableCSConfigurationProvider provider = new MutableCSConfigurationProvider(rmContext);
    try {
      Configuration conf = new Configuration();
      conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
          YarnConfiguration.MEMORY_CONFIGURATION_STORE);
      provider.init(conf);
      provider.logAndApplyMutation(UserGroupInformation.getCurrentUser(), mutationInfo);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return Response.status(Status.OK).
        entity("Configuration change successfully applied.").build();
  }

  @Override
  public Response getSchedulerConfiguration(HttpServletRequest req) throws AuthorizationException {
    return Response.status(Status.OK).entity(new ConfInfo(mockRM.getConfig()))
        .build();
  }

  public ClusterInfo getClusterInfo() {
    ClusterInfo clusterInfo = new ClusterInfo(mockRM);
    return clusterInfo;
  }

  @Override
  public ClusterUserInfo getClusterUserInfo(HttpServletRequest hsr) {
    String remoteUser = hsr.getRemoteUser();
    UserGroupInformation callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    return new ClusterUserInfo(mockRM, callerUGI);
  }
}