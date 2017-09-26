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
import java.lang.reflect.UndeclaredThrowableException;
import java.security.AccessControlException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import com.google.common.base.Joiner;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NodeLabelsUtils;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
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
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FifoSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewReservation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntry;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDefinitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteResponseInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationListInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateResponseInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.AdHocLogDumper;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@Path(RMWSConsts.RM_WEB_SERVICE_PATH)
public class RMWebServices extends WebServices implements RMWebServiceProtocol {

  private static final Log LOG =
      LogFactory.getLog(RMWebServices.class.getName());

  private final ResourceManager rm;
  private static RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private final Configuration conf;
  private @Context HttpServletResponse response;

  // -------Default values of QueryParams for RMWebServiceProtocol--------

  public static final String DEFAULT_QUEUE = "default";
  public static final String DEFAULT_RESERVATION_ID = "";
  public static final String DEFAULT_START_TIME = "0";
  public static final String DEFAULT_END_TIME = "-1";
  public static final String DEFAULT_INCLUDE_RESOURCE = "false";

  @VisibleForTesting
  boolean isCentralizedNodeLabelConfiguration = true;

  public final static String DELEGATION_TOKEN_HEADER =
      "Hadoop-YARN-RM-Delegation-Token";

  @Inject
  public RMWebServices(final ResourceManager rm, Configuration conf) {
    // don't inject, always take appBaseRoot from RM.
    super(null);
    this.rm = rm;
    this.conf = conf;
    isCentralizedNodeLabelConfiguration =
        YarnConfiguration.isCentralizedNodeLabelConfiguration(conf);
  }

  RMWebServices(ResourceManager rm, Configuration conf,
      HttpServletResponse response) {
    this(rm, conf);
    this.response = response;
  }

  protected Boolean hasAccess(RMApp app, HttpServletRequest hsr) {
    // Check for the authorization.
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    List<String> forwardedAddresses = null;
    String forwardedFor = hsr.getHeader(RMWSConsts.FORWARDED_FOR);
    if (forwardedFor != null) {
      forwardedAddresses = Arrays.asList(forwardedFor.split(","));
    }
    if (callerUGI != null
        && !(this.rm.getApplicationACLsManager().checkAccess(callerUGI,
            ApplicationAccessType.VIEW_APP, app.getUser(),
            app.getApplicationId())
            || this.rm.getQueueACLsManager().checkAccess(callerUGI,
                QueueACL.ADMINISTER_QUEUE, app, hsr.getRemoteAddr(),
                forwardedAddresses))) {
      return false;
    }
    return true;
  }

  private void init() {
    // clear content type
    response.setContentType(null);
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
    return new ClusterInfo(this.rm);
  }

  @GET
  @Path(RMWSConsts.METRICS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ClusterMetricsInfo getClusterMetricsInfo() {
    init();
    return new ClusterMetricsInfo(this.rm);
  }

  @GET
  @Path(RMWSConsts.SCHEDULER)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public SchedulerTypeInfo getSchedulerInfo() {
    init();
    ResourceScheduler rs = rm.getResourceScheduler();
    SchedulerInfo sinfo;
    if (rs instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) rs;
      CSQueue root = cs.getRootQueue();
      sinfo = new CapacitySchedulerInfo(root, cs);
    } else if (rs instanceof FairScheduler) {
      FairScheduler fs = (FairScheduler) rs;
      sinfo = new FairSchedulerInfo(fs);
    } else if (rs instanceof FifoScheduler) {
      sinfo = new FifoSchedulerInfo(this.rm);
    } else {
      throw new NotFoundException("Unknown scheduler configured");
    }
    return new SchedulerTypeInfo(sinfo);
  }

  @POST
  @Path(RMWSConsts.SCHEDULER_LOGS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public String dumpSchedulerLogs(@FormParam(RMWSConsts.TIME) String time,
      @Context HttpServletRequest hsr) throws IOException {
    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    ApplicationACLsManager aclsManager = rm.getApplicationACLsManager();
    if (aclsManager.areACLsEnabled()) {
      if (callerUGI == null || !aclsManager.isAdmin(callerUGI)) {
        String msg = "Only admins can carry out this operation.";
        throw new ForbiddenException(msg);
      }
    }
    ResourceScheduler rs = rm.getResourceScheduler();
    int period = Integer.parseInt(time);
    if (period <= 0) {
      throw new BadRequestException("Period must be greater than 0");
    }
    final String logHierarchy =
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler";
    String logfile = "yarn-scheduler-debug.log";
    if (rs instanceof CapacityScheduler) {
      logfile = "yarn-capacity-scheduler-debug.log";
    } else if (rs instanceof FairScheduler) {
      logfile = "yarn-fair-scheduler-debug.log";
    }
    AdHocLogDumper dumper = new AdHocLogDumper(logHierarchy, logfile);
    // time period is sent to us in seconds
    dumper.dumpLogs("DEBUG", period * 1000);
    return "Capacity scheduler logs are being created.";
  }

  @GET
  @Path(RMWSConsts.NODES)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodesInfo getNodes(@QueryParam(RMWSConsts.STATES) String states) {
    init();
    ResourceScheduler sched = this.rm.getResourceScheduler();
    if (sched == null) {
      throw new NotFoundException("Null ResourceScheduler instance");
    }

    EnumSet<NodeState> acceptedStates;
    if (states == null) {
      acceptedStates = EnumSet.allOf(NodeState.class);
    } else {
      acceptedStates = EnumSet.noneOf(NodeState.class);
      for (String stateStr : states.split(",")) {
        acceptedStates
            .add(NodeState.valueOf(StringUtils.toUpperCase(stateStr)));
      }
    }

    Collection<RMNode> rmNodes =
        RMServerUtils.queryRMNodes(this.rm.getRMContext(), acceptedStates);
    NodesInfo nodesInfo = new NodesInfo();
    for (RMNode rmNode : rmNodes) {
      NodeInfo nodeInfo = new NodeInfo(rmNode, sched);
      if (EnumSet
          .of(NodeState.LOST, NodeState.DECOMMISSIONED, NodeState.REBOOTED)
          .contains(rmNode.getState())) {
        nodeInfo.setNodeHTTPAddress(RMWSConsts.EMPTY);
      }
      nodesInfo.add(nodeInfo);
    }

    return nodesInfo;
  }

  @GET
  @Path(RMWSConsts.NODES_NODEID)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodeInfo getNode(@PathParam(RMWSConsts.NODEID) String nodeId) {
    init();
    if (nodeId == null || nodeId.isEmpty()) {
      throw new NotFoundException("nodeId, " + nodeId + ", is empty or null");
    }
    ResourceScheduler sched = this.rm.getResourceScheduler();
    if (sched == null) {
      throw new NotFoundException("Null ResourceScheduler instance");
    }
    NodeId nid = NodeId.fromString(nodeId);
    RMNode ni = this.rm.getRMContext().getRMNodes().get(nid);
    boolean isInactive = false;
    if (ni == null) {
      ni = this.rm.getRMContext().getInactiveRMNodes().get(nid);
      if (ni == null) {
        throw new NotFoundException("nodeId, " + nodeId + ", is not found");
      }
      isInactive = true;
    }
    NodeInfo nodeInfo = new NodeInfo(ni, sched);
    if (isInactive) {
      nodeInfo.setNodeHTTPAddress(RMWSConsts.EMPTY);
    }
    return nodeInfo;
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
    boolean checkCount = false;
    boolean checkStart = false;
    boolean checkEnd = false;
    boolean checkAppTypes = false;
    boolean checkAppStates = false;
    boolean checkAppTags = false;
    long countNum = 0;

    // set values suitable in case both of begin/end not specified
    long sBegin = 0;
    long sEnd = Long.MAX_VALUE;
    long fBegin = 0;
    long fEnd = Long.MAX_VALUE;

    init();
    if (count != null && !count.isEmpty()) {
      checkCount = true;
      countNum = Long.parseLong(count);
      if (countNum <= 0) {
        throw new BadRequestException("limit value must be greater then 0");
      }
    }

    if (startedBegin != null && !startedBegin.isEmpty()) {
      checkStart = true;
      sBegin = Long.parseLong(startedBegin);
      if (sBegin < 0) {
        throw new BadRequestException(
            "startedTimeBegin must be greater than 0");
      }
    }
    if (startedEnd != null && !startedEnd.isEmpty()) {
      checkStart = true;
      sEnd = Long.parseLong(startedEnd);
      if (sEnd < 0) {
        throw new BadRequestException("startedTimeEnd must be greater than 0");
      }
    }
    if (sBegin > sEnd) {
      throw new BadRequestException(
          "startedTimeEnd must be greater than startTimeBegin");
    }

    if (finishBegin != null && !finishBegin.isEmpty()) {
      checkEnd = true;
      fBegin = Long.parseLong(finishBegin);
      if (fBegin < 0) {
        throw new BadRequestException("finishTimeBegin must be greater than 0");
      }
    }
    if (finishEnd != null && !finishEnd.isEmpty()) {
      checkEnd = true;
      fEnd = Long.parseLong(finishEnd);
      if (fEnd < 0) {
        throw new BadRequestException("finishTimeEnd must be greater than 0");
      }
    }
    if (fBegin > fEnd) {
      throw new BadRequestException(
          "finishTimeEnd must be greater than finishTimeBegin");
    }

    Set<String> appTypes = parseQueries(applicationTypes, false);
    if (!appTypes.isEmpty()) {
      checkAppTypes = true;
    }

    Set<String> appTags = parseQueries(applicationTags, false);
    if (!appTags.isEmpty()) {
      checkAppTags = true;
    }

    // stateQuery is deprecated.
    if (stateQuery != null && !stateQuery.isEmpty()) {
      statesQuery.add(stateQuery);
    }
    Set<String> appStates = parseQueries(statesQuery, true);
    if (!appStates.isEmpty()) {
      checkAppStates = true;
    }

    GetApplicationsRequest request = GetApplicationsRequest.newInstance();

    if (checkStart) {
      request.setStartRange(sBegin, sEnd);
    }

    if (checkEnd) {
      request.setFinishRange(fBegin, fEnd);
    }

    if (checkCount) {
      request.setLimit(countNum);
    }

    if (checkAppTypes) {
      request.setApplicationTypes(appTypes);
    }

    if (checkAppTags) {
      request.setApplicationTags(appTags);
    }

    if (checkAppStates) {
      request.setApplicationStates(appStates);
    }

    if (queueQuery != null && !queueQuery.isEmpty()) {
      ResourceScheduler rs = rm.getResourceScheduler();
      if (rs instanceof CapacityScheduler) {
        CapacityScheduler cs = (CapacityScheduler) rs;
        // validate queue exists
        try {
          cs.getQueueInfo(queueQuery, false, false);
        } catch (IOException e) {
          throw new BadRequestException(e.getMessage());
        }
      }
      Set<String> queues = new HashSet<String>(1);
      queues.add(queueQuery);
      request.setQueues(queues);
    }

    if (userQuery != null && !userQuery.isEmpty()) {
      Set<String> users = new HashSet<String>(1);
      users.add(userQuery);
      request.setUsers(users);
    }

    List<ApplicationReport> appReports = null;
    try {
      appReports = rm.getClientRMService().getApplications(request)
          .getApplicationList();
    } catch (YarnException e) {
      LOG.error("Unable to retrieve apps from ClientRMService", e);
      throw new YarnRuntimeException(
          "Unable to retrieve apps from ClientRMService", e);
    }

    final ConcurrentMap<ApplicationId, RMApp> apps =
        rm.getRMContext().getRMApps();
    AppsInfo allApps = new AppsInfo();
    for (ApplicationReport report : appReports) {
      RMApp rmapp = apps.get(report.getApplicationId());
      if (rmapp == null) {
        continue;
      }

      if (finalStatusQuery != null && !finalStatusQuery.isEmpty()) {
        FinalApplicationStatus.valueOf(finalStatusQuery);
        if (!rmapp.getFinalApplicationStatus().toString()
            .equalsIgnoreCase(finalStatusQuery)) {
          continue;
        }
      }

      DeSelectFields deSelectFields = new DeSelectFields();
      deSelectFields.initFields(unselectedFields);

      AppInfo app = new AppInfo(rm, rmapp, hasAccess(rmapp, hsr),
          WebAppUtils.getHttpSchemePrefix(conf), deSelectFields);
      allApps.add(app);
    }
    return allApps;
  }

  @GET
  @Path(RMWSConsts.SCHEDULER_ACTIVITIES)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ActivitiesInfo getActivities(@Context HttpServletRequest hsr,
      @QueryParam(RMWSConsts.NODEID) String nodeId) {
    YarnScheduler scheduler = rm.getRMContext().getScheduler();

    if (scheduler instanceof AbstractYarnScheduler) {
      String errMessage = "";

      AbstractYarnScheduler abstractYarnScheduler =
          (AbstractYarnScheduler) scheduler;

      ActivitiesManager activitiesManager =
          abstractYarnScheduler.getActivitiesManager();
      if (null == activitiesManager) {
        errMessage = "Not Capacity Scheduler";
        return new ActivitiesInfo(errMessage, nodeId);
      }

      List<FiCaSchedulerNode> nodeList =
          abstractYarnScheduler.getNodeTracker().getAllNodes();

      boolean illegalInput = false;

      if (nodeList.size() == 0) {
        illegalInput = true;
        errMessage = "No node manager running in the cluster";
      } else {
        if (nodeId != null) {
          String hostName = nodeId;
          String portName = "";
          if (nodeId.contains(":")) {
            int index = nodeId.indexOf(":");
            hostName = nodeId.substring(0, index);
            portName = nodeId.substring(index + 1);
          }

          boolean correctNodeId = false;
          for (FiCaSchedulerNode node : nodeList) {
            if ((portName.equals("")
                && node.getRMNode().getHostName().equals(hostName))
                || (!portName.equals("")
                    && node.getRMNode().getHostName().equals(hostName)
                    && String.valueOf(node.getRMNode().getCommandPort())
                        .equals(portName))) {
              correctNodeId = true;
              nodeId = node.getNodeID().toString();
              break;
            }
          }
          if (!correctNodeId) {
            illegalInput = true;
            errMessage = "Cannot find node manager with given node id";
          }
        }
      }

      if (!illegalInput) {
        activitiesManager.recordNextNodeUpdateActivities(nodeId);
        return activitiesManager.getActivitiesInfo(nodeId);
      }

      // Return a activities info with error message
      return new ActivitiesInfo(errMessage, nodeId);
    }

    return null;
  }

  @GET
  @Path(RMWSConsts.SCHEDULER_APP_ACTIVITIES)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppActivitiesInfo getAppActivities(@Context HttpServletRequest hsr,
      @QueryParam(RMWSConsts.APP_ID) String appId,
      @QueryParam(RMWSConsts.MAX_TIME) String time) {
    YarnScheduler scheduler = rm.getRMContext().getScheduler();

    if (scheduler instanceof AbstractYarnScheduler) {
      AbstractYarnScheduler abstractYarnScheduler =
          (AbstractYarnScheduler) scheduler;

      ActivitiesManager activitiesManager =
          abstractYarnScheduler.getActivitiesManager();
      if (null == activitiesManager) {
        String errMessage = "Not Capacity Scheduler";
        return new AppActivitiesInfo(errMessage, appId);
      }

      if (appId == null) {
        String errMessage = "Must provide an application Id";
        return new AppActivitiesInfo(errMessage, null);
      }

      double maxTime = 3.0;

      if (time != null) {
        if (time.contains(".")) {
          maxTime = Double.parseDouble(time);
        } else {
          maxTime = Double.parseDouble(time + ".0");
        }
      }

      ApplicationId applicationId;
      try {
        applicationId = ApplicationId.fromString(appId);
        activitiesManager.turnOnAppActivitiesRecording(applicationId, maxTime);
        AppActivitiesInfo appActivitiesInfo =
            activitiesManager.getAppActivitiesInfo(applicationId);

        return appActivitiesInfo;
      } catch (Exception e) {
        String errMessage = "Cannot find application with given appId";
        return new AppActivitiesInfo(errMessage, appId);
      }

    }
    return null;
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

    // parse the params and build the scoreboard
    // converting state/type name to lowercase
    Set<String> states = parseQueries(stateQueries, true);
    Set<String> types = parseQueries(typeQueries, false);
    // if no types, counts the applications of any types
    if (types.size() == 0) {
      types.add(RMWSConsts.ANY);
    } else if (types.size() != 1) {
      throw new BadRequestException("# of applicationTypes = " + types.size()
          + ", we temporarily support at most one applicationType");
    }
    // if no states, returns the counts of all RMAppStates
    if (states.size() == 0) {
      for (YarnApplicationState state : YarnApplicationState.values()) {
        states.add(StringUtils.toLowerCase(state.toString()));
      }
    }
    // in case we extend to multiple applicationTypes in the future
    Map<YarnApplicationState, Map<String, Long>> scoreboard =
        buildScoreboard(states, types);

    // go through the apps in RM to count the numbers, ignoring the case of
    // the state/type name
    ConcurrentMap<ApplicationId, RMApp> apps = rm.getRMContext().getRMApps();
    for (RMApp rmapp : apps.values()) {
      YarnApplicationState state = rmapp.createApplicationState();
      String type = StringUtils.toLowerCase(rmapp.getApplicationType().trim());
      if (states.contains(StringUtils.toLowerCase(state.toString()))) {
        if (types.contains(RMWSConsts.ANY)) {
          countApp(scoreboard, state, RMWSConsts.ANY);
        } else if (types.contains(type)) {
          countApp(scoreboard, state, type);
        }
      }
    }

    // fill the response object
    ApplicationStatisticsInfo appStatInfo = new ApplicationStatisticsInfo();
    for (Map.Entry<YarnApplicationState, Map<String, Long>> partScoreboard : scoreboard
        .entrySet()) {
      for (Map.Entry<String, Long> statEntry : partScoreboard.getValue()
          .entrySet()) {
        StatisticsItemInfo statItem = new StatisticsItemInfo(
            partScoreboard.getKey(), statEntry.getKey(), statEntry.getValue());
        appStatInfo.add(statItem);
      }
    }
    return appStatInfo;
  }

  private static Map<YarnApplicationState, Map<String, Long>> buildScoreboard(
      Set<String> states, Set<String> types) {
    Map<YarnApplicationState, Map<String, Long>> scoreboard =
        new HashMap<YarnApplicationState, Map<String, Long>>();
    // default states will result in enumerating all YarnApplicationStates
    assert !states.isEmpty();
    for (String state : states) {
      Map<String, Long> partScoreboard = new HashMap<String, Long>();
      scoreboard.put(
          YarnApplicationState.valueOf(StringUtils.toUpperCase(state)),
          partScoreboard);
      // types is verified no to be empty
      for (String type : types) {
        partScoreboard.put(type, 0L);
      }
    }
    return scoreboard;
  }

  private static void countApp(
      Map<YarnApplicationState, Map<String, Long>> scoreboard,
      YarnApplicationState state, String type) {
    Map<String, Long> partScoreboard = scoreboard.get(state);
    Long count = partScoreboard.get(type);
    partScoreboard.put(type, count + 1L);
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
    ApplicationId id = WebAppUtils.parseApplicationId(recordFactory, appId);
    RMApp app = rm.getRMContext().getRMApps().get(id);
    if (app == null) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    DeSelectFields deSelectFields = new DeSelectFields();
    deSelectFields.initFields(unselectedFields);

    return new AppInfo(rm, app, hasAccess(app, hsr), hsr.getScheme() + "://",
        deSelectFields);
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_APPATTEMPTS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppAttemptsInfo getAppAttempts(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) {

    init();
    ApplicationId id = WebAppUtils.parseApplicationId(recordFactory, appId);
    RMApp app = rm.getRMContext().getRMApps().get(id);
    if (app == null) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    AppAttemptsInfo appAttemptsInfo = new AppAttemptsInfo();
    for (RMAppAttempt attempt : app.getAppAttempts().values()) {
      AppAttemptInfo attemptInfo = new AppAttemptInfo(rm, attempt,
          app.getUser(), hsr.getScheme() + "://");
      appAttemptsInfo.add(attemptInfo);
    }

    return appAttemptsInfo;
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_APPATTEMPTS_APPATTEMPTID)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo getAppAttempt(
      @Context HttpServletRequest req, @Context HttpServletResponse res,
      @PathParam(RMWSConsts.APPID) String appId,
      @PathParam(RMWSConsts.APPATTEMPTID) String appAttemptId) {
    init(res);
    return super.getAppAttempt(req, res, appId, appAttemptId);
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_APPATTEMPTS_APPATTEMPTID_CONTAINERS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ContainersInfo getContainers(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(RMWSConsts.APPID) String appId,
      @PathParam(RMWSConsts.APPATTEMPTID) String appAttemptId) {
    init(res);
    return super.getContainers(req, res, appId, appAttemptId);
  }

  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}/containers/{containerid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ContainerInfo getContainer(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(RMWSConsts.APPID) String appId,
      @PathParam(RMWSConsts.APPATTEMPTID) String appAttemptId,
      @PathParam("containerid") String containerId) {
    init(res);
    return super.getContainer(req, res, appId, appAttemptId, containerId);
  }

  @GET
  @Path("/apps/{appid}/state")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppState getAppState(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException {
    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    String userName = "";
    if (callerUGI != null) {
      userName = callerUGI.getUserName();
    }
    RMApp app = null;
    try {
      app = getRMAppForAppId(appId);
    } catch (NotFoundException e) {
      RMAuditLogger.logFailure(userName, AuditConstants.GET_APP_STATE,
          "UNKNOWN", "RMWebService",
          "Trying to get state of an absent application " + appId);
      throw e;
    }

    AppState ret = new AppState();
    ret.setState(app.getState().toString());

    return ret;
  }

  // can't return POJO because we can't control the status code
  // it's always set to 200 when we need to allow it to be set
  // to 202

  @PUT
  @Path(RMWSConsts.APPS_APPID_STATE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public Response updateAppState(AppState targetState,
      @Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {

    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      String msg = "Unable to obtain user name, user not authenticated";
      throw new AuthorizationException(msg);
    }

    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      return Response.status(Status.FORBIDDEN).entity(msg).build();
    }

    String userName = callerUGI.getUserName();
    RMApp app = null;
    try {
      app = getRMAppForAppId(appId);
    } catch (NotFoundException e) {
      RMAuditLogger.logFailure(userName, AuditConstants.KILL_APP_REQUEST,
          "UNKNOWN", "RMWebService",
          "Trying to kill an absent application " + appId);
      throw e;
    }

    if (!app.getState().toString().equals(targetState.getState())) {
      // user is attempting to change state. right we only
      // allow users to kill the app

      if (targetState.getState()
          .equals(YarnApplicationState.KILLED.toString())) {
        return killApp(app, callerUGI, hsr, targetState.getDiagnostics());
      }
      throw new BadRequestException(
          "Only '" + YarnApplicationState.KILLED.toString()
              + "' is allowed as a target state.");
    }

    AppState ret = new AppState();
    ret.setState(app.getState().toString());

    return Response.status(Status.OK).entity(ret).build();
  }

  @GET
  @Path(RMWSConsts.GET_NODE_TO_LABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodeToLabelsInfo getNodeToLabels(@Context HttpServletRequest hsr)
      throws IOException {
    init();

    NodeToLabelsInfo ntl = new NodeToLabelsInfo();
    HashMap<String, NodeLabelsInfo> ntlMap = ntl.getNodeToLabels();
    Map<NodeId, Set<NodeLabel>> nodeIdToLabels =
        rm.getRMContext().getNodeLabelManager().getNodeLabelsInfo();

    for (Map.Entry<NodeId, Set<NodeLabel>> nitle : nodeIdToLabels.entrySet()) {
      List<NodeLabel> labels = new ArrayList<NodeLabel>(nitle.getValue());
      ntlMap.put(nitle.getKey().toString(), new NodeLabelsInfo(labels));
    }

    return ntl;
  }

  @GET
  @Path(RMWSConsts.LABEL_MAPPINGS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public LabelsToNodesInfo getLabelsToNodes(
      @QueryParam(RMWSConsts.LABELS) Set<String> labels) throws IOException {
    init();

    LabelsToNodesInfo lts = new LabelsToNodesInfo();
    Map<NodeLabelInfo, NodeIDsInfo> ltsMap = lts.getLabelsToNodes();
    Map<NodeLabel, Set<NodeId>> labelsToNodeId = null;
    if (labels == null || labels.size() == 0) {
      labelsToNodeId =
          rm.getRMContext().getNodeLabelManager().getLabelsInfoToNodes();
    } else {
      labelsToNodeId =
          rm.getRMContext().getNodeLabelManager().getLabelsInfoToNodes(labels);
    }

    for (Entry<NodeLabel, Set<NodeId>> entry : labelsToNodeId.entrySet()) {
      List<String> nodeIdStrList = new ArrayList<String>();
      for (NodeId nodeId : entry.getValue()) {
        nodeIdStrList.add(nodeId.toString());
      }
      ltsMap.put(new NodeLabelInfo(entry.getKey()),
          new NodeIDsInfo(nodeIdStrList));
    }
    return lts;
  }

  @POST
  @Path(RMWSConsts.REPLACE_NODE_TO_LABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response replaceLabelsOnNodes(
      final NodeToLabelsEntryList newNodeToLabels,
      @Context HttpServletRequest hsr) throws IOException {
    Map<NodeId, Set<String>> nodeIdToLabels =
        new HashMap<NodeId, Set<String>>();

    for (NodeToLabelsEntry nitle : newNodeToLabels.getNodeToLabels()) {
      nodeIdToLabels.put(
          ConverterUtils.toNodeIdWithDefaultPort(nitle.getNodeId()),
          new HashSet<String>(nitle.getNodeLabels()));
    }

    return replaceLabelsOnNode(nodeIdToLabels, hsr, "/replace-node-to-labels");
  }

  @POST
  @Path(RMWSConsts.NODES_NODEID_REPLACE_LABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response replaceLabelsOnNode(
      @QueryParam("labels") Set<String> newNodeLabelsName,
      @Context HttpServletRequest hsr, @PathParam("nodeId") String nodeId)
      throws Exception {
    NodeId nid = ConverterUtils.toNodeIdWithDefaultPort(nodeId);
    Map<NodeId, Set<String>> newLabelsForNode =
        new HashMap<NodeId, Set<String>>();
    newLabelsForNode.put(nid, new HashSet<String>(newNodeLabelsName));

    return replaceLabelsOnNode(newLabelsForNode, hsr,
        "/nodes/nodeid/replace-labels");
  }

  private Response replaceLabelsOnNode(
      Map<NodeId, Set<String>> newLabelsForNode, HttpServletRequest hsr,
      String operation) throws IOException {
    init();

    NodeLabelsUtils.verifyCentralizedNodeLabelConfEnabled("replaceLabelsOnNode",
        isCentralizedNodeLabelConfiguration);

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      String msg = "Unable to obtain user name, user not authenticated for"
          + " post to ..." + operation;
      throw new AuthorizationException(msg);
    }

    if (!rm.getRMContext().getNodeLabelManager().checkAccess(callerUGI)) {
      String msg = "User " + callerUGI.getShortUserName() + " not authorized"
          + " for post to ..." + operation;
      throw new AuthorizationException(msg);
    }
    try {
      rm.getRMContext().getNodeLabelManager()
          .replaceLabelsOnNode(newLabelsForNode);
    } catch (IOException e) {
      throw new BadRequestException(e);
    }

    return Response.status(Status.OK).build();
  }

  @GET
  @Path(RMWSConsts.GET_NODE_LABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodeLabelsInfo getClusterNodeLabels(@Context HttpServletRequest hsr)
      throws IOException {
    init();

    List<NodeLabel> nodeLabels =
        rm.getRMContext().getNodeLabelManager().getClusterNodeLabels();
    NodeLabelsInfo ret = new NodeLabelsInfo(nodeLabels);

    return ret;
  }

  @POST
  @Path(RMWSConsts.ADD_NODE_LABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response addToClusterNodeLabels(final NodeLabelsInfo newNodeLabels,
      @Context HttpServletRequest hsr) throws Exception {
    init();

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      String msg = "Unable to obtain user name, user not authenticated for"
          + " post to .../add-node-labels";
      throw new AuthorizationException(msg);
    }
    if (!rm.getRMContext().getNodeLabelManager().checkAccess(callerUGI)) {
      String msg = "User " + callerUGI.getShortUserName() + " not authorized"
          + " for post to .../add-node-labels ";
      throw new AuthorizationException(msg);
    }

    try {
      rm.getRMContext().getNodeLabelManager()
          .addToCluserNodeLabels(newNodeLabels.getNodeLabels());
    } catch (IOException e) {
      throw new BadRequestException(e);
    }

    return Response.status(Status.OK).build();

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

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      String msg = "Unable to obtain user name, user not authenticated for"
          + " post to .../remove-node-labels";
      throw new AuthorizationException(msg);
    }
    if (!rm.getRMContext().getNodeLabelManager().checkAccess(callerUGI)) {
      String msg = "User " + callerUGI.getShortUserName() + " not authorized"
          + " for post to .../remove-node-labels ";
      throw new AuthorizationException(msg);
    }

    try {
      rm.getRMContext().getNodeLabelManager()
          .removeFromClusterNodeLabels(new HashSet<String>(oldNodeLabels));
    } catch (IOException e) {
      throw new BadRequestException(e);
    }

    return Response.status(Status.OK).build();
  }

  @GET
  @Path(RMWSConsts.NODES_NODEID_GETLABELS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodeLabelsInfo getLabelsOnNode(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.NODEID) String nodeId) throws IOException {
    init();

    NodeId nid = ConverterUtils.toNodeIdWithDefaultPort(nodeId);
    List<NodeLabel> labels = new ArrayList<NodeLabel>(
        rm.getRMContext().getNodeLabelManager().getLabelsInfoByNode(nid));
    return new NodeLabelsInfo(labels);
  }

  protected Response killApp(RMApp app, UserGroupInformation callerUGI,
      HttpServletRequest hsr, String diagnostic)
      throws IOException, InterruptedException {

    if (app == null) {
      throw new IllegalArgumentException("app cannot be null");
    }
    String userName = callerUGI.getUserName();
    final ApplicationId appid = app.getApplicationId();
    KillApplicationResponse resp = null;
    try {
      resp = callerUGI
          .doAs(new PrivilegedExceptionAction<KillApplicationResponse>() {
            @Override
            public KillApplicationResponse run()
                throws IOException, YarnException {
              KillApplicationRequest req =
                  KillApplicationRequest.newInstance(appid);
              if (diagnostic != null) {
                req.setDiagnostics(diagnostic);
              }
              return rm.getClientRMService().forceKillApplication(req);
            }
          });
    } catch (UndeclaredThrowableException ue) {
      // if the root cause is a permissions issue
      // bubble that up to the user
      if (ue.getCause() instanceof YarnException) {
        YarnException ye = (YarnException) ue.getCause();
        if (ye.getCause() instanceof AccessControlException) {
          String appId = app.getApplicationId().toString();
          String msg = "Unauthorized attempt to kill appid " + appId
              + " by remote user " + userName;
          return Response.status(Status.FORBIDDEN).entity(msg).build();
        } else {
          throw ue;
        }
      } else {
        throw ue;
      }
    }

    AppState ret = new AppState();
    ret.setState(app.getState().toString());

    if (resp.getIsKillCompleted()) {
      RMAuditLogger.logSuccess(userName, AuditConstants.KILL_APP_REQUEST,
          "RMWebService", app.getApplicationId());
    } else {
      return Response.status(Status.ACCEPTED).entity(ret)
          .header(HttpHeaders.LOCATION, hsr.getRequestURL()).build();
    }
    return Response.status(Status.OK).entity(ret).build();
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_PRIORITY)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppPriority getAppPriority(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException {
    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    String userName = "UNKNOWN-USER";
    if (callerUGI != null) {
      userName = callerUGI.getUserName();
    }
    RMApp app = null;
    try {
      app = getRMAppForAppId(appId);
    } catch (NotFoundException e) {
      RMAuditLogger.logFailure(userName, AuditConstants.GET_APP_PRIORITY,
          "UNKNOWN", "RMWebService",
          "Trying to get priority of an absent application " + appId);
      throw e;
    }

    AppPriority ret = new AppPriority();
    ret.setPriority(app.getApplicationPriority().getPriority());

    return ret;
  }

  @PUT
  @Path(RMWSConsts.APPS_APPID_PRIORITY)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public Response updateApplicationPriority(AppPriority targetPriority,
      @Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    init();
    if (targetPriority == null) {
      throw new YarnException("Target Priority cannot be null");
    }

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      throw new AuthorizationException(
          "Unable to obtain user name, user not authenticated");
    }

    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      return Response.status(Status.FORBIDDEN)
          .entity("The default static user cannot carry out this operation.")
          .build();
    }

    String userName = callerUGI.getUserName();
    RMApp app = null;
    try {
      app = getRMAppForAppId(appId);
    } catch (NotFoundException e) {
      RMAuditLogger.logFailure(userName, AuditConstants.UPDATE_APP_PRIORITY,
          "UNKNOWN", "RMWebService",
          "Trying to update priority an absent application " + appId);
      throw e;
    }
    Priority priority = app.getApplicationPriority();
    if (priority == null
        || priority.getPriority() != targetPriority.getPriority()) {
      return modifyApplicationPriority(app, callerUGI,
          targetPriority.getPriority());
    }
    return Response.status(Status.OK).entity(targetPriority).build();
  }

  private Response modifyApplicationPriority(final RMApp app,
      UserGroupInformation callerUGI, final int appPriority)
      throws IOException, InterruptedException {
    String userName = callerUGI.getUserName();
    try {
      callerUGI.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException, YarnException {
          Priority priority = Priority.newInstance(appPriority);
          UpdateApplicationPriorityRequest request =
              UpdateApplicationPriorityRequest
                  .newInstance(app.getApplicationId(), priority);
          rm.getClientRMService().updateApplicationPriority(request);
          return null;
        }
      });
    } catch (UndeclaredThrowableException ue) {
      // if the root cause is a permissions issue
      // bubble that up to the user
      if (ue.getCause() instanceof YarnException) {
        YarnException ye = (YarnException) ue.getCause();
        if (ye.getCause() instanceof AccessControlException) {
          String appId = app.getApplicationId().toString();
          String msg = "Unauthorized attempt to change priority of appid "
              + appId + " by remote user " + userName;
          return Response.status(Status.FORBIDDEN).entity(msg).build();
        } else if (ye.getMessage().startsWith("Application in")
            && ye.getMessage().endsWith("state cannot be update priority.")) {
          return Response.status(Status.BAD_REQUEST).entity(ye.getMessage())
              .build();
        } else {
          throw ue;
        }
      } else {
        throw ue;
      }
    }
    AppPriority ret =
        new AppPriority(app.getApplicationPriority().getPriority());
    return Response.status(Status.OK).entity(ret).build();
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_QUEUE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppQueue getAppQueue(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException {
    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    String userName = "UNKNOWN-USER";
    if (callerUGI != null) {
      userName = callerUGI.getUserName();
    }
    RMApp app = null;
    try {
      app = getRMAppForAppId(appId);
    } catch (NotFoundException e) {
      RMAuditLogger.logFailure(userName, AuditConstants.GET_APP_QUEUE,
          "UNKNOWN", "RMWebService",
          "Trying to get queue of an absent application " + appId);
      throw e;
    }

    AppQueue ret = new AppQueue();
    ret.setQueue(app.getQueue());

    return ret;
  }

  @PUT
  @Path(RMWSConsts.APPS_APPID_QUEUE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public Response updateAppQueue(AppQueue targetQueue,
      @Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {

    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      String msg = "Unable to obtain user name, user not authenticated";
      throw new AuthorizationException(msg);
    }

    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      return Response.status(Status.FORBIDDEN).entity(msg).build();
    }

    String userName = callerUGI.getUserName();
    RMApp app = null;
    try {
      app = getRMAppForAppId(appId);
    } catch (NotFoundException e) {
      RMAuditLogger.logFailure(userName, AuditConstants.MOVE_APP_REQUEST,
          "UNKNOWN", "RMWebService",
          "Trying to move an absent application " + appId);
      throw e;
    }

    if (!app.getQueue().equals(targetQueue.getQueue())) {
      // user is attempting to change queue.
      return moveApp(app, callerUGI, targetQueue.getQueue());
    }

    AppQueue ret = new AppQueue();
    ret.setQueue(app.getQueue());

    return Response.status(Status.OK).entity(ret).build();
  }

  protected Response moveApp(RMApp app, UserGroupInformation callerUGI,
      String targetQueue) throws IOException, InterruptedException {

    if (app == null) {
      throw new IllegalArgumentException("app cannot be null");
    }
    String userName = callerUGI.getUserName();
    final ApplicationId appid = app.getApplicationId();
    final String reqTargetQueue = targetQueue;
    try {
      callerUGI.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException, YarnException {
          MoveApplicationAcrossQueuesRequest req =
              MoveApplicationAcrossQueuesRequest.newInstance(appid,
                  reqTargetQueue);
          rm.getClientRMService().moveApplicationAcrossQueues(req);
          return null;
        }
      });
    } catch (UndeclaredThrowableException ue) {
      // if the root cause is a permissions issue
      // bubble that up to the user
      if (ue.getCause() instanceof YarnException) {
        YarnException ye = (YarnException) ue.getCause();
        if (ye.getCause() instanceof AccessControlException) {
          String appId = app.getApplicationId().toString();
          String msg = "Unauthorized attempt to move appid " + appId
              + " by remote user " + userName;
          return Response.status(Status.FORBIDDEN).entity(msg).build();
        } else if (ye.getMessage().startsWith("App in")
            && ye.getMessage().endsWith("state cannot be moved.")) {
          return Response.status(Status.BAD_REQUEST).entity(ye.getMessage())
              .build();
        } else {
          throw ue;
        }
      } else {
        throw ue;
      }
    }

    AppQueue ret = new AppQueue();
    ret.setQueue(app.getQueue());
    return Response.status(Status.OK).entity(ret).build();
  }

  private RMApp getRMAppForAppId(String appId) {
    ApplicationId id = WebAppUtils.parseApplicationId(recordFactory, appId);
    RMApp app = rm.getRMContext().getRMApps().get(id);
    if (app == null) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }
    return app;
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

  private boolean isStaticUser(UserGroupInformation callerUGI) {
    String staticUser =
        conf.get(CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER,
            CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER);
    return staticUser.equals(callerUGI.getUserName());
  }

  @POST
  @Path(RMWSConsts.APPS_NEW_APPLICATION)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response createNewApplication(@Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      throw new AuthorizationException(
          "Unable to obtain user name, " + "user not authenticated");
    }
    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      return Response.status(Status.FORBIDDEN).entity(msg).build();
    }

    NewApplication appId = createNewApplication();
    return Response.status(Status.OK).entity(appId).build();

  }

  // reuse the code in ClientRMService to create new app
  // get the new app id and submit app
  // set location header with new app location
  @POST
  @Path(RMWSConsts.APPS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public Response submitApplication(ApplicationSubmissionContextInfo newApp,
      @Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {

    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      throw new AuthorizationException(
          "Unable to obtain user name, " + "user not authenticated");
    }

    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      return Response.status(Status.FORBIDDEN).entity(msg).build();
    }

    ApplicationSubmissionContext appContext =
        RMWebAppUtil.createAppSubmissionContext(newApp, conf);

    final SubmitApplicationRequest req =
        SubmitApplicationRequest.newInstance(appContext);

    try {
      callerUGI
          .doAs(new PrivilegedExceptionAction<SubmitApplicationResponse>() {
            @Override
            public SubmitApplicationResponse run()
                throws IOException, YarnException {
              return rm.getClientRMService().submitApplication(req);
            }
          });
    } catch (UndeclaredThrowableException ue) {
      if (ue.getCause() instanceof YarnException) {
        throw new BadRequestException(ue.getCause().getMessage());
      }
      LOG.info("Submit app request failed", ue);
      throw ue;
    }

    String url = hsr.getRequestURL() + "/" + newApp.getApplicationId();
    return Response.status(Status.ACCEPTED).header(HttpHeaders.LOCATION, url)
        .build();
  }

  /**
   * Function that actually creates the ApplicationId by calling the
   * ClientRMService
   * 
   * @return returns structure containing the app-id and maximum resource
   *         capabilities
   */
  private NewApplication createNewApplication() {
    GetNewApplicationRequest req =
        recordFactory.newRecordInstance(GetNewApplicationRequest.class);
    GetNewApplicationResponse resp;
    try {
      resp = rm.getClientRMService().getNewApplication(req);
    } catch (YarnException e) {
      String msg = "Unable to create new app from RM web service";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
    NewApplication appId =
        new NewApplication(resp.getApplicationId().toString(),
            new ResourceInfo(resp.getMaximumResourceCapability()));
    return appId;
  }

  private UserGroupInformation createKerberosUserGroupInformation(
      HttpServletRequest hsr) throws AuthorizationException, YarnException {

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      String msg = "Unable to obtain user name, user not authenticated";
      throw new AuthorizationException(msg);
    }

    String authType = hsr.getAuthType();
    if (!KerberosAuthenticationHandler.TYPE.equalsIgnoreCase(authType)) {
      String msg = "Delegation token operations can only be carried out on a "
          + "Kerberos authenticated channel. Expected auth type is "
          + KerberosAuthenticationHandler.TYPE + ", got type " + authType;
      throw new YarnException(msg);
    }
    if (hsr.getAttribute(
        DelegationTokenAuthenticationHandler.DELEGATION_TOKEN_UGI_ATTRIBUTE) != null) {
      String msg =
          "Delegation token operations cannot be carried out using delegation"
              + " token authentication.";
      throw new YarnException(msg);
    }

    callerUGI.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
    return callerUGI;
  }

  @POST
  @Path(RMWSConsts.DELEGATION_TOKEN)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public Response postDelegationToken(DelegationToken tokenData,
      @Context HttpServletRequest hsr) throws AuthorizationException,
      IOException, InterruptedException, Exception {

    init();
    UserGroupInformation callerUGI;
    try {
      callerUGI = createKerberosUserGroupInformation(hsr);
    } catch (YarnException ye) {
      return Response.status(Status.FORBIDDEN).entity(ye.getMessage()).build();
    }
    return createDelegationToken(tokenData, hsr, callerUGI);
  }

  @POST
  @Path(RMWSConsts.DELEGATION_TOKEN_EXPIRATION)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public Response postDelegationTokenExpiration(@Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {

    init();
    UserGroupInformation callerUGI;
    try {
      callerUGI = createKerberosUserGroupInformation(hsr);
    } catch (YarnException ye) {
      return Response.status(Status.FORBIDDEN).entity(ye.getMessage()).build();
    }

    DelegationToken requestToken = new DelegationToken();
    requestToken.setToken(extractToken(hsr).encodeToUrlString());
    return renewDelegationToken(requestToken, hsr, callerUGI);
  }

  private Response createDelegationToken(DelegationToken tokenData,
      HttpServletRequest hsr, UserGroupInformation callerUGI)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {

    final String renewer = tokenData.getRenewer();
    GetDelegationTokenResponse resp;
    try {
      resp = callerUGI
          .doAs(new PrivilegedExceptionAction<GetDelegationTokenResponse>() {
            @Override
            public GetDelegationTokenResponse run()
                throws IOException, YarnException {
              GetDelegationTokenRequest createReq =
                  GetDelegationTokenRequest.newInstance(renewer);
              return rm.getClientRMService().getDelegationToken(createReq);
            }
          });
    } catch (Exception e) {
      LOG.info("Create delegation token request failed", e);
      throw e;
    }

    Token<RMDelegationTokenIdentifier> tk =
        new Token<RMDelegationTokenIdentifier>(
            resp.getRMDelegationToken().getIdentifier().array(),
            resp.getRMDelegationToken().getPassword().array(),
            new Text(resp.getRMDelegationToken().getKind()),
            new Text(resp.getRMDelegationToken().getService()));
    RMDelegationTokenIdentifier identifier = tk.decodeIdentifier();
    long currentExpiration = rm.getRMContext()
        .getRMDelegationTokenSecretManager().getRenewDate(identifier);
    DelegationToken respToken = new DelegationToken(tk.encodeToUrlString(),
        renewer, identifier.getOwner().toString(), tk.getKind().toString(),
        currentExpiration, identifier.getMaxDate());
    return Response.status(Status.OK).entity(respToken).build();
  }

  private Response renewDelegationToken(DelegationToken tokenData,
      HttpServletRequest hsr, UserGroupInformation callerUGI)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {

    Token<RMDelegationTokenIdentifier> token =
        extractToken(tokenData.getToken());

    org.apache.hadoop.yarn.api.records.Token dToken = BuilderUtils
        .newDelegationToken(token.getIdentifier(), token.getKind().toString(),
            token.getPassword(), token.getService().toString());
    final RenewDelegationTokenRequest req =
        RenewDelegationTokenRequest.newInstance(dToken);

    RenewDelegationTokenResponse resp;
    try {
      resp = callerUGI
          .doAs(new PrivilegedExceptionAction<RenewDelegationTokenResponse>() {
            @Override
            public RenewDelegationTokenResponse run() throws YarnException {
              return rm.getClientRMService().renewDelegationToken(req);
            }
          });
    } catch (UndeclaredThrowableException ue) {
      if (ue.getCause() instanceof YarnException) {
        if (ue.getCause().getCause() instanceof InvalidToken) {
          throw new BadRequestException(ue.getCause().getCause().getMessage());
        } else if (ue.getCause()
            .getCause() instanceof org.apache.hadoop.security.AccessControlException) {
          return Response.status(Status.FORBIDDEN)
              .entity(ue.getCause().getCause().getMessage()).build();
        }
        LOG.info("Renew delegation token request failed", ue);
        throw ue;
      }
      LOG.info("Renew delegation token request failed", ue);
      throw ue;
    } catch (Exception e) {
      LOG.info("Renew delegation token request failed", e);
      throw e;
    }
    long renewTime = resp.getNextExpirationTime();

    DelegationToken respToken = new DelegationToken();
    respToken.setNextExpirationTime(renewTime);
    return Response.status(Status.OK).entity(respToken).build();
  }

  // For cancelling tokens, the encoded token is passed as a header
  // There are two reasons for this -
  // 1. Passing a request body as part of a DELETE request is not
  // allowed by Jetty
  // 2. Passing the encoded token as part of the url is not ideal
  // since urls tend to get logged and anyone with access to
  // the logs can extract tokens which are meant to be secret
  @DELETE
  @Path(RMWSConsts.DELEGATION_TOKEN)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response cancelDelegationToken(@Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException,
      Exception {

    init();
    UserGroupInformation callerUGI;
    try {
      callerUGI = createKerberosUserGroupInformation(hsr);
    } catch (YarnException ye) {
      return Response.status(Status.FORBIDDEN).entity(ye.getMessage()).build();
    }

    Token<RMDelegationTokenIdentifier> token = extractToken(hsr);

    org.apache.hadoop.yarn.api.records.Token dToken = BuilderUtils
        .newDelegationToken(token.getIdentifier(), token.getKind().toString(),
            token.getPassword(), token.getService().toString());
    final CancelDelegationTokenRequest req =
        CancelDelegationTokenRequest.newInstance(dToken);

    try {
      callerUGI
          .doAs(new PrivilegedExceptionAction<CancelDelegationTokenResponse>() {
            @Override
            public CancelDelegationTokenResponse run()
                throws IOException, YarnException {
              return rm.getClientRMService().cancelDelegationToken(req);
            }
          });
    } catch (UndeclaredThrowableException ue) {
      if (ue.getCause() instanceof YarnException) {
        if (ue.getCause().getCause() instanceof InvalidToken) {
          throw new BadRequestException(ue.getCause().getCause().getMessage());
        } else if (ue.getCause()
            .getCause() instanceof org.apache.hadoop.security.AccessControlException) {
          return Response.status(Status.FORBIDDEN)
              .entity(ue.getCause().getCause().getMessage()).build();
        }
        LOG.info("Renew delegation token request failed", ue);
        throw ue;
      }
      LOG.info("Renew delegation token request failed", ue);
      throw ue;
    } catch (Exception e) {
      LOG.info("Renew delegation token request failed", e);
      throw e;
    }

    return Response.status(Status.OK).build();
  }

  private Token<RMDelegationTokenIdentifier> extractToken(
      HttpServletRequest request) {
    String encodedToken = request.getHeader(DELEGATION_TOKEN_HEADER);
    if (encodedToken == null) {
      String msg = "Header '" + DELEGATION_TOKEN_HEADER
          + "' containing encoded token not found";
      throw new BadRequestException(msg);
    }
    return extractToken(encodedToken);
  }

  private Token<RMDelegationTokenIdentifier> extractToken(String encodedToken) {
    Token<RMDelegationTokenIdentifier> token =
        new Token<RMDelegationTokenIdentifier>();
    try {
      token.decodeFromUrlString(encodedToken);
    } catch (Exception ie) {
      String msg = "Could not decode encoded token";
      throw new BadRequestException(msg);
    }
    return token;
  }

  @POST
  @Path(RMWSConsts.RESERVATION_NEW)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response createNewReservation(@Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      throw new AuthorizationException(
          "Unable to obtain user name, " + "user not authenticated");
    }
    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      return Response.status(Status.FORBIDDEN).entity(msg).build();
    }

    NewReservation reservationId = createNewReservation();
    return Response.status(Status.OK).entity(reservationId).build();

  }

  /**
   * Function that actually creates the {@link ReservationId} by calling the
   * ClientRMService.
   *
   * @return returns structure containing the {@link ReservationId}
   * @throws IOException if creation fails.
   */
  private NewReservation createNewReservation() throws IOException {
    GetNewReservationRequest req =
        recordFactory.newRecordInstance(GetNewReservationRequest.class);
    GetNewReservationResponse resp;
    try {
      resp = rm.getClientRMService().getNewReservation(req);
    } catch (YarnException e) {
      String msg = "Unable to create new reservation from RM web service";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
    NewReservation reservationId =
        new NewReservation(resp.getReservationId().toString());
    return reservationId;
  }

  @POST
  @Path(RMWSConsts.RESERVATION_SUBMIT)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public Response submitReservation(ReservationSubmissionRequestInfo resContext,
      @Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {

    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      throw new AuthorizationException(
          "Unable to obtain user name, " + "user not authenticated");
    }
    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      return Response.status(Status.FORBIDDEN).entity(msg).build();
    }

    final ReservationSubmissionRequest reservation =
        createReservationSubmissionRequest(resContext);

    try {
      callerUGI
          .doAs(new PrivilegedExceptionAction<ReservationSubmissionResponse>() {
            @Override
            public ReservationSubmissionResponse run()
                throws IOException, YarnException {
              return rm.getClientRMService().submitReservation(reservation);
            }
          });
    } catch (UndeclaredThrowableException ue) {
      if (ue.getCause() instanceof YarnException) {
        throw new BadRequestException(ue.getCause().getMessage());
      }
      LOG.info("Submit reservation request failed", ue);
      throw ue;
    }

    return Response.status(Status.ACCEPTED).build();
  }

  private ReservationSubmissionRequest createReservationSubmissionRequest(
      ReservationSubmissionRequestInfo resContext) throws IOException {

    // defending against a couple of common submission format problems
    if (resContext == null) {
      throw new BadRequestException(
          "Input ReservationSubmissionContext should not be null");
    }
    ReservationDefinitionInfo resInfo = resContext.getReservationDefinition();
    if (resInfo == null) {
      throw new BadRequestException(
          "Input ReservationDefinition should not be null");
    }

    ReservationRequestsInfo resReqsInfo = resInfo.getReservationRequests();

    if (resReqsInfo == null || resReqsInfo.getReservationRequest() == null
        || resReqsInfo.getReservationRequest().size() == 0) {
      throw new BadRequestException("The ReservationDefinition should"
          + " contain at least one ReservationRequest");
    }

    ReservationRequestInterpreter[] values =
        ReservationRequestInterpreter.values();
    ReservationRequestInterpreter resInt =
        values[resReqsInfo.getReservationRequestsInterpreter()];
    List<ReservationRequest> list = new ArrayList<ReservationRequest>();

    for (ReservationRequestInfo resReqInfo : resReqsInfo
        .getReservationRequest()) {
      ResourceInfo rInfo = resReqInfo.getCapability();
      Resource capability =
          Resource.newInstance(rInfo.getMemorySize(), rInfo.getvCores());
      int numContainers = resReqInfo.getNumContainers();
      int minConcurrency = resReqInfo.getMinConcurrency();
      long duration = resReqInfo.getDuration();
      ReservationRequest rr = ReservationRequest.newInstance(capability,
          numContainers, minConcurrency, duration);
      list.add(rr);
    }
    ReservationRequests reqs = ReservationRequests.newInstance(list, resInt);
    ReservationDefinition rDef =
        ReservationDefinition.newInstance(resInfo.getArrival(),
            resInfo.getDeadline(), reqs, resInfo.getReservationName());

    ReservationId reservationId =
        ReservationId.parseReservationId(resContext.getReservationId());
    ReservationSubmissionRequest request = ReservationSubmissionRequest
        .newInstance(rDef, resContext.getQueue(), reservationId);

    return request;
  }

  @POST
  @Path(RMWSConsts.RESERVATION_UPDATE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public Response updateReservation(ReservationUpdateRequestInfo resContext,
      @Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {

    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      throw new AuthorizationException(
          "Unable to obtain user name, " + "user not authenticated");
    }
    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      return Response.status(Status.FORBIDDEN).entity(msg).build();
    }

    final ReservationUpdateRequest reservation =
        createReservationUpdateRequest(resContext);

    ReservationUpdateResponseInfo resRespInfo;
    try {
      resRespInfo = callerUGI
          .doAs(new PrivilegedExceptionAction<ReservationUpdateResponseInfo>() {
            @Override
            public ReservationUpdateResponseInfo run()
                throws IOException, YarnException {
              rm.getClientRMService().updateReservation(reservation);
              return new ReservationUpdateResponseInfo();
            }
          });
    } catch (UndeclaredThrowableException ue) {
      if (ue.getCause() instanceof YarnException) {
        throw new BadRequestException(ue.getCause().getMessage());
      }
      LOG.info("Update reservation request failed", ue);
      throw ue;
    }

    return Response.status(Status.OK).entity(resRespInfo).build();
  }

  private ReservationUpdateRequest createReservationUpdateRequest(
      ReservationUpdateRequestInfo resContext) throws IOException {

    // defending against a couple of common submission format problems
    if (resContext == null) {
      throw new BadRequestException(
          "Input ReservationSubmissionContext should not be null");
    }
    ReservationDefinitionInfo resInfo = resContext.getReservationDefinition();
    if (resInfo == null) {
      throw new BadRequestException(
          "Input ReservationDefinition should not be null");
    }
    ReservationRequestsInfo resReqsInfo = resInfo.getReservationRequests();
    if (resReqsInfo == null || resReqsInfo.getReservationRequest() == null
        || resReqsInfo.getReservationRequest().size() == 0) {
      throw new BadRequestException("The ReservationDefinition should"
          + " contain at least one ReservationRequest");
    }
    if (resContext.getReservationId() == null) {
      throw new BadRequestException(
          "Update operations must specify an existing ReservaitonId");
    }

    ReservationRequestInterpreter[] values =
        ReservationRequestInterpreter.values();
    ReservationRequestInterpreter resInt =
        values[resReqsInfo.getReservationRequestsInterpreter()];
    List<ReservationRequest> list = new ArrayList<ReservationRequest>();

    for (ReservationRequestInfo resReqInfo : resReqsInfo
        .getReservationRequest()) {
      ResourceInfo rInfo = resReqInfo.getCapability();
      Resource capability =
          Resource.newInstance(rInfo.getMemorySize(), rInfo.getvCores());
      int numContainers = resReqInfo.getNumContainers();
      int minConcurrency = resReqInfo.getMinConcurrency();
      long duration = resReqInfo.getDuration();
      ReservationRequest rr = ReservationRequest.newInstance(capability,
          numContainers, minConcurrency, duration);
      list.add(rr);
    }
    ReservationRequests reqs = ReservationRequests.newInstance(list, resInt);
    ReservationDefinition rDef =
        ReservationDefinition.newInstance(resInfo.getArrival(),
            resInfo.getDeadline(), reqs, resInfo.getReservationName());
    ReservationUpdateRequest request = ReservationUpdateRequest.newInstance(
        rDef, ReservationId.parseReservationId(resContext.getReservationId()));

    return request;
  }

  @POST
  @Path(RMWSConsts.RESERVATION_DELETE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public Response deleteReservation(ReservationDeleteRequestInfo resContext,
      @Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {

    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      throw new AuthorizationException(
          "Unable to obtain user name, " + "user not authenticated");
    }
    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      return Response.status(Status.FORBIDDEN).entity(msg).build();
    }

    final ReservationDeleteRequest reservation =
        createReservationDeleteRequest(resContext);

    ReservationDeleteResponseInfo resRespInfo;
    try {
      resRespInfo = callerUGI
          .doAs(new PrivilegedExceptionAction<ReservationDeleteResponseInfo>() {
            @Override
            public ReservationDeleteResponseInfo run()
                throws IOException, YarnException {
              rm.getClientRMService().deleteReservation(reservation);
              return new ReservationDeleteResponseInfo();
            }
          });
    } catch (UndeclaredThrowableException ue) {
      if (ue.getCause() instanceof YarnException) {
        throw new BadRequestException(ue.getCause().getMessage());
      }
      LOG.info("Update reservation request failed", ue);
      throw ue;
    }

    return Response.status(Status.OK).entity(resRespInfo).build();
  }

  private ReservationDeleteRequest createReservationDeleteRequest(
      ReservationDeleteRequestInfo resContext) throws IOException {

    ReservationDeleteRequest request = ReservationDeleteRequest.newInstance(
        ReservationId.parseReservationId(resContext.getReservationId()));

    return request;
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

    final ReservationListRequest request = ReservationListRequest.newInstance(
        queue, reservationId, startTime, endTime, includeResourceAllocations);

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      throw new AuthorizationException(
          "Unable to obtain user name, " + "user not authenticated");
    }
    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      return Response.status(Status.FORBIDDEN).entity(msg).build();
    }

    ReservationListResponse resRespInfo;
    try {
      resRespInfo = callerUGI
          .doAs(new PrivilegedExceptionAction<ReservationListResponse>() {
            @Override
            public ReservationListResponse run()
                throws IOException, YarnException {
              return rm.getClientRMService().listReservations(request);
            }
          });
    } catch (UndeclaredThrowableException ue) {
      if (ue.getCause() instanceof YarnException) {
        throw new BadRequestException(ue.getCause().getMessage());
      }
      LOG.info("List reservation request failed", ue);
      throw ue;
    }

    ReservationListInfo resResponse =
        new ReservationListInfo(resRespInfo, includeResourceAllocations);
    return Response.status(Status.OK).entity(resResponse).build();
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
    RMApp app = validateAppTimeoutRequest(hsr, appId);

    ApplicationTimeoutType appTimeoutType = parseTimeoutType(type);
    Long timeoutValue = app.getApplicationTimeouts().get(appTimeoutType);
    AppTimeoutInfo timeout =
        constructAppTimeoutDao(appTimeoutType, timeoutValue);
    return timeout;
  }

  private RMApp validateAppTimeoutRequest(HttpServletRequest hsr,
      String appId) {
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    String userName = "UNKNOWN-USER";
    if (callerUGI != null) {
      userName = callerUGI.getUserName();
    }

    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      RMAuditLogger.logFailure(userName, AuditConstants.GET_APP_TIMEOUTS,
          "UNKNOWN", "RMWebService", msg);
      throw new ForbiddenException(msg);
    }

    RMApp app = null;
    try {
      app = getRMAppForAppId(appId);
    } catch (NotFoundException e) {
      RMAuditLogger.logFailure(userName, AuditConstants.GET_APP_TIMEOUTS,
          "UNKNOWN", "RMWebService",
          "Trying to get timeouts of an absent application " + appId);
      throw e;
    }
    return app;
  }

  @GET
  @Path(RMWSConsts.APPS_TIMEOUTS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppTimeoutsInfo getAppTimeouts(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException {
    init();

    RMApp app = validateAppTimeoutRequest(hsr, appId);

    AppTimeoutsInfo timeouts = new AppTimeoutsInfo();
    Map<ApplicationTimeoutType, Long> applicationTimeouts =
        app.getApplicationTimeouts();
    if (applicationTimeouts.isEmpty()) {
      // If application is not set timeout, lifetime should be sent as default
      // with expiryTime=UNLIMITED and remainingTime=-1
      timeouts
          .add(constructAppTimeoutDao(ApplicationTimeoutType.LIFETIME, null));
    } else {
      for (Entry<ApplicationTimeoutType, Long> timeout : app
          .getApplicationTimeouts().entrySet()) {
        AppTimeoutInfo timeoutInfo =
            constructAppTimeoutDao(timeout.getKey(), timeout.getValue());
        timeouts.add(timeoutInfo);
      }
    }
    return timeouts;
  }

  private ApplicationTimeoutType parseTimeoutType(String type) {
    try {
      // enum string is in the uppercase
      return ApplicationTimeoutType
          .valueOf(StringUtils.toUpperCase(type.trim()));
    } catch (RuntimeException e) {
      ApplicationTimeoutType[] typeArray = ApplicationTimeoutType.values();
      String allAppTimeoutTypes = Arrays.toString(typeArray);
      throw new BadRequestException("Invalid application-state " + type.trim()
          + " specified. It should be one of " + allAppTimeoutTypes);
    }
  }

  private AppTimeoutInfo constructAppTimeoutDao(ApplicationTimeoutType type,
      Long timeoutInMillis) {
    AppTimeoutInfo timeout = new AppTimeoutInfo();
    timeout.setTimeoutType(type);
    if (timeoutInMillis != null) {
      timeout.setExpiryTime(Times.formatISO8601(timeoutInMillis.longValue()));
      timeout.setRemainingTime(
          Math.max((timeoutInMillis - System.currentTimeMillis()) / 1000, 0));
    }
    return timeout;
  }

  @PUT
  @Path(RMWSConsts.APPS_TIMEOUT)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public Response updateApplicationTimeout(AppTimeoutInfo appTimeout,
      @Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException,
      YarnException, InterruptedException, IOException {
    init();

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      throw new AuthorizationException(
          "Unable to obtain user name, user not authenticated");
    }

    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      return Response.status(Status.FORBIDDEN)
          .entity("The default static user cannot carry out this operation.")
          .build();
    }

    String userName = callerUGI.getUserName();
    RMApp app = null;
    try {
      app = getRMAppForAppId(appId);
    } catch (NotFoundException e) {
      RMAuditLogger.logFailure(userName, AuditConstants.UPDATE_APP_TIMEOUTS,
          "UNKNOWN", "RMWebService",
          "Trying to update timeout of an absent application " + appId);
      throw e;
    }

    return updateApplicationTimeouts(app, callerUGI, appTimeout);
  }

  private Response updateApplicationTimeouts(final RMApp app,
      UserGroupInformation callerUGI, final AppTimeoutInfo appTimeout)
      throws IOException, InterruptedException {
    if (appTimeout.getTimeoutType() == null
        || appTimeout.getExpireTime() == null) {
      return Response.status(Status.BAD_REQUEST)
          .entity("Timeout type or ExpiryTime is null.").build();
    }

    String userName = callerUGI.getUserName();
    try {
      callerUGI.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException, YarnException {
          UpdateApplicationTimeoutsRequest request =
              UpdateApplicationTimeoutsRequest
                  .newInstance(app.getApplicationId(), Collections.singletonMap(
                      appTimeout.getTimeoutType(), appTimeout.getExpireTime()));
          rm.getClientRMService().updateApplicationTimeouts(request);
          return null;
        }
      });
    } catch (UndeclaredThrowableException ue) {
      // if the root cause is a permissions issue
      // bubble that up to the user
      if (ue.getCause() instanceof YarnException) {
        YarnException ye = (YarnException) ue.getCause();
        if (ye.getCause() instanceof AccessControlException) {
          String appId = app.getApplicationId().toString();
          String msg = "Unauthorized attempt to change timeout of app " + appId
              + " by remote user " + userName;
          return Response.status(Status.FORBIDDEN).entity(msg).build();
        } else if (ye.getCause() instanceof ParseException) {
          return Response.status(Status.BAD_REQUEST).entity(ye.getMessage())
              .build();
        } else {
          throw ue;
        }
      } else {
        throw ue;
      }
    }
    AppTimeoutInfo timeout = constructAppTimeoutDao(appTimeout.getTimeoutType(),
        app.getApplicationTimeouts().get(appTimeout.getTimeoutType()));
    return Response.status(Status.OK).entity(timeout).build();
  }

  @Override
  protected ApplicationReport getApplicationReport(
      GetApplicationReportRequest request) throws YarnException, IOException {
    return rm.getClientRMService().getApplicationReport(request)
        .getApplicationReport();
  }

  @Override
  protected List<ApplicationReport> getApplicationsReport(
      final GetApplicationsRequest request) throws YarnException, IOException {
    return rm.getClientRMService().getApplications(request)
        .getApplicationList();
  }

  @Override
  protected ApplicationAttemptReport getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request)
      throws YarnException, IOException {
    return rm.getClientRMService().getApplicationAttemptReport(request)
        .getApplicationAttemptReport();
  }

  @Override
  protected List<ApplicationAttemptReport> getApplicationAttemptsReport(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    return rm.getClientRMService().getApplicationAttempts(request)
        .getApplicationAttemptList();
  }

  @Override
  protected ContainerReport getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    return rm.getClientRMService().getContainerReport(request)
        .getContainerReport();
  }

  @Override
  protected List<ContainerReport> getContainersReport(
      GetContainersRequest request) throws YarnException, IOException {
    return rm.getClientRMService().getContainers(request).getContainerList();
  }

  @PUT
  @Path("/scheduler-conf")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public synchronized Response updateSchedulerConfiguration(SchedConfUpdateInfo
      mutationInfo, @Context HttpServletRequest hsr)
      throws AuthorizationException, InterruptedException {
    init();

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    ApplicationACLsManager aclsManager = rm.getApplicationACLsManager();
    if (aclsManager.areACLsEnabled()) {
      if (callerUGI == null || !aclsManager.isAdmin(callerUGI)) {
        String msg = "Only admins can carry out this operation.";
        throw new ForbiddenException(msg);
      }
    }

    ResourceScheduler scheduler = rm.getResourceScheduler();
    if (scheduler instanceof MutableConfScheduler && ((MutableConfScheduler)
        scheduler).isConfigurationMutable()) {
      try {
        callerUGI.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            MutableConfigurationProvider provider = ((MutableConfScheduler)
                scheduler).getMutableConfProvider();
            if (!provider.getAclMutationPolicy().isMutationAllowed(callerUGI,
                mutationInfo)) {
              throw new org.apache.hadoop.security.AccessControlException("User"
                  + " is not admin of all modified queues.");
            }
            provider.logAndApplyMutation(callerUGI, mutationInfo);
            try {
              rm.getRMContext().getRMAdminService().refreshQueues();
            } catch (IOException | YarnException e) {
              provider.confirmPendingMutation(false);
              throw e;
            }
            provider.confirmPendingMutation(true);
            return null;
          }
        });
      } catch (IOException e) {
        LOG.error("Exception thrown when modifying configuration.", e);
        return Response.status(Status.BAD_REQUEST).entity(e.getMessage())
            .build();
      }
      return Response.status(Status.OK).entity("Configuration change " +
          "successfully applied.").build();
    } else {
      return Response.status(Status.BAD_REQUEST)
          .entity("Configuration change only supported by " +
              "MutableConfScheduler.")
          .build();
    }
  }
}
