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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

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

import org.apache.commons.lang3.EnumUtils;
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
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
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
import org.apache.hadoop.yarn.api.records.ContainerId;
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
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigValidator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
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
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;
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
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.RMQueueAclInfo;
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
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ConfigVersionInfo;
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
import org.apache.hadoop.yarn.webapp.dao.ConfInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@Path(RMWSConsts.RM_WEB_SERVICE_PATH)
public class RMWebServices extends WebServices implements RMWebServiceProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMWebServices.class.getName());

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
  public static final String DEFAULT_SUMMARIZE = "false";

  @VisibleForTesting
  boolean isCentralizedNodeLabelConfiguration = true;
  private boolean filterAppsByUser = false;
  private boolean filterInvalidXMLChars = false;
  private boolean enableRestAppSubmissions = true;

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
    this.filterAppsByUser  = conf.getBoolean(
        YarnConfiguration.FILTER_ENTITY_LIST_BY_USER,
        YarnConfiguration.DEFAULT_DISPLAY_APPS_FOR_LOGGED_IN_USER);
    this.filterInvalidXMLChars = conf.getBoolean(
        YarnConfiguration.FILTER_INVALID_XML_CHARS,
        YarnConfiguration.DEFAULT_FILTER_INVALID_XML_CHARS);
    this.enableRestAppSubmissions = conf.getBoolean(
        YarnConfiguration.ENABLE_REST_APP_SUBMISSIONS,
        YarnConfiguration.DEFAULT_ENABLE_REST_APP_SUBMISSIONS);
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

  /**
   * initForReadableEndpoints does the init for all readable REST end points.
   */
  private void initForReadableEndpoints() {
    // clear content type
    response.setContentType(null);
  }

  /**
   * initForWritableEndpoints does the init and acls verification for all
   * writable REST end points.
   *
   * @param callerUGI
   *          remote caller who initiated the request
   * @param doAdminACLsCheck
   *          boolean flag to indicate whether ACLs check is needed
   * @throws AuthorizationException
   *           in case of no access to perfom this op.
   */
  private void initForWritableEndpoints(UserGroupInformation callerUGI,
      boolean doAdminACLsCheck) throws AuthorizationException {
    // clear content type
    response.setContentType(null);

    if (callerUGI == null) {
      String msg = "Unable to obtain user name, user not authenticated";
      throw new AuthorizationException(msg);
    }

    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      throw new ForbiddenException(msg);
    }

    if (doAdminACLsCheck) {
      ApplicationACLsManager aclsManager = rm.getApplicationACLsManager();
      if (aclsManager.areACLsEnabled()) {
        if (!aclsManager.isAdmin(callerUGI)) {
          String msg = "Only admins can carry out this operation.";
          throw new ForbiddenException(msg);
        }
      }
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
    initForReadableEndpoints();
    return new ClusterInfo(this.rm);
  }

  @GET
  @Path(RMWSConsts.CLUSTER_USER_INFO)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ClusterUserInfo getClusterUserInfo(@Context HttpServletRequest hsr) {
    initForReadableEndpoints();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    return new ClusterUserInfo(this.rm, callerUGI);
  }

  @GET
  @Path(RMWSConsts.METRICS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ClusterMetricsInfo getClusterMetricsInfo() {
    initForReadableEndpoints();
    return new ClusterMetricsInfo(this.rm);
  }

  @GET
  @Path(RMWSConsts.SCHEDULER)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public SchedulerTypeInfo getSchedulerInfo() {
    initForReadableEndpoints();

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
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, true);

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
    initForReadableEndpoints();

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
      if (rmNode.getState().isInactiveState()) {
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
    initForReadableEndpoints();

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

  @POST
  @Path(RMWSConsts.NODE_RESOURCE)
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public ResourceInfo updateNodeResource(
      @Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.NODEID) String nodeId,
      ResourceOptionInfo resourceOption) throws AuthorizationException {

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

    RMNode rmNode = getRMNode(nodeId);
    Map<NodeId, ResourceOption> nodeResourceMap =
        Collections.singletonMap(
            rmNode.getNodeID(), resourceOption.getResourceOption());
    UpdateNodeResourceRequest updateRequest =
        UpdateNodeResourceRequest.newInstance(nodeResourceMap);

    try {
      RMContext rmContext = this.rm.getRMContext();
      AdminService admin = rmContext.getRMAdminService();
      admin.updateNodeResource(updateRequest);
    } catch (YarnException e) {
      String message = "Failed to update the node resource " +
          rmNode.getNodeID() + ".";
      LOG.error(message, e);
      throw new YarnRuntimeException(message, e);
    } catch (IOException e) {
      LOG.error("Failed to update the node resource {}.",
          rmNode.getNodeID(), e);
    }

    return new ResourceInfo(rmNode.getTotalCapability());
  }

  /**
   * Get the RMNode in the RM from the node identifier.
   * @param nodeId Node identifier.
   * @return The RMNode in the RM.
   */
  private RMNode getRMNode(final String nodeId) {
    if (nodeId == null || nodeId.isEmpty()) {
      throw new NotFoundException("nodeId, " + nodeId + ", is empty or null");
    }
    NodeId nid = NodeId.fromString(nodeId);
    RMContext rmContext = this.rm.getRMContext();
    RMNode ni = rmContext.getRMNodes().get(nid);
    if (ni == null) {
      ni = rmContext.getInactiveRMNodes().get(nid);
      if (ni == null) {
        throw new NotFoundException("nodeId, " + nodeId + ", is not found");
      }
    }
    return ni;
  }

  /**
   * This method ensures that the output String has only
   * valid XML unicode characters as specified by the
   * XML 1.0 standard. For reference, please see
   * <a href="http://www.w3.org/TR/2000/REC-xml-20001006#NT-Char">
   * the standard</a>.
   *
   * @param str The String whose invalid xml characters we want to escape.
   * @return The str String after escaping invalid xml characters.
   */
  public static String escapeInvalidXMLCharacters(String str) {
    StringBuffer out = new StringBuffer();
    final int strlen = str.length();
    final String substitute = "\uFFFD";
    int idx = 0;
    while (idx < strlen) {
      final int cpt = str.codePointAt(idx);
      idx += Character.isSupplementaryCodePoint(cpt) ? 2 : 1;
      if ((cpt == 0x9) ||
          (cpt == 0xA) ||
          (cpt == 0xD) ||
          ((cpt >= 0x20) && (cpt <= 0xD7FF)) ||
          ((cpt >= 0xE000) && (cpt <= 0xFFFD)) ||
          ((cpt >= 0x10000) && (cpt <= 0x10FFFF))) {
        out.append(Character.toChars(cpt));
      } else {
        out.append(substitute);
      }
    }
    return out.toString();
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
      @QueryParam(RMWSConsts.LIMIT) String limit,
      @QueryParam(RMWSConsts.STARTED_TIME_BEGIN) String startedBegin,
      @QueryParam(RMWSConsts.STARTED_TIME_END) String startedEnd,
      @QueryParam(RMWSConsts.FINISHED_TIME_BEGIN) String finishBegin,
      @QueryParam(RMWSConsts.FINISHED_TIME_END) String finishEnd,
      @QueryParam(RMWSConsts.APPLICATION_TYPES) Set<String> applicationTypes,
      @QueryParam(RMWSConsts.APPLICATION_TAGS) Set<String> applicationTags,
      @QueryParam(RMWSConsts.NAME) String name,
      @QueryParam(RMWSConsts.DESELECTS) Set<String> unselectedFields) {

    initForReadableEndpoints();

    GetApplicationsRequest request =
            ApplicationsRequestBuilder.create()
                    .withStateQuery(stateQuery)
                    .withStatesQuery(statesQuery)
                    .withUserQuery(userQuery)
                    .withQueueQuery(rm, queueQuery)
                    .withLimit(limit)
                    .withStartedTimeBegin(startedBegin)
                    .withStartedTimeEnd(startedEnd)
                    .withFinishTimeBegin(finishBegin)
                    .withFinishTimeEnd(finishEnd)
                    .withApplicationTypes(applicationTypes)
                    .withApplicationTags(applicationTags)
                    .withName(name)
            .build();

    List<ApplicationReport> appReports;
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

      boolean allowAccess = hasAccess(rmapp, hsr);
      // Given RM is configured to display apps per user, skip apps to which
      // this caller doesn't have access to view.
      if (filterAppsByUser && !allowAccess) {
        continue;
      }

      AppInfo app = new AppInfo(rm, rmapp, allowAccess,
          WebAppUtils.getHttpSchemePrefix(conf), deSelectFields);
      allApps.add(app);
    }

    if (filterInvalidXMLChars) {
      final String format = hsr.getHeader(HttpHeaders.ACCEPT);
      if (format != null &&
          format.toLowerCase().contains(MediaType.APPLICATION_XML)) {
        for (AppInfo appInfo : allApps.getApps()) {
          appInfo.setNote(escapeInvalidXMLCharacters(appInfo.getNote()));
        }
      }
    }

    return allApps;
  }

  @GET
  @Path(RMWSConsts.SCHEDULER_ACTIVITIES)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ActivitiesInfo getActivities(@Context HttpServletRequest hsr,
      @QueryParam(RMWSConsts.NODEID) String nodeId,
      @QueryParam(RMWSConsts.GROUP_BY) String groupBy) {
    initForReadableEndpoints();

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

      RMWSConsts.ActivitiesGroupBy activitiesGroupBy;
      try {
        activitiesGroupBy = parseActivitiesGroupBy(groupBy);
      } catch (IllegalArgumentException e) {
        return new ActivitiesInfo(e.getMessage(), nodeId);
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
        return activitiesManager.getActivitiesInfo(nodeId, activitiesGroupBy);
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
      @PathParam(RMWSConsts.APPID) String appId,
      @QueryParam(RMWSConsts.MAX_TIME) String time,
      @QueryParam(RMWSConsts.REQUEST_PRIORITIES) Set<String> requestPriorities,
      @QueryParam(RMWSConsts.ALLOCATION_REQUEST_IDS)
          Set<String> allocationRequestIds,
      @QueryParam(RMWSConsts.GROUP_BY) String groupBy,
      @QueryParam(RMWSConsts.LIMIT) String limit,
      @QueryParam(RMWSConsts.ACTIONS) Set<String> actions,
      @QueryParam(RMWSConsts.SUMMARIZE) @DefaultValue(DEFAULT_SUMMARIZE)
          boolean summarize) {
    initForReadableEndpoints();

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

      RMWSConsts.ActivitiesGroupBy activitiesGroupBy;
      try {
        activitiesGroupBy = parseActivitiesGroupBy(groupBy);
      } catch (IllegalArgumentException e) {
        return new AppActivitiesInfo(e.getMessage(), appId);
      }

      Set<RMWSConsts.AppActivitiesRequiredAction> requiredActions;
      try {
        requiredActions =
            parseAppActivitiesRequiredActions(getFlatSet(actions));
      } catch (IllegalArgumentException e) {
        return new AppActivitiesInfo(e.getMessage(), appId);
      }

      Set<Integer> parsedRequestPriorities;
      try {
        parsedRequestPriorities = getFlatSet(requestPriorities).stream()
            .map(e -> Integer.valueOf(e)).collect(Collectors.toSet());
      } catch (NumberFormatException e) {
        return new AppActivitiesInfo("request priorities must be integers!",
            appId);
      }
      Set<Long> parsedAllocationRequestIds;
      try {
        parsedAllocationRequestIds = getFlatSet(allocationRequestIds).stream()
            .map(e -> Long.valueOf(e)).collect(Collectors.toSet());
      } catch (NumberFormatException e) {
        return new AppActivitiesInfo(
            "allocation request Ids must be integers!", appId);
      }

      int limitNum = -1;
      if (limit != null) {
        try {
          limitNum = Integer.parseInt(limit);
          if (limitNum <= 0) {
            return new AppActivitiesInfo(
                "limit must be greater than 0!", appId);
          }
        } catch (NumberFormatException e) {
          return new AppActivitiesInfo("limit must be integer!", appId);
        }
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
        if (requiredActions
            .contains(RMWSConsts.AppActivitiesRequiredAction.REFRESH)) {
          activitiesManager
              .turnOnAppActivitiesRecording(applicationId, maxTime);
        }
        if (requiredActions
            .contains(RMWSConsts.AppActivitiesRequiredAction.GET)) {
          AppActivitiesInfo appActivitiesInfo = activitiesManager
              .getAppActivitiesInfo(applicationId, parsedRequestPriorities,
                  parsedAllocationRequestIds, activitiesGroupBy, limitNum,
                  summarize, maxTime);
          return appActivitiesInfo;
        }
        return new AppActivitiesInfo("Successfully received "
            + (actions.size() == 1 ? "action: " : "actions: ")
            + StringUtils.join(',', actions), appId);
      } catch (Exception e) {
        String errMessage = "Cannot find application with given appId";
        LOG.error(errMessage, e);
        return new AppActivitiesInfo(errMessage, appId);
      }

    }
    return null;
  }

  private Set<String> getFlatSet(Set<String> set) {
    if (set == null) {
      return null;
    }
    return set.stream()
        .flatMap(e -> Arrays.asList(e.split(StringUtils.COMMA_STR)).stream())
        .collect(Collectors.toSet());
  }

  private Set<RMWSConsts.AppActivitiesRequiredAction>
      parseAppActivitiesRequiredActions(Set<String> actions) {
    Set<RMWSConsts.AppActivitiesRequiredAction> requiredActions =
        new HashSet<>();
    if (actions == null || actions.isEmpty()) {
      requiredActions.add(RMWSConsts.AppActivitiesRequiredAction.REFRESH);
      requiredActions.add(RMWSConsts.AppActivitiesRequiredAction.GET);
    } else {
      for (String action : actions) {
        if (!EnumUtils.isValidEnum(RMWSConsts.AppActivitiesRequiredAction.class,
            action.toUpperCase())) {
          String errMesasge =
              "Got invalid action: " + action + ", valid actions: " + Arrays
                  .asList(RMWSConsts.AppActivitiesRequiredAction.values());
          throw new IllegalArgumentException(errMesasge);
        }
        requiredActions.add(RMWSConsts.AppActivitiesRequiredAction
            .valueOf(action.toUpperCase()));
      }
    }
    return requiredActions;
  }

  private RMWSConsts.ActivitiesGroupBy parseActivitiesGroupBy(String groupBy) {
    if (groupBy != null) {
      if (!EnumUtils.isValidEnum(RMWSConsts.ActivitiesGroupBy.class,
          groupBy.toUpperCase())) {
        String errMesasge =
            "Got invalid groupBy: " + groupBy + ", valid groupBy types: "
                + Arrays.asList(RMWSConsts.ActivitiesGroupBy.values());
        throw new IllegalArgumentException(errMesasge);
      }
      return RMWSConsts.ActivitiesGroupBy.valueOf(groupBy.toUpperCase());
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
    initForReadableEndpoints();

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
    initForReadableEndpoints();

    ApplicationId id = WebAppUtils.parseApplicationId(recordFactory, appId);
    RMApp app = rm.getRMContext().getRMApps().get(id);
    if (app == null) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    DeSelectFields deSelectFields = new DeSelectFields();
    deSelectFields.initFields(unselectedFields);

    AppInfo appInfo =  new AppInfo(rm, app, hasAccess(app, hsr),
        hsr.getScheme() + "://", deSelectFields);

    if (filterInvalidXMLChars) {
      final String format = hsr.getHeader(HttpHeaders.ACCEPT);
      if (format != null &&
          format.toLowerCase().contains(MediaType.APPLICATION_XML)) {
        appInfo.setNote(escapeInvalidXMLCharacters(appInfo.getNote()));
      }
    }

    return appInfo;
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_APPATTEMPTS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppAttemptsInfo getAppAttempts(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) {
    initForReadableEndpoints();

    ApplicationId id = WebAppUtils.parseApplicationId(recordFactory, appId);
    RMApp app = rm.getRMContext().getRMApps().get(id);
    if (app == null) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    AppAttemptsInfo appAttemptsInfo = new AppAttemptsInfo();
    for (RMAppAttempt attempt : app.getAppAttempts().values()) {
      AppAttemptInfo attemptInfo = new AppAttemptInfo(rm, attempt,
          hasAccess(app, hsr), app.getUser(), hsr.getScheme() + "://");
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
    initForReadableEndpoints(res);
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
    initForReadableEndpoints(res);
    return super.getContainers(req, res, appId, appAttemptId);
  }

  @GET
  @Path(RMWSConsts.GET_CONTAINER)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ContainerInfo getContainer(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(RMWSConsts.APPID) String appId,
      @PathParam(RMWSConsts.APPATTEMPTID) String appAttemptId,
      @PathParam("containerid") String containerId) {
    initForReadableEndpoints(res);
    return super.getContainer(req, res, appId, appAttemptId, containerId);
  }

  @GET
  @Path(RMWSConsts.APPS_APPID_STATE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppState getAppState(@Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.APPID) String appId) throws AuthorizationException {
    initForReadableEndpoints();

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
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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
    initForReadableEndpoints();

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
    initForReadableEndpoints();

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
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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
    initForReadableEndpoints();

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
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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
    initForReadableEndpoints();

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
    initForReadableEndpoints();

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
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

    if (targetPriority == null) {
      throw new YarnException("Target Priority cannot be null");
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
    initForReadableEndpoints();

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

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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
    if (!enableRestAppSubmissions) {
      String msg = "App submission via REST is disabled.";
      return Response.status(Status.FORBIDDEN).entity(msg).build();
    }
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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
    if (!enableRestAppSubmissions) {
      String msg = "App submission via REST is disabled.";
      return Response.status(Status.FORBIDDEN).entity(msg).build();
    }

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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

  private void createKerberosUserGroupInformation(HttpServletRequest hsr,
      UserGroupInformation callerUGI)
      throws AuthorizationException, YarnException {

    String authType = hsr.getAuthType();
    if (!KerberosAuthenticationHandler.TYPE.equalsIgnoreCase(authType)) {
      String msg = "Delegation token operations can only be carried out on a "
          + "Kerberos authenticated channel. Expected auth type is "
          + KerberosAuthenticationHandler.TYPE + ", got type " + authType;
      throw new YarnException(msg);
    }
    if (hsr.getAttribute(
        DelegationTokenAuthenticationHandler.DELEGATION_TOKEN_UGI_ATTRIBUTE) != null) {
      String msg = "Delegation token operations cannot be carried out using "
          + "delegation token authentication.";
      throw new YarnException(msg);
    }
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

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

    try {
      createKerberosUserGroupInformation(hsr, callerUGI);
      callerUGI.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
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

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

    try {
      createKerberosUserGroupInformation(hsr, callerUGI);
      callerUGI.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
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

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

    try {
      createKerberosUserGroupInformation(hsr, callerUGI);
      callerUGI.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
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
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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
    ReservationDefinition rDef = ReservationDefinition.newInstance(
        resInfo.getArrival(), resInfo.getDeadline(), reqs,
        resInfo.getReservationName(), resInfo.getRecurrenceExpression(),
        Priority.newInstance(resInfo.getPriority()));

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

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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
    ReservationDefinition rDef = ReservationDefinition.newInstance(
        resInfo.getArrival(), resInfo.getDeadline(), reqs,
        resInfo.getReservationName(), resInfo.getRecurrenceExpression(),
        Priority.newInstance(resInfo.getPriority()));
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

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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
    initForReadableEndpoints();

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
    initForReadableEndpoints();
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
    initForReadableEndpoints();

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

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);

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

  @GET
  @Path(RMWSConsts.FORMAT_SCHEDULER_CONF)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
       MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public Response formatSchedulerConfiguration(@Context HttpServletRequest hsr)
      throws AuthorizationException {
    // Only admin user allowed to format scheduler conf in configuration store
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, true);

    ResourceScheduler scheduler = rm.getResourceScheduler();
    if (scheduler instanceof MutableConfScheduler
        && ((MutableConfScheduler) scheduler).isConfigurationMutable()) {
      try {
        MutableConfigurationProvider mutableConfigurationProvider =
            ((MutableConfScheduler) scheduler).getMutableConfProvider();
        mutableConfigurationProvider.formatConfigurationInStore(conf);
        try {
          rm.getRMContext().getRMAdminService().refreshQueues();
        } catch (IOException | YarnException e) {
          LOG.error("Exception thrown when formatting configuration.", e);
          mutableConfigurationProvider.revertToOldConfig(conf);
          throw e;
        }
        return Response.status(Status.OK).entity("Configuration under " +
            "store successfully formatted.").build();
      } catch (Exception e) {
        LOG.error("Exception thrown when formating configuration", e);
        return Response.status(Status.BAD_REQUEST).entity(e.getMessage())
            .build();
      }
    } else {
      return Response.status(Status.BAD_REQUEST)
          .entity("Scheduler Configuration format only supported by " +
          "MutableConfScheduler.").build();
    }
  }

  @POST
  @Path(RMWSConsts.SCHEDULER_CONF_VALIDATE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public synchronized Response validateAndGetSchedulerConfiguration(
          SchedConfUpdateInfo mutationInfo,
          @Context HttpServletRequest hsr) throws AuthorizationException {
    // Only admin user is allowed to read scheduler conf,
    // in order to avoid leaking sensitive info, such as ACLs
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, true);
    ResourceScheduler scheduler = rm.getResourceScheduler();
    if (scheduler instanceof MutableConfScheduler && ((MutableConfScheduler)
            scheduler).isConfigurationMutable()) {
      try {
        MutableConfigurationProvider mutableConfigurationProvider =
                ((MutableConfScheduler) scheduler).getMutableConfProvider();
        Configuration schedulerConf = mutableConfigurationProvider
                .getConfiguration();
        Configuration newSchedulerConf = mutableConfigurationProvider
                .applyChanges(schedulerConf, mutationInfo);
        Configuration yarnConf = ((CapacityScheduler) scheduler).getConf();

        Configuration newConfig = new Configuration(yarnConf);
        Iterator<Map.Entry<String, String>> iter = newSchedulerConf.iterator();
        Entry<String, String> e = null;
        while (iter.hasNext()) {
          e = iter.next();
          newConfig.set(e.getKey(), e.getValue());
        }
        CapacitySchedulerConfigValidator.validateCSConfiguration(yarnConf,
                newConfig, rm.getRMContext());

        return Response.status(Status.OK)
                .entity(new ConfInfo(newSchedulerConf))
                .build();
      } catch (Exception e) {
        String errorMsg = "CapacityScheduler configuration validation failed:"
                  + e.toString();
        LOG.warn(errorMsg);
        return Response.status(Status.BAD_REQUEST)
                  .entity(errorMsg)
                  .build();
      }
    } else {
      String errorMsg = "Configuration change validation only supported by " +
              "MutableConfScheduler.";
      LOG.warn(errorMsg);
      return Response.status(Status.BAD_REQUEST)
              .entity(errorMsg)
              .build();
    }
  }

  @PUT
  @Path(RMWSConsts.SCHEDULER_CONF)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public synchronized Response updateSchedulerConfiguration(SchedConfUpdateInfo
      mutationInfo, @Context HttpServletRequest hsr)
      throws AuthorizationException, InterruptedException {

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, true);

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
            LogMutation logMutation = provider.logAndApplyMutation(callerUGI,
                mutationInfo);
            try {
              rm.getRMContext().getRMAdminService().refreshQueues();
            } catch (IOException | YarnException e) {
              provider.confirmPendingMutation(logMutation, false);
              throw e;
            }
            provider.confirmPendingMutation(logMutation, true);
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

  @GET
  @Path(RMWSConsts.SCHEDULER_CONF)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public Response getSchedulerConfiguration(@Context HttpServletRequest hsr)
      throws AuthorizationException {
    // Only admin user is allowed to read scheduler conf,
    // in order to avoid leaking sensitive info, such as ACLs
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, true);

    ResourceScheduler scheduler = rm.getResourceScheduler();
    if (scheduler instanceof MutableConfScheduler
        && ((MutableConfScheduler) scheduler).isConfigurationMutable()) {
      MutableConfigurationProvider mutableConfigurationProvider =
          ((MutableConfScheduler) scheduler).getMutableConfProvider();
      // We load the cached configuration from configuration store,
      // this should be the conf properties used by the scheduler.
      Configuration schedulerConf = mutableConfigurationProvider
          .getConfiguration();
      return Response.status(Status.OK)
          .entity(new ConfInfo(schedulerConf))
          .build();
    } else {
      return Response.status(Status.BAD_REQUEST).entity(
          "This API only supports to retrieve scheduler configuration"
              + " from a mutable-conf scheduler, underneath scheduler "
              + scheduler.getClass().getSimpleName()
              + " is not an instance of MutableConfScheduler")
          .build();
    }
  }

  @GET
  @Path(RMWSConsts.SCHEDULER_CONF_VERSION)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
       MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public Response getSchedulerConfigurationVersion(@Context
      HttpServletRequest hsr) throws AuthorizationException {
    // Only admin user is allowed to get scheduler conf version
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, true);

    ResourceScheduler scheduler = rm.getResourceScheduler();
    if (scheduler instanceof MutableConfScheduler
        && ((MutableConfScheduler) scheduler).isConfigurationMutable()) {
      MutableConfigurationProvider mutableConfigurationProvider =
          ((MutableConfScheduler) scheduler).getMutableConfProvider();

      try {
        long configVersion = mutableConfigurationProvider
            .getConfigVersion();
        return Response.status(Status.OK)
            .entity(new ConfigVersionInfo(configVersion)).build();
      } catch (Exception e) {
        LOG.error("Exception thrown when fetching configuration version.", e);
        return Response.status(Status.BAD_REQUEST).entity(e.getMessage())
            .build();
      }
    } else {
      return Response.status(Status.BAD_REQUEST)
          .entity("Configuration Version only supported by "
          + "MutableConfScheduler.").build();
    }
  }

  @GET
  @Path(RMWSConsts.CHECK_USER_ACCESS_TO_QUEUE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
                MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public RMQueueAclInfo checkUserAccessToQueue(
      @PathParam(RMWSConsts.QUEUE) String queue,
      @QueryParam(RMWSConsts.USER) String username,
      @QueryParam(RMWSConsts.QUEUE_ACL_TYPE)
        @DefaultValue("SUBMIT_APPLICATIONS") String queueAclType,
      @Context HttpServletRequest hsr) throws AuthorizationException {
    initForReadableEndpoints();

    // For the user who invokes this REST call, he/she should have admin access
    // to the queue. Otherwise we will reject the call.
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI != null && !this.rm.getResourceScheduler().checkAccess(
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

    if (!this.rm.getResourceScheduler().checkAccess(user, queueACL, queue)) {
      return new RMQueueAclInfo(false, user.getUserName(),
          "User=" + username + " doesn't have access to queue=" + queue
              + " with acl-type=" + queueAclType);
    }

    return new RMQueueAclInfo(true, user.getUserName(), "");
  }

  @POST
  @Path(RMWSConsts.SIGNAL_TO_CONTAINER)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public Response signalToContainer(
      @PathParam(RMWSConsts.CONTAINERID) String containerId,
      @PathParam(RMWSConsts.COMMAND) String command,
      @Context HttpServletRequest hsr)
      throws AuthorizationException {
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    initForWritableEndpoints(callerUGI, false);
    if (!EnumUtils.isValidEnum(
        SignalContainerCommand.class, command.toUpperCase())) {
      String errMsg =
          "Invalid command: " + command.toUpperCase() + ", valid commands are: "
              + Arrays.asList(SignalContainerCommand.values());
      return Response.status(Status.BAD_REQUEST).entity(errMsg).build();
    }
    try {
      ContainerId containerIdObj = ContainerId.fromString(containerId);
      rm.getClientRMService().signalToContainer(SignalContainerRequest
          .newInstance(containerIdObj,
              SignalContainerCommand.valueOf(command.toUpperCase())));
    } catch (Exception e) {
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(e.getMessage()).build();
    }
    return Response.status(Status.OK).build();
  }
}
