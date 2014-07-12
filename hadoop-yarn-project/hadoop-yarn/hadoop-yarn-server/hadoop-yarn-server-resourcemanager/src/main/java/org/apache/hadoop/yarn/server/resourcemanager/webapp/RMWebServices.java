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
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CredentialsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FifoSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LocalResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@Path("/ws/v1/cluster")
public class RMWebServices {
  private static final Log LOG =
      LogFactory.getLog(RMWebServices.class.getName());
  private static final String EMPTY = "";
  private static final String ANY = "*";
  private final ResourceManager rm;
  private static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  private final Configuration conf;
  private @Context HttpServletResponse response;

  @Inject
  public RMWebServices(final ResourceManager rm, Configuration conf) {
    this.rm = rm;
    this.conf = conf;
  }

  protected Boolean hasAccess(RMApp app, HttpServletRequest hsr) {
    // Check for the authorization.
    String remoteUser = hsr.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null
        && !(this.rm.getApplicationACLsManager().checkAccess(callerUGI,
              ApplicationAccessType.VIEW_APP, app.getUser(),
              app.getApplicationId()) ||
            this.rm.getQueueACLsManager().checkAccess(callerUGI,
              QueueACL.ADMINISTER_QUEUE, app.getQueue()))) {
      return false;
    }
    return true;
  }

  private void init() {
    //clear content type
    response.setContentType(null);
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ClusterInfo get() {
    return getClusterInfo();
  }

  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ClusterInfo getClusterInfo() {
    init();
    return new ClusterInfo(this.rm);
  }

  @GET
  @Path("/metrics")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ClusterMetricsInfo getClusterMetricsInfo() {
    init();
    return new ClusterMetricsInfo(this.rm, this.rm.getRMContext());
  }

  @GET
  @Path("/scheduler")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public SchedulerTypeInfo getSchedulerInfo() {
    init();
    ResourceScheduler rs = rm.getResourceScheduler();
    SchedulerInfo sinfo;
    if (rs instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) rs;
      CSQueue root = cs.getRootQueue();
      sinfo = new CapacitySchedulerInfo(root);
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

  /**
   * Returns all nodes in the cluster. If the states param is given, returns
   * all nodes that are in the comma-separated list of states.
   */
  @GET
  @Path("/nodes")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public NodesInfo getNodes(@QueryParam("states") String states) {
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
        acceptedStates.add(NodeState.valueOf(stateStr.toUpperCase()));
      }
    }
    
    Collection<RMNode> rmNodes = RMServerUtils.queryRMNodes(this.rm.getRMContext(),
        acceptedStates);
    NodesInfo nodesInfo = new NodesInfo();
    for (RMNode rmNode : rmNodes) {
      NodeInfo nodeInfo = new NodeInfo(rmNode, sched);
      if (EnumSet.of(NodeState.LOST, NodeState.DECOMMISSIONED, NodeState.REBOOTED)
          .contains(rmNode.getState())) {
        nodeInfo.setNodeHTTPAddress(EMPTY);
      }
      nodesInfo.add(nodeInfo);
    }
    
    return nodesInfo;
  }

  @GET
  @Path("/nodes/{nodeId}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public NodeInfo getNode(@PathParam("nodeId") String nodeId) {
    init();
    if (nodeId == null || nodeId.isEmpty()) {
      throw new NotFoundException("nodeId, " + nodeId + ", is empty or null");
    }
    ResourceScheduler sched = this.rm.getResourceScheduler();
    if (sched == null) {
      throw new NotFoundException("Null ResourceScheduler instance");
    }
    NodeId nid = ConverterUtils.toNodeId(nodeId);
    RMNode ni = this.rm.getRMContext().getRMNodes().get(nid);
    boolean isInactive = false;
    if (ni == null) {
      ni = this.rm.getRMContext().getInactiveRMNodes().get(nid.getHost());
      if (ni == null) {
        throw new NotFoundException("nodeId, " + nodeId + ", is not found");
      }
      isInactive = true;
    }
    NodeInfo nodeInfo = new NodeInfo(ni, sched);
    if (isInactive) {
      nodeInfo.setNodeHTTPAddress(EMPTY);
    }
    return nodeInfo;
  }

  @GET
  @Path("/apps")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppsInfo getApps(@Context HttpServletRequest hsr,
      @QueryParam("state") String stateQuery,
      @QueryParam("states") Set<String> statesQuery,
      @QueryParam("finalStatus") String finalStatusQuery,
      @QueryParam("user") String userQuery,
      @QueryParam("queue") String queueQuery,
      @QueryParam("limit") String count,
      @QueryParam("startedTimeBegin") String startedBegin,
      @QueryParam("startedTimeEnd") String startedEnd,
      @QueryParam("finishedTimeBegin") String finishBegin,
      @QueryParam("finishedTimeEnd") String finishEnd,
      @QueryParam("applicationTypes") Set<String> applicationTypes,
      @QueryParam("applicationTags") Set<String> applicationTags) {
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
        throw new BadRequestException("startedTimeBegin must be greater than 0");
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
      appReports = rm.getClientRMService()
          .getApplications(request, false).getApplicationList();
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

      if (finalStatusQuery != null && !finalStatusQuery.isEmpty()) {
        FinalApplicationStatus.valueOf(finalStatusQuery);
        if (!rmapp.getFinalApplicationStatus().toString()
            .equalsIgnoreCase(finalStatusQuery)) {
          continue;
        }
      }

      AppInfo app = new AppInfo(rmapp, hasAccess(rmapp, hsr),
          WebAppUtils.getHttpSchemePrefix(conf));
      allApps.add(app);
    }
    return allApps;
  }

  @GET
  @Path("/appstatistics")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ApplicationStatisticsInfo getAppStatistics(
      @Context HttpServletRequest hsr,
      @QueryParam("states") Set<String> stateQueries,
      @QueryParam("applicationTypes") Set<String> typeQueries) {
    init();

    // parse the params and build the scoreboard
    // converting state/type name to lowercase
    Set<String> states = parseQueries(stateQueries, true);
    Set<String> types = parseQueries(typeQueries, false);
    // if no types, counts the applications of any types
    if (types.size() == 0) {
      types.add(ANY);
    } else if (types.size() != 1) {
      throw new BadRequestException("# of applicationTypes = " + types.size()
          + ", we temporarily support at most one applicationType");
    }
    // if no states, returns the counts of all RMAppStates
    if (states.size() == 0) {
      for (YarnApplicationState state : YarnApplicationState.values()) {
        states.add(state.toString().toLowerCase());
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
      String type = rmapp.getApplicationType().trim().toLowerCase();
      if (states.contains(state.toString().toLowerCase())) {
        if (types.contains(ANY)) {
          countApp(scoreboard, state, ANY);
        } else if (types.contains(type)) {
          countApp(scoreboard, state, type);
        }
      }
    }

    // fill the response object
    ApplicationStatisticsInfo appStatInfo = new ApplicationStatisticsInfo();
    for (Map.Entry<YarnApplicationState, Map<String, Long>> partScoreboard
        : scoreboard.entrySet()) {
      for (Map.Entry<String, Long> statEntry
          : partScoreboard.getValue().entrySet()) {
        StatisticsItemInfo statItem = new StatisticsItemInfo(
            partScoreboard.getKey(), statEntry.getKey(), statEntry.getValue());
        appStatInfo.add(statItem);
      }
    }
    return appStatInfo;
  }

  private static Set<String> parseQueries(
      Set<String> queries, boolean isState) {
    Set<String> params = new HashSet<String>();
    if (!queries.isEmpty()) {
      for (String query : queries) {
        if (query != null && !query.trim().isEmpty()) {
          String[] paramStrs = query.split(",");
          for (String paramStr : paramStrs) {
            if (paramStr != null && !paramStr.trim().isEmpty()) {
              if (isState) {
                try {
                  // enum string is in the uppercase
                  YarnApplicationState.valueOf(paramStr.trim().toUpperCase());
                } catch (RuntimeException e) {
                  YarnApplicationState[] stateArray =
                      YarnApplicationState.values();
                  String allAppStates = Arrays.toString(stateArray);
                  throw new BadRequestException(
                      "Invalid application-state " + paramStr.trim()
                      + " specified. It should be one of " + allAppStates);
                }
              }
              params.add(paramStr.trim().toLowerCase());
            }
          }
        }
      }
    }
    return params;
  }

  private static Map<YarnApplicationState, Map<String, Long>> buildScoreboard(
     Set<String> states, Set<String> types) {
    Map<YarnApplicationState, Map<String, Long>> scoreboard
        = new HashMap<YarnApplicationState, Map<String, Long>>();
    // default states will result in enumerating all YarnApplicationStates
    assert !states.isEmpty();
    for (String state : states) {
      Map<String, Long> partScoreboard = new HashMap<String, Long>();
      scoreboard.put(
          YarnApplicationState.valueOf(state.toUpperCase()), partScoreboard);
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
  @Path("/apps/{appid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppInfo getApp(@Context HttpServletRequest hsr,
      @PathParam("appid") String appId) {
    init();
    if (appId == null || appId.isEmpty()) {
      throw new NotFoundException("appId, " + appId + ", is empty or null");
    }
    ApplicationId id;
    id = ConverterUtils.toApplicationId(recordFactory, appId);
    if (id == null) {
      throw new NotFoundException("appId is null");
    }
    RMApp app = rm.getRMContext().getRMApps().get(id);
    if (app == null) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }
    return new AppInfo(app, hasAccess(app, hsr), hsr.getScheme() + "://");
  }

  @GET
  @Path("/apps/{appid}/appattempts")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppAttemptsInfo getAppAttempts(@PathParam("appid") String appId) {

    init();
    if (appId == null || appId.isEmpty()) {
      throw new NotFoundException("appId, " + appId + ", is empty or null");
    }
    ApplicationId id;
    id = ConverterUtils.toApplicationId(recordFactory, appId);
    if (id == null) {
      throw new NotFoundException("appId is null");
    }
    RMApp app = rm.getRMContext().getRMApps().get(id);
    if (app == null) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }

    AppAttemptsInfo appAttemptsInfo = new AppAttemptsInfo();
    for (RMAppAttempt attempt : app.getAppAttempts().values()) {
      AppAttemptInfo attemptInfo = new AppAttemptInfo(attempt, app.getUser());
      appAttemptsInfo.add(attemptInfo);
    }

    return appAttemptsInfo;
  }

  @GET
  @Path("/apps/{appid}/state")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppState getAppState(@Context HttpServletRequest hsr,
      @PathParam("appid") String appId) throws AuthorizationException {
    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr);
    String userName = "";
    if (callerUGI != null) {
      userName = callerUGI.getUserName();
    }
    RMApp app = null;
    try {
      app = getRMAppForAppId(appId);
    } catch (NotFoundException e) {
      RMAuditLogger.logFailure(userName, AuditConstants.KILL_APP_REQUEST,
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
  @Path("/apps/{appid}/state")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response updateAppState(AppState targetState,
      @Context HttpServletRequest hsr, @PathParam("appid") String appId)
      throws AuthorizationException, YarnException, InterruptedException,
      IOException {

    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr);
    if (callerUGI == null) {
      String msg = "Unable to obtain user name, user not authenticated";
      throw new AuthorizationException(msg);
    }

    String userName = callerUGI.getUserName();
    RMApp app = null;
    try {
      app = getRMAppForAppId(appId);
    } catch (NotFoundException e) {
      RMAuditLogger.logFailure(userName, AuditConstants.KILL_APP_REQUEST,
        "UNKNOWN", "RMWebService", "Trying to kill/move an absent application "
            + appId);
      throw e;
    }

    if (!app.getState().toString().equals(targetState.getState())) {
      // user is attempting to change state. right we only
      // allow users to kill the app

      if (targetState.getState().equals(YarnApplicationState.KILLED.toString())) {
        return killApp(app, callerUGI, hsr);
      }
      throw new BadRequestException("Only '"
          + YarnApplicationState.KILLED.toString()
          + "' is allowed as a target state.");
    }

    AppState ret = new AppState();
    ret.setState(app.getState().toString());

    return Response.status(Status.OK).entity(ret).build();
  }

  protected Response killApp(RMApp app, UserGroupInformation callerUGI,
      HttpServletRequest hsr) throws IOException, InterruptedException {

    if (app == null) {
      throw new IllegalArgumentException("app cannot be null");
    }
    String userName = callerUGI.getUserName();
    final ApplicationId appid = app.getApplicationId();
    KillApplicationResponse resp = null;
    try {
      resp =
          callerUGI
            .doAs(new PrivilegedExceptionAction<KillApplicationResponse>() {
              @Override
              public KillApplicationResponse run() throws IOException,
                  YarnException {
                KillApplicationRequest req =
                    KillApplicationRequest.newInstance(appid);
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
          String msg =
              "Unauthorized attempt to kill appid " + appId
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

  private RMApp getRMAppForAppId(String appId) {

    if (appId == null || appId.isEmpty()) {
      throw new NotFoundException("appId, " + appId + ", is empty or null");
    }
    ApplicationId id;
    try {
      id = ConverterUtils.toApplicationId(recordFactory, appId);
    } catch (NumberFormatException e) {
      throw new NotFoundException("appId is invalid");
    }
    if (id == null) {
      throw new NotFoundException("appId is invalid");
    }
    RMApp app = rm.getRMContext().getRMApps().get(id);
    if (app == null) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }
    return app;
  }

  private UserGroupInformation getCallerUserGroupInformation(
      HttpServletRequest hsr) {

    String remoteUser = hsr.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }

    return callerUGI;
  }

  /**
   * Generates a new ApplicationId which is then sent to the client
   * 
   * @param hsr
   *          the servlet request
   * @return Response containing the app id and the maximum resource
   *         capabilities
   * @throws AuthorizationException
   * @throws IOException
   * @throws InterruptedException
   */
  @POST
  @Path("/apps/new-application")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response createNewApplication(@Context HttpServletRequest hsr)
      throws AuthorizationException, IOException, InterruptedException {
    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr);
    if (callerUGI == null) {
      throw new AuthorizationException("Unable to obtain user name, "
          + "user not authenticated");
    }

    NewApplication appId = createNewApplication();
    return Response.status(Status.OK).entity(appId).build();

  }

  // reuse the code in ClientRMService to create new app
  // get the new app id and submit app
  // set location header with new app location
  /**
   * Function to submit an app to the RM
   * 
   * @param newApp
   *          structure containing information to construct the
   *          ApplicationSubmissionContext
   * @param hsr
   *          the servlet request
   * @return Response containing the status code
   * @throws AuthorizationException
   * @throws IOException
   * @throws InterruptedException
   */
  @POST
  @Path("/apps")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response submitApplication(ApplicationSubmissionContextInfo newApp,
      @Context HttpServletRequest hsr) throws AuthorizationException,
      IOException, InterruptedException {

    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr);
    if (callerUGI == null) {
      throw new AuthorizationException("Unable to obtain user name, "
          + "user not authenticated");
    }

    ApplicationSubmissionContext appContext =
        createAppSubmissionContext(newApp);
    final SubmitApplicationRequest req =
        SubmitApplicationRequest.newInstance(appContext);

    try {
      callerUGI
        .doAs(new PrivilegedExceptionAction<SubmitApplicationResponse>() {
          @Override
          public SubmitApplicationResponse run() throws IOException,
              YarnException {
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
        new NewApplication(resp.getApplicationId().toString(), new ResourceInfo(
          resp.getMaximumResourceCapability()));
    return appId;
  }

  /**
   * Create the actual ApplicationSubmissionContext to be submitted to the RM
   * from the information provided by the user.
   * 
   * @param newApp
   *          the information provided by the user
   * @return returns the constructed ApplicationSubmissionContext
   * @throws IOException
   */
  protected ApplicationSubmissionContext createAppSubmissionContext(
      ApplicationSubmissionContextInfo newApp) throws IOException {

    // create local resources and app submission context

    ApplicationId appid;
    String error =
        "Could not parse application id " + newApp.getApplicationId();
    try {
      appid =
          ConverterUtils.toApplicationId(recordFactory,
            newApp.getApplicationId());
    } catch (Exception e) {
      throw new BadRequestException(error);
    }
    ApplicationSubmissionContext appContext =
        ApplicationSubmissionContext.newInstance(appid,
          newApp.getApplicationName(), newApp.getQueue(),
          Priority.newInstance(newApp.getPriority()),
          createContainerLaunchContext(newApp), newApp.getUnmanagedAM(),
          newApp.getCancelTokensWhenComplete(), newApp.getMaxAppAttempts(),
          createAppSubmissionContextResource(newApp),
          newApp.getApplicationType(),
          newApp.getKeepContainersAcrossApplicationAttempts());
    appContext.setApplicationTags(newApp.getApplicationTags());

    return appContext;
  }

  protected Resource createAppSubmissionContextResource(
      ApplicationSubmissionContextInfo newApp) throws BadRequestException {
    if (newApp.getResource().getvCores() > rm.getConfig().getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES)) {
      String msg = "Requested more cores than configured max";
      throw new BadRequestException(msg);
    }
    if (newApp.getResource().getMemory() > rm.getConfig().getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB)) {
      String msg = "Requested more memory than configured max";
      throw new BadRequestException(msg);
    }
    Resource r =
        Resource.newInstance(newApp.getResource().getMemory(), newApp
          .getResource().getvCores());
    return r;
  }

  /**
   * Create the ContainerLaunchContext required for the
   * ApplicationSubmissionContext. This function takes the user information and
   * generates the ByteBuffer structures required by the ContainerLaunchContext
   * 
   * @param newApp
   *          the information provided by the user
   * @return
   * @throws BadRequestException
   * @throws IOException
   */
  protected ContainerLaunchContext createContainerLaunchContext(
      ApplicationSubmissionContextInfo newApp) throws BadRequestException, IOException {

    // create container launch context

    HashMap<String, ByteBuffer> hmap = new HashMap<String, ByteBuffer>();
    for (Map.Entry<String, String> entry : newApp
      .getContainerLaunchContextInfo().getAuxillaryServiceData().entrySet()) {
      if (entry.getValue().isEmpty() == false) {
        Base64 decoder = new Base64(0, null, true);
        byte[] data = decoder.decode(entry.getValue());
        hmap.put(entry.getKey(), ByteBuffer.wrap(data));
      }
    }

    HashMap<String, LocalResource> hlr = new HashMap<String, LocalResource>();
    for (Map.Entry<String, LocalResourceInfo> entry : newApp
      .getContainerLaunchContextInfo().getResources().entrySet()) {
      LocalResourceInfo l = entry.getValue();
      LocalResource lr =
          LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(l.getUrl()), l.getType(),
            l.getVisibility(), l.getSize(), l.getTimestamp());
      hlr.put(entry.getKey(), lr);
    }

    DataOutputBuffer out = new DataOutputBuffer();
    Credentials cs =
        createCredentials(newApp.getContainerLaunchContextInfo()
          .getCredentials());
    cs.writeTokenStorageToStream(out);
    ByteBuffer tokens = ByteBuffer.wrap(out.getData());

    ContainerLaunchContext ctx =
        ContainerLaunchContext.newInstance(hlr, newApp
          .getContainerLaunchContextInfo().getEnvironment(), newApp
          .getContainerLaunchContextInfo().getCommands(), hmap, tokens, newApp
          .getContainerLaunchContextInfo().getAcls());

    return ctx;
  }

  /**
   * Generate a Credentials object from the information in the CredentialsInfo
   * object.
   * 
   * @param credentials
   *          the CredentialsInfo provided by the user.
   * @return
   */
  private Credentials createCredentials(CredentialsInfo credentials) {
    Credentials ret = new Credentials();
    try {
      for (Map.Entry<String, String> entry : credentials.getTokens().entrySet()) {
        Text alias = new Text(entry.getKey());
        Token<TokenIdentifier> token = new Token<TokenIdentifier>();
        token.decodeFromUrlString(entry.getValue());
        ret.addToken(alias, token);
      }
      for (Map.Entry<String, String> entry : credentials.getTokens().entrySet()) {
        Text alias = new Text(entry.getKey());
        Base64 decoder = new Base64(0, null, true);
        byte[] secret = decoder.decode(entry.getValue());
        ret.addSecretKey(alias, secret);
      }
    } catch (IOException ie) {
      throw new BadRequestException(
        "Could not parse credentials data; exception message = "
            + ie.getMessage());
    }
    return ret;
  }
}
