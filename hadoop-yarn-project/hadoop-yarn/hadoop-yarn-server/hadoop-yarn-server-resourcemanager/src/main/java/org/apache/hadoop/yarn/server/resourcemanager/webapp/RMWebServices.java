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
import java.util.Collection;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FifoSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@Path("/ws/v1/cluster")
public class RMWebServices {
  private static final String EMPTY = "";
  private final ResourceManager rm;
  private static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  private final ApplicationACLsManager aclsManager;

  private @Context HttpServletResponse response;

  @Inject
  public RMWebServices(final ResourceManager rm,
      final ApplicationACLsManager aclsManager) {
    this.rm = rm;
    this.aclsManager = aclsManager;
  }

  protected Boolean hasAccess(RMApp app, HttpServletRequest hsr) {
    // Check for the authorization.
    String remoteUser = hsr.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null
        && !this.aclsManager.checkAccess(callerUGI,
            ApplicationAccessType.VIEW_APP, app.getUser(),
            app.getApplicationId())) {
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
      @QueryParam("finalStatus") String finalStatusQuery,
      @QueryParam("user") String userQuery,
      @QueryParam("queue") String queueQuery,
      @QueryParam("limit") String count,
      @QueryParam("startedTimeBegin") String startedBegin,
      @QueryParam("startedTimeEnd") String startedEnd,
      @QueryParam("finishedTimeBegin") String finishBegin,
      @QueryParam("finishedTimeEnd") String finishEnd) {
    long num = 0;
    boolean checkCount = false;
    boolean checkStart = false;
    boolean checkEnd = false;
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

    final ConcurrentMap<ApplicationId, RMApp> apps = rm.getRMContext()
        .getRMApps();
    AppsInfo allApps = new AppsInfo();
    for (RMApp rmapp : apps.values()) {

      if (checkCount && num == countNum) {
        break;
      }
      if (stateQuery != null && !stateQuery.isEmpty()) {
        RMAppState.valueOf(stateQuery);
        if (!rmapp.getState().toString().equalsIgnoreCase(stateQuery)) {
          continue;
        }
      }
      if (finalStatusQuery != null && !finalStatusQuery.isEmpty()) {
        FinalApplicationStatus.valueOf(finalStatusQuery);
        if (!rmapp.getFinalApplicationStatus().toString()
            .equalsIgnoreCase(finalStatusQuery)) {
          continue;
        }
      }
      if (userQuery != null && !userQuery.isEmpty()) {
        if (!rmapp.getUser().equals(userQuery)) {
          continue;
        }
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
        if (!rmapp.getQueue().equals(queueQuery)) {
          continue;
        }
      }

      if (checkStart
          && (rmapp.getStartTime() < sBegin || rmapp.getStartTime() > sEnd)) {
        continue;
      }
      if (checkEnd
          && (rmapp.getFinishTime() < fBegin || rmapp.getFinishTime() > fEnd)) {
        continue;
      }
      AppInfo app = new AppInfo(rmapp, hasAccess(rmapp, hsr));

      allApps.add(app);
      num++;
    }
    return allApps;
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
    return new AppInfo(app, hasAccess(app, hsr));
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
}
