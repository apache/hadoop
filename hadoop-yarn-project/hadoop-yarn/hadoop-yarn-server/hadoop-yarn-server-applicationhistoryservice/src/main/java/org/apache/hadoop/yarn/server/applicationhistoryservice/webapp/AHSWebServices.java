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

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import java.util.Collections;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.server.webapp.LogServlet;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.server.webapp.WrappedLogMetaRequest;
import org.apache.hadoop.yarn.server.webapp.YarnWebServiceParams;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@Path("/ws/v1/applicationhistory")
public class AHSWebServices extends WebServices {

  private LogServlet logServlet;

  @Inject
  public AHSWebServices(ApplicationBaseProtocol appBaseProt,
      Configuration conf) {
    super(appBaseProt);
    this.logServlet = new LogServlet(conf, this);
  }

  @GET
  @Path("/about")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public TimelineAbout about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    initForReadableEndpoints(res);
    return TimelineUtils.createTimelineAbout("Generic History Service API");
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public AppsInfo get(@Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    return getApps(req, res, null, Collections.<String> emptySet(), null, null,
      null, null, null, null, null, null, null,
        Collections.<String> emptySet());
  }

  @GET
  @Path("/apps")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppsInfo getApps(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @QueryParam("state") String stateQuery,
      @QueryParam("states") Set<String> statesQuery,
      @QueryParam("finalStatus") String finalStatusQuery,
      @QueryParam("user") String userQuery,
      @QueryParam("queue") String queueQuery,
      @QueryParam("limit") String count,
      @QueryParam("startedTimeBegin") String startedBegin,
      @QueryParam("startedTimeEnd") String startedEnd,
      @QueryParam("finishedTimeBegin") String finishBegin,
      @QueryParam("finishedTimeEnd") String finishEnd,
      @QueryParam("name") String name,
      @QueryParam("applicationTypes") Set<String> applicationTypes) {
    initForReadableEndpoints(res);
    validateStates(stateQuery, statesQuery);
    return super.getApps(req, res, stateQuery, statesQuery, finalStatusQuery,
      userQuery, queueQuery, count, startedBegin, startedEnd, finishBegin,
      finishEnd, name, applicationTypes);
  }

  @GET
  @Path("/apps/{appid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppInfo getApp(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId) {
    initForReadableEndpoints(res);
    return super.getApp(req, res, appId);
  }

  @GET
  @Path("/apps/{appid}/appattempts")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppAttemptsInfo getAppAttempts(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId) {
    initForReadableEndpoints(res);
    return super.getAppAttempts(req, res, appId);
  }

  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppAttemptInfo getAppAttempt(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("appattemptid") String appAttemptId) {
    initForReadableEndpoints(res);
    return super.getAppAttempt(req, res, appId, appAttemptId);
  }

  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}/containers")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ContainersInfo getContainers(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("appattemptid") String appAttemptId) {
    initForReadableEndpoints(res);
    return super.getContainers(req, res, appId, appAttemptId);
  }

  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}/containers/{containerid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ContainerInfo getContainer(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("appattemptid") String appAttemptId,
      @PathParam("containerid") String containerId) {
    initForReadableEndpoints(res);
    return super.getContainer(req, res, appId, appAttemptId, containerId);
  }

  private static void
      validateStates(String stateQuery, Set<String> statesQuery) {
    // stateQuery is deprecated.
    if (stateQuery != null && !stateQuery.isEmpty()) {
      statesQuery.add(stateQuery);
    }
    Set<String> appStates = parseQueries(statesQuery, true);
    for (String appState : appStates) {
      switch (YarnApplicationState.valueOf(
          StringUtils.toUpperCase(appState))) {
        case FINISHED:
        case FAILED:
        case KILLED:
          continue;
        default:
          throw new BadRequestException("Invalid application-state " + appState
              + " specified. It should be a final state");
      }
    }
  }

  // TODO: YARN-6080: Create WebServiceUtils to have common functions used in
  //       RMWebService, NMWebService and AHSWebService.
  /**
   * Returns log file's name as well as current file size for a container.
   *
   * @param req
   *    HttpServletRequest
   * @param res
   *    HttpServletResponse
   * @param containerIdStr
   *    The container ID
   * @param nmId
   *    The Node Manager NodeId
   * @param redirectedFromNode
   *    Whether this is a redirected request from NM
   * @return
   *    The log file's name and current file size
   */
  @GET
  @Path("/containers/{containerid}/logs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getContainerLogsInfo(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    initForReadableEndpoints(res);

    WrappedLogMetaRequest.Builder logMetaRequestBuilder =
        LogServlet.createRequestFromContainerId(containerIdStr);

    return logServlet.getContainerLogsInfo(req, logMetaRequestBuilder, nmId,
        redirectedFromNode, null, manualRedirection);
  }

  /**
   * Returns the contents of a container's log file in plain text.
   *
   * @param req
   *    HttpServletRequest
   * @param res
   *    HttpServletResponse
   * @param containerIdStr
   *    The container ID
   * @param filename
   *    The name of the log file
   * @param format
   *    The content type
   * @param size
   *    the size of the log file
   * @param nmId
   *    The Node Manager NodeId
   * @param redirectedFromNode
   *    Whether this is the redirect request from NM
   * @return
   *    The contents of the container's log file
   */
  @GET
  @Path("/containers/{containerid}/logs/{filename}")
  @Produces({ MediaType.TEXT_PLAIN })
  @Public
  @Unstable
  public Response getContainerLogFile(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    return getLogs(req, res, containerIdStr, filename, format,
        size, nmId, redirectedFromNode, manualRedirection);
  }

  //TODO: YARN-4993: Refactory ContainersLogsBlock, AggregatedLogsBlock and
  //      container log webservice introduced in AHS to minimize
  //      the duplication.
  @GET
  @Path("/containerlogs/{containerid}/{filename}")
  @Produces({ MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8 })
  @Public
  @Unstable
  public Response getLogs(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    initForReadableEndpoints(res);
    return logServlet.getLogFile(req, containerIdStr, filename, format, size,
        nmId, redirectedFromNode, null, manualRedirection);
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  LogServlet getLogServlet() {
    return this.logServlet;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  void setLogServlet(LogServlet logServlet) {
    this.logServlet = logServlet;
  }
}