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
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.webapp.LogWebServiceUtils;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.server.webapp.YarnWebServiceParams;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Path("/ws/v1/applicationhistory")
public class AHSWebServices extends WebServices {

  private static final Logger LOG = LoggerFactory
      .getLogger(AHSWebServices.class);
  private static final String NM_DOWNLOAD_URI_STR =
      "/ws/v1/node/containers";
  private static final Joiner JOINER = Joiner.on("");
  private static final Joiner DOT_JOINER = Joiner.on(". ");
  private final Configuration conf;
  private final LogAggregationFileControllerFactory factory;

  @Inject
  public AHSWebServices(ApplicationBaseProtocol appBaseProt,
      Configuration conf) {
    super(appBaseProt);
    this.conf = conf;
    this.factory = new LogAggregationFileControllerFactory(conf);
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
      null, null, null, null, null, null, Collections.<String> emptySet());
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
      @QueryParam("applicationTypes") Set<String> applicationTypes) {
    initForReadableEndpoints(res);
    validateStates(stateQuery, statesQuery);
    return super.getApps(req, res, stateQuery, statesQuery, finalStatusQuery,
      userQuery, queueQuery, count, startedBegin, startedEnd, finishBegin,
      finishEnd, applicationTypes);
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
   * @param redirected_from_node
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
      @DefaultValue("false") boolean redirected_from_node) {
    ContainerId containerId = null;
    initForReadableEndpoints(res);
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("invalid container id, " + containerIdStr);
    }

    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();
    AppInfo appInfo;
    try {
      appInfo = super.getApp(req, res, appId.toString());
    } catch (Exception ex) {
      // directly find logs from HDFS.
      return LogWebServiceUtils
          .getContainerLogMeta(factory, appId, null, null, containerIdStr,
              false);
    }
    // if the application finishes, directly find logs
    // from HDFS.
    if (LogWebServiceUtils.isFinishedState(appInfo.getAppState())) {
      return LogWebServiceUtils
          .getContainerLogMeta(factory, appId, null, null, containerIdStr,
              false);
    }
    if (LogWebServiceUtils.isRunningState(appInfo.getAppState())) {
      String appOwner = appInfo.getUser();
      String nodeHttpAddress = null;
      if (nmId != null && !nmId.isEmpty()) {
        try {
          nodeHttpAddress = getNMWebAddressFromRM(conf, nmId);
        } catch (Exception ex) {
          LOG.debug("{}", ex);
        }
      }
      if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()) {
        ContainerInfo containerInfo;
        try {
          containerInfo = super.getContainer(
              req, res, appId.toString(),
              containerId.getApplicationAttemptId().toString(),
              containerId.toString());
        } catch (Exception ex) {
          // return log meta for the aggregated logs if exists.
          // It will also return empty log meta for the local logs.
          return LogWebServiceUtils
              .getContainerLogMeta(factory, appId, appOwner, null,
                  containerIdStr, true);
        }
        nodeHttpAddress = containerInfo.getNodeHttpAddress();
        // make sure nodeHttpAddress is not null and not empty. Otherwise,
        // we would only get log meta for aggregated logs instead of
        // re-directing the request
        if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()
            || redirected_from_node) {
          // return log meta for the aggregated logs if exists.
          // It will also return empty log meta for the local logs.
          // If this is the redirect request from NM, we should not
          // re-direct the request back. Simply output the aggregated log meta.
          return LogWebServiceUtils
              .getContainerLogMeta(factory, appId, appOwner, null,
                  containerIdStr, true);
        }
      }
      String uri = "/" + containerId.toString() + "/logs";
      String resURI = JOINER.join(
          LogWebServiceUtils.getAbsoluteNMWebAddress(conf, nodeHttpAddress),
          NM_DOWNLOAD_URI_STR, uri);
      String query = req.getQueryString();
      if (query != null && !query.isEmpty()) {
        resURI += "?" + query;
      }
      ResponseBuilder response = Response.status(
          HttpServletResponse.SC_TEMPORARY_REDIRECT);
      response.header("Location", resURI);
      return response.build();
    } else {
      throw new NotFoundException(
          "The application is not at Running or Finished State.");
    }
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
   * @param redirected_from_node
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
      boolean redirected_from_node) {
    return getLogs(req, res, containerIdStr, filename, format,
        size, nmId, redirected_from_node);
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
      @DefaultValue("false") boolean redirected_from_node) {
    initForReadableEndpoints(res);
    ContainerId containerId;
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException ex) {
      return LogWebServiceUtils.createBadResponse(Status.NOT_FOUND,
          "Invalid ContainerId: " + containerIdStr);
    }

    final long length = LogWebServiceUtils.parseLongParam(size);

    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();
    AppInfo appInfo;
    try {
      appInfo = super.getApp(req, res, appId.toString());
    } catch (Exception ex) {
      // directly find logs from HDFS.
      return LogWebServiceUtils
          .sendStreamOutputResponse(factory, appId, null, null, containerIdStr,
              filename, format, length, false);
    }
    String appOwner = appInfo.getUser();
    if (LogWebServiceUtils.isFinishedState(appInfo.getAppState())) {
      // directly find logs from HDFS.
      return LogWebServiceUtils
          .sendStreamOutputResponse(factory, appId, appOwner, null,
              containerIdStr, filename, format, length, false);
    }

    if (LogWebServiceUtils.isRunningState(appInfo.getAppState())) {
      String nodeHttpAddress = null;
      if (nmId != null && !nmId.isEmpty()) {
        try {
          nodeHttpAddress = getNMWebAddressFromRM(conf, nmId);
        } catch (Exception ex) {
          LOG.debug("{}", ex);
        }
      }
      if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()) {
        ContainerInfo containerInfo;
        try {
          containerInfo = super.getContainer(
              req, res, appId.toString(),
              containerId.getApplicationAttemptId().toString(),
              containerId.toString());
        } catch (Exception ex) {
          // output the aggregated logs
          return LogWebServiceUtils
              .sendStreamOutputResponse(factory, appId, appOwner, null,
                  containerIdStr, filename, format, length, true);
        }
        nodeHttpAddress = containerInfo.getNodeHttpAddress();
        // make sure nodeHttpAddress is not null and not empty. Otherwise,
        // we would only get aggregated logs instead of re-directing the
        // request.
        // If this is the redirect request from NM, we should not re-direct the
        // request back. Simply output the aggregated logs.
        if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()
            || redirected_from_node) {
          // output the aggregated logs
          return LogWebServiceUtils
              .sendStreamOutputResponse(factory, appId, appOwner, null,
                  containerIdStr, filename, format, length, true);
        }
      }
      String uri = "/" + containerId.toString() + "/logs/" + filename;
      String resURI = JOINER.join(
          LogWebServiceUtils.getAbsoluteNMWebAddress(conf, nodeHttpAddress),
          NM_DOWNLOAD_URI_STR, uri);
      String query = req.getQueryString();
      if (query != null && !query.isEmpty()) {
        resURI += "?" + query;
      }
      ResponseBuilder response = Response.status(
          HttpServletResponse.SC_TEMPORARY_REDIRECT);
      response.header("Location", resURI);
      return response.build();
    } else {
      return LogWebServiceUtils.createBadResponse(Status.NOT_FOUND,
          "The application is not at Running or Finished State.");
    }
  }

  @VisibleForTesting @InterfaceAudience.Private
  public String getNMWebAddressFromRM(Configuration configuration,
      String nodeId)
      throws ClientHandlerException, UniformInterfaceException, JSONException {
    return LogWebServiceUtils.getNMWebAddressFromRM(configuration, nodeId);
  }
}