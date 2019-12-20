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

package org.apache.hadoop.yarn.server.webapp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.inject.Singleton;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

/**
 * Support only ATSv2 client only.
 */
@Singleton @Path("/ws/v2/applicationlog") public class LogWebService {
  private static final Logger LOG =
      LoggerFactory.getLogger(LogWebService.class);
  private static final String RESOURCE_URI_STR_V2 = "/ws/v2/timeline/";
  private static final String NM_DOWNLOAD_URI_STR = "/ws/v1/node/containers";
  private static final Joiner JOINER = Joiner.on("");
  private static Configuration yarnConf = new YarnConfiguration();
  private static LogAggregationFileControllerFactory factory;
  private static String base;
  private static String defaultClusterid;
  private volatile Client webTimelineClient;

  static {
    init();
  }

  // initialize all the common resources - order is important
  private static void init() {
    factory = new LogAggregationFileControllerFactory(yarnConf);
    base = JOINER.join(WebAppUtils.getHttpSchemePrefix(yarnConf),
        WebAppUtils.getTimelineReaderWebAppURLWithoutScheme(yarnConf),
        RESOURCE_URI_STR_V2);
    defaultClusterid = yarnConf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
    LOG.info("Initialized LogWeService with clusterid " + defaultClusterid
        + " for URI: " + base);
  }

  private Client createTimelineWebClient() {
    ClientConfig cfg = new DefaultClientConfig();
    cfg.getClasses().add(YarnJacksonJaxbJsonProvider.class);
    Client client = new Client(
        new URLConnectionClientHandler(new HttpURLConnectionFactory() {
          @Override public HttpURLConnection getHttpURLConnection(URL url)
              throws IOException {
            AuthenticatedURL.Token token = new AuthenticatedURL.Token();
            HttpURLConnection conn = null;
            try {
              conn = new AuthenticatedURL().openConnection(url, token);
              LOG.info("LogWeService:Connecetion created.");
            } catch (AuthenticationException e) {
              throw new IOException(e);
            }
            return conn;
          }
        }), cfg);

    return client;
  }

  private void initForReadableEndpoints(HttpServletResponse response) {
    // clear content type
    response.setContentType(null);
  }

  /**
   * Returns log file's name as well as current file size for a container.
   *
   * @param req                HttpServletRequest
   * @param res                HttpServletResponse
   * @param containerIdStr     The container ID
   * @param nmId               The Node Manager NodeId
   * @param redirectedFromNode Whether this is a redirected request from NM
   * @return The log file's name and current file size
   */
  @GET @Path("/containers/{containerid}/logs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getContainerLogsInfo(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId) {
    ContainerId containerId = null;
    initForReadableEndpoints(res);
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("invalid container id, " + containerIdStr);
    }

    ApplicationId appId =
        containerId.getApplicationAttemptId().getApplicationId();
    AppInfo appInfo;
    try {
      appInfo = getApp(req, appId.toString(), clusterId);
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
          nodeHttpAddress =
              LogWebServiceUtils.getNMWebAddressFromRM(yarnConf, nmId);
        } catch (Exception ex) {
          LOG.debug("{}", ex);
        }
      }
      if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()) {
        ContainerInfo containerInfo;
        try {
          containerInfo =
              getContainer(req, appId.toString(), containerId.toString(),
                  clusterId);
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
            || redirectedFromNode) {
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
          LogWebServiceUtils.getAbsoluteNMWebAddress(yarnConf, nodeHttpAddress),
          NM_DOWNLOAD_URI_STR, uri);
      String query = req.getQueryString();
      if (query != null && !query.isEmpty()) {
        resURI += "?" + query;
      }
      Response.ResponseBuilder response =
          Response.status(HttpServletResponse.SC_TEMPORARY_REDIRECT);
      response.header("Location", resURI);
      return response.build();
    } else {
      throw new NotFoundException(
          "The application is not at Running or Finished State.");
    }
  }

  protected ContainerInfo getContainer(HttpServletRequest req, String appId,
      String containerId, String clusterId) {
    UserGroupInformation callerUGI = LogWebServiceUtils.getUser(req);
    String cId = clusterId != null ? clusterId : defaultClusterid;
    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("fields", "INFO");
    String path = JOINER.join("clusters/", cId, "/apps/", appId, "/entities/",
        TimelineEntityType.YARN_CONTAINER.toString(), "/", containerId);
    TimelineEntity conEntity = null;
    try {
      if (callerUGI == null) {
        conEntity = getEntity(path, params);
      } else {
        setUserName(params, callerUGI.getShortUserName());
        conEntity =
            callerUGI.doAs(new PrivilegedExceptionAction<TimelineEntity>() {
              @Override public TimelineEntity run() throws Exception {
                return getEntity(path, params);
              }
            });
      }
    } catch (Exception e) {
      LogWebServiceUtils.rewrapAndThrowException(e);
    }
    if (conEntity == null) {
      return null;
    }
    String nodeHttpAddress = (String) conEntity.getInfo()
        .get(ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO);

    ContainerInfo info = new ContainerInfo(nodeHttpAddress);
    return info;
  }

  protected AppInfo getApp(HttpServletRequest req, String appId,
      String clusterId) {
    UserGroupInformation callerUGI = LogWebServiceUtils.getUser(req);

    String cId = clusterId != null ? clusterId : defaultClusterid;
    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("fields", "INFO");
    String path = JOINER.join("clusters/", cId, "/apps/", appId);
    TimelineEntity appEntity = null;

    try {
      if (callerUGI == null) {
        appEntity = getEntity(path, params);
      } else {
        setUserName(params, callerUGI.getShortUserName());
        appEntity =
            callerUGI.doAs(new PrivilegedExceptionAction<TimelineEntity>() {
              @Override public TimelineEntity run() throws Exception {
                return getEntity(path, params);
              }
            });
      }
    } catch (Exception e) {
      LogWebServiceUtils.rewrapAndThrowException(e);
    }

    if (appEntity == null) {
      return null;
    }
    String appOwner = (String) appEntity.getInfo()
        .get(ApplicationMetricsConstants.USER_ENTITY_INFO);
    String state = (String) appEntity.getInfo()
        .get(ApplicationMetricsConstants.STATE_EVENT_INFO);
    YarnApplicationState appState = YarnApplicationState.valueOf(state);
    AppInfo info = new AppInfo(appState, appOwner);
    return info;
  }

  /**
   * Returns the contents of a container's log file in plain text.
   *
   * @param req                HttpServletRequest
   * @param res                HttpServletResponse
   * @param containerIdStr     The container ID
   * @param filename           The name of the log file
   * @param format             The content type
   * @param size               the size of the log file
   * @param nmId               The Node Manager NodeId
   * @param redirectedFromNode Whether this is the redirect request from NM
   * @return The contents of the container's log file
   */
  @GET @Path("/containers/{containerid}/logs/{filename}")
  @Produces({ MediaType.TEXT_PLAIN }) @InterfaceAudience.Public
  @InterfaceStability.Unstable public Response getContainerLogFile(
      @Context HttpServletRequest req, @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
          boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId) {
    return getLogs(req, res, containerIdStr, filename, format, size, nmId,
        redirectedFromNode, clusterId);
  }

  //TODO: YARN-4993: Refactory ContainersLogsBlock, AggregatedLogsBlock and
  //      container log webservice introduced in AHS to minimize
  //      the duplication.
  @GET @Path("/containerlogs/{containerid}/{filename}")
  @Produces({ MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8 })
  @InterfaceAudience.Public @InterfaceStability.Unstable
  public Response getLogs(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId) {
    initForReadableEndpoints(res);
    ContainerId containerId;
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException ex) {
      return LogWebServiceUtils.createBadResponse(Response.Status.NOT_FOUND,
          "Invalid ContainerId: " + containerIdStr);
    }

    final long length = LogWebServiceUtils.parseLongParam(size);

    ApplicationId appId =
        containerId.getApplicationAttemptId().getApplicationId();
    AppInfo appInfo;
    try {
      appInfo = getApp(req, appId.toString(), clusterId);
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
          nodeHttpAddress =
              LogWebServiceUtils.getNMWebAddressFromRM(yarnConf, nmId);
        } catch (Exception ex) {
          LOG.debug("{}", ex);
        }
      }
      if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()) {
        ContainerInfo containerInfo;
        try {
          containerInfo =
              getContainer(req, appId.toString(), containerId.toString(),
                  clusterId);
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
            || redirectedFromNode) {
          // output the aggregated logs
          return LogWebServiceUtils
              .sendStreamOutputResponse(factory, appId, appOwner, null,
                  containerIdStr, filename, format, length, true);
        }
      }
      String uri = "/" + containerId.toString() + "/logs/" + filename;
      String resURI = JOINER.join(
          LogWebServiceUtils.getAbsoluteNMWebAddress(yarnConf, nodeHttpAddress),
          NM_DOWNLOAD_URI_STR, uri);
      String query = req.getQueryString();
      if (query != null && !query.isEmpty()) {
        resURI += "?" + query;
      }
      Response.ResponseBuilder response =
          Response.status(HttpServletResponse.SC_TEMPORARY_REDIRECT);
      response.header("Location", resURI);
      return response.build();
    } else {
      return LogWebServiceUtils.createBadResponse(Response.Status.NOT_FOUND,
          "The application is not at Running or Finished State.");
    }
  }

  protected static class AppInfo {
    private YarnApplicationState appState;
    private String user;

    AppInfo(YarnApplicationState appState, String user) {
      this.appState = appState;
      this.user = user;
    }

    public YarnApplicationState getAppState() {
      return this.appState;
    }

    public String getUser() {
      return this.user;
    }
  }

  protected static class ContainerInfo {
    private String nodeHttpAddress;

    ContainerInfo(String nodeHttpAddress) {
      this.nodeHttpAddress = nodeHttpAddress;
    }

    public String getNodeHttpAddress() {
      return nodeHttpAddress;
    }
  }

  @VisibleForTesting protected TimelineEntity getEntity(String path,
      MultivaluedMap<String, String> params) throws IOException {
    ClientResponse resp =
        getClient().resource(base).path(path).queryParams(params)
            .accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
    if (resp == null
        || resp.getStatusInfo().getStatusCode() != ClientResponse.Status.OK
        .getStatusCode()) {
      String msg =
          "Response from the timeline reader server is " + ((resp == null) ?
              "null" :
              "not successful," + " HTTP error code: " + resp.getStatus()
                  + ", Server response:\n" + resp.getEntity(String.class));
      LOG.error(msg);
      throw new IOException(msg);
    }
    TimelineEntity entity = resp.getEntity(TimelineEntity.class);
    return entity;
  }

  private Client getClient() {
    if (webTimelineClient == null) {
      synchronized (LogWebService.class) {
        if (webTimelineClient == null) {
          webTimelineClient = createTimelineWebClient();
        }
      }
    }
    return webTimelineClient;
  }

  /**
   * Set user.name in non-secure mode to delegate to next rest call.
   */
  private void setUserName(MultivaluedMap<String, String> params, String user) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      params.add("user.name", user);
    }
  }
}