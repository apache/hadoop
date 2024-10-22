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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.security.PrivilegedExceptionAction;

/**
 * Support only ATSv2 client only.
 */
@Singleton
@Path("/ws/v2/applicationlog")
public class LogWebService implements AppInfoProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(LogWebService.class);
  private static final String RESOURCE_URI_STR_V2 = "/ws/v2/timeline/";
  private static final String NM_DOWNLOAD_URI_STR = "/ws/v1/node/containers";
  private static final Joiner JOINER = Joiner.on("");
  private static Configuration yarnConf = new YarnConfiguration();
  private static LogAggregationFileControllerFactory factory;
  private static String base;
  private static String defaultClusterid;

  private final LogServlet logServlet;
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
    LOG.info("Initialized LogWeService with clusterid {} for URI: {}.",
        defaultClusterid, base);
  }

  public LogWebService() {
    this.logServlet = new LogServlet(yarnConf, this);
  }

  private Client createTimelineWebClient() {
    ClientConfig cfg = new ClientConfig();
    cfg.register(YarnJacksonJaxbJsonProvider.class);

    HttpUrlConnectorProvider httpUrlConnectorProvider =
        new HttpUrlConnectorProvider().connectionFactory(url -> {
          AuthenticatedURL.Token token = new AuthenticatedURL.Token();
          HttpURLConnection conn;
          try {
            conn = new AuthenticatedURL().openConnection(url, token);
            LOG.info("LogWeService:Connecetion created.");
          } catch (AuthenticationException e) {
            throw new IOException(e);
          }
          return conn;
        });
    cfg.connectorProvider(httpUrlConnectorProvider);

    return ClientBuilder.newBuilder().withConfig(cfg).build();
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
   * @param clusterId          clusterId the id of the cluster
   * @param manualRedirection  whether to return a response with a Location
   *                           instead of an automatic redirection
   * @return The log file's name and current file size
   */
  @GET
  @Path("/containers/{containerid}/logs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getContainerLogsInfo(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    initForReadableEndpoints(res);

    WrappedLogMetaRequest.Builder logMetaRequestBuilder =
        LogServlet.createRequestFromContainerId(containerIdStr);

    return logServlet.getContainerLogsInfo(req, logMetaRequestBuilder, nmId,
        redirectedFromNode, clusterId, manualRedirection);
  }

  @Override
  public String getNodeHttpAddress(HttpServletRequest req, String appId,
      String appAttemptId, String containerId, String clusterId) {
    UserGroupInformation callerUGI = LogWebServiceUtils.getUser(req);
    String cId = clusterId != null ? clusterId : defaultClusterid;
    MultivaluedMap<String, String> params = new MultivaluedHashMap();
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
    return (String) conEntity.getInfo()
        .get(ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO);
  }

  @Override
  public BasicAppInfo getApp(HttpServletRequest req, String appId,
      String clusterId) {
    UserGroupInformation callerUGI = LogWebServiceUtils.getUser(req);

    String cId = clusterId != null ? clusterId : defaultClusterid;
    MultivaluedMap<String, String> params = new MultivaluedHashMap();
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
    return new BasicAppInfo(appState, appOwner);
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
   * @param clusterId          the id of the cluster
   * @param manualRedirection  whether to return a response with a Location
   *                           instead of an automatic redirection
   * @return The contents of the container's log file
   */
  @GET
  @Path("/containers/{containerid}/logs/{filename}")
  @Produces({ MediaType.TEXT_PLAIN })
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public Response getContainerLogFile(
      @Context HttpServletRequest req, @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
          boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    return getLogs(req, res, containerIdStr, filename, format, size, nmId,
        redirectedFromNode, clusterId, manualRedirection);
  }

  //TODO: YARN-4993: Refactory ContainersLogsBlock, AggregatedLogsBlock and
  //      container log webservice introduced in AHS to minimize
  //      the duplication.
  @GET
  @Path("/containerlogs/{containerid}/{filename}")
  @Produces({ MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8 })
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public Response getLogs(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    initForReadableEndpoints(res);
    return logServlet.getLogFile(req, containerIdStr, filename, format, size,
        nmId, redirectedFromNode, clusterId, manualRedirection);
  }

  @VisibleForTesting protected TimelineEntity getEntity(String path,
      MultivaluedMap<String, String> params) throws IOException {
    Response resp =
        getClient().target(base).path(path)
            .request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    if (resp == null
        || resp.getStatusInfo().getStatusCode() != Response.Status.OK
        .getStatusCode()) {
      String msg =
          "Response from the timeline reader server is " + ((resp == null) ?
              "null" :
              "not successful," + " HTTP error code: " + resp.getStatus()
                  + ", Server response:\n" + resp.readEntity(String.class));
      LOG.error(msg);
      throw new IOException(msg);
    }
    TimelineEntity entity = resp.readEntity(TimelineEntity.class);
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