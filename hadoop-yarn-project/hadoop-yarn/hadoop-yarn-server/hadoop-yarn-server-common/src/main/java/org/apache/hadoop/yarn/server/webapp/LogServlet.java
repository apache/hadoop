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

package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Extracts aggregated logs and related information.
 * Used by various WebServices (AHS, ATS).
 */
public class LogServlet extends Configured {

  private static final Logger LOG = LoggerFactory
      .getLogger(LogServlet.class);

  private static final Joiner JOINER = Joiner.on("");
  private static final String NM_DOWNLOAD_URI_STR = "/ws/v1/node/containers";

  private LogAggregationFileControllerFactory factoryInstance = null;
  private final AppInfoProvider appInfoProvider;

  public LogServlet(Configuration conf, AppInfoProvider appInfoProvider) {
    super(conf);
    this.appInfoProvider = appInfoProvider;
  }

  private LogAggregationFileControllerFactory getOrCreateFactory() {
    if (factoryInstance != null) {
      return factoryInstance;
    } else {
      factoryInstance = new LogAggregationFileControllerFactory(getConf());
      return factoryInstance;
    }
  }

  @VisibleForTesting
  public String getNMWebAddressFromRM(String nodeId)
      throws ClientHandlerException, UniformInterfaceException, JSONException {
    return LogWebServiceUtils.getNMWebAddressFromRM(getConf(), nodeId);
  }

  private static List<ContainerLogsInfo> convertToContainerLogsInfo(
      List<ContainerLogMeta> containerLogMetas,
      boolean emptyLocalContainerLogMeta) {
    List<ContainerLogsInfo> containersLogsInfo = new ArrayList<>();
    for (ContainerLogMeta meta : containerLogMetas) {
      ContainerLogsInfo logInfo =
          new ContainerLogsInfo(meta, ContainerLogAggregationType.AGGREGATED);
      containersLogsInfo.add(logInfo);

      if (emptyLocalContainerLogMeta) {
        ContainerLogMeta emptyMeta =
            new ContainerLogMeta(logInfo.getContainerId(),
                logInfo.getNodeId() == null ? "N/A" : logInfo.getNodeId());
        ContainerLogsInfo empty =
            new ContainerLogsInfo(emptyMeta, ContainerLogAggregationType.LOCAL);
        containersLogsInfo.add(empty);
      }
    }
    return containersLogsInfo;
  }

  private static Response getContainerLogMeta(
      WrappedLogMetaRequest request, boolean emptyLocalContainerLogMeta) {
    try {
      List<ContainerLogMeta> containerLogMeta = request.getContainerLogMetas();
      if (containerLogMeta.isEmpty()) {
        throw new NotFoundException("Can not get log meta for request.");
      }
      List<ContainerLogsInfo> containersLogsInfo = convertToContainerLogsInfo(
          containerLogMeta, emptyLocalContainerLogMeta);

      GenericEntity<List<ContainerLogsInfo>> meta =
          new GenericEntity<List<ContainerLogsInfo>>(containersLogsInfo) {
          };
      Response.ResponseBuilder response = Response.ok(meta);
      // Sending the X-Content-Type-Options response header with the value
      // nosniff will prevent Internet Explorer from MIME-sniffing a response
      // away from the declared content-type.
      response.header("X-Content-Type-Options", "nosniff");
      return response.build();
    } catch (Exception ex) {
      LOG.debug("Exception during request", ex);
      throw new WebApplicationException(ex);
    }
  }

  /**
   * Validates whether the user has provided at least one query param for
   * the request. Also validates that if multiple query params are provided,
   * they do not contradict.
   */
  private void validateUserInput(ApplicationId applicationId,
      ApplicationAttemptId applicationAttemptId, ContainerId containerId) {
    // At least one field should be set
    if (applicationId == null && applicationAttemptId == null &&
        containerId == null) {
      throw new IllegalArgumentException("Should set application id, " +
          "application attempt id or container id.");
    }

    // container id should belong to the app attempt and the app id,
    // if provided
    if (containerId != null) {
      if (applicationAttemptId != null && !applicationAttemptId.equals(
          containerId.getApplicationAttemptId())) {
        throw new IllegalArgumentException(
            String.format(
                "Container %s does not belong to application attempt %s!",
                containerId, applicationAttemptId));
      }
      if (applicationId != null && !applicationId.equals(
          containerId.getApplicationAttemptId().getApplicationId())) {
        throw new IllegalArgumentException(
            String.format(
                "Container %s does not belong to application %s!",
                containerId, applicationId));
      }
    }

    // app attempt id should match the app id, if provided
    if (applicationAttemptId != null && applicationId != null &&
        !applicationId.equals(applicationAttemptId.getApplicationId())) {
      throw new IllegalArgumentException(
          String.format(
              "Application attempt %s does not belong to application %s!",
              applicationAttemptId, applicationId));
    }
  }

  public Response getLogsInfo(HttpServletRequest hsr, String appIdStr,
      String appAttemptIdStr, String containerIdStr, String nmId,
      boolean redirectedFromNode, boolean manualRedirection) {
    ApplicationId appId = null;
    if (appIdStr != null) {
      try {
        appId = ApplicationId.fromString(appIdStr);
      } catch (IllegalArgumentException iae) {
        throw new BadRequestException(iae);
      }
    }

    ApplicationAttemptId appAttemptId = null;
    if (appAttemptIdStr != null) {
      try {
        appAttemptId = ApplicationAttemptId.fromString(appAttemptIdStr);
      } catch (IllegalArgumentException iae) {
        throw new BadRequestException(iae);
      }
    }

    ContainerId containerId = null;
    if (containerIdStr != null) {
      try {
        containerId = ContainerId.fromString(containerIdStr);
      } catch (IllegalArgumentException iae) {
        throw new BadRequestException(iae);
      }
    }

    validateUserInput(appId, appAttemptId, containerId);

    WrappedLogMetaRequest.Builder logMetaRequestBuilder =
        WrappedLogMetaRequest.builder()
            .setApplicationId(appId)
            .setApplicationAttemptId(appAttemptId)
            .setContainerId(containerIdStr);

    return getContainerLogsInfo(hsr, logMetaRequestBuilder, nmId,
        redirectedFromNode, null, manualRedirection);
  }


  /**
   * Returns information about the logs for a specific container.
   *
   * @param req the {@link HttpServletRequest}
   * @param builder builder instance for the log meta request
   * @param nmId NodeManager id
   * @param redirectedFromNode whether the request was redirected
   * @param clusterId the id of the cluster
   * @param manualRedirection whether to return a response with a Location
   *                          instead of an automatic redirection
   * @return {@link Response} object containing information about the logs
   */
  public Response getContainerLogsInfo(HttpServletRequest req,
      WrappedLogMetaRequest.Builder builder,
      String nmId, boolean redirectedFromNode,
      String clusterId, boolean manualRedirection) {

    builder.setFactory(getOrCreateFactory());

    BasicAppInfo appInfo;
    try {
      appInfo = appInfoProvider.getApp(req, builder.getAppId(), clusterId);
    } catch (Exception ex) {
      LOG.warn("Could not obtain appInfo object from provider.", ex);
      // directly find logs from HDFS.
      return getContainerLogMeta(builder.build(), false);
    }
    // if the application finishes, directly find logs
    // from HDFS.
    if (Apps.isApplicationFinalState(appInfo.getAppState())) {
      return getContainerLogMeta(builder.build(), false);
    }
    if (LogWebServiceUtils.isRunningState(appInfo.getAppState())) {
      String appOwner = appInfo.getUser();
      builder.setAppOwner(appOwner);
      WrappedLogMetaRequest request = builder.build();

      String nodeHttpAddress = null;
      if (nmId != null && !nmId.isEmpty()) {
        try {
          nodeHttpAddress = getNMWebAddressFromRM(nmId);
        } catch (Exception ex) {
          LOG.info("Exception during getting NM web address.", ex);
        }
      }
      if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()) {
        if (request.getContainerId() != null) {
          try {
            nodeHttpAddress = appInfoProvider.getNodeHttpAddress(
                req, request.getAppId(), request.getAppAttemptId(),
                request.getContainerId().toString(), clusterId);
          } catch (Exception ex) {
            LOG.warn("Could not obtain node HTTP address from provider.", ex);
            // return log meta for the aggregated logs if exists.
            // It will also return empty log meta for the local logs.
            return getContainerLogMeta(request, true);
          }
        }
        // make sure nodeHttpAddress is not null and not empty. Otherwise,
        // we would only get log meta for aggregated logs instead of
        // re-directing the request
        if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()
            || redirectedFromNode) {
          // return log meta for the aggregated logs if exists.
          // It will also return empty log meta for the local logs.
          // If this is the redirect request from NM, we should not
          // re-direct the request back. Simply output the aggregated log meta.
          return getContainerLogMeta(request, true);
        }
      }
      ContainerId containerId = request.getContainerId();
      if (containerId == null) {
        throw new WebApplicationException(
            new Exception("Could not redirect to node, as app attempt or " +
                "application logs are requested."));
      }
      String uri = "/" + containerId.toString() + "/logs";
      String resURI = JOINER.join(
          LogWebServiceUtils.getAbsoluteNMWebAddress(getConf(),
              nodeHttpAddress),
          NM_DOWNLOAD_URI_STR, uri);
      String query = req.getQueryString();
      if (query != null && !query.isEmpty()) {
        resURI += "?" + query;
      }
      if (manualRedirection) {
        return createLocationResponse(resURI, createEmptyLogsInfo());
      }

      Response.ResponseBuilder response = Response.status(
          HttpServletResponse.SC_TEMPORARY_REDIRECT);
      response.header("Location", resURI);
      return response.build();
    } else {
      throw new NotFoundException(
          "The application is not at Running or Finished State.");
    }
  }

  /**
   * Creates a response with empty payload and a location header to preserve
   * API compatibility.
   *
   * @param uri redirection url
   * @param emptyPayload a payload that is discarded
   * @return a response with empty payload
   */
  private static <T> Response createLocationResponse(
      String uri, T emptyPayload) {
    Response.ResponseBuilder response = Response.status(
        HttpServletResponse.SC_OK).entity(emptyPayload);
    response.header("Location", uri);
    response.header("Access-Control-Expose-Headers", "Location");
    return response.build();
  }

  private static GenericEntity<List<ContainerLogsInfo>> createEmptyLogsInfo() {
    return new GenericEntity<List<ContainerLogsInfo>>(
        Collections.EMPTY_LIST, List.class);
  }

  private static StreamingOutput createEmptyStream() {
    return outputStream -> outputStream.write(
        "".getBytes(Charset.defaultCharset()));
  }

  /**
   * Returns an aggregated log file belonging to a container.
   *
   * @param req the {@link HttpServletRequest}
   * @param containerIdStr container id
   * @param filename the name of the file
   * @param format the format of the response
   * @param size the size of bytes of the log file that should be returned
   * @param nmId NodeManager id
   * @param redirectedFromNode whether the request was redirected
   * @param clusterId the id of the cluster
   * @param manualRedirection whether to return a response with a Location
   *                          instead of an automatic redirection
   * @return {@link Response} object containing information about the logs
   */
  public Response getLogFile(HttpServletRequest req, String containerIdStr,
      String filename, String format, String size, String nmId,
      boolean redirectedFromNode, String clusterId, boolean manualRedirection) {
    ContainerId containerId;
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException ex) {
      return LogWebServiceUtils.createBadResponse(Status.NOT_FOUND,
          "Invalid ContainerId: " + containerIdStr);
    }

    LogAggregationFileControllerFactory factory = getOrCreateFactory();

    final long length = LogWebServiceUtils.parseLongParam(size);

    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();
    BasicAppInfo appInfo;
    try {
      appInfo = appInfoProvider.getApp(req, appId.toString(), clusterId);
    } catch (Exception ex) {
      LOG.warn("Could not obtain appInfo object from provider.", ex);
      return LogWebServiceUtils
          .sendStreamOutputResponse(factory, appId, null, null, containerIdStr,
              filename, format, length, false);
    }
    String appOwner = appInfo.getUser();
    if (Apps.isApplicationFinalState(appInfo.getAppState())) {
      // directly find logs from HDFS.
      return LogWebServiceUtils
          .sendStreamOutputResponse(factory, appId, appOwner, null,
              containerIdStr, filename, format, length, false);
    }

    if (LogWebServiceUtils.isRunningState(appInfo.getAppState())) {
      String nodeHttpAddress = null;
      if (nmId != null && !nmId.isEmpty()) {
        try {
          nodeHttpAddress = getNMWebAddressFromRM(nmId);
        } catch (Exception ex) {
          LOG.debug("Exception happened during obtaining NM web address " +
              "from RM.", ex);
        }
      }
      if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()) {
        try {
          nodeHttpAddress = appInfoProvider.getNodeHttpAddress(
              req, appId.toString(),
              containerId.getApplicationAttemptId().toString(),
              containerId.toString(), clusterId);
        } catch (Exception ex) {
          LOG.warn("Could not obtain node HTTP address from provider.", ex);
          // output the aggregated logs
          return LogWebServiceUtils
              .sendStreamOutputResponse(factory, appId, appOwner, null,
                  containerIdStr, filename, format, length, true);
        }
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
          LogWebServiceUtils.getAbsoluteNMWebAddress(getConf(),
              nodeHttpAddress),
          NM_DOWNLOAD_URI_STR, uri);
      String query = req.getQueryString();
      if (query != null && !query.isEmpty()) {
        resURI += "?" + query;
      }


      if (manualRedirection) {
        return createLocationResponse(resURI, createEmptyStream());
      }

      Response.ResponseBuilder response = Response.status(
          HttpServletResponse.SC_TEMPORARY_REDIRECT);
      response.header("Location", resURI);
      return response.build();
    } else {
      return LogWebServiceUtils.createBadResponse(Status.NOT_FOUND,
          "The application is not at Running or Finished State.");
    }
  }

  public static WrappedLogMetaRequest.Builder createRequestFromContainerId(
      String containerIdStr) {
    WrappedLogMetaRequest.Builder logMetaRequestBuilder =
        WrappedLogMetaRequest.builder();
    try {
      logMetaRequestBuilder.setContainerId(containerIdStr);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Invalid container id: " + containerIdStr);
    }
    return logMetaRequestBuilder;
  }
}
