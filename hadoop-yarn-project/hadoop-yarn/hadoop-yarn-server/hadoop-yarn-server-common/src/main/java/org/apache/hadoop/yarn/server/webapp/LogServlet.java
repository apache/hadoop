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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * Extracts aggregated logs and related information.
 * Used by various WebServices (AHS, ATS).
 */
public class LogServlet extends Configured {
  private static final Logger LOG = LoggerFactory
      .getLogger(LogServlet.class);

  private static final Joiner JOINER = Joiner.on("");
  private static final String NM_DOWNLOAD_URI_STR = "/ws/v1/node/containers";

  private final LogAggregationFileControllerFactory factory;
  private final AppInfoProvider appInfoProvider;

  public LogServlet(Configuration conf, AppInfoProvider appInfoProvider) {
    super(conf);
    this.factory = new LogAggregationFileControllerFactory(conf);
    this.appInfoProvider = appInfoProvider;
  }

  @VisibleForTesting
  public String getNMWebAddressFromRM(String nodeId)
      throws ClientHandlerException, UniformInterfaceException, JSONException {
    return LogWebServiceUtils.getNMWebAddressFromRM(getConf(), nodeId);
  }

  /**
   * Returns information about the logs for a specific container.
   *
   * @param req the {@link HttpServletRequest}
   * @param containerIdStr container id
   * @param nmId NodeManager id
   * @param redirectedFromNode whether the request was redirected
   * @param clusterId the id of the cluster
   * @return {@link Response} object containing information about the logs
   */
  public Response getContainerLogsInfo(HttpServletRequest req,
      String containerIdStr, String nmId, boolean redirectedFromNode,
      String clusterId) {
    ContainerId containerId = null;
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("invalid container id, " + containerIdStr);
    }

    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();
    BasicAppInfo appInfo;
    try {
      appInfo = appInfoProvider.getApp(req, appId.toString(), clusterId);
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
          nodeHttpAddress = getNMWebAddressFromRM(nmId);
        } catch (Exception ex) {
          LOG.info("Exception during getting NM web address.", ex);
        }
      }
      if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()) {
        try {
          nodeHttpAddress = appInfoProvider.getNodeHttpAddress(
              req, appId.toString(),
              containerId.getApplicationAttemptId().toString(),
              containerId.toString(), clusterId);
        } catch (Exception ex) {
          // return log meta for the aggregated logs if exists.
          // It will also return empty log meta for the local logs.
          return LogWebServiceUtils
              .getContainerLogMeta(factory, appId, appOwner, null,
                  containerIdStr, true);
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
          return LogWebServiceUtils
              .getContainerLogMeta(factory, appId, appOwner, null,
                  containerIdStr, true);
        }
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
   * @return {@link Response} object containing information about the logs
   */
  public Response getLogFile(HttpServletRequest req, String containerIdStr,
      String filename, String format, String size, String nmId,
      boolean redirectedFromNode, String clusterId) {
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
    BasicAppInfo appInfo;
    try {
      appInfo = appInfoProvider.getApp(req, appId.toString(), clusterId);
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
      Response.ResponseBuilder response = Response.status(
          HttpServletResponse.SC_TEMPORARY_REDIRECT);
      response.header("Location", resURI);
      return response.build();
    } else {
      return LogWebServiceUtils.createBadResponse(Status.NOT_FOUND,
          "The application is not at Running or Finished State.");
    }
  }
}
