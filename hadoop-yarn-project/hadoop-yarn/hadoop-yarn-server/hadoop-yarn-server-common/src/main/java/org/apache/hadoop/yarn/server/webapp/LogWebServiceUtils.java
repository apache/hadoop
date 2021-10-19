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

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.util.YarnWebServiceUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

/**
 * Log web service utils class.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class LogWebServiceUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(LogWebServiceUtils.class);

  private LogWebServiceUtils() {
  }

  private static final Joiner DOT_JOINER = Joiner.on(". ");

  public static Response sendStreamOutputResponse(
      LogAggregationFileControllerFactory factory, ApplicationId appId,
      String appOwner, String nodeId, String containerIdStr, String fileName,
      String format, long bytes, boolean printEmptyLocalContainerLog) {
    String contentType = WebAppUtils.getDefaultLogContentType();
    if (format != null && !format.isEmpty()) {
      contentType = WebAppUtils.getSupportedLogContentType(format);
      if (contentType == null) {
        String errorMessage =
            "The valid values for the parameter : format " + "are "
                + WebAppUtils.listSupportedLogContentType();
        return Response.status(Response.Status.BAD_REQUEST).entity(errorMessage)
            .build();
      }
    }
    StreamingOutput stream = null;
    try {
      stream =
          getStreamingOutput(factory, appId, appOwner, nodeId, containerIdStr,
              fileName, bytes, printEmptyLocalContainerLog);
    } catch (Exception ex) {
      LOG.debug("Exception", ex);
      return createBadResponse(Response.Status.INTERNAL_SERVER_ERROR,
          ex.getMessage());
    }
    Response.ResponseBuilder response = Response.ok(stream);
    response.header("Content-Type", contentType);
    // Sending the X-Content-Type-Options response header with the value
    // nosniff will prevent Internet Explorer from MIME-sniffing a response
    // away from the declared content-type.
    response.header("X-Content-Type-Options", "nosniff");
    return response.build();
  }

  private static StreamingOutput getStreamingOutput(
      final LogAggregationFileControllerFactory factory,
      final ApplicationId appId, final String appOwner, final String nodeId,
      final String containerIdStr, final String logFile, final long bytes,
      final boolean printEmptyLocalContainerLog) throws IOException {
    StreamingOutput stream = new StreamingOutput() {

      @Override public void write(OutputStream os)
          throws IOException, WebApplicationException {
        ContainerLogsRequest request = new ContainerLogsRequest();
        request.setAppId(appId);
        request.setAppOwner(appOwner);
        request.setContainerId(containerIdStr);
        request.setBytes(bytes);
        request.setNodeId(nodeId);
        Set<String> logTypes = new HashSet<>();
        logTypes.add(logFile);
        request.setLogTypes(logTypes);
        boolean findLogs = factory.getFileControllerForRead(appId, appOwner)
            .readAggregatedLogs(request, os);
        if (!findLogs) {
          os.write(("Can not find logs for container:" + containerIdStr)
              .getBytes(Charset.forName("UTF-8")));
        } else {
          if (printEmptyLocalContainerLog) {
            StringBuilder sb = new StringBuilder();
            sb.append(containerIdStr + "\n");
            sb.append("LogAggregationType: " + ContainerLogAggregationType.LOCAL
                + "\n");
            sb.append("LogContents:\n");
            sb.append(getNoRedirectWarning() + "\n");
            os.write(sb.toString().getBytes(Charset.forName("UTF-8")));
          }
        }
      }
    };
    return stream;
  }

  public static String getNoRedirectWarning() {
    return "We do not have NodeManager web address, so we can not "
        + "re-direct the request to related NodeManager "
        + "for local container logs.";
  }

  public static void rewrapAndThrowException(Exception e) {
    if (e instanceof UndeclaredThrowableException) {
      rewrapAndThrowThrowable(e.getCause());
    } else {
      rewrapAndThrowThrowable(e);
    }
  }

  public static void rewrapAndThrowThrowable(Throwable t) {
    if (t instanceof AuthorizationException) {
      throw new ForbiddenException(t);
    } else {
      throw new WebApplicationException(t);
    }
  }

  public static long parseLongParam(String bytes) {
    if (bytes == null || bytes.isEmpty()) {
      return Long.MAX_VALUE;
    }
    return Long.parseLong(bytes);
  }

  public static Response createBadResponse(Response.Status status,
      String errMessage) {
    Response response = Response.status(status)
        .entity(DOT_JOINER.join(status.toString(), errMessage)).build();
    return response;
  }

  public static boolean isRunningState(YarnApplicationState appState) {
    return appState == YarnApplicationState.RUNNING;
  }

  protected static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUGI;
  }

  public static String getNMWebAddressFromRM(Configuration yarnConf,
      String nodeId)
      throws ClientHandlerException, UniformInterfaceException, JSONException {
    JSONObject nodeInfo =
        YarnWebServiceUtils.getNodeInfoFromRMWebService(yarnConf, nodeId)
            .getJSONObject("node");
    return nodeInfo.has("nodeHTTPAddress") ?
        nodeInfo.getString("nodeHTTPAddress") : null;
  }

  public static String getAbsoluteNMWebAddress(Configuration yarnConf,
      String nmWebAddress) {
    if (nmWebAddress.contains(WebAppUtils.HTTP_PREFIX) || nmWebAddress
        .contains(WebAppUtils.HTTPS_PREFIX)) {
      return nmWebAddress;
    }
    return WebAppUtils.getHttpSchemePrefix(yarnConf) + nmWebAddress;
  }
}