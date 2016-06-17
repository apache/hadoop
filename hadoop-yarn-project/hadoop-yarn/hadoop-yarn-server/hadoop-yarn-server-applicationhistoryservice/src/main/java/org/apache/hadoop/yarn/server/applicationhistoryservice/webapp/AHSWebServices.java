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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@Path("/ws/v1/applicationhistory")
public class AHSWebServices extends WebServices {

  private static final String NM_DOWNLOAD_URI_STR =
      "/ws/v1/node/containerlogs";
  private static final Joiner JOINER = Joiner.on("");
  private static final Joiner DOT_JOINER = Joiner.on(". ");
  private final Configuration conf;

  @Inject
  public AHSWebServices(ApplicationBaseProtocol appBaseProt,
      Configuration conf) {
    super(appBaseProt);
    this.conf = conf;
  }

  @GET
  @Path("/about")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TimelineAbout about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    init(res);
    return TimelineUtils.createTimelineAbout("Generic History Service API");
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppsInfo get(@Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    return getApps(req, res, null, Collections.<String> emptySet(), null, null,
      null, null, null, null, null, null, Collections.<String> emptySet());
  }

  @GET
  @Path("/apps")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
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
    init(res);
    validateStates(stateQuery, statesQuery);
    return super.getApps(req, res, stateQuery, statesQuery, finalStatusQuery,
      userQuery, queueQuery, count, startedBegin, startedEnd, finishBegin,
      finishEnd, applicationTypes);
  }

  @GET
  @Path("/apps/{appid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public AppInfo getApp(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId) {
    init(res);
    return super.getApp(req, res, appId);
  }

  @GET
  @Path("/apps/{appid}/appattempts")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public AppAttemptsInfo getAppAttempts(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId) {
    init(res);
    return super.getAppAttempts(req, res, appId);
  }

  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public AppAttemptInfo getAppAttempt(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("appattemptid") String appAttemptId) {
    init(res);
    return super.getAppAttempt(req, res, appId, appAttemptId);
  }

  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}/containers")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public ContainersInfo getContainers(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("appattemptid") String appAttemptId) {
    init(res);
    return super.getContainers(req, res, appId, appAttemptId);
  }

  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}/containers/{containerid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Override
  public ContainerInfo getContainer(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("appattemptid") String appAttemptId,
      @PathParam("containerid") String containerId) {
    init(res);
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

  @GET
  @Path("/containerlogs/{containerid}/{filename}")
  @Produces({ MediaType.TEXT_PLAIN })
  @Public
  @Unstable
  public Response getLogs(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("containerid") String containerIdStr,
      @PathParam("filename") String filename,
      @QueryParam("format") String format,
      @QueryParam("size") String size) {
    init(res);
    ContainerId containerId;
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException ex) {
      return createBadResponse(Status.NOT_FOUND,
          "Invalid ContainerId: " + containerIdStr);
    }

    final long length = parseLongParam(size);

    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();
    AppInfo appInfo;
    try {
      appInfo = super.getApp(req, res, appId.toString());
    } catch (Exception ex) {
      // directly find logs from HDFS.
      return sendStreamOutputResponse(appId, null, null, containerIdStr,
          filename, format, length);
    }
    String appOwner = appInfo.getUser();

    ContainerInfo containerInfo;
    try {
      containerInfo = super.getContainer(
          req, res, appId.toString(),
          containerId.getApplicationAttemptId().toString(),
          containerId.toString());
    } catch (Exception ex) {
      if (isFinishedState(appInfo.getAppState())) {
        // directly find logs from HDFS.
        return sendStreamOutputResponse(appId, appOwner, null, containerIdStr,
            filename, format, length);
      }
      return createBadResponse(Status.INTERNAL_SERVER_ERROR,
          "Can not get ContainerInfo for the container: " + containerId);
    }
    String nodeId = containerInfo.getNodeId();
    if (isRunningState(appInfo.getAppState())) {
      String nodeHttpAddress = containerInfo.getNodeHttpAddress();
      String uri = "/" + containerId.toString() + "/" + filename;
      String resURI = JOINER.join(nodeHttpAddress, NM_DOWNLOAD_URI_STR, uri);
      String query = req.getQueryString();
      if (query != null && !query.isEmpty()) {
        resURI += "?" + query;
      }
      ResponseBuilder response = Response.status(
          HttpServletResponse.SC_TEMPORARY_REDIRECT);
      response.header("Location", resURI);
      return response.build();
    } else if (isFinishedState(appInfo.getAppState())) {
      return sendStreamOutputResponse(appId, appOwner, nodeId,
          containerIdStr, filename, format, length);
    } else {
      return createBadResponse(Status.NOT_FOUND,
          "The application is not at Running or Finished State.");
    }
  }

  private boolean isRunningState(YarnApplicationState appState) {
    return appState == YarnApplicationState.RUNNING;
  }

  private boolean isFinishedState(YarnApplicationState appState) {
    return appState == YarnApplicationState.FINISHED
        || appState == YarnApplicationState.FAILED
        || appState == YarnApplicationState.KILLED;
  }

  private Response createBadResponse(Status status, String errMessage) {
    Response response = Response.status(status)
        .entity(DOT_JOINER.join(status.toString(), errMessage)).build();
    return response;
  }

  private Response sendStreamOutputResponse(ApplicationId appId,
      String appOwner, String nodeId, String containerIdStr,
      String fileName, String format, long bytes) {
    String contentType = WebAppUtils.getDefaultLogContentType();
    if (format != null && !format.isEmpty()) {
      contentType = WebAppUtils.getSupportedLogContentType(format);
      if (contentType == null) {
        String errorMessage = "The valid values for the parameter : format "
            + "are " + WebAppUtils.listSupportedLogContentType();
        return Response.status(Status.BAD_REQUEST).entity(errorMessage)
            .build();
      }
    }
    StreamingOutput stream = null;
    try {
      stream = getStreamingOutput(appId, appOwner, nodeId,
          containerIdStr, fileName, bytes);
    } catch (Exception ex) {
      return createBadResponse(Status.INTERNAL_SERVER_ERROR,
          ex.getMessage());
    }
    if (stream == null) {
      return createBadResponse(Status.INTERNAL_SERVER_ERROR,
          "Can not get log for container: " + containerIdStr);
    }
    ResponseBuilder response = Response.ok(stream);
    response.header("Content-Type", contentType);
    // Sending the X-Content-Type-Options response header with the value
    // nosniff will prevent Internet Explorer from MIME-sniffing a response
    // away from the declared content-type.
    response.header("X-Content-Type-Options", "nosniff");
    return response.build();
  }

  private StreamingOutput getStreamingOutput(ApplicationId appId,
      String appOwner, final String nodeId, final String containerIdStr,
      final String logFile, final long bytes) throws IOException{
    String suffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(conf);
    org.apache.hadoop.fs.Path remoteRootLogDir = new org.apache.hadoop.fs.Path(
        conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    org.apache.hadoop.fs.Path qualifiedRemoteRootLogDir =
        FileContext.getFileContext(conf).makeQualified(remoteRootLogDir);
    FileContext fc = FileContext.getFileContext(
        qualifiedRemoteRootLogDir.toUri(), conf);
    org.apache.hadoop.fs.Path remoteAppDir = null;
    if (appOwner == null) {
      org.apache.hadoop.fs.Path toMatch = LogAggregationUtils
          .getRemoteAppLogDir(remoteRootLogDir, appId, "*", suffix);
      FileStatus[] matching  = fc.util().globStatus(toMatch);
      if (matching == null || matching.length != 1) {
        return null;
      }
      remoteAppDir = matching[0].getPath();
    } else {
      remoteAppDir = LogAggregationUtils
          .getRemoteAppLogDir(remoteRootLogDir, appId, appOwner, suffix);
    }
    final RemoteIterator<FileStatus> nodeFiles;
    nodeFiles = fc.listStatus(remoteAppDir);
    if (!nodeFiles.hasNext()) {
      return null;
    }

    StreamingOutput stream = new StreamingOutput() {

      @Override
      public void write(OutputStream os) throws IOException,
          WebApplicationException {
        byte[] buf = new byte[65535];
        boolean findLogs = false;
        while (nodeFiles.hasNext()) {
          final FileStatus thisNodeFile = nodeFiles.next();
          String nodeName = thisNodeFile.getPath().getName();
          if ((nodeId == null || nodeName.contains(LogAggregationUtils
              .getNodeString(nodeId))) && !nodeName.endsWith(
              LogAggregationUtils.TMP_FILE_SUFFIX)) {
            AggregatedLogFormat.LogReader reader = null;
            try {
              reader = new AggregatedLogFormat.LogReader(conf,
                  thisNodeFile.getPath());
              DataInputStream valueStream;
              LogKey key = new LogKey();
              valueStream = reader.next(key);
              while (valueStream != null && !key.toString()
                  .equals(containerIdStr)) {
                // Next container
                key = new LogKey();
                valueStream = reader.next(key);
              }
              if (valueStream == null) {
                continue;
              }
              while (true) {
                try {
                  String fileType = valueStream.readUTF();
                  String fileLengthStr = valueStream.readUTF();
                  long fileLength = Long.parseLong(fileLengthStr);
                  if (fileType.equalsIgnoreCase(logFile)) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("LogType:");
                    sb.append(fileType + "\n");
                    sb.append("Log Upload Time:");
                    sb.append(Times.format(System.currentTimeMillis()) + "\n");
                    sb.append("LogLength:");
                    sb.append(fileLengthStr + "\n");
                    sb.append("Log Contents:\n");
                    byte[] b = sb.toString().getBytes(
                        Charset.forName("UTF-8"));
                    os.write(b, 0, b.length);

                    long toSkip = 0;
                    long totalBytesToRead = fileLength;
                    long skipAfterRead = 0;
                    if (bytes < 0) {
                      long absBytes = Math.abs(bytes);
                      if (absBytes < fileLength) {
                        toSkip = fileLength - absBytes;
                        totalBytesToRead = absBytes;
                      }
                      org.apache.hadoop.io.IOUtils.skipFully(
                          valueStream, toSkip);
                    } else {
                      if (bytes < fileLength) {
                        totalBytesToRead = bytes;
                        skipAfterRead = fileLength - bytes;
                      }
                    }

                    long curRead = 0;
                    long pendingRead = totalBytesToRead - curRead;
                    int toRead = pendingRead > buf.length ? buf.length
                        : (int) pendingRead;
                    int len = valueStream.read(buf, 0, toRead);
                    while (len != -1 && curRead < totalBytesToRead) {
                      os.write(buf, 0, len);
                      curRead += len;

                      pendingRead = totalBytesToRead - curRead;
                      toRead = pendingRead > buf.length ? buf.length
                          : (int) pendingRead;
                      len = valueStream.read(buf, 0, toRead);
                    }
                    org.apache.hadoop.io.IOUtils.skipFully(
                        valueStream, skipAfterRead);
                    sb = new StringBuilder();
                    sb.append("\nEnd of LogType:" + fileType + "\n");
                    b = sb.toString().getBytes(Charset.forName("UTF-8"));
                    os.write(b, 0, b.length);
                    findLogs = true;
                  } else {
                    long totalSkipped = 0;
                    long currSkipped = 0;
                    while (currSkipped != -1 && totalSkipped < fileLength) {
                      currSkipped = valueStream.skip(
                          fileLength - totalSkipped);
                      totalSkipped += currSkipped;
                    }
                  }
                } catch (EOFException eof) {
                  break;
                }
              }
            } finally {
              if (reader != null) {
                reader.close();
              }
            }
          }
        }
        os.flush();
        if (!findLogs) {
          throw new IOException("Can not find logs for container:"
              + containerIdStr);
        }
      }
    };
    return stream;
  }

  private long parseLongParam(String bytes) {
    if (bytes == null || bytes.isEmpty()) {
      return Long.MAX_VALUE;
    }
    return Long.parseLong(bytes);
  }
}