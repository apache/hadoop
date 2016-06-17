/** * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@Path("/ws/v1/node")
public class NMWebServices {
  private Context nmContext;
  private ResourceView rview;
  private WebApp webapp;
  private static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private @javax.ws.rs.core.Context 
    HttpServletRequest request;
  
  private @javax.ws.rs.core.Context 
    HttpServletResponse response;

  @javax.ws.rs.core.Context
    UriInfo uriInfo;

  @Inject
  public NMWebServices(final Context nm, final ResourceView view,
      final WebApp webapp) {
    this.nmContext = nm;
    this.rview = view;
    this.webapp = webapp;
  }

  private void init() {
    //clear content type
    response.setContentType(null);
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public NodeInfo get() {
    return getNodeInfo();
  }

  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public NodeInfo getNodeInfo() {
    init();
    return new NodeInfo(this.nmContext, this.rview);
  }

  @GET
  @Path("/apps")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppsInfo getNodeApps(@QueryParam("state") String stateQuery,
      @QueryParam("user") String userQuery) {
    init();
    AppsInfo allApps = new AppsInfo();
    for (Entry<ApplicationId, Application> entry : this.nmContext
        .getApplications().entrySet()) {

      AppInfo appInfo = new AppInfo(entry.getValue());
      if (stateQuery != null && !stateQuery.isEmpty()) {
        ApplicationState.valueOf(stateQuery);
        if (!appInfo.getState().equalsIgnoreCase(stateQuery)) {
          continue;
        }
      }
      if (userQuery != null) {
        if (userQuery.isEmpty()) {
          String msg = "Error: You must specify a non-empty string for the user";
          throw new BadRequestException(msg);
        }
        if (!appInfo.getUser().equals(userQuery)) {
          continue;
        }
      }
      allApps.add(appInfo);
    }
    return allApps;
  }

  @GET
  @Path("/apps/{appid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppInfo getNodeApp(@PathParam("appid") String appId) {
    init();
    ApplicationId id = WebAppUtils.parseApplicationId(recordFactory, appId);
    Application app = this.nmContext.getApplications().get(id);
    if (app == null) {
      throw new NotFoundException("app with id " + appId + " not found");
    }
    return new AppInfo(app);

  }

  @GET
  @Path("/containers")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ContainersInfo getNodeContainers(@javax.ws.rs.core.Context
      HttpServletRequest hsr) {
    init();
    ContainersInfo allContainers = new ContainersInfo();
    for (Entry<ContainerId, Container> entry : this.nmContext.getContainers()
        .entrySet()) {
      if (entry.getValue() == null) {
        // just skip it
        continue;
      }
      ContainerInfo info = new ContainerInfo(this.nmContext, entry.getValue(),
          uriInfo.getBaseUri().toString(), webapp.name(), hsr.getRemoteUser());
      allContainers.add(info);
    }
    return allContainers;
  }

  @GET
  @Path("/containers/{containerid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ContainerInfo getNodeContainer(@javax.ws.rs.core.Context
      HttpServletRequest hsr, @PathParam("containerid") String id) {
    ContainerId containerId = null;
    init();
    try {
      containerId = ContainerId.fromString(id);
    } catch (Exception e) {
      throw new BadRequestException("invalid container id, " + id);
    }

    Container container = nmContext.getContainers().get(containerId);
    if (container == null) {
      throw new NotFoundException("container with id, " + id + ", not found");
    }
    return new ContainerInfo(this.nmContext, container, uriInfo.getBaseUri()
        .toString(), webapp.name(), hsr.getRemoteUser());

  }
  
  /**
   * Returns the contents of a container's log file in plain text. 
   *
   * Only works for containers that are still in the NodeManager's memory, so
   * logs are no longer available after the corresponding application is no
   * longer running.
   * 
   * @param containerIdStr
   *    The container ID
   * @param filename
   *    The name of the log file
   * @param format
   *    The content type
   * @param size
   *    the size of the log file
   * @return
   *    The contents of the container's log file
   */
  @GET
  @Path("/containerlogs/{containerid}/{filename}")
  @Produces({ MediaType.TEXT_PLAIN })
  @Public
  @Unstable
  public Response getLogs(@PathParam("containerid") String containerIdStr,
      @PathParam("filename") String filename,
      @QueryParam("format") String format,
      @QueryParam("size") String size) {
    ContainerId containerId;
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException ex) {
      return Response.status(Status.BAD_REQUEST).build();
    }
    
    File logFile = null;
    try {
      logFile = ContainerLogsUtils.getContainerLogFile(
          containerId, filename, request.getRemoteUser(), nmContext);
    } catch (NotFoundException ex) {
      return Response.status(Status.NOT_FOUND).entity(ex.getMessage()).build();
    } catch (YarnException ex) {
      return Response.serverError().entity(ex.getMessage()).build();
    }
    final long bytes = parseLongParam(size);
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

    try {
      final FileInputStream fis = ContainerLogsUtils.openLogFileForRead(
          containerIdStr, logFile, nmContext);
      final long fileLength = logFile.length();

      StreamingOutput stream = new StreamingOutput() {
        @Override
        public void write(OutputStream os) throws IOException,
            WebApplicationException {
          try {
            int bufferSize = 65536;
            byte[] buf = new byte[bufferSize];
            long toSkip = 0;
            long totalBytesToRead = fileLength;
            long skipAfterRead = 0;
            if (bytes < 0) {
              long absBytes = Math.abs(bytes);
              if (absBytes < fileLength) {
                toSkip = fileLength - absBytes;
                totalBytesToRead = absBytes;
              }
              org.apache.hadoop.io.IOUtils.skipFully(fis, toSkip);
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
            int len = fis.read(buf, 0, toRead);
            while (len != -1 && curRead < totalBytesToRead) {
              os.write(buf, 0, len);
              curRead += len;

              pendingRead = totalBytesToRead - curRead;
              toRead = pendingRead > buf.length ? buf.length
                  : (int) pendingRead;
              len = fis.read(buf, 0, toRead);
            }
            org.apache.hadoop.io.IOUtils.skipFully(fis, skipAfterRead);
            os.flush();
          } finally {
            IOUtils.closeQuietly(fis);
          }
        }
      };
      ResponseBuilder resp = Response.ok(stream);
      resp.header("Content-Type", contentType);
      // Sending the X-Content-Type-Options response header with the value
      // nosniff will prevent Internet Explorer from MIME-sniffing a response
      // away from the declared content-type.
      resp.header("X-Content-Type-Options", "nosniff");
      return resp.build();
    } catch (IOException ex) {
      return Response.serverError().entity(ex.getMessage()).build();
    }
  }

  private long parseLongParam(String bytes) {
    if (bytes == null || bytes.isEmpty()) {
      return Long.MAX_VALUE;
    }
    return Long.parseLong(bytes);
  }
}
