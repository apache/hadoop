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
import java.nio.charset.Charset;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecord;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecords;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AuxiliaryServicesInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.LogToolUtils;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMContainerLogsInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.webapp.YarnWebServiceParams;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@Path("/ws/v1/node")
public class NMWebServices {
  private static final Logger LOG =
       LoggerFactory.getLogger(NMWebServices.class);
  private Context nmContext;
  private ResourceView rview;
  private WebApp webapp;
  private static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  private final String redirectWSUrl;
  private final LogAggregationFileControllerFactory factory;
  private boolean filterAppsByUser = false;

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
    this.redirectWSUrl = this.nmContext.getConf().get(
        YarnConfiguration.YARN_LOG_SERVER_WEBSERVICE_URL);
    this.factory = new LogAggregationFileControllerFactory(
        this.nmContext.getConf());
    this.filterAppsByUser = this.nmContext.getConf().getBoolean(
        YarnConfiguration.FILTER_ENTITY_LIST_BY_USER,
        YarnConfiguration.DEFAULT_DISPLAY_APPS_FOR_LOGGED_IN_USER);
  }

  public NMWebServices(final Context nm, final ResourceView view,
      final WebApp webapp, HttpServletResponse response) {
    this(nm, view, webapp);
    this.response = response;
  }

  private void init() {
    //clear content type
    response.setContentType(null);
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public NodeInfo get() {
    return getNodeInfo();
  }

  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public NodeInfo getNodeInfo() {
    init();
    return new NodeInfo(this.nmContext, this.rview);
  }

  @GET
  @Path("/apps")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public AppsInfo getNodeApps(@javax.ws.rs.core.Context HttpServletRequest hsr,
      @QueryParam("state") String stateQuery,
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

      // Allow only application-owner/admin for any type of access on the
      // application.
      if (filterAppsByUser
          && !hasAccess(appInfo.getUser(), entry.getKey(), hsr)) {
        continue;
      }

      allApps.add(appInfo);
    }
    return allApps;
  }

  @GET
  @Path("/apps/{appid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
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
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
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

      ApplicationId appId = entry.getKey().getApplicationAttemptId()
          .getApplicationId();
      // Allow only application-owner/admin for any type of access on the
      // application.
      if (filterAppsByUser
          && !hasAccess(entry.getValue().getUser(), appId, hsr)) {
        continue;
      }

      allContainers.add(info);
    }
    return allContainers;
  }

  @GET
  @Path("/containers/{containerid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
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
   * Returns log file's name as well as current file size for a container.
   *
   * @param hsr
   *    HttpServletRequest
   * @param res
   *    HttpServletResponse
   * @param containerIdStr
   *    The container ID
   * @return
   *    The log file's name and current file size
   */
  @GET
  @Path("/containers/{containerid}/logs")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public Response getContainerLogsInfo(
      @javax.ws.rs.core.Context HttpServletRequest hsr,
      @javax.ws.rs.core.Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr) {
    ContainerId containerId = null;
    init();
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException ex) {
      throw new BadRequestException("invalid container id, " + containerIdStr);
    }

    try {
      List<ContainerLogsInfo> containersLogsInfo = new ArrayList<>();
      containersLogsInfo.add(new NMContainerLogsInfo(
          this.nmContext, containerId,
          hsr.getRemoteUser(), ContainerLogAggregationType.LOCAL));
      // check whether we have aggregated logs in RemoteFS. If exists, show the
      // the log meta for the aggregated logs as well.
      ApplicationId appId = containerId.getApplicationAttemptId()
          .getApplicationId();
      Application app = this.nmContext.getApplications().get(appId);
      String appOwner = app == null ? null : app.getUser();
      try {
        ContainerLogsRequest logRequest = new ContainerLogsRequest();
        logRequest.setAppId(appId);
        logRequest.setAppOwner(appOwner);
        logRequest.setContainerId(containerIdStr);
        logRequest.setNodeId(this.nmContext.getNodeId().toString());
        List<ContainerLogMeta> containerLogMeta = factory
            .getFileControllerForRead(appId, appOwner)
            .readAggregatedLogsMeta(logRequest);
        if (!containerLogMeta.isEmpty()) {
          for (ContainerLogMeta logMeta : containerLogMeta) {
            containersLogsInfo.add(new ContainerLogsInfo(logMeta,
                ContainerLogAggregationType.AGGREGATED));
          }
        }
      } catch (IOException ex) {
        // Something wrong with we tries to access the remote fs for the logs.
        // Skip it and do nothing
        LOG.debug("{}", ex);
      }
      GenericEntity<List<ContainerLogsInfo>> meta = new GenericEntity<List<
          ContainerLogsInfo>>(containersLogsInfo){};
      ResponseBuilder resp = Response.ok(meta);
      // Sending the X-Content-Type-Options response header with the value
      // nosniff will prevent Internet Explorer from MIME-sniffing a response
      // away from the declared content-type.
      resp.header("X-Content-Type-Options", "nosniff");
      return resp.build();
    } catch (Exception ex) {
      if (redirectWSUrl == null || redirectWSUrl.isEmpty()) {
        throw new WebApplicationException(ex);
      }
      // redirect the request to the configured log server
      String redirectURI = "/containers/" + containerIdStr
          + "/logs";
      return createRedirectResponse(hsr, redirectWSUrl, redirectURI);
    }
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
  @Path("/containers/{containerid}/logs/{filename}")
  @Produces({ MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8 })
  @Public
  @Unstable
  public Response getContainerLogFile(
      @PathParam(YarnWebServiceParams.CONTAINER_ID)
      final String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME)
      String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT)
      String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE)
      String size) {
    return getLogs(containerIdStr, filename, format, size);
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
  @Produces({ MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8 })
  @Public
  @Unstable
  public Response getLogs(
      @PathParam(YarnWebServiceParams.CONTAINER_ID)
      final String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME)
      String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT)
      String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE)
      String size) {
    ContainerId tempContainerId;
    try {
      tempContainerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException ex) {
      return Response.status(Status.BAD_REQUEST).build();
    }
    final ContainerId containerId = tempContainerId;
    boolean tempIsRunning = false;
    // check what is the status for container
    try {
      Container container = nmContext.getContainers().get(containerId);
      tempIsRunning = (container.getContainerState() == ContainerState.RUNNING);
    } catch (Exception ex) {
      // This NM does not have this container any more. We
      // assume the container has already finished.
      LOG.debug("Can not find the container:{} in this node.",
          containerId);
    }
    final boolean isRunning = tempIsRunning;
    File logFile = null;
    try {
      logFile = ContainerLogsUtils.getContainerLogFile(
          containerId, filename, request.getRemoteUser(), nmContext);
    } catch (NotFoundException ex) {
      if (redirectWSUrl == null || redirectWSUrl.isEmpty()) {
        return Response.status(Status.NOT_FOUND).entity(ex.getMessage())
            .build();
      }
      // redirect the request to the configured log server
      String redirectURI = "/containers/" + containerIdStr
          + "/logs/" + filename;
      return createRedirectResponse(request, redirectWSUrl, redirectURI);
    } catch (YarnException ex) {
      return Response.serverError().entity(ex.getMessage()).build();
    }
    final long bytes = parseLongParam(size);
    final String lastModifiedTime = Times.format(logFile.lastModified());
    final String outputFileName = filename;
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
            LogToolUtils.outputContainerLogThroughZeroCopy(
                containerId.toString(), nmContext.getNodeId().toString(),
                outputFileName, fileLength, bytes, lastModifiedTime, fis, os,
                ContainerLogAggregationType.LOCAL);
            StringBuilder sb = new StringBuilder();
            String endOfFile = "End of LogType:" + outputFileName;
            sb.append(endOfFile + ".");
            if (isRunning) {
              sb.append("This log file belongs to a running container ("
                  + containerIdStr + ") and so may not be complete." + "\n");
            } else {
              sb.append("\n");
            }
            sb.append(StringUtils.repeat("*", endOfFile.length() + 50)
                + "\n\n");
            os.write(sb.toString().getBytes(Charset.forName("UTF-8")));
            // If we have aggregated logs for this container,
            // output the aggregation logs as well.
            ApplicationId appId = containerId.getApplicationAttemptId()
                .getApplicationId();
            Application app = nmContext.getApplications().get(appId);
            String appOwner = app == null ? null : app.getUser();
            try {
              ContainerLogsRequest logRequest = new ContainerLogsRequest();
              logRequest.setAppId(appId);
              logRequest.setAppOwner(appOwner);
              logRequest.setContainerId(containerId.toString());
              logRequest.setNodeId(nmContext.getNodeId().toString());
              logRequest.setBytes(bytes);
              Set<String> logTypes = new HashSet<>();
              logTypes.add(outputFileName);
              logRequest.setLogTypes(logTypes);
              factory.getFileControllerForRead(appId, appOwner)
                  .readAggregatedLogs(logRequest, os);
            } catch (Exception ex) {
              // Something wrong when we try to access the aggregated log.
              if (LOG.isDebugEnabled()) {
                LOG.debug("Can not access the aggregated log for "
                    + "the container:" + containerId);
                LOG.debug(ex.getMessage());
              }
            }
          } finally {
            IOUtils.closeQuietly(fis);
          }
        }
      };
      ResponseBuilder resp = Response.ok(stream);
      resp.header("Content-Type", contentType + "; " + JettyUtils.UTF_8);
      // Sending the X-Content-Type-Options response header with the value
      // nosniff will prevent Internet Explorer from MIME-sniffing a response
      // away from the declared content-type.
      resp.header("X-Content-Type-Options", "nosniff");
      return resp.build();
    } catch (IOException ex) {
      return Response.serverError().entity(ex.getMessage()).build();
    }
  }

  @GET
  @Path("/resources/{resourcename}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
                MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public Object getNMResourceInfo(
      @PathParam("resourcename")
          String resourceName) throws YarnException {
    init();
    ResourcePluginManager rpm = this.nmContext.getResourcePluginManager();
    if (rpm != null && rpm.getNameToPlugins() != null) {
      ResourcePlugin plugin = rpm.getNameToPlugins().get(resourceName);
      if (plugin != null) {
        NMResourceInfo nmResourceInfo = plugin.getNMResourceInfo();
        if (nmResourceInfo != null) {
          return nmResourceInfo;
        }
      }
    }

    return new NMResourceInfo();
  }

  @GET
  @Path("/auxiliaryservices")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public AuxiliaryServicesInfo getAuxiliaryServices(@javax.ws.rs.core.Context
      HttpServletRequest hsr) {
    init();
    if (!this.nmContext.getAuxServices().isManifestEnabled()) {
      throw new BadRequestException("Auxiliary services manifest is not " +
          "enabled");
    }
    AuxiliaryServicesInfo auxiliaryServices = new AuxiliaryServicesInfo();
    Collection<AuxServiceRecord> loadedServices = nmContext.getAuxServices()
        .getServiceRecords();
    if (loadedServices != null) {
      auxiliaryServices.addAll(loadedServices);
    }
    return auxiliaryServices;
  }

  @PUT
  @Path("/auxiliaryservices")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public Response putAuxiliaryServices(@javax.ws.rs.core.Context
      HttpServletRequest req, AuxServiceRecords services) {
    init();
    if (!this.nmContext.getAuxServices().isManifestEnabled()) {
      throw new BadRequestException("Auxiliary services manifest is not " +
          "enabled");
    }
    if (!hasAdminAccess(req)) {
      return Response.status(Status.FORBIDDEN).build();
    }
    if (services == null) {
      return Response.status(Status.BAD_REQUEST).build();
    }
    try {
      nmContext.getAuxServices().reload(services);
    } catch (Exception e) {
      LOG.error("Fail to reload auxiliary services, reason: ", e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e).build();
    }
    return Response.ok().build();
  }

  @PUT
  @Path("/yarn/sysfs/{user}/{appId}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
                MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public Response syncYarnSysFS(@javax.ws.rs.core.Context
      HttpServletRequest req,
      @PathParam("user") String user,
      @PathParam("appId") String appId,
      String spec) {
    if (UserGroupInformation.isSecurityEnabled()) {
      if (!req.getRemoteUser().equals(user)) {
        return Response.status(Status.FORBIDDEN).build();
      }
    }
    try {
      nmContext.getContainerExecutor().updateYarnSysFS(nmContext, user, appId,
          spec);
    } catch (IOException | ServiceStateException e) {
      LOG.error("Fail to sync yarn sysfs for application ID: {}, reason: ",
          appId, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e).build();
    }
    return Response.ok().build();
  }

  private long parseLongParam(String bytes) {
    if (bytes == null || bytes.isEmpty()) {
      return Long.MAX_VALUE;
    }
    return Long.parseLong(bytes);
  }

  private Response createRedirectResponse(HttpServletRequest httpRequest,
      String redirectWSUrlPrefix, String uri) {
    // redirect the request to the configured log server
    StringBuilder redirectPath = new StringBuilder();
    if (redirectWSUrlPrefix.endsWith("/")) {
      redirectWSUrlPrefix = redirectWSUrlPrefix.substring(0,
          redirectWSUrlPrefix.length() - 1);
    }
    redirectPath.append(redirectWSUrlPrefix + uri);
    // append all the request query parameters except nodeId parameter
    String requestParams = WebAppUtils.removeQueryParams(httpRequest,
        YarnWebServiceParams.NM_ID);
    if (requestParams != null && !requestParams.isEmpty()) {
      redirectPath.append("?" + requestParams + "&"
          + YarnWebServiceParams.REDIRECTED_FROM_NODE + "=true");
    } else {
      redirectPath.append("?" + YarnWebServiceParams.REDIRECTED_FROM_NODE
          + "=true");
    }
    ResponseBuilder res = Response.status(
        HttpServletResponse.SC_TEMPORARY_REDIRECT);
    res.header("Location", redirectPath.toString());
    return res.build();
  }

  protected Boolean hasAccess(String user, ApplicationId appId,
      HttpServletRequest hsr) {
    // Check for the authorization.
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);

    if (callerUGI != null && !(this.nmContext.getApplicationACLsManager()
        .checkAccess(callerUGI, ApplicationAccessType.VIEW_APP, user, appId))) {
      return false;
    }
    return true;
  }

  protected Boolean hasAdminAccess(HttpServletRequest hsr) {
    // Check for the authorization.
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);

    if (callerUGI == null) {
      return false;
    }

    if (!this.nmContext.getApplicationACLsManager().isAdmin(callerUGI)) {
      return false;
    }

    return true;
  }

  private UserGroupInformation getCallerUserGroupInformation(
      HttpServletRequest hsr, boolean usePrincipal) {

    String remoteUser = hsr.getRemoteUser();
    if (usePrincipal) {
      Principal princ = hsr.getUserPrincipal();
      remoteUser = princ == null ? null : princ.getName();
    }

    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }

    return callerUGI;
  }
}
