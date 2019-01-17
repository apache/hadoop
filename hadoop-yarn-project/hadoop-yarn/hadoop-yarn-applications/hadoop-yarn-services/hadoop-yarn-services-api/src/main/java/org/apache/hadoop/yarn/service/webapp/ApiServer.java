/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.webapp;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ComponentContainers;
import org.apache.hadoop.yarn.service.api.records.ComponentState;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.api.records.ServiceStatus;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.conf.RestApiConstants;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.service.api.records.ServiceState.ACCEPTED;
import static org.apache.hadoop.yarn.service.api.records.ServiceState.CANCEL_UPGRADING;
import static org.apache.hadoop.yarn.service.conf.RestApiConstants.*;
import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes.*;

/**
 * The rest API endpoints for users to manage services on YARN.
 */
@Singleton
@Path(CONTEXT_ROOT)
public class ApiServer {

  public ApiServer() {
    super();
  }
  
  @Inject
  public ApiServer(Configuration conf) {
    super();
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(ApiServer.class);
  private static Configuration YARN_CONFIG = new YarnConfiguration();
  private ServiceClient serviceClientUnitTest;
  private boolean unitTest = false;

  static {
    init();
  }

  // initialize all the common resources - order is important
  private static void init() {
  }

  @GET
  @Path(VERSION)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
  public Response getVersion() {
    String version = VersionInfo.getBuildVersion();
    LOG.info(version);
    return Response.ok("{ \"hadoop_version\": \"" + version + "\"}").build();
  }

  @POST
  @Path(SERVICE_ROOT_PATH)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
  public Response createService(@Context HttpServletRequest request,
      Service service) {
    ServiceStatus serviceStatus = new ServiceStatus();
    try {
      UserGroupInformation ugi = getProxyUser(request);
      LOG.info("POST: createService = {} user = {}", service, ugi);
      if(service.getState()==ServiceState.STOPPED) {
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws YarnException, IOException {
            ServiceClient sc = getServiceClient();
            try {
              sc.init(YARN_CONFIG);
              sc.start();
              sc.actionBuild(service);
            } finally {
              sc.close();
            }
            return null;
          }
        });
        serviceStatus.setDiagnostics("Service " + service.getName() +
            " version " + service.getVersion() + " saved.");
      } else {
        ApplicationId applicationId = ugi
            .doAs(new PrivilegedExceptionAction<ApplicationId>() {
              @Override
              public ApplicationId run() throws IOException, YarnException {
                ServiceClient sc = getServiceClient();
                try {
                  sc.init(YARN_CONFIG);
                  sc.start();
                  ApplicationId applicationId = sc.actionCreate(service);
                  return applicationId;
                } finally {
                  sc.close();
                }
              }
            });
        serviceStatus.setDiagnostics("Application ID: " + applicationId);
      }
      serviceStatus.setState(ACCEPTED);
      serviceStatus.setUri(
          CONTEXT_ROOT + SERVICE_ROOT_PATH + "/" + service
              .getName());
      return formatResponse(Status.ACCEPTED, serviceStatus);
    } catch (AccessControlException e) {
      serviceStatus.setDiagnostics(e.getMessage());
      return formatResponse(Status.FORBIDDEN, e.getCause().getMessage());
    } catch (IllegalArgumentException e) {
      return formatResponse(Status.BAD_REQUEST, e.getMessage());
    } catch (IOException | InterruptedException e) {
      String message = "Failed to create service " + service.getName()
          + ": {}";
      LOG.error(message, e);
      return formatResponse(Status.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (UndeclaredThrowableException e) {
      String message = "Failed to create service " + service.getName()
          + ": {}";
      LOG.error(message, e);
      if (e.getCause().getMessage().contains("already exists")) {
        message = "Service name " + service.getName() + " is already taken.";
      } else {
        message = e.getCause().getMessage();
      }
      return formatResponse(Status.INTERNAL_SERVER_ERROR,
          message);
    }
  }

  @GET
  @Path(SERVICE_PATH)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
  public Response getService(@Context HttpServletRequest request,
      @PathParam(SERVICE_NAME) String appName) {
    ServiceStatus serviceStatus = new ServiceStatus();
    try {
      if (appName == null) {
        throw new IllegalArgumentException("Service name cannot be null.");
      }
      UserGroupInformation ugi = getProxyUser(request);
      LOG.info("GET: getService for appName = {} user = {}", appName, ugi);
      Service app = getServiceFromClient(ugi, appName);
      return Response.ok(app).build();
    } catch (AccessControlException e) {
      return formatResponse(Status.FORBIDDEN, e.getMessage());
    } catch (IllegalArgumentException e) {
      serviceStatus.setDiagnostics(e.getMessage());
      serviceStatus.setCode(ERROR_CODE_APP_NAME_INVALID);
      return Response.status(Status.NOT_FOUND).entity(serviceStatus)
          .build();
    } catch (FileNotFoundException e) {
      serviceStatus.setDiagnostics("Service " + appName + " not found");
      serviceStatus.setCode(ERROR_CODE_APP_NAME_INVALID);
      return Response.status(Status.NOT_FOUND).entity(serviceStatus)
          .build();
    } catch (IOException | InterruptedException e) {
      LOG.error("Get service failed: {}", e);
      return formatResponse(Status.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (UndeclaredThrowableException e) {
      LOG.error("Get service failed: {}", e);
      return formatResponse(Status.INTERNAL_SERVER_ERROR,
          e.getCause().getMessage());
    }
  }

  @DELETE
  @Path(SERVICE_PATH)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
  public Response deleteService(@Context HttpServletRequest request,
      @PathParam(SERVICE_NAME) String appName) {
    try {
      if (appName == null) {
        throw new IllegalArgumentException("Service name can not be null.");
      }
      UserGroupInformation ugi = getProxyUser(request);
      LOG.info("DELETE: deleteService for appName = {} user = {}",
          appName, ugi);
      return stopService(appName, true, ugi);
    } catch (AccessControlException e) {
      return formatResponse(Status.FORBIDDEN, e.getMessage());
    } catch (IllegalArgumentException e) {
      return formatResponse(Status.BAD_REQUEST, e.getMessage());
    } catch (UndeclaredThrowableException e) {
      LOG.error("Fail to stop service: {}", e);
      return formatResponse(Status.BAD_REQUEST,
          e.getCause().getMessage());
    } catch (YarnException | FileNotFoundException e) {
      return formatResponse(Status.NOT_FOUND, e.getMessage());
    } catch (Exception e) {
      LOG.error("Fail to stop service: {}", e);
      return formatResponse(Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  private Response stopService(String appName, boolean destroy,
      final UserGroupInformation ugi) throws Exception {
    int result = ugi.doAs(new PrivilegedExceptionAction<Integer>() {
      @Override
      public Integer run() throws Exception {
        int result = 0;
        ServiceClient sc = getServiceClient();
        try {
          sc.init(YARN_CONFIG);
          sc.start();
          Exception stopException = null;
          try {
            result = sc.actionStop(appName, destroy);
            if (result == EXIT_SUCCESS) {
              LOG.info("Successfully stopped service {}", appName);
            }
          } catch (Exception e) {
            LOG.info("Got exception stopping service", e);
            stopException = e;
          }
          if (destroy) {
            result = sc.actionDestroy(appName);
            if (result == EXIT_SUCCESS) {
              LOG.info("Successfully deleted service {}", appName);
            }
          } else {
            if (stopException != null) {
              throw stopException;
            }
          }
        } finally {
          sc.close();
        }
        return result;
      }
    });
    ServiceStatus serviceStatus = new ServiceStatus();
    if (destroy) {
      if (result == EXIT_SUCCESS) {
        serviceStatus.setDiagnostics("Successfully destroyed service " +
            appName);
      } else {
        if (result == EXIT_NOT_FOUND) {
          serviceStatus
              .setDiagnostics("Service " + appName + " doesn't exist");
          return formatResponse(Status.BAD_REQUEST, serviceStatus);
        } else {
          serviceStatus
              .setDiagnostics("Service " + appName + " error cleaning up " +
                  "registry");
          return formatResponse(Status.INTERNAL_SERVER_ERROR, serviceStatus);
        }
      }
    } else {
      if (result == EXIT_COMMAND_ARGUMENT_ERROR) {
        serviceStatus
            .setDiagnostics("Service " + appName + " is already stopped");
        return formatResponse(Status.BAD_REQUEST, serviceStatus);
      } else {
        serviceStatus.setDiagnostics("Successfully stopped service " + appName);
      }
    }
    return formatResponse(Status.OK, serviceStatus);
  }

  @PUT
  @Path(COMPONENTS_PATH)
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces({RestApiConstants.MEDIA_TYPE_JSON_UTF8, MediaType.TEXT_PLAIN})
  public Response updateComponents(@Context HttpServletRequest request,
      @PathParam(SERVICE_NAME) String serviceName,
      List<Component> requestComponents) {

    try {
      if (requestComponents == null || requestComponents.isEmpty()) {
        throw new YarnException("No components provided.");
      }
      UserGroupInformation ugi = getProxyUser(request);
      Set<String> compNamesToUpgrade = new HashSet<>();
      requestComponents.forEach(reqComp -> {
        if (reqComp.getState() != null &&
            reqComp.getState().equals(ComponentState.UPGRADING)) {
          compNamesToUpgrade.add(reqComp.getName());
        }
      });
      LOG.info("PUT: upgrade components {} for service {} " +
          "user = {}", compNamesToUpgrade, serviceName, ugi);
      return processComponentsUpgrade(ugi, serviceName, compNamesToUpgrade);
    } catch (AccessControlException e) {
      return formatResponse(Response.Status.FORBIDDEN, e.getMessage());
    } catch (YarnException e) {
      return formatResponse(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException | InterruptedException e) {
      return formatResponse(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage());
    } catch (UndeclaredThrowableException e) {
      return formatResponse(Response.Status.INTERNAL_SERVER_ERROR,
          e.getCause().getMessage());
    }
  }

  @PUT
  @Path(COMPONENT_PATH)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8",
              MediaType.TEXT_PLAIN  })
  public Response updateComponent(@Context HttpServletRequest request,
      @PathParam(SERVICE_NAME) String appName,
      @PathParam(COMPONENT_NAME) String componentName, Component component) {

    try {
      if (component == null) {
        throw new YarnException("No component data provided");
      }
      if (component.getName() != null
          && !component.getName().equals(componentName)) {
        String msg = "Component name in the request object ("
            + component.getName() + ") does not match that in the URI path ("
            + componentName + ")";
        throw new YarnException(msg);
      }
      UserGroupInformation ugi = getProxyUser(request);
      if (component.getState() != null &&
          component.getState().equals(ComponentState.UPGRADING)) {
        LOG.info("PUT: upgrade component {} for service {} " +
            "user = {}", component.getName(), appName, ugi);
        return processComponentsUpgrade(ugi, appName,
            Sets.newHashSet(componentName));
      }

      if (component.getNumberOfContainers() == null) {
        throw new YarnException("No container count provided");
      }
      if (component.getNumberOfContainers() < 0) {
        String message = "Invalid number of containers specified "
            + component.getNumberOfContainers();
        throw new YarnException(message);
      }
      Map<String, Long> original = ugi
          .doAs(new PrivilegedExceptionAction<Map<String, Long>>() {
            @Override
            public Map<String, Long> run() throws YarnException, IOException {
              ServiceClient sc = new ServiceClient();
              try {
                sc.init(YARN_CONFIG);
                sc.start();
                Map<String, Long> original = sc.flexByRestService(appName,
                    Collections.singletonMap(componentName,
                        component.getNumberOfContainers()));
                return original;
              } finally {
                sc.close();
              }
            }
          });
      ServiceStatus status = new ServiceStatus();
      status.setDiagnostics(
          "Updating component (" + componentName + ") size from " + original
              .get(componentName) + " to " + component.getNumberOfContainers());
      return formatResponse(Status.OK, status);
    } catch (AccessControlException e) {
      return formatResponse(Status.FORBIDDEN, e.getMessage());
    } catch (YarnException e) {
      return formatResponse(Status.BAD_REQUEST, e.getMessage());
    } catch (IOException | InterruptedException e) {
      return formatResponse(Status.INTERNAL_SERVER_ERROR,
          e.getMessage());
    } catch (UndeclaredThrowableException e) {
      return formatResponse(Status.INTERNAL_SERVER_ERROR,
          e.getCause().getMessage());
    }
  }

  @PUT
  @Path(SERVICE_PATH)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
  public Response updateService(@Context HttpServletRequest request,
      @PathParam(SERVICE_NAME) String appName,
      Service updateServiceData) {
    try {
      UserGroupInformation ugi = getProxyUser(request);
      LOG.info("PUT: updateService for app = {} with data = {} user = {}",
          appName, updateServiceData, ugi);
      // Ignore the app name provided in updateServiceData and always use
      // appName path param
      updateServiceData.setName(appName);

      if (updateServiceData.getState() != null
          && updateServiceData.getState() == ServiceState.FLEX) {
        return flexService(updateServiceData, ugi);
      }
      // For STOP the app should be running. If already stopped then this
      // operation will be a no-op. For START it should be in stopped state.
      // If already running then this operation will be a no-op.
      if (updateServiceData.getState() != null
          && updateServiceData.getState() == ServiceState.STOPPED) {
        return stopService(appName, false, ugi);
      }

      // If a START is requested
      if (updateServiceData.getState() != null
          && updateServiceData.getState() == ServiceState.STARTED) {
        return startService(appName, ugi);
      }

      // If an UPGRADE is requested
      if (updateServiceData.getState() != null && (
          updateServiceData.getState() == ServiceState.UPGRADING ||
              updateServiceData.getState() ==
                  ServiceState.UPGRADING_AUTO_FINALIZE) ||
          updateServiceData.getState() == ServiceState.EXPRESS_UPGRADING) {
        return upgradeService(updateServiceData, ugi);
      }

      // If CANCEL_UPGRADING is requested
      if (updateServiceData.getState() != null &&
          updateServiceData.getState() == CANCEL_UPGRADING) {
        return cancelUpgradeService(appName, ugi);
      }

      // If new lifetime value specified then update it
      if (updateServiceData.getLifetime() != null
          && updateServiceData.getLifetime() > 0) {
        return updateLifetime(appName, updateServiceData, ugi);
      }

      for (Component c : updateServiceData.getComponents()) {
        if (c.getDecommissionedInstances().size() > 0) {
          return decommissionInstances(updateServiceData, ugi);
        }
      }
    } catch (UndeclaredThrowableException e) {
      return formatResponse(Status.BAD_REQUEST,
          e.getCause().getMessage());
    } catch (AccessControlException e) {
      return formatResponse(Status.FORBIDDEN, e.getMessage());
    } catch (FileNotFoundException e) {
      String message = "Application is not found app: " + appName;
      LOG.error(message, e);
      return formatResponse(Status.NOT_FOUND, e.getMessage());
    } catch (YarnException e) {
      LOG.error(e.getMessage(), e);
      return formatResponse(Status.NOT_FOUND, e.getMessage());
    } catch (Exception e) {
      String message = "Error while performing operation for app: " + appName;
      LOG.error(message, e);
      return formatResponse(Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    // If nothing happens consider it a no-op
    return Response.status(Status.NO_CONTENT).build();
  }

  @PUT
  @Path(COMP_INSTANCE_LONG_PATH)
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces({RestApiConstants.MEDIA_TYPE_JSON_UTF8, MediaType.TEXT_PLAIN})
  public Response updateComponentInstance(@Context HttpServletRequest request,
      @PathParam(SERVICE_NAME) String serviceName,
      @PathParam(COMPONENT_NAME) String componentName,
      @PathParam(COMP_INSTANCE_NAME) String compInstanceName,
      Container reqContainer) {

    try {
      UserGroupInformation ugi = getProxyUser(request);
      LOG.info("PUT: update component instance {} for component = {}" +
              " service = {} user = {}", compInstanceName, componentName,
          serviceName, ugi);
      if (reqContainer == null) {
        throw new YarnException("No container data provided.");
      }
      Service service = getServiceFromClient(ugi, serviceName);
      Component component = service.getComponent(componentName);
      if (component == null) {
        throw new YarnException(String.format(
            "The component name in the URI path (%s) is invalid.",
            componentName));
      }

      Container liveContainer = component.getComponentInstance(
          compInstanceName);
      if (liveContainer == null) {
        throw new YarnException(String.format(
            "The component (%s) does not have a component instance (%s).",
            componentName, compInstanceName));
      }

      if (reqContainer.getState() != null
          && reqContainer.getState().equals(ContainerState.UPGRADING)) {
        return processContainersUpgrade(ugi, service,
            Lists.newArrayList(liveContainer));
      }
    } catch (AccessControlException e) {
      return formatResponse(Response.Status.FORBIDDEN, e.getMessage());
    } catch (YarnException e) {
      return formatResponse(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException | InterruptedException e) {
      return formatResponse(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage());
    } catch (UndeclaredThrowableException e) {
      return formatResponse(Response.Status.INTERNAL_SERVER_ERROR,
          e.getCause().getMessage());
    }
    return Response.status(Status.NO_CONTENT).build();
  }

  @PUT
  @Path(COMP_INSTANCES_PATH)
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces({RestApiConstants.MEDIA_TYPE_JSON_UTF8, MediaType.TEXT_PLAIN})
  public Response updateComponentInstances(@Context HttpServletRequest request,
      @PathParam(SERVICE_NAME) String serviceName,
      List<Container> requestContainers) {

    try {
      if (requestContainers == null || requestContainers.isEmpty()) {
        throw new YarnException("No containers provided.");
      }
      UserGroupInformation ugi = getProxyUser(request);
      List<String> toUpgrade = new ArrayList<>();
      for (Container reqContainer : requestContainers) {
        if (reqContainer.getState() != null &&
            reqContainer.getState().equals(ContainerState.UPGRADING)) {
          toUpgrade.add(reqContainer.getComponentInstanceName());
        }
      }

      if (!toUpgrade.isEmpty()) {
        Service service = getServiceFromClient(ugi, serviceName);
        LOG.info("PUT: upgrade component instances {} for service = {} " +
            "user = {}", toUpgrade, serviceName, ugi);
        List<Container> liveContainers = ServiceApiUtil
            .getLiveContainers(service, toUpgrade);

        return processContainersUpgrade(ugi, service, liveContainers);
      }
    } catch (AccessControlException e) {
      return formatResponse(Response.Status.FORBIDDEN, e.getMessage());
    } catch (YarnException e) {
      return formatResponse(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException | InterruptedException e) {
      return formatResponse(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage());
    } catch (UndeclaredThrowableException e) {
      return formatResponse(Response.Status.INTERNAL_SERVER_ERROR,
          e.getCause().getMessage());
    }
    return Response.status(Status.NO_CONTENT).build();
  }

  @GET
  @Path(COMP_INSTANCES_PATH)
  @Produces({RestApiConstants.MEDIA_TYPE_JSON_UTF8})
  public Response getComponentInstances(@Context HttpServletRequest request,
      @PathParam(SERVICE_NAME) String serviceName,
      @QueryParam(PARAM_COMP_NAME) List<String> componentNames,
      @QueryParam(PARAM_VERSION) String version,
      @QueryParam(PARAM_CONTAINER_STATE) List<String> containerStates) {
    try {
      UserGroupInformation ugi = getProxyUser(request);
      LOG.info("GET: component instances for service = {}, compNames in {}, " +
          "version = {}, containerStates in {}, user = {}", serviceName,
          Objects.toString(componentNames, "[]"), Objects.toString(version, ""),
          Objects.toString(containerStates, "[]"), ugi);

        List<ContainerState> containerStatesDe = containerStates.stream().map(
            ContainerState::valueOf).collect(Collectors.toList());

        return Response.ok(getContainers(ugi, serviceName, componentNames,
            version, containerStatesDe)).build();
    } catch (IllegalArgumentException iae) {
      return formatResponse(Status.BAD_REQUEST, "valid container states are: " +
          Arrays.toString(ContainerState.values()));
    } catch (AccessControlException e) {
      return formatResponse(Response.Status.FORBIDDEN, e.getMessage());
    } catch (IOException | InterruptedException e) {
      return formatResponse(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage());
    } catch (UndeclaredThrowableException e) {
      return formatResponse(Response.Status.INTERNAL_SERVER_ERROR,
          e.getCause().getMessage());
    }
  }

  private Response flexService(Service service, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    String appName = service.getName();
    Response response = Response.status(Status.BAD_REQUEST).build();
    Map<String, String> componentCountStrings = new HashMap<String, String>();
    for (Component c : service.getComponents()) {
      componentCountStrings.put(c.getName(),
          c.getNumberOfContainers().toString());
    }
    Integer result = ugi.doAs(new PrivilegedExceptionAction<Integer>() {

      @Override
      public Integer run() throws YarnException, IOException {
        int result = 0;
        ServiceClient sc = new ServiceClient();
        try {
          sc.init(YARN_CONFIG);
          sc.start();
          result = sc
              .actionFlex(appName, componentCountStrings);
          return Integer.valueOf(result);
        } finally {
          sc.close();
        }
      }
    });
    if (result == EXIT_SUCCESS) {
      String message = "Service " + appName + " is successfully flexed.";
      LOG.info(message);
      ServiceStatus status = new ServiceStatus();
      status.setDiagnostics(message);
      status.setState(ServiceState.ACCEPTED);
      response = formatResponse(Status.ACCEPTED, status);
    }
    return response;
  }

  private Response updateLifetime(String appName, Service updateAppData,
      final UserGroupInformation ugi) throws IOException,
      InterruptedException {
    String newLifeTime = ugi.doAs(new PrivilegedExceptionAction<String>() {
      @Override
      public String run() throws YarnException, IOException {
        ServiceClient sc = getServiceClient();
        try {
          sc.init(YARN_CONFIG);
          sc.start();
          String newLifeTime = sc.updateLifetime(appName,
              updateAppData.getLifetime());
          return newLifeTime;
        } finally {
          sc.close();
        }
      }
    });
    ServiceStatus status = new ServiceStatus();
    status.setDiagnostics(
        "Service (" + appName + ")'s lifeTime is updated to " + newLifeTime
            + ", " + updateAppData.getLifetime() + " seconds remaining");
    return formatResponse(Status.OK, status);
  }

  private Response startService(String appName,
      final UserGroupInformation ugi) throws IOException,
      InterruptedException {
    ApplicationId appId =
        ugi.doAs(new PrivilegedExceptionAction<ApplicationId>() {
          @Override public ApplicationId run()
              throws YarnException, IOException {
            ServiceClient sc = getServiceClient();
            try {
              sc.init(YARN_CONFIG);
              sc.start();
              ApplicationId appId = sc.actionStartAndGetId(appName);
              return appId;
            } finally {
              sc.close();
            }
          }
        });
    LOG.info("Successfully started service " + appName);
    ServiceStatus status = new ServiceStatus();
    status.setDiagnostics(
        "Service " + appName + " is successfully started with ApplicationId: "
            + appId);
    status.setState(ServiceState.ACCEPTED);
    return formatResponse(Status.OK, status);
  }

  private Response upgradeService(Service service,
      final UserGroupInformation ugi) throws IOException, InterruptedException {
    ServiceStatus status = new ServiceStatus();
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      ServiceClient sc = getServiceClient();
      try {
        sc.init(YARN_CONFIG);
        sc.start();
        if (service.getState().equals(ServiceState.EXPRESS_UPGRADING)) {
          sc.actionUpgradeExpress(service);
        } else {
          sc.initiateUpgrade(service);
        }
      } finally {
        sc.close();
      }
      return null;
    });
    LOG.info("Service {} version {} upgrade initialized", service.getName(),
        service.getVersion());
    status.setDiagnostics("Service " + service.getName() +
        " version " + service.getVersion() + " saved.");
    status.setState(ServiceState.ACCEPTED);
    return formatResponse(Status.ACCEPTED, status);
  }

  private Response cancelUpgradeService(String serviceName,
      final UserGroupInformation ugi) throws IOException, InterruptedException {
    int result = ugi.doAs((PrivilegedExceptionAction<Integer>) () -> {
      ServiceClient sc = getServiceClient();
      try {
        sc.init(YARN_CONFIG);
        sc.start();
        int exitCode = sc.actionCancelUpgrade(serviceName);
        return exitCode;
      } finally {
        sc.close();
      }
    });
    if (result == EXIT_SUCCESS) {
      ServiceStatus status = new ServiceStatus();
      LOG.info("Service {} cancelling upgrade", serviceName);
      status.setDiagnostics("Service " + serviceName +
          " cancelling upgrade.");
      status.setState(ServiceState.ACCEPTED);
      return formatResponse(Status.ACCEPTED, status);
    }
    return Response.status(Status.BAD_REQUEST).build();
  }

  private Response processComponentsUpgrade(UserGroupInformation ugi,
      String serviceName, Set<String> compNames) throws YarnException,
      IOException, InterruptedException {
    Service service = getServiceFromClient(ugi, serviceName);
    if (!service.getState().equals(ServiceState.UPGRADING) &&
        !service.getState().equals(ServiceState.UPGRADING_AUTO_FINALIZE)) {
      throw new YarnException(
          String.format("The upgrade of service %s has not been initiated.",
              service.getName()));
    }
    List<Container> containersToUpgrade = ServiceApiUtil
        .validateAndResolveCompsUpgrade(service, compNames);
    Integer result = invokeContainersUpgrade(ugi, service, containersToUpgrade);
    if (result == EXIT_SUCCESS) {
      ServiceStatus status = new ServiceStatus();
      status.setDiagnostics(
          "Upgrading components " + Joiner.on(',').join(compNames) + ".");
      return formatResponse(Response.Status.ACCEPTED, status);
    }
    // If result is not a success, consider it a no-op
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  private Response processContainersUpgrade(UserGroupInformation ugi,
      Service service, List<Container> containers) throws YarnException,
      IOException, InterruptedException {

    if (!service.getState().equals(ServiceState.UPGRADING) &&
        !service.getState().equals(ServiceState.UPGRADING_AUTO_FINALIZE)) {
      throw new YarnException(
          String.format("The upgrade of service %s has not been initiated.",
              service.getName()));
    }
    ServiceApiUtil.validateInstancesUpgrade(containers);
    Integer result = invokeContainersUpgrade(ugi, service, containers);
    if (result == EXIT_SUCCESS) {
      ServiceStatus status = new ServiceStatus();
      status.setDiagnostics(
          "Upgrading component instances " + containers.stream()
              .map(Container::getId).collect(Collectors.joining(",")) + ".");
      return formatResponse(Response.Status.ACCEPTED, status);
    }
    // If result is not a success, consider it a no-op
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  private int invokeContainersUpgrade(UserGroupInformation ugi,
      Service service, List<Container> containers) throws IOException,
      InterruptedException {
    return ugi.doAs((PrivilegedExceptionAction<Integer>) () -> {
      int result1;
      ServiceClient sc = getServiceClient();
      try {
        sc.init(YARN_CONFIG);
        sc.start();
        result1 = sc.actionUpgrade(service, containers);
      } finally {
        sc.close();
      }
      return result1;
    });
  }

  private Response decommissionInstances(Service service, UserGroupInformation
      ugi) throws IOException, InterruptedException {
    String appName = service.getName();
    Response response = Response.status(Status.BAD_REQUEST).build();

    List<String> instances = new ArrayList<>();
    for (Component c : service.getComponents()) {
      instances.addAll(c.getDecommissionedInstances());
    }
    Integer result = ugi.doAs(new PrivilegedExceptionAction<Integer>() {
      @Override
      public Integer run() throws YarnException, IOException {
        int result = 0;
        ServiceClient sc = new ServiceClient();
        try {
          sc.init(YARN_CONFIG);
          sc.start();
          result = sc
              .actionDecommissionInstances(appName, instances);
          return Integer.valueOf(result);
        } finally {
          sc.close();
        }
      }
    });
    if (result == EXIT_SUCCESS) {
      String message = "Service " + appName + " has successfully " +
          "decommissioned instances.";
      LOG.info(message);
      ServiceStatus status = new ServiceStatus();
      status.setDiagnostics(message);
      status.setState(ServiceState.ACCEPTED);
      response = formatResponse(Status.ACCEPTED, status);
    }
    return response;
  }

  private Service getServiceFromClient(UserGroupInformation ugi,
      String serviceName) throws IOException, InterruptedException {

    return ugi.doAs((PrivilegedExceptionAction<Service>) () -> {
      ServiceClient sc = getServiceClient();
      try {
        sc.init(YARN_CONFIG);
        sc.start();
        Service app1 = sc.getStatus(serviceName);
        return app1;
      } finally {
        sc.close();
      }
    });
  }

  private ComponentContainers[] getContainers(UserGroupInformation ugi,
      String serviceName, List<String> componentNames, String version,
      List<ContainerState> containerStates) throws IOException,
      InterruptedException {
    return ugi.doAs((PrivilegedExceptionAction<ComponentContainers[]>) () -> {
      ComponentContainers[] result;
      ServiceClient sc = getServiceClient();
      try {
        sc.init(YARN_CONFIG);
        sc.start();
        result = sc.getContainers(serviceName, componentNames, version,
            containerStates);
        return result;
      } finally {
        sc.close();
      }
    });
  }

  /**
   * Used by negative test case.
   *
   * @param mockServerClient - A mocked version of ServiceClient
   */
  public void setServiceClient(ServiceClient mockServerClient) {
    serviceClientUnitTest = mockServerClient;
    unitTest = true;
  }

  private ServiceClient getServiceClient() {
    if (unitTest) {
      return serviceClientUnitTest;
    } else {
      return new ServiceClient();
    }
  }

  /**
   * Configure impersonation callback.
   *
   * @param request - web request
   * @return - configured UGI class for proxy callback
   * @throws IOException - if user is not login.
   */
  private UserGroupInformation getProxyUser(HttpServletRequest request)
      throws AccessControlException {
    UserGroupInformation proxyUser;
    UserGroupInformation ugi;
    String remoteUser = request.getRemoteUser();
    try {
      if (UserGroupInformation.isSecurityEnabled()) {
        proxyUser = UserGroupInformation.getLoginUser();
        ugi = UserGroupInformation.createProxyUser(remoteUser, proxyUser);
      } else {
        ugi = UserGroupInformation.createRemoteUser(remoteUser);
      }
      return ugi;
    } catch (IOException e) {
      throw new AccessControlException(e.getCause());
    }
  }

  /**
   * Format HTTP response.
   *
   * @param status - HTTP Code
   * @param message - Diagnostic message
   * @return - HTTP response
   */
  private Response formatResponse(Status status, String message) {
    ServiceStatus entity = new ServiceStatus();
    entity.setDiagnostics(message);
    return formatResponse(status, entity);
  }

  /**
   * Format HTTP response.
   *
   * @param status - HTTP Code
   * @param entity - ServiceStatus object
   * @return - HTTP response
   */
  private Response formatResponse(Status status, ServiceStatus entity) {
    return Response.status(status).entity(entity).build();
  }
}
