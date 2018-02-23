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
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.api.records.ServiceStatus;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.service.api.records.ServiceState.ACCEPTED;
import static org.apache.hadoop.yarn.service.conf.RestApiConstants.*;
import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes.EXIT_SUCCESS;

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
            sc.init(YARN_CONFIG);
            sc.start();
            sc.actionBuild(service);
            sc.close();
            return null;
          }
        });
        serviceStatus.setDiagnostics("Service "+service.getName() +
            " saved.");
      } else {
        ApplicationId applicationId = ugi
            .doAs(new PrivilegedExceptionAction<ApplicationId>() {
              @Override
              public ApplicationId run() throws IOException, YarnException {
                ServiceClient sc = getServiceClient();
                sc.init(YARN_CONFIG);
                sc.start();
                ApplicationId applicationId = sc.actionCreate(service);
                sc.close();
                return applicationId;
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
      return formatResponse(Status.INTERNAL_SERVER_ERROR,
          e.getCause().getMessage());
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
        throw new IllegalArgumentException("Service name can not be null.");
      }
      UserGroupInformation ugi = getProxyUser(request);
      LOG.info("GET: getService for appName = {} user = {}", appName, ugi);
      Service app = ugi.doAs(new PrivilegedExceptionAction<Service>() {
        @Override
        public Service run() throws IOException, YarnException {
          ServiceClient sc = getServiceClient();
          sc.init(YARN_CONFIG);
          sc.start();
          Service app = sc.getStatus(appName);
          sc.close();
          return app;
        }
      });
      return Response.ok(app).build();
    } catch (AccessControlException e) {
      return formatResponse(Status.FORBIDDEN, e.getMessage());
    } catch (IllegalArgumentException |
        FileNotFoundException e) {
      serviceStatus.setDiagnostics(e.getMessage());
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
    } catch (IOException | InterruptedException e) {
      LOG.error("Fail to stop service: {}", e);
      return formatResponse(Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  private Response stopService(String appName, boolean destroy,
      final UserGroupInformation ugi) throws IOException,
      InterruptedException, YarnException, FileNotFoundException {
    ugi.doAs(new PrivilegedExceptionAction<Integer>() {
      @Override
      public Integer run() throws IOException, YarnException,
          FileNotFoundException {
        int result = 0;
        ServiceClient sc = getServiceClient();
        sc.init(YARN_CONFIG);
        sc.start();
        result = sc.actionStop(appName, destroy);
        if (destroy) {
          result = sc.actionDestroy(appName);
          LOG.info("Successfully deleted service {}", appName);
        } else {
          LOG.info("Successfully stopped service {}", appName);
        }
        sc.close();
        return result;
      }
    });
    ServiceStatus serviceStatus = new ServiceStatus();
    if (destroy) {
      serviceStatus.setDiagnostics("Successfully destroyed service " +
          appName);
    } else {
      serviceStatus.setDiagnostics("Successfully stopped service " +
          appName);
    }
    return formatResponse(Status.OK, serviceStatus);
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
      if (component.getNumberOfContainers() == null) {
        throw new YarnException("No container count provided");
      }
      if (component.getNumberOfContainers() < 0) {
        String message = "Invalid number of containers specified "
            + component.getNumberOfContainers();
        throw new YarnException(message);
      }
      UserGroupInformation ugi = getProxyUser(request);
      Map<String, Long> original = ugi
          .doAs(new PrivilegedExceptionAction<Map<String, Long>>() {
            @Override
            public Map<String, Long> run() throws YarnException, IOException {
              ServiceClient sc = new ServiceClient();
              sc.init(YARN_CONFIG);
              sc.start();
              Map<String, Long> original = sc.flexByRestService(appName,
                  Collections.singletonMap(componentName,
                      component.getNumberOfContainers()));
              sc.close();
              return original;
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

      // If new lifetime value specified then update it
      if (updateServiceData.getLifetime() != null
          && updateServiceData.getLifetime() > 0) {
        return updateLifetime(appName, updateServiceData, ugi);
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
      String message = "Service is not found in hdfs: " + appName;
      LOG.error(message, e);
      return formatResponse(Status.NOT_FOUND, e.getMessage());
    } catch (IOException | InterruptedException e) {
      String message = "Error while performing operation for app: " + appName;
      LOG.error(message, e);
      return formatResponse(Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    // If nothing happens consider it a no-op
    return Response.status(Status.NO_CONTENT).build();
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
        sc.init(YARN_CONFIG);
        sc.start();
        result = sc
            .actionFlex(appName, componentCountStrings);
        sc.close();
        return Integer.valueOf(result);
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
        sc.init(YARN_CONFIG);
        sc.start();
        String newLifeTime = sc.updateLifetime(appName,
            updateAppData.getLifetime());
        sc.close();
        return newLifeTime;
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
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws YarnException, IOException {
        ServiceClient sc = getServiceClient();
        sc.init(YARN_CONFIG);
        sc.start();
        sc.actionStart(appName);
        sc.close();
        return null;
      }
    });
    LOG.info("Successfully started service " + appName);
    ServiceStatus status = new ServiceStatus();
    status.setDiagnostics("Service " + appName + " is successfully started.");
    status.setState(ServiceState.ACCEPTED);
    return formatResponse(Status.OK, status);
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
