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

import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.api.records.Application;
import org.apache.hadoop.yarn.service.api.records.ApplicationState;
import org.apache.hadoop.yarn.service.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.utils.SliderUtils;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.apache.hadoop.yarn.service.conf.RestApiConstants.*;

@Singleton
@Path(CONTEXT_ROOT)
public class ApplicationApiService {
  private static final Logger LOG =
      LoggerFactory.getLogger(ApplicationApiService.class);
  private static Configuration YARN_CONFIG = new YarnConfiguration();
  private static ServiceClient SERVICE_CLIENT;

  static {
    init();
  }

  // initialize all the common resources - order is important
  private static void init() {
    SERVICE_CLIENT = new ServiceClient();
    SERVICE_CLIENT.init(YARN_CONFIG);
    SERVICE_CLIENT.start();
  }

  @GET
  @Path(VERSION)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
  public Response getVersion() {
    String version = VersionInfo.getBuildVersion();
    LOG.info(version);
    return Response.ok(version).build();
  }

  @POST
  @Path(APP_ROOT_PATH)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response createApplication(Application application) {
    LOG.info("POST: createApplication = {}", application);
    ApplicationStatus applicationStatus = new ApplicationStatus();
    try {
      ApplicationId applicationId = SERVICE_CLIENT.actionCreate(application);
      LOG.info("Successfully created application " + application.getName()
          + " applicationId = " + applicationId);
      applicationStatus.setState(ApplicationState.ACCEPTED);
      applicationStatus.setUri(
          CONTEXT_ROOT + APP_ROOT_PATH + "/" + application
              .getName());
      return Response.status(Status.CREATED).entity(applicationStatus).build();
    } catch (IllegalArgumentException e) {
      applicationStatus.setDiagnostics(e.getMessage());
      return Response.status(Status.BAD_REQUEST).entity(applicationStatus)
          .build();
    } catch (Exception e) {
      String message = "Failed to create application " + application.getName();
      LOG.error(message, e);
      applicationStatus.setDiagnostics(message + ": " + e.getMessage());
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(applicationStatus).build();
    }
  }

  @GET
  @Path(APP_PATH)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response getApplication(@PathParam(APP_NAME) String appName) {
    LOG.info("GET: getApplication for appName = {}", appName);
    ApplicationStatus applicationStatus = new ApplicationStatus();

    // app name validation
    if (!SliderUtils.isClusternameValid(appName)) {
      applicationStatus.setDiagnostics("Invalid application name: " + appName);
      applicationStatus.setCode(ERROR_CODE_APP_NAME_INVALID);
      return Response.status(Status.NOT_FOUND).entity(applicationStatus)
          .build();
    }

    try {
      Application app = SERVICE_CLIENT.getStatus(appName);
      return Response.ok(app).build();
    } catch (Exception e) {
      LOG.error("Get application failed", e);
      applicationStatus
          .setDiagnostics("Failed to retrieve application: " + e.getMessage());
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(applicationStatus).build();
    }
  }

  @DELETE
  @Path(APP_PATH)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response deleteApplication(@PathParam(APP_NAME) String appName) {
    LOG.info("DELETE: deleteApplication for appName = {}", appName);
    return stopApplication(appName, true);
  }

  private Response stopApplication(String appName, boolean destroy) {
    try {
      SERVICE_CLIENT.actionStop(appName, destroy);
      if (destroy) {
        SERVICE_CLIENT.actionDestroy(appName);
        LOG.info("Successfully deleted application {}", appName);
      } else {
        LOG.info("Successfully stopped application {}", appName);
      }
      return Response.status(Status.NO_CONTENT).build();
    } catch (ApplicationNotFoundException e) {
      ApplicationStatus applicationStatus = new ApplicationStatus();
      applicationStatus.setDiagnostics(
          "Application " + appName + " not found " + e.getMessage());
      return Response.status(Status.NOT_FOUND).entity(applicationStatus)
          .build();
    } catch (Exception e) {
      ApplicationStatus applicationStatus = new ApplicationStatus();
      applicationStatus.setDiagnostics(e.getMessage());
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(applicationStatus).build();
    }
  }

  @PUT
  @Path(COMPONENT_PATH)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN  })
  public Response updateComponent(@PathParam(APP_NAME) String appName,
      @PathParam(COMPONENT_NAME) String componentName, Component component) {

    if (component.getNumberOfContainers() < 0) {
      return Response.status(Status.BAD_REQUEST).entity(
          "Application = " + appName + ", Component = " + component.getName()
              + ": Invalid number of containers specified " + component
              .getNumberOfContainers()).build();
    }
    try {
      Map<String, Long> original = SERVICE_CLIENT.flexByRestService(appName,
          Collections.singletonMap(component.getName(),
              component.getNumberOfContainers()));
      return Response.ok().entity(
          "Updating component " + componentName + " size from " + original
              .get(componentName) + " to " + component.getNumberOfContainers())
          .build();
    } catch (YarnException | IOException e) {
      ApplicationStatus status = new ApplicationStatus();
      status.setDiagnostics(e.getMessage());
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(status)
          .build();
    }
  }

  @PUT
  @Path(APP_PATH)
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response updateApplication(@PathParam(APP_NAME) String appName,
      Application updateAppData) {
    LOG.info("PUT: updateApplication for app = {} with data = {}", appName,
        updateAppData);

    // Ignore the app name provided in updateAppData and always use appName
    // path param
    updateAppData.setName(appName);

    // For STOP the app should be running. If already stopped then this
    // operation will be a no-op. For START it should be in stopped state.
    // If already running then this operation will be a no-op.
    if (updateAppData.getState() != null
        && updateAppData.getState() == ApplicationState.STOPPED) {
      return stopApplication(appName, false);
    }

    // If a START is requested
    if (updateAppData.getState() != null
        && updateAppData.getState() == ApplicationState.STARTED) {
      return startApplication(appName);
    }

    // If new lifetime value specified then update it
    if (updateAppData.getLifetime() != null
        && updateAppData.getLifetime() > 0) {
      return updateLifetime(appName, updateAppData);
    }

    // flex a single component app
    if (updateAppData.getNumberOfContainers() != null && !ServiceApiUtil
        .hasComponent(updateAppData)) {
      Component defaultComp = ServiceApiUtil.createDefaultComponent(updateAppData);
      return updateComponent(updateAppData.getName(), defaultComp.getName(),
          defaultComp);
    }

    // If nothing happens consider it a no-op
    return Response.status(Status.NO_CONTENT).build();
  }

  private Response updateLifetime(String appName, Application updateAppData) {
    try {
      String newLifeTime =
          SERVICE_CLIENT.updateLifetime(appName, updateAppData.getLifetime());
      return Response.ok("Application " + appName + " lifeTime is successfully updated to "
          + updateAppData.getLifetime() + " seconds from now: " + newLifeTime).build();
    } catch (Exception e) {
      String message =
          "Failed to update application (" + appName + ") lifetime ("
              + updateAppData.getLifetime() + ")";
      LOG.error(message, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(message + " : " + e.getMessage()).build();
    }
  }

  private Response startApplication(String appName) {
    try {
      SERVICE_CLIENT.actionStart(appName);
      LOG.info("Successfully started application " + appName);
      return Response.ok("Application " + appName + " is successfully started").build();
    } catch (Exception e) {
      String message = "Failed to start application " + appName;
      LOG.info(message, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(message + ": " + e.getMessage()).build();
    }
  }
}
