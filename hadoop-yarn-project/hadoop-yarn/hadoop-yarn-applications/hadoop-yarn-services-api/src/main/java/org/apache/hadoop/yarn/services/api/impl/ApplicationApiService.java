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

package org.apache.hadoop.yarn.services.api.impl;

import com.google.inject.Singleton;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.ApplicationState;
import org.apache.slider.api.resource.ApplicationStatus;
import org.apache.slider.api.resource.Component;
import org.apache.slider.util.ServiceApiUtil;
import org.apache.slider.client.SliderClient;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.common.tools.SliderVersionInfo;
import org.apache.slider.core.buildutils.BuildHelper;
import org.apache.slider.core.exceptions.SliderException;
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
import java.util.HashMap;
import java.util.Map;

import static org.apache.slider.util.RestApiConstants.*;

@Singleton
@Path(APPLICATIONS_API_RESOURCE_PATH)
@Consumes({ MediaType.APPLICATION_JSON })
@Produces({ MediaType.APPLICATION_JSON })
public class ApplicationApiService {
  private static final Logger logger =
      LoggerFactory.getLogger(ApplicationApiService.class);
  private static org.apache.hadoop.conf.Configuration SLIDER_CONFIG =
      new YarnConfiguration();
  private static SliderClient SLIDER_CLIENT;
  private static Response SLIDER_VERSION;
  private static final ActionFreezeArgs ACTION_FREEZE_ARGS = new ActionFreezeArgs();

  static {
    init();
  }

  // initialize all the common resources - order is important
  protected static void init() {
    SLIDER_CLIENT = createSliderClient();
    SLIDER_VERSION = initSliderVersion();
  }

  @GET
  @Path("/versions/slider-version")
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON }) public Response getSliderVersion() {
    logger.info("GET: getSliderVersion");
    return SLIDER_VERSION;
  }

  private static Response initSliderVersion() {
    Map<String, Object> metadata = new HashMap<>();
    BuildHelper.addBuildMetadata(metadata, "org.apache.hadoop.yarn.services");
    String sliderVersion = metadata.toString();
    logger.info("Slider version = {}", sliderVersion);
    String hadoopVersion = SliderVersionInfo.getHadoopVersionString();
    logger.info("Hadoop version = {}", hadoopVersion);
    return Response.ok("{ \"slider_version\": \"" + sliderVersion
        + "\", \"hadoop_version\": \"" + hadoopVersion + "\"}").build();
  }

  @POST @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response createApplication(Application application) {
    logger.info("POST: createApplication = {}", application);
    ApplicationStatus applicationStatus = new ApplicationStatus();
    try {
      ApplicationId applicationId = SLIDER_CLIENT.actionCreate(application);
      logger.info("Successfully created application " + application.getName()
          + " applicationId = " + applicationId);
      applicationStatus.setState(ApplicationState.ACCEPTED);
      applicationStatus.setUri(
          CONTEXT_ROOT + APPLICATIONS_API_RESOURCE_PATH + "/" + application
              .getName());
      return Response.status(Status.CREATED).entity(applicationStatus).build();
    } catch (IllegalArgumentException e) {
      applicationStatus.setDiagnostics(e.getMessage());
      return Response.status(Status.BAD_REQUEST).entity(applicationStatus)
          .build();
    } catch (Exception e) {
      String message = "Failed to create application " + application.getName();
      logger.error(message, e);
      applicationStatus.setDiagnostics(message + ": " + e.getMessage());
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(applicationStatus).build();
    }
  }

  protected static SliderClient createSliderClient() {
    if (SLIDER_CLIENT != null) {
      return SLIDER_CLIENT;
    }
    org.apache.hadoop.conf.Configuration sliderClientConfiguration =
        SLIDER_CONFIG;
    SliderClient client = new SliderClient() {
      @Override public void init(org.apache.hadoop.conf.Configuration conf) {
        super.init(conf);
        try {
          initHadoopBinding();
        } catch (SliderException | IOException e) {
          throw new RuntimeException(
              "Unable to automatically init Hadoop binding", e);
        }
      }
    };
    try {
      logger
          .debug("Slider Client configuration: {}", sliderClientConfiguration);
      sliderClientConfiguration = client.bindArgs(sliderClientConfiguration, new String[] { "help" });
      client.init(sliderClientConfiguration);
      client.start();
    } catch (Exception e) {
      logger.error("Unable to create SliderClient", e);
      throw new RuntimeException(e.getMessage(), e);
    }
    return client;
  }

  // The information this REST endpoint currently returned can be retrieved from
  // RM web services
  // Probably the data from AM is more important. Do that later.
//  @GET @Consumes({ MediaType.APPLICATION_JSON })
//  @Produces({ MediaType.APPLICATION_JSON })
//  public Response getApplications(@QueryParam("state") String state) {
//    logger.info("GET: getApplications with param state = {}", state);
//    return null;
//  }

  @GET @Path("/{app_name}")
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response getApplication(@PathParam("app_name") String appName) {
    logger.info("GET: getApplication for appName = {}", appName);
    ApplicationStatus applicationStatus = new ApplicationStatus();

    // app name validation
    if (!SliderUtils.isClusternameValid(appName)) {
      applicationStatus.setDiagnostics("Invalid application name: " + appName);
      applicationStatus.setCode(ERROR_CODE_APP_NAME_INVALID);
      return Response.status(Status.NOT_FOUND).entity(applicationStatus)
          .build();
    }

    try {
      Application app = SLIDER_CLIENT.actionStatus(appName);
      ApplicationReport report = SLIDER_CLIENT.findInstance(appName);
      if (app != null && report != null) {
        app.setLifetime(
            report.getApplicationTimeouts().get(ApplicationTimeoutType.LIFETIME)
                .getRemainingTime());
        logger.info("Application = {}", app);
        return Response.ok(app).build();
      } else {
        String message = "Application " + appName + " does not exist.";
        logger.info(message);
        applicationStatus.setCode(ERROR_CODE_APP_DOES_NOT_EXIST);
        applicationStatus.setDiagnostics(message);
        return Response.status(Status.NOT_FOUND).entity(applicationStatus)
            .build();
      }
    } catch (Exception e) {
      logger.error("Get application failed", e);
      applicationStatus
          .setDiagnostics("Failed to retrieve application: " + e.getMessage());
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(applicationStatus).build();
    }
  }

  @DELETE
  @Path("/{app_name}")
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response deleteApplication(@PathParam("app_name") String appName) {
    logger.info("DELETE: deleteApplication for appName = {}", appName);
    return stopApplication(appName, true);
  }

  private Response stopApplication(String appName, boolean destroy) {
    try {
      SLIDER_CLIENT.actionStop(appName, ACTION_FREEZE_ARGS);
      if (destroy) {
        SLIDER_CLIENT.actionDestroy(appName);
        logger.info("Successfully deleted application {}", appName);
      } else {
        logger.info("Successfully stopped application {}", appName);
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

  @PUT @Path("/{app_name}/components/{component_name}")
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response updateComponent(@PathParam("app_name") String appName,
      @PathParam("component_name") String componentName, Component component) {

    if (component.getNumberOfContainers() < 0) {
      return Response.status(Status.BAD_REQUEST).entity(
          "Application = " + appName + ", Component = " + component.getName()
              + ": Invalid number of containers specified " + component
              .getNumberOfContainers()).build();
    }
    try {
      Map<String, Long> original = SLIDER_CLIENT.flex(appName, Collections
          .singletonMap(component.getName(),
              component.getNumberOfContainers()));
      return Response.ok().entity("Updating " + componentName + " size from "
          + original.get(componentName) + " to "
          + component.getNumberOfContainers()).build();
    } catch (YarnException | IOException e) {
      ApplicationStatus status = new ApplicationStatus();
      status.setDiagnostics(e.getMessage());
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(status)
          .build();
    }
  }

  @PUT @Path("/{app_name}")
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response updateApplication(@PathParam("app_name") String appName,
      Application updateAppData) {
    logger.info("PUT: updateApplication for app = {} with data = {}", appName,
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
        .hasComponent(
        updateAppData)) {
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
          SLIDER_CLIENT.updateLifetime(appName, updateAppData.getLifetime());
      return Response.ok("Application " + appName + " lifeTime is successfully updated to "
          + updateAppData.getLifetime() + " seconds from now: " + newLifeTime).build();
    } catch (Exception e) {
      String message =
          "Failed to update application (" + appName + ") lifetime ("
              + updateAppData.getLifetime() + ")";
      logger.error(message, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(message + " : " + e.getMessage()).build();
    }
  }

  private Response startApplication(String appName) {
    try {
      int ret = SLIDER_CLIENT.actionList(appName);
      if (ret == 0) {
        return Response.ok()
            .entity("Application " + appName + " is already alive.").build();
      }
      SLIDER_CLIENT.actionStart(appName, null);
      logger.info("Successfully started application " + appName);
      return Response.ok("Application " + appName + " is successfully started").build();
    } catch (Exception e) {
      String message = "Failed to start application " + appName;
      logger.info(message, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(message + ": " + e.getMessage()).build();
    }
  }
}
