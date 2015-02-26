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

package org.apache.hadoop.yarn.server.timelineservice.aggregator;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.inject.Singleton;

/**
 * The main per-node REST end point for timeline service writes. It is
 * essentially a container service that routes requests to the appropriate
 * per-app services.
 */
@Private
@Unstable
@Singleton
@Path("/ws/v2/timeline")
public class PerNodeAggregatorWebService {
  private static final Log LOG =
      LogFactory.getLog(PerNodeAggregatorWebService.class);

  private @Context ServletContext context;

  @XmlRootElement(name = "about")
  @XmlAccessorType(XmlAccessType.NONE)
  @Public
  @Unstable
  public static class AboutInfo {

    private String about;

    public AboutInfo() {

    }

    public AboutInfo(String about) {
      this.about = about;
    }

    @XmlElement(name = "About")
    public String getAbout() {
      return about;
    }

    public void setAbout(String about) {
      this.about = about;
    }

  }

  /**
   * Return the description of the timeline web services.
   */
  @GET
  @Produces({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  public AboutInfo about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    init(res);
    return new AboutInfo("Timeline API");
  }

  /**
   * Accepts writes to the aggregator, and returns a response. It simply routes
   * the request to the app level aggregator. It expects an application as a
   * context.
   */
  @PUT
  @Path("/entities")
  @Consumes({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  public Response putEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @QueryParam("async") String async,
      @QueryParam("appid") String appId,
      TimelineEntities entities) {
    init(res);
    UserGroupInformation callerUgi = getUser(req);
    if (callerUgi == null) {
      String msg = "The owner of the posted timeline entities is not set";
      LOG.error(msg);
      throw new ForbiddenException(msg);
    }

    // TODO how to express async posts and handle them
    boolean isAsync = async != null && async.trim().equalsIgnoreCase("true");

    try {
      appId = parseApplicationId(appId);
      if (appId == null) {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
      AppLevelAggregatorService service = getAggregatorService(req, appId);
      if (service == null) {
        LOG.error("Application not found");
        throw new NotFoundException(); // different exception?
      }
      service.postEntities(entities, callerUgi);
      return Response.ok().build();
    } catch (Exception e) {
      LOG.error("Error putting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private String parseApplicationId(String appId) {
    // Make sure the appId is not null and is valid
    ApplicationId appID;
    try {
      if (appId != null) {
        return ConverterUtils.toApplicationId(appId.trim()).toString();
      } else {
        return null;
      }
    } catch (Exception e) {
      return null;
    }
  }

  private AppLevelAggregatorService
      getAggregatorService(HttpServletRequest req, String appIdToParse) {
    String appIdString = parseApplicationId(appIdToParse);
    final AppLevelServiceManager serviceManager =
        (AppLevelServiceManager) context.getAttribute(
            PerNodeAggregatorServer.AGGREGATOR_COLLECTION_ATTR_KEY);
    return serviceManager.getService(appIdString);
  }

  private void init(HttpServletResponse response) {
    response.setContentType(null);
  }

  private UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUgi = null;
    if (remoteUser != null) {
      callerUgi = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUgi;
  }
}
