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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationAttemptEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ClusterEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ContainerEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.QueueEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.SubApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.UserEntity;
import org.apache.hadoop.yarn.server.timelineservice.metrics.PerNodeAggTimelineCollectorMetrics;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.IllegalFormatException;

/**
 * The main per-node REST end point for timeline service writes. It is
 * essentially a container service that routes requests to the appropriate
 * per-app services.
 */
@Private
@Unstable
@Singleton
@Path("/ws/v2/timeline")
public class TimelineCollectorWebService {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineCollectorWebService.class);

  private @Context ServletContext context;
  private static final PerNodeAggTimelineCollectorMetrics METRICS =
      PerNodeAggTimelineCollectorMetrics.getInstance();

  /**
   * Gives information about timeline collector.
   */
  @XmlRootElement(name = "about")
  @XmlAccessorType(XmlAccessType.NONE)
  @Public
  @Unstable
  public static class AboutInfo {

    private String about;

    public AboutInfo() {

    }

    public AboutInfo(String abt) {
      this.about = abt;
    }

    @XmlElement(name = "About")
    public String getAbout() {
      return about;
    }

    public void setAbout(String abt) {
      this.about = abt;
    }

  }

  /**
   * Return the description of the timeline web services.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @return description of timeline web service.
   */
  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public AboutInfo about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    init(res);
    return new AboutInfo("Timeline Collector API");
  }

  /**
   * Accepts writes to the collector, and returns a response. It simply routes
   * the request to the app level collector. It expects an application as a
   * context.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param async flag indicating whether its an async put or not. "true"
   *     indicates, its an async call. If null, its considered false.
   * @param appId Application Id to which the entities to be put belong to. If
   *     appId is not there or it cannot be parsed, HTTP 400 will be sent back.
   * @param entities timeline entities to be put.
   * @return a Response with appropriate HTTP status.
   */
  @PUT
  @Path("/entities")
  @Consumes({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  public Response putEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @QueryParam("async") String async,
      @QueryParam("subappwrite") String isSubAppEntities,
      @QueryParam("appid") String appId,
      TimelineEntities entities) {
    init(res);
    UserGroupInformation callerUgi = getUser(req);
    boolean isAsync = async != null && async.trim().equalsIgnoreCase("true");
    if (callerUgi == null) {
      String msg = "The owner of the posted timeline entities is not set";
      LOG.error(msg);
      throw new ForbiddenException(msg);
    }

    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    try {
      ApplicationId appID = parseApplicationId(appId);
      if (appID == null) {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
      NodeTimelineCollectorManager collectorManager =
          (NodeTimelineCollectorManager) context.getAttribute(
              NodeTimelineCollectorManager.COLLECTOR_MANAGER_ATTR_KEY);
      TimelineCollector collector = collectorManager.get(appID);
      if (collector == null) {
        LOG.error("Application: "+ appId + " is not found");
        throw new NotFoundException("Application: "+ appId + " is not found");
      }

      if (isAsync) {
        collector.putEntitiesAsync(processTimelineEntities(entities, appId,
            Boolean.valueOf(isSubAppEntities)), callerUgi);
      } else {
        collector.putEntities(processTimelineEntities(entities, appId,
            Boolean.valueOf(isSubAppEntities)), callerUgi);
      }

      succeeded = true;
      return Response.ok().build();
    } catch (NotFoundException | ForbiddenException e) {
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IOException e) {
      LOG.error("Error putting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    } finally {
      long latency = Time.monotonicNow() - startTime;
      if (isAsync) {
        METRICS.addAsyncPutEntitiesLatency(latency, succeeded);
      } else {
        METRICS.addPutEntitiesLatency(latency, succeeded);
      }
    }
  }

  /**
   * @param req    Servlet request.
   * @param res    Servlet response.
   * @param domain timeline domain to be put.
   * @param appId Application Id to which the domain to be put belong to. If
   *     appId is not there or it cannot be parsed, HTTP 400 will be sent back.
   * @return a Response with appropriate HTTP status.
   */
  @PUT
  @Path("/domain")
  @Consumes({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */ })
  public Response putDomain(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @QueryParam("appid") String appId,
      TimelineDomain domain) {
    init(res);
    UserGroupInformation callerUgi = getUser(req);
    if (callerUgi == null) {
      String msg = "The owner of the posted timeline entities is not set";
      LOG.error(msg);
      throw new ForbiddenException(msg);
    }

    try {
      ApplicationId appID = parseApplicationId(appId);
      if (appID == null) {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
      NodeTimelineCollectorManager collectorManager =
          (NodeTimelineCollectorManager) context.getAttribute(
              NodeTimelineCollectorManager.COLLECTOR_MANAGER_ATTR_KEY);
      TimelineCollector collector = collectorManager.get(appID);
      if (collector == null) {
        LOG.error("Application: " + appId + " is not found");
        throw new NotFoundException("Application: " + appId + " is not found");
      }

      domain.setOwner(callerUgi.getShortUserName());
      collector.putDomain(domain, callerUgi);

      return Response.ok().build();
    } catch (NotFoundException e) {
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IOException e) {
      LOG.error("Error putting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private static ApplicationId parseApplicationId(String appId) {
    try {
      if (appId != null) {
        return ApplicationId.fromString(appId.trim());
      } else {
        return null;
      }
    } catch (IllegalFormatException e) {
      LOG.error("Invalid application ID: " + appId);
      return null;
    }
  }

  private static void init(HttpServletResponse response) {
    response.setContentType(null);
  }

  private static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUgi = null;
    if (remoteUser != null) {
      callerUgi = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUgi;
  }

  // The process may not be necessary according to the way we write the backend,
  // but let's keep it for now in case we need to use sub-classes APIs in the
  // future (e.g., aggregation).
  private static TimelineEntities processTimelineEntities(
      TimelineEntities entities, String appId, boolean isSubAppWrite) {
    TimelineEntities entitiesToReturn = new TimelineEntities();
    for (TimelineEntity entity : entities.getEntities()) {
      TimelineEntityType type = null;
      try {
        type = TimelineEntityType.valueOf(entity.getType());
      } catch (IllegalArgumentException e) {
        type = null;
      }
      if (type != null) {
        switch (type) {
        case YARN_CLUSTER:
          entitiesToReturn.addEntity(new ClusterEntity(entity));
          break;
        case YARN_FLOW_RUN:
          entitiesToReturn.addEntity(new FlowRunEntity(entity));
          break;
        case YARN_APPLICATION:
          entitiesToReturn.addEntity(new ApplicationEntity(entity));
          break;
        case YARN_APPLICATION_ATTEMPT:
          entitiesToReturn.addEntity(new ApplicationAttemptEntity(entity));
          break;
        case YARN_CONTAINER:
          entitiesToReturn.addEntity(new ContainerEntity(entity));
          break;
        case YARN_QUEUE:
          entitiesToReturn.addEntity(new QueueEntity(entity));
          break;
        case YARN_USER:
          entitiesToReturn.addEntity(new UserEntity(entity));
          break;
        default:
          break;
        }
      } else {
        if (isSubAppWrite) {
          SubApplicationEntity se = new SubApplicationEntity(entity);
          se.setApplicationId(appId);
          entitiesToReturn.addEntity(se);
        } else {
          entitiesToReturn.addEntity(entity);
        }
      }
    }
    return entitiesToReturn;
  }
}
