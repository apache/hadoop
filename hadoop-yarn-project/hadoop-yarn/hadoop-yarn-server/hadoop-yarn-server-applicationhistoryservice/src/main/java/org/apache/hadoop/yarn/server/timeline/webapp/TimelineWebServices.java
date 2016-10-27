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

package org.apache.hadoop.yarn.server.timeline.webapp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.EntityIdentifier;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@Path("/ws/v1/timeline")
//TODO: support XML serialization/deserialization
public class TimelineWebServices {

  private static final Log LOG = LogFactory.getLog(TimelineWebServices.class);

  private TimelineDataManager timelineDataManager;

  @Inject
  public TimelineWebServices(TimelineDataManager timelineDataManager) {
    this.timelineDataManager = timelineDataManager;
  }

  /**
   * Return the description of the timeline web services.
   */
  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineAbout about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    init(res);
    return TimelineUtils.createTimelineAbout("Timeline API");
  }

  /**
   * Return a list of entities that match the given parameters.
   */
  @GET
  @Path("/{entityType}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineEntities getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("entityType") String entityType,
      @QueryParam("primaryFilter") String primaryFilter,
      @QueryParam("secondaryFilter") String secondaryFilter,
      @QueryParam("windowStart") String windowStart,
      @QueryParam("windowEnd") String windowEnd,
      @QueryParam("fromId") String fromId,
      @QueryParam("fromTs") String fromTs,
      @QueryParam("limit") String limit,
      @QueryParam("fields") String fields) {
    init(res);
    try {
      return timelineDataManager.getEntities(
          parseStr(entityType),
          parsePairStr(primaryFilter, ":"),
          parsePairsStr(secondaryFilter, ",", ":"),
          parseLongStr(windowStart),
          parseLongStr(windowEnd),
          parseStr(fromId),
          parseLongStr(fromTs),
          parseLongStr(limit),
          parseFieldsStr(fields, ","),
          getUser(req));
    } catch (NumberFormatException e) {
      throw new BadRequestException(
        "windowStart, windowEnd, fromTs or limit is not a numeric value: " + e);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("requested invalid field: " + e);
    } catch (Exception e) {
      LOG.error("Error getting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Return a single entity of the given entity type and Id.
   */
  @GET
  @Path("/{entityType}/{entityId}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("entityType") String entityType,
      @PathParam("entityId") String entityId,
      @QueryParam("fields") String fields) {
    init(res);
    TimelineEntity entity = null;
    try {
      entity = timelineDataManager.getEntity(
          parseStr(entityType),
          parseStr(entityId),
          parseFieldsStr(fields, ","),
          getUser(req));
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (Exception e) {
      LOG.error("Error getting entity", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (entity == null) {
      throw new NotFoundException("Timeline entity "
          + new EntityIdentifier(parseStr(entityId), parseStr(entityType))
          + " is not found");
    }
    return entity;
  }

  /**
   * Return the events that match the given parameters.
   */
  @GET
  @Path("/{entityType}/events")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineEvents getEvents(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("entityType") String entityType,
      @QueryParam("entityId") String entityId,
      @QueryParam("eventType") String eventType,
      @QueryParam("windowStart") String windowStart,
      @QueryParam("windowEnd") String windowEnd,
      @QueryParam("limit") String limit) {
    init(res);
    try {
      return timelineDataManager.getEvents(
          parseStr(entityType),
          parseArrayStr(entityId, ","),
          parseArrayStr(eventType, ","),
          parseLongStr(windowStart),
          parseLongStr(windowEnd),
          parseLongStr(limit),
          getUser(req));
    } catch (NumberFormatException e) {
      throw (BadRequestException)new BadRequestException(
          "windowStart, windowEnd or limit is not a numeric value.")
          .initCause(e);
    } catch (Exception e) {
      LOG.error("Error getting entity timelines", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Store the given entities into the timeline store, and return the errors
   * that happen during storing.
   */
  @POST
  @Consumes({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelinePutResponse postEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      TimelineEntities entities) {
    init(res);
    UserGroupInformation callerUGI = getUser(req);
    if (callerUGI == null) {
      String msg = "The owner of the posted timeline entities is not set";
      LOG.error(msg);
      throw new ForbiddenException(msg);
    }
    try {
      return timelineDataManager.postEntities(entities, callerUGI);
    } catch (BadRequestException bre) {
      throw bre;
    } catch (Exception e) {
      LOG.error("Error putting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Store the given domain into the timeline store, and return the errors
   * that happen during storing.
   */
  @PUT
  @Path("/domain")
  @Consumes({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelinePutResponse putDomain(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      TimelineDomain domain) {
    init(res);
    UserGroupInformation callerUGI = getUser(req);
    if (callerUGI == null) {
      String msg = "The owner of the posted timeline domain is not set";
      LOG.error(msg);
      throw new ForbiddenException(msg);
    }
    domain.setOwner(callerUGI.getShortUserName());
    try {
      timelineDataManager.putDomain(domain, callerUGI);
    } catch (YarnException e) {
      // The user doesn't have the access to override the existing domain.
      LOG.error(e.getMessage(), e);
      throw new ForbiddenException(e);
    } catch (IOException e) {
      LOG.error("Error putting domain", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return new TimelinePutResponse();
  }

  /**
   * Return a single domain of the given domain Id.
   */
  @GET
  @Path("/domain/{domainId}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineDomain getDomain(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("domainId") String domainId) {
    init(res);
    domainId = parseStr(domainId);
    if (domainId == null || domainId.length() == 0) {
      throw new BadRequestException("Domain ID is not specified.");
    }
    TimelineDomain domain = null;
    try {
      domain = timelineDataManager.getDomain(
          parseStr(domainId), getUser(req));
    } catch (Exception e) {
      LOG.error("Error getting domain", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (domain == null) {
      throw new NotFoundException("Timeline domain ["
          + domainId + "] is not found");
    }
    return domain;
  }

  /**
   * Return a list of domains of the given owner.
   */
  @GET
  @Path("/domain")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineDomains getDomains(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @QueryParam("owner") String owner) {
    init(res);
    owner = parseStr(owner);
    UserGroupInformation callerUGI = getUser(req);
    if (owner == null || owner.length() == 0) {
      if (callerUGI == null) {
        throw new BadRequestException("Domain owner is not specified.");
      } else {
        // By default it's going to list the caller's domains
        owner = callerUGI.getShortUserName();
      }
    }
    try {
      return timelineDataManager.getDomains(owner, callerUGI);
    } catch (Exception e) {
      LOG.error("Error getting domains", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private void init(HttpServletResponse response) {
    response.setContentType(null);
  }

  private static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUGI;
  }

  private static SortedSet<String> parseArrayStr(String str, String delimiter) {
    if (str == null) {
      return null;
    }
    SortedSet<String> strSet = new TreeSet<String>();
    String[] strs = str.split(delimiter);
    for (String aStr : strs) {
      strSet.add(aStr.trim());
    }
    return strSet;
  }

  private static NameValuePair parsePairStr(String str, String delimiter) {
    if (str == null) {
      return null;
    }
    String[] strs = str.split(delimiter, 2);
    try {
      return new NameValuePair(strs[0].trim(),
          GenericObjectMapper.OBJECT_READER.readValue(strs[1].trim()));
    } catch (Exception e) {
      // didn't work as an Object, keep it as a String
      return new NameValuePair(strs[0].trim(), strs[1].trim());
    }
  }

  private static Collection<NameValuePair> parsePairsStr(
      String str, String aDelimiter, String pDelimiter) {
    if (str == null) {
      return null;
    }
    String[] strs = str.split(aDelimiter);
    Set<NameValuePair> pairs = new HashSet<NameValuePair>();
    for (String aStr : strs) {
      pairs.add(parsePairStr(aStr, pDelimiter));
    }
    return pairs;
  }

  private static EnumSet<Field> parseFieldsStr(String str, String delimiter) {
    if (str == null) {
      return null;
    }
    String[] strs = str.split(delimiter);
    List<Field> fieldList = new ArrayList<Field>();
    for (String s : strs) {
      s = StringUtils.toUpperCase(s.trim());
      if (s.equals("EVENTS")) {
        fieldList.add(Field.EVENTS);
      } else if (s.equals("LASTEVENTONLY")) {
        fieldList.add(Field.LAST_EVENT_ONLY);
      } else if (s.equals("RELATEDENTITIES")) {
        fieldList.add(Field.RELATED_ENTITIES);
      } else if (s.equals("PRIMARYFILTERS")) {
        fieldList.add(Field.PRIMARY_FILTERS);
      } else if (s.equals("OTHERINFO")) {
        fieldList.add(Field.OTHER_INFO);
      } else {
        throw new IllegalArgumentException("Requested nonexistent field " + s);
      }
    }
    if (fieldList.size() == 0) {
      return null;
    }
    Field f1 = fieldList.remove(fieldList.size() - 1);
    if (fieldList.size() == 0) {
      return EnumSet.of(f1);
    } else {
      return EnumSet.of(f1, fieldList.toArray(new Field[fieldList.size()]));
    }
  }

  private static Long parseLongStr(String str) {
    return str == null ? null : Long.parseLong(str.trim());
  }

  private static String parseStr(String str) {
    return str == null ? null : str.trim();
  }

}
