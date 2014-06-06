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

import static org.apache.hadoop.yarn.util.StringHelper.CSV_JOINER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.EntityIdentifier;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@Path("/ws/v1/timeline")
//TODO: support XML serialization/deserialization
public class TimelineWebServices {

  private static final Log LOG = LogFactory.getLog(TimelineWebServices.class);

  private TimelineStore store;
  private TimelineACLsManager timelineACLsManager;

  @Inject
  public TimelineWebServices(TimelineStore store,
      TimelineACLsManager timelineACLsManager) {
    this.store = store;
    this.timelineACLsManager = timelineACLsManager;
  }

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
   * Return a list of entities that match the given parameters.
   */
  @GET
  @Path("/{entityType}")
  @Produces({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
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
    TimelineEntities entities = null;
    try {
      EnumSet<Field> fieldEnums = parseFieldsStr(fields, ",");
      boolean modified = extendFields(fieldEnums);
      UserGroupInformation callerUGI = getUser(req);
      entities = store.getEntities(
          parseStr(entityType),
          parseLongStr(limit),
          parseLongStr(windowStart),
          parseLongStr(windowEnd),
          parseStr(fromId),
          parseLongStr(fromTs),
          parsePairStr(primaryFilter, ":"),
          parsePairsStr(secondaryFilter, ",", ":"),
          fieldEnums);
      if (entities != null) {
        Iterator<TimelineEntity> entitiesItr =
            entities.getEntities().iterator();
        while (entitiesItr.hasNext()) {
          TimelineEntity entity = entitiesItr.next();
          try {
            // check ACLs
            if (!timelineACLsManager.checkAccess(callerUGI, entity)) {
              entitiesItr.remove();
            } else {
              // clean up system data
              if (modified) {
                entity.setPrimaryFilters(null);
              } else {
                cleanupOwnerInfo(entity);
              }
            }
          } catch (YarnException e) {
            LOG.error("Error when verifying access for user " + callerUGI
                + " on the events of the timeline entity "
                + new EntityIdentifier(entity.getEntityId(),
                    entity.getEntityType()), e);
            entitiesItr.remove();
          }
        }
      }
    } catch (NumberFormatException e) {
      throw new BadRequestException(
          "windowStart, windowEnd or limit is not a numeric value.");
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("requested invalid field.");
    } catch (IOException e) {
      LOG.error("Error getting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (entities == null) {
      return new TimelineEntities();
    }
    return entities;
  }

  /**
   * Return a single entity of the given entity type and Id.
   */
  @GET
  @Path("/{entityType}/{entityId}")
  @Produces({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("entityType") String entityType,
      @PathParam("entityId") String entityId,
      @QueryParam("fields") String fields) {
    init(res);
    TimelineEntity entity = null;
    try {
      EnumSet<Field> fieldEnums = parseFieldsStr(fields, ",");
      boolean modified = extendFields(fieldEnums);
      entity =
          store.getEntity(parseStr(entityId), parseStr(entityType),
              fieldEnums);
      if (entity != null) {
        // check ACLs
        UserGroupInformation callerUGI = getUser(req);
        if (!timelineACLsManager.checkAccess(callerUGI, entity)) {
          entity = null;
        } else {
          // clean up the system data
          if (modified) {
            entity.setPrimaryFilters(null);
          } else {
            cleanupOwnerInfo(entity);
          }
        }
      }
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(
          "requested invalid field.");
    } catch (IOException e) {
      LOG.error("Error getting entity", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (YarnException e) {
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
  @Produces({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
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
    TimelineEvents events = null;
    try {
      UserGroupInformation callerUGI = getUser(req);
      events = store.getEntityTimelines(
          parseStr(entityType),
          parseArrayStr(entityId, ","),
          parseLongStr(limit),
          parseLongStr(windowStart),
          parseLongStr(windowEnd),
          parseArrayStr(eventType, ","));
      if (events != null) {
        Iterator<TimelineEvents.EventsOfOneEntity> eventsItr =
            events.getAllEvents().iterator();
        while (eventsItr.hasNext()) {
          TimelineEvents.EventsOfOneEntity eventsOfOneEntity = eventsItr.next();
          try {
            TimelineEntity entity = store.getEntity(
                eventsOfOneEntity.getEntityId(),
                eventsOfOneEntity.getEntityType(),
                EnumSet.of(Field.PRIMARY_FILTERS));
            // check ACLs
            if (!timelineACLsManager.checkAccess(callerUGI, entity)) {
              eventsItr.remove();
            }
          } catch (Exception e) {
            LOG.error("Error when verifying access for user " + callerUGI
                + " on the events of the timeline entity "
                + new EntityIdentifier(eventsOfOneEntity.getEntityId(),
                    eventsOfOneEntity.getEntityType()), e);
            eventsItr.remove();
          }
        }
      }
    } catch (NumberFormatException e) {
      throw new BadRequestException(
          "windowStart, windowEnd or limit is not a numeric value.");
    } catch (IOException e) {
      LOG.error("Error getting entity timelines", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (events == null) {
      return new TimelineEvents();
    }
    return events;
  }

  /**
   * Store the given entities into the timeline store, and return the errors
   * that happen during storing.
   */
  @POST
  @Consumes({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  public TimelinePutResponse postEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      TimelineEntities entities) {
    init(res);
    if (entities == null) {
      return new TimelinePutResponse();
    }
    UserGroupInformation callerUGI = getUser(req);
    try {
      List<EntityIdentifier> entityIDs = new ArrayList<EntityIdentifier>();
      TimelineEntities entitiesToPut = new TimelineEntities();
      List<TimelinePutResponse.TimelinePutError> errors =
          new ArrayList<TimelinePutResponse.TimelinePutError>();
      for (TimelineEntity entity : entities.getEntities()) {
        EntityIdentifier entityID =
            new EntityIdentifier(entity.getEntityId(), entity.getEntityType());

        // check if there is existing entity
        TimelineEntity existingEntity = null;
        try {
          existingEntity =
              store.getEntity(entityID.getId(), entityID.getType(),
                  EnumSet.of(Field.PRIMARY_FILTERS));
          if (existingEntity != null
              && !timelineACLsManager.checkAccess(callerUGI, existingEntity)) {
            throw new YarnException("The timeline entity " + entityID
                + " was not put by " + callerUGI + " before");
          }
        } catch (Exception e) {
          // Skip the entity which already exists and was put by others
          LOG.warn("Skip the timeline entity: " + entityID + ", because "
              + e.getMessage());
          TimelinePutResponse.TimelinePutError error =
              new TimelinePutResponse.TimelinePutError();
          error.setEntityId(entityID.getId());
          error.setEntityType(entityID.getType());
          error.setErrorCode(
              TimelinePutResponse.TimelinePutError.ACCESS_DENIED);
          errors.add(error);
          continue;
        }

        // inject owner information for the access check if this is the first
        // time to post the entity, in case it's the admin who is updating
        // the timeline data.
        try {
          if (existingEntity == null) {
            injectOwnerInfo(entity,
                callerUGI == null ? "" : callerUGI.getShortUserName());
          }
        } catch (YarnException e) {
          // Skip the entity which messes up the primary filter and record the
          // error
          LOG.warn("Skip the timeline entity: " + entityID + ", because "
              + e.getMessage());
          TimelinePutResponse.TimelinePutError error =
              new TimelinePutResponse.TimelinePutError();
          error.setEntityId(entityID.getId());
          error.setEntityType(entityID.getType());
          error.setErrorCode(
              TimelinePutResponse.TimelinePutError.SYSTEM_FILTER_CONFLICT);
          errors.add(error);
          continue;
        }

        entityIDs.add(entityID);
        entitiesToPut.addEntity(entity);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Storing the entity " + entityID + ", JSON-style content: "
              + TimelineUtils.dumpTimelineRecordtoJSON(entity));
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing entities: " + CSV_JOINER.join(entityIDs));
      }
      TimelinePutResponse response =  store.put(entitiesToPut);
      // add the errors of timeline system filter key conflict
      response.addErrors(errors);
      return response;
    } catch (IOException e) {
      LOG.error("Error putting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private void init(HttpServletResponse response) {
    response.setContentType(null);
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
      s = s.trim().toUpperCase();
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

  private static boolean extendFields(EnumSet<Field> fieldEnums) {
    boolean modified = false;
    if (fieldEnums != null && !fieldEnums.contains(Field.PRIMARY_FILTERS)) {
      fieldEnums.add(Field.PRIMARY_FILTERS);
      modified = true;
    }
    return modified;
  }
  private static Long parseLongStr(String str) {
    return str == null ? null : Long.parseLong(str.trim());
  }

  private static String parseStr(String str) {
    return str == null ? null : str.trim();
  }

  private static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUGI;
  }

  private static void injectOwnerInfo(TimelineEntity timelineEntity,
      String owner) throws YarnException {
    if (timelineEntity.getPrimaryFilters() != null &&
        timelineEntity.getPrimaryFilters().containsKey(
            TimelineStore.SystemFilter.ENTITY_OWNER.toString())) {
      throw new YarnException(
          "User should not use the timeline system filter key: "
              + TimelineStore.SystemFilter.ENTITY_OWNER);
    }
    timelineEntity.addPrimaryFilter(
        TimelineStore.SystemFilter.ENTITY_OWNER
            .toString(), owner);
  }

  private static void cleanupOwnerInfo(TimelineEntity timelineEntity) {
    if (timelineEntity.getPrimaryFilters() != null) {
      timelineEntity.getPrimaryFilters().remove(
          TimelineStore.SystemFilter.ENTITY_OWNER.toString());
    }
  }

}
