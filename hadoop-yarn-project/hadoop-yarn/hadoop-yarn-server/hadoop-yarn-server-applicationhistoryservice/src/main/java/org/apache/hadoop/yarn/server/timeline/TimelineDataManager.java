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

package org.apache.hadoop.yarn.server.timeline;

import static org.apache.hadoop.yarn.util.StringHelper.CSV_JOINER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * The class wrap over the timeline store and the ACLs manager. It does some non
 * trivial manipulation of the timeline data before putting or after getting it
 * from the timeline store, and checks the user's access to it.
 * 
 */
public class TimelineDataManager {

  private static final Log LOG = LogFactory.getLog(TimelineDataManager.class);

  private TimelineStore store;
  private TimelineACLsManager timelineACLsManager;

  public TimelineDataManager(TimelineStore store,
      TimelineACLsManager timelineACLsManager) {
    this.store = store;
    this.timelineACLsManager = timelineACLsManager;
  }

  /**
   * Get the timeline entities that the given user have access to. The meaning
   * of each argument has been documented with
   * {@link TimelineReader#getEntities}.
   * 
   * @see TimelineReader#getEntities
   */
  public TimelineEntities getEntities(
      String entityType,
      NameValuePair primaryFilter,
      Collection<NameValuePair> secondaryFilter,
      Long windowStart,
      Long windowEnd,
      String fromId,
      Long fromTs,
      Long limit,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineEntities entities = null;
    boolean modified = extendFields(fields);
    entities = store.getEntities(
        entityType,
        limit,
        windowStart,
        windowEnd,
        fromId,
        fromTs,
        primaryFilter,
        secondaryFilter,
        fields);
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
    if (entities == null) {
      return new TimelineEntities();
    }
    return entities;
  }

  /**
   * Get the single timeline entity that the given user has access to. The
   * meaning of each argument has been documented with
   * {@link TimelineReader#getEntity}.
   * 
   * @see TimelineReader#getEntity
   */
  public TimelineEntity getEntity(
      String entityType,
      String entityId,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineEntity entity = null;
    boolean modified = extendFields(fields);
    entity =
        store.getEntity(entityId, entityType, fields);
    if (entity != null) {
      // check ACLs
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
    return entity;
  }

  /**
   * Get the events whose entities the given user has access to. The meaning of
   * each argument has been documented with
   * {@link TimelineReader#getEntityTimelines}.
   * 
   * @see TimelineReader#getEntityTimelines
   */
  public TimelineEvents getEvents(
      String entityType,
      SortedSet<String> entityIds,
      SortedSet<String> eventTypes,
      Long windowStart,
      Long windowEnd,
      Long limit,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineEvents events = null;
    events = store.getEntityTimelines(
        entityType,
        entityIds,
        limit,
        windowStart,
        windowEnd,
        eventTypes);
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
    if (events == null) {
      return new TimelineEvents();
    }
    return events;
  }

  /**
   * Store the timeline entities into the store and set the owner of them to the
   * given user.
   */
  public TimelinePutResponse postEntities(
      TimelineEntities entities,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    if (entities == null) {
      return new TimelinePutResponse();
    }
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
        LOG.error("Skip the timeline entity: " + entityID + ", because "
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
          injectOwnerInfo(entity, callerUGI.getShortUserName());
        }
      } catch (YarnException e) {
        // Skip the entity which messes up the primary filter and record the
        // error
        LOG.error("Skip the timeline entity: " + entityID + ", because "
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
    TimelinePutResponse response = store.put(entitiesToPut);
    // add the errors of timeline system filter key conflict
    response.addErrors(errors);
    return response;
  }

  private static boolean extendFields(EnumSet<Field> fieldEnums) {
    boolean modified = false;
    if (fieldEnums != null && !fieldEnums.contains(Field.PRIMARY_FILTERS)) {
      fieldEnums.add(Field.PRIMARY_FILTERS);
      modified = true;
    }
    return modified;
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
