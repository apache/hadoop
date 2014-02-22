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

package org.apache.hadoop.yarn.server.applicationhistoryservice.apptimeline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntities;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntity;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvent;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvents;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvents.ATSEventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSPutErrors;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSPutErrors.ATSPutError;

/**
 * In-memory implementation of {@link ApplicationTimelineStore}. This
 * implementation is for test purpose only. If users improperly instantiate it,
 * they may encounter reading and writing history data in different memory
 * store.
 * 
 */
@Private
@Unstable
public class MemoryApplicationTimelineStore
    extends AbstractService implements ApplicationTimelineStore {

  private Map<EntityIdentifier, ATSEntity> entities =
      new HashMap<EntityIdentifier, ATSEntity>();

  public MemoryApplicationTimelineStore() {
    super(MemoryApplicationTimelineStore.class.getName());
  }

  @Override
  public ATSEntities getEntities(String entityType, Long limit,
      Long windowStart, Long windowEnd, NameValuePair primaryFilter,
      Collection<NameValuePair> secondaryFilters, EnumSet<Field> fields) {
    if (limit == null) {
      limit = DEFAULT_LIMIT;
    }
    if (windowStart == null) {
      windowStart = Long.MIN_VALUE;
    }
    if (windowEnd == null) {
      windowEnd = Long.MAX_VALUE;
    }
    if (fields == null) {
      fields = EnumSet.allOf(Field.class);
    }
    List<ATSEntity> entitiesSelected = new ArrayList<ATSEntity>();
    for (ATSEntity entity : new PriorityQueue<ATSEntity>(entities.values())) {
      if (entitiesSelected.size() >= limit) {
        break;
      }
      if (!entity.getEntityType().equals(entityType)) {
        continue;
      }
      if (entity.getStartTime() <= windowStart) {
        continue;
      }
      if (entity.getStartTime() > windowEnd) {
        continue;
      }
      if (primaryFilter != null &&
          !matchPrimaryFilter(entity.getPrimaryFilters(), primaryFilter)) {
        continue;
      }
      if (secondaryFilters != null) { // OR logic
        boolean flag = false;
        for (NameValuePair secondaryFilter : secondaryFilters) {
          if (secondaryFilter != null &&
              matchFilter(entity.getOtherInfo(), secondaryFilter)) {
            flag = true;
            break;
          }
        }
        if (!flag) {
          continue;
        }
      }
      entitiesSelected.add(entity);
    }
    List<ATSEntity> entitiesToReturn = new ArrayList<ATSEntity>();
    for (ATSEntity entitySelected : entitiesSelected) {
      entitiesToReturn.add(maskFields(entitySelected, fields));
    }
    Collections.sort(entitiesToReturn);
    ATSEntities entitiesWrapper = new ATSEntities();
    entitiesWrapper.setEntities(entitiesToReturn);
    return entitiesWrapper;
  }

  @Override
  public ATSEntity getEntity(String entityId, String entityType,
      EnumSet<Field> fieldsToRetrieve) {
    if (fieldsToRetrieve == null) {
      fieldsToRetrieve = EnumSet.allOf(Field.class);
    }
    ATSEntity entity = entities.get(new EntityIdentifier(entityId, entityType));
    if (entity == null) {
      return null;
    } else {
      return maskFields(entity, fieldsToRetrieve);
    }
  }

  @Override
  public ATSEvents getEntityTimelines(String entityType,
      SortedSet<String> entityIds, Long limit, Long windowStart,
      Long windowEnd,
      Set<String> eventTypes) {
    ATSEvents allEvents = new ATSEvents();
    if (entityIds == null) {
      return allEvents;
    }
    if (limit == null) {
      limit = DEFAULT_LIMIT;
    }
    if (windowStart == null) {
      windowStart = Long.MIN_VALUE;
    }
    if (windowEnd == null) {
      windowEnd = Long.MAX_VALUE;
    }
    for (String entityId : entityIds) {
      EntityIdentifier entityID = new EntityIdentifier(entityId, entityType);
      ATSEntity entity = entities.get(entityID);
      if (entity == null) {
        continue;
      }
      ATSEventsOfOneEntity events = new ATSEventsOfOneEntity();
      events.setEntityId(entityId);
      events.setEntityType(entityType);
      for (ATSEvent event : entity.getEvents()) {
        if (events.getEvents().size() >= limit) {
          break;
        }
        if (event.getTimestamp() <= windowStart) {
          continue;
        }
        if (event.getTimestamp() > windowEnd) {
          continue;
        }
        if (eventTypes != null && !eventTypes.contains(event.getEventType())) {
          continue;
        }
        events.addEvent(event);
      }
      allEvents.addEvent(events);
    }
    return allEvents;
  }

  @Override
  public ATSPutErrors put(ATSEntities data) {
    ATSPutErrors errors = new ATSPutErrors();
    for (ATSEntity entity : data.getEntities()) {
      EntityIdentifier entityId =
          new EntityIdentifier(entity.getEntityId(), entity.getEntityType());
      // store entity info in memory
      ATSEntity existingEntity = entities.get(entityId);
      if (existingEntity == null) {
        existingEntity = new ATSEntity();
        existingEntity.setEntityId(entity.getEntityId());
        existingEntity.setEntityType(entity.getEntityType());
        existingEntity.setStartTime(entity.getStartTime());
        entities.put(entityId, existingEntity);
      }
      if (entity.getEvents() != null) {
        if (existingEntity.getEvents() == null) {
          existingEntity.setEvents(entity.getEvents());
        } else {
          existingEntity.addEvents(entity.getEvents());
        }
        Collections.sort(existingEntity.getEvents());
      }
      // check startTime
      if (existingEntity.getStartTime() == null) {
        if (existingEntity.getEvents() == null
            || existingEntity.getEvents().isEmpty()) {
          ATSPutError error = new ATSPutError();
          error.setEntityId(entityId.getId());
          error.setEntityType(entityId.getType());
          error.setErrorCode(ATSPutError.NO_START_TIME);
          errors.addError(error);
          entities.remove(entityId);
          continue;
        } else {
          existingEntity.setStartTime(entity.getEvents().get(0).getTimestamp());
        }
      }
      if (entity.getPrimaryFilters() != null) {
        if (existingEntity.getPrimaryFilters() == null) {
          existingEntity.setPrimaryFilters(entity.getPrimaryFilters());
        } else {
          existingEntity.addPrimaryFilters(entity.getPrimaryFilters());
        }
      }
      if (entity.getOtherInfo() != null) {
        if (existingEntity.getOtherInfo() == null) {
          existingEntity.setOtherInfo(entity.getOtherInfo());
        } else {
          existingEntity.addOtherInfo(entity.getOtherInfo());
        }
      }
      // relate it to other entities
      if (entity.getRelatedEntities() == null) {
        continue;
      }
      for (Map.Entry<String, Set<String>> partRelatedEntities : entity
          .getRelatedEntities().entrySet()) {
        if (partRelatedEntities == null) {
          continue;
        }
        for (String idStr : partRelatedEntities.getValue()) {
          EntityIdentifier relatedEntityId =
              new EntityIdentifier(idStr, partRelatedEntities.getKey());
          ATSEntity relatedEntity = entities.get(relatedEntityId);
          if (relatedEntity != null) {
            relatedEntity.addRelatedEntity(
                existingEntity.getEntityType(), existingEntity.getEntityId());
          } else {
            relatedEntity = new ATSEntity();
            relatedEntity.setEntityId(relatedEntityId.getId());
            relatedEntity.setEntityType(relatedEntityId.getType());
            relatedEntity.setStartTime(existingEntity.getStartTime());
            relatedEntity.addRelatedEntity(existingEntity.getEntityType(),
                existingEntity.getEntityId());
            entities.put(relatedEntityId, relatedEntity);
          }
        }
      }
    }
    return errors;
  }

  private static ATSEntity maskFields(
      ATSEntity entity, EnumSet<Field> fields) {
    // Conceal the fields that are not going to be exposed
    ATSEntity entityToReturn = new ATSEntity();
    entityToReturn.setEntityId(entity.getEntityId());
    entityToReturn.setEntityType(entity.getEntityType());
    entityToReturn.setStartTime(entity.getStartTime());
    entityToReturn.setEvents(fields.contains(Field.EVENTS) ?
        entity.getEvents() : fields.contains(Field.LAST_EVENT_ONLY) ?
            Arrays.asList(entity.getEvents().get(0)) : null);
    entityToReturn.setRelatedEntities(fields.contains(Field.RELATED_ENTITIES) ?
        entity.getRelatedEntities() : null);
    entityToReturn.setPrimaryFilters(fields.contains(Field.PRIMARY_FILTERS) ?
        entity.getPrimaryFilters() : null);
    entityToReturn.setOtherInfo(fields.contains(Field.OTHER_INFO) ?
        entity.getOtherInfo() : null);
    return entityToReturn;
  }

  private static boolean matchFilter(Map<String, Object> tags,
      NameValuePair filter) {
    Object value = tags.get(filter.getName());
    if (value == null) { // doesn't have the filter
      return false;
    } else if (!value.equals(filter.getValue())) { // doesn't match the filter
      return false;
    }
    return true;
  }

  private static boolean matchPrimaryFilter(Map<String, Set<Object>> tags,
      NameValuePair filter) {
    Set<Object> value = tags.get(filter.getName());
    if (value == null) { // doesn't have the filter
      return false;
    } else {
      return value.contains(filter.getValue());
    }
  }

}
