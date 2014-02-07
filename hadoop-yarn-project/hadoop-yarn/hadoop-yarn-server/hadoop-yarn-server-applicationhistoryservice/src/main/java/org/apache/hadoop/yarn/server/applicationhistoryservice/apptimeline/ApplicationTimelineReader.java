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

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntities;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntity;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvents;

/**
 * This interface is for retrieving application timeline information.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ApplicationTimelineReader {

  /**
   * Possible fields to retrieve for {@link #getEntities} and {@link
   * #getEntity}.
   */
  enum Field {
    EVENTS,
    RELATED_ENTITIES,
    PRIMARY_FILTERS,
    OTHER_INFO,
    LAST_EVENT_ONLY
  }

  /**
   * Default limit for {@link #getEntities} and {@link #getEntityTimelines}.
   */
  final long DEFAULT_LIMIT = 100;

  /**
   * This method retrieves a list of entity information, {@link ATSEntity},
   * sorted by the starting timestamp for the entity, descending.
   *
   * @param entityType The type of entities to return (required).
   * @param limit A limit on the number of entities to return. If null,
   *              defaults to {@link #DEFAULT_LIMIT}.
   * @param windowStart The earliest start timestamp to retrieve (exclusive).
   *                    If null, defaults to retrieving all entities until the
   *                    limit is reached.
   * @param windowEnd The latest start timestamp to retrieve (inclusive).
   *                  If null, defaults to {@link Long#MAX_VALUE}
   * @param primaryFilter Retrieves only entities that have the specified
   *                      primary filter. If null, retrieves all entities.
   *                      This is an indexed retrieval, and no entities that
   *                      do not match the filter are scanned.
   * @param secondaryFilters Retrieves only entities that have exact matches
   *                         for all the specified filters in their primary
   *                         filters or other info. This is not an indexed
   *                         retrieval, so all entities are scanned but only
   *                         those matching the filters are returned.
   * @param fieldsToRetrieve Specifies which fields of the entity object to
   *                         retrieve (see {@link Field}). If the set of fields
   *                         contains {@link Field#LAST_EVENT_ONLY} and not
   *                         {@link Field#EVENTS}, the most recent event for
   *                         each entity is retrieved.
   * @return An {@link ATSEntities} object.
   */
  ATSEntities getEntities(String entityType,
      Long limit, Long windowStart, Long windowEnd,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fieldsToRetrieve);

  /**
   * This method retrieves the entity information for a given entity.
   *
   * @param entity The entity whose information will be retrieved.
   * @param entityType The type of the entity.
   * @param fieldsToRetrieve Specifies which fields of the entity object to
   *                         retrieve (see {@link Field}). If the set of
   *                         fields contains {@link Field#LAST_EVENT_ONLY} and
   *                         not {@link Field#EVENTS}, the most recent event
   *                         for each entity is retrieved.
   * @return An {@link ATSEntity} object.
   */
  ATSEntity getEntity(String entity, String entityType, EnumSet<Field>
      fieldsToRetrieve);

  /**
   * This method retrieves the events for a list of entities all of the same
   * entity type. The events for each entity are sorted in order of their
   * timestamps, descending.
   *
   * @param entityType The type of entities to retrieve events for.
   * @param entityIds The entity IDs to retrieve events for.
   * @param limit A limit on the number of events to return for each entity.
   *              If null, defaults to  {@link #DEFAULT_LIMIT} events per
   *              entity.
   * @param windowStart If not null, retrieves only events later than the
   *                    given time (exclusive)
   * @param windowEnd If not null, retrieves only events earlier than the
   *                  given time (inclusive)
   * @param eventTypes Restricts the events returned to the given types. If
   *                   null, events of all types will be returned.
   * @return An {@link ATSEvents} object.
   */
  ATSEvents getEntityTimelines(String entityType,
      SortedSet<String> entityIds, Long limit, Long windowStart,
      Long windowEnd, Set<String> eventTypes);
}
