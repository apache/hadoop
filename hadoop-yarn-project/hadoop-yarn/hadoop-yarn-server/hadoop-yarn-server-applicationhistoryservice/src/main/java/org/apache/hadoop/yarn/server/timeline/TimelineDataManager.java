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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.webapp.BadRequestException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class wrap over the timeline store and the ACLs manager. It does some non
 * trivial manipulation of the timeline data before putting or after getting it
 * from the timeline store, and checks the user's access to it.
 *
 */
public class TimelineDataManager extends AbstractService {

  private static final Logger LOG =
          LoggerFactory.getLogger(TimelineDataManager.class);
  @VisibleForTesting
  public static final String DEFAULT_DOMAIN_ID = "DEFAULT";

  private TimelineDataManagerMetrics metrics;
  private TimelineStore store;
  private TimelineACLsManager timelineACLsManager;

  public TimelineDataManager(TimelineStore store,
      TimelineACLsManager timelineACLsManager) {
    super(TimelineDataManager.class.getName());
    this.store = store;
    this.timelineACLsManager = timelineACLsManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    metrics = TimelineDataManagerMetrics.create();
    TimelineDomain domain = store.getDomain("DEFAULT");
    // it is okay to reuse an existing domain even if it was created by another
    // user of the timeline server before, because it allows everybody to access.
    if (domain == null) {
      // create a default domain, which allows everybody to access and modify
      // the entities in it.
      domain = new TimelineDomain();
      domain.setId(DEFAULT_DOMAIN_ID);
      domain.setDescription("System Default Domain");
      domain.setOwner(
          UserGroupInformation.getCurrentUser().getShortUserName());
      domain.setReaders("*");
      domain.setWriters("*");
      store.put(domain);
    }
    super.serviceInit(conf);
  }

  public interface CheckAcl {
    boolean check(TimelineEntity entity) throws IOException;
  }

  class CheckAclImpl implements CheckAcl {
    final UserGroupInformation ugi;

    public CheckAclImpl(UserGroupInformation callerUGI) {
      ugi = callerUGI;
    }

    public boolean check(TimelineEntity entity) throws IOException {
      try{
        return timelineACLsManager.checkAccess(
          ugi, ApplicationAccessType.VIEW_APP, entity);
      } catch (YarnException e) {
        LOG.info("Error when verifying access for user " + ugi
          + " on the events of the timeline entity "
          + new EntityIdentifier(entity.getEntityId(),
          entity.getEntityType()), e);
        return false;
      }
    }
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
    long startTime = Time.monotonicNow();
    metrics.incrGetEntitiesOps();
    try {
      TimelineEntities entities = doGetEntities(
          entityType,
          primaryFilter,
          secondaryFilter,
          windowStart,
          windowEnd,
          fromId,
          fromTs,
          limit,
          fields,
          callerUGI);
      metrics.incrGetEntitiesTotal(entities.getEntities().size());
      return entities;
    } finally {
      metrics.addGetEntitiesTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineEntities doGetEntities(
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
    entities = store.getEntities(
        entityType,
        limit,
        windowStart,
        windowEnd,
        fromId,
        fromTs,
        primaryFilter,
        secondaryFilter,
        fields,
        new CheckAclImpl(callerUGI));

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
    long startTime = Time.monotonicNow();
    metrics.incrGetEntityOps();
    try {
      return doGetEntity(entityType, entityId, fields, callerUGI);
    } finally {
      metrics.addGetEntityTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineEntity doGetEntity(
      String entityType,
      String entityId,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineEntity entity = null;
    entity =
        store.getEntity(entityId, entityType, fields);
    if (entity != null) {
      addDefaultDomainIdIfAbsent(entity);
      // check ACLs
      if (!timelineACLsManager.checkAccess(
          callerUGI, ApplicationAccessType.VIEW_APP, entity)) {
        final String user = callerUGI != null ? callerUGI.getShortUserName():
            null;
        throw new YarnException(
            user + " is not allowed to get the timeline entity "
            + "{ id: " + entity.getEntityId() + ", type: "
            + entity.getEntityType() + " }.");
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
    long startTime = Time.monotonicNow();
    metrics.incrGetEventsOps();
    try {
      TimelineEvents events = doGetEvents(
          entityType,
          entityIds,
          eventTypes,
          windowStart,
          windowEnd,
          limit,
          callerUGI);
      metrics.incrGetEventsTotal(events.getAllEvents().size());
      return events;
    } finally {
      metrics.addGetEventsTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineEvents doGetEvents(
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
          addDefaultDomainIdIfAbsent(entity);
          // check ACLs
          if (!timelineACLsManager.checkAccess(
              callerUGI, ApplicationAccessType.VIEW_APP, entity)) {
            eventsItr.remove();
          }
        } catch (Exception e) {
          LOG.warn("Error when verifying access for user " + callerUGI
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
    long startTime = Time.monotonicNow();
    metrics.incrPostEntitiesOps();
    try {
      return doPostEntities(entities, callerUGI);
    } finally {
      metrics.addPostEntitiesTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelinePutResponse doPostEntities(
      TimelineEntities entities,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    if (entities == null) {
      return new TimelinePutResponse();
    }
    metrics.incrPostEntitiesTotal(entities.getEntities().size());
    TimelineEntities entitiesToPut = new TimelineEntities();
    List<TimelinePutResponse.TimelinePutError> errors =
        new ArrayList<TimelinePutResponse.TimelinePutError>();
    for (TimelineEntity entity : entities.getEntities()) {

      // if the domain id is not specified, the entity will be put into
      // the default domain
      if (entity.getDomainId() == null ||
          entity.getDomainId().length() == 0) {
        entity.setDomainId(DEFAULT_DOMAIN_ID);
      }
      if (entity.getEntityId() == null || entity.getEntityType() == null) {
        throw new BadRequestException("Incomplete entity without entity"
            + " id/type");
      }
      // check if there is existing entity
      TimelineEntity existingEntity = null;
      try {
        existingEntity =
            store.getEntity(entity.getEntityId(), entity.getEntityType(),
                EnumSet.of(Field.PRIMARY_FILTERS));
        if (existingEntity != null) {
          addDefaultDomainIdIfAbsent(existingEntity);
          if (!existingEntity.getDomainId().equals(entity.getDomainId())) {
            throw new YarnException("The domain of the timeline entity "
              + "{ id: " + entity.getEntityId() + ", type: "
              + entity.getEntityType() + " } is not allowed to be changed from "
              + existingEntity.getDomainId() + " to " + entity.getDomainId());
          }
        }
        if (!timelineACLsManager.checkAccess(
            callerUGI, ApplicationAccessType.MODIFY_APP, entity)) {
          throw new YarnException(callerUGI
              + " is not allowed to put the timeline entity "
              + "{ id: " + entity.getEntityId() + ", type: "
              + entity.getEntityType() + " } into the domain "
              + entity.getDomainId() + ".");
        }
      } catch (Exception e) {
        // Skip the entity which already exists and was put by others
        LOG.warn("Skip the timeline entity: { id: " + entity.getEntityId()
            + ", type: "+ entity.getEntityType() + " }", e);
        TimelinePutResponse.TimelinePutError error =
            new TimelinePutResponse.TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(
            TimelinePutResponse.TimelinePutError.ACCESS_DENIED);
        errors.add(error);
        continue;
      }

      entitiesToPut.addEntity(entity);
    }

    TimelinePutResponse response = store.put(entitiesToPut);
    // add the errors of timeline system filter key conflict
    response.addErrors(errors);
    return response;
  }

  /**
   * Add or update an domain. If the domain already exists, only the owner
   * and the admin can update it.
   */
  public void putDomain(TimelineDomain domain,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrPutDomainOps();
    try {
      doPutDomain(domain, callerUGI);
    } finally {
      metrics.addPutDomainTime(Time.monotonicNow() - startTime);
    }
  }

  private void doPutDomain(TimelineDomain domain,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineDomain existingDomain =
        store.getDomain(domain.getId());
    if (existingDomain != null) {
      if (!timelineACLsManager.checkAccess(callerUGI, existingDomain)) {
        throw new YarnException(callerUGI.getShortUserName() +
            " is not allowed to override an existing domain " +
            existingDomain.getId());
      }
      // Set it again in case ACLs are not enabled: The domain can be
      // modified by every body, but the owner is not changed.
      domain.setOwner(existingDomain.getOwner());
    }
    store.put(domain);
    // If the domain exists already, it is likely to be in the cache.
    // We need to invalidate it.
    if (existingDomain != null) {
      timelineACLsManager.replaceIfExist(domain);
    }
  }

  /**
   * Get a single domain of the particular ID. If callerUGI is not the owner
   * or the admin of the domain, null will be returned.
   */
  public TimelineDomain getDomain(String domainId,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrGetDomainOps();
    try {
      return doGetDomain(domainId, callerUGI);
    } finally {
      metrics.addGetDomainTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineDomain doGetDomain(String domainId,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineDomain domain = store.getDomain(domainId);
    if (domain != null) {
      if (timelineACLsManager.checkAccess(callerUGI, domain)) {
        return domain;
      }
    }
    return null;
  }

  /**
   * Get all the domains that belong to the given owner. If callerUGI is not
   * the owner or the admin of the domain, empty list is going to be returned.
   */
  public TimelineDomains getDomains(String owner,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrGetDomainsOps();
    try {
      TimelineDomains domains = doGetDomains(owner, callerUGI);
      metrics.incrGetDomainsTotal(domains.getDomains().size());
      return domains;
    } finally {
      metrics.addGetDomainsTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineDomains doGetDomains(String owner,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineDomains domains = store.getDomains(owner);
    boolean hasAccess = true;
    if (domains.getDomains().size() > 0) {
      // The owner for each domain is the same, just need to check one
      hasAccess = timelineACLsManager.checkAccess(
          callerUGI, domains.getDomains().get(0));
    }
    if (hasAccess) {
      return domains;
    } else {
      return new TimelineDomains();
    }
  }

  private static void addDefaultDomainIdIfAbsent(TimelineEntity entity) {
    // be compatible with the timeline data created before 2.6
    if (entity.getDomainId() == null) {
      entity.setDomainId(DEFAULT_DOMAIN_ID);
    }
  }

}
