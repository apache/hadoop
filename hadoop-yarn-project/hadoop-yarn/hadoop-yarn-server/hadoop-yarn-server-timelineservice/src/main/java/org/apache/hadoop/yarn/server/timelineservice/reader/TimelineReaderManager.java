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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AdminACLsManager;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;

/**
 * This class wraps over the timeline reader store implementation. It does some
 * non trivial manipulation of the timeline data before or after getting
 * it from the backend store.
 */
@Private
@Unstable
public class TimelineReaderManager extends AbstractService {

  private TimelineReader reader;
  private AdminACLsManager adminACLsManager;

  public TimelineReaderManager(TimelineReader timelineReader) {
    super(TimelineReaderManager.class.getName());
    this.reader = timelineReader;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // TODO Once ACLS story is played, this need to be removed or modified.
    this.adminACLsManager = new AdminACLsManager(conf);
  }

  /**
   * Gets cluster ID from config yarn.resourcemanager.cluster-id
   * if not supplied by client.
   * @param clusterId
   * @param conf
   * @return clusterId
   */
  private static String getClusterID(String clusterId, Configuration conf) {
    if (clusterId == null || clusterId.isEmpty()) {
      return conf.get(
          YarnConfiguration.RM_CLUSTER_ID,
              YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
    }
    return clusterId;
  }

  private static TimelineEntityType getTimelineEntityType(String entityType) {
    if (entityType == null) {
      return null;
    }
    try {
      return TimelineEntityType.valueOf(entityType);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Fill UID in the info field of entity based on the query(identified by
   * entity type).
   * @param entityType Entity type of query.
   * @param entity Timeline Entity.
   * @param context Context defining the query.
   */
  private static void fillUID(TimelineEntityType entityType,
      TimelineEntity entity, TimelineReaderContext context) {
    if (entityType != null) {
      switch(entityType) {
      case YARN_FLOW_ACTIVITY:
        FlowActivityEntity activityEntity = (FlowActivityEntity)entity;
        context.setUserId(activityEntity.getUser());
        context.setFlowName(activityEntity.getFlowName());
        entity.setUID(TimelineReaderUtils.UID_KEY,
            TimelineUIDConverter.FLOW_UID.encodeUID(context));
        return;
      case YARN_FLOW_RUN:
        FlowRunEntity runEntity = (FlowRunEntity)entity;
        context.setFlowRunId(runEntity.getRunId());
        entity.setUID(TimelineReaderUtils.UID_KEY,
            TimelineUIDConverter.FLOWRUN_UID.encodeUID(context));
        return;
      case YARN_APPLICATION:
        context.setAppId(entity.getId());
        entity.setUID(TimelineReaderUtils.UID_KEY,
            TimelineUIDConverter.APPLICATION_UID.encodeUID(context));
        return;
      default:
        break;
      }
    }
    context.setEntityType(entity.getType());
    context.setEntityIdPrefix(entity.getIdPrefix());
    context.setEntityId(entity.getId());
    if (context.getDoAsUser() != null) {
      entity.setUID(TimelineReaderUtils.UID_KEY,
          TimelineUIDConverter.SUB_APPLICATION_ENTITY_UID.encodeUID(context));
    } else {
      entity.setUID(TimelineReaderUtils.UID_KEY,
          TimelineUIDConverter.GENERIC_ENTITY_UID.encodeUID(context));
    }
  }

  /**
   * Get a set of entities matching given predicates by making a call to
   * backend storage implementation. The meaning of each argument has been
   * documented in detail with {@link TimelineReader#getEntities}.If cluster ID
   * has not been supplied by the client, fills the cluster id from config
   * before making a call to backend storage. After fetching entities from
   * backend, fills the appropriate UID based on entity type for each entity.
   *
   * @param context Timeline context within the scope of which entities have to
   *     be fetched.
   * @param filters Filters which limit the number of entities to be returned.
   * @param dataToRetrieve Data to carry in each entity fetched.
   * @return a set of <cite>TimelineEntity</cite> objects.
   * @throws IOException if any problem occurs while getting entities.
   * @see TimelineReader#getEntities
   */
  public Set<TimelineEntity> getEntities(TimelineReaderContext context,
      TimelineEntityFilters filters, TimelineDataToRetrieve dataToRetrieve)
      throws IOException {
    context.setClusterId(getClusterID(context.getClusterId(), getConfig()));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext(context), filters, dataToRetrieve);
    if (entities != null) {
      TimelineEntityType type = getTimelineEntityType(context.getEntityType());
      for (TimelineEntity entity : entities) {
        fillUID(type, entity, context);
      }
    }
    return entities;
  }

  /**
   * Get single timeline entity by making a call to backend storage
   * implementation. The meaning of each argument in detail has been
   * documented with {@link TimelineReader#getEntity}. If cluster ID has not
   * been supplied by the client, fills the cluster id from config before making
   * a call to backend storage. After fetching entity from backend, fills the
   * appropriate UID based on entity type.
   *
   * @param context Timeline context within the scope of which entity has to be
   *     fetched.
   * @param dataToRetrieve Data to carry in the entity fetched.
   * @return A <cite>TimelineEntity</cite> object if found, null otherwise.
   * @throws IOException  if any problem occurs while getting entity.
   * @see TimelineReader#getEntity
   */
  public TimelineEntity getEntity(TimelineReaderContext context,
      TimelineDataToRetrieve dataToRetrieve) throws IOException {
    context.setClusterId(
        getClusterID(context.getClusterId(), getConfig()));
    TimelineEntity entity = reader.getEntity(
        new TimelineReaderContext(context), dataToRetrieve);
    if (entity != null) {
      TimelineEntityType type = getTimelineEntityType(context.getEntityType());
      fillUID(type, entity, context);
    }
    return entity;
  }

  /**
   * Gets a list of available timeline entity types for an application. This can
   * be done by making a call to the backend storage implementation. The meaning
   * of each argument in detail is the same as {@link TimelineReader#getEntity}.
   * If cluster ID has not been supplied by the client, fills the cluster id
   * from config before making a call to backend storage.
   *
   * @param context Timeline context within the scope of which entity types
   *                have to be fetched. Entity type field of this context should
   *                be null.
   * @return A set which contains available timeline entity types, represented
   * as strings if found, empty otherwise.
   * @throws IOException  if any problem occurs while getting entity types.
   */
  public Set<String> getEntityTypes(TimelineReaderContext context)
      throws IOException{
    context.setClusterId(getClusterID(context.getClusterId(), getConfig()));
    return reader.getEntityTypes(new TimelineReaderContext(context));
  }

  /**
   * The API to confirm is a User is allowed to read this data.
   * @param callerUGI UserGroupInformation of the user
   */
  public boolean checkAccess(UserGroupInformation callerUGI) {
    // TODO to be removed or modified once ACL story is played
    if (!adminACLsManager.areACLsEnabled()) {
      return true;
    }
    return callerUGI != null && adminACLsManager.isAdmin(callerUGI);
  }
}
