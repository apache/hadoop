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
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class TimelineReaderManager extends AbstractService {

  @VisibleForTesting
  public static final String UID_KEY = "UID";
  private TimelineReader reader;

  public TimelineReaderManager(TimelineReader timelineReader) {
    super(TimelineReaderManager.class.getName());
    this.reader = timelineReader;
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
        entity.setUID(UID_KEY,
            TimelineUIDConverter.FLOW_UID.encodeUID(context));
        return;
      case YARN_FLOW_RUN:
        FlowRunEntity runEntity = (FlowRunEntity)entity;
        context.setFlowRunId(runEntity.getRunId());
        entity.setUID(UID_KEY,
            TimelineUIDConverter.FLOWRUN_UID.encodeUID(context));
        return;
      case YARN_APPLICATION:
        context.setAppId(entity.getId());
        entity.setUID(UID_KEY,
            TimelineUIDConverter.APPLICATION_UID.encodeUID(context));
        return;
      default:
        break;
      }
    }
    context.setEntityType(entity.getType());
    context.setEntityId(entity.getId());
    entity.setUID(UID_KEY,
        TimelineUIDConverter.GENERIC_ENTITY_UID.encodeUID(context));
  }

  /**
   * Get a set of entities matching given predicates. The meaning of each
   * argument has been documented with {@link TimelineReader#getEntities}.
   *
   * @see TimelineReader#getEntities
   */
  public Set<TimelineEntity> getEntities(String userId, String clusterId,
      String flowName, Long flowRunId, String appId, String entityType,
      Long limit, Long createdTimeBegin, Long createdTimeEnd,
      Map<String, Set<String>> relatesTo, Map<String, Set<String>> isRelatedTo,
      Map<String, Object> infoFilters, Map<String, String> configFilters,
      Set<String>  metricFilters, Set<String> eventFilters,
      EnumSet<Field> fieldsToRetrieve) throws IOException {
    String cluster = getClusterID(clusterId, getConfig());
    Set<TimelineEntity> entities =
        reader.getEntities(userId, cluster, flowName, flowRunId, appId,
        entityType, limit, createdTimeBegin, createdTimeEnd, relatesTo,
        isRelatedTo, infoFilters, configFilters, metricFilters, eventFilters,
        null, null, fieldsToRetrieve);
    if (entities != null) {
      TimelineEntityType type = getTimelineEntityType(entityType);
      TimelineReaderContext context = new TimelineReaderContext(cluster, userId,
          flowName, flowRunId, appId, entityType, null);
      for (TimelineEntity entity : entities) {
        fillUID(type, entity, context);
      }
    }
    return entities;
  }

  /**
   * Get single timeline entity. The meaning of each argument has been
   * documented with {@link TimelineReader#getEntity}.
   *
   * @see TimelineReader#getEntity
   */
  public TimelineEntity getEntity(String userId, String clusterId,
      String flowName, Long flowRunId, String appId, String entityType,
      String entityId, EnumSet<Field> fields) throws IOException {
    String cluster = getClusterID(clusterId, getConfig());
    TimelineEntity entity =
        reader.getEntity(userId, cluster, flowName, flowRunId, appId,
        entityType, entityId, null, null, fields);

    if (entity != null) {
      TimelineEntityType type = getTimelineEntityType(entityType);
      TimelineReaderContext context = new TimelineReaderContext(cluster, userId,
          flowName, flowRunId, appId, entityType, null);
      fillUID(type, entity, context);
    }
    return entity;
  }
}
