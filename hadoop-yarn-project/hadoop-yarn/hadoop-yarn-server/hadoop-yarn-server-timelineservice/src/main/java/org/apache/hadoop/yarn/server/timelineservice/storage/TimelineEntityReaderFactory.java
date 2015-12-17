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
package org.apache.hadoop.yarn.server.timelineservice.storage;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;

/**
 * Factory methods for instantiating a timeline entity reader.
 */
class TimelineEntityReaderFactory {
  /**
   * Creates a timeline entity reader instance for reading a single entity with
   * the specified input.
   */
  public static TimelineEntityReader createSingleEntityReader(String userId,
      String clusterId, String flowName, Long flowRunId, String appId,
      String entityType, String entityId, TimelineFilterList confs,
      TimelineFilterList metrics, EnumSet<Field> fieldsToRetrieve) {
    // currently the types that are handled separate from the generic entity
    // table are application, flow run, and flow activity entities
    if (TimelineEntityType.YARN_APPLICATION.matches(entityType)) {
      return new ApplicationEntityReader(userId, clusterId, flowName, flowRunId,
          appId, entityType, entityId, confs, metrics, fieldsToRetrieve);
    } else if (TimelineEntityType.YARN_FLOW_RUN.matches(entityType)) {
      return new FlowRunEntityReader(userId, clusterId, flowName, flowRunId,
          appId, entityType, entityId, confs, metrics, fieldsToRetrieve);
    } else if (TimelineEntityType.YARN_FLOW_ACTIVITY.matches(entityType)) {
      return new FlowActivityEntityReader(userId, clusterId, flowName, flowRunId,
          appId, entityType, entityId, fieldsToRetrieve);
    } else {
      // assume we're dealing with a generic entity read
      return new GenericEntityReader(userId, clusterId, flowName, flowRunId,
        appId, entityType, entityId, confs, metrics, fieldsToRetrieve);
    }
  }

  /**
   * Creates a timeline entity reader instance for reading set of entities with
   * the specified input and predicates.
   */
  public static TimelineEntityReader createMultipleEntitiesReader(String userId,
      String clusterId, String flowName, Long flowRunId, String appId,
      String entityType, Long limit, Long createdTimeBegin, Long createdTimeEnd,
      Long modifiedTimeBegin, Long modifiedTimeEnd,
      Map<String, Set<String>> relatesTo, Map<String, Set<String>> isRelatedTo,
      Map<String, Object> infoFilters, Map<String, String> configFilters,
      Set<String> metricFilters, Set<String> eventFilters,
      TimelineFilterList confs, TimelineFilterList metrics,
      EnumSet<Field> fieldsToRetrieve) {
    // currently the types that are handled separate from the generic entity
    // table are application, flow run, and flow activity entities
    if (TimelineEntityType.YARN_APPLICATION.matches(entityType)) {
      return new ApplicationEntityReader(userId, clusterId, flowName, flowRunId,
          appId, entityType, limit, createdTimeBegin, createdTimeEnd,
          modifiedTimeBegin, modifiedTimeEnd, relatesTo, isRelatedTo,
          infoFilters, configFilters, metricFilters, eventFilters, confs,
          metrics, fieldsToRetrieve);
    } else if (TimelineEntityType.YARN_FLOW_ACTIVITY.matches(entityType)) {
      return new FlowActivityEntityReader(userId, clusterId, flowName, flowRunId,
          appId, entityType, limit, createdTimeBegin, createdTimeEnd,
          modifiedTimeBegin, modifiedTimeEnd, relatesTo, isRelatedTo,
          infoFilters, configFilters, metricFilters, eventFilters,
          fieldsToRetrieve);
    } else if (TimelineEntityType.YARN_FLOW_RUN.matches(entityType)) {
      return new FlowRunEntityReader(userId, clusterId, flowName, flowRunId,
          appId, entityType, limit, createdTimeBegin, createdTimeEnd,
          modifiedTimeBegin, modifiedTimeEnd, relatesTo, isRelatedTo,
          infoFilters, configFilters, metricFilters, eventFilters, confs,
          metrics, fieldsToRetrieve);
    } else {
      // assume we're dealing with a generic entity read
      return new GenericEntityReader(userId, clusterId, flowName, flowRunId,
          appId, entityType, limit, createdTimeBegin, createdTimeEnd,
          modifiedTimeBegin, modifiedTimeEnd, relatesTo, isRelatedTo,
          infoFilters, configFilters, metricFilters, eventFilters, confs,
          metrics, fieldsToRetrieve, false);
    }
  }
}
