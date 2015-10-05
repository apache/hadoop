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

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineReaderUtils;

import com.google.common.base.Preconditions;

/**
 * Timeline entity reader for application entities that are stored in the
 * application table.
 */
class ApplicationEntityReader extends GenericEntityReader {
  private static final ApplicationTable APPLICATION_TABLE =
      new ApplicationTable();

  public ApplicationEntityReader(String userId, String clusterId,
      String flowId, Long flowRunId, String appId, String entityType,
      Long limit, Long createdTimeBegin, Long createdTimeEnd,
      Long modifiedTimeBegin, Long modifiedTimeEnd,
      Map<String, Set<String>> relatesTo, Map<String, Set<String>> isRelatedTo,
      Map<String, Object> infoFilters, Map<String, String> configFilters,
      Set<String> metricFilters, Set<String> eventFilters,
      EnumSet<Field> fieldsToRetrieve) {
    super(userId, clusterId, flowId, flowRunId, appId, entityType, limit,
        createdTimeBegin, createdTimeEnd, modifiedTimeBegin, modifiedTimeEnd,
        relatesTo, isRelatedTo, infoFilters, configFilters, metricFilters,
        eventFilters, fieldsToRetrieve, true);
  }

  public ApplicationEntityReader(String userId, String clusterId,
      String flowId, Long flowRunId, String appId, String entityType,
      String entityId, EnumSet<Field> fieldsToRetrieve) {
    super(userId, clusterId, flowId, flowRunId, appId, entityType, entityId,
        fieldsToRetrieve);
  }

  /**
   * Uses the {@link ApplicationTable}.
   */
  protected BaseTable<?> getTable() {
    return APPLICATION_TABLE;
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn)
      throws IOException {
    byte[] rowKey =
        ApplicationRowKey.getRowKey(clusterId, userId, flowId, flowRunId,
            appId);
    Get get = new Get(rowKey);
    get.setMaxVersions(Integer.MAX_VALUE);
    return table.getResult(hbaseConf, conn, get);
  }

  @Override
  protected void validateParams() {
    Preconditions.checkNotNull(userId, "userId shouldn't be null");
    Preconditions.checkNotNull(clusterId, "clusterId shouldn't be null");
    Preconditions.checkNotNull(entityType, "entityType shouldn't be null");
    if (singleEntityRead) {
      Preconditions.checkNotNull(appId, "appId shouldn't be null");
    } else {
      Preconditions.checkNotNull(flowId, "flowId shouldn't be null");
    }
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn)
      throws IOException {
    if (singleEntityRead) {
      if (flowId == null || flowRunId == null) {
        FlowContext context =
            lookupFlowContext(clusterId, appId, hbaseConf, conn);
        flowId = context.flowId;
        flowRunId = context.flowRunId;
      }
    }
    if (fieldsToRetrieve == null) {
      fieldsToRetrieve = EnumSet.noneOf(Field.class);
    }
    if (!singleEntityRead) {
      if (limit == null || limit < 0) {
        limit = TimelineReader.DEFAULT_LIMIT;
      }
      if (createdTimeBegin == null) {
        createdTimeBegin = DEFAULT_BEGIN_TIME;
      }
      if (createdTimeEnd == null) {
        createdTimeEnd = DEFAULT_END_TIME;
      }
      if (modifiedTimeBegin == null) {
        modifiedTimeBegin = DEFAULT_BEGIN_TIME;
      }
      if (modifiedTimeEnd == null) {
        modifiedTimeEnd = DEFAULT_END_TIME;
      }
    }
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf,
      Connection conn) throws IOException {
    Scan scan = new Scan();
    if (flowRunId != null) {
      scan.setRowPrefixFilter(ApplicationRowKey.
          getRowKeyPrefix(clusterId, userId, flowId, flowRunId));
    } else {
      scan.setRowPrefixFilter(ApplicationRowKey.
          getRowKeyPrefix(clusterId, userId, flowId));
    }
    scan.setFilter(new PageFilter(limit));
    return table.getResultScanner(hbaseConf, conn, scan);
  }

  @Override
  protected TimelineEntity parseEntity(Result result) throws IOException {
    if (result == null || result.isEmpty()) {
      return null;
    }
    TimelineEntity entity = new TimelineEntity();
    entity.setType(TimelineEntityType.YARN_APPLICATION.toString());
    String entityId = ApplicationColumn.ID.readResult(result).toString();
    entity.setId(entityId);

    // fetch created time
    Number createdTime =
        (Number)ApplicationColumn.CREATED_TIME.readResult(result);
    entity.setCreatedTime(createdTime.longValue());
    if (!singleEntityRead && (entity.getCreatedTime() < createdTimeBegin ||
        entity.getCreatedTime() > createdTimeEnd)) {
      return null;
    }

    // fetch modified time
    Number modifiedTime =
        (Number)ApplicationColumn.MODIFIED_TIME.readResult(result);
    entity.setModifiedTime(modifiedTime.longValue());
    if (!singleEntityRead && (entity.getModifiedTime() < modifiedTimeBegin ||
        entity.getModifiedTime() > modifiedTimeEnd)) {
      return null;
    }

    // fetch is related to entities
    boolean checkIsRelatedTo = isRelatedTo != null && isRelatedTo.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.IS_RELATED_TO) || checkIsRelatedTo) {
      readRelationship(entity, result, ApplicationColumnPrefix.IS_RELATED_TO,
          true);
      if (checkIsRelatedTo && !TimelineReaderUtils.matchRelations(
          entity.getIsRelatedToEntities(), isRelatedTo)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.IS_RELATED_TO)) {
        entity.getIsRelatedToEntities().clear();
      }
    }

    // fetch relates to entities
    boolean checkRelatesTo = relatesTo != null && relatesTo.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.RELATES_TO) || checkRelatesTo) {
      readRelationship(entity, result, ApplicationColumnPrefix.RELATES_TO,
          false);
      if (checkRelatesTo && !TimelineReaderUtils.matchRelations(
          entity.getRelatesToEntities(), relatesTo)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.RELATES_TO)) {
        entity.getRelatesToEntities().clear();
      }
    }

    // fetch info
    boolean checkInfo = infoFilters != null && infoFilters.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.INFO) || checkInfo) {
      readKeyValuePairs(entity, result, ApplicationColumnPrefix.INFO, false);
      if (checkInfo &&
          !TimelineReaderUtils.matchFilters(entity.getInfo(), infoFilters)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.INFO)) {
        entity.getInfo().clear();
      }
    }

    // fetch configs
    boolean checkConfigs = configFilters != null && configFilters.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.CONFIGS) || checkConfigs) {
      readKeyValuePairs(entity, result, ApplicationColumnPrefix.CONFIG, true);
      if (checkConfigs && !TimelineReaderUtils.matchFilters(
          entity.getConfigs(), configFilters)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.CONFIGS)) {
        entity.getConfigs().clear();
      }
    }

    // fetch events
    boolean checkEvents = eventFilters != null && eventFilters.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.EVENTS) || checkEvents) {
      readEvents(entity, result, true);
      if (checkEvents && !TimelineReaderUtils.matchEventFilters(
          entity.getEvents(), eventFilters)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.EVENTS)) {
        entity.getEvents().clear();
      }
    }

    // fetch metrics
    boolean checkMetrics = metricFilters != null && metricFilters.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.METRICS) || checkMetrics) {
      readMetrics(entity, result, ApplicationColumnPrefix.METRIC);
      if (checkMetrics && !TimelineReaderUtils.matchMetricFilters(
          entity.getMetrics(), metricFilters)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.METRICS)) {
        entity.getMetrics().clear();
      }
    }
    return entity;
  }
}
