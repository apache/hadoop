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
package org.apache.hadoop.yarn.server.timelineservice.storage.reader;

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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;

import com.google.common.base.Preconditions;

/**
 * Timeline entity reader for application entities that are stored in the
 * application table.
 */
class ApplicationEntityReader extends GenericEntityReader {
  private static final ApplicationTable APPLICATION_TABLE =
      new ApplicationTable();

  public ApplicationEntityReader(String userId, String clusterId,
      String flowName, Long flowRunId, String appId, String entityType,
      Long limit, Long createdTimeBegin, Long createdTimeEnd,
      Map<String, Set<String>> relatesTo, Map<String, Set<String>> isRelatedTo,
      Map<String, Object> infoFilters, Map<String, String> configFilters,
      Set<String> metricFilters, Set<String> eventFilters,
      TimelineFilterList confsToRetrieve, TimelineFilterList metricsToRetrieve,
      EnumSet<Field> fieldsToRetrieve) {
    super(userId, clusterId, flowName, flowRunId, appId, entityType, limit,
        createdTimeBegin, createdTimeEnd, relatesTo, isRelatedTo, infoFilters,
        configFilters, metricFilters, eventFilters, confsToRetrieve,
        metricsToRetrieve, fieldsToRetrieve, true);
  }

  public ApplicationEntityReader(String userId, String clusterId,
      String flowName, Long flowRunId, String appId, String entityType,
      String entityId, TimelineFilterList confsToRetrieve,
      TimelineFilterList metricsToRetrieve, EnumSet<Field> fieldsToRetrieve) {
    super(userId, clusterId, flowName, flowRunId, appId, entityType, entityId,
        confsToRetrieve, metricsToRetrieve, fieldsToRetrieve);
  }

  /**
   * Uses the {@link ApplicationTable}.
   */
  protected BaseTable<?> getTable() {
    return APPLICATION_TABLE;
  }

  @Override
  protected FilterList constructFilterListBasedOnFields() {
    FilterList list = new FilterList(Operator.MUST_PASS_ONE);
    // Fetch all the columns.
    if (fieldsToRetrieve.contains(Field.ALL) &&
        (confsToRetrieve == null ||
        confsToRetrieve.getFilterList().isEmpty()) &&
        (metricsToRetrieve == null ||
        metricsToRetrieve.getFilterList().isEmpty())) {
      return list;
    }
    FilterList infoColFamilyList = new FilterList();
    // By default fetch everything in INFO column family.
    FamilyFilter infoColumnFamily =
        new FamilyFilter(CompareOp.EQUAL,
           new BinaryComparator(ApplicationColumnFamily.INFO.getBytes()));
    infoColFamilyList.addFilter(infoColumnFamily);
    // Events not required.
    if (!fieldsToRetrieve.contains(Field.EVENTS) &&
        !fieldsToRetrieve.contains(Field.ALL) && eventFilters == null) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
          ApplicationColumnPrefix.EVENT.getColumnPrefixBytes(""))));
    }
    // info not required.
    if (!fieldsToRetrieve.contains(Field.INFO) &&
        !fieldsToRetrieve.contains(Field.ALL) && infoFilters == null) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
          ApplicationColumnPrefix.INFO.getColumnPrefixBytes(""))));
    }
    // is releated to not required.
    if (!fieldsToRetrieve.contains(Field.IS_RELATED_TO) &&
        !fieldsToRetrieve.contains(Field.ALL) && isRelatedTo == null) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
          ApplicationColumnPrefix.IS_RELATED_TO.getColumnPrefixBytes(""))));
    }
    // relates to not required.
    if (!fieldsToRetrieve.contains(Field.RELATES_TO) &&
        !fieldsToRetrieve.contains(Field.ALL) && relatesTo == null) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
          ApplicationColumnPrefix.RELATES_TO.getColumnPrefixBytes(""))));
    }
    list.addFilter(infoColFamilyList);
    if ((fieldsToRetrieve.contains(Field.CONFIGS) || configFilters != null) ||
        (confsToRetrieve != null &&
        !confsToRetrieve.getFilterList().isEmpty())) {
      FilterList filterCfg =
          new FilterList(new FamilyFilter(CompareOp.EQUAL,
          new BinaryComparator(ApplicationColumnFamily.CONFIGS.getBytes())));
      if (confsToRetrieve != null &&
          !confsToRetrieve.getFilterList().isEmpty()) {
        filterCfg.addFilter(TimelineFilterUtils.createHBaseFilterList(
            ApplicationColumnPrefix.CONFIG, confsToRetrieve));
      }
      list.addFilter(filterCfg);
    }
    if ((fieldsToRetrieve.contains(Field.METRICS) || metricFilters != null) ||
        (metricsToRetrieve != null &&
        !metricsToRetrieve.getFilterList().isEmpty())) {
      FilterList filterMetrics =
          new FilterList(new FamilyFilter(CompareOp.EQUAL,
          new BinaryComparator(ApplicationColumnFamily.METRICS.getBytes())));
      if (metricsToRetrieve != null &&
          !metricsToRetrieve.getFilterList().isEmpty()) {
        filterMetrics.addFilter(TimelineFilterUtils.createHBaseFilterList(
            ApplicationColumnPrefix.METRIC, metricsToRetrieve));
      }
      list.addFilter(filterMetrics);
    }
    return list;
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    byte[] rowKey =
        ApplicationRowKey.getRowKey(clusterId, userId, flowName, flowRunId,
            appId);
    Get get = new Get(rowKey);
    get.setMaxVersions(Integer.MAX_VALUE);
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      get.setFilter(filterList);
    }
    return table.getResult(hbaseConf, conn, get);
  }

  @Override
  protected void validateParams() {
    Preconditions.checkNotNull(clusterId, "clusterId shouldn't be null");
    Preconditions.checkNotNull(entityType, "entityType shouldn't be null");
    if (singleEntityRead) {
      Preconditions.checkNotNull(appId, "appId shouldn't be null");
    } else {
      Preconditions.checkNotNull(userId, "userId shouldn't be null");
      Preconditions.checkNotNull(flowName, "flowName shouldn't be null");
    }
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn)
      throws IOException {
    if (singleEntityRead) {
      if (flowName == null || flowRunId == null || userId == null) {
        FlowContext context =
            lookupFlowContext(clusterId, appId, hbaseConf, conn);
        flowName = context.flowName;
        flowRunId = context.flowRunId;
        userId = context.userId;
      }
    }
    if (fieldsToRetrieve == null) {
      fieldsToRetrieve = EnumSet.noneOf(Field.class);
    }
    if (!fieldsToRetrieve.contains(Field.CONFIGS) &&
        confsToRetrieve != null && !confsToRetrieve.getFilterList().isEmpty()) {
      fieldsToRetrieve.add(Field.CONFIGS);
    }
    if (!fieldsToRetrieve.contains(Field.METRICS) &&
        metricsToRetrieve != null &&
        !metricsToRetrieve.getFilterList().isEmpty()) {
      fieldsToRetrieve.add(Field.METRICS);
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
    }
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf,
      Connection conn, FilterList filterList) throws IOException {
    Scan scan = new Scan();
    if (flowRunId != null) {
      scan.setRowPrefixFilter(ApplicationRowKey.
          getRowKeyPrefix(clusterId, userId, flowName, flowRunId));
    } else {
      scan.setRowPrefixFilter(ApplicationRowKey.
          getRowKeyPrefix(clusterId, userId, flowName));
    }
    FilterList newList = new FilterList();
    newList.addFilter(new PageFilter(limit));
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      newList.addFilter(filterList);
    }
    scan.setFilter(newList);
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

    // fetch is related to entities
    boolean checkIsRelatedTo = isRelatedTo != null && isRelatedTo.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.IS_RELATED_TO) || checkIsRelatedTo) {
      readRelationship(entity, result, ApplicationColumnPrefix.IS_RELATED_TO,
          true);
      if (checkIsRelatedTo && !TimelineStorageUtils.matchRelations(
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
      if (checkRelatesTo && !TimelineStorageUtils.matchRelations(
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
          !TimelineStorageUtils.matchFilters(entity.getInfo(), infoFilters)) {
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
      if (checkConfigs && !TimelineStorageUtils.matchFilters(
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
      if (checkEvents && !TimelineStorageUtils.matchEventFilters(
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
      if (checkMetrics && !TimelineStorageUtils.matchMetricFilters(
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
