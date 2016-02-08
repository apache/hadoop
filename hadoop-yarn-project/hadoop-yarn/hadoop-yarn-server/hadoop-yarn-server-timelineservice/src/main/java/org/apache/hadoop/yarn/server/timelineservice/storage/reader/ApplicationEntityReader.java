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
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterUtils;
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

  public ApplicationEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt, entityFilters, toRetrieve, true);
  }

  public ApplicationEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
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
    TimelineDataToRetrieve dataToRetrieve = getDataToRetrieve();
    // Fetch all the columns.
    if (dataToRetrieve.getFieldsToRetrieve().contains(Field.ALL) &&
        (dataToRetrieve.getConfsToRetrieve() == null ||
        dataToRetrieve.getConfsToRetrieve().getFilterList().isEmpty()) &&
        (dataToRetrieve.getMetricsToRetrieve() == null ||
        dataToRetrieve.getMetricsToRetrieve().getFilterList().isEmpty())) {
      return list;
    }
    FilterList infoColFamilyList = new FilterList();
    // By default fetch everything in INFO column family.
    FamilyFilter infoColumnFamily =
        new FamilyFilter(CompareOp.EQUAL,
           new BinaryComparator(ApplicationColumnFamily.INFO.getBytes()));
    infoColFamilyList.addFilter(infoColumnFamily);
    // Events not required.
    TimelineEntityFilters filters = getFilters();
    if (!dataToRetrieve.getFieldsToRetrieve().contains(Field.EVENTS) &&
        !dataToRetrieve.getFieldsToRetrieve().contains(Field.ALL) &&
        (isSingleEntityRead() || filters.getEventFilters() == null)) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
          ApplicationColumnPrefix.EVENT.getColumnPrefixBytes(""))));
    }
    // info not required.
    if (!dataToRetrieve.getFieldsToRetrieve().contains(Field.INFO) &&
        !dataToRetrieve.getFieldsToRetrieve().contains(Field.ALL) &&
        (isSingleEntityRead() || filters.getInfoFilters() == null)) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
          ApplicationColumnPrefix.INFO.getColumnPrefixBytes(""))));
    }
    // is releated to not required.
    if (!dataToRetrieve.getFieldsToRetrieve().contains(Field.IS_RELATED_TO) &&
        !dataToRetrieve.getFieldsToRetrieve().contains(Field.ALL) &&
        (isSingleEntityRead() || filters.getIsRelatedTo() == null)) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
          ApplicationColumnPrefix.IS_RELATED_TO.getColumnPrefixBytes(""))));
    }
    // relates to not required.
    if (!dataToRetrieve.getFieldsToRetrieve().contains(Field.RELATES_TO) &&
        !dataToRetrieve.getFieldsToRetrieve().contains(Field.ALL) &&
        (isSingleEntityRead() || filters.getRelatesTo() == null)) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
          ApplicationColumnPrefix.RELATES_TO.getColumnPrefixBytes(""))));
    }
    list.addFilter(infoColFamilyList);
    if ((dataToRetrieve.getFieldsToRetrieve().contains(Field.CONFIGS) ||
        (!isSingleEntityRead() && filters.getConfigFilters() != null)) ||
        (dataToRetrieve.getConfsToRetrieve() != null &&
        !dataToRetrieve.getConfsToRetrieve().getFilterList().isEmpty())) {
      FilterList filterCfg =
          new FilterList(new FamilyFilter(CompareOp.EQUAL,
          new BinaryComparator(ApplicationColumnFamily.CONFIGS.getBytes())));
      if (dataToRetrieve.getConfsToRetrieve() != null &&
          !dataToRetrieve.getConfsToRetrieve().getFilterList().isEmpty()) {
        filterCfg.addFilter(TimelineFilterUtils.createHBaseFilterList(
            ApplicationColumnPrefix.CONFIG,
            dataToRetrieve.getConfsToRetrieve()));
      }
      list.addFilter(filterCfg);
    }
    if ((dataToRetrieve.getFieldsToRetrieve().contains(Field.METRICS) ||
        (!isSingleEntityRead() && filters.getMetricFilters() != null)) ||
        (dataToRetrieve.getMetricsToRetrieve() != null &&
        !dataToRetrieve.getMetricsToRetrieve().getFilterList().isEmpty())) {
      FilterList filterMetrics =
          new FilterList(new FamilyFilter(CompareOp.EQUAL,
          new BinaryComparator(ApplicationColumnFamily.METRICS.getBytes())));
      if (dataToRetrieve.getMetricsToRetrieve() != null &&
          !dataToRetrieve.getMetricsToRetrieve().getFilterList().isEmpty()) {
        filterMetrics.addFilter(TimelineFilterUtils.createHBaseFilterList(
            ApplicationColumnPrefix.METRIC,
            dataToRetrieve.getMetricsToRetrieve()));
      }
      list.addFilter(filterMetrics);
    }
    return list;
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    TimelineReaderContext context = getContext();
    byte[] rowKey =
        ApplicationRowKey.getRowKey(context.getClusterId(), context.getUserId(),
            context.getFlowName(), context.getFlowRunId(), context.getAppId());
    Get get = new Get(rowKey);
    get.setMaxVersions(Integer.MAX_VALUE);
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      get.setFilter(filterList);
    }
    return getTable().getResult(hbaseConf, conn, get);
  }

  @Override
  protected void validateParams() {
    Preconditions.checkNotNull(getContext().getClusterId(),
        "clusterId shouldn't be null");
    Preconditions.checkNotNull(getContext().getEntityType(),
        "entityType shouldn't be null");
    if (isSingleEntityRead()) {
      Preconditions.checkNotNull(getContext().getAppId(),
          "appId shouldn't be null");
    } else {
      Preconditions.checkNotNull(getContext().getUserId(),
          "userId shouldn't be null");
      Preconditions.checkNotNull(getContext().getFlowName(),
          "flowName shouldn't be null");
    }
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn)
      throws IOException {
    TimelineReaderContext context = getContext();
    if (isSingleEntityRead()) {
      if (context.getFlowName() == null || context.getFlowRunId() == null ||
          context.getUserId() == null) {
        FlowContext flowContext = lookupFlowContext(
            context.getClusterId(), context.getAppId(), hbaseConf, conn);
        context.setFlowName(flowContext.getFlowName());
        context.setFlowRunId(flowContext.getFlowRunId());
        context.setUserId(flowContext.getUserId());
      }
    }
    getDataToRetrieve().addFieldsBasedOnConfsAndMetricsToRetrieve();
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf,
      Connection conn, FilterList filterList) throws IOException {
    Scan scan = new Scan();
    TimelineReaderContext context = getContext();
    if (context.getFlowRunId() != null) {
      scan.setRowPrefixFilter(ApplicationRowKey.
          getRowKeyPrefix(context.getClusterId(), context.getUserId(),
              context.getFlowName(), context.getFlowRunId()));
    } else {
      scan.setRowPrefixFilter(ApplicationRowKey.
          getRowKeyPrefix(context.getClusterId(), context.getUserId(),
              context.getFlowName()));
    }
    FilterList newList = new FilterList();
    newList.addFilter(new PageFilter(getFilters().getLimit()));
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      newList.addFilter(filterList);
    }
    scan.setFilter(newList);
    return getTable().getResultScanner(hbaseConf, conn, scan);
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

    TimelineEntityFilters filters = getFilters();
    // fetch created time
    Number createdTime =
        (Number)ApplicationColumn.CREATED_TIME.readResult(result);
    entity.setCreatedTime(createdTime.longValue());
    if (!isSingleEntityRead() &&
        (entity.getCreatedTime() < filters.getCreatedTimeBegin() ||
        entity.getCreatedTime() > filters.getCreatedTimeEnd())) {
      return null;
    }
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // fetch is related to entities
    boolean checkIsRelatedTo =
        filters != null && filters.getIsRelatedTo() != null &&
        filters.getIsRelatedTo().size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.IS_RELATED_TO) || checkIsRelatedTo) {
      readRelationship(entity, result, ApplicationColumnPrefix.IS_RELATED_TO,
          true);
      if (checkIsRelatedTo && !TimelineStorageUtils.matchRelations(
          entity.getIsRelatedToEntities(), filters.getIsRelatedTo())) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.IS_RELATED_TO)) {
        entity.getIsRelatedToEntities().clear();
      }
    }

    // fetch relates to entities
    boolean checkRelatesTo =
        filters != null && filters.getRelatesTo() != null &&
        filters.getRelatesTo().size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.RELATES_TO) || checkRelatesTo) {
      readRelationship(entity, result, ApplicationColumnPrefix.RELATES_TO,
          false);
      if (checkRelatesTo && !TimelineStorageUtils.matchRelations(
          entity.getRelatesToEntities(), filters.getRelatesTo())) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.RELATES_TO)) {
        entity.getRelatesToEntities().clear();
      }
    }

    // fetch info
    boolean checkInfo = filters != null && filters.getInfoFilters() != null &&
        filters.getInfoFilters().size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.INFO) || checkInfo) {
      readKeyValuePairs(entity, result, ApplicationColumnPrefix.INFO, false);
      if (checkInfo &&
          !TimelineStorageUtils.matchFilters(
          entity.getInfo(), filters.getInfoFilters())) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.INFO)) {
        entity.getInfo().clear();
      }
    }

    // fetch configs
    boolean checkConfigs =
        filters != null && filters.getConfigFilters() != null &&
        filters.getConfigFilters().size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.CONFIGS) || checkConfigs) {
      readKeyValuePairs(entity, result, ApplicationColumnPrefix.CONFIG, true);
      if (checkConfigs && !TimelineStorageUtils.matchFilters(
          entity.getConfigs(), filters.getConfigFilters())) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.CONFIGS)) {
        entity.getConfigs().clear();
      }
    }

    // fetch events
    boolean checkEvents =
        filters != null && filters.getEventFilters() != null &&
        filters.getEventFilters().size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.EVENTS) || checkEvents) {
      readEvents(entity, result, true);
      if (checkEvents && !TimelineStorageUtils.matchEventFilters(
          entity.getEvents(), filters.getEventFilters())) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.EVENTS)) {
        entity.getEvents().clear();
      }
    }

    // fetch metrics
    boolean checkMetrics =
        filters != null && filters.getMetricFilters() != null &&
        filters.getMetricFilters().size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.METRICS) || checkMetrics) {
      readMetrics(entity, result, ApplicationColumnPrefix.METRIC);
      if (checkMetrics && !TimelineStorageUtils.matchMetricFilters(
          entity.getMetrics(), filters.getMetricFilters())) {
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
