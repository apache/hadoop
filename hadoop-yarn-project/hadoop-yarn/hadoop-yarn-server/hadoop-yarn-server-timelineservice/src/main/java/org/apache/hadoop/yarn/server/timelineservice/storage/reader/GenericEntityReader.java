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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.common.base.Preconditions;

/**
 * Timeline entity reader for generic entities that are stored in the entity
 * table.
 */
class GenericEntityReader extends TimelineEntityReader {
  private static final EntityTable ENTITY_TABLE = new EntityTable();
  private static final Log LOG = LogFactory.getLog(GenericEntityReader.class);

  /**
   * Used to look up the flow context.
   */
  private final AppToFlowTable appToFlowTable = new AppToFlowTable();

  public GenericEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve,
      boolean sortedKeys) {
    super(ctxt, entityFilters, toRetrieve, sortedKeys);
  }

  public GenericEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
  }

  /**
   * Uses the {@link EntityTable}.
   */
  protected BaseTable<?> getTable() {
    return ENTITY_TABLE;
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
           new BinaryComparator(EntityColumnFamily.INFO.getBytes()));
    infoColFamilyList.addFilter(infoColumnFamily);
    TimelineEntityFilters filters = getFilters();
    // Events not required.
    if (!dataToRetrieve.getFieldsToRetrieve().contains(Field.EVENTS) &&
        !dataToRetrieve.getFieldsToRetrieve().contains(Field.ALL) &&
        (singleEntityRead || filters.getEventFilters() == null)) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
          EntityColumnPrefix.EVENT.getColumnPrefixBytes(""))));
    }
    // info not required.
    if (!dataToRetrieve.getFieldsToRetrieve().contains(Field.INFO) &&
        !dataToRetrieve.getFieldsToRetrieve().contains(Field.ALL) &&
        (singleEntityRead || filters.getInfoFilters() == null)) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
              EntityColumnPrefix.INFO.getColumnPrefixBytes(""))));
    }
    // is related to not required.
    if (!dataToRetrieve.getFieldsToRetrieve().contains(Field.IS_RELATED_TO) &&
        !dataToRetrieve.getFieldsToRetrieve().contains(Field.ALL) &&
        (singleEntityRead || filters.getIsRelatedTo() == null)) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
              EntityColumnPrefix.IS_RELATED_TO.getColumnPrefixBytes(""))));
    }
    // relates to not required.
    if (!dataToRetrieve.getFieldsToRetrieve().contains(Field.RELATES_TO) &&
        !dataToRetrieve.getFieldsToRetrieve().contains(Field.ALL) &&
        (singleEntityRead || filters.getRelatesTo() == null)) {
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
              EntityColumnPrefix.RELATES_TO.getColumnPrefixBytes(""))));
    }
    list.addFilter(infoColFamilyList);
    if ((dataToRetrieve.getFieldsToRetrieve().contains(Field.CONFIGS) ||
        (!singleEntityRead && filters.getConfigFilters() != null)) ||
        (dataToRetrieve.getConfsToRetrieve() != null &&
        !dataToRetrieve.getConfsToRetrieve().getFilterList().isEmpty())) {
      FilterList filterCfg =
          new FilterList(new FamilyFilter(CompareOp.EQUAL,
              new BinaryComparator(EntityColumnFamily.CONFIGS.getBytes())));
      if (dataToRetrieve.getConfsToRetrieve() != null &&
          !dataToRetrieve.getConfsToRetrieve().getFilterList().isEmpty()) {
        filterCfg.addFilter(TimelineFilterUtils.createHBaseFilterList(
            EntityColumnPrefix.CONFIG, dataToRetrieve.getConfsToRetrieve()));
      }
      list.addFilter(filterCfg);
    }
    if ((dataToRetrieve.getFieldsToRetrieve().contains(Field.METRICS) ||
        (!singleEntityRead && filters.getMetricFilters() != null)) ||
        (dataToRetrieve.getMetricsToRetrieve() != null &&
        !dataToRetrieve.getMetricsToRetrieve().getFilterList().isEmpty())) {
      FilterList filterMetrics =
          new FilterList(new FamilyFilter(CompareOp.EQUAL,
              new BinaryComparator(EntityColumnFamily.METRICS.getBytes())));
      if (dataToRetrieve.getMetricsToRetrieve() != null &&
          !dataToRetrieve.getMetricsToRetrieve().getFilterList().isEmpty()) {
        filterMetrics.addFilter(TimelineFilterUtils.createHBaseFilterList(
            EntityColumnPrefix.METRIC, dataToRetrieve.getMetricsToRetrieve()));
      }
      list.addFilter(filterMetrics);
    }
    return list;
  }

  protected FlowContext lookupFlowContext(String clusterId, String appId,
      Configuration hbaseConf, Connection conn) throws IOException {
    byte[] rowKey = AppToFlowRowKey.getRowKey(clusterId, appId);
    Get get = new Get(rowKey);
    Result result = appToFlowTable.getResult(hbaseConf, conn, get);
    if (result != null && !result.isEmpty()) {
      return new FlowContext(
          AppToFlowColumn.USER_ID.readResult(result).toString(),
          AppToFlowColumn.FLOW_ID.readResult(result).toString(),
          ((Number)AppToFlowColumn.FLOW_RUN_ID.readResult(result)).longValue());
    } else {
      throw new NotFoundException(
          "Unable to find the context flow ID and flow run ID for clusterId=" +
          clusterId + ", appId=" + appId);
    }
  }

  protected static class FlowContext {
    protected final String userId;
    protected final String flowName;
    protected final Long flowRunId;
    public FlowContext(String user, String flowName, Long flowRunId) {
      this.userId = user;
      this.flowName = flowName;
      this.flowRunId = flowRunId;
    }
  }

  @Override
  protected void validateParams() {
    Preconditions.checkNotNull(getContext().getClusterId(),
        "clusterId shouldn't be null");
    Preconditions.checkNotNull(getContext().getAppId(),
        "appId shouldn't be null");
    Preconditions.checkNotNull(getContext().getEntityType(),
        "entityType shouldn't be null");
    if (singleEntityRead) {
      Preconditions.checkNotNull(getContext().getEntityId(),
          "entityId shouldn't be null");
    }
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn)
      throws IOException {
    TimelineReaderContext context = getContext();
    // In reality all three should be null or neither should be null
    if (context.getFlowName() == null || context.getFlowRunId() == null ||
        context.getUserId() == null) {
      FlowContext flowContext = lookupFlowContext(
          context.getClusterId(), context.getAppId(), hbaseConf, conn);
      context.setFlowName(flowContext.flowName);
      context.setFlowRunId(flowContext.flowRunId);
      context.setUserId(flowContext.userId);
    }
    getDataToRetrieve().addFieldsBasedOnConfsAndMetricsToRetrieve();
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    TimelineReaderContext context = getContext();
    byte[] rowKey =
        EntityRowKey.getRowKey(context.getClusterId(), context.getUserId(),
            context.getFlowName(), context.getFlowRunId(), context.getAppId(),
            context.getEntityType(), context.getEntityId());
    Get get = new Get(rowKey);
    get.setMaxVersions(Integer.MAX_VALUE);
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      get.setFilter(filterList);
    }
    return table.getResult(hbaseConf, conn, get);
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf,
      Connection conn, FilterList filterList) throws IOException {
    // Scan through part of the table to find the entities belong to one app
    // and one type
    Scan scan = new Scan();
    TimelineReaderContext context = getContext();
    scan.setRowPrefixFilter(EntityRowKey.getRowKeyPrefix(
        context.getClusterId(), context.getUserId(), context.getFlowName(),
        context.getFlowRunId(), context.getAppId(), context.getEntityType()));
    scan.setMaxVersions(Integer.MAX_VALUE);
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      scan.setFilter(filterList);
    }
    return table.getResultScanner(hbaseConf, conn, scan);
  }

  @Override
  protected TimelineEntity parseEntity(Result result) throws IOException {
    if (result == null || result.isEmpty()) {
      return null;
    }
    TimelineEntity entity = new TimelineEntity();
    String entityType = EntityColumn.TYPE.readResult(result).toString();
    entity.setType(entityType);
    String entityId = EntityColumn.ID.readResult(result).toString();
    entity.setId(entityId);

    TimelineEntityFilters filters = getFilters();
    // fetch created time
    Number createdTime = (Number)EntityColumn.CREATED_TIME.readResult(result);
    entity.setCreatedTime(createdTime.longValue());
    if (!singleEntityRead &&
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
      readRelationship(entity, result, EntityColumnPrefix.IS_RELATED_TO, true);
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
      readRelationship(entity, result, EntityColumnPrefix.RELATES_TO, false);
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
      readKeyValuePairs(entity, result, EntityColumnPrefix.INFO, false);
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
      readKeyValuePairs(entity, result, EntityColumnPrefix.CONFIG, true);
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
      readEvents(entity, result, false);
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
      readMetrics(entity, result, EntityColumnPrefix.METRIC);
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

  /**
   * Helper method for reading relationship.
   */
  protected <T> void readRelationship(
      TimelineEntity entity, Result result, ColumnPrefix<T> prefix,
      boolean isRelatedTo) throws IOException {
    // isRelatedTo and relatesTo are of type Map<String, Set<String>>
    Map<String, Object> columns = prefix.readResults(result);
    for (Map.Entry<String, Object> column : columns.entrySet()) {
      for (String id : Separator.VALUES.splitEncoded(
          column.getValue().toString())) {
        if (isRelatedTo) {
          entity.addIsRelatedToEntity(column.getKey(), id);
        } else {
          entity.addRelatesToEntity(column.getKey(), id);
        }
      }
    }
  }

  /**
   * Helper method for reading key-value pairs for either info or config.
   */
  protected <T> void readKeyValuePairs(
      TimelineEntity entity, Result result, ColumnPrefix<T> prefix,
      boolean isConfig) throws IOException {
    // info and configuration are of type Map<String, Object or String>
    Map<String, Object> columns = prefix.readResults(result);
    if (isConfig) {
      for (Map.Entry<String, Object> column : columns.entrySet()) {
        entity.addConfig(column.getKey(), column.getValue().toString());
      }
    } else {
      entity.addInfo(columns);
    }
  }

  /**
   * Read events from the entity table or the application table. The column name
   * is of the form "eventId=timestamp=infoKey" where "infoKey" may be omitted
   * if there is no info associated with the event.
   *
   * See {@link EntityTable} and {@link ApplicationTable} for a more detailed
   * schema description.
   */
  protected void readEvents(TimelineEntity entity, Result result,
      boolean isApplication) throws IOException {
    Map<String, TimelineEvent> eventsMap = new HashMap<>();
    Map<?, Object> eventsResult = isApplication ?
        ApplicationColumnPrefix.EVENT.
            readResultsHavingCompoundColumnQualifiers(result) :
        EntityColumnPrefix.EVENT.
            readResultsHavingCompoundColumnQualifiers(result);
    for (Map.Entry<?, Object> eventResult : eventsResult.entrySet()) {
      byte[][] karr = (byte[][])eventResult.getKey();
      // the column name is of the form "eventId=timestamp=infoKey"
      if (karr.length == 3) {
        String id = Bytes.toString(karr[0]);
        long ts = TimelineStorageUtils.invertLong(Bytes.toLong(karr[1]));
        String key = Separator.VALUES.joinEncoded(id, Long.toString(ts));
        TimelineEvent event = eventsMap.get(key);
        if (event == null) {
          event = new TimelineEvent();
          event.setId(id);
          event.setTimestamp(ts);
          eventsMap.put(key, event);
        }
        // handle empty info
        String infoKey = karr[2].length == 0 ? null : Bytes.toString(karr[2]);
        if (infoKey != null) {
          event.addInfo(infoKey, eventResult.getValue());
        }
      } else {
        LOG.warn("incorrectly formatted column name: it will be discarded");
        continue;
      }
    }
    Set<TimelineEvent> eventsSet = new HashSet<>(eventsMap.values());
    entity.addEvents(eventsSet);
  }
}
