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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
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
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;

import com.google.common.base.Preconditions;

/**
 * Timeline entity reader for application entities that are stored in the
 * application table.
 */
class ApplicationEntityReader extends GenericEntityReader {
  private static final ApplicationTableRW APPLICATION_TABLE =
      new ApplicationTableRW();

  ApplicationEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt, entityFilters, toRetrieve);
  }

  ApplicationEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
  }

  /**
   * Uses the {@link ApplicationTableRW}.
   */
  protected BaseTableRW<?> getTable() {
    return APPLICATION_TABLE;
  }

  /**
   * This method is called only for multiple entity reads.
   */
  @Override
  protected FilterList constructFilterListBasedOnFilters() throws IOException {
    // Filters here cannot be null for multiple entity reads as they are set in
    // augmentParams if null.
    TimelineEntityFilters filters = getFilters();
    FilterList listBasedOnFilters = new FilterList();
    // Create filter list based on created time range and add it to
    // listBasedOnFilters.
    long createdTimeBegin = filters.getCreatedTimeBegin();
    long createdTimeEnd = filters.getCreatedTimeEnd();
    if (createdTimeBegin != 0 || createdTimeEnd != Long.MAX_VALUE) {
      listBasedOnFilters.addFilter(
          TimelineFilterUtils.createSingleColValueFiltersByRange(
          ApplicationColumn.CREATED_TIME, createdTimeBegin, createdTimeEnd));
    }
    // Create filter list based on metric filters and add it to
    // listBasedOnFilters.
    TimelineFilterList metricFilters = filters.getMetricFilters();
    if (metricFilters != null && !metricFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(
          TimelineFilterUtils.createHBaseFilterList(
              ApplicationColumnPrefix.METRIC, metricFilters));
    }
    // Create filter list based on config filters and add it to
    // listBasedOnFilters.
    TimelineFilterList configFilters = filters.getConfigFilters();
    if (configFilters != null && !configFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(
          TimelineFilterUtils.createHBaseFilterList(
              ApplicationColumnPrefix.CONFIG, configFilters));
    }
    // Create filter list based on info filters and add it to listBasedOnFilters
    TimelineFilterList infoFilters = filters.getInfoFilters();
    if (infoFilters != null && !infoFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(
          TimelineFilterUtils.createHBaseFilterList(
              ApplicationColumnPrefix.INFO, infoFilters));
    }
    return listBasedOnFilters;
  }

  /**
   * Add {@link QualifierFilter} filters to filter list for each column of
   * application table.
   *
   * @param list filter list to which qualifier filters have to be added.
   */
  @Override
  protected void updateFixedColumns(FilterList list) {
    for (ApplicationColumn column : ApplicationColumn.values()) {
      list.addFilter(new QualifierFilter(CompareOp.EQUAL,
          new BinaryComparator(column.getColumnQualifierBytes())));
    }
  }

  /**
   * Creates a filter list which indicates that only some of the column
   * qualifiers in the info column family will be returned in result.
   *
   * @return filter list.
   * @throws IOException if any problem occurs while creating filter list.
   */
  private FilterList createFilterListForColsOfInfoFamily()
      throws IOException {
    FilterList infoFamilyColsFilter = new FilterList(Operator.MUST_PASS_ONE);
    // Add filters for each column in entity table.
    updateFixedColumns(infoFamilyColsFilter);
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // If INFO field has to be retrieved, add a filter for fetching columns
    // with INFO column prefix.
    if (hasField(fieldsToRetrieve, Field.INFO)) {
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.EQUAL, ApplicationColumnPrefix.INFO));
    }
    TimelineFilterList relatesTo = getFilters().getRelatesTo();
    if (hasField(fieldsToRetrieve, Field.RELATES_TO)) {
      // If RELATES_TO field has to be retrieved, add a filter for fetching
      // columns with RELATES_TO column prefix.
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.EQUAL, ApplicationColumnPrefix.RELATES_TO));
    } else if (relatesTo != null && !relatesTo.getFilterList().isEmpty()) {
      // Even if fields to retrieve does not contain RELATES_TO, we still
      // need to have a filter to fetch some of the column qualifiers if
      // relatesTo filters are specified. relatesTo filters will then be
      // matched after fetching rows from HBase.
      Set<String> relatesToCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(relatesTo);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          ApplicationColumnPrefix.RELATES_TO, relatesToCols));
    }
    TimelineFilterList isRelatedTo = getFilters().getIsRelatedTo();
    if (hasField(fieldsToRetrieve, Field.IS_RELATED_TO)) {
      // If IS_RELATED_TO field has to be retrieved, add a filter for fetching
      // columns with IS_RELATED_TO column prefix.
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.EQUAL, ApplicationColumnPrefix.IS_RELATED_TO));
    } else if (isRelatedTo != null && !isRelatedTo.getFilterList().isEmpty()) {
      // Even if fields to retrieve does not contain IS_RELATED_TO, we still
      // need to have a filter to fetch some of the column qualifiers if
      // isRelatedTo filters are specified. isRelatedTo filters will then be
      // matched after fetching rows from HBase.
      Set<String> isRelatedToCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(isRelatedTo);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          ApplicationColumnPrefix.IS_RELATED_TO, isRelatedToCols));
    }
    TimelineFilterList eventFilters = getFilters().getEventFilters();
    if (hasField(fieldsToRetrieve, Field.EVENTS)) {
      // If EVENTS field has to be retrieved, add a filter for fetching columns
      // with EVENT column prefix.
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.EQUAL, ApplicationColumnPrefix.EVENT));
    } else if (eventFilters != null && !eventFilters.getFilterList().isEmpty()){
      // Even if fields to retrieve does not contain EVENTS, we still need to
      // have a filter to fetch some of the column qualifiers on the basis of
      // event filters specified. Event filters will then be matched after
      // fetching rows from HBase.
      Set<String> eventCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(eventFilters);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          ApplicationColumnPrefix.EVENT, eventCols));
    }
    return infoFamilyColsFilter;
  }

  /**
   * Exclude column prefixes via filters which are not required(based on fields
   * to retrieve) from info column family. These filters are added to filter
   * list which contains a filter for getting info column family.
   *
   * @param infoColFamilyList filter list for info column family.
   */
  private void excludeFieldsFromInfoColFamily(FilterList infoColFamilyList) {
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // Events not required.
    if (!hasField(fieldsToRetrieve, Field.EVENTS)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.NOT_EQUAL, ApplicationColumnPrefix.EVENT));
    }
    // info not required.
    if (!hasField(fieldsToRetrieve, Field.INFO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.NOT_EQUAL, ApplicationColumnPrefix.INFO));
    }
    // is related to not required.
    if (!hasField(fieldsToRetrieve, Field.IS_RELATED_TO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.NOT_EQUAL, ApplicationColumnPrefix.IS_RELATED_TO));
    }
    // relates to not required.
    if (!hasField(fieldsToRetrieve, Field.RELATES_TO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.NOT_EQUAL, ApplicationColumnPrefix.RELATES_TO));
    }
  }

  /**
   * Updates filter list based on fields for confs and metrics to retrieve.
   *
   * @param listBasedOnFields filter list based on fields.
   * @throws IOException if any problem occurs while updating filter list.
   */
  private void updateFilterForConfsAndMetricsToRetrieve(
      FilterList listBasedOnFields) throws IOException {
    TimelineDataToRetrieve dataToRetrieve = getDataToRetrieve();
    // Please note that if confsToRetrieve is specified, we would have added
    // CONFS to fields to retrieve in augmentParams() even if not specified.
    if (dataToRetrieve.getFieldsToRetrieve().contains(Field.CONFIGS)) {
      // Create a filter list for configs.
      listBasedOnFields.addFilter(TimelineFilterUtils.
          createFilterForConfsOrMetricsToRetrieve(
              dataToRetrieve.getConfsToRetrieve(),
              ApplicationColumnFamily.CONFIGS, ApplicationColumnPrefix.CONFIG));
    }

    // Please note that if metricsToRetrieve is specified, we would have added
    // METRICS to fields to retrieve in augmentParams() even if not specified.
    if (dataToRetrieve.getFieldsToRetrieve().contains(Field.METRICS)) {
      // Create a filter list for metrics.
      listBasedOnFields.addFilter(TimelineFilterUtils.
          createFilterForConfsOrMetricsToRetrieve(
              dataToRetrieve.getMetricsToRetrieve(),
              ApplicationColumnFamily.METRICS, ApplicationColumnPrefix.METRIC));
    }
  }

  @Override
  protected FilterList constructFilterListBasedOnFields() throws IOException {
    if (!needCreateFilterListBasedOnFields()) {
      // Fetch all the columns. No need of a filter.
      return null;
    }
    FilterList listBasedOnFields = new FilterList(Operator.MUST_PASS_ONE);
    FilterList infoColFamilyList = new FilterList();
    // By default fetch everything in INFO column family.
    FamilyFilter infoColumnFamily =
        new FamilyFilter(CompareOp.EQUAL,
            new BinaryComparator(ApplicationColumnFamily.INFO.getBytes()));
    infoColFamilyList.addFilter(infoColumnFamily);
    if (!isSingleEntityRead() && fetchPartialColsFromInfoFamily()) {
      // We can fetch only some of the columns from info family.
      infoColFamilyList.addFilter(createFilterListForColsOfInfoFamily());
    } else {
      // Exclude column prefixes in info column family which are not required
      // based on fields to retrieve.
      excludeFieldsFromInfoColFamily(infoColFamilyList);
    }
    listBasedOnFields.addFilter(infoColFamilyList);

    updateFilterForConfsAndMetricsToRetrieve(listBasedOnFields);
    return listBasedOnFields;
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    TimelineReaderContext context = getContext();
    ApplicationRowKey applicationRowKey =
        new ApplicationRowKey(context.getClusterId(), context.getUserId(),
            context.getFlowName(), context.getFlowRunId(), context.getAppId());
    byte[] rowKey = applicationRowKey.getRowKey();
    Get get = new Get(rowKey);
    // Set time range for metric values.
    setMetricsTimeRange(get);
    get.setMaxVersions(getDataToRetrieve().getMetricsLimit());
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      get.setFilter(filterList);
    }
    return getTable().getResult(hbaseConf, conn, get);
  }

  @Override
  protected void validateParams() {
    Preconditions.checkNotNull(getContext(), "context shouldn't be null");
    Preconditions.checkNotNull(
        getDataToRetrieve(), "data to retrieve shouldn't be null");
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
    if (isSingleEntityRead()) {
      // Get flow context information from AppToFlow table.
      defaultAugmentParams(hbaseConf, conn);
    }
    // Add configs/metrics to fields to retrieve if confsToRetrieve and/or
    // metricsToRetrieve are specified.
    getDataToRetrieve().addFieldsBasedOnConfsAndMetricsToRetrieve();
    if (!isSingleEntityRead()) {
      createFiltersIfNull();
    }
  }

  private void setMetricsTimeRange(Query query) {
    // Set time range for metric values.
    HBaseTimelineStorageUtils.setMetricsTimeRange(
        query, ApplicationColumnFamily.METRICS.getBytes(),
        getDataToRetrieve().getMetricsTimeBegin(),
        getDataToRetrieve().getMetricsTimeEnd());
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf,
      Connection conn, FilterList filterList) throws IOException {
    Scan scan = new Scan();
    TimelineReaderContext context = getContext();
    RowKeyPrefix<ApplicationRowKey> applicationRowKeyPrefix = null;

    // Whether or not flowRunID is null doesn't matter, the
    // ApplicationRowKeyPrefix will do the right thing.
    // default mode, will always scans from beginning of entity type.
    if (getFilters().getFromId() == null) {
      applicationRowKeyPrefix = new ApplicationRowKeyPrefix(
          context.getClusterId(), context.getUserId(), context.getFlowName(),
          context.getFlowRunId());
      scan.setRowPrefixFilter(applicationRowKeyPrefix.getRowKeyPrefix());
    } else {
      ApplicationRowKey applicationRowKey = null;
      try {
        applicationRowKey =
            ApplicationRowKey.parseRowKeyFromString(getFilters().getFromId());
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Invalid filter fromid is provided.");
      }
      if (!context.getClusterId().equals(applicationRowKey.getClusterId())) {
        throw new BadRequestException(
            "fromid doesn't belong to clusterId=" + context.getClusterId());
      }

      // set start row
      scan.setStartRow(applicationRowKey.getRowKey());

      // get the bytes for stop row
      applicationRowKeyPrefix = new ApplicationRowKeyPrefix(
          context.getClusterId(), context.getUserId(), context.getFlowName(),
          context.getFlowRunId());

      // set stop row
      scan.setStopRow(
          HBaseTimelineStorageUtils.calculateTheClosestNextRowKeyForPrefix(
              applicationRowKeyPrefix.getRowKeyPrefix()));
    }

    FilterList newList = new FilterList();
    newList.addFilter(new PageFilter(getFilters().getLimit()));
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      newList.addFilter(filterList);
    }
    scan.setFilter(newList);

    // Set time range for metric values.
    setMetricsTimeRange(scan);
    scan.setMaxVersions(getDataToRetrieve().getMetricsLimit());
    return getTable().getResultScanner(hbaseConf, conn, scan);
  }

  @Override
  protected TimelineEntity parseEntity(Result result) throws IOException {
    if (result == null || result.isEmpty()) {
      return null;
    }
    TimelineEntity entity = new TimelineEntity();
    entity.setType(TimelineEntityType.YARN_APPLICATION.toString());
    String entityId =
        ColumnRWHelper.readResult(result, ApplicationColumn.ID).toString();
    entity.setId(entityId);

    TimelineEntityFilters filters = getFilters();
    // fetch created time
    Long createdTime = (Long) ColumnRWHelper.readResult(result,
        ApplicationColumn.CREATED_TIME);
    entity.setCreatedTime(createdTime);

    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // fetch is related to entities and match isRelatedTo filter. If isRelatedTo
    // filters do not match, entity would be dropped. We have to match filters
    // locally as relevant HBase filters to filter out rows on the basis of
    // isRelatedTo are not set in HBase scan.
    boolean checkIsRelatedTo =
        !isSingleEntityRead() && filters.getIsRelatedTo() != null &&
        filters.getIsRelatedTo().getFilterList().size() > 0;
    if (hasField(fieldsToRetrieve, Field.IS_RELATED_TO) || checkIsRelatedTo) {
      readRelationship(entity, result, ApplicationColumnPrefix.IS_RELATED_TO,
          true);
      if (checkIsRelatedTo && !TimelineStorageUtils.matchIsRelatedTo(entity,
          filters.getIsRelatedTo())) {
        return null;
      }
      if (!hasField(fieldsToRetrieve,
          Field.IS_RELATED_TO)) {
        entity.getIsRelatedToEntities().clear();
      }
    }

    // fetch relates to entities and match relatesTo filter. If relatesTo
    // filters do not match, entity would be dropped. We have to match filters
    // locally as relevant HBase filters to filter out rows on the basis of
    // relatesTo are not set in HBase scan.
    boolean checkRelatesTo =
        !isSingleEntityRead() && filters.getRelatesTo() != null &&
        filters.getRelatesTo().getFilterList().size() > 0;
    if (hasField(fieldsToRetrieve, Field.RELATES_TO) ||
        checkRelatesTo) {
      readRelationship(entity, result, ApplicationColumnPrefix.RELATES_TO,
          false);
      if (checkRelatesTo && !TimelineStorageUtils.matchRelatesTo(entity,
          filters.getRelatesTo())) {
        return null;
      }
      if (!hasField(fieldsToRetrieve, Field.RELATES_TO)) {
        entity.getRelatesToEntities().clear();
      }
    }

    // fetch info if fieldsToRetrieve contains INFO or ALL.
    if (hasField(fieldsToRetrieve, Field.INFO)) {
      readKeyValuePairs(entity, result, ApplicationColumnPrefix.INFO, false);
    }

    // fetch configs if fieldsToRetrieve contains CONFIGS or ALL.
    if (hasField(fieldsToRetrieve, Field.CONFIGS)) {
      readKeyValuePairs(entity, result, ApplicationColumnPrefix.CONFIG, true);
    }

    // fetch events and match event filters if they exist. If event filters do
    // not match, entity would be dropped. We have to match filters locally
    // as relevant HBase filters to filter out rows on the basis of events
    // are not set in HBase scan.
    boolean checkEvents =
        !isSingleEntityRead() && filters.getEventFilters() != null &&
        filters.getEventFilters().getFilterList().size() > 0;
    if (hasField(fieldsToRetrieve, Field.EVENTS) || checkEvents) {
      readEvents(entity, result, ApplicationColumnPrefix.EVENT);
      if (checkEvents && !TimelineStorageUtils.matchEventFilters(entity,
          filters.getEventFilters())) {
        return null;
      }
      if (!hasField(fieldsToRetrieve, Field.EVENTS)) {
        entity.getEvents().clear();
      }
    }

    // fetch metrics if fieldsToRetrieve contains METRICS or ALL.
    if (hasField(fieldsToRetrieve, Field.METRICS)) {
      readMetrics(entity, result, ApplicationColumnPrefix.METRIC);
    }

    ApplicationRowKey rowKey = ApplicationRowKey.parseRowKey(result.getRow());
    entity.getInfo().put(TimelineReaderUtils.FROMID_KEY,
        rowKey.getRowKeyAsString());
    return entity;
  }
}
