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
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationTable;
import org.apache.hadoop.yarn.webapp.BadRequestException;

import com.google.common.base.Preconditions;

class SubApplicationEntityReader extends GenericEntityReader {
  private static final SubApplicationTable SUB_APPLICATION_TABLE =
      new SubApplicationTable();

  SubApplicationEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt, entityFilters, toRetrieve);
  }

  SubApplicationEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
  }

  /**
   * Uses the {@link SubApplicationTable}.
   */
  protected BaseTable<?> getTable() {
    return SUB_APPLICATION_TABLE;
  }

  @Override
  protected FilterList constructFilterListBasedOnFilters() throws IOException {
    // Filters here cannot be null for multiple entity reads as they are set in
    // augmentParams if null.
    FilterList listBasedOnFilters = new FilterList();
    TimelineEntityFilters filters = getFilters();
    // Create filter list based on created time range and add it to
    // listBasedOnFilters.
    long createdTimeBegin = filters.getCreatedTimeBegin();
    long createdTimeEnd = filters.getCreatedTimeEnd();
    if (createdTimeBegin != 0 || createdTimeEnd != Long.MAX_VALUE) {
      listBasedOnFilters.addFilter(TimelineFilterUtils
          .createSingleColValueFiltersByRange(SubApplicationColumn.CREATED_TIME,
              createdTimeBegin, createdTimeEnd));
    }
    // Create filter list based on metric filters and add it to
    // listBasedOnFilters.
    TimelineFilterList metricFilters = filters.getMetricFilters();
    if (metricFilters != null && !metricFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(TimelineFilterUtils.createHBaseFilterList(
          SubApplicationColumnPrefix.METRIC, metricFilters));
    }
    // Create filter list based on config filters and add it to
    // listBasedOnFilters.
    TimelineFilterList configFilters = filters.getConfigFilters();
    if (configFilters != null && !configFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(TimelineFilterUtils.createHBaseFilterList(
          SubApplicationColumnPrefix.CONFIG, configFilters));
    }
    // Create filter list based on info filters and add it to listBasedOnFilters
    TimelineFilterList infoFilters = filters.getInfoFilters();
    if (infoFilters != null && !infoFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(TimelineFilterUtils
          .createHBaseFilterList(SubApplicationColumnPrefix.INFO, infoFilters));
    }
    return listBasedOnFilters;
  }

  /**
   * Add {@link QualifierFilter} filters to filter list for each column of
   * entity table.
   *
   * @param list filter list to which qualifier filters have to be added.
   */
  protected void updateFixedColumns(FilterList list) {
    for (SubApplicationColumn column : SubApplicationColumn.values()) {
      list.addFilter(new QualifierFilter(CompareOp.EQUAL,
          new BinaryComparator(column.getColumnQualifierBytes())));
    }
  }

  /**
   * Creates a filter list which indicates that only some of the column
   * qualifiers in the info column family will be returned in result.
   *
   * @param isApplication If true, it means operations are to be performed for
   *          application table, otherwise for entity table.
   * @return filter list.
   * @throws IOException if any problem occurs while creating filter list.
   */
  private FilterList createFilterListForColsOfInfoFamily() throws IOException {
    FilterList infoFamilyColsFilter = new FilterList(Operator.MUST_PASS_ONE);
    // Add filters for each column in entity table.
    updateFixedColumns(infoFamilyColsFilter);
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // If INFO field has to be retrieved, add a filter for fetching columns
    // with INFO column prefix.
    if (hasField(fieldsToRetrieve, Field.INFO)) {
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.EQUAL,
              SubApplicationColumnPrefix.INFO));
    }
    TimelineFilterList relatesTo = getFilters().getRelatesTo();
    if (hasField(fieldsToRetrieve, Field.RELATES_TO)) {
      // If RELATES_TO field has to be retrieved, add a filter for fetching
      // columns with RELATES_TO column prefix.
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.EQUAL,
              SubApplicationColumnPrefix.RELATES_TO));
    } else if (relatesTo != null && !relatesTo.getFilterList().isEmpty()) {
      // Even if fields to retrieve does not contain RELATES_TO, we still
      // need to have a filter to fetch some of the column qualifiers if
      // relatesTo filters are specified. relatesTo filters will then be
      // matched after fetching rows from HBase.
      Set<String> relatesToCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(relatesTo);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          SubApplicationColumnPrefix.RELATES_TO, relatesToCols));
    }
    TimelineFilterList isRelatedTo = getFilters().getIsRelatedTo();
    if (hasField(fieldsToRetrieve, Field.IS_RELATED_TO)) {
      // If IS_RELATED_TO field has to be retrieved, add a filter for fetching
      // columns with IS_RELATED_TO column prefix.
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.EQUAL,
              SubApplicationColumnPrefix.IS_RELATED_TO));
    } else if (isRelatedTo != null && !isRelatedTo.getFilterList().isEmpty()) {
      // Even if fields to retrieve does not contain IS_RELATED_TO, we still
      // need to have a filter to fetch some of the column qualifiers if
      // isRelatedTo filters are specified. isRelatedTo filters will then be
      // matched after fetching rows from HBase.
      Set<String> isRelatedToCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(isRelatedTo);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          SubApplicationColumnPrefix.IS_RELATED_TO, isRelatedToCols));
    }
    TimelineFilterList eventFilters = getFilters().getEventFilters();
    if (hasField(fieldsToRetrieve, Field.EVENTS)) {
      // If EVENTS field has to be retrieved, add a filter for fetching columns
      // with EVENT column prefix.
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.EQUAL,
              SubApplicationColumnPrefix.EVENT));
    } else if (eventFilters != null
        && !eventFilters.getFilterList().isEmpty()) {
      // Even if fields to retrieve does not contain EVENTS, we still need to
      // have a filter to fetch some of the column qualifiers on the basis of
      // event filters specified. Event filters will then be matched after
      // fetching rows from HBase.
      Set<String> eventCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(eventFilters);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          SubApplicationColumnPrefix.EVENT, eventCols));
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
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              SubApplicationColumnPrefix.EVENT));
    }
    // info not required.
    if (!hasField(fieldsToRetrieve, Field.INFO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              SubApplicationColumnPrefix.INFO));
    }
    // is related to not required.
    if (!hasField(fieldsToRetrieve, Field.IS_RELATED_TO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              SubApplicationColumnPrefix.IS_RELATED_TO));
    }
    // relates to not required.
    if (!hasField(fieldsToRetrieve, Field.RELATES_TO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              SubApplicationColumnPrefix.RELATES_TO));
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
      listBasedOnFields.addFilter(
          TimelineFilterUtils.createFilterForConfsOrMetricsToRetrieve(
              dataToRetrieve.getConfsToRetrieve(),
              SubApplicationColumnFamily.CONFIGS,
              SubApplicationColumnPrefix.CONFIG));
    }

    // Please note that if metricsToRetrieve is specified, we would have added
    // METRICS to fields to retrieve in augmentParams() even if not specified.
    if (dataToRetrieve.getFieldsToRetrieve().contains(Field.METRICS)) {
      // Create a filter list for metrics.
      listBasedOnFields.addFilter(
          TimelineFilterUtils.createFilterForConfsOrMetricsToRetrieve(
              dataToRetrieve.getMetricsToRetrieve(),
              SubApplicationColumnFamily.METRICS,
              SubApplicationColumnPrefix.METRIC));
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
    FamilyFilter infoColumnFamily = new FamilyFilter(CompareOp.EQUAL,
        new BinaryComparator(SubApplicationColumnFamily.INFO.getBytes()));
    infoColFamilyList.addFilter(infoColumnFamily);
    if (fetchPartialColsFromInfoFamily()) {
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
  protected void validateParams() {
    Preconditions.checkNotNull(getContext(), "context shouldn't be null");
    Preconditions.checkNotNull(getDataToRetrieve(),
        "data to retrieve shouldn't be null");
    Preconditions.checkNotNull(getContext().getClusterId(),
        "clusterId shouldn't be null");
    Preconditions.checkNotNull(getContext().getDoAsUser(),
        "DoAsUser shouldn't be null");
    Preconditions.checkNotNull(getContext().getEntityType(),
        "entityType shouldn't be null");
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn)
      throws IOException {
    getDataToRetrieve().addFieldsBasedOnConfsAndMetricsToRetrieve();
    createFiltersIfNull();
  }

  private void setMetricsTimeRange(Query query) {
    // Set time range for metric values.
    HBaseTimelineStorageUtils.setMetricsTimeRange(query,
        SubApplicationColumnFamily.METRICS.getBytes(),
        getDataToRetrieve().getMetricsTimeBegin(),
        getDataToRetrieve().getMetricsTimeEnd());
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {

    // Scan through part of the table to find the entities belong to one app
    // and one type
    Scan scan = new Scan();
    TimelineReaderContext context = getContext();
    if (context.getDoAsUser() == null) {
      throw new BadRequestException("Invalid user!");
    }

    RowKeyPrefix<SubApplicationRowKey> subApplicationRowKeyPrefix = null;
    // default mode, will always scans from beginning of entity type.
    if (getFilters() == null || getFilters().getFromId() == null) {
      subApplicationRowKeyPrefix = new SubApplicationRowKeyPrefix(
          context.getDoAsUser(), context.getClusterId(),
          context.getEntityType(), null, null, null);
      scan.setRowPrefixFilter(subApplicationRowKeyPrefix.getRowKeyPrefix());
    } else { // pagination mode, will scan from given entityIdPrefix!enitityId

      SubApplicationRowKey entityRowKey = null;
      try {
        entityRowKey = SubApplicationRowKey
            .parseRowKeyFromString(getFilters().getFromId());
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Invalid filter fromid is provided.");
      }
      if (!context.getClusterId().equals(entityRowKey.getClusterId())) {
        throw new BadRequestException(
            "fromid doesn't belong to clusterId=" + context.getClusterId());
      }

      // set start row
      scan.setStartRow(entityRowKey.getRowKey());

      // get the bytes for stop row
      subApplicationRowKeyPrefix = new SubApplicationRowKeyPrefix(
          context.getDoAsUser(), context.getClusterId(),
          context.getEntityType(), null, null, null);

      // set stop row
      scan.setStopRow(
          HBaseTimelineStorageUtils.calculateTheClosestNextRowKeyForPrefix(
              subApplicationRowKeyPrefix.getRowKeyPrefix()));

      // set page filter to limit. This filter has to set only in pagination
      // mode.
      filterList.addFilter(new PageFilter(getFilters().getLimit()));
    }
    setMetricsTimeRange(scan);
    scan.setMaxVersions(getDataToRetrieve().getMetricsLimit());
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      scan.setFilter(filterList);
    }
    return getTable().getResultScanner(hbaseConf, conn, scan);
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    throw new UnsupportedOperationException(
        "we don't support a single entity query");
  }

  @Override
  protected TimelineEntity parseEntity(Result result) throws IOException {
    if (result == null || result.isEmpty()) {
      return null;
    }
    TimelineEntity entity = new TimelineEntity();
    SubApplicationRowKey parseRowKey =
        SubApplicationRowKey.parseRowKey(result.getRow());
    entity.setType(parseRowKey.getEntityType());
    entity.setId(parseRowKey.getEntityId());
    entity.setIdPrefix(parseRowKey.getEntityIdPrefix().longValue());

    TimelineEntityFilters filters = getFilters();
    // fetch created time
    Long createdTime =
        (Long) SubApplicationColumn.CREATED_TIME.readResult(result);
    entity.setCreatedTime(createdTime);

    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // fetch is related to entities and match isRelatedTo filter. If isRelatedTo
    // filters do not match, entity would be dropped. We have to match filters
    // locally as relevant HBase filters to filter out rows on the basis of
    // isRelatedTo are not set in HBase scan.
    boolean checkIsRelatedTo =
        filters.getIsRelatedTo() != null
            && filters.getIsRelatedTo().getFilterList().size() > 0;
    if (hasField(fieldsToRetrieve, Field.IS_RELATED_TO) || checkIsRelatedTo) {
      readRelationship(entity, result, SubApplicationColumnPrefix.IS_RELATED_TO,
          true);
      if (checkIsRelatedTo && !TimelineStorageUtils.matchIsRelatedTo(entity,
          filters.getIsRelatedTo())) {
        return null;
      }
      if (!hasField(fieldsToRetrieve, Field.IS_RELATED_TO)) {
        entity.getIsRelatedToEntities().clear();
      }
    }

    // fetch relates to entities and match relatesTo filter. If relatesTo
    // filters do not match, entity would be dropped. We have to match filters
    // locally as relevant HBase filters to filter out rows on the basis of
    // relatesTo are not set in HBase scan.
    boolean checkRelatesTo =
        !isSingleEntityRead() && filters.getRelatesTo() != null
            && filters.getRelatesTo().getFilterList().size() > 0;
    if (hasField(fieldsToRetrieve, Field.RELATES_TO) || checkRelatesTo) {
      readRelationship(entity, result, SubApplicationColumnPrefix.RELATES_TO,
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
      readKeyValuePairs(entity, result, SubApplicationColumnPrefix.INFO, false);
    }

    // fetch configs if fieldsToRetrieve contains CONFIGS or ALL.
    if (hasField(fieldsToRetrieve, Field.CONFIGS)) {
      readKeyValuePairs(entity, result, SubApplicationColumnPrefix.CONFIG,
          true);
    }

    // fetch events and match event filters if they exist. If event filters do
    // not match, entity would be dropped. We have to match filters locally
    // as relevant HBase filters to filter out rows on the basis of events
    // are not set in HBase scan.
    boolean checkEvents =
        !isSingleEntityRead() && filters.getEventFilters() != null
            && filters.getEventFilters().getFilterList().size() > 0;
    if (hasField(fieldsToRetrieve, Field.EVENTS) || checkEvents) {
      readEvents(entity, result, SubApplicationColumnPrefix.EVENT);
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
      readMetrics(entity, result, SubApplicationColumnPrefix.METRIC);
    }

    entity.getInfo().put(TimelineReaderUtils.FROMID_KEY,
        parseRowKey.getRowKeyAsString());
    return entity;
  }

}
